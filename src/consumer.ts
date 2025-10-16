import {
    Codec,
    Envelope,
    LoggerLike,
    RetryPolicy,
    RetryResult,
    GlideKitClient,
    noopLogger,
} from "./core/types.js";

export type Handler<T> = (
    payload: T,
    ctx: { headers: Envelope<T>["headers"]; id: string }
) => Promise<RetryResult | void>;

export type MakeConsumerOpts<T> = {
    client: GlideKitClient;
    stream: string;
    group: string;
    consumer: string;
    codec: Codec<T>;
    handler: Handler<T>;
    retryPolicy: RetryPolicy;
    scheduling?: { mode: "zset" | "none"; retryZset?: string };
    batch?: { count: number; blockMs: number };
    log?: LoggerLike;
};

export interface ConsumerWorker<T> {
    start(): Promise<void>;

    stop(opts?: { drain?: boolean; timeoutMs?: number }): Promise<void>;
}

export function makeConsumer<T>(opts: MakeConsumerOpts<T>): ConsumerWorker<T> {
    const {
        client,
        stream,
        group,
        consumer,
        codec,
        retryPolicy,
        handler,
        batch = {count: 16, blockMs: 2000},
        scheduling = {mode: "zset"},
        log = noopLogger,
    } = opts;

    let running = false;
    let inFlight = 0;

    async function ensureGroup() {
        const groups = await client.xinfoGroups(stream).catch(() => []);
        const groupCreated = groups.some(record => record['name'] === group);
        if (groupCreated) return;
        return await client.xgroupCreate(stream, group, "$", {
            mkStream: true
        }).catch((e) => {
            const msg = e instanceof Error ? e.message : String(e);
            if (msg.includes("BUSYGROUP")) return;
            throw e;
        });
    }

    async function processMessage(id: string, fields: Record<string, string>) {
        log.debug("processMessage", {stream, group, id, type: fields.headers_type});
        try {
            const env = codec.decode(fields);
            const handlerResult = await handler(env.payload, {headers: env.headers, id})
                .catch((e) => {
                    return retryPolicy.next(env.headers, e);
                });
            const res = handlerResult || {
                action: "ack",
            };

            if (res.action === "ack") {
                await client.xack(stream, group, [id]);
                log.debug("ack", {stream, group, id, type: env.headers.type});
                return;
            }

            if (res.action === "retry") {
                const delay = res.delayMs ?? 0;
                // Strategy: re-enqueue with attempt+1, then ack original
                const nextEnv: Envelope<T> = {
                    headers: {
                        ...env.headers,
                        attempt: env.headers.attempt + 1,
                        enqueuedAt: Date.now(),
                    },
                    payload: env.payload,
                };

                if (scheduling.mode === "zset" && client.zadd) {
                    const retryKey = opts.scheduling?.retryZset || `${stream}:retry`;
                    const member = JSON.stringify({stream, env: nextEnv});
                    await client.zadd(retryKey, [
                        {score: Date.now() + delay, member},
                    ]);
                } else {
                    // immediate requeue fallback
                    await client.xadd(stream, codec.encode(nextEnv));
                }
                await client.xack(stream, group, [id]);
                log.info("retry scheduled", {
                    stream,
                    group,
                    id,
                    attempt: nextEnv.headers.attempt,
                    delay,
                });
                return;
            }

            if (res.action === "dlq") {
                const dlqStream = `${stream}:dlq`;
                const dlqPayload = {
                    headers: {...env.headers},
                    payload: env.payload,
                    error: {reason: res.reason, meta: res.meta},
                    handledBy: {group, consumer},
                };
                await client.xadd(dlqStream, {
                    headers: JSON.stringify(dlqPayload.headers),
                    payload: JSON.stringify(dlqPayload.payload),
                    error: JSON.stringify(dlqPayload.error),
                    handledBy: JSON.stringify(dlqPayload.handledBy),
                });
                await client.xack(stream, group, [id]);
                log.warn("dlq", {stream, group, id, reason: res.reason});
                return;
            }
        } catch (err) {
            // Last-chance handler error → schedule retry with policy based on a synthetic envelope
            log.error("handler exception", {stream, group, id, err});
            // Naive: ack to avoid tight loop; caller should rely on idle sweeper for robustness
            await client.xack(stream, group, [id]);
        }
    }

    async function loop() {

        while (running) {
            const res = await client.xreadgroup({
                group,
                consumer,
                blockMs: batch.blockMs,
                count: batch.count,
                streams: [{key: stream, id: ">"}],
            });

            log.debug("xreadgroup", {stream, group, count: res?.length});

            if (!res || res.length === 0) continue;

            for (const part of res) {
                log.debug("xreadgroup.part", {stream, group, count: part.messages.length});
                for (const msg of part.messages) {
                    log.debug("xreadgroup.msg", {stream, group, id: msg.id});
                    inFlight++;
                    await processMessage(msg.id, msg.fields);
                    inFlight--;
                }
            }
        }
    }

    return {
        async start() {
            if (running) return;
            running = true;
            await ensureGroup();
            // Fire-and-forget; if you prefer, you can manage the promise outside
            void loop();
        },
        async stop({drain = true, timeoutMs = 10_000} = {}) {
            if (!running) return;
            if (!drain) {
                running = false;
                return;
            }
            const deadline = Date.now() + timeoutMs;
            running = false;
            while (inFlight > 0 && Date.now() < deadline) {
                await new Promise((r) => setTimeout(r, 25));
            }
        },
    };
}
