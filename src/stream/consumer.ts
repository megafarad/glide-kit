import {
    Codec,
    Envelope,
    LoggerLike,
    RetryPolicy,
    RetryResult,
    IGlideKitClient,
    noopLogger,
} from "../core/types";

export type Handler<T> = (
    payload: T,
    ctx: { headers: Envelope<T>["headers"]; id: string }
) => Promise<RetryResult | void>;

export type MakeConsumerOpts<T> = {
    client: IGlideKitClient;
    stream: string;
    group: string;
    consumer: string;
    codec: Codec<T>;
    handler: Handler<T>;
    retryPolicy: RetryPolicy;
    scheduling?: { mode: "zset" | "none"; retryZset?: string };
    batch?: { count: number; blockMs: number };
    log?: LoggerLike;
    idempotency?: { pendingTtlSec: number, doneTtlSec: number }
    pelClaim?: {
        enabled?: boolean;        // default true
        minIdleMs: number;        // older than this is eligible
        maxPerTick?: number;      // default 128
        intervalMs?: number;      // default 1000
    };
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
        pelClaim
    } = opts;

    let running = false;
    let inFlight = 0;
    let claimLoopPromise: Promise<void> | null = null;

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
        const env = codec.decode(fields);
        const idempotencyKey = opts.idempotency?.pendingTtlSec && env.headers.key ?
            `consumed:${stream}:${env.headers.key}` : undefined;

        let reservedByUs = false;

        try {
            if (idempotencyKey) {
                const ok = await client.setNx(idempotencyKey, `PENDING:${consumer}`,
                    opts.idempotency?.pendingTtlSec);

                if (ok !== 'OK') {
                    const value = await client.get(idempotencyKey);
                    if (value === 'DONE') {
                        log.debug("idempotency: already done", {idempotencyKey});
                        await client.xack(stream, group, [id]);
                        return;
                    }

                    await client.xack(stream, group, [id]);
                    await client.zadd?.(`${stream}:retry`, [{score: Date.now() + 500, member: JSON.stringify({stream, fields})}]);
                    return;
                }
                reservedByUs = true;
            }

            const handlerResult = await handler(env.payload, {headers: env.headers, id})
                .catch((e) => {
                    return retryPolicy.next(env.headers, e);
                });
            const res = handlerResult || {
                action: "ack",
            };

            if (res.action === "ack") {
                await client.xack(stream, group, [id]);
                if (idempotencyKey) {
                    await client.setNx(idempotencyKey, `DONE`, opts.idempotency?.doneTtlSec);
                    log.debug("idempotency: done", {idempotencyKey});
                }
                log.debug("ack", {stream, group, id, type: env.headers.type});
                return;
            }

            if (res.action === "retry") {
                if (reservedByUs && idempotencyKey) {
                    await client.del(idempotencyKey);
                }

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

                const fields = codec.encode(nextEnv);

                if (scheduling.mode === "zset" && client.zadd) {
                    const retryKey = opts.scheduling?.retryZset || `${stream}:retry`;
                    const member = JSON.stringify({stream, fields});
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
                if (reservedByUs && idempotencyKey) {
                    await client.del(idempotencyKey);
                }

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
            if (reservedByUs && idempotencyKey) {
                await client.del(idempotencyKey);
            }
            // Last-chance handler error → schedule retry with policy based on a synthetic envelope
            log.error("handler exception", {stream, group, id, err});
            // Naive: ack to avoid tight loop; caller should rely on idle sweeper for robustness
            await client.xack(stream, group, [id]);
        }
    }

    async function claimOnce() {
        const cfg = pelClaim ?? { enabled: true, minIdleMs: 30_000, maxPerTick: 128, intervalMs: 1000 };
        if (!cfg.enabled || !client.xpending || !client.xclaim) return;
        try {
            const pendingEntries = await client.xpending(stream, group, {
                idle: cfg.minIdleMs,
                count: cfg.maxPerTick ?? 128,
                start: "-",
                end: "+",
            });

            if (!pendingEntries || pendingEntries.length === 0) return;
            const ids = pendingEntries.map((p) => p.id);
            const claimed = await client.xclaim(stream, group, consumer, cfg.minIdleMs, ids, { retrycount: 1 });
            for (const { id, fields } of claimed) {
                inFlight++;
                await processMessage(id, fields);
                inFlight--;
            }
            log.debug("pel-claimed", { count: claimed.length });
        } catch (err) {
            log.error("pel-claim error", { err });
        }
    }

    async function claimLoop() {
        const interval = (pelClaim?.intervalMs ?? 1000);
        while (running) {
            await claimOnce();
            await new Promise((r) => setTimeout(r, interval));
        }
    }

    async function loop() {

        while (running) {
            try {
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
            } catch (err) {
                log.error("xreadgroup error", {err});
                await new Promise((r) => setTimeout(r, 250));
            }
        }
    }

    return {
        async start() {
            if (running) return;
            await ensureGroup();
            running = true;
            // Fire-and-forget; if you prefer, you can manage the promise outside
            void loop();
            if (pelClaim?.enabled && client.xpending && client.xclaim) {
                claimLoopPromise = claimLoop();
            }
        },
        async stop({drain = true, timeoutMs = 10_000} = {}) {
            if (!running) return;
            if (!drain) {
                running = false;
                if (claimLoopPromise) await claimLoopPromise;
                claimLoopPromise = null;
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
