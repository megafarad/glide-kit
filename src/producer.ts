import {Codec, Envelope, IdempotencyCache, MessageHeaders, IGlideKitClient} from "./core/types.js";

export type MakeProducerOpts<T> = {
    client: IGlideKitClient;
    stream: string;
    codec: Codec<T>;
    defaultType?: string;
    idempotency?: { cache: IdempotencyCache; ttlSec: number };
};

export interface Producer<T> {
    send: (payload: T, opts?: { type?: string; key?: string }) => Promise<string | null>;
}

export function makeProducer<T>(opts: MakeProducerOpts<T>): Producer<T> {
    const { client, stream, codec, defaultType } = opts;

    return {
        async send(payload, sendOpts) {
            const headers: MessageHeaders = {
                type: sendOpts?.type ?? defaultType ?? "msg",
                attempt: 0,
                enqueuedAt: Date.now(),
                key: sendOpts?.key,
            };

            if (headers.key && opts.idempotency) {
                const ok = await opts.idempotency.cache.setIfNotExists(
                    `idemp:${stream}:${headers.key}`,
                    opts.idempotency.ttlSec
                );
                if (!ok) {
                    // Treat as successfully enqueued (duplicate suppressed)
                    return "0-0"; // sentinel id for dedup’d enqueue
                }
            }

            const env: Envelope<T> = { headers, payload };
            return await client.xadd(stream, codec.encode(env));
        },
    };
}
