import { GlideKitClient } from "../core/types.js";

export type RetryDaemonOpts = {
    client: GlideKitClient;
    retryZset: string;        // e.g., orders:retry
    targetStream: string;     // e.g., orders
    maxBatch?: number;        // how many to move per tick
    tickMs?: number;          // sleep between ticks
    jitterPct?: number;       // 0..1 add +- jitter to tick
    log?: { info: Function; warn: Function; error: Function; debug: Function };
};

export type RetryDaemon = {
    start(): void;
    stop(): Promise<void>; // resolves when loop has exited
    isRunning(): boolean;
};

export function startRetryDaemon(opts: RetryDaemonOpts): RetryDaemon {
    const {
        client,
        retryZset,
        targetStream,
        maxBatch = 256,
        tickMs = 250,
        jitterPct = 0.2,
        log = { info: () => {}, warn: () => {}, error: () => {}, debug: () => {} },
    } = opts;

    let running = false;
    let loopPromise: Promise<void> | null = null;

    async function tickOnce() {
        const now = Date.now();

        const ready: Array<{ score: number; member: string }> = [];

        if (client.zpopmin) {
            // Pop up to maxBatch entries whose score <= now
            for (let i = 0; i < maxBatch; i++) {
                const items = await client.zpopmin(retryZset);
                if (!items || items.length === 0) break;
                const first = items[0];
                if (first.score > now) {
                    // Not ready yet; reinsert and exit (best-effort)
                    await client.zadd?.(retryZset, [first]);
                    break;
                }
                ready.push(first);
                if (items.length > 1) {
                    // If binding returns more than one, push the rest back (conservative)
                    const rest = items.slice(1);
                    if (rest.length) await client.zadd?.(retryZset, rest);
                }
            }
        } else if (client.zrangebyscore && client.zrem) {
            const items = await client.zrangebyscore(retryZset, -Infinity as any, now, {
                limit: maxBatch,
            });
            if (items?.length) {
                const members = items.map((x) => x.member);
                const removed = await client.zrem(retryZset, members);
                if (removed > 0) ready.push(...items.slice(0, removed));
            }
        } else {
            log.warn("retry-daemon: no ZPOP or ZRANGEBYSCORE available; disabled");
            return;
        }

        for (const { member } of ready) {
            try {
                log.debug("member: ", member);
                const parsed = JSON.parse(member) as { stream: string; fields: Record<string, string> };
                const dest = parsed.stream || targetStream;
                await client.xadd(dest, parsed.fields);
                log.debug("retry->xadd", { dest });
            } catch (err) {
                log.error("retry-daemon parse/enqueue error", { err });
            }
        }
    }

    async function loop() {
        running = true;
        try {
            while (running) {
                await tickOnce();
                const jitter = 1 + (Math.random() * 2 - 1) * jitterPct; // 1±jitterPct
                await new Promise((r) => setTimeout(r, Math.max(25, tickMs * jitter)));
            }
        } finally {
            running = false;
        }
    }

    return {
        start() {
            if (loopPromise) return;
            loopPromise = loop();
        },
        async stop() {
            if (!loopPromise) return;
            running = false;
            await loopPromise; // wait for exit
            loopPromise = null;
        },
        isRunning() {
            return !!loopPromise && running;
        },
    };
}
