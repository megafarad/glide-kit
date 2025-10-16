import {GlideKitClient} from "../core/types.js";

export type PendingSweeperOpts = {
    client: GlideKitClient;
    stream: string;
    group: string;
    consumer: string;            // who will claim
    minIdleMs: number;           // older than this → eligible
    maxPerTick?: number;         // limit claims per tick
    tickMs?: number;             // polling interval
    log?: { info: Function; warn: Function; error: Function; debug: Function };
};

export type PendingSweeper = {
    start(): void;
    stop(): Promise<void>;
    isRunning(): boolean;
};

export function startPendingSweeper(opts: PendingSweeperOpts): PendingSweeper {
    const {
        client,
        stream,
        group,
        consumer,
        minIdleMs,
        maxPerTick = 128,
        tickMs = 1000,
        log = { info: () => {}, warn: () => {}, error: () => {}, debug: () => {} },
    } = opts;

    if (!client.xpending || !client.xclaim) {
        log.warn("pending-sweeper: xpending/xclaim not available; disabled");
        return { start() {}, async stop() {}, isRunning: () => false };
    }

    let running = false;
    let loopPromise: Promise<void> | null = null;

    async function tickOnce() {
        try {
            const pendingEntries = client.xpending ? await client.xpending(stream, group, {
                idle: minIdleMs,
                count: maxPerTick,
                start: "-",
                end: "+",
            }) : [];
            if (!pendingEntries || pendingEntries.length === 0) return;

            const ids = pendingEntries.map((p) => p.id);
            const claimed = client.xclaim ? await client.xclaim(stream, group, consumer, minIdleMs, ids, {
                retrycount: 1,
            }) : [];

            log.debug("pending-sweeper: claimed", { count: claimed.length });
            // Claimed messages will be seen by the consumer loop if reading PEL; our loop reads new entries only.
            // Two options:
            // 1) Re-enqueue cloned messages (attempt++) and XACK originals (requires PEL read to ack original)
            // 2) Let a separate PEL-processing loop handle them.
            //
            // For now, we just claim to the current consumer; your consumer can add a path to also read from PEL if desired.
        } catch (err) {
            log.error("pending-sweeper error", { err });
        }
    }

    async function loop() {
        running = true;
        try {
            while (running) {
                await tickOnce();
                await new Promise((r) => setTimeout(r, tickMs));
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
            await loopPromise;
            loopPromise = null;
        },
        isRunning() {
            return !!loopPromise && running;
        },
    };
}

