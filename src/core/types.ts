export type Millis = number;

export type MessageHeaders = {
    type: string;
    attempt: number; // 0 on first try
    enqueuedAt: number; // epoch ms
    traceId?: string;
    key?: string; // optional idempotency key
};

export type Envelope<T> = {
    headers: MessageHeaders;
    payload: T;
};

export type RetryResult =
    | { action: "ack" }
    | { action: "retry"; delayMs?: Millis }
    | { action: "dlq"; reason?: string; meta?: unknown };

export interface RetryPolicy {
    next: (headers: MessageHeaders, err: unknown) => RetryResult;
}

export interface Codec<T> {
    encode: (env: Envelope<T>) => Record<string, string>;
    decode: (fields: Record<string, string>) => Envelope<T>;
}

export interface LoggerLike {
    debug: (...args: unknown[]) => void;
    info: (...args: unknown[]) => void;
    warn: (...args: unknown[]) => void;
    error: (...args: unknown[]) => void;
}

export const noopLogger: LoggerLike = {
    debug: () => {
    },
    info: () => {
    },
    warn: () => {
    },
    error: () => {
    },
};

export const consoleLogger: LoggerLike = {
    debug: console.debug,
    info: console.info,
    warn: console.warn,
    error: console.error,
};

export enum Decoder {
    Bytes = 0,
    String = 1
}

export interface IGlideKitClient {
    xadd: (
        stream: string,
        fields: Record<string, string>,
        opts?: { id?: string }
    ) => Promise<string | null>; // returns id

    xack: (stream: string, group: string, ids: string[]) => Promise<number>;

    xreadgroup: (args: {
        group: string;
        consumer: string;
        blockMs: number;
        count: number;
        streams: { key: string; id: ">" }[]; // we only read new entries in this skeleton
    }) => Promise<XReadGroupResult | null>;

    xinfoGroups: (key: string, options?: { decoder?: Decoder }) => Promise<Record<string, number | string | null>[]>;

    xgroupCreate: (
        key: string,
        group: string,
        id: string,
        opts?: { mkStream?: boolean, entriesRead?: string }
    ) => Promise<string>;

    xlen: (key: string) => Promise<number>;

    xpending?: (
        stream: string,
        group: string,
        opts: { idle: number; count: number; start: string; end: string }
    ) => Promise<Array<{ id: string; consumer: string; idle: number; deliveries: number }>>;

    xclaim?: (
        stream: string,
        group: string,
        consumer: string,
        minIdleMs: number,
        ids: string[],
        opts?: { retrycount?: number }
    ) => Promise<Array<{ id: string; fields: Record<string, string> }>>;

    // Optional helpers for retries (ZSET based scheduler)
    zadd?: (
        key: string,
        scoreMembers: Array<{ score: number; member: string }>
    ) => Promise<number>;

    zpopmin?: (key: string) => Promise<Array<{ score: number; member: string }>>;

    zrangebyscore?: (
        key: string,
        min: number,
        max: number,
        opts?: { limit?: number }
    ) => Promise<Array<{ score: number; member: string }>>;

    zrem?: (key: string, members: string[]) => Promise<number>;

}

export type IdempotencyCache = {
    setIfNotExists: (
        key: string,
        ttlSeconds: number
    ) => Promise<boolean>; // true if set, false if already present
};

export type XReadGroupResult = Array<{
    stream: string;
    messages: Array<{ id: string; fields: Record<string, string> }>;
}>;
