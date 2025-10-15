import { MessageHeaders, RetryPolicy, RetryResult } from "./core/types.js";

export type BackoffStrategy =
    | { kind: "constant"; delayMs: number }
    | { kind: "exponential-jitter"; baseMs: number; maxDelayMs: number };

export function backoffPolicy(opts: {
    maxAttempts: number;
    strategy: BackoffStrategy;
    isRetryable?: (err: unknown) => boolean;
}): RetryPolicy {
    const isRetryable = opts.isRetryable ?? (() => true);
    return {
        next(headers: MessageHeaders, err: unknown): RetryResult {
            if (!isRetryable(err)) return { action: "dlq", reason: "non-retryable" };
            const attempt = headers.attempt + 1;
            if (attempt >= opts.maxAttempts) {
                return { action: "dlq", reason: `maxAttempts(${opts.maxAttempts})` };
            }
            let delayMs = 0;
            if (opts.strategy.kind === "constant") {
                delayMs = opts.strategy.delayMs;
            } else {
                const exp = Math.min(
                    opts.strategy.maxDelayMs,
                    opts.strategy.baseMs * 2 ** headers.attempt
                );
                delayMs = Math.floor(Math.random() * (exp + 1)); // full jitter
            }
            return { action: "retry", delayMs };
        },
    };
}
