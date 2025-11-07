# glide-kit

> Type-safe, opinionated job runner for **Valkey Streams** (via GLIDE): producer, consumer, retries with backoff, DLQ, and crash recovery — all in ~a few files.

---

## Why

Valkey/Redis Streams + consumer groups are powerful but spiky to use directly. This library focuses on a **small, sharp** set of patterns:

* **Typed payloads** with pluggable codecs (JSON by default).
* **At-least-once** delivery with simple idempotency hooks.
* **First‑class retries** (exponential jitter) using a retry ZSET + daemon.
* **DLQ** with rich error metadata.
* **Crash recovery** by draining the Pending Entries List (PEL) using `XPENDING`/`XCLAIM`.
* **Composable** primitives, not a framework.

---

## Install

```bash
npm i @sirhc77/glide-kit
# or
pnpm add @sirhc77/glide-kit
```

> Requires Node 18+ and a Valkey/Redis server. Uses the GLIDE client under the hood via a tiny adapter.

---

## Quick start

### 1) Wire the GLIDE adapter

```ts
import { StandaloneGlideKitClient } from "@sirhc77/glide-kit";

const client = new StandaloneGlideKitClient({
    addresses: [
        {
            host: process.env.VALKEY_HOST!,
            port: Number(process.env.VALKEY_PORT!)
        }
    ],
});

```

### 2) Produce jobs

```ts
import { makeProducer, jsonCodec } from "@sirhc77/glide-kit";

type EmailJob = { to: string; subject: string; body: string };

const producer = makeProducer<EmailJob>({
  client,
  stream: "email",
  codec: jsonCodec<EmailJob>(),
  defaultType: "email.send",
});

await producer.send({ to: "a@b.com", subject: "hi", body: "…" }, { key: "a@b.com:hi" });
```

### 3) Consume + handle retries/DLQ automatically

```ts
import { makeConsumer, backoffPolicy } from "@sirhc77/glide-kit";

const worker = makeConsumer<EmailJob>({
  client,
  stream: "email",
  group: "email:svc-sender",
  consumer: `c-${process.pid}`,
  codec: jsonCodec<EmailJob>(),
  handler: async (job) => {
    await sendEmail(job.to, job.subject, job.body); // your side effect
    return { action: "ack" };
  },
  retryPolicy: backoffPolicy({
    maxAttempts: 5,
    strategy: { kind: "exponential-jitter", baseMs: 250, maxDelayMs: 60_000 },
    isRetryable: (err) => !(err as any)?.fatal,
  }),
  scheduling: { mode: "zset", retryZset: "email:retry" },
  pelClaim: { enabled: true, minIdleMs: 30_000, maxPerTick: 128, intervalMs: 1000 },
});

await worker.start();
// later: await worker.stop({ drain: true, timeoutMs: 10_000 });
```

### 4) Start the retry daemon

```ts
import { startRetryDaemon } from "@sirhc77/glide-kit";

const retry = startRetryDaemon({
  client,
  retryZset: "email:retry",
  targetStream: "email",
  maxBatch: 256,
  tickMs: 250,
  jitterPct: 0.2,
  log: console,
});

retry.start();
// later: await retry.stop();
```

---

## Concepts & defaults

* **Streams**: one main stream per domain (e.g., `email`).
* **Consumer group**: one per service (e.g., `email:svc-sender`).
* **Retry**: a ZSET `<stream>:retry` stores encoded messages with score = `now + delay`.
* **DLQ**: `<stream>:dlq` receives full envelope + error metadata.
* **PEL drain**: background loop claims long‑idle PEL entries and processes them.

```txt
email            (main stream)
email:retry      (ZSET scheduler for backoff)
email:dlq        (dead letter stream)
```

---

## API (surface)

### Producer

```ts
makeProducer<T>({ client, stream, codec, defaultType?, idempotency? }): Producer<T>
Producer<T>.send(payload: T, opts?: { type?: string; key?: string }): Promise<string>
```

* `key`: optional idempotency key; if configured with an idempotency cache, duplicate enqueues are dropped.

### Consumer

```ts
makeConsumer<T>({
  client, stream, group, consumer,
  codec, handler, retryPolicy,
  scheduling?: { mode: "zset" | "none"; retryZset?: string },
  batch?: { count: number; blockMs: number },
  pelClaim?: { enabled?: boolean; minIdleMs: number; maxPerTick?: number; intervalMs?: number },
  log?: LoggerLike,
}): ConsumerWorker<T>

ConsumerWorker.start(): Promise<void>
ConsumerWorker.stop({ drain?: boolean; timeoutMs?: number }): Promise<void>
```

* **Handler contract**: return `{action: "ack"}` on success; `{action: "retry", delayMs?}` to backoff; `{action: "dlq", reason?, meta?}` to give up.

### Retry daemon

```ts
startRetryDaemon({ client, retryZset, targetStream, maxBatch?, tickMs?, jitterPct?, log? }): RetryDaemon
RetryDaemon.start();
RetryDaemon.stop();
```

### Types

```ts
Envelope<T> { headers: { type, attempt, enqueuedAt, key?, traceId? }, payload: T }
RetryPolicy.next(headers, err) => RetryResult
Codec<T> encode/decode fields <-> Envelope<T>
```

---

## Error handling & backoff

Ship with a default **exponential‑jitter** policy; override `isRetryable` to route validation/fatal errors to DLQ immediately. Attempts are counted in headers and preserved across retries.

---

## Crash recovery (PEL drain)

Enable `pelClaim` to periodically claim long‑idle messages for the current consumer via `XPENDING`/`XCLAIM`, then process them through the same handler pipeline (ack/retry/DLQ). Tunables let you control cadence and per‑tick load.

---

## Observability (suggested)

Expose counters/gauges via your metrics backend (e.g., Prometheus):

* `produced_total`, `processed_total`, `retry_scheduled_total`, `dlq_total`, `claimed_total`
* `in_flight`, `retry_queue_depth` (use `ZCARD`), `pel_size` (from `XPENDING` summary)
* `handler_duration_ms` histogram

Structured logs include `{ stream, type, id, attempt }` on ack/retry/dlq.

---

## Testing

* **Integration tests** with a real Valkey container (or Embedded Valkey). Cover:

    * happy path ack;
    * retry → final ack;
    * max attempts → DLQ;
* Provide a stub `StreamsClient` for pure unit tests of the loop.

---

## Idempotency (optional)

Hook an `IdempotencyCache` (e.g., Redis `SETNX EX`) at the producer. Duplicate enqueues with the same `key` can be dropped and return a sentinel id.

---

## GLIDE adapter

A small adapter normalizes GLIDE’s `xreadgroup` return (`GlideRecord<Record<string, [GlideString, GlideString][] | null>> | null>`) into a simple array of `{ stream, messages: [{ id, fields }] }`, handling multiple field encodings (JSON string / flat array / object map).

---

## Roadmap

* Metrics module (Prom + OpenTelemetry).
* Idempotency cache helper.
* DLQ CLI: peek/requeue/purge.
* Multi‑stream fan‑in.
* Examples repo with flaky handlers and dashboards.

---

## Contributing

Issues and PRs welcome. Please include a reproduction or failing test. Keep the surface small and typed.

---

## License

MIT
