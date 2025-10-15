import {StandaloneGlideKitClient} from "../src/core/standaloneGlideKitClient.js";
import dotenv from "dotenv";
import {makeProducer} from "../src/producer.js";
import {jsonCodec} from "../src/codec/jsonCodec.js";
import {makeConsumer} from "../src/consumer.js";
import {backoffPolicy} from "../src/retry.js";
import {expect} from "vitest";
import {consoleLogger} from "../src/core/types.js";

dotenv.config();

describe('Integration', async () => {
    it('should work end-to-end', async () => {
        if (!process.env.VALKEY_HOST) throw new Error("VALKEY_HOST not set");
        if (!process.env.VALKEY_PORT) throw new Error("VALKEY_PORT not set");

        const testFn = vi.fn()

        const client = new StandaloneGlideKitClient({
            addresses: [
                {
                    host: process.env.VALKEY_HOST,
                    port: Number(process.env.VALKEY_PORT)
                }
            ]
        });

        interface TestJob {
            value: string;
        }

        const producer = makeProducer<TestJob>({
            client,
            stream: "test",
            codec: jsonCodec<TestJob>(),
            defaultType: "test.set",
            log: consoleLogger
        });

        const worker = makeConsumer({
            client,
            stream: 'test',
            group: 'test:svc',
            consumer: `c-${Math.random().toString(36).slice(2)}`,
            codec: jsonCodec<TestJob>(),
            retryPolicy: backoffPolicy({
                maxAttempts: 5,
                strategy: {kind: 'exponential-jitter', baseMs: 250, maxDelayMs: 60_000}
            }),
            handler: async (job) => {
                testFn(job);
            },
            log: consoleLogger
        });

        const job = {value: "hello world"};

        await worker.setup();
        await producer.send(job);
        await worker.start();

        await expect.poll(() => testFn, {timeout: 10_000}).toBeCalledWith(job);

    }, 10_000);

})