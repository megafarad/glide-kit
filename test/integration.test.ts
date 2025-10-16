import {
    backoffPolicy,
    consoleLogger,
    jsonCodec,
    makeConsumer,
    makeProducer,
    startRetryDaemon,
    StandaloneGlideKitClient
} from "../src";
import dotenv from "dotenv";
import {expect} from "vitest";

dotenv.config();

interface TestJob {
    value: string;
}


describe('Integration', async () => {
    it('should work end-to-end', async () => {
        if (!process.env.VALKEY_HOST) throw new Error("VALKEY_HOST not set");
        if (!process.env.VALKEY_PORT) throw new Error("VALKEY_PORT not set");

        const testFn = vi.fn((_job: TestJob) => Promise.resolve());
        testFn.mockRejectedValueOnce(new Error("test error"));
        testFn.mockRejectedValueOnce(new Error("test error 2"));
        testFn.mockResolvedValue()

        const client = new StandaloneGlideKitClient({
            addresses: [
                {
                    host: process.env.VALKEY_HOST,
                    port: Number(process.env.VALKEY_PORT)
                }
            ],
            requestTimeout: 10_000,
        });

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
                await testFn(job);
            },
            log: consoleLogger
        });

        const job = {value: "hello world"};

        await worker.start().then(() => {
            console.log("worker started");
        });

        const daemon = startRetryDaemon({
            client,
            retryZset: "test:retry",
            targetStream: "test",
            log: consoleLogger
        });

        daemon.start();

        await producer.send(job).then(() => {
            console.log("job sent");
        }).catch((e) => {
            console.error("job send error", e);
        });

        await expect.poll(() => testFn, {timeout: 10_000}).toBeCalledTimes(3);

    }, 15_000);

})