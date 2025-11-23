import {Codec, Envelope, MessageHeaders, IGlideKitClient, LoggerLike} from "../core/types";
import {Script} from "@valkey/valkey-glide";

export type MakeProducerOpts<T> = {
    client: IGlideKitClient;
    stream: string;
    codec: Codec<T>;
    defaultType?: string;
    idempotency?: { ttlSec: number };
    log?: LoggerLike;
};

export interface Producer<T> {
    send: (payload: T, opts?: { type?: string; key?: string }) => Promise<string | null>;
}

const idempotencyScript = new Script(
    `
if ((#ARGV - 2) % 2) ~= 0 then
  return server.error_reply('XADD fields must be key/value pairs')
end

local reserved = server.call('SET', KEYS[1], 'PENDING', 'NX', 'EX', ARGV[1])
if reserved then
  -- First time: enqueue, then store final id
  local id = server.call('XADD', ARGV[2], '*', unpack(ARGV, 3, #ARGV))
  server.call('SET', KEYS[1], id, 'EX', ARGV[1])
  return id
else
  -- Duplicate: return whatever’s there ("PENDING" or a real id)
  local val = server.call('GET', KEYS[1])
  return val or ''
end`)

export function makeProducer<T>(opts: MakeProducerOpts<T>): Producer<T> {
    const { client, stream, codec, defaultType, log } = opts;

    return {
        async send(payload, sendOpts) {
            const headers: MessageHeaders = {
                type: sendOpts?.type ?? defaultType ?? "msg",
                attempt: 0,
                enqueuedAt: Date.now(),
                key: sendOpts?.key,
            };

            const env: Envelope<T> = { headers, payload };
            log?.debug("send", { stream, type: headers.type, key: headers.key });

            if (headers.key && opts.idempotency) {
                const idempotencyKey = `idempotency:${stream}:${headers.type}:${headers.key}`;

                const fields: string[] = [];
                fields.push(opts.idempotency.ttlSec.toString(), stream);
                for (const [key, value] of Object.entries(codec.encode(env))) fields.push(key, value);

                const result =  await client.invokeScript(idempotencyScript,
                    {keys: [idempotencyKey], args: fields});
                if (typeof result === "string") {
                    return result;
                } else {
                    return result?.toString() ?? null;
                }
            } else {
                return await client.xadd(stream, codec.encode(env));
            }
        },
    };
}
