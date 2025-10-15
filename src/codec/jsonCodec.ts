import { Codec, Envelope } from "../core/types.js";

export function jsonCodec<T>(): Codec<T> {
    return {
        encode: (env: Envelope<T>) => ({
            headers: JSON.stringify(env.headers),
            payload: JSON.stringify(env.payload),
        }),
        decode: (fields: Record<string, string>) => ({
            headers: JSON.parse(fields["headers"]),
            payload: JSON.parse(fields["payload"]),
        }),
    };
}
