import {Decoder, IGlideKitClient, XReadGroupResult} from "./types.js";
import {GlideClient, GlideClientConfiguration} from "@valkey/valkey-glide";

export class StandaloneGlideKitClient implements IGlideKitClient {

    private readonly createdClient: Promise<GlideClient>;

    constructor(config: GlideClientConfiguration, private encoding?: BufferEncoding) {
        this.createdClient = GlideClient.createClient(config)
    }

    async xack(stream: string, group: string, ids: string[]): Promise<number> {
        const client = await this.createdClient;
        return await client.xack(stream, group, ids);
    }

    async xadd(stream: string, fields: Record<string, string>, opts: {
        id?: string
    } | undefined): Promise<string | null> {
        const client = await this.createdClient;
        const values: [string, string][] = Object.entries(fields).map(([k, v]) => [k, v]);
        const result = await client.xadd(stream, values, opts);
        if (result) {
            if (Buffer.isBuffer(result) && this.encoding) result.toString(this.encoding)
            return result.toString();
        } else {
            return null;
        }
    }

    async xreadgroup(args: {
        group: string;
        consumer: string;
        blockMs: number;
        count: number;
        streams: { key: string; id: ">" }[]
    }): Promise<XReadGroupResult | null> {
        const {group, consumer, blockMs, count, streams} = args;
        const keysAndIds: Record<string, string> = {};
        for (const stream of streams) {
            keysAndIds[stream.key] = stream.id;
        }

        const client = await this.createdClient;
        const result = await client.xreadgroup(group, consumer, keysAndIds, {
            block: blockMs,
            count,
        });
        console.debug("xreadgroup result", result);

        if (!result) return null;
        const out: XReadGroupResult = [];
        for (const [stream, arr] of Object.entries(result)) {
            const messages: Array<{ id: string; fields: Record<string, string> }> = [];
            if (Array.isArray(arr)) {
                for (const tuple of arr as any[]) {
                    const id: string = String(tuple[0]);
                    const raw = tuple[1];
                    let fields: Record<string, string> = {};


                    if (typeof raw === "string") {
                        // Try to parse JSON-encoded fields
                        try {
                            const obj = JSON.parse(raw);
                            if (obj && typeof obj === "object") {
                                for (const [k, v] of Object.entries(obj as Record<string, any>)) {
                                    fields[k] = String(v);
                                }
                            }
                        } catch (_) {
                            // Fallback: store as single field "payload"
                            fields = {payload: raw};
                        }
                    } else if (Array.isArray(raw)) {
                        // Flat [f1,v1,f2,v2,...]
                        for (let i = 0; i < raw.length; i += 2) {
                            const k = String(raw[i]);
                            fields[k] = String(raw[i + 1] ?? "");
                        }
                    } else if (raw && typeof raw === "object") {
                        // Already an object map
                        for (const [k, v] of Object.entries(raw as Record<string, any>)) {
                            fields[k] = String(v);
                        }
                    } else {
                        // Unknown; preserve as stringy payload
                        fields = {payload: String(raw)};
                    }


                    messages.push({id, fields});
                }
            }
            out.push({stream, messages});
        }
        return out;
    }

    async zadd(key: string, scoreMembers: Array<{ score: number; member: string }>): Promise<number> {
        const client = await this.createdClient;
        const membersAndScores: Record<string, number> = {};
        for (const {score, member} of scoreMembers) {
            membersAndScores[member] = score;
        }
        return await client.zadd(key, membersAndScores)
    }

    async zpopmin(key: string): Promise<Array<{ score: number; member: string }>> {
        const client = await this.createdClient;
        const result = await client.zpopmin(key);
        return result.map(entry => {
            const member = Buffer.isBuffer(entry.element) && this.encoding ?
                entry.element.toString(this.encoding) : entry.element.toString();
            const score = entry.score;
            return {member, score};
        });
    }

    async xgroupCreate(key: string, group: string, id: string, opts: {
        mkstream?: boolean;
        entriesRead?: string
    } | undefined): Promise<string> {
        const client = await this.createdClient;
         return  client.xgroupCreate(key, group, id, opts);
    }

    async xinfoGroups(key: string, options?: {decoder?: Decoder}): Promise<Record<string, number | string | null>[]> {
        const client = await this.createdClient;
        const infoGroups = await client.xinfoGroups(key, options);
        const result: Record<string, number | string | null>[] = [];
        for (const infoGroup of infoGroups) {
            const returnedInfo: Record<string, number | string | null> = {};
            Object.entries(infoGroup).forEach(([infoKey, infoValue]) => {
                if (Buffer.isBuffer(infoValue) && this.encoding) {
                    returnedInfo[infoKey] = infoValue.toString(this.encoding);
                } else if (Buffer.isBuffer(infoValue)) {
                    returnedInfo[infoKey] = infoValue.toString();
                } else if (typeof infoValue === "number" || typeof infoValue === "string" || infoValue === null) {
                    returnedInfo[infoKey] = infoValue
                }
            });
            result.push(returnedInfo);
        }
        return result;
    }

}