import {Decoder, IGlideKitClient, XReadGroupResult} from "./types.js";
import {GlideClient, GlideClientConfiguration, GlideString} from "@valkey/valkey-glide";

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
            return this.convertGlideString(result);
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

        if (!result) return null;
        const out: XReadGroupResult = [];
        result.forEach(({key, value}) => {
            const keyString = this.convertGlideString(key);
            Object.entries(value).forEach(([id, fields]) => {
                const messageFields: Record<string, string> = {};
                fields?.forEach(([fieldName, fieldValue]) => {
                    const fieldNameString = this.convertGlideString(fieldName);
                    messageFields[fieldNameString] = this.convertGlideString(fieldValue);
                });
                out.push({stream: keyString, messages: [{id, fields: messageFields}]});
            })
        });
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
            const member = this.convertGlideString(entry.element);
            const score = entry.score;
            return {member, score};
        });
    }

    async xgroupCreate(key: string, group: string, id: string, opts: {
        mkStream?: boolean;
        entriesRead?: string
    } | undefined): Promise<string> {
        const client = await this.createdClient;
        return client.xgroupCreate(key, group, id, opts);
    }

    async xinfoGroups(key: string, options?: { decoder?: Decoder }): Promise<Record<string, number | string | null>[]> {
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

    private convertGlideString(string: GlideString) {
        return Buffer.isBuffer(string) && this.encoding ? string.toString(this.encoding) : string.toString();
    }

}