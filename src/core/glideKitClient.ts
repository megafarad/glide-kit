import {ConditionalSet, Decoder, IGlideKitClient, XReadGroupResult} from "./types.js";
import {
    Boundary,
    BaseClient,
    GlideString,
    InfBoundary,
    StreamClaimOptions, GlideReturnType, Script, TimeUnit
} from "@valkey/valkey-glide";

export class GlideKitClient implements IGlideKitClient {

    constructor(private readonly createdClient: Promise<BaseClient>, private encoding?: BufferEncoding) {

    }

    async del(key: string): Promise<number> {
        const client = await this.createdClient;
        return client.del([key]);
    }

    async get(key: string): Promise<string | null> {
        const client = await this.createdClient;
        const result = await client.get(key);
        if (result) {
            return this.convertGlideString(result);
        } else {
            return null;
        }
    }

    async invokeScript(script: Script, options?: {
        keys?: string[];
        args?: string[];
    }): Promise<GlideReturnType> {
        const client = await this.createdClient;
        return await client.invokeScript(script, options);
    }

    async set(key: string, value: string, ttlSec?: number, conditionalSet?: ConditionalSet): Promise<string | null> {
        const client = await this.createdClient;
        const expiry = ttlSec ? {type: TimeUnit.Seconds, count: ttlSec} : undefined;
        const result = await client.set(key, value, {conditionalSet, expiry});
        if (result) {
            return this.convertGlideString(result);
        } else {
            return null;
        }
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

    async xlen(key: string): Promise<number> {
        const client = await this.createdClient;
        return await client.xlen(key);
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

    async zrangebyscore(
        key: string,
        min: number,
        max: number,
        opts?: { limit?: number }
    ): Promise<Array<{ score: number; member: string }>> {
        const client = await this.createdClient;
        const limit = opts?.limit ? {count: opts.limit, offset: 0} : undefined;
        const result = await client.zrangeWithScores(key, {start: min, end: max, limit});
        return result.map(entry => {
            const member = this.convertGlideString(entry.element);
            const score = entry.score;
            return {member, score};
        });
    }

    async zrem(key: string, members: string[]): Promise<number> {
        const client = await this.createdClient;
        return await client.zrem(key, members);
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

    async xpending(
        key: string,
        group: string,
        opts: {
            idle: number;
            count: number;
            start: string;
            end: string;
        }
    ): Promise<Array<{ id: string; consumer: string; idle: number; deliveries: number }>> {
        const client = await this.createdClient;
        const start = this.convertStringToBoundary(opts.start);
        const end = this.convertStringToBoundary(opts.end);
        const result = await client.xpendingWithOptions(key, group, {
            minIdleTime: opts.idle,
            count: opts.count,
            start,
            end
        });
        const out: { id: string; consumer: string, idle: number, deliveries: number }[] = [];
        for (const entry of result) {
            const id = this.convertGlideString(entry[0]);
            const consumer = this.convertGlideString(entry[1]);
            const idle = entry[2];
            const deliveries = entry[3];
            out.push({id, consumer, idle, deliveries});
        }
        return out;
    }

    async xclaim(key: string,
                 group: string,
                 consumer: string,
                 minIdleMs: number,
                 ids: string[],
                 opts?: { retrycount?: number; force?: boolean }): Promise<Array<{
        id: string;
        fields: Record<string, string>
    }>> {
        const client = await this.createdClient;
        const xclaimOpts: StreamClaimOptions | undefined = opts ? {
            retryCount: opts.retrycount,
            isForce: opts.force,
        } : undefined;
        const result = await client.xclaim(key, group, consumer, minIdleMs, ids, xclaimOpts);
        const out: { id: string; fields: Record<string, string> }[] = [];
        for (const entry of Object.entries(result)) {
            const fields: Record<string, string> = {};
            for (const [fieldName, fieldValue] of entry[1]) {
                const fieldNameString = this.convertGlideString(fieldName);
                fields[fieldNameString] = this.convertGlideString(fieldValue);
            }
            out.push({id: entry[0], fields});
        }
        return out;
    }

    private convertGlideString(string: GlideString) {
        return Buffer.isBuffer(string) && this.encoding ? string.toString(this.encoding) : string.toString();
    }

    private convertStringToBoundary(string: string): Boundary<string> {
        switch (string) {
            case "-":
                return InfBoundary.NegativeInfinity
            case "+":
                return InfBoundary.PositiveInfinity
            default:
                return {
                    value: string,
                }
        }
    }

}
