import type { StoredDocument } from '../schema.js'
import type { Connection } from './driver.js'
import { isNotFound, notFound } from './errors.js'

const registry = new Map<string, (document: StoredDocument) => Date | undefined>()

export function registerExpiry(
    table: string,
    expiresAt: (document: StoredDocument) => Date | undefined,
) {
    if (registry.has(table)) {
        throw new Error(`Expiry is already defined on table '${table}'.`)
    }
    registry.set(table, expiresAt)
}

export function nowSecondsOf(context: { now?: () => Date }) {
    if (!context.now) {
        return epochSeconds(new Date())
    }
    const now = context.now()
    if (!Number.isFinite(now.getTime())) {
        throw new TypeError('The clock of the context must return a Date with a finite time.')
    }
    return epochSeconds(now)
}

export function expiryOf(table: string, document: StoredDocument): { expiresAt?: number } {
    const expiresAt = registry.get(table)?.(document)
    if (expiresAt === undefined) {
        return {}
    }
    if (!(expiresAt instanceof Date) || !Number.isFinite(expiresAt.getTime())) {
        throw new TypeError(
            `The expiry of a document in table '${table}' must be a Date with a finite time, or undefined.`,
        )
    }
    return { expiresAt: epochSeconds(expiresAt) }
}

export async function getUnexpired(
    c: Connection,
    table: string,
    partition: string,
    key: string,
    nowSeconds: number,
) {
    const { live } = await getRow(c, table, partition, key, nowSeconds)
    if (!live) {
        throw notFound()
    }
    return live
}

export async function findUnexpired(
    c: Connection,
    table: string,
    partition: string,
    key: string,
    nowSeconds: number,
) {
    const { live } = await getRow(c, table, partition, key, nowSeconds)
    return live
}

// Each distinct ref at most once, in the order of `refs`; a ref whose row is
// missing or expired is left out.
export async function findEachUnexpired(
    c: Connection,
    table: string,
    refs: readonly { partition: string; key: string }[],
    nowSeconds: number,
) {
    const distinct = new Map(refs.map(ref => [refKey(ref), ref])).values().toArray()
    if (distinct.length === 0) {
        return []
    }
    const live = new Map<string, LiveRow>()
    for (const { expiresAt, ...row } of await getManyRaw(c, table, distinct)) {
        if (!isExpired(expiresAt, nowSeconds)) {
            live.set(refKey(row), row)
        }
    }
    return distinct.flatMap(ref => live.get(refKey(ref)) ?? [])
}

type LiveRow = Omit<Awaited<ReturnType<Connection['get']>>, 'expiresAt'>

// Partitions and keys are arbitrary strings, so no delimiter is safe to join
// them with; a JSON pair is.
function refKey(ref: { partition: string; key: string }) {
    return JSON.stringify([ref.partition, ref.key])
}

// Without a batch read, a driver pays one round trip per ref and, on a cold
// connection, one TLS connection per read in flight — an unbounded burst over
// a tenant-sized list has failed a production request outright with
// `getaddrinfo EBUSY`. 16 in flight keeps the burst harmless.
const readsInFlightMax = 16

async function getManyRaw(
    c: Connection,
    table: string,
    refs: readonly { partition: string; key: string }[],
) {
    if (c.getMany) {
        return await c.getMany(table, refs)
    }
    const rows: Awaited<ReturnType<Connection['get']>>[] = []
    for (let start = 0; start < refs.length; start += readsInFlightMax) {
        const chunk = await Promise.all(
            refs.slice(start, start + readsInFlightMax).map(ref => getRaw(c, table, ref)),
        )
        rows.push(...chunk.filter(row => row !== undefined))
    }
    return rows
}

async function getRaw(c: Connection, table: string, ref: { partition: string; key: string }) {
    try {
        return await c.get(table, ref.partition, ref.key)
    } catch (e) {
        if (isNotFound(e)) {
            return undefined
        }
        throw e
    }
}

export async function getRow(
    c: Connection,
    table: string,
    partition: string,
    key: string,
    nowSeconds: number,
) {
    try {
        const { expiresAt, ...row } = await c.get(table, partition, key)
        if (isExpired(expiresAt, nowSeconds)) {
            return { expired: row }
        }
        return { live: row }
    } catch (e) {
        if (isNotFound(e)) {
            return {}
        }
        throw e
    }
}

export async function* unexpired<Row extends { expiresAt?: number }>(
    rows: AsyncIterable<Row>,
    nowSeconds: number,
) {
    for await (const { expiresAt, ...row } of rows) {
        if (!isExpired(expiresAt, nowSeconds)) {
            yield row
        }
    }
}

function isExpired(expiresAt: number | undefined, nowSeconds: number) {
    return expiresAt !== undefined && expiresAt <= nowSeconds
}

function epochSeconds(date: Date) {
    return Math.floor(date.getTime() / 1000)
}
