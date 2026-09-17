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
