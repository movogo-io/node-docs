import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import type { TransactionItem } from './lib/driver.js'
import { conflict, notFound } from './lib/errors.js'
import type { KeyRange } from './schema.js'

const documentsEntry = Symbol()

export class MemoryDriver {
    connect(context: object) {
        const ext = context as { [documentsEntry]?: MemoryDocuments }
        return Promise.resolve((ext[documentsEntry] ??= new MemoryDocuments()))
    }
}

export class PersistentMemoryDriver {
    readonly #documents = new MemoryDocuments()

    connect() {
        return Promise.resolve(this.#documents)
    }
}

export class DelayedPersistentMemoryDriver {
    readonly #documents = new DelayedDocuments()

    connect() {
        return Promise.resolve(this.#documents)
    }
}

type Row = {
    json: string
    revision: string
    expiresAt?: number
}

class MemoryDocuments {
    readonly #tables = new MapWithDefault(
        () => new MapWithDefault<string, Map<string, Row>>(() => new Map<string, Row>()),
    )
    #closed = false

    async add(
        table: string,
        partition: string,
        key: string,
        document: unknown,
        options: { now: number; expiresAt?: number },
    ) {
        await this.#throwIfClosed()
        const revision = randomUUID()
        const p = this.#tables.get(table).get(partition)
        if (isLive(p.get(key), options.now)) {
            throw conflict()
        }
        p.set(key, storedRow(revision, document, options.expiresAt))
        return revision
    }

    async get(table: string, partition: string, key: string) {
        await this.#throwIfClosed()
        const row = this.#tables.get(table).get(partition).get(key)
        if (!row) {
            throw notFound()
        }
        return { partition, ...readRow(key, row) }
    }

    async *getPartitions(table: string) {
        await this.#throwIfClosed()
        for (const [partition, rows] of this.#tables.get(table).entries()) {
            await Promise.resolve()
            if (rows.size !== 0) {
                yield partition
            }
        }
    }

    async *getPartition(table: string, partition: string, range?: KeyRange) {
        await this.#throwIfClosed()
        const matches = matchRange(range)
        for (const [key, row] of sortedByKey(this.#tables.get(table).get(partition))) {
            await Promise.resolve()
            if (matches(key)) {
                yield readRow(key, row)
            }
        }
    }

    async update(
        table: string,
        partition: string,
        key: string,
        currentRevision: unknown,
        document: unknown,
        options: { now: number; expiresAt?: number },
    ) {
        await this.#throwIfClosed()
        const p = this.#tables.get(table).get(partition)
        if (!hasRevision(p.get(key), currentRevision, options.now)) {
            throw conflict()
        }
        const revision = randomUUID()
        p.set(key, storedRow(revision, document, options.expiresAt))
        return revision
    }

    async delete(
        table: string,
        partition: string,
        key: string,
        currentRevision: unknown,
        options: { now: number },
    ) {
        await this.#throwIfClosed()
        const p = this.#tables.get(table).get(partition)
        if (!hasRevision(p.get(key), currentRevision, options.now)) {
            throw conflict()
        }
        p.delete(key)
    }

    async transact(items: TransactionItem[], options: { now: number }) {
        await this.#throwIfClosed()
        throwIfAnyDocumentRepeats(items)
        const applies = items.map(item => {
            const p = this.#tables.get(item.table).get(item.partition)
            const existing = p.get(item.key)
            switch (item.op) {
                case 'add':
                    if (isLive(existing, options.now)) {
                        throw conflict()
                    }
                    return () =>
                        p.set(item.key, storedRow(item.newRevision, item.document, item.expiresAt))
                case 'update':
                    if (!hasRevision(existing, item.revision, options.now)) {
                        throw conflict()
                    }
                    return () =>
                        p.set(item.key, storedRow(item.newRevision, item.document, item.expiresAt))
                case 'delete':
                    if (!hasRevision(existing, item.revision, options.now)) {
                        throw conflict()
                    }
                    return () => p.delete(item.key)
                case 'check':
                    if (!hasRevision(existing, item.revision, options.now)) {
                        throw conflict()
                    }
                    return () => undefined
                case 'put':
                    return () =>
                        p.set(item.key, storedRow(item.newRevision, item.document, item.expiresAt))
                case 'clear':
                    return () => p.delete(item.key)
            }
        })
        for (const apply of applies) {
            apply()
        }
    }

    close() {
        this.#closed = true
        return Promise.resolve()
    }

    #throwIfClosed() {
        if (this.#closed) {
            return Promise.reject(new Error('Connection has been closed.'))
        }
        return Promise.resolve()
    }
}

class DelayedDocuments {
    readonly #inner = new MemoryDocuments()

    async add(
        table: string,
        partition: string,
        key: string,
        document: unknown,
        options: { now: number; expiresAt?: number },
    ) {
        await using _ = await delayed()
        return await this.#inner.add(table, partition, key, document, options)
    }

    async get(table: string, partition: string, key: string) {
        await using _ = await delayed()
        return await this.#inner.get(table, partition, key)
    }

    async *getPartitions(table: string) {
        await using _ = await delayed()
        for await (const partition of this.#inner.getPartitions(table)) {
            yield partition
        }
    }

    async *getPartition(table: string, partition: string, range?: KeyRange) {
        await using _ = await delayed()
        for await (const row of this.#inner.getPartition(table, partition, range)) {
            yield row
        }
    }

    async update(
        table: string,
        partition: string,
        key: string,
        currentRevision: unknown,
        document: unknown,
        options: { now: number; expiresAt?: number },
    ) {
        await using _ = await delayed()
        return await this.#inner.update(table, partition, key, currentRevision, document, options)
    }

    async delete(
        table: string,
        partition: string,
        key: string,
        currentRevision: unknown,
        options: { now: number },
    ) {
        await using _ = await delayed()
        await this.#inner.delete(table, partition, key, currentRevision, options)
    }

    async transact(items: TransactionItem[], options: { now: number }) {
        await using _ = await delayed()
        await this.#inner.transact(items, options)
    }

    async close() {
        await using _ = await delayed()
        await this.#inner.close()
    }
}

function storedRow(revision: unknown, document: unknown, expiresAt: number | undefined): Row {
    return {
        revision: revision as string,
        json: JSON.stringify(document),
        ...(expiresAt !== undefined && { expiresAt }),
    }
}

function readRow(key: string, row: Row) {
    return {
        key,
        revision: row.revision,
        document: JSON.parse(row.json) as unknown,
        ...(row.expiresAt !== undefined && { expiresAt: row.expiresAt }),
    }
}

function isLive(row: Row | undefined, now: number): row is Row {
    return row !== undefined && (row.expiresAt === undefined || now < row.expiresAt)
}

function hasRevision(row: Row | undefined, revision: unknown, now: number) {
    return isLive(row, now) && row.revision === revision
}

function sortedByKey(rows: Map<string, Row>) {
    return [...rows].sort(([a], [b]) => {
        if (a < b) {
            return -1
        }
        if (b < a) {
            return 1
        }
        return 0
    })
}

function throwIfAnyDocumentRepeats(items: TransactionItem[]) {
    const touched = new Set<string>()
    for (const item of items) {
        const id = JSON.stringify([item.table, item.partition, item.key])
        if (touched.has(id)) {
            throw new Error(
                `Transaction contains more than one operation on '${item.key}' in partition '${item.partition}' of table '${item.table}'.`,
            )
        }
        touched.add(id)
    }
}

async function delayed() {
    await setTimeout(Math.random())
    return {
        [Symbol.asyncDispose]: () => setTimeout(Math.random()),
    }
}

function matchRange(range?: KeyRange) {
    if (!range) {
        return () => true
    }
    if ('withPrefix' in range) {
        return (key: string) => key.startsWith(range.withPrefix)
    }
    if ('before' in range || 'after' in range) {
        const { after, before } = range
        if (after) {
            if (before) {
                return (key: string) => after <= key && key < before
            }
            return (key: string) => after <= key
        }
        if (before) {
            return (key: string) => key < before
        }
    }
    return alwaysFalse
}

function alwaysFalse() {
    return false
}

class MapWithDefault<K, V> {
    readonly #map: Map<K, V>
    readonly #default: () => V

    constructor(d: () => V) {
        this.#map = new Map()
        this.#default = d
    }

    get(key: K) {
        const existing = this.#map.get(key)
        if (existing) {
            return existing
        }
        const d = this.#default()
        this.#map.set(key, d)
        return d
    }

    entries() {
        return this.#map.entries()
    }
}
