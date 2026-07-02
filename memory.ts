import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import type { TransactionItem } from './lib/driver.js'
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
}

class MemoryDocuments {
    readonly #tables = new MapWithDefault(
        () => new MapWithDefault<string, Map<string, Row>>(() => new Map<string, Row>()),
    )
    #closed = false

    async add(table: string, partition: string, key: string, document: unknown) {
        await this.#throwIfClosed()
        const revision = randomUUID()
        const p = this.#tables.get(table).get(partition)
        if (p.get(key)) {
            throw conflict()
        }
        p.set(key, { revision, json: JSON.stringify(document) })
        return revision
    }

    async get(table: string, partition: string, key: string) {
        await this.#throwIfClosed()
        const row = this.#tables.get(table).get(partition).get(key)
        if (!row) {
            throw notFound()
        }
        return {
            partition,
            key,
            revision: row.revision,
            document: JSON.parse(row.json) as unknown,
        }
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
        for (const [key, row] of this.#tables.get(table).get(partition)) {
            await Promise.resolve()
            if (matches(key)) {
                yield {
                    key,
                    revision: row.revision,
                    document: JSON.parse(row.json) as unknown,
                }
            }
        }
    }

    async update(
        table: string,
        partition: string,
        key: string,
        currentRevision: unknown,
        document: unknown,
    ) {
        await this.#throwIfClosed()
        const p = this.#tables.get(table).get(partition)
        const r = p.get(key)
        if (!r) {
            throw conflict()
        }
        if (r.revision !== currentRevision) {
            throw conflict()
        }
        const revision = randomUUID()
        p.set(key, { revision, json: JSON.stringify(document) })
        return revision
    }

    async delete(table: string, partition: string, key: string, currentRevision: unknown) {
        await this.#throwIfClosed()
        const p = this.#tables.get(table).get(partition)
        const r = p.get(key)
        if (!r) {
            throw conflict()
        }
        if (r.revision !== currentRevision) {
            throw conflict()
        }
        p.delete(key)
    }

    async transact(items: TransactionItem[]) {
        await this.#throwIfClosed()
        const applies = items.map(item => {
            const p = this.#tables.get(item.table).get(item.partition)
            const existing = p.get(item.key)
            switch (item.op) {
                case 'add':
                    if (existing) {
                        throw conflict()
                    }
                    return () =>
                        p.set(item.key, {
                            revision: item.newRevision as string,
                            json: JSON.stringify(item.document),
                        })
                case 'update':
                    if (!existing || existing.revision !== item.revision) {
                        throw conflict()
                    }
                    return () =>
                        p.set(item.key, {
                            revision: item.newRevision as string,
                            json: JSON.stringify(item.document),
                        })
                case 'delete':
                    if (!existing || existing.revision !== item.revision) {
                        throw conflict()
                    }
                    return () => p.delete(item.key)
                case 'check':
                    if (!existing || existing.revision !== item.revision) {
                        throw conflict()
                    }
                    return () => undefined
            }
        })
        for (const apply of applies) {
            apply()
        }
    }

    #throwIfClosed() {
        if (this.#closed) {
            return Promise.reject(new Error('Connection has been closed.'))
        }
        return Promise.resolve()
    }

    close() {
        this.#closed = true
        return Promise.resolve()
    }
}

class DelayedDocuments {
    readonly #inner = new MemoryDocuments()

    async add(table: string, partition: string, key: string, document: unknown) {
        await using _ = await delayed()
        return await this.#inner.add(table, partition, key, document)
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
    ) {
        await using _ = await delayed()
        return await this.#inner.update(table, partition, key, currentRevision, document)
    }

    async delete(table: string, partition: string, key: string, currentRevision: unknown) {
        await using _ = await delayed()
        await this.#inner.delete(table, partition, key, currentRevision)
    }

    async transact(items: TransactionItem[]) {
        await using _ = await delayed()
        await this.#inner.transact(items)
    }

    async close() {
        await using _ = await delayed()
        await this.#inner.close()
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
            } else {
                return (key: string) => after <= key
            }
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

function conflict() {
    const e = new Error('Conflict')
    ;(e as unknown as { status: number }).status = 409
    return e
}

function notFound() {
    const e = new Error('Not found')
    ;(e as unknown as { status: number }).status = 404
    return e
}
