import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import type { ReadOptions, TransactionItem } from './lib/driver.js'
import { conflict, notFound } from './lib/errors.js'
import { isoOf } from './lib/expiry.js'
import { maxTransactionBytes, maxTransactionItems } from './lib/transaction.js'
import type { KeyRange } from './schema.js'

const documentsEntry = Symbol()

export class MemoryDriver {
    connect(context: object) {
        const ext = context as { [documentsEntry]?: MemoryDocuments }
        return Promise.resolve(
            new MemoryConnection((ext[documentsEntry] ??= new MemoryDocuments())),
        )
    }
}

export class PersistentMemoryDriver {
    readonly #documents = new MemoryDocuments()

    connect() {
        return Promise.resolve(new MemoryConnection(this.#documents))
    }
}

export class DelayedPersistentMemoryDriver {
    readonly #documents = new DelayedDocuments()

    connect() {
        return Promise.resolve(new MemoryConnection(this.#documents))
    }
}

// Eventually consistent reads, deterministically: the first read of a row
// without `consistent: true` after a write to it sees the row as it was before
// that write (present, absent or expired), the way a DynamoDB replica can for
// about a second; every later read, and every consistent read, sees the write.
// A store that reads back what it just wrote and decides on what it sees fails
// here instead of in production.
export class LaggingPersistentMemoryDriver {
    readonly #documents = new LaggingDocuments()

    connect() {
        return Promise.resolve(new MemoryConnection(this.#documents))
    }
}

type Row = {
    json: string
    revision: string
    seq: number
    updatedAt: string
    expiresAt?: number
}

class MemoryConnection {
    readonly #documents: MemoryDocuments | DelayedDocuments | LaggingDocuments
    #closed = false

    constructor(documents: MemoryDocuments | DelayedDocuments | LaggingDocuments) {
        this.#documents = documents
    }

    async add(
        table: string,
        partition: string,
        key: string,
        document: unknown,
        options: { now: number; expiresAt?: number },
    ) {
        this.#throwIfClosed()
        return await this.#documents.add(table, partition, key, document, options)
    }

    // Memory is consistent by construction, so the read options are accepted
    // and ignored.
    async get(table: string, partition: string, key: string, options?: ReadOptions) {
        this.#throwIfClosed()
        return await this.#documents.get(table, partition, key, options)
    }

    async getMany(
        table: string,
        refs: readonly { partition: string; key: string }[],
        options?: ReadOptions,
    ) {
        this.#throwIfClosed()
        return await this.#documents.getMany(table, refs, options)
    }

    async *getPartitions(table: string) {
        this.#throwIfClosed()
        yield* this.#documents.getPartitions(table)
    }

    async *getPartition(table: string, partition: string, range?: KeyRange, options?: ReadOptions) {
        this.#throwIfClosed()
        yield* this.#documents.getPartition(table, partition, range, options)
    }

    async update(
        table: string,
        partition: string,
        key: string,
        currentRevision: unknown,
        document: unknown,
        options: { now: number; expiresAt?: number },
    ) {
        this.#throwIfClosed()
        return await this.#documents.update(
            table,
            partition,
            key,
            currentRevision,
            document,
            options,
        )
    }

    async delete(
        table: string,
        partition: string,
        key: string,
        currentRevision: unknown,
        options: { now: number },
    ) {
        this.#throwIfClosed()
        await this.#documents.delete(table, partition, key, currentRevision, options)
    }

    async transact(items: TransactionItem[], options: { now: number }) {
        this.#throwIfClosed()
        await this.#documents.transact(items, options)
    }

    close() {
        this.#closed = true
        return Promise.resolve()
    }

    #throwIfClosed() {
        if (this.#closed) {
            throw new Error('Connection has been closed.')
        }
    }
}

class MemoryDocuments {
    readonly #tables = new MapWithDefault(
        () => new MapWithDefault<string, Map<string, Row>>(() => new Map<string, Row>()),
    )

    add(
        table: string,
        partition: string,
        key: string,
        document: unknown,
        options: { now: number; expiresAt?: number },
    ) {
        const p = this.#tables.get(table).get(partition)
        const existing = p.get(key)
        if (isLive(existing, options.now)) {
            throw conflict()
        }
        const row = storedRow(
            randomUUID(),
            document,
            options.expiresAt,
            nextSeq(existing),
            isoOf(options.now),
        )
        p.set(key, row)
        return written(row)
    }

    get(table: string, partition: string, key: string, _options?: ReadOptions) {
        const row = this.#tables.get(table).get(partition).get(key)
        if (!row) {
            throw notFound()
        }
        return { partition, ...readRow(key, row) }
    }

    // Reversed, so that a store relying on the order a batch read happens to
    // return rows in fails here rather than against a driver whose order
    // varies from call to call.
    getMany(
        table: string,
        refs: readonly { partition: string; key: string }[],
        _options?: ReadOptions,
    ) {
        const found = []
        for (const { partition, key } of refs.toReversed()) {
            const row = this.#tables.get(table).get(partition).get(key)
            if (row) {
                found.push({ partition, ...readRow(key, row) })
            }
        }
        return found
    }

    // The stored row as it is, expired or not, for a driver layered on top.
    peek(table: string, partition: string, key: string) {
        return this.#tables.get(table).get(partition).get(key)
    }

    async *getPartitions(table: string) {
        for (const [partition, rows] of this.#tables.get(table).entries()) {
            await Promise.resolve()
            if (rows.size !== 0) {
                yield partition
            }
        }
    }

    async *getPartition(
        table: string,
        partition: string,
        range?: KeyRange,
        _options?: ReadOptions,
    ) {
        const matches = matchRange(range)
        for (const [key, row] of sortedByKey(this.#tables.get(table).get(partition))) {
            await Promise.resolve()
            if (matches(key)) {
                yield readRow(key, row)
            }
        }
    }

    update(
        table: string,
        partition: string,
        key: string,
        currentRevision: unknown,
        document: unknown,
        options: { now: number; expiresAt?: number },
    ) {
        const p = this.#tables.get(table).get(partition)
        const existing = p.get(key)
        if (!hasRevision(existing, currentRevision, options.now)) {
            throw conflict()
        }
        const row = storedRow(
            randomUUID(),
            document,
            options.expiresAt,
            existing.seq + 1,
            isoOf(options.now),
        )
        p.set(key, row)
        return written(row)
    }

    delete(
        table: string,
        partition: string,
        key: string,
        currentRevision: unknown,
        options: { now: number },
    ) {
        const p = this.#tables.get(table).get(partition)
        if (!hasRevision(p.get(key), currentRevision, options.now)) {
            throw conflict()
        }
        p.delete(key)
    }

    transact(items: TransactionItem[], options: { now: number }) {
        throwIfAnyDocumentRepeats(items)
        throwIfOverLimits(items)
        const updatedAt = isoOf(options.now)
        const applies = items.map(item => {
            const p = this.#tables.get(item.table).get(item.partition)
            const existing = p.get(item.key)
            switch (item.op) {
                case 'add':
                    if (isLive(existing, options.now)) {
                        throw conflict()
                    }
                    return () => {
                        p.set(
                            item.key,
                            storedRow(
                                item.newRevision,
                                item.document,
                                item.expiresAt,
                                nextSeq(existing),
                                updatedAt,
                            ),
                        )
                    }
                case 'update':
                    if (!hasRevision(existing, item.revision, options.now)) {
                        throw conflict()
                    }
                    return () => {
                        p.set(
                            item.key,
                            storedRow(
                                item.newRevision,
                                item.document,
                                item.expiresAt,
                                existing.seq + 1,
                                updatedAt,
                            ),
                        )
                    }
                case 'delete':
                    if (!hasRevision(existing, item.revision, options.now)) {
                        throw conflict()
                    }
                    return () => {
                        p.delete(item.key)
                    }
                case 'check':
                    if (!hasRevision(existing, item.revision, options.now)) {
                        throw conflict()
                    }
                    return () => undefined
                case 'put':
                    return () => {
                        p.set(
                            item.key,
                            storedRow(
                                item.newRevision,
                                item.document,
                                item.expiresAt,
                                nextSeq(existing),
                                updatedAt,
                            ),
                        )
                    }
                case 'clear':
                    return () => {
                        p.delete(item.key)
                    }
            }
        })
        for (const apply of applies) {
            apply()
        }
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
        return this.#inner.add(table, partition, key, document, options)
    }

    async get(table: string, partition: string, key: string, options?: ReadOptions) {
        await using _ = await delayed()
        return this.#inner.get(table, partition, key, options)
    }

    async getMany(
        table: string,
        refs: readonly { partition: string; key: string }[],
        options?: ReadOptions,
    ) {
        await using _ = await delayed()
        return this.#inner.getMany(table, refs, options)
    }

    async *getPartitions(table: string) {
        await using _ = await delayed()
        for await (const partition of this.#inner.getPartitions(table)) {
            yield partition
        }
    }

    async *getPartition(table: string, partition: string, range?: KeyRange, options?: ReadOptions) {
        await using _ = await delayed()
        for await (const row of this.#inner.getPartition(table, partition, range, options)) {
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
        return this.#inner.update(table, partition, key, currentRevision, document, options)
    }

    async delete(
        table: string,
        partition: string,
        key: string,
        currentRevision: unknown,
        options: { now: number },
    ) {
        await using _ = await delayed()
        this.#inner.delete(table, partition, key, currentRevision, options)
    }

    async transact(items: TransactionItem[], options: { now: number }) {
        await using _ = await delayed()
        this.#inner.transact(items, options)
    }
}

// A stale row is the version before the first write since the replica last
// caught up, `absent` when the row did not exist. One eventual read of the
// row catches the replica up; a consistent read never touches it.
const absent = Symbol('absent')
type Stale = Row | typeof absent

class LaggingDocuments {
    readonly #inner = new MemoryDocuments()
    readonly #stale = new MapWithDefault(
        () => new MapWithDefault<string, Map<string, Stale>>(() => new Map<string, Stale>()),
    )

    add(
        table: string,
        partition: string,
        key: string,
        document: unknown,
        options: { now: number; expiresAt?: number },
    ) {
        this.#remember(table, partition, key)
        return this.#inner.add(table, partition, key, document, options)
    }

    get(table: string, partition: string, key: string, options?: ReadOptions) {
        if (options?.consistent) {
            return this.#inner.get(table, partition, key)
        }
        const stale = this.#catchUp(table, partition, key)
        if (stale === undefined) {
            return this.#inner.get(table, partition, key)
        }
        if (stale === absent) {
            throw notFound()
        }
        return { partition, ...readRow(key, stale) }
    }

    getMany(
        table: string,
        refs: readonly { partition: string; key: string }[],
        options?: ReadOptions,
    ) {
        if (options?.consistent) {
            return this.#inner.getMany(table, refs)
        }
        const found = []
        for (const { partition, key } of refs.toReversed()) {
            const row =
                this.#catchUp(table, partition, key) ?? this.#inner.peek(table, partition, key)
            if (row !== undefined && row !== absent) {
                found.push({ partition, ...readRow(key, row) })
            }
        }
        return found
    }

    getPartitions(table: string) {
        return this.#inner.getPartitions(table)
    }

    async *getPartition(table: string, partition: string, range?: KeyRange, options?: ReadOptions) {
        if (options?.consistent) {
            yield* this.#inner.getPartition(table, partition, range)
            return
        }
        const matches = matchRange(range)
        const view = new Map<string, Row>()
        for await (const row of this.#inner.getPartition(table, partition, range)) {
            const { key, ...rest } = row
            view.set(
                key,
                storedRow(rest.revision, rest.document, rest.expiresAt, rest.seq, rest.updatedAt),
            )
        }
        for (const [key, stale] of this.#stale.get(table).get(partition)) {
            if (!matches(key)) {
                continue
            }
            this.#catchUp(table, partition, key)
            if (stale === absent) {
                view.delete(key)
            } else {
                view.set(key, stale)
            }
        }
        for (const [key, row] of sortedByKey(view)) {
            await Promise.resolve()
            yield readRow(key, row)
        }
    }

    update(
        table: string,
        partition: string,
        key: string,
        currentRevision: unknown,
        document: unknown,
        options: { now: number; expiresAt?: number },
    ) {
        this.#remember(table, partition, key)
        return this.#inner.update(table, partition, key, currentRevision, document, options)
    }

    delete(
        table: string,
        partition: string,
        key: string,
        currentRevision: unknown,
        options: { now: number },
    ) {
        this.#remember(table, partition, key)
        this.#inner.delete(table, partition, key, currentRevision, options)
    }

    transact(items: TransactionItem[], options: { now: number }) {
        for (const item of items) {
            this.#remember(item.table, item.partition, item.key)
        }
        this.#inner.transact(items, options)
    }

    // A write that fails leaves the row as it was, and the remembered version
    // is what it was, so remembering before the write is right either way.
    #remember(table: string, partition: string, key: string) {
        const stale = this.#stale.get(table).get(partition)
        if (!stale.has(key)) {
            stale.set(key, this.#inner.peek(table, partition, key) ?? absent)
        }
    }

    #catchUp(table: string, partition: string, key: string) {
        const stale = this.#stale.get(table).get(partition)
        const version = stale.get(key)
        stale.delete(key)
        return version
    }
}

function storedRow(
    revision: unknown,
    document: unknown,
    expiresAt: number | undefined,
    seq: number,
    updatedAt: string,
): Row {
    return {
        revision: revision as string,
        json: JSON.stringify(document),
        seq,
        updatedAt,
        ...(expiresAt !== undefined && { expiresAt }),
    }
}

// The next write's `seq`: one past the row under the key, live or expired; 0
// when there is none, a deleted row included.
function nextSeq(existing: Row | undefined) {
    return existing === undefined ? 0 : existing.seq + 1
}

function written(row: Row) {
    return { revision: row.revision, seq: row.seq, updatedAt: row.updatedAt }
}

function readRow(key: string, row: Row) {
    return {
        key,
        revision: row.revision,
        document: JSON.parse(row.json) as unknown,
        seq: row.seq,
        updatedAt: row.updatedAt,
        ...(row.expiresAt !== undefined && { expiresAt: row.expiresAt }),
    }
}

function isLive(row: Row | undefined, now: number): row is Row {
    return row !== undefined && (row.expiresAt === undefined || now < row.expiresAt)
}

function hasRevision(row: Row | undefined, revision: unknown, now: number): row is Row {
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

// DynamoDB refuses a transaction of more than 100 items or 4 MB, on every
// retry alike. The items arrive here after index maintenance and auditing have
// added theirs, so this is where a test sees the transaction production sends.
function throwIfOverLimits(items: TransactionItem[]) {
    if (maxTransactionItems < items.length) {
        throw new Error(
            `Transaction contains ${String(items.length)} operations on ${tablesOf(items)}; at most ${String(maxTransactionItems)} are allowed.`,
        )
    }
    const bytes = items.reduce((sum, item) => sum + itemBytes(item), 0)
    if (maxTransactionBytes < bytes) {
        throw new Error(
            `Transaction of ${String(items.length)} operations on ${tablesOf(items)} is about ${String(bytes)} bytes; at most ${String(maxTransactionBytes)} are allowed.`,
        )
    }
}

// DynamoDB measures the request, not the items it stores: 14 documents of
// 300 KB sent through the DynamoDB driver measured 393 bytes each beyond the
// document's JSON string, for the table name, key, revision, timestamps and
// the update expressions around them. The allowance stays above that, so
// the estimate errs high. An operation without a document sends only its
// table and key.
function itemBytes(item: TransactionItem) {
    const keyBytes =
        Buffer.byteLength(item.table) +
        Buffer.byteLength(item.partition) +
        Buffer.byteLength(item.key)
    if ('document' in item) {
        return keyBytes + Buffer.byteLength(JSON.stringify(item.document)) + envelopeBytes
    }
    return keyBytes
}

const envelopeBytes = 512

function tablesOf(items: TransactionItem[]) {
    return [...new Set(items.map(item => `'${item.table}'`))].join(', ')
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
        if (after !== undefined) {
            if (before !== undefined) {
                return (key: string) => after <= key && key < before
            }
            return (key: string) => after <= key
        }
        if (before !== undefined) {
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
