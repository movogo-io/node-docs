import assert from 'node:assert/strict'
import { setTimeout } from 'node:timers/promises'
import { getDriver, type Connection } from './lib/driver.js'
import type { KeyRange, Revision, StoredDocument } from './schema.js'

type Context = {
    on?: (event: 'free', handler: () => Promise<void>) => boolean
}

type TableNamesOf<Schema> = keyof Schema & string
type PartitionKeyOf<Schema, Table extends TableNamesOf<Schema>> = keyof Schema[Table] & string
type KeyOf<
    Schema,
    Table extends TableNamesOf<Schema> = TableNamesOf<Schema>,
> = keyof Schema[Table][PartitionKeyOf<Schema, Table>] & string
type DocumentOfFixedPartition<
    Schema,
    Table extends TableNamesOf<Schema>,
    P extends PartitionKeyOf<Schema, Table>,
> = Schema[Table][P][KeyOf<Schema, Table>]

type DocumentOf<
    Schema,
    Table extends TableNamesOf<Schema> = TableNamesOf<Schema>,
> = Schema[Table][PartitionKeyOf<Schema, Table>][KeyOf<Schema, Table>]

type DocumentOfFixedKey<
    Schema,
    Table extends TableNamesOf<Schema>,
    K extends KeyOf<Schema, Table>,
> = Schema[Table][PartitionKeyOf<Schema, Table>][K]

type Tables<Schema> = string extends TableNamesOf<Schema> ? never : NamedTables<Schema>

type NamedTables<Schema> = {
    readonly [P in TableNamesOf<Schema>]: Documents<Schema, P>
}

type Documents<Schema, Table extends TableNamesOf<Schema>> =
    string extends PartitionKeyOf<Schema, Table>
        ? string extends KeyOf<Schema, Table>
            ? Partitions<Schema, Table>
            : PartitionsWithFixedKey<Schema, Table>
        : NamedPartitions<Schema, Table>

type PartitionsWithFixedKey<Schema, Table extends TableNamesOf<Schema>> = {
    withKey<K extends KeyOf<Schema, Table>>(key: K): FixedKey<DocumentOfFixedKey<Schema, Table, K>>
    getPartitions(): AsyncIterable<string>
}

type NamedPartitions<Schema, Table extends TableNamesOf<Schema>> = {
    readonly [P in PartitionKeyOf<Schema, Table>]: NamedPartition<
        DocumentOfFixedPartition<Schema, Table, P>
    >
} & {
    getPartitions(): AsyncIterable<string>
}

type Partitions<Schema, Table extends TableNamesOf<Schema>> = {
    partition(partition: string): NamedPartition<DocumentOf<Schema, Table>>
    getPartitions(): AsyncIterable<string>
}

type FixedKey<Document> = {
    add: (partition: string, document: Document) => Promise<Revision>
    get: (
        partition: string,
    ) => Promise<{ partition: string; revision: Revision; document: Document }>
    getDocument: (partition: string) => Promise<Document>
    update: (partition: string, revision: Revision, document: Document) => Promise<Revision>
    updateRow: (row: {
        partition: string
        revision: Revision
        document: Document
    }) => Promise<Revision>
    getOrAddComputed: (
        partition: string,
        computed: () => Promise<Document> | Document,
        options?: RetryOptions,
    ) => Promise<{ partition: string; revision: Revision; document: Document }>
    addOrUpdate: (
        partition: string,
        document: Document,
        update: (existing: Document) => void,
        options?: RetryOptions,
    ) => Promise<{
        action: 'add' | 'update'
        partition: Revision
        key: string
        revision: Revision
        document: Document
    }>
    addOrUpdateComputed: (
        partition: string,
        computed: () => Promise<Document> | Document,
        update: (existing: Document) => void,
        options?: RetryOptions,
    ) => Promise<{
        action: 'add' | 'update'
        partition: Revision
        key: string
        revision: Revision
        document: Document
    }>
    converge: (
        partition: string,
        target: (document: Document) => boolean,
        initial: Document,
        update: (existing: Document) => void,
        options?: RetryOptions,
    ) => Promise<{
        partition: Revision
        key: string
        revision: Revision
        document: Document
    }>
    delete: (partition: string, revision: Revision) => Promise<void>
}

type NamedPartition<Document> = {
    add: (key: string, document: Document) => Promise<Revision>
    get: (key: string) => Promise<{ key: string; revision: Revision; document: Document }>
    getDocument: (key: string) => Promise<Document>
    getAll: () => AsyncIterable<{ key: string; revision: Revision; document: Document }>
    getRange: (
        range: KeyRange,
    ) => AsyncIterable<{ key: string; revision: Revision; document: Document }>
    update: (key: string, revision: Revision, document: Document) => Promise<Revision>
    updateRow: (row: { key: string; revision: Revision; document: Document }) => Promise<Revision>
    getOrAdd: (
        key: string,
        document: Document,
        options?: RetryOptions,
    ) => Promise<{ key: string; revision: Revision; document: Document }>
    getOrAddComputed: (
        key: string,
        computed: () => Promise<Document> | Document,
        options?: RetryOptions,
    ) => Promise<{ key: string; revision: Revision; document: Document }>
    addOrUpdate: (
        key: string,
        document: Document,
        update: (existing: Document) => void,
        options?: RetryOptions,
    ) => Promise<{
        action: 'add' | 'update'
        partition: Revision
        key: string
        revision: Revision
        document: Document
    }>
    addOrUpdateComputed: (
        key: string,
        computed: () => Promise<Document> | Document,
        update: (existing: Document) => void,
        options?: RetryOptions,
    ) => Promise<{
        action: 'add' | 'update'
        partition: Revision
        key: string
        revision: Revision
        document: Document
    }>
    converge: (
        key: string,
        target: (document: Document) => boolean,
        initial: Document,
        update: (existing: Document) => void,
        options?: RetryOptions,
    ) => Promise<{
        partition: Revision
        key: string
        revision: Revision
        document: Document
    }>
    delete: (key: string, revision: Revision) => Promise<void>
}

type GenericSchema = {
    [table: string]: {
        [partition: string]: {
            [key: string]: StoredDocument
        }
    }
}

export function tables<Schema = GenericSchema>(
    context: Context & { on?: undefined },
): Tables<Schema> & AsyncDisposable
export function tables<Schema = GenericSchema>(
    context: Context & { on: (event: 'free', handler: () => Promise<void>) => void },
): Tables<Schema>

export function tables<Schema = GenericSchema>(context: Context) {
    const d = getDriver()
    const connection = d.connect(context)
    const closer = async () => {
        const c = await connection
        await c.close()
    }
    const p = new Proxy(tablesBase(connection), tablesProxy) as unknown as Tables<Schema>
    if (!context.on?.('free', closer)) {
        const dp = p as Tables<Schema> & AsyncDisposable
        dp[Symbol.asyncDispose] = closer
    }
    return p
}

const connectionEntry = Symbol()
const tableNameEntry = Symbol()

function tablesBase(connection: Promise<Connection>) {
    return {
        [connectionEntry]: connection,
    }
}

type GenericProxyTarget = { [k: string | symbol]: unknown }

const tablesProxy = {
    get: (
        target: GenericProxyTarget & ReturnType<typeof tablesBase>,
        property: string | symbol,
    ) => {
        if (property in target) {
            return target[property]
        }
        if (typeof property === 'symbol') {
            return undefined
        }
        return new Proxy(tableBase(target, property), tableProxy)
    },
}

function tableBase(db: ReturnType<typeof tablesBase>, table: string) {
    return {
        [connectionEntry]: db[connectionEntry],
        [tableNameEntry]: table,
        withKey: (key: string) => ({
            async add(partition: string, document: unknown) {
                const c = await db[connectionEntry]
                return c.add(table, partition, key, document)
            },
            async get(partition: string) {
                const c = await db[connectionEntry]
                return await c.get(table, partition, key)
            },
            async getDocument(partition: string) {
                const c = await db[connectionEntry]
                const r = await c.get(table, partition, key)
                return r.document
            },
            async update(partition: string, revision: Revision, document: StoredDocument) {
                const c = await db[connectionEntry]
                return await c.update(table, partition, key, revision, document)
            },
            async updateRow(row: {
                partition: string
                revision: Revision
                document: StoredDocument
            }) {
                const c = await db[connectionEntry]
                return await c.update(table, row.partition, key, row.revision, row.document)
            },
            async getOrAdd(partition: string, document: unknown, options?: RetryOptions) {
                const c = await db[connectionEntry]
                return await getOrAdd(c, table, partition, key, document, options)
            },
            async getOrAddComputed<T>(
                partition: string,
                computed: () => Promise<T> | T,
                options?: RetryOptions,
            ) {
                const c = await db[connectionEntry]
                return await getOrAddComputed(c, table, partition, key, computed, options)
            },
            async addOrUpdate(
                partition: string,
                document: unknown,
                update: (existing: unknown) => void,
                options?: RetryOptions,
            ) {
                const c = await db[connectionEntry]
                return await addOrUpdate(c, table, partition, key, document, update, options)
            },
            async addOrUpdateComputed<T>(
                partition: string,
                computed: () => Promise<T> | T,
                update: (existing: T) => void,
                options?: RetryOptions,
            ) {
                const c = await db[connectionEntry]
                return await addOrUpdateComputed(
                    c,
                    table,
                    partition,
                    key,
                    computed,
                    update,
                    options,
                )
            },
            async converge<T>(
                partition: string,
                target: (document: T) => boolean,
                initial: T,
                update: (existing: T) => void,
                options?: RetryOptions,
            ) {
                const c = await db[connectionEntry]
                return await converge(c, table, partition, key, target, initial, update, options)
            },
            async convergeComputed<T>(
                partition: string,
                target: (document: T) => boolean,
                computed: () => Promise<T> | T,
                update: (existing: T) => void,
                options?: RetryOptions,
            ) {
                const c = await db[connectionEntry]
                return await convergeComputed(
                    c,
                    table,
                    partition,
                    key,
                    target,
                    computed,
                    update,
                    options,
                )
            },
            async delete(partition: string, revision: Revision) {
                const c = await db[connectionEntry]
                await c.delete(table, partition, key, revision)
            },
        }),
        partition: (partition: string) => new Partition(db[connectionEntry], table, partition),
        async *getPartitions() {
            const c = await db[connectionEntry]
            for await (const partition of c.getPartitions(table)) {
                yield partition
            }
        },
    }
}

const tableProxy = {
    get: (target: GenericProxyTarget & ReturnType<typeof tableBase>, property: string | symbol) => {
        if (property in target) {
            return target[property]
        }
        if (typeof property === 'symbol') {
            return undefined
        }
        return new Partition(target[connectionEntry], target[tableNameEntry], property)
    },
}

class Partition {
    readonly #connection
    readonly #table
    readonly #partition

    constructor(connection: Promise<Connection>, table: string, partition: string) {
        this.#connection = connection
        this.#table = table
        this.#partition = partition
    }

    async add(key: string, document: StoredDocument) {
        const c = await this.#connection
        return c.add(this.#table, this.#partition, key, document)
    }
    async get(key: string) {
        const c = await this.#connection
        return c.get(this.#table, this.#partition, key)
    }
    async getDocument(key: string) {
        const r = await this.get(key)
        return r.document
    }
    async *getAll() {
        const c = await this.#connection
        for await (const r of c.getPartition(this.#table, this.#partition)) {
            yield r
        }
    }
    async *getRange(range: KeyRange) {
        const c = await this.#connection
        for await (const r of c.getPartition(this.#table, this.#partition, range)) {
            yield r
        }
    }
    async update(key: string, revision: Revision, document: StoredDocument) {
        const c = await this.#connection
        return c.update(this.#table, this.#partition, key, revision, document)
    }
    async updateRow(row: { key: string; revision: Revision; document: StoredDocument }) {
        const c = await this.#connection
        return c.update(this.#table, this.#partition, row.key, row.revision, row.document)
    }
    async getOrAdd(key: string, document: unknown, options?: RetryOptions) {
        const c = await this.#connection
        return await getOrAdd(c, this.#table, this.#partition, key, document, options)
    }
    async getOrAddComputed(key: string, computed: () => Promise<unknown>, options?: RetryOptions) {
        const c = await this.#connection
        return await getOrAddComputed(c, this.#table, this.#partition, key, computed, options)
    }
    async addOrUpdate(
        key: string,
        document: unknown,
        update: (existing: unknown) => void,
        options?: RetryOptions,
    ) {
        const c = await this.#connection
        return await addOrUpdate(c, this.#table, this.#partition, key, document, update, options)
    }
    async addOrUpdateComputed(
        key: string,
        computed: () => Promise<unknown>,
        update: (existing: unknown) => void,
        options?: RetryOptions,
    ) {
        const c = await this.#connection
        return await addOrUpdateComputed(
            c,
            this.#table,
            this.#partition,
            key,
            computed,
            update,
            options,
        )
    }
    async converge<T>(
        key: string,
        target: (document: T) => boolean,
        initial: T,
        update: (existing: T) => void,
        options?: RetryOptions,
    ) {
        const c = await this.#connection
        return await converge(
            c,
            this.#table,
            this.#partition,
            key,
            target,
            initial,
            update,
            options,
        )
    }

    async convergeComputed<T>(
        key: string,
        target: (document: T) => boolean,
        computed: () => Promise<T> | T,
        update: (existing: T) => void,
        options?: RetryOptions,
    ) {
        const c = await this.#connection
        return await convergeComputed(
            c,
            this.#table,
            this.#partition,
            key,
            target,
            computed,
            update,
            options,
        )
    }
    async delete(key: string, revision: Revision) {
        const c = await this.#connection
        await c.delete(this.#table, this.#partition, key, revision)
    }
}

type RetryOptions = { retries: number; delay: number; signal: AbortSignal }
type Row = { partition: string; key: string; revision: unknown; document: unknown }

async function getOrAdd(
    c: Connection,
    table: string,
    partition: string,
    key: string,
    document: unknown,
    options?: RetryOptions,
): Promise<Row> {
    return await retryConflict(async () => {
        try {
            return await c.get(table, partition, key)
        } catch (e) {
            if (isNotFound(e)) {
                const revision = await c.add(table, partition, key, document)
                return { partition, key, revision, document }
            }
            throw e
        }
    }, options)
}

async function getOrAddComputed<T>(
    c: Connection,
    table: string,
    partition: string,
    key: string,
    callback: () => Promise<T> | T,
    options?: RetryOptions,
): Promise<Row> {
    return await retryConflict(async () => {
        try {
            return await c.get(table, partition, key)
        } catch (e) {
            if (isNotFound(e)) {
                const document = await callback()
                const revision = await c.add(table, partition, key, document)
                return { partition, key, revision, document }
            }
            throw e
        }
    }, options)
}

async function addOrUpdate<T>(
    c: Connection,
    table: string,
    partition: string,
    key: string,
    document: T,
    update: (existing: T) => void,
    options?: RetryOptions,
): Promise<Row> {
    return await retryConflict(async () => {
        try {
            const row = await c.get(table, partition, key)
            update(row.document as T)
            const revision = await c.update(table, partition, key, row.revision, row.document)
            return { action: 'update', partition, key, revision, document: row.document }
        } catch (e) {
            if (isNotFound(e)) {
                const revision = await c.add(table, partition, key, document)
                return { action: 'add', partition, key, revision, document }
            }
            throw e
        }
    }, options)
}

async function addOrUpdateComputed<T>(
    c: Connection,
    table: string,
    partition: string,
    key: string,
    computed: () => Promise<T> | T,
    update: (existing: T) => void,
    options?: RetryOptions,
): Promise<Row> {
    return await retryConflict(async () => {
        try {
            const row = await c.get(table, partition, key)
            update(row.document as T)
            const revision = await c.update(table, partition, key, row.revision, row.document)
            return { action: 'update', partition, key, revision, document: row.document }
        } catch (e) {
            if (isNotFound(e)) {
                const document = await computed()
                const revision = await c.add(table, partition, key, document)
                return { action: 'add', partition, key, revision, document }
            }
            throw e
        }
    }, options)
}

async function converge<T>(
    c: Connection,
    table: string,
    partition: string,
    key: string,
    target: (document: T) => boolean,
    initial: T,
    update: (existing: T) => void,
    options?: RetryOptions,
): Promise<Row> {
    assert.ok(target(initial), 'Initial document does not meet target.')
    return await retryConflict(async () => {
        try {
            const row = await c.get(table, partition, key)
            if (target(row.document as T)) {
                return row
            }
            update(row.document as T)
            assert.ok(target(row.document as T), 'Updated document does not meet target.')
            const revision = await c.update(table, partition, key, row.revision, row.document)
            return { partition, key, revision, document: row.document }
        } catch (e) {
            if (isNotFound(e)) {
                const revision = await c.add(table, partition, key, initial)
                return { partition, key, revision, document: initial }
            }
            throw e
        }
    }, options)
}

async function convergeComputed<T>(
    c: Connection,
    table: string,
    partition: string,
    key: string,
    target: (document: T) => boolean,
    initial: () => Promise<T> | T,
    update: (existing: T) => void,
    options?: RetryOptions,
): Promise<Row> {
    return await retryConflict(async () => {
        try {
            const row = await c.get(table, partition, key)
            if (target(row.document as T)) {
                return row
            }
            update(row.document as T)
            assert.ok(target(row.document as T), 'Updated document does not meet target.')
            const revision = await c.update(table, partition, key, row.revision, row.document)
            return { partition, key, revision, document: row.document }
        } catch (e) {
            if (isNotFound(e)) {
                const document = await initial()
                assert.ok(target(document), 'Initial document does not meet target.')
                const revision = await c.add(table, partition, key, document)
                return { partition, key, revision, document }
            }
            throw e
        }
    }, options)
}

export async function retryConflict<T>(fn: () => Promise<T>, options?: RetryOptions) {
    for (let remaining = options?.retries ?? 3; ; --remaining) {
        try {
            return await fn()
        } catch (e) {
            if (!remaining) {
                throw e
            }
            if (isConflict(e)) {
                await setTimeout((options?.delay ?? 250) * (Math.random() + 0.5), undefined, {
                    signal: options?.signal,
                })
                continue
            }
            throw e
        }
    }
}

export function isConflict(e: unknown) {
    return (e as { status?: unknown }).status === 409
}

export function isNotFound(e: unknown) {
    return (e as { status?: unknown }).status === 404
}
