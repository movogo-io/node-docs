import assert from 'node:assert/strict'
import { setTimeout } from 'node:timers/promises'
import { isConflict } from './lib/errors.js'
import { findEachUnexpired, findUnexpired, getRow, getUnexpired, unexpired } from './lib/expiry.js'
import {
    addWithIndexes,
    deleteWithIndexes,
    expandIndexOperations,
    replacedOf,
    updateWithIndexes,
} from './lib/indexes.js'
import type { ReadOptions } from './lib/driver.js'
import { openSession, type Session } from './lib/session.js'
import { TransactionBuffer } from './lib/transaction.js'
import type { KeyRange, Revision, StoredDocument } from './schema.js'

// Intersected with `object` so it is not a weak type: a context with other
// properties and neither of these, like a @riddance/service one, still fits.
export type Context = object & {
    on?: (event: 'free', handler: () => Promise<void>) => boolean
    now?: () => Date
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

export type Tables<Schema> = string extends TableNamesOf<Schema> ? never : NamedTables<Schema>

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
        options?: ReadOptions,
    ) => Promise<{
        partition: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
    }>
    getDocument: (partition: string, options?: ReadOptions) => Promise<Document>
    find: (
        partition: string,
        options?: ReadOptions,
    ) => Promise<
        | {
              partition: string
              revision: Revision
              document: Document
              seq: number
              updatedAt: string
          }
        | undefined
    >
    findEach: (
        partitions: readonly string[],
        options?: ReadOptions,
    ) => Promise<
        {
            partition: string
            revision: Revision
            document: Document
            seq: number
            updatedAt: string
        }[]
    >
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
    ) => Promise<{
        partition: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
    }>
    addOrUpdate: (
        partition: string,
        document: Document,
        update: (existing: Document) => Document | void,
        options?: RetryOptions,
    ) => Promise<{
        action: 'add' | 'update'
        partition: Revision
        key: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
    }>
    addOrUpdateComputed: (
        partition: string,
        computed: () => Promise<Document> | Document,
        update: (existing: Document) => Document | void,
        options?: RetryOptions,
    ) => Promise<{
        action: 'add' | 'update'
        partition: Revision
        key: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
    }>
    converge: (
        partition: string,
        target: (document: Document) => boolean,
        initial: Document,
        update: (existing: Document) => Document | void,
        options?: RetryOptions,
    ) => Promise<{
        partition: Revision
        key: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
    }>
    convergeComputed: (
        partition: string,
        target: (document: Document) => boolean,
        computed: () => Promise<Document> | Document,
        update: (existing: Document) => Document | void,
        options?: RetryOptions,
    ) => Promise<{
        partition: Revision
        key: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
    }>
    delete: (partition: string, revision: Revision) => Promise<void>
}

type NamedPartition<Document> = {
    add: (key: string, document: Document) => Promise<Revision>
    get: (
        key: string,
        options?: ReadOptions,
    ) => Promise<{
        key: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
    }>
    getDocument: (key: string, options?: ReadOptions) => Promise<Document>
    find: (
        key: string,
        options?: ReadOptions,
    ) => Promise<
        | {
              key: string
              revision: Revision
              document: Document
              seq: number
              updatedAt: string
          }
        | undefined
    >
    findEach: (
        keys: readonly string[],
        options?: ReadOptions,
    ) => Promise<
        { key: string; revision: Revision; document: Document; seq: number; updatedAt: string }[]
    >
    getAll: (options?: ReadOptions) => AsyncIterable<{
        key: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
    }>
    getRange: (
        range: KeyRange,
        options?: ReadOptions,
    ) => AsyncIterable<{
        key: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
    }>
    update: (key: string, revision: Revision, document: Document) => Promise<Revision>
    updateRow: (row: { key: string; revision: Revision; document: Document }) => Promise<Revision>
    getOrAdd: (
        key: string,
        document: Document,
        options?: RetryOptions,
    ) => Promise<{
        key: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
    }>
    getOrAddComputed: (
        key: string,
        computed: () => Promise<Document> | Document,
        options?: RetryOptions,
    ) => Promise<{
        key: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
    }>
    addOrUpdate: (
        key: string,
        document: Document,
        update: (existing: Document) => Document | void,
        options?: RetryOptions,
    ) => Promise<{
        action: 'add' | 'update'
        partition: Revision
        key: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
    }>
    addOrUpdateComputed: (
        key: string,
        computed: () => Promise<Document> | Document,
        update: (existing: Document) => Document | void,
        options?: RetryOptions,
    ) => Promise<{
        action: 'add' | 'update'
        partition: Revision
        key: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
    }>
    converge: (
        key: string,
        target: (document: Document) => boolean,
        initial: Document,
        update: (existing: Document) => Document | void,
        options?: RetryOptions,
    ) => Promise<{
        partition: Revision
        key: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
    }>
    convergeComputed: (
        key: string,
        target: (document: Document) => boolean,
        computed: () => Promise<Document> | Document,
        update: (existing: Document) => Document | void,
        options?: RetryOptions,
    ) => Promise<{
        partition: Revision
        key: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
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
    const session = openSession(context)
    const closer = async () => {
        const c = await session.connection
        await c.close()
    }
    const p = new Proxy(tablesBase(session), tablesProxy) as unknown as Tables<Schema>
    if (!context.on?.('free', closer)) {
        const dp = p as Tables<Schema> & AsyncDisposable
        dp[Symbol.asyncDispose] = closer
    }
    return p
}

const sessionEntry = Symbol()
const tableNameEntry = Symbol()

function tablesBase(session: Session) {
    return {
        [sessionEntry]: session,
    }
}

type GenericProxyTarget = { [k: string | symbol]: unknown }

function facadeProxy<B extends object>(sub: (target: B, name: string) => unknown): ProxyHandler<B> {
    return {
        get: (target, property) => {
            if (Object.hasOwn(target, property)) {
                return (target as GenericProxyTarget)[property]
            }
            if (typeof property === 'symbol') {
                return undefined
            }
            return sub(target, property)
        },
    }
}

const tablesProxy = facadeProxy((target: ReturnType<typeof tablesBase>, table) => {
    return new Proxy(tableBase(target, table), tableProxy)
})

function tableBase(db: ReturnType<typeof tablesBase>, table: string) {
    const session = db[sessionEntry]
    return {
        [sessionEntry]: session,
        [tableNameEntry]: table,
        withKey: (key: string) => ({
            async add(partition: string, document: unknown) {
                const c = await session.connection
                return (
                    await addWithIndexes(c, table, partition, key, document, session.nowSeconds())
                ).revision
            },
            async get(partition: string, options?: ReadOptions) {
                const c = await session.connection
                return await getUnexpired(c, table, partition, key, session.nowSeconds(), options)
            },
            async getDocument(partition: string, options?: ReadOptions) {
                const c = await session.connection
                const r = await getUnexpired(
                    c,
                    table,
                    partition,
                    key,
                    session.nowSeconds(),
                    options,
                )
                return r.document
            },
            async find(partition: string, options?: ReadOptions) {
                const c = await session.connection
                return await findUnexpired(c, table, partition, key, session.nowSeconds(), options)
            },
            async findEach(partitions: readonly string[], options?: ReadOptions) {
                const c = await session.connection
                return await findEachUnexpired(
                    c,
                    table,
                    partitions.map(partition => ({ partition, key })),
                    session.nowSeconds(),
                    options,
                )
            },
            async update(partition: string, revision: Revision, document: StoredDocument) {
                const c = await session.connection
                return (
                    await updateWithIndexes(
                        c,
                        table,
                        partition,
                        key,
                        revision,
                        document,
                        session.nowSeconds(),
                    )
                ).revision
            },
            async updateRow(row: {
                partition: string
                revision: Revision
                document: StoredDocument
            }) {
                const c = await session.connection
                return (
                    await updateWithIndexes(
                        c,
                        table,
                        row.partition,
                        key,
                        row.revision,
                        row.document,
                        session.nowSeconds(),
                    )
                ).revision
            },
            getOrAdd: async (partition: string, document: unknown, options?: RetryOptions) =>
                await getOrAdd(session, table, partition, key, document, options),
            getOrAddComputed: async <T>(
                partition: string,
                computed: () => Promise<T> | T,
                options?: RetryOptions,
            ) => await getOrAddComputed(session, table, partition, key, computed, options),
            addOrUpdate: async (
                partition: string,
                document: unknown,
                update: (existing: unknown) => unknown,
                options?: RetryOptions,
            ) => await addOrUpdate(session, table, partition, key, document, update, options),
            addOrUpdateComputed: async <T>(
                partition: string,
                computed: () => Promise<T> | T,
                update: (existing: T) => T | void,
                options?: RetryOptions,
            ) =>
                await addOrUpdateComputed(
                    session,
                    table,
                    partition,
                    key,
                    computed,
                    update,
                    options,
                ),
            converge: async <T>(
                partition: string,
                target: (document: T) => boolean,
                initial: T,
                update: (existing: T) => T | void,
                options?: RetryOptions,
            ) => await converge(session, table, partition, key, target, initial, update, options),
            convergeComputed: async <T>(
                partition: string,
                target: (document: T) => boolean,
                computed: () => Promise<T> | T,
                update: (existing: T) => T | void,
                options?: RetryOptions,
            ) =>
                await convergeComputed(
                    session,
                    table,
                    partition,
                    key,
                    target,
                    computed,
                    update,
                    options,
                ),
            async delete(partition: string, revision: Revision) {
                const c = await session.connection
                await deleteWithIndexes(c, table, partition, key, revision, session.nowSeconds())
            },
        }),
        partition: (partition: string) => new Partition(session, table, partition),
        async *getPartitions() {
            const c = await session.connection
            for await (const partition of c.getPartitions(table)) {
                yield partition
            }
        },
    }
}

const tableProxy = facadeProxy((target: ReturnType<typeof tableBase>, partition) => {
    return new Partition(target[sessionEntry], target[tableNameEntry], partition)
})

class Partition {
    readonly #session
    readonly #table
    readonly #partition

    constructor(session: Session, table: string, partition: string) {
        this.#session = session
        this.#table = table
        this.#partition = partition
    }

    async add(key: string, document: StoredDocument) {
        const c = await this.#session.connection
        return (
            await addWithIndexes(
                c,
                this.#table,
                this.#partition,
                key,
                document,
                this.#session.nowSeconds(),
            )
        ).revision
    }
    async get(key: string, options?: ReadOptions) {
        const c = await this.#session.connection
        return await getUnexpired(
            c,
            this.#table,
            this.#partition,
            key,
            this.#session.nowSeconds(),
            options,
        )
    }
    async getDocument(key: string, options?: ReadOptions) {
        const r = await this.get(key, options)
        return r.document
    }
    async find(key: string, options?: ReadOptions) {
        const c = await this.#session.connection
        return await findUnexpired(
            c,
            this.#table,
            this.#partition,
            key,
            this.#session.nowSeconds(),
            options,
        )
    }
    async findEach(keys: readonly string[], options?: ReadOptions) {
        const c = await this.#session.connection
        return await findEachUnexpired(
            c,
            this.#table,
            keys.map(key => ({ partition: this.#partition, key })),
            this.#session.nowSeconds(),
            options,
        )
    }
    async *getAll(options?: ReadOptions) {
        const c = await this.#session.connection
        yield* unexpired(
            c.getPartition(this.#table, this.#partition, undefined, options),
            this.#session.nowSeconds(),
        )
    }
    async *getRange(range: KeyRange, options?: ReadOptions) {
        const c = await this.#session.connection
        yield* unexpired(
            c.getPartition(this.#table, this.#partition, range, options),
            this.#session.nowSeconds(),
        )
    }
    async update(key: string, revision: Revision, document: StoredDocument) {
        const c = await this.#session.connection
        return (
            await updateWithIndexes(
                c,
                this.#table,
                this.#partition,
                key,
                revision,
                document,
                this.#session.nowSeconds(),
            )
        ).revision
    }
    async updateRow(row: { key: string; revision: Revision; document: StoredDocument }) {
        const c = await this.#session.connection
        return (
            await updateWithIndexes(
                c,
                this.#table,
                this.#partition,
                row.key,
                row.revision,
                row.document,
                this.#session.nowSeconds(),
            )
        ).revision
    }
    async getOrAdd(key: string, document: unknown, options?: RetryOptions) {
        return await getOrAdd(this.#session, this.#table, this.#partition, key, document, options)
    }
    async getOrAddComputed(key: string, computed: () => Promise<unknown>, options?: RetryOptions) {
        return await getOrAddComputed(
            this.#session,
            this.#table,
            this.#partition,
            key,
            computed,
            options,
        )
    }
    async addOrUpdate(
        key: string,
        document: unknown,
        update: (existing: unknown) => unknown,
        options?: RetryOptions,
    ) {
        return await addOrUpdate(
            this.#session,
            this.#table,
            this.#partition,
            key,
            document,
            update,
            options,
        )
    }
    async addOrUpdateComputed(
        key: string,
        computed: () => Promise<unknown>,
        update: (existing: unknown) => unknown,
        options?: RetryOptions,
    ) {
        return await addOrUpdateComputed(
            this.#session,
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
        update: (existing: T) => T | void,
        options?: RetryOptions,
    ) {
        return await converge(
            this.#session,
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
        update: (existing: T) => T | void,
        options?: RetryOptions,
    ) {
        return await convergeComputed(
            this.#session,
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
        const c = await this.#session.connection
        await deleteWithIndexes(
            c,
            this.#table,
            this.#partition,
            key,
            revision,
            this.#session.nowSeconds(),
        )
    }
}

export type TransactionTables<Schema> =
    string extends TableNamesOf<Schema> ? never : NamedTransactionTables<Schema>

type NamedTransactionTables<Schema> = {
    readonly [P in TableNamesOf<Schema>]: TransactionDocuments<Schema, P>
}

type TransactionDocuments<Schema, Table extends TableNamesOf<Schema>> =
    string extends PartitionKeyOf<Schema, Table>
        ? string extends KeyOf<Schema, Table>
            ? TransactionPartitions<Schema, Table>
            : TransactionPartitionsWithFixedKey<Schema, Table>
        : NamedTransactionPartitions<Schema, Table>

type TransactionPartitionsWithFixedKey<Schema, Table extends TableNamesOf<Schema>> = {
    withKey<K extends KeyOf<Schema, Table>>(
        key: K,
    ): TransactionFixedKey<DocumentOfFixedKey<Schema, Table, K>>
}

type NamedTransactionPartitions<Schema, Table extends TableNamesOf<Schema>> = {
    readonly [P in PartitionKeyOf<Schema, Table>]: TransactionNamedPartition<
        DocumentOfFixedPartition<Schema, Table, P>
    >
}

type TransactionPartitions<Schema, Table extends TableNamesOf<Schema>> = {
    partition(partition: string): TransactionNamedPartition<DocumentOf<Schema, Table>>
}

type TransactionFixedKey<Document> = {
    add: (partition: string, document: Document) => Promise<Revision>
    get: (
        partition: string,
        options?: ReadOptions,
    ) => Promise<{
        partition: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
    }>
    getDocument: (partition: string, options?: ReadOptions) => Promise<Document>
    find: (
        partition: string,
        options?: ReadOptions,
    ) => Promise<
        | {
              partition: string
              revision: Revision
              document: Document
              seq: number
              updatedAt: string
          }
        | undefined
    >
    findEach: (
        partitions: readonly string[],
        options?: ReadOptions,
    ) => Promise<
        {
            partition: string
            revision: Revision
            document: Document
            seq: number
            updatedAt: string
        }[]
    >
    update: (partition: string, revision: Revision, document: Document) => Promise<Revision>
    updateRow: (row: {
        partition: string
        revision: Revision
        document: Document
    }) => Promise<Revision>
    check: (partition: string, revision: Revision) => Promise<void>
    delete: (partition: string, revision: Revision) => Promise<void>
}

type TransactionNamedPartition<Document> = {
    add: (key: string, document: Document) => Promise<Revision>
    get: (
        key: string,
        options?: ReadOptions,
    ) => Promise<{
        key: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
    }>
    getDocument: (key: string, options?: ReadOptions) => Promise<Document>
    find: (
        key: string,
        options?: ReadOptions,
    ) => Promise<
        | {
              key: string
              revision: Revision
              document: Document
              seq: number
              updatedAt: string
          }
        | undefined
    >
    findEach: (
        keys: readonly string[],
        options?: ReadOptions,
    ) => Promise<
        { key: string; revision: Revision; document: Document; seq: number; updatedAt: string }[]
    >
    getAll: (options?: ReadOptions) => AsyncIterable<{
        key: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
    }>
    getRange: (
        range: KeyRange,
        options?: ReadOptions,
    ) => AsyncIterable<{
        key: string
        revision: Revision
        document: Document
        seq: number
        updatedAt: string
    }>
    update: (key: string, revision: Revision, document: Document) => Promise<Revision>
    updateRow: (row: { key: string; revision: Revision; document: Document }) => Promise<Revision>
    check: (key: string, revision: Revision) => Promise<void>
    delete: (key: string, revision: Revision) => Promise<void>
}

export async function withTransaction<Schema = GenericSchema, T = void>(
    context: Context,
    fn: (tx: TransactionTables<Schema>) => Promise<T>,
    options?: RetryOptions,
): Promise<T> {
    const session = openSession(context)
    const closer = async () => {
        const c = await session.connection
        await c.close()
    }
    const registered = context.on?.('free', closer) ?? false
    try {
        const c = await session.connection
        return await retryConflict(async () => {
            const buffer = new TransactionBuffer()
            const tx = new Proxy(
                transactionTablesBase(session, buffer),
                transactionTablesProxy,
            ) as unknown as TransactionTables<Schema>
            const result = await fn(tx)
            const now = session.nowSeconds()
            const items = await expandIndexOperations(c, buffer.seal(), now)
            if (items.length !== 0) {
                await c.transact(items, { now })
            }
            return result
        }, options)
    } finally {
        if (!registered) {
            await closer()
        }
    }
}

const bufferEntry = Symbol()

function transactionTablesBase(session: Session, buffer: TransactionBuffer) {
    return {
        [sessionEntry]: session,
        [bufferEntry]: buffer,
    }
}

const transactionTablesProxy = facadeProxy(
    (target: ReturnType<typeof transactionTablesBase>, table) => {
        return new Proxy(transactionTableBase(target, table), transactionTableProxy)
    },
)

function transactionTableBase(db: ReturnType<typeof transactionTablesBase>, table: string) {
    return {
        [sessionEntry]: db[sessionEntry],
        [bufferEntry]: db[bufferEntry],
        [tableNameEntry]: table,
        withKey: (key: string) =>
            new TransactionFixedKeySet(db[sessionEntry], db[bufferEntry], table, key),
        partition: (partition: string) =>
            new TransactionPartition(db[sessionEntry], db[bufferEntry], table, partition),
    }
}

const transactionTableProxy = facadeProxy(
    (target: ReturnType<typeof transactionTableBase>, partition) => {
        return new TransactionPartition(
            target[sessionEntry],
            target[bufferEntry],
            target[tableNameEntry],
            partition,
        )
    },
)

class TransactionPartition {
    readonly #reads
    readonly #buffer
    readonly #table
    readonly #partition

    constructor(session: Session, buffer: TransactionBuffer, table: string, partition: string) {
        this.#reads = new Partition(session, table, partition)
        this.#buffer = buffer
        this.#table = table
        this.#partition = partition
    }

    get(key: string, options?: ReadOptions) {
        return this.#reads.get(key, options)
    }
    getDocument(key: string, options?: ReadOptions) {
        return this.#reads.getDocument(key, options)
    }
    find(key: string, options?: ReadOptions) {
        return this.#reads.find(key, options)
    }
    findEach(keys: readonly string[], options?: ReadOptions) {
        return this.#reads.findEach(keys, options)
    }
    getAll(options?: ReadOptions) {
        return this.#reads.getAll(options)
    }
    getRange(range: KeyRange, options?: ReadOptions) {
        return this.#reads.getRange(range, options)
    }
    add(key: string, document: StoredDocument) {
        return this.#buffer.add(this.#table, this.#partition, key, document)
    }
    update(key: string, revision: Revision, document: StoredDocument) {
        return this.#buffer.update(this.#table, this.#partition, key, revision, document)
    }
    updateRow(row: { key: string; revision: Revision; document: StoredDocument }) {
        return this.#buffer.update(
            this.#table,
            this.#partition,
            row.key,
            row.revision,
            row.document,
        )
    }
    check(key: string, revision: Revision) {
        return this.#buffer.check(this.#table, this.#partition, key, revision)
    }
    delete(key: string, revision: Revision) {
        return this.#buffer.delete(this.#table, this.#partition, key, revision)
    }
}

class TransactionFixedKeySet {
    readonly #session
    readonly #buffer
    readonly #table
    readonly #key

    constructor(session: Session, buffer: TransactionBuffer, table: string, key: string) {
        this.#session = session
        this.#buffer = buffer
        this.#table = table
        this.#key = key
    }

    async get(partition: string, options?: ReadOptions) {
        const c = await this.#session.connection
        return await getUnexpired(
            c,
            this.#table,
            partition,
            this.#key,
            this.#session.nowSeconds(),
            options,
        )
    }
    async getDocument(partition: string, options?: ReadOptions) {
        const r = await this.get(partition, options)
        return r.document
    }
    async find(partition: string, options?: ReadOptions) {
        const c = await this.#session.connection
        return await findUnexpired(
            c,
            this.#table,
            partition,
            this.#key,
            this.#session.nowSeconds(),
            options,
        )
    }
    async findEach(partitions: readonly string[], options?: ReadOptions) {
        const c = await this.#session.connection
        return await findEachUnexpired(
            c,
            this.#table,
            partitions.map(partition => ({ partition, key: this.#key })),
            this.#session.nowSeconds(),
            options,
        )
    }
    add(partition: string, document: StoredDocument) {
        return this.#buffer.add(this.#table, partition, this.#key, document)
    }
    update(partition: string, revision: Revision, document: StoredDocument) {
        return this.#buffer.update(this.#table, partition, this.#key, revision, document)
    }
    updateRow(row: { partition: string; revision: Revision; document: StoredDocument }) {
        return this.#buffer.update(
            this.#table,
            row.partition,
            this.#key,
            row.revision,
            row.document,
        )
    }
    check(partition: string, revision: Revision) {
        return this.#buffer.check(this.#table, partition, this.#key, revision)
    }
    delete(partition: string, revision: Revision) {
        return this.#buffer.delete(this.#table, partition, this.#key, revision)
    }
}

// `consistent` reaches the read a helper makes before it writes; see ReadOptions.
export type RetryOptions = {
    retries?: number
    delay?: number
    signal?: AbortSignal
    consistent?: boolean
}

function readOptionsOf(options: RetryOptions | undefined): ReadOptions | undefined {
    return options?.consistent === undefined ? undefined : { consistent: options.consistent }
}
type Row = {
    partition: string
    key: string
    revision: unknown
    document: unknown
    seq: number
    updatedAt: string
}

async function getOrAdd(
    session: Session,
    table: string,
    partition: string,
    key: string,
    document: unknown,
    options?: RetryOptions,
): Promise<Row> {
    return await retryConflict(async () => {
        const c = await session.connection
        const now = session.nowSeconds()
        const { live, expired } = await getRow(
            c,
            table,
            partition,
            key,
            now,
            readOptionsOf(options),
        )
        if (live) {
            return live
        }
        const replaced = replacedOf(table, partition, key, expired)
        const written = await addWithIndexes(c, table, partition, key, document, now, replaced)
        return { partition, key, document, ...written }
    }, options)
}

async function getOrAddComputed<T>(
    session: Session,
    table: string,
    partition: string,
    key: string,
    callback: () => Promise<T> | T,
    options?: RetryOptions,
): Promise<Row> {
    return await retryConflict(async () => {
        const c = await session.connection
        const now = session.nowSeconds()
        const { live, expired } = await getRow(
            c,
            table,
            partition,
            key,
            now,
            readOptionsOf(options),
        )
        if (live) {
            return live
        }
        const document = await callback()
        const replaced = replacedOf(table, partition, key, expired)
        const written = await addWithIndexes(c, table, partition, key, document, now, replaced)
        return { partition, key, document, ...written }
    }, options)
}

async function addOrUpdate<T>(
    session: Session,
    table: string,
    partition: string,
    key: string,
    document: T,
    update: (existing: T) => T | void,
    options?: RetryOptions,
): Promise<Row> {
    return await retryConflict(async () => {
        const c = await session.connection
        const now = session.nowSeconds()
        const { live, expired } = await getRow(
            c,
            table,
            partition,
            key,
            now,
            readOptionsOf(options),
        )
        if (!live) {
            const replaced = replacedOf(table, partition, key, expired)
            const written = await addWithIndexes(c, table, partition, key, document, now, replaced)
            return { action: 'add', partition, key, document, ...written }
        }
        const replaced = replacedOf(table, partition, key, live)
        const updated = update(live.document as T) ?? (live.document as T)
        const written = await updateWithIndexes(
            c,
            table,
            partition,
            key,
            live.revision,
            updated,
            now,
            replaced,
        )
        return { action: 'update', partition, key, document: updated, ...written }
    }, options)
}

async function addOrUpdateComputed<T>(
    session: Session,
    table: string,
    partition: string,
    key: string,
    computed: () => Promise<T> | T,
    update: (existing: T) => T | void,
    options?: RetryOptions,
): Promise<Row> {
    return await retryConflict(async () => {
        const c = await session.connection
        const now = session.nowSeconds()
        const { live, expired } = await getRow(
            c,
            table,
            partition,
            key,
            now,
            readOptionsOf(options),
        )
        if (!live) {
            const document = await computed()
            const replaced = replacedOf(table, partition, key, expired)
            const written = await addWithIndexes(c, table, partition, key, document, now, replaced)
            return { action: 'add', partition, key, document, ...written }
        }
        const replaced = replacedOf(table, partition, key, live)
        const updated = update(live.document as T) ?? (live.document as T)
        const written = await updateWithIndexes(
            c,
            table,
            partition,
            key,
            live.revision,
            updated,
            now,
            replaced,
        )
        return { action: 'update', partition, key, document: updated, ...written }
    }, options)
}

async function converge<T>(
    session: Session,
    table: string,
    partition: string,
    key: string,
    target: (document: T) => boolean,
    initial: T,
    update: (existing: T) => T | void,
    options?: RetryOptions,
): Promise<Row> {
    assert.ok(target(initial), 'Initial document does not meet target.')
    return await retryConflict(async () => {
        const c = await session.connection
        const now = session.nowSeconds()
        const { live, expired } = await getRow(
            c,
            table,
            partition,
            key,
            now,
            readOptionsOf(options),
        )
        if (!live) {
            const replaced = replacedOf(table, partition, key, expired)
            const written = await addWithIndexes(c, table, partition, key, initial, now, replaced)
            return { partition, key, document: initial, ...written }
        }
        if (target(live.document as T)) {
            return live
        }
        const replaced = replacedOf(table, partition, key, live)
        const updated = update(live.document as T) ?? (live.document as T)
        assert.ok(target(updated), 'Updated document does not meet target.')
        const written = await updateWithIndexes(
            c,
            table,
            partition,
            key,
            live.revision,
            updated,
            now,
            replaced,
        )
        return { partition, key, document: updated, ...written }
    }, options)
}

async function convergeComputed<T>(
    session: Session,
    table: string,
    partition: string,
    key: string,
    target: (document: T) => boolean,
    initial: () => Promise<T> | T,
    update: (existing: T) => T | void,
    options?: RetryOptions,
): Promise<Row> {
    return await retryConflict(async () => {
        const c = await session.connection
        const now = session.nowSeconds()
        const { live, expired } = await getRow(
            c,
            table,
            partition,
            key,
            now,
            readOptionsOf(options),
        )
        if (!live) {
            const document = await initial()
            assert.ok(target(document), 'Initial document does not meet target.')
            const replaced = replacedOf(table, partition, key, expired)
            const written = await addWithIndexes(c, table, partition, key, document, now, replaced)
            return { partition, key, document, ...written }
        }
        if (target(live.document as T)) {
            return live
        }
        const replaced = replacedOf(table, partition, key, live)
        const updated = update(live.document as T) ?? (live.document as T)
        assert.ok(target(updated), 'Updated document does not meet target.')
        const written = await updateWithIndexes(
            c,
            table,
            partition,
            key,
            live.revision,
            updated,
            now,
            replaced,
        )
        return { partition, key, document: updated, ...written }
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

export { isConflict, isNotFound } from './lib/errors.js'
export { compositeKey, compositeRange } from './lib/keys.js'
export type { ReadOptions } from './lib/driver.js'
