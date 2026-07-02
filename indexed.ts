import { getDriver, type Connection } from './lib/driver.js'
import {
    assertClean,
    indexKeyDelimiter,
    indexTable,
    registerIndex,
    type IndexDefinition,
    type IndexSourceRow,
} from './lib/indexes.js'
import { tables, type Tables } from './partitioned.js'
import type { KeyRange, Revision } from './schema.js'

type Context = {
    on?: (event: 'free', handler: () => Promise<void>) => boolean
}

type TableNamesOf<Schema> = keyof Schema & string
type PartitionKeyOf<Schema, Table extends TableNamesOf<Schema>> = keyof Schema[Table] & string
type KeyOf<
    Schema,
    Table extends TableNamesOf<Schema> = TableNamesOf<Schema>,
> = keyof Schema[Table][PartitionKeyOf<Schema, Table>] & string

type DocumentOf<
    Schema,
    Table extends TableNamesOf<Schema> = TableNamesOf<Schema>,
> = Schema[Table][PartitionKeyOf<Schema, Table>][KeyOf<Schema, Table>]

type GenericSchema = {
    [table: string]: {
        [partition: string]: {
            [key: string]: unknown
        }
    }
}

export type IndexSource<Schema, Table extends TableNamesOf<Schema>> = {
    partition: string
    key: string
    document: DocumentOf<Schema, Table>
}

export type SchemaHandle<Schema> = {
    tables(context: Context & { on?: undefined }): Tables<Schema> & AsyncDisposable
    tables(
        context: Context & { on: (event: 'free', handler: () => Promise<void>) => void },
    ): Tables<Schema>
    index<Table extends TableNamesOf<Schema>, const PartitionKey extends string>(
        table: Table,
        name: string,
        partition: (row: IndexSource<Schema, Table>) => PartitionKey | undefined,
        key: (row: IndexSource<Schema, Table>) => string | undefined,
    ): IndexAccessor<DocumentOf<Schema, Table>, PartitionKey>
}

export type IndexAccessor<Document, PartitionKey extends string> = {
    (
        context: Context & { on?: undefined },
    ): IndexPartitions<Document, PartitionKey> & AsyncDisposable
    (
        context: Context & { on: (event: 'free', handler: () => Promise<void>) => void },
    ): IndexPartitions<Document, PartitionKey>
}

type IndexPartitions<Document, PartitionKey extends string> = string extends PartitionKey
    ? IndexPartition<Document>
    : NamedIndexPartition<PartitionKey, Document>

type IndexPartition<Document> = {
    partition: (partition: string) => Index<Document>
}

type NamedIndexPartition<PartitionKey extends string, Document> = {
    readonly [P in PartitionKey]: Index<Document>
}

export type IndexRow<Document> = {
    key: string
    revision: Revision
    document: Document
    source: { partition: string; key: string }
}

type Index<Document> = {
    get: (key: string) => Promise<IndexRow<Document> | undefined>
    getDocument: (key: string) => Promise<Document | undefined>
    getRange: (range: KeyRange) => AsyncIterable<IndexRow<Document>>
}

export function docs<Schema = GenericSchema>(): SchemaHandle<Schema> {
    return {
        tables: (context: Context) => tables<Schema>(context as Context & { on?: undefined }),
        index: (
            table: string,
            name: string,
            partition: (row: IndexSourceRow) => string | undefined,
            key: (row: IndexSourceRow) => string | undefined,
        ) => {
            const definition = { table, name, partition, key }
            registerIndex(definition)
            return indexAccessor(definition)
        },
    } as unknown as SchemaHandle<Schema>
}

function indexAccessor(definition: IndexDefinition) {
    return (context: Context) => {
        const connection = getDriver().connect(context)
        const closer = async () => {
            const c = await connection
            await c.close()
        }
        const p = new Proxy(
            {
                partition: (partition: string) =>
                    new IndexReader(connection, definition, partition),
            },
            indexPartitionsProxy(connection, definition),
        )
        if (!context.on?.('free', closer)) {
            ;(p as typeof p & AsyncDisposable)[Symbol.asyncDispose] = closer
        }
        return p
    }
}

type GenericProxyTarget = { [k: string | symbol]: unknown }

function indexPartitionsProxy<B extends object>(
    connection: Promise<Connection>,
    definition: IndexDefinition,
): ProxyHandler<B> {
    return {
        get: (target, property) => {
            if (property in target) {
                return (target as GenericProxyTarget)[property]
            }
            if (typeof property === 'symbol') {
                return undefined
            }
            return new IndexReader(connection, definition, property)
        },
    }
}

class IndexReader {
    readonly #connection
    readonly #definition
    readonly #partition

    constructor(connection: Promise<Connection>, definition: IndexDefinition, partition: string) {
        this.#connection = connection
        this.#definition = definition
        this.#partition = partition
    }

    async get(key: string) {
        assertClean(key, 'index key')
        const rows = this.#rows({ withPrefix: key + indexKeyDelimiter })
        try {
            const first = await rows.next()
            return first.done ? undefined : first.value
        } finally {
            await rows.return(undefined)
        }
    }

    async getDocument(key: string) {
        return (await this.get(key))?.document
    }

    async *getRange(range: KeyRange) {
        validateRange(range)
        const matches = matchRange(range)
        for await (const row of this.#rows(range)) {
            if (matches(row.key)) {
                yield row
            }
        }
    }

    async *#rows(range: KeyRange) {
        const c = await this.#connection
        for await (const row of c.getPartition(
            indexTable(this.#definition),
            this.#partition,
            range,
        )) {
            yield decode(row)
        }
    }
}

function decode(row: { key: string; revision: Revision; document: unknown }): IndexRow<unknown> {
    const first = row.key.indexOf(indexKeyDelimiter)
    const second = first === -1 ? -1 : row.key.indexOf(indexKeyDelimiter, first + 1)
    return {
        key: first === -1 ? row.key : row.key.slice(0, first),
        revision: row.revision,
        document: row.document,
        source: {
            partition:
                first === -1 ? '' : row.key.slice(first + 1, second === -1 ? undefined : second),
            key: second === -1 ? '' : row.key.slice(second + 1),
        },
    }
}

function validateRange(range: KeyRange) {
    if ('withPrefix' in range) {
        assertClean(range.withPrefix, 'index range prefix')
        return
    }
    if (range.after !== undefined) {
        assertClean(range.after, "index range 'after'")
    }
    if (range.before !== undefined) {
        assertClean(range.before, "index range 'before'")
    }
}

function matchRange(range: KeyRange) {
    if ('withPrefix' in range) {
        return (key: string) => key.startsWith(range.withPrefix)
    }
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
    return alwaysFalse
}

function alwaysFalse() {
    return false
}
