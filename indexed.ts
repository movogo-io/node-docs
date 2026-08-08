import { getDriver, type Connection } from './lib/driver.js'
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
            ? Partitions<DocumentOf<Schema, Table>>
            : PartitionsWithFixedKey<Schema, Table>
        : NamedPartitions<Schema, Table>

type PartitionsWithFixedKey<Schema, Table extends TableNamesOf<Schema>> = {
    withKey<K extends KeyOf<Schema, Table>>(key: K): FixedKey<DocumentOfFixedKey<Schema, Table, K>>
}

type NamedPartitions<Schema, Table extends TableNamesOf<Schema>> = {
    readonly [P in PartitionKeyOf<Schema, Table>]: NamedPartition<
        DocumentOfFixedPartition<Schema, Table, P>
    >
}

type GenericSchema = {
    [table: string]: {
        [partition: string]: {
            [key: string]: unknown
        }
    }
}

export function index<Schema = GenericSchema>(
    context: Context & { on?: undefined },
    name: string,
): Tables<Schema> & AsyncDisposable
export function index<Schema = GenericSchema>(
    context: Context & { on: (event: 'free', handler: () => Promise<void>) => void },
    name: string,
): Tables<Schema>

export function index<Schema = GenericSchema>(context: Context, name: string) {
    const d = getDriver()
    const connection = d.connect(context)
    const closer = async () => {
        const c = await connection
        await c.close()
    }
    const p = new Proxy(indexBase(connection, name), indexProxy) as unknown as Tables<Schema>
    if (!context.on?.('free', closer)) {
        const dp = p as Tables<Schema> & AsyncDisposable
        dp[Symbol.asyncDispose] = closer
    }
    return p
}

const connectionEntry = Symbol()
const indexNameEntry = Symbol()

function indexBase(connection: Promise<Connection>, name: string) {
    return {
        [connectionEntry]: connection,
        [indexNameEntry]: name,
    }
}

type GenericProxyTarget = { [k: string | symbol]: unknown }

const indexProxy = {
    get: (target: GenericProxyTarget & ReturnType<typeof indexBase>, property: string | symbol) => {
        if (Object.hasOwn(target, property)) {
            return target[property]
        }
        if (typeof property === 'symbol') {
            return undefined
        }
        throw new Error('Not implemented.')
    },
}

type NamedPartition<Document> = {
    by: (key: (document: Document) => string) => Index<Document>
}

type FixedKey<Document> = {
    by: <PartitionKey extends string>(
        partition: (d: Document) => PartitionKey,
        key: (d: Document) => string,
    ) => 'string' extends PartitionKey
        ? IndexPartition<Document>
        : NamedIndexPartition<PartitionKey, Document>
}

type Partitions<Document> = {
    by: <PartitionKey extends string>(
        partition: (r: { partition: string; key: string; document: Document }) => PartitionKey,
        key: (r: { partition: string; key: string; document: Document }) => string,
    ) => 'string' extends PartitionKey
        ? IndexPartition<Document>
        : NamedIndexPartition<PartitionKey, Document>
}

type IndexPartition<Document> = {
    partition: (partition: string) => Index<Document>
}

type NamedIndexPartition<PartitionKey extends string, Document> = {
    readonly [P in PartitionKey]: Index<Document>
}

type Index<Document> = {
    get: (
        key: string,
    ) => Promise<{ key: string; revision: Revision; document: Document } | undefined>
    getDocument: (key: string) => Promise<Document | undefined>
    getRange: (
        range: KeyRange,
    ) => AsyncIterable<{ key: string; revision: Revision; document: Document }>
}
