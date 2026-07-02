import { randomUUID } from 'node:crypto'
import type { Revision, StoredDocument } from '../schema.js'
import type { Connection, TransactionItem } from './driver.js'
import { maxTransactionItems } from './transaction.js'

// Separates the index key value from the owning row's partition and key in the
// physical key of an index entry. Being the smallest possible character, it is
// the only delimiter that keeps prefix and before/after range semantics on the
// physical keys identical to those on the undecorated index key values.
export const indexKeyDelimiter = '\u0000'

export type IndexSourceRow = {
    partition: string
    key: string
    document: StoredDocument
}

export type IndexDefinition = {
    readonly table: string
    readonly name: string
    readonly partition: (row: IndexSourceRow) => string | undefined
    readonly key: (row: IndexSourceRow) => string | undefined
}

const registry = new Map<string, IndexDefinition[]>()

export function registerIndex(definition: IndexDefinition) {
    if (!/^[A-Za-z][\dA-Za-z-]*$/u.test(definition.name)) {
        throw new Error(`Invalid index name '${definition.name}'; use letters, digits and hyphens.`)
    }
    const definitions = registry.get(definition.table)
    if (definitions?.some(d => d.name === definition.name)) {
        throw new Error(
            `Index '${definition.name}' is already defined on table '${definition.table}'.`,
        )
    }
    if (definitions) {
        definitions.push(definition)
    } else {
        registry.set(definition.table, [definition])
    }
}

export function indexTable(definition: Pick<IndexDefinition, 'table' | 'name'>) {
    return `${definition.table}.${definition.name}`
}

export function hasIndexes(table: string) {
    return registry.has(table)
}

export type IndexEntry = { table: string; partition: string; key: string }

export function indexEntriesOf(
    table: string,
    partition: string,
    key: string,
    document: StoredDocument,
): IndexEntry[] {
    const definitions = registry.get(table)
    if (!definitions) {
        return []
    }
    assertClean(partition, `partition of a row in indexed table '${table}'`)
    assertClean(key, `key of a row in indexed table '${table}'`)
    const row = { partition, key, document }
    const entries: IndexEntry[] = []
    for (const definition of definitions) {
        const partitionValue = definition.partition(row)
        if (partitionValue === undefined) {
            continue
        }
        const keyValue = definition.key(row)
        if (keyValue === undefined) {
            continue
        }
        assertClean(partitionValue, `partition computed for index '${definition.name}'`)
        assertClean(keyValue, `key computed for index '${definition.name}'`)
        entries.push({
            table: indexTable(definition),
            partition: partitionValue,
            key: keyValue + indexKeyDelimiter + partition + indexKeyDelimiter + key,
        })
    }
    return entries
}

export function assertClean(value: string, what: string) {
    if (value.includes(indexKeyDelimiter)) {
        throw new Error(`The ${what} must not contain the reserved character u+0000.`)
    }
}

function putItems(
    entries: IndexEntry[],
    document: StoredDocument,
    newRevision: Revision,
): TransactionItem[] {
    return entries.map(entry => ({
        op: 'put',
        table: entry.table,
        partition: entry.partition,
        key: entry.key,
        document,
        newRevision,
    }))
}

function clearItems(oldEntries: IndexEntry[], keep: IndexEntry[]): TransactionItem[] {
    const kept = new Set(keep.map(entryId))
    return oldEntries
        .filter(entry => !kept.has(entryId(entry)))
        .map(entry => ({
            op: 'clear',
            table: entry.table,
            partition: entry.partition,
            key: entry.key,
        }))
}

function entryId(entry: IndexEntry) {
    return JSON.stringify([entry.table, entry.partition, entry.key])
}

export async function addWithIndexes(
    c: Connection,
    table: string,
    partition: string,
    key: string,
    document: StoredDocument,
): Promise<Revision> {
    if (!hasIndexes(table)) {
        return await c.add(table, partition, key, document)
    }
    const newRevision: Revision = randomUUID()
    await c.transact([
        { op: 'add', table, partition, key, document, newRevision },
        ...putItems(indexEntriesOf(table, partition, key, document), document, newRevision),
    ])
    return newRevision
}

export async function updateWithIndexes(
    c: Connection,
    table: string,
    partition: string,
    key: string,
    revision: Revision,
    document: StoredDocument,
    oldEntries?: IndexEntry[],
): Promise<Revision> {
    if (!hasIndexes(table)) {
        return await c.update(table, partition, key, revision, document)
    }
    const old =
        oldEntries ??
        indexEntriesOf(
            table,
            partition,
            key,
            (await existingRow(c, table, partition, key, revision)).document,
        )
    const newEntries = indexEntriesOf(table, partition, key, document)
    const newRevision: Revision = randomUUID()
    await c.transact([
        { op: 'update', table, partition, key, revision, document, newRevision },
        ...putItems(newEntries, document, newRevision),
        ...clearItems(old, newEntries),
    ])
    return newRevision
}

export async function deleteWithIndexes(
    c: Connection,
    table: string,
    partition: string,
    key: string,
    revision: Revision,
): Promise<void> {
    if (!hasIndexes(table)) {
        await c.delete(table, partition, key, revision)
        return
    }
    const old = await existingRow(c, table, partition, key, revision)
    await c.transact([
        { op: 'delete', table, partition, key, revision },
        ...clearItems(indexEntriesOf(table, partition, key, old.document), []),
    ])
}

export async function expandIndexOperations(
    c: Connection,
    items: TransactionItem[],
): Promise<TransactionItem[]> {
    if (!items.some(item => hasIndexes(item.table))) {
        return items
    }
    const expanded = await Promise.all(
        items.map(async (item): Promise<TransactionItem[]> => {
            if (!hasIndexes(item.table)) {
                return [item]
            }
            switch (item.op) {
                case 'add':
                    return [
                        item,
                        ...putItems(
                            indexEntriesOf(item.table, item.partition, item.key, item.document),
                            item.document,
                            item.newRevision,
                        ),
                    ]
                case 'update': {
                    const old = await existingRow(
                        c,
                        item.table,
                        item.partition,
                        item.key,
                        item.revision,
                    )
                    const oldEntries = indexEntriesOf(
                        item.table,
                        item.partition,
                        item.key,
                        old.document,
                    )
                    const newEntries = indexEntriesOf(
                        item.table,
                        item.partition,
                        item.key,
                        item.document,
                    )
                    return [
                        item,
                        ...putItems(newEntries, item.document, item.newRevision),
                        ...clearItems(oldEntries, newEntries),
                    ]
                }
                case 'delete': {
                    const old = await existingRow(
                        c,
                        item.table,
                        item.partition,
                        item.key,
                        item.revision,
                    )
                    return [
                        item,
                        ...clearItems(
                            indexEntriesOf(item.table, item.partition, item.key, old.document),
                            [],
                        ),
                    ]
                }
                default:
                    return [item]
            }
        }),
    )
    const flat = expanded.flat()
    if (flat.length > maxTransactionItems) {
        throw new Error(
            `Transaction cannot contain more than ${String(maxTransactionItems)} operations; ` +
                `${String(items.length)} requested operations expanded to ${String(flat.length)} including index maintenance.`,
        )
    }
    return flat
}

async function existingRow(
    c: Connection,
    table: string,
    partition: string,
    key: string,
    revision: Revision,
) {
    let row
    try {
        row = await c.get(table, partition, key)
    } catch (e) {
        if ((e as { status?: unknown }).status === 404) {
            throw conflict()
        }
        throw e
    }
    if (row.revision !== revision) {
        throw conflict()
    }
    return row
}

function conflict() {
    const e = new Error('Conflict')
    ;(e as unknown as { status: number }).status = 409
    return e
}
