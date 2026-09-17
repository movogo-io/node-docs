import { randomUUID } from 'node:crypto'
import type { Revision, StoredDocument } from '../schema.js'
import type { Connection, TransactionItem } from './driver.js'
import { conflict, isNotFound } from './errors.js'
import { expiryOf, getUnexpired } from './expiry.js'
import { maxTransactionItems } from './transaction.js'

// Separates the index key value from the owning row's partition and key in the
// physical key of an index entry. Being the smallest possible character, it is
// the only delimiter that keeps prefix and before/after range semantics on the
// physical keys identical to those on the undecorated index key values.
export const indexKeyDelimiter = '\u{0}'

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

type Add = Extract<TransactionItem, { op: 'add' }>
type Update = Extract<TransactionItem, { op: 'update' }>
type Delete = Extract<TransactionItem, { op: 'delete' }>

export async function addWithIndexes(
    c: Connection,
    table: string,
    partition: string,
    key: string,
    document: StoredDocument,
    now: number,
    leftover?: IndexEntry[],
): Promise<Revision> {
    const expiry = expiryOf(table, document)
    if (!hasIndexes(table)) {
        return await c.add(table, partition, key, document, { now, ...expiry })
    }
    const item: Add = {
        op: 'add',
        table,
        partition,
        key,
        document,
        newRevision: randomUUID(),
        ...expiry,
    }
    await c.transact([item, ...(await addIndexItems(c, item, leftover))], { now })
    return item.newRevision
}

export async function updateWithIndexes(
    c: Connection,
    table: string,
    partition: string,
    key: string,
    revision: Revision,
    document: StoredDocument,
    now: number,
    oldEntries?: IndexEntry[],
): Promise<Revision> {
    const expiry = expiryOf(table, document)
    if (!hasIndexes(table)) {
        return await c.update(table, partition, key, revision, document, { now, ...expiry })
    }
    const item: Update = {
        op: 'update',
        table,
        partition,
        key,
        revision,
        document,
        newRevision: randomUUID(),
        ...expiry,
    }
    await c.transact([item, ...(await updateIndexItems(c, item, now, oldEntries))], { now })
    return item.newRevision
}

export async function deleteWithIndexes(
    c: Connection,
    table: string,
    partition: string,
    key: string,
    revision: Revision,
    now: number,
): Promise<void> {
    if (!hasIndexes(table)) {
        await c.delete(table, partition, key, revision, { now })
        return
    }
    const item: Delete = { op: 'delete', table, partition, key, revision }
    await c.transact([item, ...(await deleteIndexItems(c, item, now))], { now })
}

export async function expandIndexOperations(
    c: Connection,
    items: TransactionItem[],
    now: number,
): Promise<TransactionItem[]> {
    if (items.every(item => !hasIndexes(item.table))) {
        return items
    }
    const expanded = await Promise.all(
        items.map(async (item): Promise<TransactionItem[]> => {
            if (!hasIndexes(item.table)) {
                return [item]
            }
            switch (item.op) {
                case 'add':
                    return [item, ...(await addIndexItems(c, item))]
                case 'update':
                    return [item, ...(await updateIndexItems(c, item, now))]
                case 'delete':
                    return [item, ...(await deleteIndexItems(c, item, now))]
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

async function addIndexItems(
    c: Connection,
    item: Add,
    leftover?: IndexEntry[],
): Promise<TransactionItem[]> {
    const entries = indexEntriesOf(item.table, item.partition, item.key, item.document)
    return [
        ...putItems(entries, item.document, item.newRevision, item.expiresAt),
        ...clearItems(leftover ?? (await leftoverEntries(c, item)), entries),
    ]
}

async function updateIndexItems(
    c: Connection,
    item: Update,
    now: number,
    oldEntries?: IndexEntry[],
): Promise<TransactionItem[]> {
    const old =
        oldEntries ??
        indexEntriesOf(
            item.table,
            item.partition,
            item.key,
            (await existingRow(c, item, now)).document,
        )
    const entries = indexEntriesOf(item.table, item.partition, item.key, item.document)
    return [
        ...putItems(entries, item.document, item.newRevision, item.expiresAt),
        ...clearItems(old, entries),
    ]
}

async function deleteIndexItems(
    c: Connection,
    item: Delete,
    now: number,
): Promise<TransactionItem[]> {
    const old = await existingRow(c, item, now)
    return clearItems(indexEntriesOf(item.table, item.partition, item.key, old.document), [])
}

function putItems(
    entries: IndexEntry[],
    document: StoredDocument,
    newRevision: Revision,
    expiresAt: number | undefined,
): TransactionItem[] {
    return entries.map(entry => ({
        op: 'put',
        table: entry.table,
        partition: entry.partition,
        key: entry.key,
        document,
        newRevision,
        ...(expiresAt !== undefined && { expiresAt }),
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

// The entries an expired document under the same key left behind. A live one
// makes the add conflict, so its entries are never cleared.
export function leftoverEntriesOf(
    table: string,
    partition: string,
    key: string,
    expired: { document: StoredDocument } | undefined,
) {
    if (!expired) {
        return []
    }
    return indexEntriesOf(table, partition, key, expired.document)
}

async function leftoverEntries(c: Connection, item: Add) {
    try {
        const { document } = await c.get(item.table, item.partition, item.key)
        return indexEntriesOf(item.table, item.partition, item.key, document)
    } catch (e) {
        if (isNotFound(e)) {
            return []
        }
        throw e
    }
}

async function existingRow(
    c: Connection,
    item: { table: string; partition: string; key: string; revision: Revision },
    now: number,
) {
    try {
        const row = await getUnexpired(c, item.table, item.partition, item.key, now)
        if (row.revision === item.revision) {
            return row
        }
    } catch (e) {
        if (!isNotFound(e)) {
            throw e
        }
    }
    throw conflict()
}
