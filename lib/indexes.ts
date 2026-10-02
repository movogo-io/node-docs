import { randomUUID } from 'node:crypto'
import type { Revision, StoredDocument } from '../schema.js'
import type { Connection, ReadOptions, TransactionItem, Written } from './driver.js'
import { conflict, isNotFound, transactionTooLarge } from './errors.js'
import { expiryOf, getUnexpired, isExpired, isoOf } from './expiry.js'
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
    readonly partition: (row: IndexSourceRow) => IndexValues
    readonly key: (row: IndexSourceRow) => IndexValues
}

// One value, several (an entry per partition × key pair), or none: a sparse
// index leaves out a row whose extractor answers `undefined` or `[]`.
export type IndexValues = string | readonly string[] | undefined

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
    return entriesOf(table, definitions, partition, key, document)
}

function entriesOf(
    table: string,
    definitions: readonly IndexDefinition[],
    partition: string,
    key: string,
    document: StoredDocument,
) {
    assertClean(partition, `partition of a row in indexed table '${table}'`)
    assertClean(key, `key of a row in indexed table '${table}'`)
    const row = { partition, key, document }
    // Keyed by entry, since a repeated value would be two operations on one
    // item, which a transaction refuses.
    const entries = new Map<string, IndexEntry>()
    for (const definition of definitions) {
        const partitionValues = valuesOf(definition.partition(row))
        if (partitionValues.length === 0) {
            continue
        }
        const keyValues = valuesOf(definition.key(row))
        for (const partitionValue of partitionValues) {
            assertClean(partitionValue, `partition computed for index '${definition.name}'`)
            for (const keyValue of keyValues) {
                assertClean(keyValue, `key computed for index '${definition.name}'`)
                const entry = {
                    table: indexTable(definition),
                    partition: partitionValue,
                    key: keyValue + indexKeyDelimiter + partition + indexKeyDelimiter + key,
                }
                entries.set(entryId(entry), entry)
            }
        }
    }
    return entries.values().toArray()
}

function valuesOf(values: IndexValues): readonly string[] {
    if (values === undefined) {
        return []
    }
    if (typeof values === 'string') {
        return [values]
    }
    return values
}

// Writes a row's entries in one index without writing the row: the backfill
// for an index declared after rows were written, which a rewrite would do at
// the price of a new revision, an audit entry and an announcement per row. The
// entries carry the row's revision and stored expiry, as a write gives them;
// the check makes a write landing in between a conflict, so its entries are
// never overwritten with older ones. Entries an earlier extractor wrote are
// not found here: they are keyed by values no current extractor produces.
export async function reindexRow(
    c: Connection,
    definition: IndexDefinition,
    partition: string,
    key: string,
    now: number,
) {
    const row = await storedRow(c, definition.table, partition, key)
    if (row === undefined || isExpired(row.expiresAt, now)) {
        return
    }
    const entries = entriesOf(definition.table, [definition], partition, key, row.document)
    if (entries.length === 0) {
        return
    }
    const check: TransactionItem = {
        op: 'check',
        table: definition.table,
        partition,
        key,
        revision: row.revision,
    }
    await c.transact(
        withinItemLimit(
            c,
            [check, ...putItems(entries, row.document, row.revision, row.expiresAt)],
            1,
        ),
        { now },
    )
}

async function storedRow(c: Connection, table: string, partition: string, key: string) {
    try {
        return await c.get(table, partition, key, { consistent: true })
    } catch (e) {
        if (isNotFound(e)) {
            return undefined
        }
        throw e
    }
}

export function assertClean(value: string, what: string) {
    if (value.includes(indexKeyDelimiter)) {
        throw new Error(`The ${what} must not contain the reserved character u+0000.`)
    }
}

type Add = Extract<TransactionItem, { op: 'add' }>
type Update = Extract<TransactionItem, { op: 'update' }>
type Delete = Extract<TransactionItem, { op: 'delete' }>

// A table without indexes is written through `add`, which answers what it
// stored. An indexed table is written in a transaction, which answers nothing:
// `seq` and `updatedAt` are then what the driver stores for a write over the
// row read beforehand, `replaced` when the caller read it, or a read here.
export async function addWithIndexes(
    c: Connection,
    table: string,
    partition: string,
    key: string,
    document: StoredDocument,
    now: number,
    replaced?: Replaced,
): Promise<Written> {
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
    const { entries, seq } = replaced ?? (await replacedByAdd(c, item))
    await c.transact(withinItemLimit(c, [item, ...addIndexItems(item, entries)], 1), { now })
    return { revision: item.newRevision, seq, updatedAt: isoOf(now) }
}

export async function updateWithIndexes(
    c: Connection,
    table: string,
    partition: string,
    key: string,
    revision: Revision,
    document: StoredDocument,
    now: number,
    replaced?: Replaced,
): Promise<Written> {
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
    const { entries, seq } =
        replaced ?? replacedOf(table, partition, key, await existingRow(c, item, now))
    await c.transact(withinItemLimit(c, [item, ...updateIndexItems(item, entries)], 1), { now })
    return { revision: item.newRevision, seq, updatedAt: isoOf(now) }
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
    await c.transact(withinItemLimit(c, [item, ...(await deleteIndexItems(c, item, now))], 1), {
        now,
    })
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
                    return [item, ...addIndexItems(item, (await replacedByAdd(c, item)).entries)]
                case 'update':
                    return [
                        item,
                        ...updateIndexItems(
                            item,
                            indexEntriesOf(
                                item.table,
                                item.partition,
                                item.key,
                                (await existingRow(c, item, now)).document,
                            ),
                        ),
                    ]
                case 'delete':
                    return [item, ...(await deleteIndexItems(c, item, now))]
                default:
                    return [item]
            }
        }),
    )
    return withinItemLimit(c, expanded.flat(), items.length)
}

function withinItemLimit(c: Connection, items: TransactionItem[], requestedCount: number) {
    const itemsMax = c.transactionItemsMax ?? maxTransactionItems
    if (itemsMax < items.length) {
        throw transactionTooLarge(
            `Transaction cannot contain more than ${String(itemsMax)} operations; ` +
                `${String(requestedCount)} requested operations expanded to ${String(items.length)} including index maintenance.`,
        )
    }
    return items
}

function addIndexItems(item: Add, oldEntries: IndexEntry[]): TransactionItem[] {
    const entries = indexEntriesOf(item.table, item.partition, item.key, item.document)
    return [
        ...putItems(entries, item.document, item.newRevision, item.expiresAt),
        ...clearItems(oldEntries, entries),
    ]
}

function updateIndexItems(item: Update, oldEntries: IndexEntry[]): TransactionItem[] {
    const entries = indexEntriesOf(item.table, item.partition, item.key, item.document)
    return [
        ...putItems(entries, item.document, item.newRevision, item.expiresAt),
        ...clearItems(oldEntries, entries),
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

// The row a write replaces, as read before the write: its index entries, and
// the `seq` the write stores over it. For an add that is the expired row under
// the key, or nothing; a live one makes the add conflict, so its entries are
// never cleared. A caller that lets a callback mutate the document in place
// takes this before the callback runs, or the entries to clear are computed
// from the document as it will be, and the ones it had are left behind.
export type Replaced = { entries: IndexEntry[]; seq: number }

export function replacedOf(
    table: string,
    partition: string,
    key: string,
    row: { document: StoredDocument; seq: number } | undefined,
): Replaced {
    if (!row) {
        return { entries: [], seq: 0 }
    }
    return { entries: indexEntriesOf(table, partition, key, row.document), seq: row.seq + 1 }
}

async function replacedByAdd(c: Connection, item: Add): Promise<Replaced> {
    try {
        const row = await c.get(item.table, item.partition, item.key)
        return replacedOf(item.table, item.partition, item.key, row)
    } catch (e) {
        if (isNotFound(e)) {
            return replacedOf(item.table, item.partition, item.key, undefined)
        }
        throw e
    }
}

// The row the write is about to replace, for the index entries it leaves
// behind. A revision other than the caller's is either a lost race or a stale
// read of a row this process wrote milliseconds earlier; only a consistent
// read tells which, so one is spent before the write is called a conflict.
async function existingRow(
    c: Connection,
    item: { table: string; partition: string; key: string; revision: Revision },
    now: number,
) {
    const row = await liveRow(c, item, now)
    if (row !== undefined && row.revision === item.revision) {
        return row
    }
    const fresh = await liveRow(c, item, now, { consistent: true })
    if (fresh !== undefined && fresh.revision === item.revision) {
        return fresh
    }
    throw conflict()
}

async function liveRow(
    c: Connection,
    item: { table: string; partition: string; key: string },
    now: number,
    options?: ReadOptions,
) {
    try {
        return await getUnexpired(c, item.table, item.partition, item.key, now, options)
    } catch (e) {
        if (isNotFound(e)) {
            return undefined
        }
        throw e
    }
}
