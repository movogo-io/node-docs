import { randomUUID } from 'node:crypto'
import type { Revision, StoredDocument } from '../schema.js'
import {
    declaredBound,
    isExtended,
    prepareExtensions,
    type Connection,
    type ExtendedWrite,
    type ReadOptions,
    type TransactionItem,
    type Written,
} from './driver.js'
import { conflict, isNotFound, transactionTooLarge } from './errors.js'
import { expiryOf, getUnexpired, inFlightMax, isExpired, isoOf } from './expiry.js'
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

// A table with indexes, or one a write extension applies to, is written in a
// transaction, which carries the items they add.
function transactional(table: string) {
    return hasIndexes(table) || isExtended(table)
}

// A table written plainly goes through `add`, which answers what it stored,
// and so does a table whose extensions add nothing to the write, when the
// driver stores the revision the extensions were handed. A transaction answers
// nothing: `seq` and `updatedAt` are then what the driver stores for a write
// over the row read beforehand, `replaced` when the caller read it, or a read
// here.
export async function addWithIndexes(
    c: Connection,
    context: object,
    table: string,
    partition: string,
    key: string,
    document: StoredDocument,
    now: number,
    replaced?: Replaced,
): Promise<Written> {
    const expiry = expiryOf(table, document)
    if (!transactional(table)) {
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
    const extended = prepareExtensions(context, [item])
    if (writesPlainly(c, table, extended)) {
        const written = await c.add(table, partition, key, document, {
            now,
            ...expiry,
            newRevision: item.newRevision,
        })
        assertStoredAsHanded(written, item)
        extended.committed()
        return written
    }
    const { entries, seq } = replaced ?? (await replacedByAdd(c, item))
    await transactOne(c, item, addIndexItems(item, entries), extended, now)
    return { revision: item.newRevision, seq, updatedAt: isoOf(now) }
}

export async function updateWithIndexes(
    c: Connection,
    context: object,
    table: string,
    partition: string,
    key: string,
    revision: Revision,
    document: StoredDocument,
    now: number,
    replaced?: Replaced,
): Promise<Written> {
    const expiry = expiryOf(table, document)
    if (!transactional(table)) {
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
    const extended = prepareExtensions(context, [item])
    if (writesPlainly(c, table, extended)) {
        const written = await c.update(table, partition, key, revision, document, {
            now,
            ...expiry,
            newRevision: item.newRevision,
        })
        assertStoredAsHanded(written, item)
        extended.committed()
        return written
    }
    const { entries, seq } =
        replaced ?? replacedOf(table, partition, key, await existingRow(c, item, now))
    await transactOne(c, item, updateIndexItems(item, entries), extended, now)
    return { revision: item.newRevision, seq, updatedAt: isoOf(now) }
}

export async function deleteWithIndexes(
    c: Connection,
    context: object,
    table: string,
    partition: string,
    key: string,
    revision: Revision,
    now: number,
): Promise<void> {
    if (!transactional(table)) {
        await c.delete(table, partition, key, revision, { now })
        return
    }
    const item: Delete = { op: 'delete', table, partition, key, revision }
    const { document } = await existingRow(c, item, now)
    const extended = prepareExtensions(context, [{ ...item, document }])
    // A delete stores no revision, so any driver deletes plainly.
    if (extended.items.length === 0 && !hasIndexes(table)) {
        await c.delete(table, partition, key, revision, { now })
        extended.committed()
        return
    }
    await transactOne(c, item, deleteIndexItems(item, document), extended, now)
}

// An add or update the extensions add nothing to needs no transaction on a
// table without indexes, provided the driver stores the revision they were
// handed.
function writesPlainly(c: Connection, table: string, extended: Expanded) {
    return extended.items.length === 0 && !hasIndexes(table) && c.acceptsNewRevision === true
}

// The extensions were handed `newRevision` before the write; a driver, or a
// decorator, that declares `acceptsNewRevision` and stores another would have
// them report a revision that does not exist, so they are not told.
function assertStoredAsHanded(written: Written, item: Add | Update) {
    if (written.revision !== item.newRevision) {
        throw new Error(
            `A connection declares acceptsNewRevision but stored revision ${String(written.revision)} for ${item.table}, not the ${String(item.newRevision)} it was given.`,
        )
    }
}

// The extensions are told only after the transaction resolved, so they hear
// of a write exactly when it committed.
async function transactOne(
    c: Connection,
    item: TransactionItem,
    indexItems: TransactionItem[],
    extended: Expanded,
    now: number,
) {
    await c.transact(withinItemLimit(c, [item, ...indexItems, ...extended.items], 1), { now })
    extended.committed()
}

export type Expanded = { items: TransactionItem[]; committed: () => void }

// A transaction's buffered items with the index items they need and what the
// write extensions add, read a bounded number at a time, and the call that
// tells the extensions once it committed.
export async function expandWrites(
    c: Connection,
    context: object,
    items: TransactionItem[],
    now: number,
): Promise<Expanded> {
    if (items.every(item => !transactional(item.table))) {
        return { items, committed: () => undefined }
    }
    const burst = declaredBound(c, 'requestsInFlightMax', inFlightMax)
    const expanded: ExpandedItem[] = []
    for (let start = 0; start < items.length; start += burst) {
        expanded.push(
            ...(await Promise.all(
                items.slice(start, start + burst).map(item => expandItem(c, item, now)),
            )),
        )
    }
    const extended = prepareExtensions(
        context,
        expanded.flatMap(e => (e.write ? [e.write] : [])),
    )
    return {
        items: withinItemLimit(
            c,
            [...expanded.flatMap(e => e.items), ...extended.items],
            items.length,
        ),
        committed: extended.committed,
    }
}

type ExpandedItem = { items: TransactionItem[]; write?: ExtendedWrite }

// Only an indexed add or update reads the row it replaces, for the entries
// to clear; a delete reads it for an extension too, which is handed the
// document it removes.
async function expandItem(
    c: Connection,
    item: TransactionItem,
    now: number,
): Promise<ExpandedItem> {
    const extended = isExtended(item.table)
    switch (item.op) {
        case 'add':
            return {
                items: hasIndexes(item.table)
                    ? [item, ...addIndexItems(item, (await replacedByAdd(c, item)).entries)]
                    : [item],
                ...(extended && { write: item }),
            }
        case 'update':
            return {
                items: hasIndexes(item.table)
                    ? [
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
                    : [item],
                ...(extended && { write: item }),
            }
        case 'delete': {
            if (!extended && !hasIndexes(item.table)) {
                return { items: [item] }
            }
            const { document } = await existingRow(c, item, now)
            return {
                items: [item, ...deleteIndexItems(item, document)],
                ...(extended && { write: { ...item, document } }),
            }
        }
        default:
            return { items: [item] }
    }
}

function withinItemLimit(c: Connection, items: TransactionItem[], requestedCount: number) {
    const itemsMax = declaredBound(c, 'transactionItemsMax', maxTransactionItems)
    if (itemsMax < items.length) {
        throw transactionTooLarge(
            `Transaction cannot contain more than ${String(itemsMax)} operations; ` +
                `${String(requestedCount)} requested operations expanded to ${String(items.length)} including index maintenance and write extensions.`,
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

function deleteIndexItems(item: Delete, document: StoredDocument): TransactionItem[] {
    return clearItems(indexEntriesOf(item.table, item.partition, item.key, document), [])
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
