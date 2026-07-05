# Overview

This package provides **document**-based cloud **persistence** with **optimistic concurrency**.

## High-level

Documents are stored in tables with a partition key and sort key. A document can be retrieved by specifying a partition and key. When retrieving or adding a document, a revision is also returned. If the document needs to be updated or deleted, that revision needs to be provided. If the document has changed in the meantime, an conflict error will be thrown.

The use of this package is an **implementation detail**. **DO NOT** use it from tests. Only use the package's entry point from tests.

## Schema

Start by specify the schema for data used in your service, typically in `./lib/schema.ts`. A schema is a type with four levels: table name, partition, key, and finally the document.

```ts
type Schema = {
    // A table of one type of documents stored by arbitrary string as partition key (userId) and arbitrary string as sort key (messageId). messageId is likely prefixed with ISO timestamp to ensure chronological ordering.
    Conversations: {
        [userId: string]: {
            [messageId: string]: {
                timestamp: string;
                subject: string;
                body: string;
            };
        };
    };

    // A table with only two partitions: `settings` and `key` each with a set up documents stored by an arbitrary string sort key (companyId)
    Companies: {
        settings: {
            [companyId: string]: {
                website: string;
                count: number;
            };
        };
        keys: {
            [companyId: string]: {
                secret: string;
            };
        };
    };

    // A table of users, each stored in their own partition. Each partition has two documents with sort key `profile` and `invitations` respectively, each with their own document type.
    Users: {
        [id: string]: {
            profile: {
                name: string;
                email: string;
            };
            invitations: {
                id: string;
                scopes: string[];
            }[];
        };
    };
};
```

## Table Access

The schema is then used with the `tables` functions taking the @riddance/service context, like this:

```ts
import { tables } from "@riddance/docs";

// Arbitrary strings as partition and key
const userMessages = tables<Schema>(context).Conversations.partition(userId);

// Fixed set of partitions
const companySettings = tables<Schema>(context).Companies.settings;
const companyKeys = tables<Schema>(context).Companies.keys;

// Fixed set of document types stored by a fixed set of sort keys.
const userProfiles = getTables<Schema>(context).Users.withKey("profile");
const invitations = getTables<Schema>(context).Users.withKey("invitations");

// For reference, each of the above variables satisfy this type which is not exported. When coming from the `withKey` function, the `key` argument is actually the partition, since the key was already specified.
type DocumentSet<Document> = {
    add: (key: string, document: Document) => Promise<Revision>;
    get: (key: string) => Promise<Row<Document>>;
    getDocument: (key: string) => Promise<Document>;
    getAll: () => AsyncIterable<Row<Document>>;
    getRange: (
        range:
            | { withPrefix: string }
            | { before?: string; after: string }
            | { before: string; after?: string },
    ) => AsyncIterable<Row<Document>>;
    update: (key: string, revision: Revision, document: Document) => Promise<Revision>;
    updateRow: (row: Row<Document>) => Promise<Revision>;
    getOrAdd: (key: string, document: Document) => Promise<Row<Document>>;
    addOrUpdate: (
        key: string,
        document: Document,
        update: (existing: Document) => Document | void,
    ) => Promise<Row<Document>>;
    converge: (
        key: string,
        target: (document: Document) => boolean,
        document: Document,
        update: (existing: Document) => Document | void,
    ) => Promise<Row<Document>>;
    delete: (key: string, revision: Revision) => Promise<void>;
};
type Row<Document> = { key: string; revision: Revision; document: Document };
```

Consider adding helper functions to `./lib/schema.ts` like this

```ts
export function userMessages(context: object, userId: string) {
    return tables<Schema>(context).Conversations.partition(userId);
}
export function companySettings(context: object) {
    return tables<Schema>(context).Companies.settings;
}
export function companyKeys(context: object) {
    return tables<Schema>(context).Companies.keys;
}
export function userProfiles(context: object) {
    return tables<Schema>(context).Users.withKey("profile");
}
export function invitations(context: object) {
    return tables<Schema>(context).Users.withKey("invitations");
}
```

You may then not need to export the `Schema` type. To help deal with errors there are two utility types `isNotFound` and `isConflict`:

```ts
async function getUserProfile(context: object, userId: string) {
    try {
        return await userProfiles(context).getDocument(userId);
    } catch (e) {
        if (isNotFound(e)) {
            return {
                ...defaultProfiles,
            };
        }
        throw e;
    }
}

async function updateUserProfile(context: object, userId: string, newProfile, revision) {
    try {
        return await userProfiles(context).update(userId, revision, newProfile);
    } catch (e) {
        if (isConflict(e)) {
            // TODO: Retry
        }
        throw e;
    }
}
```

`getOrAdd`, `addOrUpdate`, `converge` on `DocumentSet` are helper functions that manages concurrency issues by retrying conflict errors. Their `document` argument is added if it doesn't exist, `update` is called if it does exist, and `target` determines if the document needs updating. The `update` callback may either mutate the existing document in place, or return a replacement document; when it returns a document, that document is persisted instead of the existing one. That makes whole-document replacement (PUT semantics) a one-liner:

```ts
documents.addOrUpdate(key, newDocument, () => newDocument);
```

You can e.g. make updates idempotent like this:

```ts
type Document = { processedMessages: string[]; count: number };
documents.converge(
    key,
    /*target*/ (doc) => doc.processedMessages.includes(messageId),
    /*initial document*/ { processedMessages: [messageId], count: 1 },
    /*update*/ (doc) => {
        if (doc.processedMessages.length === 8) {
            // only keep recently processed messages
            doc.processedMessages.shift();
        }
        doc.processedMessages.push(messageId);
        doc.count += 1;
    },
);
```

## Transactions

`withTransaction` applies writes to multiple documents — across partitions and tables — atomically: either all of them are applied, or none of them are. Use it when several tables express different access patterns over the same data (e.g. a main table plus a lookup table) and a partial write would corrupt the invariant between them.

```ts
import { withTransaction } from "@riddance/docs";

await withTransaction<Schema>(context, async (tx) => {
    const row = await tx.Outbox.partition(userId).get(messageId);
    await tx.Outbox.partition(userId).delete(messageId, row.revision);
    await tx.Sent.partition(userId).add(messageId, row.document);
});
```

The `tx` argument mirrors the `tables` surface with these rules:

- **Writes are buffered.** `add`, `update`, `updateRow`, `check`, and `delete` do not touch storage when called; they are queued and applied atomically when the callback resolves. Returned revisions are final and usable after the commit.
- **Reads return committed state.** `get`, `getDocument`, `getAll`, `getRange`, and `getPartitions` pass through to storage — you cannot read your own buffered writes.
- **The whole callback retries on conflict** (default 3 retries with jittered delay; pass `{ retries: 0 }` as the third argument to disable). The callback must therefore be safe to re-run: no side effects other than the buffered writes.
- **At most 100 operations, and at most one operation per document.** Two operations on the same document — including delete-then-add — throw immediately and are not retried.
- **`check(key, revision)`** asserts a document still has the given revision without writing to it, e.g. "the parent still looks like it did when I read it" while writing a child.
- The retry helpers (`getOrAdd`, `addOrUpdate`, `converge`) are not available inside a transaction; the whole-transaction retry replaces them.
- If the callback throws, nothing is written.
- Do not nest `withTransaction` calls: the inner transaction commits independently, and outer retries would re-run it.

Transactional writes cost roughly twice as much as plain writes, so don't reach for `withTransaction` when writing a single document.

## Secondary Indexes

A table can declare additional access patterns as **secondary indexes**: alternative partition/sort-key pairs computed from each row. The store maintains index entries atomically with every write — replacing the pattern of hand-maintaining a separate lookup table and a transaction to keep the two in step.

Declare indexes once, next to the schema in `./lib/schema.ts`, through the `docs` handle:

```ts
import { docs } from "@riddance/docs/indexed";

const schema = docs<Schema>();

// Find rentals by the unit they concern, regardless of supplier partition.
export const rentalsByUnit = schema.index(
    "Rentals",
    "byUnit",
    (r) => r.document.unitId, // index partition
    (r) => r.key, // index sort key
);

// Sweep pending rentals by due date. Documents without a due date are omitted.
export const rentalsDue = schema.index(
    "Rentals",
    "byStatus",
    (r) => r.document.status, // 'pending' | 'active' — gives named accessors
    (r) => r.document.due, // string | undefined — undefined ⇒ sparse
);

export function rentals(context: object, supplierId: string) {
    return schema.tables(context).Rentals.partition(supplierId);
}
```

Both extractors receive `{ partition, key, document }` of the row being written and return a string, or `undefined` to omit the row from that index (a **sparse** index — ideal for due/deadline sweeps). The value returned by `schema.index` is the typed, **read-only** accessor:

```ts
// Open-string index partitions:
const row = await rentalsByUnit(context).partition(unitId).get(rentalId);
// row: { key, revision, document, source: { partition, key } } | undefined

// Literal-union index partitions become named accessors:
for await (const r of rentalsDue(context).pending.getRange({ before: today })) {
    // r.key is the due date; r.document is the full rental
}
```

Rules and properties:

- **Declare before the first write.** Writes consult the registered definitions, so `schema.index` calls must run before the table is written to — automatic when declarations live in `./lib/schema.ts` beside the accessor helpers.
- **Maintenance is atomic.** Every write to an indexed table (including the retry helpers and writes inside `withTransaction`) commits the document and its index entries in one transaction; a document can never be observed missing from, or stale in, an index. Reads are as consistent as the main table.
- **Index rows carry the document and its revision.** A row read through an index can be updated directly: `rentals(context, row.source.partition).update(row.source.key, row.revision, …)`.
- `get` returns `undefined` when nothing matches (several rows may share an index key; `get` returns the first in key order). `getRange` accepts the same ranges as `getRange` on a table.
- **The character `"\u0000"` is reserved** in partitions, keys, and extractor results of indexed tables.
- **Cost:** writes to an indexed table are transactional (roughly 2× write cost) and each index entry stores a copy of the document. Index maintenance operations also count toward the 100-operation transaction budget inside `withTransaction`.
- **Changing an index definition needs a backfill.** Entries are only rewritten when their document is written, and cleanup computes old entries with the *current* extractors — after changing extractors, sweep the table and rewrite each row (and clear the index's old shadow table, named `<Table>.<indexName>`).
