# Overview

This package provides **document**-based cloud **persistence** with **optimistic concurrency**.

## High-level

Documents are stored in tables with a partition key and sort key. A document can be retrieved by specifying a partition and key. When retrieving or adding a document, a revision is also returned. If the document needs to be updated or deleted, that revision needs to be provided. If the document has changed in the meantime, an conflict error will be thrown. Every row read also carries `seq`, a per-key write counter (0 on the first add, +1 per later write, an add over an expired row included; a delete removes the count, so a re-add of the key starts at 0 again), and `updatedAt`, the ISO instant of the last write at second precision from the clock of the context; `seq` orders the writes of a key, `updatedAt` only dates them.

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

Table names follow one convention across the platform: a table a platform package manages carries a leading underscore (`_idempotency` of `@movogo-io/idempotency`, `_sagas` of `@movogo-io/sagas`), and a dot introduces only a shadow table the store or a package maintains for a table — an index shadow `<Table>.<index>` or the audit shadow `<Table>._audit`. A service never declares a leading-underscore table itself, and never a name with a dot, so its own tables cannot collide with a package's, and an operator reading the console can tell them apart.

## Table Access

The schema is then used with the `tables` functions taking the @riddance/service context, like this:

```ts
import { tables } from "@movogo-io/docs";

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
    find: (key: string) => Promise<Row<Document> | undefined>;
    findEach: (keys: readonly string[]) => Promise<Row<Document>[]>;
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
type Row<Document> = {
    key: string;
    revision: Revision;
    document: Document;
    seq: number; // 0 on the add that creates the row; previous + 1 on every later write, an add over an expired row included; 0 again after a delete
    updatedAt: string; // ISO instant of the last write, second precision
};
```

`get` throws not found; `find` answers `undefined` instead, for the callers to whom absence is a normal case — an existence probe, an optional relation, an event handler for which a document that is gone is a state to skip. `get` stays the default: a `find` whose caller forgets the `undefined` branch becomes a 500, or an accidental default, far from the read.

Every read (`get`, `getDocument`, `find`, `findEach`, `getAll`, `getRange`) takes an optional trailing `{ consistent: true }`. It asks the driver for a read that sees every write acknowledged before it began; the in-memory driver is consistent by construction and ignores it, the DynamoDB driver sends a strongly consistent read, at twice the read cost. A plain read is the default and is right almost everywhere, because almost every stale answer is caught by something else: a write made on a stale row conflicts on its revision and is retried, a refusal decided on one is retried by the client, and a reader reached through an event or another service's request arrives after the replicas have converged. Ask for `{ consistent: true }` only where a stale answer is acted on and nothing checks it again: a lease claim or a fencing check; a sweep, right after a tombstone or fence write, of the rows committed just before it (the children of a parent being deleted, the members of a set being replaced), since a row the sweep misses is never visited again; and a decision inside a transaction to skip a row as absent or unchanged, which writes nothing, so no condition checks it. Never on an item or list read served to another service: its consumer, told of a change by an event, compares what it reads with the change and asks again while a replica lags, so a consistent read there doubles the cost of every call, every page of a reconcile scan included, for nothing. The retry helpers (`getOrAdd`, `addOrUpdate`, `converge` and their `Computed` forms) accept `consistent` in their options object, for the read they make before they write.

`findEach` resolves a list of keys. The result follows the order of `keys`, holds each distinct key at most once, and leaves out a key whose document is missing or expired — an index row can outlive the document it points at, and a list read from a lagging replica can still name one a delete has removed. A driver without a batch read pays one round trip per key and, on a cold connection, one TLS connection per read in flight — an unbounded `Promise.all` over a tenant-sized list has failed a production request outright with `getaddrinfo EBUSY` — so the store then reads at most 16 at a time. So a service neither writes the chunked loop itself nor the `for … await get` ladder it replaces; it calls `findEach`, and a driver with a batch read serves the whole list in as few calls as its batch limit allows. On `withKey` sets, `find` and `findEach` take partitions, like `get` does.

Each table also exposes `getPartitions()`, an async iterable of the partition keys currently holding documents (expired ones included until the driver removes them). In DynamoDB that is a full-table scan whose cost grows with everything ever stored, so reserve it for sweeps and migrations and never call it from a request handler.

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

You may then not need to export the `Schema` type. Where absence has a default, `find` says so in one line:

```ts
async function getUserProfile(context: object, userId: string) {
    const row = await userProfiles(context).find(userId);
    return row?.document ?? { ...defaultProfiles };
}
```

`isNotFound` and `isConflict` identify the store's errors where a `try` is still needed. A conflict is not handled with a `try`, though: it is retried.

Not-found and conflict errors carry `statusCode` 404 and 409, so one that escapes a @riddance/service handler — typically a conflict once the retries of a helper or `withTransaction` are spent — is answered 404 or 409, not 500. `isNotFound` and `isConflict` identify errors raised by the store, not any 404 or 409: an error carrying only `statusCode`, such as one from `@riddance/service`, does not match, so a domain conflict thrown inside a retrying helper is not retried and not mistaken for contention.

Range bounds are inclusive for `after` and exclusive for `before`, and an empty string is a bound like any other: `{ after: '' }` matches every key and `{ before: '' }` none.

`getOrAdd`, `addOrUpdate`, `converge` on `DocumentSet` are helper functions that manage concurrency issues by retrying conflict errors, as `retryConflict` does. Their `document` argument is added if it doesn't exist, `update` is called if it does exist, and `target` determines if the document needs updating. The `update` callback may either mutate the existing document in place, or return a replacement document; when it returns a document, that document is persisted instead of the existing one. That makes whole-document replacement (PUT semantics) a one-liner:

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

## Retrying conflicts

A read-modify-write that the helpers above cannot express, because it decides on the document before writing it (a state transition that refuses, a write whose revision a client sent), goes through `retryConflict`:

```ts
import { retryConflict } from "@movogo-io/docs";

const written = await retryConflict(async () => {
    const row = await rentals(context, supplierId).get(rentalId);
    if (row.document.status !== "active") {
        throw fail("rental.not_active");
    }
    const next = { ...row.document, status: "completed", completedAt: context.now().toISOString() };
    return await rentals(context, supplierId).update(rentalId, row.revision, next);
});
```

`retryConflict(fn, options?)` runs `fn` again when it throws a conflict of the store, after a jittered delay, and rethrows anything else at once. `retries` (default 3) caps the reruns, so sustained contention ends in the conflict, which a handler answers as a 409 the client retries, instead of a livelock; `delay` (default 250 ms, jittered between half and one and a half times) spaces them; `signal` aborts the wait. Use the defaults: they are the ones the helpers and `withTransaction` retry with, so one service does not contend differently in one handler than in the next.

- **Read inside `fn`, and decide inside it.** A revision read before the call fails identically on every rerun, and a state or ownership check made on a row read outside it passes on a document the rerun no longer writes against.
- **Mint ids and timestamps inside `fn`,** and when `fn` writes several documents, write first the one most likely to conflict. An id minted outside, with a later write losing the race, makes the rerun conflict with its own previous attempt's first write.
- **`fn` must be safe to run again:** no emit, no outbound call, nothing but reads and store writes. Do those after `retryConflict` returns.
- Only the store's conflicts are retried. A refusal carrying `statusCode: 409` of its own, a domain conflict or a package's (`queue.not_dead_lettered`), is a verdict on the document and is thrown at once; retrying it would only repeat it.
- Several documents that must change together are one `withTransaction`, which retries the whole callback the same way, not several writes inside one `retryConflict`.

## Transactions

`withTransaction` applies writes to multiple documents — across partitions and tables — atomically: either all of them are applied, or none of them are. Use it when several tables express different access patterns over the same data (e.g. a main table plus a lookup table) and a partial write would corrupt the invariant between them.

```ts
import { withTransaction } from "@movogo-io/docs";

await withTransaction<Schema>(context, async (tx) => {
    const row = await tx.Outbox.partition(userId).get(messageId);
    await tx.Outbox.partition(userId).delete(messageId, row.revision);
    await tx.Sent.partition(userId).add(messageId, row.document);
});
```

The `tx` argument mirrors the `tables` surface with these rules:

- **Writes are buffered.** `add`, `update`, `updateRow`, `check`, and `delete` do not touch storage when called; they are queued and applied atomically when the callback resolves. Returned revisions are final and usable after the commit.
- **Reads return committed state.** `get`, `getDocument`, `find`, `findEach`, `getAll`, and `getRange` pass through to storage — you cannot read your own buffered writes. `getPartitions` is not available inside a transaction. They take the same trailing `{ consistent: true }` as outside a transaction. The transaction's own conditions already catch a stale revision, which only costs a retry; the option is for a row the callback skips as absent or unchanged, which no condition checks.
- **The whole callback retries on conflict** (default 3 retries with jittered delay; pass `{ retries: 0 }` as the third argument to disable). The callback must therefore be safe to re-run: no side effects other than the buffered writes.
- **At most 100 operations, and at most one operation per document.** Two operations on the same document — including delete-then-add — reject immediately and are not retried. Index maintenance and audit entries count toward the 100.
- **At most 4 MB in all.** DynamoDB refuses a larger transaction, and refuses it again on every retry, so work queued behind it never finishes. Every index entry and every audit entry is a copy of the document, so a batch of large documents reaches 4 MB long before 100 operations. The memory drivers refuse both limits, so a test sees what production would; a service that packs writes into batches fills each one up to both, with the exported `maxTransactionItems` and `maxTransactionBytes` from `@movogo-io/docs/driver`, instead of a fixed count.
- **`check(key, revision)`** asserts a document still has the given revision without writing to it, e.g. "the parent still looks like it did when I read it" while writing a child.
- The retry helpers (`getOrAdd`, `addOrUpdate`, `converge`) are not available inside a transaction; the whole-transaction retry replaces them.
- If the callback throws, nothing is written.
- Do not nest `withTransaction` calls: the inner transaction commits independently, and outer retries would re-run it.

Transactional writes cost roughly twice as much as plain writes, so don't reach for `withTransaction` when writing a single document.

## Secondary Indexes

A table can declare additional access patterns as **secondary indexes**: alternative partition/sort-key pairs computed from each row. The store maintains index entries atomically with every write — replacing the pattern of hand-maintaining a separate lookup table and a transaction to keep the two in step.

Declare indexes once, next to the schema in `./lib/schema.ts`, through the `docs` handle:

```ts
import { docs } from "@movogo-io/docs/indexed";

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

Both extractors receive `{ partition, key, document }` of the row being written and return a string, or `undefined` to omit the row from that index (a **sparse** index — ideal for due/deadline sweeps). The value returned by `schema.index` is the typed accessor, for reads and for `reindex`:

```ts
// Open-string index partitions:
const row = await rentalsByUnit(context).partition(unitId).first(rentalId);
// row: { key, revision, document, source: { partition, key } } | undefined

// Literal-union index partitions become named accessors:
for await (const r of rentalsDue(context).pending.getRange({ before: today })) {
    // r.key is the due date; r.document is the full rental
}
```

An extractor may also return an array: the row then gets an entry for every partition and key pair, so a document that belongs to several things is found under each of them, and `[]` omits it like `undefined`:

```ts
// Find equipment by the groups it belongs to.
export const equipmentByGroup = schema.index(
    "Equipment",
    "byGroup",
    (r) => r.document.groupIds.map((groupId) => JSON.stringify([r.partition, groupId])), // one partition per group
    (r) => r.key,
);

for await (const r of equipmentByGroup(context).partition(JSON.stringify([supplierId, groupId])).getRange({ after })) {
    // one page of the group's equipment
}
```

Rules and properties:

- **Declare before the first write.** Writes consult the registered definitions, so `schema.index` calls must run before the table is written to — automatic when declarations live in `./lib/schema.ts` beside the accessor helpers.
- **Maintenance is atomic.** Every write to an indexed table (including the retry helpers and writes inside `withTransaction`) commits the document and its index entries in one transaction; a document can never be observed missing from, or stale in, an index. Reads are as consistent as the main table.
- **Index rows carry the document and its revision, but not `seq` or `updatedAt`.** A row read through an index can be updated directly: `rentals(context, row.source.partition).update(row.source.key, row.revision, …)`. An entry keeps its own write counter, which is not the source document's, so neither is exposed; read the document itself for its `seq`.
- **Index keys are not unique.** Several rows may share one, so the point lookups are `first(key)` and `firstDocument(key)`: they return the first match in key order, or `undefined` when nothing matches. `getRange` accepts the same ranges as `getRange` on a table. Both, and `firstDocument`, take the trailing `{ consistent: true }` of a table read.
- **The character `"\u0000"` is reserved** in partitions, keys, and extractor results of indexed tables.
- **A multi-valued membership goes in the partition, the member's key in the key.** A range is either `withPrefix` or `after`/`before`, never both, so values encoded in the key cannot be paged with `after` within one value; a value in the partition pages with a plain `after`. Repeated values give one entry. A range spanning several values of one row returns that row once per value: count documents by `source`, not by rows.
- **Extractors must handle every document shape ever stored.** A write computes the entries of the document it replaces, including an expired one under the same key, so an extractor that throws on a legacy shape makes `add`, `update`, and `delete` of that key fail.
- **Cost:** every write to an indexed table is a transaction of the document plus one item per index entry it adds, replaces, or removes, and each entry stores a copy of the document. Every write puts every entry again, not only the ones that changed, so a document with ten entries writes eleven copies of itself on a change to any field, twelve on an audited table. A single write is refused before it reaches the driver when its entries take it past 100 operations, and the 4 MB and 100-operation limits of a transaction (see Transactions) arrive sooner the more entries a document has. An `add`, `update`, or `delete` additionally reads the current document first, to find the entries to remove, including those an expired document left behind, unless it runs through a retry helper that has already read it. That read is eventual; a revision other than the caller's is read again consistently before the write is called a conflict, so a row this process updated milliseconds earlier does not fail its own next update on a stale replica. Index maintenance operations also count toward the 100-operation transaction budget inside `withTransaction`.
- **Backfill with `reindex`, not by rewriting.** Entries are only written when their document is written, so rows written before an index was declared, or before its extractor changed, are missing from it. `index(context).reindex(sourcePartition, sourceKey)` writes one row's entries in that index and leaves the row alone: the entries carry the row's revision and stored expiry, a write landing in between makes it retry, and an absent or expired row writes nothing. A rewrite would do the same at the price of a new revision (stale `If-Match` for every client), an audit entry and a `document.changed` per row. Sweep the table with `getAll` and call `reindex` per row; it is idempotent, so the sweep can be re-run.
- **A changed extractor needs a new index name.** Cleanup computes old entries with the *current* extractors, and so does `reindex`, so entries an old extractor wrote are keyed by values nothing computes any more and are never cleared. Give the changed index a new name, backfill it, and drop the old shadow table, named `<Table>.<indexName>`.

## Expiry

A table can declare that its documents **expire**: a point in time computed from each document after which the document is gone. Use it for holds, one-time codes, OAuth states, idempotency records, and other rows that are only meaningful for a while — a table declaring expiry needs no sweeper.

Declare expiry once, next to the schema in `./lib/schema.ts`, through the `docs` handle:

```ts
import { docs } from "@movogo-io/docs/indexed";

const schema = docs<Schema>();

// Documents are stored as JSON, so a date field comes back as a string or a
// number: always construct the Date in the extractor, and return undefined
// for documents that do not expire — including legacy ones lacking the field.
schema.expiry("Holds", (hold) => {
    if (hold.expiresAt === undefined) {
        return undefined;
    }
    return new Date(hold.expiresAt);
});
```

The extractor receives the typed document and returns a `Date`, or `undefined` for a document that does not expire. It runs on every write, and only on writes, so it must be pure and must not throw. Any other result — including an invalid `Date` — rejects the write, so a broken extractor is caught by the service's own tests under the memory driver.

Rules and properties:

- **At most one expiry per table, declared before the first write.** A second declaration for the same table throws. Writes consult the declaration, so `schema.expiry` must run before the table is written to — automatic when declarations live in `./lib/schema.ts` beside the accessor helpers.
- **Expiry is second-granular.** A document is expired from the start of the second containing its expiry instant.
- **An expired document is invisible to every operation, under every driver.** `get` and `getDocument` throw not found; `find` answers `undefined`; `findEach`, `getAll`, `getRange`, and index lookups skip it; reads inside `withTransaction` behave the same; `add` replaces it as if it were absent, so `getOrAdd`, `addOrUpdate`, and `converge` add a fresh document over an expired one; `update`, `delete`, and `check` conflict, like on a deleted document: re-read, find nothing, add fresh.
- **Drivers remove expired rows lazily.** The store hands every driver the expiry explicitly; the DynamoDB driver stores it in the table's time-to-live attribute and leaves the deletion to DynamoDB. Nothing may rely on an expired row still existing, and nothing needs to remove it — though `getPartitions()` may list partitions whose only documents are expired but not yet removed.
- **The clock comes from the context.** When the context has a `now()` function — every @riddance/service context does — the store reads the time from it, so tests can move the clock instead of sleeping. Without one the wall clock is used. A `now()` returning an invalid `Date` fails the operation.
- **The stored expiry is what counts.** Reads never run the extractor: a document written before the declaration, or under a previous extractor, keeps its stored expiry (or none) until it is rewritten, so it stays visible and never expires until an update stores a new expiry. Changing an extractor therefore needs a backfill that rewrites each row, exactly like changing an index.
- **Expiry needs a driver implementing the 0.2 connection contract.** An older driver ignores it, so rows never expire in storage and adds over expired rows conflict.

## Driver Decoration

Extension packages (auditing, tracing, metrics) can wrap the active driver with `decorateDriver` from `@movogo-io/docs/driver`:

```ts
import { decorateDriver, type Driver } from '@movogo-io/docs/driver';

decorateDriver((driver: Driver) => wrapped(driver));
```

Decorators are applied lazily whenever the driver is used, regardless of the order of `decorateDriver` and `setDriver` calls, and survive driver replacement. The last-registered decorator becomes the outermost wrapper. `decorateDriver` returns a function that removes the decorator again. This is a plumbing API for infrastructure packages — services should not need it. Decorators that append operations to `transact` calls can preflight against the exported `maxTransactionItems` and `maxTransactionBytes` budgets. `getMany` is optional on a connection: a decorator that lists the methods it forwards and leaves it out turns every `findEach` beneath it into single reads, 16 at a time, which is correct but forfeits the driver's batch read — forward it when the wrapped connection has one.

A decorator must forward the trailing read options of `get`, `getMany` and `getPartition` to the wrapped connection; one that drops them makes every read beneath it silently lose the consistency it asked for.

The driver, its decorators and the index and expiry registries are module state, so one process must hold one copy of this package: the first copy loaded claims `globalThis[Symbol.for('@movogo-io/docs')]`, and a second copy refuses to load with an error naming both versions and paths, which turns a nested duplicate under a mis-pinned package into a failing test run instead of writes that silently bypass auditing and indexes.

## Test drivers

`@movogo-io/docs/test/memory` ships in-memory drivers for tests, all implementing the connection contract and refusing a transaction DynamoDB would refuse (more than 100 operations, two on one document, or more than 4 MB): `MemoryDriver` keeps one store per context, `PersistentMemoryDriver` one store per driver instance with a closable connection per `connect()`, as DynamoDB connections are, and `DelayedPersistentMemoryDriver` the same with a random sub-millisecond delay on every operation, so interleavings a synchronous store would never produce do show up. `LaggingPersistentMemoryDriver` models eventual consistency deterministically: the first read of a row without `{ consistent: true }` after a write to it sees the row as it was before that write (present, absent or expired), every later read and every consistent read sees the write. Run the flows that read back what they just wrote under it, so a read that needed `{ consistent: true }` fails in the test run rather than once a second on DynamoDB. A plain read that follows a write in the same test lags under it by design: the test acts on the revision its write answered, as a client does, and verifies through a read that is consistent for its own reasons, such as the audit trail. Never add `{ consistent: true }` to a handler to make such a test pass: that is how a service ends up paying for consistent reads everywhere. `@movogo-io/docs/test/mock` is the mocha root hook that installs a fresh `PersistentMemoryDriver` before each test.
