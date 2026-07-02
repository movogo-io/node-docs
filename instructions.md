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
        update: (existing: Document) => void,
    ) => Promise<Row<Document>>;
    converge: (
        key: string,
        target: (document: Document) => boolean,
        document: Document,
        update: (existing: Document) => void,
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

`getOrAdd`, `addOrUpdate`, `converge` on `DocumentSet` are helper functions that manages concurrency issues by retrying conflict errors. Their `document` argument is added if it doesn't exist, `update` is called to mutate the document if it does exist, and `target` determines if the document needs updating. You can e.g. make updates idempotent like this:

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
