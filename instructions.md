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
