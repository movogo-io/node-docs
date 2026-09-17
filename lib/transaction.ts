import { randomUUID } from 'node:crypto'
import type { Revision, StoredDocument } from '../schema.js'
import type { TransactionItem } from './driver.js'

export const maxTransactionItems = 100

export class TransactionBuffer {
    readonly #items: TransactionItem[] = []
    readonly #touched = new Set<string>()
    #sealed = false

    add(table: string, partition: string, key: string, document: StoredDocument) {
        const newRevision: Revision = randomUUID()
        return this.#enqueue(
            { op: 'add', table, partition, key, document, newRevision },
            newRevision,
        )
    }

    update(
        table: string,
        partition: string,
        key: string,
        revision: Revision,
        document: StoredDocument,
    ) {
        const newRevision: Revision = randomUUID()
        return this.#enqueue(
            { op: 'update', table, partition, key, revision, document, newRevision },
            newRevision,
        )
    }

    check(table: string, partition: string, key: string, revision: Revision) {
        return this.#enqueue({ op: 'check', table, partition, key, revision }, undefined)
    }

    delete(table: string, partition: string, key: string, revision: Revision) {
        return this.#enqueue({ op: 'delete', table, partition, key, revision }, undefined)
    }

    seal() {
        this.#sealed = true
        return this.#items
    }

    #enqueue<T>(item: TransactionItem, result: T) {
        if (this.#sealed) {
            return Promise.reject(new Error('Transaction has already been committed.'))
        }
        const id = JSON.stringify([item.table, item.partition, item.key])
        if (this.#touched.has(id)) {
            return Promise.reject(
                new Error(
                    `Transaction already contains an operation on '${item.key}' in partition '${item.partition}' of table '${item.table}'.`,
                ),
            )
        }
        if (this.#items.length === maxTransactionItems) {
            return Promise.reject(
                new Error(
                    `Transaction cannot contain more than ${String(maxTransactionItems)} operations.`,
                ),
            )
        }
        this.#touched.add(id)
        this.#items.push(item)
        return Promise.resolve(result)
    }
}
