import { randomUUID } from 'node:crypto'
import type { Revision, StoredDocument } from '../schema.js'
import type { TransactionItem } from './driver.js'
import { expiryOf } from './expiry.js'

export const maxTransactionItems = 100

export class TransactionBuffer {
    readonly #items: TransactionItem[] = []
    readonly #touched = new Set<string>()
    #sealed = false

    async add(table: string, partition: string, key: string, document: StoredDocument) {
        const expiry = expiryOf(table, document)
        const newRevision: Revision = randomUUID()
        await this.#enqueue({ op: 'add', table, partition, key, document, newRevision, ...expiry })
        return newRevision
    }

    async update(
        table: string,
        partition: string,
        key: string,
        revision: Revision,
        document: StoredDocument,
    ) {
        const expiry = expiryOf(table, document)
        const newRevision: Revision = randomUUID()
        await this.#enqueue({
            op: 'update',
            table,
            partition,
            key,
            revision,
            document,
            newRevision,
            ...expiry,
        })
        return newRevision
    }

    async check(table: string, partition: string, key: string, revision: Revision) {
        await this.#enqueue({ op: 'check', table, partition, key, revision })
    }

    async delete(table: string, partition: string, key: string, revision: Revision) {
        await this.#enqueue({ op: 'delete', table, partition, key, revision })
    }

    seal() {
        this.#sealed = true
        return this.#items
    }

    #enqueue(item: TransactionItem) {
        return Promise.try(() => {
            if (this.#sealed) {
                throw new Error('Transaction has already been committed.')
            }
            const id = JSON.stringify([item.table, item.partition, item.key])
            if (this.#touched.has(id)) {
                throw new Error(
                    `Transaction already contains an operation on '${item.key}' in partition '${item.partition}' of table '${item.table}'.`,
                )
            }
            if (this.#items.length === maxTransactionItems) {
                throw new Error(
                    `Transaction cannot contain more than ${String(maxTransactionItems)} operations.`,
                )
            }
            this.#touched.add(id)
            this.#items.push(item)
        })
    }
}
