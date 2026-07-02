import { randomUUID } from 'node:crypto'
import type { Revision, StoredDocument } from '../schema.js'
import type { TransactionItem } from './driver.js'

const maxItems = 100

export class TransactionBuffer {
    readonly #items: TransactionItem[] = []
    readonly #touched = new Set<string>()
    #sealed = false

    add(table: string, partition: string, key: string, document: StoredDocument) {
        const newRevision: Revision = randomUUID()
        this.#enqueue({ op: 'add', table, partition, key, document, newRevision })
        return Promise.resolve(newRevision)
    }

    update(
        table: string,
        partition: string,
        key: string,
        revision: Revision,
        document: StoredDocument,
    ) {
        const newRevision: Revision = randomUUID()
        this.#enqueue({ op: 'update', table, partition, key, revision, document, newRevision })
        return Promise.resolve(newRevision)
    }

    check(table: string, partition: string, key: string, revision: Revision) {
        this.#enqueue({ op: 'check', table, partition, key, revision })
        return Promise.resolve()
    }

    delete(table: string, partition: string, key: string, revision: Revision) {
        this.#enqueue({ op: 'delete', table, partition, key, revision })
        return Promise.resolve()
    }

    #enqueue(item: TransactionItem) {
        if (this.#sealed) {
            throw new Error('Transaction has already been committed.')
        }
        const id = JSON.stringify([item.table, item.partition, item.key])
        if (this.#touched.has(id)) {
            throw new Error(
                `Transaction already contains an operation on '${item.key}' in partition '${item.partition}' of table '${item.table}'.`,
            )
        }
        if (this.#items.length === maxItems) {
            throw new Error(`Transaction cannot contain more than ${String(maxItems)} operations.`)
        }
        this.#touched.add(id)
        this.#items.push(item)
    }

    seal() {
        this.#sealed = true
        return this.#items
    }
}
