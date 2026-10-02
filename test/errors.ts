import assert from 'node:assert/strict'
import type { TransactionItem } from '../driver.js'
import { MemoryDriver } from '../memory.js'
import { isConflict, isNotFound, isTransactionTooLarge, retryConflict } from '../partitioned.js'

describe('errors', () => {
    it('does not treat a service error with only statusCode as a store error', () => {
        assert.strictEqual(
            isConflict(Object.assign(new Error('Not a draft'), { statusCode: 409 })),
            false,
        )
        assert.strictEqual(
            isNotFound(Object.assign(new Error('No such'), { statusCode: 404 })),
            false,
        )
    })

    it('does not retry a domain conflict thrown inside the closure', async () => {
        const domainConflict = Object.assign(new Error('Not a draft'), { statusCode: 409 })
        let attempts = 0
        await assert.rejects(
            retryConflict(
                () => {
                    attempts++
                    throw domainConflict
                },
                { retries: 3, delay: 1 },
            ),
            domainConflict,
        )
        assert.strictEqual(attempts, 1)
    })

    it('recognizes the refusal of an oversized transaction, and nothing else', async () => {
        const c = await new MemoryDriver().connect({})
        const items: TransactionItem[] = Array.from({ length: 101 }, (_, i) => ({
            op: 'add',
            table: 'T',
            partition: 'p',
            key: `k${String(i)}`,
            document: {},
            newRevision: `r${String(i)}`,
        }))

        await assert.rejects(c.transact(items, { now: 0 }), isTransactionTooLarge)
        assert.strictEqual(isTransactionTooLarge(new Error('Too large')), false)
        assert.strictEqual(
            isTransactionTooLarge(Object.assign(new Error('Conflict'), { status: 409 })),
            false,
        )
    })
})
