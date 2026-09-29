import assert from 'node:assert/strict'
import type { TransactionItem } from '../driver.js'
import { harness } from '../harness.js'
import { MemoryDriver } from '../memory.js'
import { isNotFound } from '../partitioned.js'

describe('in-memory driver', () => {
    harness(it, new MemoryDriver(), () => ({}))

    it('keeps expired rows until the clock it is handed says otherwise', async () => {
        const c = await new MemoryDriver().connect({})
        const { revision } = await c.add('T', 'p', 'k', { data: 'x' }, { now: 0, expiresAt: 1 })
        assert.deepStrictEqual(await c.get('T', 'p', 'k'), {
            partition: 'p',
            key: 'k',
            revision,
            document: { data: 'x' },
            seq: 0,
            updatedAt: '1970-01-01T00:00:00.000Z',
            expiresAt: 1,
        })
    })
    it('refuses a transaction of more than 100 operations', async () => {
        const c = await new MemoryDriver().connect({})
        const items: TransactionItem[] = Array.from({ length: 101 }, (_, i) => ({
            op: 'add',
            table: 'T',
            partition: 'p',
            key: `k${String(i)}`,
            document: {},
            newRevision: `r${String(i)}`,
        }))

        await assert.rejects(c.transact(items, { now: 0 }), /101 operations on 'T'; at most 100/u)
        await assert.rejects(c.get('T', 'p', 'k0'), isNotFound)
    })

    it('refuses a transaction over 4 MB of items that each fit', async () => {
        const c = await new MemoryDriver().connect({})
        const items: TransactionItem[] = Array.from({ length: 14 }, (_, i) => ({
            op: 'add',
            table: 'T',
            partition: 'p',
            key: `k${String(i)}`,
            document: { text: 'x'.repeat(300 * 1024) },
            newRevision: `r${String(i)}`,
        }))

        await assert.rejects(c.transact(items, { now: 0 }), /at most 4194304 are allowed/u)
        await assert.rejects(c.get('T', 'p', 'k0'), isNotFound)
    })

    it('accepts a transaction just under 4 MB', async () => {
        const c = await new MemoryDriver().connect({})
        const items: TransactionItem[] = Array.from({ length: 13 }, (_, i) => ({
            op: 'add',
            table: 'T',
            partition: 'p',
            key: `k${String(i)}`,
            document: { text: 'x'.repeat(300 * 1024) },
            newRevision: `r${String(i)}`,
        }))

        await c.transact(items, { now: 0 })

        assert.strictEqual((await c.get('T', 'p', 'k12')).revision, 'r12')
    })
})
