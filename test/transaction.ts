import assert from 'node:assert/strict'
import { setDriver, type Connection, type TransactionItem } from '../driver.js'
import { DelayedPersistentMemoryDriver, PersistentMemoryDriver } from '../memory.js'
import { isConflict, isNotFound, tables, withTransaction } from '../partitioned.js'

type Schema = {
    Rentals: {
        [supplierId: string]: {
            [rentalId: string]: {
                name: string
                count: number
            }
        }
    }
    Lookup: {
        byId: {
            [rentalId: string]: {
                supplierId: string
            }
        }
    }
    Users: {
        [id: string]: {
            profile: {
                name: string
            }
        }
    }
}

describe('transactions', () => {
    beforeEach(setMemoryDriver)

    it('should commit buffered writes atomically', async () => {
        await using context = new TestContext()

        let revision: unknown
        await withTransaction<Schema>(context, async tx => {
            revision = await tx.Rentals.partition('s1').add('r1', {
                name: 'a',
                count: 1,
            })
            await tx.Lookup.byId.add('r1', { supplierId: 's1' })
            await tx.Users.withKey('profile').add('u1', { name: 'bla' })
        })

        const t = tables<Schema>(context)
        const row = await t.Rentals.partition('s1').get('r1')
        assert.deepStrictEqual(row.document, { name: 'a', count: 1 })
        assert.strictEqual(row.revision, revision)
        assert.deepStrictEqual(await t.Lookup.byId.getDocument('r1'), { supplierId: 's1' })
        assert.deepStrictEqual(await t.Users.withKey('profile').getDocument('u1'), {
            name: 'bla',
        })
    })

    it('should write nothing when the callback throws', async () => {
        await using context = new TestContext()

        await assert.rejects(
            withTransaction<Schema>(context, async tx => {
                await tx.Rentals.partition('s1').add('r1', { name: 'a', count: 1 })
                throw new Error('boom')
            }),
            /boom/u,
        )

        await assert.rejects(tables<Schema>(context).Rentals.partition('s1').get('r1'), isNotFound)
    })

    it('should read committed state, not buffered writes', async () => {
        await using context = new TestContext()
        await tables<Schema>(context).Rentals.partition('s1').add('r1', { name: 'a', count: 1 })

        await withTransaction<Schema>(context, async tx => {
            const rentals = tx.Rentals.partition('s1')
            const row = await rentals.get('r1')
            await rentals.update('r1', row.revision, { name: 'b', count: 2 })
            assert.deepStrictEqual(await rentals.getDocument('r1'), { name: 'a', count: 1 })
            await rentals.add('r2', { name: 'c', count: 3 })
            await assert.rejects(rentals.get('r2'), isNotFound)
        })

        assert.deepStrictEqual(
            await tables<Schema>(context).Rentals.partition('s1').getDocument('r1'),
            {
                name: 'b',
                count: 2,
            },
        )
    })

    it('should retry conflicting transactions', async () => {
        await using context = new TestContext()
        await tables<Schema>(context).Rentals.partition('s1').add('counter', {
            name: 'c',
            count: 0,
        })

        await Promise.all(
            Array.from({ length: 10 }, () =>
                withTransaction<Schema>(
                    context,
                    async tx => {
                        const row = await tx.Rentals.partition('s1').get('counter')
                        row.document.count += 1
                        await tx.Rentals.partition('s1').updateRow(row)
                    },
                    { retries: 50, delay: 10 },
                ),
            ),
        )

        assert.deepStrictEqual(
            await tables<Schema>(context).Rentals.partition('s1').getDocument('counter'),
            { name: 'c', count: 10 },
        )
    })

    it('should reject two operations on the same document', async () => {
        await using context = new TestContext()
        let attempts = 0

        await assert.rejects(
            withTransaction<Schema>(context, async tx => {
                ++attempts
                const revision = await tx.Rentals.partition('s1').add('r1', {
                    name: 'a',
                    count: 1,
                })
                await tx.Rentals.partition('s1').delete('r1', revision)
            }),
            /already contains an operation/u,
        )

        assert.strictEqual(attempts, 1)
        await assert.rejects(tables<Schema>(context).Rentals.partition('s1').get('r1'), isNotFound)
    })

    it('should reject more than 100 operations', async () => {
        await using context = new TestContext()

        await assert.rejects(
            withTransaction<Schema>(context, async tx => {
                const rentals = tx.Rentals.partition('s1')
                for (let i = 0; i !== 101; ++i) {
                    await rentals.add(`r${String(i)}`, { name: 'a', count: i })
                }
            }),
            /more than 100/u,
        )

        await assert.rejects(tables<Schema>(context).Rentals.partition('s1').get('r0'), isNotFound)
    })

    it('should reject writes after commit', async () => {
        await using context = new TestContext()
        let leaked:
            | ((key: string, document: { name: string; count: number }) => Promise<unknown>)
            | undefined

        await withTransaction<Schema>(context, async tx => {
            const rentals = tx.Rentals.partition('s1')
            await rentals.add('r1', { name: 'a', count: 1 })
            leaked = (key, document) => rentals.add(key, document)
        })

        assert.ok(leaked)
        assert.throws(() => leaked?.('r9', { name: 'x', count: 0 }), /already been committed/u)
    })

    it('should skip the driver call when nothing was written', async () => {
        const inner = new PersistentMemoryDriver()
        let calls = 0
        setDriver({
            connect: async () => {
                const c = await inner.connect()
                return spyConnection(c, items => {
                    ++calls
                    return c.transact(items)
                })
            },
        })
        await using context = new TestContext()

        const result = await withTransaction(context, () => Promise.resolve('done'))

        assert.strictEqual(result, 'done')
        assert.strictEqual(calls, 0)
    })

    it('should not retry when retries is zero', async () => {
        await using context = new TestContext()
        await tables<Schema>(context).Rentals.partition('s1').add('r1', { name: 'a', count: 0 })
        let attempts = 0

        await assert.rejects(
            withTransaction<Schema>(
                context,
                async tx => {
                    ++attempts
                    const row = await tx.Rentals.partition('s1').get('r1')
                    await tables<Schema>(context)
                        .Rentals.partition('s1')
                        .update('r1', row.revision, { name: 'x', count: 9 })
                    await tx.Rentals.partition('s1').updateRow(row)
                },
                { retries: 0 },
            ),
            isConflict,
        )

        assert.strictEqual(attempts, 1)
    })

    it('should close the connection when the context cannot free it', async () => {
        const driver = new PersistentMemoryDriver()
        setDriver(driver)

        await withTransaction({}, () => Promise.resolve())

        const c = await driver.connect()
        await assert.rejects(c.get('T', 'p', 'k'), /closed/u)
    })

    it('should leave freeing the connection to the context', async () => {
        const driver = new PersistentMemoryDriver()
        setDriver(driver)
        const context = new TestContext()

        await withTransaction(context, () => Promise.resolve())

        const c = await driver.connect()
        await c.add('T', 'p', 'k', { data: 'x' })
        await context[Symbol.asyncDispose]()
        await assert.rejects(c.add('T', 'p', 'k2', { data: 'y' }), /closed/u)
    })
})

class TestContext {
    readonly #releasers: (() => Promise<void>)[] = []

    on(event: string, handler: () => Promise<void>) {
        switch (event) {
            case 'free':
                this.#releasers.push(handler)
                return true
        }
        return false
    }

    async [Symbol.asyncDispose]() {
        await Promise.allSettled(this.#releasers.map(r => r()))
    }
}

function setMemoryDriver() {
    setDriver(new DelayedPersistentMemoryDriver())
}

function spyConnection(
    c: Connection,
    transact: (items: TransactionItem[]) => Promise<void>,
): Connection {
    return {
        close: () => c.close(),
        add: (table, partition, key, document) => c.add(table, partition, key, document),
        get: (table, partition, key) => c.get(table, partition, key),
        getPartitions: table => c.getPartitions(table),
        getPartition: (table, partition, range) => c.getPartition(table, partition, range),
        update: (table, partition, key, revision, document) =>
            c.update(table, partition, key, revision, document),
        delete: (table, partition, key, revision) => c.delete(table, partition, key, revision),
        transact,
    }
}
