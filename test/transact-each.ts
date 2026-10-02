import assert from 'node:assert/strict'
import { setTimeout } from 'node:timers/promises'
import { setDriver, type Connection, type TransactionItem } from '../driver.js'
import { DelayedPersistentMemoryDriver, PersistentMemoryDriver } from '../memory.js'
import { isConflict, isTransactionTooLarge, tables, transactEach } from '../partitioned.js'

type Schema = {
    Rentals: {
        [supplierId: string]: {
            [rentalId: string]: {
                name: string
                count: number
            }
        }
    }
    Fences: {
        [supplierId: string]: {
            [fenceId: string]: {
                name: string
            }
        }
    }
}

describe('transactEach', () => {
    beforeEach(() => {
        setDriver(new DelayedPersistentMemoryDriver())
    })

    it('should return nothing for no items', async () => {
        await using context = new TestContext()

        assert.deepStrictEqual(
            await transactEach<Schema, string, string>(context, [], () => Promise.resolve('x')),
            [],
        )
    })

    it('should commit every unit and return the results in item order', async () => {
        await using context = new TestContext()

        const results = await transactEach<Schema, string, string>(
            context,
            ['r1', 'r2', 'r3'],
            async (tx, key) => {
                await tx.Rentals.partition('s1').add(key, { name: key, count: 1 })
                return key.toUpperCase()
            },
        )

        assert.deepStrictEqual(results, ['R1', 'R2', 'R3'])
        assert.deepStrictEqual(await rentalsOf(context), [
            { name: 'r1', count: 1 },
            { name: 'r2', count: 1 },
            { name: 'r3', count: 1 },
        ])
    })

    it('should send nothing for a unit that writes nothing', async () => {
        const sent = spyOnTransactions()
        await using context = new TestContext()

        const results = await transactEach<Schema, string, string>(
            context,
            ['r1', 'skipped', 'r3'],
            async (tx, key) => {
                if (key !== 'skipped') {
                    await tx.Rentals.partition('s1').add(key, { name: key, count: 1 })
                }
                return key
            },
        )

        assert.deepStrictEqual(results, ['r1', 'skipped', 'r3'])
        assert.deepStrictEqual(sorted(sent.keys().flat()), ['r1', 'r3'])
    })

    it('should send units that share no document together', async () => {
        const sent = spyOnTransactions()
        await using context = new TestContext()

        await transactEach<Schema, string>(context, ['r1', 'r2', 'r3'], async (tx, key) => {
            await tx.Rentals.partition('s1').add(key, { name: key, count: 1 })
        })

        assert.strictEqual(sent.inFlightPeak(), 3)
    })

    it('should send units that check the same document one at a time, in item order', async () => {
        const sent = spyOnTransactions()
        await using context = new TestContext()
        const fence = await tables<Schema>(context).Fences.partition('s1').add('f1', { name: 'f' })

        await transactEach<Schema, string>(context, ['r1', 'r2', 'r3'], async (tx, key) => {
            await tx.Fences.partition('s1').check('f1', fence)
            await tx.Rentals.partition('s1').add(key, { name: key, count: 1 })
        })

        assert.strictEqual(sent.inFlightPeak(), 1)
        assert.deepStrictEqual(sent.keys(), [
            ['f1', 'r1'],
            ['f1', 'r2'],
            ['f1', 'r3'],
        ])
    })

    it('should apply two units that write one document in item order', async () => {
        await using context = new TestContext()
        await tables<Schema>(context).Rentals.partition('s1').add('r1', { name: '', count: 0 })

        await transactEach<Schema, string>(
            context,
            ['first', 'second'],
            async (tx, name) => {
                const row = await tx.Rentals.partition('s1').get('r1')
                await tx.Rentals.partition('s1').update('r1', row.revision, {
                    name,
                    count: row.document.count + 1,
                })
            },
            { delay: 1 },
        )

        assert.deepStrictEqual(await rentalsOf(context), [{ name: 'second', count: 2 }])
    })

    it('should run again only the unit that lost a race', async () => {
        await using context = new TestContext()
        const rentals = tables<Schema>(context).Rentals.partition('s1')
        const fence = await tables<Schema>(context).Fences.partition('s1').add('f1', { name: 'f' })
        await rentals.add('r1', { name: 'r1', count: 0 })
        await rentals.add('r2', { name: 'r2', count: 0 })
        const runs: string[] = []

        await transactEach<Schema, string>(
            context,
            ['r1', 'r2'],
            async (tx, key) => {
                const racing = runs.length === 0
                runs.push(key)
                await tx.Fences.partition('s1').check('f1', fence)
                const row = await tx.Rentals.partition('s1').get(key)
                if (racing) {
                    await rentals.update(key, row.revision, { name: 'raced', count: 0 })
                }
                await tx.Rentals.partition('s1').update(key, row.revision, {
                    ...row.document,
                    count: row.document.count + 1,
                })
            },
            { delay: 1 },
        )

        assert.deepStrictEqual(sorted(runs), ['r1', 'r1', 'r2'])
        assert.deepStrictEqual(await rentalsOf(context), [
            { name: 'raced', count: 1 },
            { name: 'r2', count: 1 },
        ])
    })

    it('should spend the retries of one unit on a lost fence and send nothing behind it', async () => {
        const sent = spyOnTransactions()
        await using context = new TestContext()
        const fences = tables<Schema>(context).Fences.partition('s1')
        const lost = await fences.add('f1', { name: 'f' })
        await fences.update('f1', lost, { name: 'moved' })
        const runs: string[] = []

        await assert.rejects(
            transactEach<Schema, string>(
                context,
                ['r1', 'r2', 'r3'],
                async (tx, key) => {
                    runs.push(key)
                    await tx.Fences.partition('s1').check('f1', lost)
                    await tx.Rentals.partition('s1').add(key, { name: key, count: 1 })
                },
                { retries: 2, delay: 1 },
            ),
            isConflict,
        )

        assert.deepStrictEqual(sent.keys(), [
            ['f1', 'r1'],
            ['f1', 'r1'],
            ['f1', 'r1'],
        ])
        assert.deepStrictEqual(sorted(runs), ['r1', 'r1', 'r1', 'r2', 'r3'])
        assert.deepStrictEqual(await rentalsOf(context), [])
    })

    it('should settle the units of a window before it throws what a callback threw', async () => {
        await using context = new TestContext()

        await assert.rejects(
            transactEach<Schema, string>(context, ['r1', 'refused', 'r3'], async (tx, key) => {
                if (key === 'refused') {
                    throw new Error('refused')
                }
                await setTimeout(5)
                await tx.Rentals.partition('s1').add(key, { name: key, count: 1 })
            }),
            /refused/u,
        )

        assert.deepStrictEqual(await rentalsOf(context), [
            { name: 'r1', count: 1 },
            { name: 'r3', count: 1 },
        ])
    })

    it('should throw the first failure in item order', async () => {
        await using context = new TestContext()

        await assert.rejects(
            transactEach<Schema, string>(context, ['slow', 'fast'], async (_, key) => {
                await setTimeout(key === 'slow' ? 10 : 0)
                throw new Error(key)
            }),
            /slow/u,
        )
    })

    it('should not throw a conflict of the caller again as a lost race', async () => {
        await using context = new TestContext()
        let runs = 0

        await assert.rejects(
            transactEach<Schema, string>(context, ['r1'], () => {
                ++runs
                return Promise.reject(Object.assign(new Error('taken'), { statusCode: 409 }))
            }),
            /taken/u,
        )

        assert.strictEqual(runs, 1)
    })

    it('should take the items a window at a time and start no window after a failure', async () => {
        const sent = spyOnTransactions()
        await using context = new TestContext()
        const keys = Array.from({ length: 40 }, (_, i) => `r${String(i).padStart(2, '0')}`)
        const runs: string[] = []

        await assert.rejects(
            transactEach<Schema, string>(context, keys, async (tx, key) => {
                runs.push(key)
                if (key === 'r20') {
                    throw new Error('refused')
                }
                await tx.Rentals.partition('s1').add(key, { name: key, count: 1 })
            }),
            /refused/u,
        )

        assert.strictEqual(sent.inFlightPeak(), 16)
        assert.deepStrictEqual(sorted(runs), keys.slice(0, 32))
        assert.deepStrictEqual(
            (await rentalsOf(context)).map(rental => rental.name),
            keys.slice(0, 32).filter(key => key !== 'r20'),
        )
    })

    it('should start no window once the signal is aborted', async () => {
        await using context = new TestContext()
        const keys = Array.from({ length: 20 }, (_, i) => `r${String(i).padStart(2, '0')}`)
        const aborting = new AbortController()

        await assert.rejects(
            transactEach<Schema, string>(
                context,
                keys,
                async (tx, key) => {
                    aborting.abort()
                    await tx.Rentals.partition('s1').add(key, { name: key, count: 1 })
                },
                { signal: aborting.signal },
            ),
            { name: 'AbortError' },
        )

        assert.deepStrictEqual(
            (await rentalsOf(context)).map(rental => rental.name),
            keys.slice(0, 16),
        )
    })

    it('should refuse a unit of more than 100 operations and commit the others', async () => {
        await using context = new TestContext()

        await assert.rejects(
            transactEach<Schema, number>(context, [1, 101], async (tx, count) => {
                for (let i = 0; i !== count; ++i) {
                    await tx.Rentals.partition('s1').add(`r${String(count)}-${String(i)}`, {
                        name: 'a',
                        count: i,
                    })
                }
            }),
            isTransactionTooLarge,
        )

        assert.deepStrictEqual(await rentalsOf(context), [{ name: 'a', count: 0 }])
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

function sorted(keys: string[]) {
    return keys.toSorted((a, b) => a.localeCompare(b))
}

async function rentalsOf(context: TestContext) {
    return await Array.fromAsync(
        tables<Schema>(context).Rentals.partition('s1').getAll(),
        row => row.document,
    )
}

// Holds every transaction for a moment, so the ones in flight together overlap.
function spyOnTransactions() {
    const inner = new PersistentMemoryDriver()
    const sent: string[][] = []
    let inFlight = 0
    let inFlightPeak = 0
    setDriver({
        connect: async () => {
            const c = await inner.connect()
            return spyConnection(c, async (items, options) => {
                sent.push(items.map(item => item.key))
                inFlight += 1
                inFlightPeak = Math.max(inFlightPeak, inFlight)
                try {
                    await setTimeout(2)
                    await c.transact(items, options)
                } finally {
                    inFlight -= 1
                }
            })
        },
    })
    return { keys: () => sent, inFlightPeak: () => inFlightPeak }
}

function spyConnection(
    c: Connection,
    transact: (items: TransactionItem[], options: { now: number }) => Promise<void>,
): Connection {
    return {
        close: () => c.close(),
        add: (table, partition, key, document, options) =>
            c.add(table, partition, key, document, options),
        get: (table, partition, key, options) => c.get(table, partition, key, options),
        getPartitions: table => c.getPartitions(table),
        getPartition: (table, partition, range, options) =>
            c.getPartition(table, partition, range, options),
        update: (table, partition, key, revision, document, options) =>
            c.update(table, partition, key, revision, document, options),
        delete: (table, partition, key, revision, options) =>
            c.delete(table, partition, key, revision, options),
        transact,
    }
}
