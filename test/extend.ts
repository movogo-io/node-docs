import assert from 'node:assert/strict'
import { setTimeout } from 'node:timers/promises'
import {
    extendWrites,
    setDriver,
    type Connection,
    type ExtendedWrite,
    type TransactionItem,
    type WriteExtension,
} from '../driver.js'
import { PersistentMemoryDriver } from '../memory.js'
import {
    isConflict,
    isTransactionTooLarge,
    tables,
    transactEach,
    withTransaction,
} from '../partitioned.js'

type Schema = {
    ExtendedRentals: {
        [supplierId: string]: {
            [rentalId: string]: { n: number }
        }
    }
    'ExtendedRentals.log': {
        [supplierId: string]: {
            [entry: string]: { op: string }
        }
    }
    PlainRentals: {
        [supplierId: string]: {
            [rentalId: string]: { n: number }
        }
    }
}

describe('write extensions', () => {
    let driver: Recording
    let extension: Logging
    let remove: () => void

    beforeEach(() => {
        driver = recordingDriver()
        extension = logging()
        remove = extendWrites(extension)
    })

    afterEach(() => {
        remove()
    })

    it('adds what the extension prepares to each single write, and tells it once committed', async () => {
        await using context = new TestContext()
        const rentals = tables<Schema>(context).ExtendedRentals.partition('s1')

        const added = await rentals.add('r1', { n: 1 })
        assert.deepStrictEqual(driver.calls(), [
            ['get', 'plain'],
            ['transact', 'add ExtendedRentals', 'put ExtendedRentals.log'],
        ])
        const updated = await rentals.update('r1', added, { n: 2 })
        assert.deepStrictEqual(driver.calls(), [
            ['get', 'plain'],
            ['transact', 'update ExtendedRentals', 'put ExtendedRentals.log'],
        ])
        await rentals.delete('r1', updated)
        assert.deepStrictEqual(driver.calls(), [
            ['get', 'plain'],
            ['transact', 'delete ExtendedRentals', 'put ExtendedRentals.log'],
        ])

        assert.deepStrictEqual(extension.prepared, [
            [{ op: 'add', key: 'r1', document: { n: 1 } }],
            [{ op: 'update', key: 'r1', document: { n: 2 } }],
            [{ op: 'delete', key: 'r1', document: { n: 2 } }],
        ])
        assert.deepStrictEqual(extension.committed, ['add r1', 'update r1', 'delete r1'])
        assert.deepStrictEqual(
            (await logOf(context)).map(row => row.op),
            ['add', 'delete', 'update'],
        )
    })

    it('answers the revision, seq and updatedAt the driver stores for an extended write', async () => {
        await using context = new TestContext()
        const db = tables<Schema>(context)

        const added = await db.ExtendedRentals.partition('s1').getOrAdd('r1', { n: 1 })
        const stored = await db.ExtendedRentals.partition('s1').get('r1')
        assert.deepStrictEqual(
            { revision: added.revision, seq: added.seq, updatedAt: added.updatedAt },
            { revision: stored.revision, seq: stored.seq, updatedAt: stored.updatedAt },
        )

        const updated = await db.ExtendedRentals.partition('s1').addOrUpdate(
            'r1',
            { n: 0 },
            () => ({
                n: 2,
            }),
        )
        const restored = await db.ExtendedRentals.partition('s1').get('r1')
        assert.deepStrictEqual(
            { revision: updated.revision, seq: updated.seq, updatedAt: updated.updatedAt },
            { revision: restored.revision, seq: 1, updatedAt: restored.updatedAt },
        )
    })

    it('reads the row a caller already read only once', async () => {
        await using context = new TestContext()
        const rentals = tables<Schema>(context).ExtendedRentals

        await rentals.partition('s1').getOrAdd('r1', { n: 1 })
        assert.deepStrictEqual(driver.calls(), [
            ['get', 'plain'],
            ['transact', 'add ExtendedRentals', 'put ExtendedRentals.log'],
        ])
    })

    it('conflicts a stale update or delete before preparing it', async () => {
        await using context = new TestContext()
        const rentals = tables<Schema>(context).ExtendedRentals.partition('s1')
        const added = await rentals.add('r1', { n: 1 })
        await rentals.update('r1', added, { n: 2 })
        driver.calls()

        await assert.rejects(rentals.update('r1', added, { n: 3 }), isConflict)
        await assert.rejects(rentals.delete('r1', added), isConflict)
        await assert.rejects(rentals.delete('r2', added), isConflict)

        assert.deepStrictEqual(
            driver.calls().map(call => call.join(' ')),
            [
                'get plain',
                'get consistent',
                'get plain',
                'get consistent',
                'get plain',
                'get consistent',
            ],
        )
        assert.deepStrictEqual(extension.committed, ['add r1', 'update r1'])
    })

    it('never tells the extension of a write the store refused', async () => {
        await using context = new TestContext()
        const rentals = tables<Schema>(context).ExtendedRentals.partition('s1')
        await rentals.add('r1', { n: 1 })

        await assert.rejects(rentals.add('r1', { n: 2 }), isConflict)

        assert.deepStrictEqual(extension.prepared.length, 2)
        assert.deepStrictEqual(extension.committed, ['add r1'])
    })

    it('tells the extension once of a transaction that committed on a retry', async () => {
        await using context = new TestContext()
        driver.conflictNext()

        await withTransaction<Schema>(context, async tx => {
            await tx.ExtendedRentals.partition('s1').add('r1', { n: 1 })
            await tx.ExtendedRentals.partition('s1').add('r2', { n: 2 })
        })

        assert.deepStrictEqual(extension.prepared, [
            [
                { op: 'add', key: 'r1', document: { n: 1 } },
                { op: 'add', key: 'r2', document: { n: 2 } },
            ],
            [
                { op: 'add', key: 'r1', document: { n: 1 } },
                { op: 'add', key: 'r2', document: { n: 2 } },
            ],
        ])
        assert.deepStrictEqual(extension.committed, ['add r1', 'add r2'])
    })

    it('tells the extension once per committed unit, never of a unit run again', async () => {
        await using context = new TestContext()
        driver.conflictNext()

        await transactEach<Schema, string>(context, ['r1', 'r2', 'r3'], async (tx, key) => {
            await tx.ExtendedRentals.partition('s1').add(key, { n: 1 })
        })

        assert.strictEqual(extension.prepared.length, 4)
        assert.deepStrictEqual(
            extension.committed.toSorted((a, b) => a.localeCompare(b)),
            ['add r1', 'add r2', 'add r3'],
        )
    })

    it('hands the extension only the document writes of tables it applies to', async () => {
        await using context = new TestContext()
        const db = tables<Schema>(context)
        const extended = await db.ExtendedRentals.partition('s1').add('r1', { n: 1 })
        await db.PlainRentals.partition('s1').add('p1', { n: 1 })
        extension.prepared.length = 0

        await withTransaction<Schema>(context, async tx => {
            await tx.ExtendedRentals.partition('s1').check('r1', extended)
            await tx.PlainRentals.partition('s1').add('p2', { n: 2 })
        })

        assert.deepStrictEqual(extension.prepared, [])
    })

    it('writes a table no extension applies to through the plain driver call', async () => {
        await using context = new TestContext()
        const rentals = tables<Schema>(context).PlainRentals.partition('s1')

        const added = await rentals.add('p1', { n: 1 })
        await rentals.delete('p1', added)

        assert.deepStrictEqual(driver.calls(), [['add'], ['delete']])
    })

    it("counts the extension's items toward the transaction limit", async () => {
        await using context = new TestContext()
        const write = (count: number, partition: string) =>
            withTransaction<Schema>(context, async tx => {
                for (const n of Array.from({ length: count }, (_, i) => i)) {
                    await tx.ExtendedRentals.partition(partition).add(`r${String(n)}`, { n })
                }
            })

        await write(50, 's1')
        await assert.rejects(
            write(51, 's2'),
            (e: unknown) =>
                isTransactionTooLarge(e) &&
                Error.isError(e) &&
                e.message.includes(
                    '51 requested operations expanded to 102 including index maintenance and write extensions',
                ),
        )
        assert.strictEqual(extension.committed.length, 50)
    })

    it('reads the documents of deletes a bounded number at a time', async () => {
        await using context = new TestContext()
        const rentals = tables<Schema>(context).ExtendedRentals.partition('s1')
        const revisions = await Promise.all(
            Array.from({ length: 12 }, (_, n) => rentals.add(`r${String(n)}`, { n })),
        )
        driver.bound(3)

        await withTransaction<Schema>(context, async tx => {
            for (const [n, revision] of revisions.entries()) {
                await tx.ExtendedRentals.partition('s1').delete(`r${String(n)}`, revision)
            }
        })

        assert.strictEqual(driver.readsInFlightPeak(), 3)
        assert.strictEqual(extension.prepared.at(-1)?.length, 12)
    })

    it('refuses an extension registered twice', () => {
        assert.throws(() => extendWrites(extension), /already registered/u)
    })
})

type Logging = WriteExtension & {
    prepared: { op: string; key: string; document: unknown }[][]
    committed: string[]
}

function logging(): Logging {
    const prepared: Logging['prepared'] = []
    const committed: string[] = []
    return {
        prepared,
        committed,
        applies: table => table === 'ExtendedRentals',
        prepare: (_context, writes: readonly ExtendedWrite[]) => {
            prepared.push(writes.map(({ op, key, document }) => ({ op, key, document })))
            return {
                items: writes.map((write): TransactionItem => ({
                    op: 'put',
                    table: 'ExtendedRentals.log',
                    partition: write.partition,
                    key: `${write.key}|${write.op}`,
                    document: { op: write.op },
                    newRevision: `${write.key}|${write.op}`,
                })),
                committed: () => {
                    committed.push(...writes.map(write => `${write.op} ${write.key}`))
                },
            }
        },
    }
}

type Recording = ReturnType<typeof recordingDriver>

// Records every call the store makes, holds reads for a moment so the ones
// in flight together overlap, and can refuse the next transaction as a lost race.
function recordingDriver() {
    const memory = new PersistentMemoryDriver()
    let calls: string[][] = []
    let conflicting = false
    let requestsInFlightMax: number | undefined
    let readsInFlight = 0
    let readsInFlightPeak = 0
    setDriver({
        connect: async (): Promise<Connection> => {
            const c = await memory.connect()
            return {
                ...(requestsInFlightMax !== undefined && { requestsInFlightMax }),
                close: () => c.close(),
                getPartitions: table => c.getPartitions(table),
                getPartition: (table, partition, range, options) =>
                    c.getPartition(table, partition, range, options),
                get: async (table, partition, key, options) => {
                    calls.push(['get', options?.consistent === true ? 'consistent' : 'plain'])
                    readsInFlight += 1
                    readsInFlightPeak = Math.max(readsInFlightPeak, readsInFlight)
                    try {
                        await setTimeout(1)
                        return await c.get(table, partition, key, options)
                    } finally {
                        readsInFlight -= 1
                    }
                },
                add: (table, partition, key, document, options) => {
                    calls.push(['add'])
                    return c.add(table, partition, key, document, options)
                },
                update: (table, partition, key, revision, document, options) => {
                    calls.push(['update'])
                    return c.update(table, partition, key, revision, document, options)
                },
                delete: (table, partition, key, revision, options) => {
                    calls.push(['delete'])
                    return c.delete(table, partition, key, revision, options)
                },
                transact: async (items, options) => {
                    calls.push(['transact', ...items.map(item => `${item.op} ${item.table}`)])
                    if (conflicting) {
                        conflicting = false
                        throw Object.assign(new Error('Conflict'), { status: 409 })
                    }
                    await c.transact(items, options)
                },
            }
        },
    })
    return {
        calls: () => {
            const made = calls
            calls = []
            return made
        },
        conflictNext: () => {
            conflicting = true
        },
        bound: (max: number) => {
            requestsInFlightMax = max
            readsInFlightPeak = 0
        },
        readsInFlightPeak: () => readsInFlightPeak,
    }
}

async function logOf(context: TestContext) {
    const rows = await Array.fromAsync(
        tables<Schema>(context)['ExtendedRentals.log'].partition('s1').getAll(),
        row => row.document,
    )
    return rows.toSorted((a, b) => a.op.localeCompare(b.op))
}

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
