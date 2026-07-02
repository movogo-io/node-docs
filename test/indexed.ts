import assert from 'node:assert/strict'
import { setDriver } from '../driver.js'
import { docs } from '../indexed.js'
import { DelayedPersistentMemoryDriver } from '../memory.js'
import { isConflict, withTransaction } from '../partitioned.js'

type Rental = {
    name: string
    unitId: string
    status: 'pending' | 'active'
    due?: string
}

type Schema = {
    IndexedRentals: {
        [supplierId: string]: {
            [rentalId: string]: Rental
        }
    }
}

const schema = docs<Schema>()
const byUnit = schema.index(
    'IndexedRentals',
    'byUnit',
    r => r.document.unitId,
    r => r.key,
)
const byId = schema.index(
    'IndexedRentals',
    'byId',
    () => 'all',
    r => r.key,
)
const byStatus = schema.index(
    'IndexedRentals',
    'byStatus',
    r => r.document.status,
    r => r.document.due,
)

describe('indexes', () => {
    beforeEach(() => {
        setDriver(new DelayedPersistentMemoryDriver())
    })

    it('should find documents through an index', async () => {
        await using context = new TestContext()
        const rentals = schema.tables(context).IndexedRentals
        await rentals.partition('s1').add('r1', aRental({ unitId: 'u1' }))
        await rentals.partition('s2').add('r2', aRental({ unitId: 'u2' }))

        const found = await byUnit(context).partition('u1').get('r1')
        assert.deepStrictEqual(found?.document, aRental({ unitId: 'u1' }))
        assert.deepStrictEqual(found.source, { partition: 's1', key: 'r1' })
        assert.strictEqual(await byUnit(context).partition('u1').getDocument('r2'), undefined)
        assert.strictEqual(await byUnit(context).partition('u3').get('r1'), undefined)
    })

    it('should read a whole index partition', async () => {
        await using context = new TestContext()
        const rentals = schema.tables(context).IndexedRentals
        await rentals.partition('s1').add('r1', aRental())
        await rentals.partition('s2').add('r2', aRental())
        await rentals.partition('s3').add('r3', aRental())

        const keys = await Array.fromAsync(
            byId(context).all.getRange({ withPrefix: '' }),
            row => row.key,
        )
        assert.deepStrictEqual(keys.sort(), ['r1', 'r2', 'r3'])
    })

    it('should move index entries when documents change', async () => {
        await using context = new TestContext()
        const rentals = schema.tables(context).IndexedRentals
        const revision = await rentals.partition('s1').add('r1', aRental({ unitId: 'u1' }))

        await rentals.partition('s1').update('r1', revision, aRental({ unitId: 'u2' }))

        assert.strictEqual(await byUnit(context).partition('u1').get('r1'), undefined)
        const moved = await byUnit(context).partition('u2').get('r1')
        assert.deepStrictEqual(moved?.document, aRental({ unitId: 'u2' }))
    })

    it('should remove index entries when documents are deleted', async () => {
        await using context = new TestContext()
        const rentals = schema.tables(context).IndexedRentals
        const revision = await rentals.partition('s1').add('r1', aRental())

        await rentals.partition('s1').delete('r1', revision)

        assert.strictEqual(await byUnit(context).partition('u1').get('r1'), undefined)
        assert.strictEqual(await byId(context).all.get('r1'), undefined)
    })

    it('should omit documents from sparse indexes', async () => {
        await using context = new TestContext()
        const rentals = schema.tables(context).IndexedRentals
        const revision = await rentals.partition('s1').add('r1', aRental())

        assert.deepStrictEqual(
            await Array.fromAsync(byStatus(context).pending.getRange({ withPrefix: '' })),
            [],
        )

        await rentals.partition('s1').update('r1', revision, aRental({ due: '2026-07-01' }))

        const due = await Array.fromAsync(
            byStatus(context).pending.getRange({ before: '2026-08-01' }),
            row => row.key,
        )
        assert.deepStrictEqual(due, ['2026-07-01'])
    })

    it('should update documents found through an index', async () => {
        await using context = new TestContext()
        const rentals = schema.tables(context).IndexedRentals
        await rentals.partition('s1').add('r1', aRental({ unitId: 'u1' }))

        const found = await byUnit(context).partition('u1').get('r1')
        assert.ok(found)
        await rentals
            .partition(found.source.partition)
            .update(found.source.key, found.revision, aRental({ unitId: 'u1', name: 'renamed' }))

        const renamed = await byUnit(context).partition('u1').get('r1')
        assert.strictEqual(renamed?.document.name, 'renamed')
    })

    it('should maintain indexes inside transactions', async () => {
        await using context = new TestContext()
        const rentals = schema.tables(context).IndexedRentals
        const revision = await rentals.partition('s1').add('r1', aRental({ unitId: 'u1' }))

        await withTransaction<Schema>(context, async tx => {
            await tx.IndexedRentals.partition('s1').update(
                'r1',
                revision,
                aRental({ unitId: 'u2' }),
            )
            await tx.IndexedRentals.partition('s1').add('r2', aRental({ unitId: 'u2' }))
        })

        assert.strictEqual(await byUnit(context).partition('u1').get('r1'), undefined)
        const keys = await Array.fromAsync(
            byUnit(context).partition('u2').getRange({ withPrefix: '' }),
            row => row.key,
        )
        assert.deepStrictEqual(keys.sort(), ['r1', 'r2'])
    })

    it('should maintain nothing when a transaction fails', async () => {
        await using context = new TestContext()
        const rentals = schema.tables(context).IndexedRentals
        await rentals.partition('s1').add('r1', aRental({ unitId: 'u1' }))

        await assert.rejects(
            withTransaction<Schema>(
                context,
                async tx => {
                    await tx.IndexedRentals.partition('s1').add('r2', aRental({ unitId: 'u2' }))
                    await tx.IndexedRentals.partition('s1').update(
                        'r1',
                        'stale revision',
                        aRental({ unitId: 'u2' }),
                    )
                },
                { retries: 0 },
            ),
            isConflict,
        )

        assert.strictEqual(await byUnit(context).partition('u2').get('r2'), undefined)
        assert.deepStrictEqual(
            (await byUnit(context).partition('u1').get('r1'))?.document,
            aRental({ unitId: 'u1' }),
        )
    })

    it('should converge concurrent updates without stale index entries', async () => {
        await using context = new TestContext()
        const rentals = schema.tables(context).IndexedRentals
        await Promise.all(
            Array.from({ length: 10 }, (_, i) =>
                rentals.partition('s1').addOrUpdate(
                    'r1',
                    aRental({ unitId: `u${String(i % 2)}` }),
                    existing => {
                        existing.unitId = `u${String(i % 2)}`
                    },
                    { retries: 50, delay: 5 },
                ),
            ),
        )

        const final = await rentals.partition('s1').getDocument('r1')
        const entries = [
            ...(await Array.fromAsync(
                byUnit(context).partition('u0').getRange({ withPrefix: '' }),
            )),
            ...(await Array.fromAsync(
                byUnit(context).partition('u1').getRange({ withPrefix: '' }),
            )),
        ]
        assert.strictEqual(entries.length, 1)
        assert.deepStrictEqual(entries[0]?.document, final)
    })

    it('should reject duplicate and invalid index definitions', () => {
        assert.throws(
            () =>
                schema.index(
                    'IndexedRentals',
                    'byUnit',
                    r => r.document.unitId,
                    r => r.key,
                ),
            /already defined/u,
        )
        assert.throws(
            () =>
                schema.index(
                    'IndexedRentals',
                    'no spaces',
                    r => r.document.unitId,
                    r => r.key,
                ),
            /Invalid index name/u,
        )
    })

    it('should reject the reserved character in indexed rows', async () => {
        await using context = new TestContext()
        const rentals = schema.tables(context).IndexedRentals
        await assert.rejects(
            rentals.partition('s1').add('r\u00001', aRental()),
            /reserved character/u,
        )
        await assert.rejects(
            rentals.partition('s1').add('r1', aRental({ unitId: 'u\u00001' })),
            /reserved character/u,
        )
    })
})

function aRental(props?: Partial<Rental>): Rental {
    return {
        name: 'a rental',
        unitId: 'u1',
        status: 'pending',
        ...props,
    }
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
        for (const release of this.#releasers) {
            await release()
        }
    }
}
