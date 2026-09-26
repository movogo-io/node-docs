import assert from 'node:assert/strict'
import { setDriver } from '../driver.js'
import { docs } from '../indexed.js'
import { LaggingPersistentMemoryDriver } from '../memory.js'
import { isNotFound, withTransaction } from '../partitioned.js'

type Schema = {
    LaggingDocs: {
        [partition: string]: {
            [key: string]: { unitId: string; n: number }
        }
    }
}

const schema = docs<Schema>()
const byUnit = schema.index(
    'LaggingDocs',
    'byUnit',
    r => r.document.unitId,
    r => r.key,
)

describe('lagging driver', () => {
    beforeEach(() => {
        setDriver(new LaggingPersistentMemoryDriver())
    })

    it('shows a row as it was once after each write, then as it is', async () => {
        const c = await new LaggingPersistentMemoryDriver().connect()
        const { revision: added } = await c.add('T', 'p', 'k', { n: 1 }, { now: 0 })
        await assert.rejects(c.get('T', 'p', 'k'), isNotFound)
        assert.deepStrictEqual((await c.get('T', 'p', 'k')).document, { n: 1 })
        const { revision: updated } = await c.update('T', 'p', 'k', added, { n: 2 }, { now: 0 })
        assert.deepStrictEqual(await c.get('T', 'p', 'k'), {
            partition: 'p',
            key: 'k',
            revision: added,
            document: { n: 1 },
            seq: 0,
            updatedAt: '1970-01-01T00:00:00.000Z',
        })
        assert.deepStrictEqual((await c.get('T', 'p', 'k')).revision, updated)
        await c.delete('T', 'p', 'k', updated, { now: 0 })
        assert.deepStrictEqual((await c.get('T', 'p', 'k')).document, { n: 2 })
        await assert.rejects(c.get('T', 'p', 'k'), isNotFound)
    })

    it('never lags a consistent read, and leaves the replica behind', async () => {
        const c = await new LaggingPersistentMemoryDriver().connect()
        await c.add('T', 'p', 'k', { n: 1 }, { now: 0 })
        assert.deepStrictEqual((await c.get('T', 'p', 'k', { consistent: true })).document, {
            n: 1,
        })
        await assert.rejects(c.get('T', 'p', 'k'), isNotFound)
        assert.deepStrictEqual((await c.get('T', 'p', 'k')).document, { n: 1 })
    })

    it('lags batch and partition reads row by row', async () => {
        const c = await new LaggingPersistentMemoryDriver().connect()
        const { revision: a } = await c.add('T', 'p', 'a', { n: 1 }, { now: 0 })
        const { revision: b } = await c.add('T', 'p', 'b', { n: 1 }, { now: 0 })
        assert.deepStrictEqual(await c.getMany('T', [{ partition: 'p', key: 'a' }]), [])
        assert.deepStrictEqual(
            (await Array.fromAsync(c.getPartition('T', 'p'))).map(row => row.key),
            ['a'],
        )
        const { revision: updated } = await c.update('T', 'p', 'a', a, { n: 2 }, { now: 60 })
        assert.deepStrictEqual(await Array.fromAsync(c.getPartition('T', 'p')), [
            {
                key: 'a',
                revision: a,
                document: { n: 1 },
                seq: 0,
                updatedAt: '1970-01-01T00:00:00.000Z',
            },
            {
                key: 'b',
                revision: b,
                document: { n: 1 },
                seq: 0,
                updatedAt: '1970-01-01T00:00:00.000Z',
            },
        ])
        assert.deepStrictEqual(
            await Array.fromAsync(c.getPartition('T', 'p', undefined, { consistent: true })),
            [
                {
                    key: 'a',
                    revision: updated,
                    document: { n: 2 },
                    seq: 1,
                    updatedAt: '1970-01-01T00:01:00.000Z',
                },
                {
                    key: 'b',
                    revision: b,
                    document: { n: 1 },
                    seq: 0,
                    updatedAt: '1970-01-01T00:00:00.000Z',
                },
            ],
        )
    })

    it('lets an indexed row be updated right after it was written', async () => {
        await using db = schema.tables({})
        const p = db.LaggingDocs.partition('p1')
        const added = await p.add('k1', { unitId: 'u1', n: 1 })
        const updated = await p.update('k1', added, { unitId: 'u2', n: 2 })
        await p.update('k1', updated, { unitId: 'u3', n: 3 })
        await withTransaction<Schema>({}, async tx => {
            const row = await tx.LaggingDocs.partition('p1').get('k1', { consistent: true })
            await tx.LaggingDocs.partition('p1').update(row.key, row.revision, {
                unitId: 'u4',
                n: 4,
            })
        })

        await using index = byUnit({})
        const consistent = { consistent: true }
        assert.deepStrictEqual(await index.partition('u1').first('k1', consistent), undefined)
        assert.deepStrictEqual(await index.partition('u2').first('k1', consistent), undefined)
        assert.deepStrictEqual(await index.partition('u3').first('k1', consistent), undefined)
        assert.deepStrictEqual((await index.partition('u4').first('k1', consistent))?.document, {
            unitId: 'u4',
            n: 4,
        })
    })

    it('still conflicts an update that really lost its race', async () => {
        await using db = schema.tables({})
        const p = db.LaggingDocs.partition('p1')
        const added = await p.add('k1', { unitId: 'u1', n: 1 })
        await p.update('k1', added, { unitId: 'u2', n: 2 })
        await assert.rejects(p.update('k1', added, { unitId: 'u3', n: 3 }), { status: 409 })
    })

    it('lets the retry helpers converge on a row written a moment ago', async () => {
        await using db = schema.tables({})
        const p = db.LaggingDocs.partition('p1')
        await p.add('k1', { unitId: 'u1', n: 1 })
        const row = await p.getOrAdd('k1', { unitId: 'u9', n: 9 })
        assert.deepStrictEqual(row.document, { unitId: 'u1', n: 1 })
        const converged = await p.converge(
            'k1',
            d => d.n === 2,
            { unitId: 'u1', n: 2 },
            d => {
                d.n = 2
            },
        )
        assert.deepStrictEqual(converged.document, { unitId: 'u1', n: 2 })
    })
})
