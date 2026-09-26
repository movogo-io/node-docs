import assert from 'node:assert/strict'
import { decorateDriver, setDriver, type Connection, type Driver } from '../driver.js'
import { docs } from '../indexed.js'
import { PersistentMemoryDriver } from '../memory.js'
import { withTransaction } from '../partitioned.js'

type Schema = {
    ConsistentDocs: {
        [partition: string]: {
            [key: string]: { unitId: string }
        }
    }
    ConsistentUsers: {
        [id: string]: {
            profile: { name: string }
        }
    }
}

const schema = docs<Schema>()
const byUnit = schema.index(
    'ConsistentDocs',
    'byUnit',
    r => r.document.unitId,
    r => r.key,
)

type Read = [method: string, options: unknown]

// The option is only worth anything if it reaches the driver, so every read
// surface is checked for passing it through, and for passing nothing when
// nothing was asked.
describe('consistent reads', () => {
    const reads: Read[] = []
    let remove: (() => void) | undefined

    beforeEach(() => {
        setDriver(new PersistentMemoryDriver())
        remove = decorateDriver(recording(reads))
    })

    afterEach(() => {
        remove?.()
        reads.length = 0
    })

    it('reaches the connection from a partition', async () => {
        await using db = schema.tables({})
        const p = db.ConsistentDocs.partition('p1')
        await p.add('k1', { unitId: 'u1' })
        reads.length = 0

        await p.get('k1', { consistent: true })
        await p.getDocument('k1', { consistent: true })
        assert.ok(await p.find('k1', { consistent: true }))
        await p.findEach(['k1'], { consistent: true })
        await Array.fromAsync(p.getAll({ consistent: true }))
        await Array.fromAsync(p.getRange({ withPrefix: 'k' }, { consistent: true }))
        await p.get('k1')
        await Array.fromAsync(p.getAll())

        assert.deepStrictEqual(reads, [
            ['get', { consistent: true }],
            ['get', { consistent: true }],
            ['get', { consistent: true }],
            ['getMany', { consistent: true }],
            ['getPartition', { consistent: true }],
            ['getPartition', { consistent: true }],
            ['get', undefined],
            ['getPartition', undefined],
        ])
    })

    it('reaches the connection from a fixed key', async () => {
        await using db = schema.tables({})
        const profiles = db.ConsistentUsers.withKey('profile')
        await profiles.add('u1', { name: 'a' })
        reads.length = 0

        await profiles.get('u1', { consistent: true })
        await profiles.getDocument('u1', { consistent: true })
        assert.ok(await profiles.find('u1', { consistent: true }))
        await profiles.findEach(['u1'], { consistent: true })

        assert.deepStrictEqual(reads, [
            ['get', { consistent: true }],
            ['get', { consistent: true }],
            ['get', { consistent: true }],
            ['getMany', { consistent: true }],
        ])
    })

    it('reaches the connection from a transaction', async () => {
        await using db = schema.tables({})
        await db.ConsistentDocs.partition('p1').add('k1', { unitId: 'u1' })
        await db.ConsistentUsers.withKey('profile').add('u1', { name: 'a' })
        reads.length = 0

        await withTransaction<Schema>({}, async tx => {
            const p = tx.ConsistentDocs.partition('p1')
            await p.get('k1', { consistent: true })
            assert.ok(await p.find('k1', { consistent: true }))
            await p.findEach(['k1'], { consistent: true })
            await Array.fromAsync(p.getAll({ consistent: true }))
            await Array.fromAsync(p.getRange({ withPrefix: 'k' }, { consistent: true }))
            await tx.ConsistentUsers.withKey('profile').get('u1', { consistent: true })
            assert.ok(await tx.ConsistentUsers.withKey('profile').find('u1', { consistent: true }))
            await tx.ConsistentUsers.withKey('profile').findEach(['u1'], { consistent: true })
        })

        assert.deepStrictEqual(reads, [
            ['get', { consistent: true }],
            ['get', { consistent: true }],
            ['getMany', { consistent: true }],
            ['getPartition', { consistent: true }],
            ['getPartition', { consistent: true }],
            ['get', { consistent: true }],
            ['get', { consistent: true }],
            ['getMany', { consistent: true }],
        ])
    })

    it('reaches the connection from an index', async () => {
        await using db = schema.tables({})
        await db.ConsistentDocs.partition('p1').add('k1', { unitId: 'u1' })
        reads.length = 0

        await using index = byUnit({})
        await index.partition('u1').first('k1', { consistent: true })
        await index.partition('u1').firstDocument('k1', { consistent: true })
        await Array.fromAsync(
            index.partition('u1').getRange({ withPrefix: 'k' }, { consistent: true }),
        )
        await index.partition('u1').first('k1')

        assert.deepStrictEqual(reads, [
            ['getPartition', { consistent: true }],
            ['getPartition', { consistent: true }],
            ['getPartition', { consistent: true }],
            ['getPartition', undefined],
        ])
    })

    it('reaches the read a retry helper makes before it writes', async () => {
        await using db = schema.tables({})
        const p = db.ConsistentDocs.partition('p1')

        await p.getOrAdd('k1', { unitId: 'u1' }, { consistent: true })
        await p.addOrUpdate('k1', { unitId: 'u1' }, () => undefined, { consistent: true })
        await p.converge(
            'k1',
            () => true,
            { unitId: 'u1' },
            () => undefined,
            { consistent: true },
        )
        await p.getOrAdd('k2', { unitId: 'u1' })

        assert.deepStrictEqual(
            reads.filter(([method]) => method === 'get'),
            [
                ['get', { consistent: true }],
                ['get', { consistent: true }],
                ['get', { consistent: true }],
                ['get', undefined],
            ],
        )
    })
})

function recording(reads: Read[]) {
    return (driver: Driver): Driver => ({
        connect: async context => observing(reads, await driver.connect(context)),
    })
}

function observing(reads: Read[], inner: Connection): Connection {
    return {
        close: () => inner.close(),
        add: (table, partition, key, document, options) =>
            inner.add(table, partition, key, document, options),
        get: (table, partition, key, options) => {
            reads.push(['get', options])
            return inner.get(table, partition, key, options)
        },
        getMany: (table, refs, options) => {
            reads.push(['getMany', options])
            return inner.getMany?.(table, refs, options) ?? Promise.resolve([])
        },
        getPartitions: table => inner.getPartitions(table),
        getPartition: (table, partition, range, options) => {
            reads.push(['getPartition', options])
            return inner.getPartition(table, partition, range, options)
        },
        update: (table, partition, key, revision, document, options) =>
            inner.update(table, partition, key, revision, document, options),
        delete: (table, partition, key, revision, options) =>
            inner.delete(table, partition, key, revision, options),
        transact: (items, options) => inner.transact(items, options),
    }
}
