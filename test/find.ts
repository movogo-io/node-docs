import assert from 'node:assert/strict'
import { setDriver, type Connection } from '../driver.js'
import { docs } from '../indexed.js'
import { DelayedPersistentMemoryDriver } from '../memory.js'
import { withTransaction } from '../partitioned.js'

type Schema = {
    Holds: {
        [supplierId: string]: {
            [holdId: string]: { unitId: string; expiresAt?: string }
        }
    }
    Users: {
        [userId: string]: {
            profile: { name: string }
        }
    }
}

const schema = docs<Schema>()
schema.expiry('Holds', hold => {
    if (hold.expiresAt === undefined) {
        return undefined
    }
    return new Date(hold.expiresAt)
})

describe('find', () => {
    beforeEach(() => {
        setDriver(new DelayedPersistentMemoryDriver())
    })

    it('should answer undefined for a missing document', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).Holds.partition('s1')

        assert.deepStrictEqual(await holds.find('h1'), undefined)
        assert.deepStrictEqual(await holds.findEach(['h1', 'h2']), [])
        assert.deepStrictEqual(await holds.findEach([]), [])
    })

    it('should find rows like get does', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).Holds.partition('s1')
        const revision = await holds.add('h1', { unitId: 'u1' })

        assert.deepStrictEqual(await holds.find('h1'), {
            partition: 's1',
            key: 'h1',
            revision,
            document: { unitId: 'u1' },
        })
        assert.deepStrictEqual(await holds.findEach(['h1']), [
            { partition: 's1', key: 'h1', revision, document: { unitId: 'u1' } },
        ])
    })

    it('should treat an expired document as missing', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).Holds.partition('s1')
        await holds.add('h1', { unitId: 'u1', expiresAt: '2026-09-17T12:30Z' })
        const revision = await holds.add('h2', { unitId: 'u2' })

        context.clock = new Date('2026-09-17T12:30Z')
        assert.deepStrictEqual(await holds.find('h1'), undefined)
        assert.deepStrictEqual(await holds.findEach(['h1', 'h2']), [
            { partition: 's1', key: 'h2', revision, document: { unitId: 'u2' } },
        ])
    })

    it('should follow the order of the keys, each distinct key at most once', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).Holds.partition('s1')
        await holds.add('h1', { unitId: 'u1' })
        await holds.add('h2', { unitId: 'u2' })
        await holds.add('h3', { unitId: 'u3' })

        const found = await holds.findEach(['h3', 'missing', 'h1', 'h3', 'h2', 'h1'])
        assert.deepStrictEqual(
            found.map(row => row.key),
            ['h3', 'h1', 'h2'],
        )
    })

    it('should find across partitions of a fixed key', async () => {
        await using context = new TestContext()
        const profiles = schema.tables(context).Users.withKey('profile')
        const revision = await profiles.add('u1', { name: 'Ann' })
        await profiles.add('u2', { name: 'Bob' })

        assert.deepStrictEqual(await profiles.find('u1'), {
            partition: 'u1',
            key: 'profile',
            revision,
            document: { name: 'Ann' },
        })
        assert.deepStrictEqual(await profiles.find('u3'), undefined)
        const found = await profiles.findEach(['u2', 'u3', 'u1'])
        assert.deepStrictEqual(
            found.map(row => row.document),
            [{ name: 'Bob' }, { name: 'Ann' }],
        )
    })

    it('should read committed state inside a transaction', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).Holds.partition('s1')
        const revision = await holds.add('h1', { unitId: 'u1' })
        const profileRevision = await schema
            .tables(context)
            .Users.withKey('profile')
            .add('u1', { name: 'Ann' })

        await withTransaction<Schema>(context, async tx => {
            await tx.Holds.partition('s1').add('h2', { unitId: 'u2' })
            assert.deepStrictEqual(await tx.Holds.partition('s1').find('h2'), undefined)
            assert.deepStrictEqual(await tx.Holds.partition('s1').findEach(['h2', 'h1']), [
                { partition: 's1', key: 'h1', revision, document: { unitId: 'u1' } },
            ])
            assert.deepStrictEqual(await tx.Users.withKey('profile').find('u1'), {
                partition: 'u1',
                key: 'profile',
                revision: profileRevision,
                document: { name: 'Ann' },
            })
            assert.deepStrictEqual(await tx.Users.withKey('profile').findEach(['u2', 'u1']), [
                {
                    partition: 'u1',
                    key: 'profile',
                    revision: profileRevision,
                    document: { name: 'Ann' },
                },
            ])
        })
    })

    it('should fail when a document cannot be read', async () => {
        const driver = new DelayedPersistentMemoryDriver()
        setDriver({
            connect: async () =>
                spyConnection(await driver.connect(), {
                    get: () => Promise.reject(new Error('Throttled')),
                }),
        })
        await using context = new TestContext()
        const holds = schema.tables(context).Holds.partition('s1')

        await assert.rejects(holds.find('h1'), /Throttled/u)
        await assert.rejects(holds.findEach(['h1', 'h2']), /Throttled/u)
        await assert.rejects(
            withTransaction<Schema>(context, async tx => {
                await tx.Holds.partition('s1').findEach(['h1'])
            }),
            /Throttled/u,
        )
    })

    it('should fail when a batch cannot be read', async () => {
        const driver = new DelayedPersistentMemoryDriver()
        setDriver({
            connect: async () =>
                spyConnection(await driver.connect(), {
                    getMany: () => Promise.reject(new Error('Throttled')),
                }),
        })
        await using context = new TestContext()
        const holds = schema.tables(context).Holds.partition('s1')

        await assert.rejects(holds.findEach(['h1', 'h2']), /Throttled/u)
    })

    it('should hand the whole list to a driver with a batch read', async () => {
        const driver = new DelayedPersistentMemoryDriver()
        const batches: { partition: string; key: string }[][] = []
        setDriver({
            connect: async () => {
                const c = await driver.connect()
                return spyConnection(c, {
                    get: () => Promise.reject(new Error('Read one by one')),
                    getMany: (table, refs) => {
                        batches.push([...refs])
                        return c.getMany(table, refs)
                    },
                })
            },
        })
        await using context = new TestContext()
        const holds = schema.tables(context).Holds.partition('s1')
        await holds.add('h1', { unitId: 'u1' })
        await holds.add('h2', { unitId: 'u2' })

        const found = await holds.findEach(['h2', 'h1', 'h2', 'h3'])
        assert.deepStrictEqual(
            found.map(row => row.key),
            ['h2', 'h1'],
        )
        assert.deepStrictEqual(batches, [
            [
                { partition: 's1', key: 'h2' },
                { partition: 's1', key: 'h1' },
                { partition: 's1', key: 'h3' },
            ],
        ])
    })

    it('should read at most 16 at a time from a driver without a batch read', async () => {
        const driver = new DelayedPersistentMemoryDriver()
        const inFlight = { current: 0, max: 0 }
        setDriver({
            connect: async () => {
                const c = await driver.connect()
                return spyConnection(c, {
                    get: async (table, partition, key) => {
                        inFlight.current += 1
                        inFlight.max = Math.max(inFlight.max, inFlight.current)
                        try {
                            return await c.get(table, partition, key)
                        } finally {
                            inFlight.current -= 1
                        }
                    },
                })
            },
        })
        await using context = new TestContext()
        const holds = schema.tables(context).Holds.partition('s1')
        const keys = Array.from({ length: 40 }, (_, i) => `h${i}`)
        for (const key of keys) {
            if (key !== 'h7' && key !== 'h20' && key !== 'h30') {
                await holds.add(key, { unitId: key })
            }
        }
        await holds.add('h20', { unitId: 'u20', expiresAt: '2026-09-17T12:30Z' })

        context.clock = new Date('2026-09-17T12:30Z')
        const found = await holds.findEach(keys)
        assert.deepStrictEqual(
            found.map(row => row.key),
            keys.filter(key => key !== 'h7' && key !== 'h20' && key !== 'h30'),
        )
        assert.deepStrictEqual(inFlight, { current: 0, max: 16 })
    })
})

class TestContext {
    clock = new Date('2026-09-17T12:00:00Z')
    readonly #releasers: (() => Promise<void>)[] = []

    now() {
        return this.clock
    }

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

// Lists the connection's methods rather than spreading it, so a connection
// built here has no batch read unless an override supplies one.
function spyConnection(c: Connection, overrides: Partial<Connection>): Connection {
    return {
        close: () => c.close(),
        add: (table, partition, key, document, options) =>
            c.add(table, partition, key, document, options),
        get: (table, partition, key) => c.get(table, partition, key),
        getPartitions: table => c.getPartitions(table),
        getPartition: (table, partition, range) => c.getPartition(table, partition, range),
        update: (table, partition, key, revision, document, options) =>
            c.update(table, partition, key, revision, document, options),
        delete: (table, partition, key, revision, options) =>
            c.delete(table, partition, key, revision, options),
        transact: (items, options) => c.transact(items, options),
        ...overrides,
    }
}
