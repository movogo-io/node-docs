import assert from 'node:assert/strict'
import { setDriver, type Connection } from '../driver.js'
import { docs } from '../indexed.js'
import { DelayedPersistentMemoryDriver } from '../memory.js'
import { isConflict, isNotFound, withTransaction } from '../partitioned.js'

type Hold = {
    unitId: string
    expiresAt?: string
}

type Schema = {
    ExpiringHolds: {
        [supplierId: string]: {
            [holdId: string]: Hold
        }
    }
    ExpiringTokens: {
        [userId: string]: {
            token: {
                value: string
                expiresAt: string
            }
        }
    }
    LegacyHolds: {
        [supplierId: string]: {
            [holdId: string]: {
                unitId: string
                validUntil: string
            }
        }
    }
}

const schema = docs<Schema>()
schema.expiry('ExpiringHolds', hold => {
    if (hold.expiresAt === undefined) {
        return undefined
    }
    return new Date(hold.expiresAt)
})
schema.expiry('ExpiringTokens', token => new Date(token.expiresAt))
const byUnit = schema.index(
    'ExpiringHolds',
    'byUnit',
    r => r.document.unitId,
    r => r.key,
)
const legacyByUnit = schema.index(
    'LegacyHolds',
    'byUnit',
    r => r.document.unitId,
    r => r.key,
)

describe('expiry', () => {
    beforeEach(() => {
        setDriver(new DelayedPersistentMemoryDriver())
    })

    it('should reject a second expiry on a table', () => {
        assert.throws(() => {
            schema.expiry('ExpiringHolds', () => undefined)
        }, /already defined/u)
    })

    it('should reject an invalid expiry when writing', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).ExpiringHolds.partition('s1')

        await assert.rejects(
            holds.add('h1', { unitId: 'u1', expiresAt: 'nonsense' }),
            /expiry of a document in table 'ExpiringHolds'/u,
        )
        const revision = await holds.add('h1', { unitId: 'u1', expiresAt: '2026-09-17T12:30Z' })
        await assert.rejects(
            holds.update('h1', revision, { unitId: 'u1', expiresAt: 'nonsense' }),
            /expiry of a document in table 'ExpiringHolds'/u,
        )
        await assert.rejects(
            withTransaction<Schema>(context, async tx => {
                await tx.ExpiringHolds.partition('s1').add('h2', {
                    unitId: 'u1',
                    expiresAt: 'nonsense',
                })
            }),
            /expiry of a document in table 'ExpiringHolds'/u,
        )
        assert.deepStrictEqual(await holds.getDocument('h1'), {
            unitId: 'u1',
            expiresAt: '2026-09-17T12:30Z',
        })
    })

    it('should reject an invalid clock', async () => {
        await using context = new TestContext()
        context.clock = new Date('nonsense')
        const holds = schema.tables(context).ExpiringHolds.partition('s1')

        await assert.rejects(holds.get('h1'), /clock of the context/u)
        await assert.rejects(holds.add('h1', { unitId: 'u1' }), /clock of the context/u)
    })

    it('should fail an add when the existing document cannot be read', async () => {
        const driver = new DelayedPersistentMemoryDriver()
        setDriver({
            connect: async () =>
                spyConnection(await driver.connect(), {
                    get: () => Promise.reject(new Error('Throttled')),
                }),
        })
        await using context = new TestContext()
        const holds = schema.tables(context).ExpiringHolds.partition('s1')

        await assert.rejects(holds.add('h1', { unitId: 'u1' }), /Throttled/u)
        await assert.rejects(holds.getOrAdd('h2', { unitId: 'u1' }), /Throttled/u)
        await assert.rejects(
            withTransaction<Schema>(context, async tx => {
                await tx.ExpiringHolds.partition('s1').add('h3', { unitId: 'u1' })
            }),
            /Throttled/u,
        )
        const c = await driver.connect()
        assert.deepStrictEqual(await Array.fromAsync(c.getPartition('ExpiringHolds', 's1')), [])
    })

    it('should not find an expired document', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).ExpiringHolds.partition('s1')
        await holds.add('h1', { unitId: 'u1', expiresAt: '2026-09-17T12:30Z' })

        context.clock = new Date('2026-09-17T12:29:59Z')
        assert.deepStrictEqual(await holds.getDocument('h1'), {
            unitId: 'u1',
            expiresAt: '2026-09-17T12:30Z',
        })

        context.clock = new Date('2026-09-17T12:30:00Z')
        await assert.rejects(holds.get('h1'), isNotFound)
        await assert.rejects(holds.getDocument('h1'), isNotFound)
    })

    it('should hide a document added with a past expiry', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).ExpiringHolds.partition('s1')
        await holds.add('h1', { unitId: 'u1', expiresAt: '2026-09-17T11:00Z' })

        await assert.rejects(holds.get('h1'), isNotFound)
        const revision = await holds.add('h1', { unitId: 'u2' })
        assert.deepStrictEqual(await holds.get('h1'), {
            partition: 's1',
            key: 'h1',
            revision,
            document: { unitId: 'u2' },
        })
    })

    it('should never expire a document without an expiry', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).ExpiringHolds.partition('s1')
        await holds.add('h1', { unitId: 'u1' })

        context.clock = new Date('2999-12-31T23:59:59Z')
        assert.deepStrictEqual(await holds.getDocument('h1'), { unitId: 'u1' })
    })

    it('should use the wall clock when the context has none', async () => {
        await using db = schema.tables({})
        const holds = db.ExpiringHolds.partition('s1')
        await holds.add('past', { unitId: 'u1', expiresAt: '2000-01-01T00:00Z' })
        await holds.add('future', { unitId: 'u1', expiresAt: '2999-01-01T00:00Z' })

        await assert.rejects(holds.get('past'), isNotFound)
        assert.deepStrictEqual(await holds.getDocument('future'), {
            unitId: 'u1',
            expiresAt: '2999-01-01T00:00Z',
        })
    })

    it('should return rows without their expiry', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).ExpiringHolds.partition('s1')
        const document = { unitId: 'u1', expiresAt: '2026-09-17T12:30Z' }
        const revision = await holds.add('h1', document)

        assert.deepStrictEqual(await holds.get('h1'), {
            partition: 's1',
            key: 'h1',
            revision,
            document,
        })
        assert.deepStrictEqual(await Array.fromAsync(holds.getAll()), [
            { key: 'h1', revision, document },
        ])
        assert.deepStrictEqual(await holds.getOrAdd('h1', { unitId: 'u2' }), {
            partition: 's1',
            key: 'h1',
            revision,
            document,
        })
        assert.deepStrictEqual(await byUnit(context).partition('u1').first('h1'), {
            key: 'h1',
            revision,
            document,
            source: { partition: 's1', key: 'h1' },
        })
        await withTransaction<Schema>(context, async tx => {
            assert.deepStrictEqual(await tx.ExpiringHolds.partition('s1').get('h1'), {
                partition: 's1',
                key: 'h1',
                revision,
                document,
            })
        })
    })

    it('should expire from the start of the second', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).ExpiringHolds.partition('s1')
        await holds.add('h1', { unitId: 'u1', expiresAt: '2026-09-17T12:30:00.500Z' })
        await holds.add('h2', { unitId: 'u1', expiresAt: '2026-09-17T12:30:00.000Z' })

        context.clock = new Date('2026-09-17T12:29:59.999Z')
        assert.deepStrictEqual(await Array.fromAsync(holds.getAll(), r => r.key), ['h1', 'h2'])

        context.clock = new Date('2026-09-17T12:30:00.000Z')
        assert.deepStrictEqual(await Array.fromAsync(holds.getAll(), r => r.key), [])
    })

    it('should skip expired documents in ranges', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).ExpiringHolds.partition('s1')
        await holds.add('h1', { unitId: 'u1', expiresAt: '2026-09-17T12:30Z' })
        await holds.add('h2', { unitId: 'u1', expiresAt: '2026-09-17T13:00Z' })
        await holds.add('h3', { unitId: 'u1' })

        context.clock = new Date('2026-09-17T12:30Z')
        assert.deepStrictEqual(await Array.fromAsync(holds.getAll(), r => r.key), ['h2', 'h3'])
        assert.deepStrictEqual(
            await Array.fromAsync(holds.getRange({ withPrefix: 'h' }), r => r.key),
            ['h2', 'h3'],
        )
    })

    it('should skip expired documents in index lookups', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).ExpiringHolds.partition('s1')
        await holds.add('h1', { unitId: 'u1', expiresAt: '2026-09-17T12:30Z' })
        await holds.add('h2', { unitId: 'u1', expiresAt: '2026-09-17T13:00Z' })

        context.clock = new Date('2026-09-17T12:30Z')
        assert.strictEqual(await byUnit(context).partition('u1').first('h1'), undefined)
        assert.strictEqual(await byUnit(context).partition('u1').firstDocument('h1'), undefined)
        assert.deepStrictEqual(
            await Array.fromAsync(
                byUnit(context).partition('u1').getRange({ withPrefix: '' }),
                r => r.key,
            ),
            ['h2'],
        )
    })

    it('should hide an expired document under a fixed key', async () => {
        await using context = new TestContext()
        const tokens = schema.tables(context).ExpiringTokens.withKey('token')
        await tokens.add('u1', { value: 'a', expiresAt: '2026-09-17T12:30Z' })

        context.clock = new Date('2026-09-17T12:30Z')
        await assert.rejects(tokens.get('u1'), isNotFound)
        await assert.rejects(tokens.getDocument('u1'), isNotFound)
        const added = await tokens.addOrUpdate(
            'u1',
            { value: 'b', expiresAt: '2026-09-17T13:00Z' },
            existing => {
                existing.value = 'updated'
            },
        )
        assert.strictEqual(added.action, 'add')
        assert.deepStrictEqual(await tokens.getDocument('u1'), {
            value: 'b',
            expiresAt: '2026-09-17T13:00Z',
        })
    })

    it('should skip expired documents inside transactions', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).ExpiringHolds.partition('s1')
        await holds.add('h1', { unitId: 'u1', expiresAt: '2026-09-17T12:30Z' })
        await schema
            .tables(context)
            .ExpiringTokens.withKey('token')
            .add('u1', { value: 'a', expiresAt: '2026-09-17T12:30Z' })

        context.clock = new Date('2026-09-17T12:30Z')
        await withTransaction<Schema>(context, async tx => {
            await assert.rejects(tx.ExpiringHolds.partition('s1').get('h1'), isNotFound)
            await assert.rejects(tx.ExpiringHolds.partition('s1').getDocument('h1'), isNotFound)
            assert.deepStrictEqual(
                await Array.fromAsync(tx.ExpiringHolds.partition('s1').getAll()),
                [],
            )
            assert.deepStrictEqual(
                await Array.fromAsync(
                    tx.ExpiringHolds.partition('s1').getRange({ withPrefix: 'h' }),
                ),
                [],
            )
            await assert.rejects(tx.ExpiringTokens.withKey('token').get('u1'), isNotFound)
            await tx.ExpiringHolds.partition('s1').add('h1', { unitId: 'u2' })
        })

        assert.deepStrictEqual(await holds.getDocument('h1'), { unitId: 'u2' })
    })

    it('should apply the expiry to updates inside transactions', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).ExpiringHolds.partition('s1')
        const revision = await holds.add('h1', { unitId: 'u1' })

        await assert.rejects(
            withTransaction<Schema>(context, async tx => {
                await tx.ExpiringHolds.partition('s1').update('h1', revision, {
                    unitId: 'u1',
                    expiresAt: 'nonsense',
                })
            }),
            /expiry of a document in table 'ExpiringHolds'/u,
        )
        await withTransaction<Schema>(context, async tx => {
            await tx.ExpiringHolds.partition('s1').update('h1', revision, {
                unitId: 'u1',
                expiresAt: '2026-09-17T12:30Z',
            })
        })
        assert.deepStrictEqual(await holds.getDocument('h1'), {
            unitId: 'u1',
            expiresAt: '2026-09-17T12:30Z',
        })

        context.clock = new Date('2026-09-17T12:30Z')
        await assert.rejects(holds.get('h1'), isNotFound)
    })

    it('should reject updating and deleting an expired document', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).ExpiringHolds.partition('s1')
        await holds.add('h1', { unitId: 'u1', expiresAt: '2026-09-17T12:30Z' })
        const row = await holds.get('h1')

        context.clock = new Date('2026-09-17T12:30Z')
        await assert.rejects(holds.update('h1', row.revision, { unitId: 'u2' }), isConflict)
        await assert.rejects(holds.updateRow(row), isConflict)
        await assert.rejects(holds.delete('h1', row.revision), isConflict)
    })

    it('should keep showing a document whose update extended its expiry', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).ExpiringHolds.partition('s1')
        await holds.add('h1', { unitId: 'u1', expiresAt: '2026-09-17T12:30Z' })
        const row = await holds.get('h1')
        await holds.update('h1', row.revision, { unitId: 'u1', expiresAt: '2026-09-17T13:00Z' })

        context.clock = new Date('2026-09-17T12:45Z')
        assert.deepStrictEqual(await holds.getDocument('h1'), {
            unitId: 'u1',
            expiresAt: '2026-09-17T13:00Z',
        })

        context.clock = new Date('2026-09-17T13:00Z')
        await assert.rejects(holds.get('h1'), isNotFound)
    })

    it('should re-add over an expired document', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).ExpiringHolds.partition('s1')
        await holds.add('h1', { unitId: 'u1', expiresAt: '2026-09-17T12:30Z' })

        context.clock = new Date('2026-09-17T12:30Z')
        const revision = await holds.add('h1', { unitId: 'u2' })

        assert.deepStrictEqual(await holds.get('h1'), {
            partition: 's1',
            key: 'h1',
            revision,
            document: { unitId: 'u2' },
        })
        assert.deepStrictEqual((await byUnit(context).partition('u2').first('h1'))?.document, {
            unitId: 'u2',
        })
    })

    it('should clear the index entries of an expired document when re-adding', async () => {
        const driver = new DelayedPersistentMemoryDriver()
        setDriver(driver)
        await using context = new TestContext()
        const holds = schema.tables(context).ExpiringHolds.partition('s1')
        await holds.add('h1', { unitId: 'u1', expiresAt: '2026-09-17T12:30Z' })

        context.clock = new Date('2026-09-17T12:30Z')
        await holds.add('h1', { unitId: 'u2', expiresAt: '2999-01-01T00:00Z' })

        assert.strictEqual(await byUnit(context).partition('u1').first('h1'), undefined)
        const c = await driver.connect()
        assert.deepStrictEqual(
            (await c.get('ExpiringHolds.byUnit', 'u2', 'h1\u{0}s1\u{0}h1')).document,
            { unitId: 'u2', expiresAt: '2999-01-01T00:00Z' },
        )
        await assert.rejects(c.get('ExpiringHolds.byUnit', 'u1', 'h1\u{0}s1\u{0}h1'), isNotFound)
    })

    it('should read a key once when adding over an expired document', async () => {
        const driver = new DelayedPersistentMemoryDriver()
        const reads: string[] = []
        setDriver({
            connect: async () => {
                const c = await driver.connect()
                return spyConnection(c, {
                    get: async (table, partition, key) => {
                        reads.push(key)
                        return await c.get(table, partition, key)
                    },
                })
            },
        })
        await using context = new TestContext()
        const holds = schema.tables(context).ExpiringHolds.partition('s1')
        for (const key of ['h1', 'h2', 'h3']) {
            await holds.add(key, { unitId: 'u1', expiresAt: '2026-09-17T12:30Z' })
        }

        context.clock = new Date('2026-09-17T12:30Z')
        reads.length = 0
        await holds.add('h1', { unitId: 'u2' })
        await holds.getOrAdd('h2', { unitId: 'u2' })
        await holds.addOrUpdate('h3', { unitId: 'u2' }, existing => {
            existing.unitId = 'updated'
        })

        assert.deepStrictEqual(reads, ['h1', 'h2', 'h3'])
    })

    it('should omit the expiry from writes of documents that do not expire', async () => {
        const driver = new DelayedPersistentMemoryDriver()
        const written: [string, boolean][] = []
        setDriver({
            connect: async () => {
                const c = await driver.connect()
                return spyConnection(c, {
                    transact: async (items, options) => {
                        written.push(
                            ...items.map((item): [string, boolean] => [
                                item.op,
                                Object.hasOwn(item, 'expiresAt'),
                            ]),
                        )
                        await c.transact(items, options)
                    },
                })
            },
        })
        await using context = new TestContext()
        const holds = schema.tables(context).ExpiringHolds.partition('s1')

        const revision = await holds.add('h1', { unitId: 'u1' })
        await holds.update('h1', revision, { unitId: 'u2' })

        assert.deepStrictEqual(written, [
            ['add', false],
            ['put', false],
            ['update', false],
            ['put', false],
            ['clear', false],
        ])
    })

    it('should re-add over an expired document through the retry helpers', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).ExpiringHolds.partition('s1')
        for (const key of ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']) {
            await holds.add(key, { unitId: 'u1', expiresAt: '2026-09-17T12:30Z' })
        }

        context.clock = new Date('2026-09-17T12:30Z')
        const got = await holds.getOrAdd('h1', { unitId: 'u2' })
        assert.deepStrictEqual(got.document, { unitId: 'u2' })
        const added = await holds.addOrUpdate('h2', { unitId: 'u2' }, existing => {
            existing.unitId = 'updated'
        })
        assert.strictEqual(added.action, 'add')
        const converged = await holds.converge(
            'h3',
            hold => hold.unitId === 'u2',
            { unitId: 'u2' },
            existing => {
                existing.unitId = 'u2'
            },
        )
        assert.deepStrictEqual(converged.document, { unitId: 'u2' })
        const gotComputed = await holds.getOrAddComputed('h4', () => ({ unitId: 'u2' }))
        assert.deepStrictEqual(gotComputed.document, { unitId: 'u2' })
        const addedComputed = await holds.addOrUpdateComputed(
            'h5',
            () => ({ unitId: 'u2' }),
            existing => {
                existing.unitId = 'updated'
            },
        )
        assert.strictEqual(addedComputed.action, 'add')
        const convergedComputed = await holds.convergeComputed(
            'h6',
            hold => hold.unitId === 'u2',
            () => ({ unitId: 'u2' }),
            existing => {
                existing.unitId = 'u2'
            },
        )
        assert.deepStrictEqual(convergedComputed.document, { unitId: 'u2' })

        assert.deepStrictEqual(
            await Array.fromAsync(holds.getAll(), r => r.document),
            Array.from({ length: 6 }, () => ({ unitId: 'u2' })),
        )
    })

    it('should keep documents written before the expiry was declared', async () => {
        await using context = new TestContext()
        const holds = schema.tables(context).LegacyHolds.partition('s1')
        const revision = await holds.add('h1', { unitId: 'u1', validUntil: 'never' })

        // Declared here, after the write, so LegacyHolds must not be used by any other test.
        schema.expiry('LegacyHolds', hold => new Date(hold.validUntil))

        context.clock = new Date('2999-12-31T23:59:59Z')
        assert.deepStrictEqual(await holds.get('h1'), {
            partition: 's1',
            key: 'h1',
            revision,
            document: { unitId: 'u1', validUntil: 'never' },
        })
        assert.deepStrictEqual(await Array.fromAsync(holds.getAll(), r => r.document), [
            { unitId: 'u1', validUntil: 'never' },
        ])
        assert.deepStrictEqual(
            (await legacyByUnit(context).partition('u1').first('h1'))?.document,
            { unitId: 'u1', validUntil: 'never' },
        )
        await assert.rejects(
            holds.update('h1', revision, { unitId: 'u1', validUntil: 'never' }),
            /expiry of a document in table 'LegacyHolds'/u,
        )

        context.clock = new Date('2026-09-17T12:00Z')
        await holds.update('h1', revision, { unitId: 'u1', validUntil: '2026-09-17T12:30Z' })
        assert.deepStrictEqual(await holds.getDocument('h1'), {
            unitId: 'u1',
            validUntil: '2026-09-17T12:30Z',
        })

        context.clock = new Date('2026-09-17T12:30Z')
        await assert.rejects(holds.get('h1'), isNotFound)
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
