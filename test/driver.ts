import assert from 'node:assert/strict'
import { decorateDriver, setDriver, type Connection, type Driver } from '../driver.js'
import { MemoryDriver } from '../memory.js'
import { tables } from '../partitioned.js'

type Schema = {
    DecoratedDocs: {
        [partition: string]: {
            [key: string]: { n: number }
        }
    }
}

describe('driver decoration', () => {
    const seen: string[] = []
    const removers: (() => void)[] = []

    afterEach(() => {
        for (const remove of removers) {
            remove()
        }
        removers.length = 0
        seen.length = 0
    })

    it('applies decorators registered before the driver is set', async () => {
        removers.push(decorateDriver(recording('early', seen)))
        setDriver(new MemoryDriver())
        await using db = tables<Schema>({})
        await db.DecoratedDocs.partition('p1').add('k1', { n: 1 })
        assert.deepStrictEqual(seen, ['early add DecoratedDocs'])
    })

    it('applies decorators registered after the driver is set', async () => {
        setDriver(new MemoryDriver())
        removers.push(decorateDriver(recording('late', seen)))
        await using db = tables<Schema>({})
        await db.DecoratedDocs.partition('p1').add('k1', { n: 1 })
        assert.deepStrictEqual(seen, ['late add DecoratedDocs'])
    })

    it('wraps later decorators around earlier ones', async () => {
        removers.push(
            decorateDriver(recording('inner', seen)),
            decorateDriver(recording('outer', seen)),
        )
        setDriver(new MemoryDriver())
        await using db = tables<Schema>({})
        await db.DecoratedDocs.partition('p1').add('k1', { n: 1 })
        assert.deepStrictEqual(seen, ['outer add DecoratedDocs', 'inner add DecoratedDocs'])
    })

    it('keeps decorators when the driver is replaced', async () => {
        removers.push(decorateDriver(recording('kept', seen)))
        setDriver(new MemoryDriver())
        setDriver(new MemoryDriver())
        await using db = tables<Schema>({})
        await db.DecoratedDocs.partition('p1').add('k1', { n: 1 })
        assert.deepStrictEqual(seen, ['kept add DecoratedDocs'])
    })

    it('stops applying removed decorators', async () => {
        const remove = decorateDriver(recording('removed', seen))
        setDriver(new MemoryDriver())
        remove()
        await using db = tables<Schema>({})
        await db.DecoratedDocs.partition('p1').add('k1', { n: 1 })
        assert.deepStrictEqual(seen, [])
    })
})

function recording(label: string, seen: string[]) {
    return (driver: Driver): Driver => ({
        connect: async context => delegating(label, seen, await driver.connect(context)),
    })
}

function delegating(label: string, seen: string[], inner: Connection): Connection {
    return {
        close: () => inner.close(),
        add: (table, partition, key, document, options) => {
            seen.push(`${label} add ${table}`)
            return inner.add(table, partition, key, document, options)
        },
        get: (table, partition, key) => inner.get(table, partition, key),
        getPartitions: table => inner.getPartitions(table),
        getPartition: (table, partition, range) => inner.getPartition(table, partition, range),
        update: (table, partition, key, revision, document, options) =>
            inner.update(table, partition, key, revision, document, options),
        delete: (table, partition, key, revision, options) =>
            inner.delete(table, partition, key, revision, options),
        transact: (items, options) => inner.transact(items, options),
    }
}
