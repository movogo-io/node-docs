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

const seen: string[] = []
decorateDriver(recording('early'))

describe('driver decoration', () => {
    beforeEach(() => {
        seen.length = 0
    })

    it('applies decorators registered before the driver is set', async () => {
        setDriver(new MemoryDriver())
        await using db = tables<Schema>({})
        await db.DecoratedDocs.partition('p1').add('k1', { n: 1 })
        assert.deepStrictEqual(seen, ['early add DecoratedDocs'])
    })

    it('applies decorators registered after the driver is set', async () => {
        setDriver(new MemoryDriver())
        decorateDriver(recording('late'))
        await using db = tables<Schema>({})
        await db.DecoratedDocs.partition('p1').add('k1', { n: 1 })
        assert.deepStrictEqual(seen, ['late add DecoratedDocs', 'early add DecoratedDocs'])
    })

    it('keeps decorators when the driver is replaced', async () => {
        setDriver(new MemoryDriver())
        await using db = tables<Schema>({})
        await db.DecoratedDocs.partition('p1').add('k1', { n: 1 })
        assert.deepStrictEqual(seen, ['late add DecoratedDocs', 'early add DecoratedDocs'])
    })
})

function recording(label: string) {
    return (driver: Driver): Driver => ({
        connect: async context => delegating(label, await driver.connect(context)),
    })
}

function delegating(label: string, inner: Connection): Connection {
    return {
        close: () => inner.close(),
        add: (table, partition, key, document) => {
            seen.push(`${label} add ${table}`)
            return inner.add(table, partition, key, document)
        },
        get: (table, partition, key) => inner.get(table, partition, key),
        getPartitions: table => inner.getPartitions(table),
        getPartition: (table, partition, range) => inner.getPartition(table, partition, range),
        update: (table, partition, key, revision, document) =>
            inner.update(table, partition, key, revision, document),
        delete: (table, partition, key, revision) => inner.delete(table, partition, key, revision),
        transact: items => inner.transact(items),
    }
}
