import assert from 'node:assert/strict'
import { setDriver } from '../driver.js'
import { docs } from '../indexed.js'
import { DelayedPersistentMemoryDriver } from '../memory.js'
import { compositeKey, compositeRange } from '../partitioned.js'

type Schema = {
    KeyedUnits: {
        [supplierId: string]: {
            [unitId: string]: { equipmentId?: unknown; depotId?: unknown }
        }
    }
}

const schema = docs<Schema>()
const unitsByEquipmentAndDepot = schema.index(
    'KeyedUnits',
    'byEquipment',
    r => r.partition,
    r => compositeKey(r.document.equipmentId, r.document.depotId, r.key),
)

describe('keys', () => {
    beforeEach(() => {
        setDriver(new DelayedPersistentMemoryDriver())
    })

    it('should join the parts with the separator', () => {
        assert.strictEqual(compositeKey('s1'), 's1')
        assert.strictEqual(compositeKey('s1', 'e1', 'd1'), 's1#e1#d1')
    })

    it('should name no key for a part that is not a non-empty string', () => {
        assert.strictEqual(compositeKey('s1', ''), undefined)
        assert.strictEqual(compositeKey('s1', undefined), undefined)
        assert.strictEqual(compositeKey('s1', 3), undefined)
        assert.strictEqual(compositeRange(['s1', '']), undefined)
    })

    it('should escape so that no two part lists share a key', () => {
        assert.strictEqual(compositeKey('a#b', 'c'), 'a%23b#c')
        assert.strictEqual(compositeKey('a', 'b#c'), 'a#b%23c')
        assert.strictEqual(compositeKey('a%23b'), 'a%2523b')
        assert.strictEqual(compositeKey('a\u{0}b'), 'a%00b')
    })

    it('should range over the keys of a prefix and nothing else', () => {
        assert.deepStrictEqual(compositeRange(['e1']), { after: 'e1#', before: 'e1$' })
        assert.deepStrictEqual(compositeRange(['e1'], 'e1#d2'), { after: 'e1#d2', before: 'e1$' })
        assert.deepStrictEqual(compositeRange(['e1'], 'd9'), { after: 'e1#', before: 'e1$' })
        assert.strictEqual(compositeRange(['e1'], 'e1$'), undefined)
        assert.strictEqual(compositeRange(['e1'], 'f'), undefined)
    })

    it('should page one prefix of an index whatever characters the ids hold', async () => {
        await using context = new TestContext()
        const units = schema.tables(context).KeyedUnits.partition('s1')
        await units.add('u1', { equipmentId: 'e', depotId: 'd1' })
        await units.add('u2', { equipmentId: 'e', depotId: 'd2' })
        await units.add('u3', { equipmentId: 'e#x', depotId: 'd1' })
        await units.add('u4', { equipmentId: 'e$', depotId: 'd1' })
        await units.add('u5', { equipmentId: 'e' })
        const index = unitsByEquipmentAndDepot(context).partition('s1')

        const range = compositeRange(['e'])
        assert.ok(range)
        const all = await Array.fromAsync(index.getRange(range))
        assert.deepStrictEqual(
            all.map(row => row.source.key),
            ['u1', 'u2'],
        )

        const next = compositeRange(['e'], all[1]?.key)
        assert.ok(next)
        assert.deepStrictEqual(
            (await Array.fromAsync(index.getRange(next))).map(row => row.source.key),
            ['u2'],
        )
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
        await Promise.all(this.#releasers.map(release => release()))
    }
}
