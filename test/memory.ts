import assert from 'node:assert/strict'
import { harness } from '../harness.js'
import { MemoryDriver } from '../memory.js'

describe('in-memory driver', () => {
    harness(it, new MemoryDriver(), () => ({}))

    it('keeps expired rows until the clock it is handed says otherwise', async () => {
        const c = await new MemoryDriver().connect({})
        const revision = await c.add('T', 'p', 'k', { data: 'x' }, { now: 0, expiresAt: 1 })
        assert.deepStrictEqual(await c.get('T', 'p', 'k'), {
            partition: 'p',
            key: 'k',
            revision,
            document: { data: 'x' },
            expiresAt: 1,
        })
    })
})
