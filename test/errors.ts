import assert from 'node:assert/strict'
import { isConflict, isNotFound, retryConflict } from '../partitioned.js'

describe('errors', () => {
    it('does not treat a service error with only statusCode as a store error', () => {
        assert.strictEqual(
            isConflict(Object.assign(new Error('Not a draft'), { statusCode: 409 })),
            false,
        )
        assert.strictEqual(
            isNotFound(Object.assign(new Error('No such'), { statusCode: 404 })),
            false,
        )
    })

    it('does not retry a domain conflict thrown inside the closure', async () => {
        const domainConflict = Object.assign(new Error('Not a draft'), { statusCode: 409 })
        let attempts = 0
        await assert.rejects(
            retryConflict(
                () => {
                    attempts++
                    throw domainConflict
                },
                { retries: 3, delay: 1 },
            ),
            domainConflict,
        )
        assert.strictEqual(attempts, 1)
    })
})
