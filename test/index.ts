import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { setDriver } from '../driver.js'
import { DelayedPersistentMemoryDriver } from '../memory.js'
import { tables } from '../partitioned.js'

describe('schema', () => {
    beforeEach(setMemoryDriver)

    it('should get company settings', async () => {
        type CompanyProfilesSchema = {
            CompanyDocs: {
                settings: {
                    [companyId: string]: {
                        website: string
                        count: number
                    }
                }
                keys: {
                    [companyId: string]: {
                        secret: string
                    }
                }
            }
        }
        function getSettings(context: TestContext) {
            return tables<CompanyProfilesSchema>(context).CompanyDocs.settings
        }
        function getKeys(context: TestContext) {
            return tables<CompanyProfilesSchema>(context).CompanyDocs.keys
        }

        await using context = new TestContext()
        const companyId = 'some-id'

        const settings = getSettings(context)
        await settings.add(companyId, { website: 'abc.com', count: 3 })
        const row = await settings.get(companyId)
        const d = row.document
        d.count += 1
        const rev = await settings.updateRow(row)
        d.count += 1
        await settings.update(companyId, rev, d)
        assert.deepStrictEqual(await settings.getDocument(companyId), {
            website: 'abc.com',
            count: 5,
        })

        const keys = getKeys(context)
        await keys.add(companyId, { secret: 'shh!' })
        const keyRow = await keys.get(companyId)
        keyRow.document.secret = 'yhm'
        await keys.updateRow(keyRow)

        await settings.add('another-id', { website: 'xyz.com', count: 2 })
        await keys.add('another-id', { secret: 'shh!!1!' })

        assert.deepStrictEqual(
            await Array.fromAsync(settings.getRange({ withPrefix: 'another' }), r => r.document),
            [
                {
                    website: 'xyz.com',
                    count: 2,
                },
            ],
        )
    })

    it('should get user profiles', async () => {
        type UsersSchema = {
            UserDocs: {
                [id: string]: {
                    profile: {
                        name: string
                        email: string
                    }
                    invitations: {
                        id: string
                        scopes: string[]
                    }[]
                }
            }
        }
        function getTables(context: TestContext) {
            return tables<UsersSchema>(context)
        }
        function getProfiles(context: TestContext) {
            return getTables(context).UserDocs.withKey('profile')
        }
        function getInvitations(context: TestContext) {
            return getTables(context).UserDocs.withKey('invitations')
        }

        await using context = new TestContext()

        const profiles = getProfiles(context)
        const userId = 'some-id'

        await profiles.add(userId, { name: 'bla', email: 'bla' })

        const invitations = getInvitations(context)
        await invitations.add('invitation ID', [{ id: '', scopes: [] }])
    })

    it('should get messages', async () => {
        type ConversationSchema = {
            Conversations: {
                [userId: string]: {
                    [messageId: string]: {
                        timestamp: string
                        subject: string
                        body: string
                    }
                }
            }
        }
        await using context = new TestContext()

        const conversations = tables<ConversationSchema>(context).Conversations
        const userId = 'some-id'
        const userMessages = conversations.partition(userId)
        const a1 = await userMessages.getOrAddComputed('a', () =>
            Promise.resolve({
                timestamp: '2024-11-19T12:31:00',
                subject: 'huh',
                body: 'hello',
            }),
        )
        const a2 = await userMessages.getOrAdd('a', {
            timestamp: '2024-11-19T12:30:00',
            subject: 'huh',
            body: 'hello',
        })

        assert.deepStrictEqual(a1, a2)

        await userMessages.add('b', {
            timestamp: '2024-11-19T12:30:00',
            subject: 'huh',
            body: 'hello',
        })
        await userMessages.addOrUpdate(
            'c',
            {
                timestamp: '2025-11-19T12:30:00',
                subject: 'huh',
                body: 'hello',
            },
            d => {
                d.body = 'hi'
            },
        )
        await userMessages.addOrUpdate(
            'c',
            {
                timestamp: '2025-11-19T12:30:00',
                subject: 'huh',
                body: 'hello',
            },
            d => {
                d.body = 'hi'
            },
        )
        await userMessages.addOrUpdateComputed(
            'd',
            () => ({
                timestamp: '2025-11-19T12:30:00',
                subject: 'huh',
                body: 'hello',
            }),
            d => {
                d.body = 'hi'
            },
        )
        await userMessages.addOrUpdateComputed(
            'd',
            () => ({
                timestamp: '2025-11-19T12:30:00',
                subject: 'huh',
                body: 'hello',
            }),
            d => {
                d.body = 'hi'
            },
        )
        const range = await Array.fromAsync(userMessages.getRange({ after: 'a' }), r => r.document)
        assert.deepStrictEqual(range, [
            a1.document,
            {
                timestamp: '2024-11-19T12:30:00',
                subject: 'huh',
                body: 'hello',
            },
            {
                timestamp: '2025-11-19T12:30:00',
                subject: 'huh',
                body: 'hi',
            },
            {
                timestamp: '2025-11-19T12:30:00',
                subject: 'huh',
                body: 'hi',
            },
        ])
    })

    it('should count', async () => {
        type CounterSchema = {
            Counters: {
                global: {
                    value: {
                        current: number
                    }
                }
            }
        }

        await using context = new TestContext()

        const { global } = tables<CounterSchema>(context).Counters

        await Promise.all(
            Array.from({ length: 20 }, () =>
                global.addOrUpdate('value', { current: 1 }, doc => {
                    doc.current += 1
                }),
            ),
        )

        assert.deepStrictEqual(await global.getDocument('value'), { current: 20 })
    })

    it('should support idempotency', async () => {
        type CounterSchema = {
            Counters: {
                potent: {
                    [key: string]: {
                        processedMessages: string[]
                        current: number
                    }
                }
            }
        }

        await using context = new TestContext()

        const { potent } = tables<CounterSchema>(context).Counters

        const messageId1 = randomUUID()
        const messageId2 = randomUUID()

        await Promise.all(
            [messageId1, messageId1, messageId2, messageId1, messageId2].map(messageId =>
                potent.converge(
                    'k1',
                    doc => doc.processedMessages.includes(messageId),
                    { processedMessages: [messageId], current: 1 },
                    doc => {
                        if (doc.processedMessages.length === 8) {
                            doc.processedMessages.shift()
                        }
                        doc.processedMessages.push(messageId)
                        doc.current += 1
                    },
                ),
            ),
        )

        const { current } = await potent.getDocument('k1')
        assert.deepStrictEqual(current, 2)
    })

    it('should replace', async () => {
        type ConversationSchema = {
            Conversations: {
                [userId: string]: {
                    [messageId: string]: {
                        timestamp: string
                        subject: string
                        body: string
                    }
                }
            }
        }
        await using context = new TestContext()

        const userMessages = tables<ConversationSchema>(context).Conversations.partition('some-id')
        const replacement = {
            timestamp: '2025-11-19T12:31:00',
            subject: 'nah',
            body: 'bye',
        }
        const added = await userMessages.addOrUpdate(
            'a',
            { timestamp: '2025-11-19T12:30:00', subject: 'huh', body: 'hello' },
            () => replacement,
        )
        assert.deepStrictEqual(added.action, 'add')
        const updated = await userMessages.addOrUpdate(
            'a',
            { timestamp: '2025-11-19T12:30:00', subject: 'huh', body: 'hello' },
            () => replacement,
        )
        assert.deepStrictEqual(updated.action, 'update')
        assert.deepStrictEqual(updated.document, replacement)
        assert.deepStrictEqual(await userMessages.getDocument('a'), replacement)

        const computed = await userMessages.addOrUpdateComputed(
            'a',
            () => replacement,
            existing => ({ ...existing, subject: 'yeah' }),
        )
        assert.deepStrictEqual(computed.action, 'update')
        assert.deepStrictEqual(await userMessages.getDocument('a'), {
            ...replacement,
            subject: 'yeah',
        })
    })

    it('should count with returned documents', async () => {
        type CounterSchema = {
            Counters: {
                global: {
                    value: {
                        current: number
                    }
                }
            }
        }

        await using context = new TestContext()

        const { global } = tables<CounterSchema>(context).Counters

        await Promise.all(
            Array.from({ length: 20 }, () =>
                global.addOrUpdate('value', { current: 1 }, doc => ({
                    current: doc.current + 1,
                })),
            ),
        )

        assert.deepStrictEqual(await global.getDocument('value'), { current: 20 })
    })

    it('should support idempotency with returned documents', async () => {
        type CounterSchema = {
            Counters: {
                potent: {
                    [key: string]: {
                        processedMessages: string[]
                        current: number
                    }
                }
            }
        }

        await using context = new TestContext()

        const { potent } = tables<CounterSchema>(context).Counters

        const messageId1 = randomUUID()
        const messageId2 = randomUUID()

        await Promise.all(
            [messageId1, messageId1, messageId2, messageId1, messageId2].map(messageId =>
                potent.converge(
                    'k1',
                    doc => doc.processedMessages.includes(messageId),
                    { processedMessages: [messageId], current: 1 },
                    doc => ({
                        processedMessages: [...doc.processedMessages.slice(-7), messageId],
                        current: doc.current + 1,
                    }),
                ),
            ),
        )

        const { current } = await potent.getDocument('k1')
        assert.deepStrictEqual(current, 2)
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
        await Promise.allSettled(this.#releasers.map(r => r()))
    }
}

function setMemoryDriver() {
    setDriver(new DelayedPersistentMemoryDriver())
}
