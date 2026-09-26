import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import type { Driver } from './lib/driver.js'
import { isConflict, isNotFound } from './partitioned.js'

const table = 'HarnessTestDocs'
const otherTable = 'HarnessTestDocs2'

// A fixed clock keeps the cases deterministic. It sits in the future so that a
// driver deleting expired rows lazily by wall-clock time (DynamoDB TTL) never
// removes a row mid-case.
const now = 4_000_000_000
// What a write at `now`, and one at `now + 60`, stamps as `updatedAt`.
const updatedAt = '2096-10-02T07:06:40.000Z'
const updatedAtLater = '2096-10-02T07:07:40.000Z'

export function harness(
    it: (message: string, runner: () => Promise<void>) => void,
    driver: Driver,
    contextFactory: () => object,
) {
    it('throws not found when not added', async () => {
        await using c = await connect(driver, contextFactory)
        await assert.rejects(c.docs.get(table, anId(), anId()), isNotFound)
    })

    it('carries the HTTP status code on not found and conflicts', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        await assert.rejects(c.docs.get(table, partition, key), { statusCode: 404 })
        await c.docs.add(table, partition, key, added, { now })
        await assert.rejects(c.docs.add(table, partition, key, added, { now }), {
            statusCode: 409,
        })
    })

    it('leaves other connections from the same context open when one closes', async () => {
        const { partition, key, document: added } = aRow()
        const context = contextFactory()
        const closed = await driver.connect(context)
        await using c = await connectTo(driver, context)
        await closed.close()
        await c.docs.add(table, partition, key, added, { now })
        assert.deepStrictEqual((await c.docs.get(table, partition, key)).document, added)
    })

    it('answers a read asked to be consistent with the same rows as one that is not', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        await c.docs.add(table, partition, key, added, { now })
        assert.deepStrictEqual(
            await c.docs.get(table, partition, key, { consistent: true }),
            await c.docs.get(table, partition, key),
        )
        assert.deepStrictEqual(
            await c.docs.get(table, partition, key, { consistent: false }),
            await c.docs.get(table, partition, key),
        )
        if (c.docs.getMany) {
            assert.deepStrictEqual(
                await c.docs.getMany(table, [{ partition, key }], { consistent: true }),
                await c.docs.getMany(table, [{ partition, key }]),
            )
        }
        assert.deepStrictEqual(
            await Array.fromAsync(
                c.docs.getPartition(table, partition, undefined, { consistent: true }),
            ),
            await Array.fromAsync(c.docs.getPartition(table, partition)),
        )
        assert.deepStrictEqual(
            await Array.fromAsync(
                c.docs.getPartition(table, partition, { withPrefix: key }, { consistent: true }),
            ),
            await Array.fromAsync(c.docs.getPartition(table, partition, { withPrefix: key })),
        )
        assert.strictEqual(
            (
                await Array.fromAsync(
                    c.docs.getPartition(table, partition, undefined, { consistent: true }),
                )
            ).length,
            1,
        )
    })

    it('gets added, as the add answered it', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const { revision, ...written } = await c.docs.add(table, partition, key, added, { now })
        assert.deepStrictEqual(written, { seq: 0, updatedAt })
        const row = { partition, key, revision, document: added, seq: 0, updatedAt }
        assert.deepStrictEqual(await c.docs.get(table, partition, key), row)
        if (c.docs.getMany) {
            assert.deepStrictEqual(await c.docs.getMany(table, [{ partition, key }]), [row])
        }
        assert.deepStrictEqual(await Array.fromAsync(c.docs.getPartition(table, partition)), [
            { key, revision, document: added, seq: 0, updatedAt },
        ])
    })

    it('gets JSON serialized', async () => {
        const time = new Date()
        const { partition, key, document: added } = aRow({ time })
        await using c = await connect(driver, contextFactory)
        await c.docs.add(table, partition, key, added, { now })
        const { document } = await c.docs.get(table, partition, key)
        assert.strictEqual(added.time.toISOString(), (document as { time: unknown }).time)
    })

    it('rejects second add', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        await c.docs.add(table, partition, key, added, { now })
        const untouched = await c.docs.get(table, partition, key)
        await assert.rejects(
            c.docs.add(table, partition, key, aDocument(), { now: now + 60 }),
            isConflict,
        )
        assert.deepStrictEqual(await c.docs.get(table, partition, key), untouched)
    })

    it('gets updated, as the update answered it', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const { revision: addedRevision } = await c.docs.add(table, partition, key, added, { now })
        const updated = aDocument()
        const { revision: updatedRevision, ...written } = await c.docs.update(
            table,
            partition,
            key,
            addedRevision,
            updated,
            { now },
        )
        assert.deepStrictEqual(written, { seq: 1, updatedAt })
        assert.deepStrictEqual(await c.docs.get(table, partition, key), {
            partition,
            key,
            revision: updatedRevision,
            document: updated,
            seq: 1,
            updatedAt,
        })
        const again = aDocument()
        const { revision: againRevision, ...writtenAgain } = await c.docs.update(
            table,
            partition,
            key,
            updatedRevision,
            again,
            { now: now + 60 },
        )
        assert.deepStrictEqual(writtenAgain, { seq: 2, updatedAt: updatedAtLater })
        assert.deepStrictEqual(await c.docs.get(table, partition, key), {
            partition,
            key,
            revision: againRevision,
            document: again,
            seq: 2,
            updatedAt: updatedAtLater,
        })
    })

    it('rejects updating updated', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const { revision: addedRevision } = await c.docs.add(table, partition, key, added, { now })
        await c.docs.update(table, partition, key, addedRevision, aDocument(), { now })
        const untouched = await c.docs.get(table, partition, key)
        await assert.rejects(
            c.docs.update(table, partition, key, addedRevision, aDocument(), { now: now + 60 }),
            isConflict,
        )
        assert.deepStrictEqual(await c.docs.get(table, partition, key), untouched)
    })

    it('gets empty range', async () => {
        await using c = await connect(driver, contextFactory)
        const partition = await Array.fromAsync(c.docs.getPartition(table, anId()), r => r.document)
        assert.deepStrictEqual(partition, [])
    })

    it('gets ranges', async () => {
        await using c = await connect(driver, contextFactory)
        const partition = anId()
        await c.docs.add(table, partition, 'a1', aDocument({ key: 'a1' }), { now })
        await c.docs.add(table, partition, 'a2', aDocument({ key: 'a2' }), { now })
        await c.docs.add(table, partition, 'b', aDocument({ key: 'b' }), { now })
        await c.docs.add(table, partition, 'c1', aDocument({ key: 'c1' }), { now })
        await c.docs.add(table, partition, 'c2', aDocument({ key: 'c2' }), { now })
        await c.docs.add(table, partition, 'c3', aDocument({ key: 'c3' }), { now })
        assert.deepStrictEqual(
            await Array.fromAsync(
                c.docs.getPartition(table, partition, { before: 'b' }),
                r => (r.document as { key: string }).key,
            ),
            ['a1', 'a2'],
        )
        assert.deepStrictEqual(
            await Array.fromAsync(
                c.docs.getPartition(table, partition, { after: 'b' }),
                r => (r.document as { key: string }).key,
            ),
            ['b', 'c1', 'c2', 'c3'],
        )
        assert.deepStrictEqual(
            await Array.fromAsync(
                c.docs.getPartition(table, partition, { after: 'c' }),
                r => (r.document as { key: string }).key,
            ),
            ['c1', 'c2', 'c3'],
        )
        assert.deepStrictEqual(
            await Array.fromAsync(
                c.docs.getPartition(table, partition, { after: 'a', before: 'c' }),
                r => (r.document as { key: string }).key,
            ),
            ['a1', 'a2', 'b'],
        )
        assert.deepStrictEqual(
            await Array.fromAsync(
                c.docs.getPartition(table, partition, { after: '' }),
                r => (r.document as { key: string }).key,
            ),
            ['a1', 'a2', 'b', 'c1', 'c2', 'c3'],
        )
        assert.deepStrictEqual(
            await Array.fromAsync(
                c.docs.getPartition(table, partition, { before: '' }),
                r => (r.document as { key: string }).key,
            ),
            [],
        )
        assert.deepStrictEqual(
            await Array.fromAsync(
                c.docs.getPartition(table, partition, { after: '', before: 'b' }),
                r => (r.document as { key: string }).key,
            ),
            ['a1', 'a2'],
        )
        assert.deepStrictEqual(
            await Array.fromAsync(
                c.docs.getPartition(table, partition, { withPrefix: 'a' }),
                r => (r.document as { key: string }).key,
            ),
            ['a1', 'a2'],
        )
    })

    it('gets keys in order regardless of insertion order', async () => {
        await using c = await connect(driver, contextFactory)
        const partition = anId()
        await c.docs.add(table, partition, 'b', aDocument({ key: 'b' }), { now })
        await c.docs.add(table, partition, 'c', aDocument({ key: 'c' }), { now })
        await c.docs.add(table, partition, 'a', aDocument({ key: 'a' }), { now })
        assert.deepStrictEqual(
            await Array.fromAsync(
                c.docs.getPartition(table, partition),
                r => (r.document as { key: string }).key,
            ),
            ['a', 'b', 'c'],
        )
    })

    it('gets no partitions from unused table', async () => {
        await using c = await connect(driver, contextFactory)
        assert.deepStrictEqual(await Array.fromAsync(c.docs.getPartitions(anId())), [])
    })

    it('gets unique partitions', async () => {
        await using c = await connect(driver, contextFactory)
        const p1 = anId()
        const p2 = anId()
        const p3 = anId()
        await c.docs.add(table, p1, 'a', aDocument({ key: 'a' }), { now })
        await c.docs.add(table, p1, 'b', aDocument({ key: 'b' }), { now })
        await c.docs.add(table, p2, 'a', aDocument({ key: 'a' }), { now })
        await c.docs.add(table, p3, 'a', aDocument({ key: 'a' }), { now })
        const partitions = await Array.fromAsync(c.docs.getPartitions(table))
        assert.strictEqual(new Set(partitions).size, partitions.length)
        for (const partition of [p1, p2, p3]) {
            assert.ok(partitions.includes(partition), `Missing partition ${partition}.`)
        }
    })

    it('deletes added from every read', async () => {
        const { partition, key, document: added } = aRow()
        const keptKey = anId()
        const kept = aDocument()
        await using c = await connect(driver, contextFactory)
        const { revision: addedRevision } = await c.docs.add(table, partition, key, added, { now })
        const { revision: keptRevision } = await c.docs.add(table, partition, keptKey, kept, {
            now,
        })
        await c.docs.delete(table, partition, key, addedRevision, { now })
        await assert.rejects(c.docs.get(table, partition, key), isNotFound)
        await assert.rejects(c.docs.get(table, partition, key, { consistent: true }), isNotFound)
        if (c.docs.getMany) {
            assert.deepStrictEqual(
                await c.docs.getMany(table, [
                    { partition, key },
                    { partition, key: keptKey },
                ]),
                [
                    {
                        partition,
                        key: keptKey,
                        revision: keptRevision,
                        document: kept,
                        seq: 0,
                        updatedAt,
                    },
                ],
            )
        }
        assert.deepStrictEqual(await Array.fromAsync(c.docs.getPartition(table, partition)), [
            { key: keptKey, revision: keptRevision, document: kept, seq: 0, updatedAt },
        ])
        assert.deepStrictEqual(
            await Array.fromAsync(c.docs.getPartition(table, partition, { withPrefix: key })),
            [],
        )
    })

    it('leaves a partition out of the partitions once its last row is deleted', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const { revision } = await c.docs.add(table, partition, key, added, { now })
        assert.ok((await Array.fromAsync(c.docs.getPartitions(table))).includes(partition))
        await c.docs.delete(table, partition, key, revision, { now })
        assert.ok(!(await Array.fromAsync(c.docs.getPartitions(table))).includes(partition))
    })

    it('conflicts on updating, deleting and checking a deleted row', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const { revision } = await c.docs.add(table, partition, key, added, { now })
        await c.docs.delete(table, partition, key, revision, { now })
        await assert.rejects(
            c.docs.update(table, partition, key, revision, aDocument(), { now }),
            isConflict,
        )
        await assert.rejects(c.docs.delete(table, partition, key, revision, { now }), isConflict)
        await assert.rejects(
            c.docs.transact([{ op: 'check', table, partition, key, revision }], { now }),
            isConflict,
        )
        await assert.rejects(c.docs.get(table, partition, key), isNotFound)
    })

    it('re-adds deleted, starting its count afresh', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const { revision: addedRevision } = await c.docs.add(table, partition, key, added, { now })
        const { revision: updatedRevision } = await c.docs.update(
            table,
            partition,
            key,
            addedRevision,
            added,
            { now },
        )
        await c.docs.delete(table, partition, key, updatedRevision, { now })
        const { revision, ...written } = await c.docs.add(table, partition, key, added, {
            now: now + 60,
        })
        assert.deepStrictEqual(written, { seq: 0, updatedAt: updatedAtLater })
        assert.deepStrictEqual(await c.docs.get(table, partition, key), {
            partition,
            key,
            revision,
            document: added,
            seq: 0,
            updatedAt: updatedAtLater,
        })
    })

    it('adds and puts over transacted deletes and clears, starting the count afresh', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const { revision: addedRevision } = await c.docs.add(table, partition, key, added, { now })
        await c.docs.transact([{ op: 'delete', table, partition, key, revision: addedRevision }], {
            now,
        })
        await assert.rejects(c.docs.get(table, partition, key), isNotFound)
        assert.deepStrictEqual(await Array.fromAsync(c.docs.getPartition(table, partition)), [])
        const reAddedRevision = anId()
        await c.docs.transact(
            [{ op: 'add', table, partition, key, document: added, newRevision: reAddedRevision }],
            { now },
        )
        const { revision: updatedRevision } = await c.docs.update(
            table,
            partition,
            key,
            reAddedRevision,
            added,
            { now },
        )
        assert.deepStrictEqual(await c.docs.get(table, partition, key), {
            partition,
            key,
            revision: updatedRevision,
            document: added,
            seq: 1,
            updatedAt,
        })
        await c.docs.transact([{ op: 'clear', table, partition, key }], { now })
        await assert.rejects(c.docs.get(table, partition, key), isNotFound)
        assert.deepStrictEqual(await Array.fromAsync(c.docs.getPartition(table, partition)), [])
        const putRevision = anId()
        await c.docs.transact(
            [{ op: 'put', table, partition, key, document: added, newRevision: putRevision }],
            { now: now + 60 },
        )
        assert.deepStrictEqual(await c.docs.get(table, partition, key), {
            partition,
            key,
            revision: putRevision,
            document: added,
            seq: 0,
            updatedAt: updatedAtLater,
        })
    })

    it('transacts adds across tables', async () => {
        const { partition, key, document: added } = aRow()
        const other = aDocument()
        await using c = await connect(driver, contextFactory)
        const revision = anId()
        const otherRevision = anId()
        await c.docs.transact(
            [
                { op: 'add', table, partition, key, document: added, newRevision: revision },
                {
                    op: 'add',
                    table: otherTable,
                    partition,
                    key,
                    document: other,
                    newRevision: otherRevision,
                },
            ],
            { now },
        )
        const row = await c.docs.get(table, partition, key)
        assert.deepStrictEqual(row.document, added)
        assert.strictEqual(row.revision, revision)
        const otherRow = await c.docs.get(otherTable, partition, key)
        assert.deepStrictEqual(otherRow.document, other)
        assert.strictEqual(otherRow.revision, otherRevision)
    })

    it('rejects two operations on one document', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        await assert.rejects(
            c.docs.transact(
                [
                    { op: 'add', table, partition, key, document: added, newRevision: anId() },
                    { op: 'clear', table, partition, key },
                ],
                { now },
            ),
            e => !isConflict(e) && !isNotFound(e),
        )
        await assert.rejects(c.docs.get(table, partition, key), isNotFound)
    })

    it('transacts nothing when any add conflicts', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        await c.docs.add(table, partition, key, added, { now })
        const freshKey = anId()
        await assert.rejects(
            c.docs.transact(
                [
                    { op: 'add', table, partition, key, document: added, newRevision: anId() },
                    {
                        op: 'add',
                        table,
                        partition,
                        key: freshKey,
                        document: aDocument(),
                        newRevision: anId(),
                    },
                ],
                { now },
            ),
            isConflict,
        )
        await assert.rejects(c.docs.get(table, partition, freshKey), isNotFound)
    })

    it('transacts nothing when any update conflicts', async () => {
        const { partition, key, document: added } = aRow()
        const secondKey = anId()
        await using c = await connect(driver, contextFactory)
        const { revision: staleRevision } = await c.docs.add(table, partition, key, added, { now })
        const { revision: secondRevision } = await c.docs.add(
            table,
            partition,
            secondKey,
            aDocument(),
            { now },
        )
        await c.docs.update(table, partition, key, staleRevision, aDocument(), { now })
        const conflicting = await c.docs.get(table, partition, key)
        const untouched = await c.docs.get(table, partition, secondKey)
        await assert.rejects(
            c.docs.transact(
                [
                    {
                        op: 'update',
                        table,
                        partition,
                        key,
                        revision: staleRevision,
                        document: aDocument(),
                        newRevision: anId(),
                    },
                    {
                        op: 'update',
                        table,
                        partition,
                        key: secondKey,
                        revision: secondRevision,
                        document: aDocument(),
                        newRevision: anId(),
                    },
                ],
                { now: now + 60 },
            ),
            isConflict,
        )
        assert.deepStrictEqual(await c.docs.get(table, partition, key), conflicting)
        assert.deepStrictEqual(await c.docs.get(table, partition, secondKey), untouched)
    })

    it('transacts mixed operations', async () => {
        const { partition, key, document: added } = aRow()
        const updateKey = anId()
        const deleteKey = anId()
        await using c = await connect(driver, contextFactory)
        const { revision: updateRevision } = await c.docs.add(
            table,
            partition,
            updateKey,
            aDocument(),
            { now },
        )
        const { revision: deleteRevision } = await c.docs.add(
            otherTable,
            partition,
            deleteKey,
            aDocument(),
            {
                now,
            },
        )
        const addedRevision = anId()
        const updated = aDocument()
        const updatedRevision = anId()
        await c.docs.transact(
            [
                { op: 'add', table, partition, key, document: added, newRevision: addedRevision },
                {
                    op: 'update',
                    table,
                    partition,
                    key: updateKey,
                    revision: updateRevision,
                    document: updated,
                    newRevision: updatedRevision,
                },
                {
                    op: 'delete',
                    table: otherTable,
                    partition,
                    key: deleteKey,
                    revision: deleteRevision,
                },
            ],
            { now },
        )
        assert.deepStrictEqual((await c.docs.get(table, partition, key)).document, added)
        const updatedRow = await c.docs.get(table, partition, updateKey)
        assert.deepStrictEqual(updatedRow.document, updated)
        assert.strictEqual(updatedRow.revision, updatedRevision)
        await assert.rejects(c.docs.get(otherTable, partition, deleteKey), isNotFound)
    })

    it('checks unchanged document', async () => {
        const { partition, key, document: added } = aRow()
        const otherKey = anId()
        await using c = await connect(driver, contextFactory)
        const { revision } = await c.docs.add(table, partition, key, added, { now })
        await c.docs.transact(
            [
                { op: 'check', table, partition, key, revision },
                {
                    op: 'add',
                    table,
                    partition,
                    key: otherKey,
                    document: aDocument(),
                    newRevision: anId(),
                },
            ],
            { now },
        )
        const row = await c.docs.get(table, partition, key)
        assert.strictEqual(row.revision, revision)
        assert.deepStrictEqual(row.document, added)
    })

    it('rejects checking changed document', async () => {
        const { partition, key, document: added } = aRow()
        const otherKey = anId()
        await using c = await connect(driver, contextFactory)
        const { revision: staleRevision } = await c.docs.add(table, partition, key, added, { now })
        await c.docs.update(table, partition, key, staleRevision, aDocument(), { now })
        await assert.rejects(
            c.docs.transact(
                [
                    { op: 'check', table, partition, key, revision: staleRevision },
                    {
                        op: 'add',
                        table,
                        partition,
                        key: otherKey,
                        document: aDocument(),
                        newRevision: anId(),
                    },
                ],
                { now },
            ),
            isConflict,
        )
        await assert.rejects(c.docs.get(table, partition, otherKey), isNotFound)
    })

    it('rejects checking missing document', async () => {
        await using c = await connect(driver, contextFactory)
        await assert.rejects(
            c.docs.transact(
                [{ op: 'check', table, partition: anId(), key: anId(), revision: anId() }],
                { now },
            ),
            isConflict,
        )
    })

    it('gets JSON serialized from transaction', async () => {
        const time = new Date()
        const { partition, key, document: added } = aRow({ time })
        await using c = await connect(driver, contextFactory)
        await c.docs.transact(
            [{ op: 'add', table, partition, key, document: added, newRevision: anId() }],
            { now },
        )
        const { document } = await c.docs.get(table, partition, key)
        assert.strictEqual(added.time.toISOString(), (document as { time: unknown }).time)
    })

    it('updates document added in a transaction', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const revision = anId()
        await c.docs.transact(
            [{ op: 'add', table, partition, key, document: added, newRevision: revision }],
            { now },
        )
        assert.deepStrictEqual(await c.docs.get(table, partition, key), {
            partition,
            key,
            revision,
            document: added,
            seq: 0,
            updatedAt,
        })
        const updated = aDocument()
        const transactionRevision = anId()
        await c.docs.transact(
            [
                {
                    op: 'update',
                    table,
                    partition,
                    key,
                    revision,
                    document: updated,
                    newRevision: transactionRevision,
                },
            ],
            { now },
        )
        assert.deepStrictEqual(await c.docs.get(table, partition, key), {
            partition,
            key,
            revision: transactionRevision,
            document: updated,
            seq: 1,
            updatedAt,
        })
        const again = aDocument()
        const { revision: updatedRevision } = await c.docs.update(
            table,
            partition,
            key,
            transactionRevision,
            again,
            { now: now + 60 },
        )
        assert.deepStrictEqual(await c.docs.get(table, partition, key), {
            partition,
            key,
            revision: updatedRevision,
            document: again,
            seq: 2,
            updatedAt: updatedAtLater,
        })
    })

    it('puts documents unconditionally', async () => {
        const { partition, key, document: first } = aRow()
        await using c = await connect(driver, contextFactory)
        const firstRevision = anId()
        await c.docs.transact(
            [{ op: 'put', table, partition, key, document: first, newRevision: firstRevision }],
            { now },
        )
        assert.deepStrictEqual(await c.docs.get(table, partition, key), {
            partition,
            key,
            revision: firstRevision,
            document: first,
            seq: 0,
            updatedAt,
        })
        const second = aDocument()
        const secondRevision = anId()
        await c.docs.transact(
            [{ op: 'put', table, partition, key, document: second, newRevision: secondRevision }],
            { now: now + 60 },
        )
        assert.deepStrictEqual(await c.docs.get(table, partition, key), {
            partition,
            key,
            revision: secondRevision,
            document: second,
            seq: 1,
            updatedAt: updatedAtLater,
        })
    })

    it('puts over an added row, continuing its count', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const { revision: addedRevision } = await c.docs.add(table, partition, key, added, { now })
        await c.docs.update(table, partition, key, addedRevision, added, { now })
        const put = aDocument()
        const putRevision = anId()
        await c.docs.transact(
            [{ op: 'put', table, partition, key, document: put, newRevision: putRevision }],
            { now: now + 60 },
        )
        assert.deepStrictEqual(await c.docs.get(table, partition, key), {
            partition,
            key,
            revision: putRevision,
            document: put,
            seq: 2,
            updatedAt: updatedAtLater,
        })
    })

    it('clears present and absent documents, leaving nothing behind', async () => {
        const { partition, key, document: added } = aRow()
        const unwrittenPartition = anId()
        await using c = await connect(driver, contextFactory)
        await c.docs.add(table, partition, key, added, { now })
        await c.docs.transact(
            [
                { op: 'clear', table, partition, key },
                { op: 'clear', table, partition, key: anId() },
                { op: 'clear', table, partition: unwrittenPartition, key: anId() },
            ],
            { now },
        )
        await assert.rejects(c.docs.get(table, partition, key), isNotFound)
        assert.deepStrictEqual(await Array.fromAsync(c.docs.getPartition(table, partition)), [])
        assert.deepStrictEqual(
            await Array.fromAsync(c.docs.getPartition(table, unwrittenPartition)),
            [],
        )
        assert.ok(!(await Array.fromAsync(c.docs.getPartitions(table))).includes(partition))
    })

    it('transacts nothing when puts and clears accompany a failing check', async () => {
        const { partition, key, document: added } = aRow()
        const putKey = anId()
        const clearKey = anId()
        await using c = await connect(driver, contextFactory)
        const { revision: staleRevision } = await c.docs.add(table, partition, key, added, { now })
        await c.docs.update(table, partition, key, staleRevision, aDocument(), { now })
        await c.docs.add(table, partition, clearKey, aDocument(), { now })
        const checked = await c.docs.get(table, partition, key)
        const untouched = await c.docs.get(table, partition, clearKey)
        await assert.rejects(
            c.docs.transact(
                [
                    {
                        op: 'put',
                        table,
                        partition,
                        key: putKey,
                        document: aDocument(),
                        newRevision: anId(),
                    },
                    { op: 'clear', table, partition, key: clearKey },
                    { op: 'check', table, partition, key, revision: staleRevision },
                ],
                { now: now + 60 },
            ),
            isConflict,
        )
        await assert.rejects(c.docs.get(table, partition, putKey), isNotFound)
        assert.deepStrictEqual(await c.docs.get(table, partition, clearKey), untouched)
        assert.deepStrictEqual(await c.docs.get(table, partition, key), checked)
    })

    it('ranges keys containing the reserved separator', async () => {
        const partition = anId()
        const separator = '\u{0}'
        const doc1 = aDocument()
        const doc2 = aDocument()
        await using c = await connect(driver, contextFactory)
        await c.docs.transact(
            [
                {
                    op: 'put',
                    table,
                    partition,
                    key: `b${separator}p1${separator}k1`,
                    document: doc1,
                    newRevision: anId(),
                },
                {
                    op: 'put',
                    table,
                    partition,
                    key: `b${separator}p2${separator}k2`,
                    document: doc2,
                    newRevision: anId(),
                },
                {
                    op: 'put',
                    table,
                    partition,
                    key: `ba${separator}p3${separator}k3`,
                    document: aDocument(),
                    newRevision: anId(),
                },
            ],
            { now },
        )
        const prefixed = await Array.fromAsync(
            c.docs.getPartition(table, partition, { withPrefix: `b${separator}` }),
            r => r.document,
        )
        assert.deepStrictEqual(prefixed, [doc1, doc2])
        const ranged = await Array.fromAsync(
            c.docs.getPartition(table, partition, { after: 'b', before: `b${separator}p2` }),
            r => r.document,
        )
        assert.deepStrictEqual(ranged, [doc1])
    })

    it('gets expired rows raw with their expiry', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const { revision } = await c.docs.add(table, partition, key, added, {
            now,
            expiresAt: now + 60,
        })
        assert.deepStrictEqual(await c.docs.get(table, partition, key), {
            partition,
            key,
            revision,
            document: added,
            seq: 0,
            updatedAt,
            expiresAt: now + 60,
        })
        assert.deepStrictEqual(
            await Array.fromAsync(c.docs.getPartition(table, partition), r => r.expiresAt),
            [now + 60],
        )
    })

    // A batch read is optional in the contract; a driver without one is served
    // by the store reading through `get`, so the cases below pass it by.
    it('gets many rows raw across partitions, leaving missing ones out', async () => {
        const first = aRow()
        const second = aRow()
        await using c = await connect(driver, contextFactory)
        if (!c.docs.getMany) {
            return
        }
        const { revision: firstRevision } = await c.docs.add(
            table,
            first.partition,
            first.key,
            first.document,
            {
                now,
                expiresAt: now + 60,
            },
        )
        const { revision: secondRevision } = await c.docs.add(
            table,
            second.partition,
            second.key,
            second.document,
            { now },
        )
        const found = await c.docs.getMany(table, [
            { partition: first.partition, key: first.key },
            { partition: second.partition, key: anId() },
            { partition: second.partition, key: second.key },
        ])
        assert.deepStrictEqual(
            found.toSorted((a, b) => a.partition.localeCompare(b.partition)),
            [
                {
                    partition: first.partition,
                    key: first.key,
                    revision: firstRevision,
                    document: first.document,
                    seq: 0,
                    updatedAt,
                    expiresAt: now + 60,
                },
                {
                    partition: second.partition,
                    key: second.key,
                    revision: secondRevision,
                    document: second.document,
                    seq: 0,
                    updatedAt,
                },
            ].toSorted((a, b) => a.partition.localeCompare(b.partition)),
        )
    })

    it('gets many rows beyond any batch limit, from partitions with and without rows', async () => {
        await using c = await connect(driver, contextFactory)
        if (!c.docs.getMany) {
            return
        }
        const empty = anId()
        const rows = Array.from({ length: 101 }, () => aRow())
        const revisions = await Promise.all(
            rows.map(
                async r =>
                    (await c.docs.add(table, r.partition, r.key, r.document, { now })).revision,
            ),
        )
        const found = await c.docs.getMany(table, [
            { partition: empty, key: anId() },
            ...rows.map(r => ({ partition: r.partition, key: r.key })),
        ])
        assert.deepStrictEqual(
            found.toSorted((a, b) => a.partition.localeCompare(b.partition)),
            rows
                .map((r, i) => ({
                    partition: r.partition,
                    key: r.key,
                    revision: revisions[i],
                    document: r.document,
                    seq: 0,
                    updatedAt,
                }))
                .toSorted((a, b) => a.partition.localeCompare(b.partition)),
        )
    })

    it('gets no rows for refs in other tables', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        if (!c.docs.getMany) {
            return
        }
        await c.docs.add(table, partition, key, added, { now })
        assert.deepStrictEqual(await c.docs.getMany(otherTable, [{ partition, key }]), [])
    })

    it('rejects adding over an unexpired row', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        await c.docs.add(table, partition, key, added, { now, expiresAt: now + 60 })
        await assert.rejects(
            c.docs.add(table, partition, key, aDocument(), { now: now + 59 }),
            isConflict,
        )
    })

    it('adds over an expired row, continuing its count', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const { revision: addedRevision } = await c.docs.add(table, partition, key, added, {
            now,
            expiresAt: now + 60,
        })
        await c.docs.update(table, partition, key, addedRevision, added, {
            now,
            expiresAt: now + 60,
        })
        const replacement = aDocument()
        const { revision, ...written } = await c.docs.add(table, partition, key, replacement, {
            now: now + 60,
        })
        assert.deepStrictEqual(written, { seq: 2, updatedAt: updatedAtLater })
        assert.deepStrictEqual(await c.docs.get(table, partition, key), {
            partition,
            key,
            revision,
            document: replacement,
            seq: 2,
            updatedAt: updatedAtLater,
        })
    })

    it('rejects updating and deleting an expired row', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const { revision } = await c.docs.add(table, partition, key, added, {
            now,
            expiresAt: now + 60,
        })
        await assert.rejects(
            c.docs.update(table, partition, key, revision, aDocument(), { now: now + 60 }),
            isConflict,
        )
        await assert.rejects(
            c.docs.delete(table, partition, key, revision, { now: now + 60 }),
            isConflict,
        )
        assert.deepStrictEqual((await c.docs.get(table, partition, key)).document, added)
    })

    it('updates the expiry of a row', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const { revision } = await c.docs.add(table, partition, key, added, {
            now,
            expiresAt: now + 60,
        })
        const { revision: extended } = await c.docs.update(table, partition, key, revision, added, {
            now,
            expiresAt: now + 120,
        })
        assert.strictEqual((await c.docs.get(table, partition, key)).expiresAt, now + 120)
        await c.docs.update(table, partition, key, extended, added, { now })
        assert.strictEqual((await c.docs.get(table, partition, key)).expiresAt, undefined)
    })

    it('transacts over expired rows as over missing ones', async () => {
        const { partition, key, document: added } = aRow()
        const checkedKey = anId()
        await using c = await connect(driver, contextFactory)
        await c.docs.add(table, partition, key, added, { now, expiresAt: now + 60 })
        const { revision: checkedRevision } = await c.docs.add(
            table,
            partition,
            checkedKey,
            aDocument(),
            {
                now,
                expiresAt: now + 60,
            },
        )
        await assert.rejects(
            c.docs.transact(
                [{ op: 'check', table, partition, key: checkedKey, revision: checkedRevision }],
                { now: now + 60 },
            ),
            isConflict,
        )
        const replacement = aDocument()
        const revision = anId()
        await c.docs.transact(
            [
                {
                    op: 'add',
                    table,
                    partition,
                    key,
                    document: replacement,
                    newRevision: revision,
                    expiresAt: now + 120,
                },
            ],
            { now: now + 60 },
        )
        assert.deepStrictEqual(await c.docs.get(table, partition, key), {
            partition,
            key,
            revision,
            document: replacement,
            seq: 1,
            updatedAt: updatedAtLater,
            expiresAt: now + 120,
        })
    })

    it('puts replace the expiry', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        await c.docs.transact(
            [
                {
                    op: 'put',
                    table,
                    partition,
                    key,
                    document: added,
                    newRevision: anId(),
                    expiresAt: now + 60,
                },
            ],
            { now },
        )
        assert.strictEqual((await c.docs.get(table, partition, key)).expiresAt, now + 60)
        await c.docs.transact(
            [{ op: 'put', table, partition, key, document: added, newRevision: anId() }],
            { now: now + 60 },
        )
        assert.strictEqual((await c.docs.get(table, partition, key)).expiresAt, undefined)
    })

    it('transacts nothing when an update hits an expired row', async () => {
        const { partition, key, document: added } = aRow()
        const siblingKey = anId()
        await using c = await connect(driver, contextFactory)
        const { revision } = await c.docs.add(table, partition, key, added, {
            now,
            expiresAt: now + 60,
        })
        await assert.rejects(
            c.docs.transact(
                [
                    {
                        op: 'update',
                        table,
                        partition,
                        key,
                        revision,
                        document: aDocument(),
                        newRevision: anId(),
                    },
                    {
                        op: 'add',
                        table: otherTable,
                        partition,
                        key: siblingKey,
                        document: aDocument(),
                        newRevision: anId(),
                    },
                ],
                { now: now + 60 },
            ),
            isConflict,
        )
        await assert.rejects(c.docs.get(otherTable, partition, siblingKey), isNotFound)
    })

    it('transacts nothing when a delete hits an expired row', async () => {
        const { partition, key, document: added } = aRow()
        const siblingKey = anId()
        await using c = await connect(driver, contextFactory)
        const { revision } = await c.docs.add(table, partition, key, added, {
            now,
            expiresAt: now + 60,
        })
        await assert.rejects(
            c.docs.transact(
                [
                    { op: 'delete', table, partition, key, revision },
                    {
                        op: 'add',
                        table: otherTable,
                        partition,
                        key: siblingKey,
                        document: aDocument(),
                        newRevision: anId(),
                    },
                ],
                { now: now + 60 },
            ),
            isConflict,
        )
        await assert.rejects(c.docs.get(otherTable, partition, siblingKey), isNotFound)
    })

    it('transacts updates of the expiry', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const { revision } = await c.docs.add(table, partition, key, added, {
            now,
            expiresAt: now + 60,
        })
        const extended = anId()
        await c.docs.transact(
            [
                {
                    op: 'update',
                    table,
                    partition,
                    key,
                    revision,
                    document: added,
                    newRevision: extended,
                    expiresAt: now + 120,
                },
            ],
            { now },
        )
        assert.strictEqual((await c.docs.get(table, partition, key)).expiresAt, now + 120)
        const cleared = anId()
        await c.docs.transact(
            [
                {
                    op: 'update',
                    table,
                    partition,
                    key,
                    revision: extended,
                    document: added,
                    newRevision: cleared,
                },
            ],
            { now },
        )
        assert.deepStrictEqual(await c.docs.get(table, partition, key), {
            partition,
            key,
            revision: cleared,
            document: added,
            seq: 2,
            updatedAt,
        })
    })

    it('deletes and checks unexpired rows', async () => {
        const { partition, key, document: added } = aRow()
        const checkedKey = anId()
        const otherKey = anId()
        const other = aDocument()
        await using c = await connect(driver, contextFactory)
        const { revision } = await c.docs.add(table, partition, key, added, {
            now,
            expiresAt: now + 60,
        })
        const { revision: checkedRevision } = await c.docs.add(
            table,
            partition,
            checkedKey,
            aDocument(),
            {
                now,
                expiresAt: now + 60,
            },
        )
        await c.docs.transact(
            [
                { op: 'check', table, partition, key: checkedKey, revision: checkedRevision },
                {
                    op: 'add',
                    table: otherTable,
                    partition,
                    key: otherKey,
                    document: other,
                    newRevision: anId(),
                },
            ],
            { now },
        )
        assert.deepStrictEqual((await c.docs.get(otherTable, partition, otherKey)).document, other)
        await c.docs.delete(table, partition, key, revision, { now })
        await assert.rejects(c.docs.get(table, partition, key), isNotFound)
    })

    it('transacts expiring and non-expiring adds together', async () => {
        const { partition, key, document: added } = aRow()
        const otherKey = anId()
        const other = aDocument()
        await using c = await connect(driver, contextFactory)
        const revision = anId()
        const otherRevision = anId()
        await c.docs.transact(
            [
                {
                    op: 'add',
                    table,
                    partition,
                    key,
                    document: added,
                    newRevision: revision,
                    expiresAt: now + 60,
                },
                {
                    op: 'add',
                    table,
                    partition,
                    key: otherKey,
                    document: other,
                    newRevision: otherRevision,
                },
            ],
            { now },
        )
        assert.deepStrictEqual(await c.docs.get(table, partition, key), {
            partition,
            key,
            revision,
            document: added,
            seq: 0,
            updatedAt,
            expiresAt: now + 60,
        })
        assert.deepStrictEqual(await c.docs.get(table, partition, otherKey), {
            partition,
            key: otherKey,
            revision: otherRevision,
            document: other,
            seq: 0,
            updatedAt,
        })
    })
}

function anId(): string {
    return randomUUID()
}

function aDocument<T extends { [key: string]: unknown }>(props?: T) {
    return {
        data: anId(),
        ...props,
    } as T & { data: string }
}

function aRow<T extends { [key: string]: unknown }>(props?: T) {
    return { table, partition: anId(), key: anId(), document: aDocument(props) } as const
}

async function connect(driver: Driver, contextFactory: () => object) {
    return await connectTo(driver, contextFactory())
}

async function connectTo(driver: Driver, context: object) {
    const connection = await driver.connect(context)
    return {
        docs: connection,
        [Symbol.asyncDispose]: () => connection.close(),
    }
}
