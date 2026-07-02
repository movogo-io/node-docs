import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import type { Driver } from './lib/driver.js'
import { isConflict, isNotFound } from './partitioned.js'

const table = 'HarnessTestDocs'
const otherTable = 'HarnessTestDocs2'

export function harness(
    it: (message: string, runner: () => Promise<void>) => void,
    driver: Driver,
    contextFactory: () => object,
) {
    it('throws not found when not added', async () => {
        await using c = await connect(driver, contextFactory)
        await assert.rejects(c.docs.get(table, anId(), anId()), isNotFound)
    })

    it('gets added', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const addedRevision = await c.docs.add(table, partition, key, added)
        const { document, revision } = await c.docs.get(table, partition, key)
        assert.deepStrictEqual(added, document)
        assert.strictEqual(addedRevision, revision)
    })

    it('gets JSON serialized', async () => {
        const now = new Date()
        const { partition, key, document: added } = aRow({ time: now })
        await using c = await connect(driver, contextFactory)
        await c.docs.add(table, partition, key, added)
        const { document } = await c.docs.get(table, partition, key)
        assert.strictEqual(added.time.toISOString(), (document as { time: unknown }).time)
    })

    it('rejects second add', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        await c.docs.add(table, partition, key, added)
        await assert.rejects(c.docs.add(table, partition, key, added), isConflict)
    })

    it('gets updated', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const addedRevision = await c.docs.add(table, partition, key, added)
        const updated = aDocument()
        const updatedRevision = await c.docs.update(table, partition, key, addedRevision, updated)
        const { document, revision } = await c.docs.get(table, partition, key)
        assert.deepStrictEqual(updated, document)
        assert.strictEqual(updatedRevision, revision)
    })

    it('rejects updating updated', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const addedRevision = await c.docs.add(table, partition, key, added)
        await c.docs.update(table, partition, key, addedRevision, aDocument())
        await assert.rejects(
            c.docs.update(table, partition, key, addedRevision, aDocument()),
            isConflict,
        )
    })

    it('gets empty range', async () => {
        await using c = await connect(driver, contextFactory)
        const partition = await Array.fromAsync(c.docs.getPartition(table, anId()), r => r.document)
        assert.deepStrictEqual(partition, [])
    })

    it('gets ranges', async () => {
        await using c = await connect(driver, contextFactory)
        const partition = anId()
        await c.docs.add(table, partition, 'a1', aDocument({ key: 'a1' }))
        await c.docs.add(table, partition, 'a2', aDocument({ key: 'a2' }))
        await c.docs.add(table, partition, 'b', aDocument({ key: 'b' }))
        await c.docs.add(table, partition, 'c1', aDocument({ key: 'c1' }))
        await c.docs.add(table, partition, 'c2', aDocument({ key: 'c2' }))
        await c.docs.add(table, partition, 'c3', aDocument({ key: 'c3' }))
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
                c.docs.getPartition(table, partition, { withPrefix: 'a' }),
                r => (r.document as { key: string }).key,
            ),
            ['a1', 'a2'],
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
        await c.docs.add(table, p1, 'a', aDocument({ key: 'a' }))
        await c.docs.add(table, p1, 'b', aDocument({ key: 'b' }))
        await c.docs.add(table, p2, 'a', aDocument({ key: 'a' }))
        await c.docs.add(table, p3, 'a', aDocument({ key: 'a' }))
        const partitions = await Array.fromAsync(c.docs.getPartitions(table))
        assert.strictEqual(new Set(partitions).size, partitions.length)
        for (const partition of [p1, p2, p3]) {
            assert.ok(partitions.includes(partition), `Missing partition ${partition}.`)
        }
    })

    it('deletes added', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const addedRevision = await c.docs.add(table, partition, key, added)
        await c.docs.delete(table, partition, key, addedRevision)
        await assert.rejects(c.docs.get(table, partition, key), isNotFound)
    })

    it('re-adds deleted', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const addedRevision = await c.docs.add(table, partition, key, added)
        await c.docs.delete(table, partition, key, addedRevision)
        const reAddedRevision = await c.docs.add(table, partition, key, added)
        const { document, revision } = await c.docs.get(table, partition, key)
        assert.deepStrictEqual(added, document)
        assert.strictEqual(reAddedRevision, revision)
    })

    it('transacts adds across tables', async () => {
        const { partition, key, document: added } = aRow()
        const other = aDocument()
        await using c = await connect(driver, contextFactory)
        const revision = anId()
        const otherRevision = anId()
        await c.docs.transact([
            { op: 'add', table, partition, key, document: added, newRevision: revision },
            {
                op: 'add',
                table: otherTable,
                partition,
                key,
                document: other,
                newRevision: otherRevision,
            },
        ])
        const row = await c.docs.get(table, partition, key)
        assert.deepStrictEqual(row.document, added)
        assert.strictEqual(row.revision, revision)
        const otherRow = await c.docs.get(otherTable, partition, key)
        assert.deepStrictEqual(otherRow.document, other)
        assert.strictEqual(otherRow.revision, otherRevision)
    })

    it('transacts nothing when any add conflicts', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        await c.docs.add(table, partition, key, added)
        const freshKey = anId()
        await assert.rejects(
            c.docs.transact([
                { op: 'add', table, partition, key, document: added, newRevision: anId() },
                {
                    op: 'add',
                    table,
                    partition,
                    key: freshKey,
                    document: aDocument(),
                    newRevision: anId(),
                },
            ]),
            isConflict,
        )
        await assert.rejects(c.docs.get(table, partition, freshKey), isNotFound)
    })

    it('transacts nothing when any update conflicts', async () => {
        const { partition, key, document: added } = aRow()
        const secondKey = anId()
        await using c = await connect(driver, contextFactory)
        const staleRevision = await c.docs.add(table, partition, key, added)
        const secondRevision = await c.docs.add(table, partition, secondKey, aDocument())
        await c.docs.update(table, partition, key, staleRevision, aDocument())
        const untouched = await c.docs.get(table, partition, secondKey)
        await assert.rejects(
            c.docs.transact([
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
            ]),
            isConflict,
        )
        assert.deepStrictEqual(await c.docs.get(table, partition, secondKey), untouched)
    })

    it('transacts mixed operations', async () => {
        const { partition, key, document: added } = aRow()
        const updateKey = anId()
        const deleteKey = anId()
        await using c = await connect(driver, contextFactory)
        const updateRevision = await c.docs.add(table, partition, updateKey, aDocument())
        const deleteRevision = await c.docs.add(otherTable, partition, deleteKey, aDocument())
        const addedRevision = anId()
        const updated = aDocument()
        const updatedRevision = anId()
        await c.docs.transact([
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
        ])
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
        const revision = await c.docs.add(table, partition, key, added)
        await c.docs.transact([
            { op: 'check', table, partition, key, revision },
            {
                op: 'add',
                table,
                partition,
                key: otherKey,
                document: aDocument(),
                newRevision: anId(),
            },
        ])
        const row = await c.docs.get(table, partition, key)
        assert.strictEqual(row.revision, revision)
        assert.deepStrictEqual(row.document, added)
    })

    it('rejects checking changed document', async () => {
        const { partition, key, document: added } = aRow()
        const otherKey = anId()
        await using c = await connect(driver, contextFactory)
        const staleRevision = await c.docs.add(table, partition, key, added)
        await c.docs.update(table, partition, key, staleRevision, aDocument())
        await assert.rejects(
            c.docs.transact([
                { op: 'check', table, partition, key, revision: staleRevision },
                {
                    op: 'add',
                    table,
                    partition,
                    key: otherKey,
                    document: aDocument(),
                    newRevision: anId(),
                },
            ]),
            isConflict,
        )
        await assert.rejects(c.docs.get(table, partition, otherKey), isNotFound)
    })

    it('rejects checking missing document', async () => {
        await using c = await connect(driver, contextFactory)
        await assert.rejects(
            c.docs.transact([
                { op: 'check', table, partition: anId(), key: anId(), revision: anId() },
            ]),
            isConflict,
        )
    })

    it('gets JSON serialized from transaction', async () => {
        const now = new Date()
        const { partition, key, document: added } = aRow({ time: now })
        await using c = await connect(driver, contextFactory)
        await c.docs.transact([
            { op: 'add', table, partition, key, document: added, newRevision: anId() },
        ])
        const { document } = await c.docs.get(table, partition, key)
        assert.strictEqual(added.time.toISOString(), (document as { time: unknown }).time)
    })

    it('updates document added in a transaction', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const revision = anId()
        await c.docs.transact([
            { op: 'add', table, partition, key, document: added, newRevision: revision },
        ])
        const updatedRevision = await c.docs.update(table, partition, key, revision, aDocument())
        const transactionRevision = anId()
        await c.docs.transact([
            {
                op: 'update',
                table,
                partition,
                key,
                revision: updatedRevision,
                document: aDocument(),
                newRevision: transactionRevision,
            },
        ])
        assert.strictEqual((await c.docs.get(table, partition, key)).revision, transactionRevision)
    })

    it('puts documents unconditionally', async () => {
        const { partition, key, document: first } = aRow()
        await using c = await connect(driver, contextFactory)
        const firstRevision = anId()
        await c.docs.transact([
            { op: 'put', table, partition, key, document: first, newRevision: firstRevision },
        ])
        const added = await c.docs.get(table, partition, key)
        assert.deepStrictEqual(added.document, first)
        assert.strictEqual(added.revision, firstRevision)
        const second = aDocument()
        const secondRevision = anId()
        await c.docs.transact([
            { op: 'put', table, partition, key, document: second, newRevision: secondRevision },
        ])
        const replaced = await c.docs.get(table, partition, key)
        assert.deepStrictEqual(replaced.document, second)
        assert.strictEqual(replaced.revision, secondRevision)
    })

    it('clears present and absent documents', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        await c.docs.add(table, partition, key, added)
        await c.docs.transact([
            { op: 'clear', table, partition, key },
            { op: 'clear', table, partition, key: anId() },
        ])
        await assert.rejects(c.docs.get(table, partition, key), isNotFound)
    })

    it('transacts nothing when puts and clears accompany a failing check', async () => {
        const { partition, key, document: added } = aRow()
        const putKey = anId()
        const clearKey = anId()
        await using c = await connect(driver, contextFactory)
        const staleRevision = await c.docs.add(table, partition, key, added)
        await c.docs.update(table, partition, key, staleRevision, aDocument())
        await c.docs.add(table, partition, clearKey, aDocument())
        const untouched = await c.docs.get(table, partition, clearKey)
        await assert.rejects(
            c.docs.transact([
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
            ]),
            isConflict,
        )
        await assert.rejects(c.docs.get(table, partition, putKey), isNotFound)
        assert.deepStrictEqual(await c.docs.get(table, partition, clearKey), untouched)
    })

    it('ranges keys containing the reserved separator', async () => {
        const partition = anId()
        const separator = '\u0000'
        const doc1 = aDocument()
        const doc2 = aDocument()
        await using c = await connect(driver, contextFactory)
        await c.docs.transact([
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
        ])
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
    const connection = await driver.connect(contextFactory())
    return {
        docs: connection,
        [Symbol.asyncDispose]: () => connection.close(),
    }
}
