import { createRequire } from 'node:module'
import type { KeyRange, Revision, Row, StoredDocument } from '../schema.js'

export type Context = object

// Options of a read. `consistent: true` asks for a read that sees every write
// acknowledged before it began, for a caller that reads back a row it wrote
// milliseconds earlier and decides on what it sees (a lease claim, a fencing
// check). A driver that cannot honour it ignores it; one that can may charge
// more for it, so callers ask per read rather than by default.
export type ReadOptions = { consistent?: boolean }

export type Driver = {
    connect: (context: Context) => Promise<Connection>
}

// Times handed to drivers are integer epoch seconds. A row whose stored
// `expiresAt` is at or before the `now` of the operation is expired: `add` and
// `put` overwrite it as if it were missing, while `update`, `delete`, and
// `check` conflict on it. `put` stamps `updatedAt` from `now` like the other
// writes; only `clear` ignores it.
//
// Every write stores `seq`: 0 when no row is under the key, the row's `seq` + 1
// otherwise, live or expired. `delete` and `clear` remove the row, so a re-add
// of the key starts at 0 again. `add` and `update` answer the revision, `seq`
// and `updatedAt` they stored; a transaction answers nothing, so the store
// works them out from the row it read before writing, which is what the driver
// stores unless it removed an expired row between the read and the write.
//
// Writes omit `expiresAt` for a row that never expires, and an `update` or
// `put` without it clears any stored expiry. Any integer is a valid expiry,
// negative or far in the future; drivers store it rather than reject it.
//
// Reads return rows raw, expired or not, with their stored `expiresAt`; a row
// without one comes back without the key, never with `expiresAt: undefined`.
// The store filters. Drivers may delete expired rows at any time and then
// report them missing on read, and `getPartitions` may list partitions whose
// only rows have expired.
export type TransactionItem =
    | {
          op: 'add'
          table: string
          partition: string
          key: string
          document: StoredDocument
          newRevision: Revision
          expiresAt?: number
      }
    | {
          op: 'update'
          table: string
          partition: string
          key: string
          revision: Revision
          document: StoredDocument
          newRevision: Revision
          expiresAt?: number
      }
    | {
          op: 'delete'
          table: string
          partition: string
          key: string
          revision: Revision
      }
    | {
          op: 'check'
          table: string
          partition: string
          key: string
          revision: Revision
      }
    | {
          op: 'put'
          table: string
          partition: string
          key: string
          document: StoredDocument
          newRevision: Revision
          expiresAt?: number
      }
    | {
          op: 'clear'
          table: string
          partition: string
          key: string
      }

// What `add` and `update` stored, as `get` would read it back.
export type Written = { revision: Revision; seq: number; updatedAt: string }

export type Connection = {
    // What the driver enforces itself, for the store to size its work from
    // instead of assuming: how many requests it holds in flight, a request
    // past that waiting its turn, and how many operations one transaction
    // takes. A connection that declares no bound is taken to have none, and
    // the store keeps its own. A decorator forwards both, or hides them.
    readonly requestsInFlightMax?: number
    readonly transactionItemsMax?: number
    close: () => Promise<void>
    add: (
        table: string,
        partition: string,
        key: string,
        document: StoredDocument,
        options: { now: number; expiresAt?: number },
    ) => Promise<Written>
    get: (
        table: string,
        partition: string,
        key: string,
        options?: ReadOptions,
    ) => Promise<Row<StoredDocument> & { partition: string; key: string; expiresAt?: number }>
    // The rows among `refs` that exist, in any order, raw like `get`; a
    // missing one is simply absent. `refs` is non-empty, distinct, and may
    // span partitions. A driver with a batch read serves the whole list here
    // in as many calls as its batch limit needs, and answers with every row
    // that exists or throws: a partial list (a batch left with unprocessed
    // keys, say) is indistinguishable from deleted documents to the store.
    // Without one the store reads each ref through `get`, a bounded number at
    // a time.
    getMany?: (
        table: string,
        refs: readonly { partition: string; key: string }[],
        options?: ReadOptions,
    ) => Promise<(Row<StoredDocument> & { partition: string; key: string; expiresAt?: number })[]>
    getPartitions: (table: string) => AsyncIterable<string>
    getPartition: (
        table: string,
        partition: string,
        keyRange?: KeyRange,
        options?: ReadOptions,
    ) => AsyncIterable<{
        key: string
        revision: Revision
        document: StoredDocument
        seq: number
        updatedAt: string
        expiresAt?: number
    }>
    update: (
        table: string,
        partition: string,
        key: string,
        revision: Revision,
        document: StoredDocument,
        options: { now: number; expiresAt?: number },
    ) => Promise<Written>
    delete: (
        table: string,
        partition: string,
        key: string,
        revision: Revision,
        options: { now: number },
    ) => Promise<void>
    transact: (items: TransactionItem[], options: { now: number }) => Promise<void>
}

// The driver, its decorators and the index and expiry registries live in this module's scope, so a second copy of the package in one process would split them: writes through one copy bypass the other's decorators and indexes. The first copy loaded claims the process; a later one refuses to load.
// The version names the copy in the error above. A deployed service is bundled
// into one file with no package.json beside it, so the read may fail there; the
// claim itself needs only the symbol, and the URL still tells the copies apart.
function packageVersion() {
    try {
        return String(
            (createRequire(import.meta.url)('../package.json') as { version: unknown }).version,
        )
    } catch {
        return 'bundled'
    }
}

function claimProcess() {
    const claim = Symbol.for('@movogo-io/docs')
    const copy = { version: packageVersion(), url: import.meta.url }
    const claimed = (globalThis as { [claim]?: { version: string; url: string } })[claim]
    if (claimed !== undefined) {
        throw new Error(
            `Two copies of @movogo-io/docs are loaded: ${copy.version} at ${copy.url} and ${claimed.version} at ${claimed.url}. The service and every package must resolve to one copy; check the peer dependency pins.`,
        )
    }
    Reflect.set(globalThis, claim, copy)
    return copy
}

const state: {
    copy: { version: string; url: string }
    driver: Driver
    decorators: ((driver: Driver) => Driver)[]
    decorated?: Driver
} = {
    copy: claimProcess(),
    driver: {
        connect: () =>
            Promise.reject<Connection>(new Error('No driver set, please call setDriver()')),
    },
    decorators: [],
}

export function setDriver(driver: Driver) {
    const previous = state.driver
    state.driver = driver
    state.decorated = undefined
    return previous
}

export function decorateDriver(decorator: (driver: Driver) => Driver) {
    state.decorators.push(decorator)
    state.decorated = undefined
    return () => {
        const index = state.decorators.lastIndexOf(decorator)
        if (index === -1) {
            return
        }
        state.decorators.splice(index, 1)
        state.decorated = undefined
    }
}

// What the connection the store would use for this context declares; nothing
// for a bound it does not declare, or one a decorator hides.
export async function declaredLimits(context: Context) {
    const c = await getDriver().connect(context)
    try {
        const requestsInFlightMax = validBound(c, 'requestsInFlightMax')
        const transactionItemsMax = validBound(c, 'transactionItemsMax')
        return {
            ...(requestsInFlightMax !== undefined && { requestsInFlightMax }),
            ...(transactionItemsMax !== undefined && { transactionItemsMax }),
        }
    } finally {
        await c.close()
    }
}

export function declaredBound(c: Connection, name: Bound, fallback: number) {
    return validBound(c, name) ?? fallback
}

type Bound = 'requestsInFlightMax' | 'transactionItemsMax'

// A bound that is not a positive integer is a misconfigured driver, and the
// store's loops step by it: NaN skips every item, 0 never ends, a fraction
// sends one item twice.
function validBound(c: Connection, name: Bound) {
    const bound = c[name]
    if (bound === undefined || (Number.isSafeInteger(bound) && bound > 0)) {
        return bound
    }
    throw new Error(
        `A connection declares ${name} ${String(bound)}; it must be a positive integer.`,
    )
}

export function getDriver() {
    return (state.decorated ??= state.decorators.reduce(
        (driver, decorator) => decorator(driver),
        state.driver,
    ))
}
