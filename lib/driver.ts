import type { KeyRange, Revision, Row, StoredDocument } from '../schema.js'

export type Context = object

export type Driver = {
    connect: (context: Context) => Promise<Connection>
}

// Times handed to drivers are integer epoch seconds. A row whose stored
// `expiresAt` is at or before the `now` of the operation is expired: `add` and
// `put` overwrite it as if it were missing, while `update`, `delete`, and
// `check` conflict on it. `put` and `clear` ignore `now`.
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

export type Connection = {
    close: () => Promise<void>
    add: (
        table: string,
        partition: string,
        key: string,
        document: StoredDocument,
        options: { now: number; expiresAt?: number },
    ) => Promise<Revision>
    get: (
        table: string,
        partition: string,
        key: string,
    ) => Promise<Row<StoredDocument> & { partition: string; key: string; expiresAt?: number }>
    getPartitions: (table: string) => AsyncIterable<string>
    getPartition: (
        table: string,
        partition: string,
        keyRange?: KeyRange,
    ) => AsyncIterable<{
        key: string
        revision: Revision
        document: StoredDocument
        expiresAt?: number
    }>
    update: (
        table: string,
        partition: string,
        key: string,
        revision: Revision,
        document: StoredDocument,
        options: { now: number; expiresAt?: number },
    ) => Promise<Revision>
    delete: (
        table: string,
        partition: string,
        key: string,
        revision: Revision,
        options: { now: number },
    ) => Promise<void>
    transact: (items: TransactionItem[], options: { now: number }) => Promise<void>
}

const state: {
    driver: Driver
    decorators: ((driver: Driver) => Driver)[]
    decorated?: Driver
} = {
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

export function getDriver() {
    return (state.decorated ??= state.decorators.reduce(
        (driver, decorator) => decorator(driver),
        state.driver,
    ))
}
