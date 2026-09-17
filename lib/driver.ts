import type { KeyRange, Revision, Row, StoredDocument } from '../schema.js'

export type Context = object

export type Driver = {
    connect: (context: Context) => Promise<Connection>
}

export type TransactionItem =
    | {
          op: 'add'
          table: string
          partition: string
          key: string
          document: StoredDocument
          newRevision: Revision
      }
    | {
          op: 'update'
          table: string
          partition: string
          key: string
          revision: Revision
          document: StoredDocument
          newRevision: Revision
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
    ) => Promise<Revision>
    get: (
        table: string,
        partition: string,
        key: string,
    ) => Promise<Row<StoredDocument> & { partition: string; key: string }>
    getPartitions: (table: string) => AsyncIterable<string>
    getPartition: (
        table: string,
        partition: string,
        keyRange?: KeyRange,
    ) => AsyncIterable<{ key: string; revision: Revision; document: StoredDocument }>
    update: (
        table: string,
        partition: string,
        key: string,
        revision: Revision,
        document: StoredDocument,
    ) => Promise<Revision>
    delete: (table: string, partition: string, key: string, revision: Revision) => Promise<void>
    transact: (items: TransactionItem[]) => Promise<void>
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
