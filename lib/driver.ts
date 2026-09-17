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

let _driver: Driver = {
    connect: () => Promise.reject<Connection>(new Error('No driver set, please call setDriver()')),
}
const _decorators: ((driver: Driver) => Driver)[] = []
let _decorated: Driver | undefined

export function setDriver(driver: Driver) {
    const previous = _driver
    _driver = driver
    _decorated = undefined
    return previous
}

export function decorateDriver(decorator: (driver: Driver) => Driver) {
    _decorators.push(decorator)
    _decorated = undefined
    return () => {
        const index = _decorators.lastIndexOf(decorator)
        if (index === -1) {
            return
        }
        _decorators.splice(index, 1)
        _decorated = undefined
    }
}

export function getDriver() {
    return (_decorated ??= _decorators.reduce((driver, decorator) => decorator(driver), _driver))
}
