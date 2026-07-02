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

export function setDriver(driver: Driver) {
    const previous = _driver
    _driver = driver
    return previous
}

export function getDriver() {
    return _driver
}
