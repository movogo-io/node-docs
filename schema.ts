export type KeyRange =
    | {
          withPrefix: string
      }
    | {
          before?: string
          after: string
      }
    | {
          before: string
          after?: string
      }

export type StoredDocument = unknown

export type Revision = unknown

// `seq` counts the writes of the row under a key: 0 on the `add` or `put` that
// creates it, and the previous row's `seq` + 1 on every later write, an add
// over an expired row included. A delete removes the row and its count, so a
// document re-added under the key starts at 0 again; a consumer ordering by
// `seq` sees the delete and the add as two documents, which they are, since
// ids are never reused. `updatedAt` is the ISO instant of the last write, at
// second precision, from the clock of the context; ordering is carried by
// `seq`. A conflicting write, a `check` and a transaction that commits nothing
// leave both untouched. Rows read through an index carry neither, since an
// entry's counter is not the source document's.
export type Row<T> = {
    readonly revision: Revision
    readonly document: T
    readonly seq: number
    readonly updatedAt: string
}
