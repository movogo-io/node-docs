const tooLargeCode = 'docs.transaction_too_large'

export function notFound() {
    return Object.assign(new Error('Not found'), { status: 404, statusCode: 404 })
}

export function conflict() {
    return Object.assign(new Error('Conflict'), { status: 409, statusCode: 409 })
}

// An atomic unit the backend cannot commit: a defect of the handler, never of
// the request, so it carries no status and is answered as a 500.
export function transactionTooLarge(message: string) {
    return Object.assign(new Error(message), { code: tooLargeCode })
}

export function isNotFound(e: unknown) {
    return hasStatus(e, 404)
}

export function isConflict(e: unknown) {
    return hasStatus(e, 409)
}

export function isTransactionTooLarge(e: unknown) {
    return typeof e === 'object' && e !== null && 'code' in e && e.code === tooLargeCode
}

function hasStatus(e: unknown, status: number) {
    return typeof e === 'object' && e !== null && 'status' in e && e.status === status
}
