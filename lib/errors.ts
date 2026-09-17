export function notFound() {
    return Object.assign(new Error('Not found'), { status: 404, statusCode: 404 })
}

export function conflict() {
    return Object.assign(new Error('Conflict'), { status: 409, statusCode: 409 })
}

export function isNotFound(e: unknown) {
    return hasStatus(e, 404)
}

export function isConflict(e: unknown) {
    return hasStatus(e, 409)
}

function hasStatus(e: unknown, status: number) {
    if (typeof e !== 'object' || e === null) {
        return false
    }
    return ('status' in e && e.status === status) || ('statusCode' in e && e.statusCode === status)
}
