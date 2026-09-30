const separator = '#'
// Must be the character right after the separator, or prefix ranges leak.
const successor = '$'

// Never throws: extractors pass stored fields of every legacy shape as they are.
export function compositeKey(first: unknown, ...rest: unknown[]): string | undefined {
    const parts = [first, ...rest]
    if (!parts.every(isNonEmptyString)) {
        return undefined
    }
    // '%' is escaped first, or the join stops being one-to-one.
    return parts
        .map(part => part.replaceAll('%', '%25').replaceAll('#', '%23').replaceAll('\u{0}', '%00'))
        .join(separator)
}

export function compositeRange(
    prefix: [unknown, ...unknown[]],
    after?: string,
): { after: string; before: string } | undefined {
    const joined = compositeKey(...prefix)
    if (joined === undefined) {
        return undefined
    }
    const first = `${joined}${separator}`
    const before = `${joined}${successor}`
    const lower = after !== undefined && first < after ? after : first
    if (before <= lower) {
        return undefined
    }
    return { after: lower, before }
}

function isNonEmptyString(part: unknown): part is string {
    return typeof part === 'string' && part !== ''
}
