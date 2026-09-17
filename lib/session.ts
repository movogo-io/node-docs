import { getDriver, type Connection } from './driver.js'
import { nowSecondsOf } from './expiry.js'

export type Session = {
    connection: Promise<Connection>
    nowSeconds: () => number
}

export function openSession(context: { now?: () => Date }): Session {
    return {
        connection: getDriver().connect(context),
        nowSeconds: () => nowSecondsOf(context),
    }
}
