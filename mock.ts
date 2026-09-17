import { setDriver, type Driver } from './lib/driver.js'
import { PersistentMemoryDriver } from './memory.js'

let previous: Driver

export const mochaHooks = {
    beforeEach() {
        // eslint-disable-next-line unicorn/no-top-level-assignment-in-function
        previous = setDriver(new PersistentMemoryDriver())
    },
    afterEach() {
        setDriver(previous)
    },
}
