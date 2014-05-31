var util = require('util')

/**
 * An operation was interrupted
 *
 * @param {String} message A message describing the error
 *
 * @constructor
 */
var InterruptedError = function (message) {
    Error.captureStackTrace(this, this)
    this.message = message
}

util.inherits(InterruptedError, Error)
InterruptedError.prototype.name = 'InterruptedError'

module.exports = InterruptedError
