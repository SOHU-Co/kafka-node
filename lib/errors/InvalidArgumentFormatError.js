var util = require('util')

/**
 * An argument format is not valid.
 *
 * @param {argument} The argument name.
 * @param {message} The error message.
 *
 * @constructor
 */
var InvalidArgumentFormatError = function (argument, message) {
    this.argument = argument;
    this.message = message;
}

util.inherits(InvalidArgumentFormatError, Error)
InvalidArgumentFormatError.prototype.name = 'InvalidArgumentFormatError'

module.exports = InvalidArgumentFormatError