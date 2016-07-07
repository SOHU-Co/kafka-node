var util = require('util');

/**
 * The offset for the comsumer is invalid
 *
 * @param {String} message A message describing the problem with the fetching of offsets for the consumer
 *
 * @constructor
 */
var InvalidConsumerOffsetError = function (message) {
  Error.captureStackTrace(this, this);
  this.message = message;
};

util.inherits(InvalidConsumerOffsetError, Error);
InvalidConsumerOffsetError.prototype.name = 'InvalidConsumerOffsetError';

module.exports = InvalidConsumerOffsetError;
