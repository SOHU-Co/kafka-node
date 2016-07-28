var util = require('util');

/**
 * Failed to register the consumer
 *
 * @param {String} message A message describing the problem with the registration of the consumer
 *
 * @constructor
 */
var FailedToRegisterConsumerError = function (message) {
  Error.captureStackTrace(this, this);
  this.message = message;
};

util.inherits(FailedToRegisterConsumerError, Error);
FailedToRegisterConsumerError.prototype.name = 'FailedToRegisterConsumerError';

module.exports = FailedToRegisterConsumerError;
