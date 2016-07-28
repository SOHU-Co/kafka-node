var util = require('util');

/**
 * Failed to rebalance the consumer
 *
 * @param {String} message A message describing the error during rebalancing of the consumer
 *
 * @constructor
 */
var FailedToRebalanceConsumerError = function (message) {
  Error.captureStackTrace(this, this);
  this.message = message;
};

util.inherits(FailedToRebalanceConsumerError, Error);
FailedToRebalanceConsumerError.prototype.name = 'FailedToRebalanceConsumerError';

module.exports = FailedToRebalanceConsumerError;
