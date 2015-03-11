var util = require('util');

/**
 * A broker/leader was not available or discoverable for the action requested
 *
 * @param {String} message A message describing the issue with the broker
 *
 * @constructor
 */
var BrokerNotAvailableError = function (message) {
    Error.captureStackTrace(this, this);
    this.message = message;
};

util.inherits(BrokerNotAvailableError, Error);
BrokerNotAvailableError.prototype.name = 'BrokerNotAvailableError';

module.exports = BrokerNotAvailableError;
