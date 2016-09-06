var util = require('util');
var NestedError = require('nested-error-stacks');

/**
 * Failed to register the consumer
 *
 * @param {String} message A message describing the problem with the registration of the consumer
 * @param {Error} error An error related to the registration of the consumer
 *
 * @constructor
 */
var FailedToRegisterConsumerError = function (message, nested) {
  NestedError.call(this, message, nested);
  this.message = message;
};

util.inherits(FailedToRegisterConsumerError, NestedError);
FailedToRegisterConsumerError.prototype.name = 'FailedToRegisterConsumerError';

module.exports = FailedToRegisterConsumerError;
