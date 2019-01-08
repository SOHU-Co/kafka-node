var util = require('util');

/**
 * The broker did not support the requested API.
 *
 *
 * @constructor
 */
var ApiNotSupportedError = function () {
  Error.captureStackTrace(this, this);
  this.message = 'The API is not supported by the receiving broker';
};

util.inherits(ApiNotSupportedError, Error);
ApiNotSupportedError.prototype.name = 'ApiNotSupportedError';

module.exports = ApiNotSupportedError;
