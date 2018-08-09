var util = require('util');

/**
 * Thrown when SASL authentication fails for any reason.
 *
 * @param {Number} errorCode the error code that caused the error.
 * @param {String} message A message describing the authentication problem.
 *
 * @constructor
 */
var SaslAuthenticationError = function (errorCode, message) {
  Error.captureStackTrace(this, this);
  this.errorCode = errorCode;
  this.message = message;
};

util.inherits(SaslAuthenticationError, Error);
SaslAuthenticationError.prototype.name = 'SaslAuthenticationError';

module.exports = SaslAuthenticationError;
