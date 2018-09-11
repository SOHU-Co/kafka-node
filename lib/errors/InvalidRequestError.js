const util = require('util');

/**
 * The request was invalid for a specific reason.
 *
 * @param {*} message A message describing the issue.
 *
 * @constructor
 */
const InvalidRequest = function (message) {
  Error.captureStackTrace(this, this);
  this.message = message;
};

util.inherits(InvalidRequest, Error);
InvalidRequest.prototype.name = 'InvalidRequest';

module.exports = InvalidRequest;
