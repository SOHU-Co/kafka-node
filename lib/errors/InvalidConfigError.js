var util = require('util');

var InvalidConfigError = function (message) {
  Error.captureStackTrace(this, this);
  this.message = message;
};

util.inherits(InvalidConfigError, Error);
InvalidConfigError.prototype.name = 'InvalidConfigError';

module.exports = InvalidConfigError;
