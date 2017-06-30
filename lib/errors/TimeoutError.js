'use strict';

var util = require('util');

var TimeoutError = function (message) {
  Error.captureStackTrace(this, this);
  this.message = message;
};

util.inherits(TimeoutError, Error);
TimeoutError.prototype.name = 'TimeoutError';

module.exports = TimeoutError;
