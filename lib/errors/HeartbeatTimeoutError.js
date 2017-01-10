'use strict';

var util = require('util');

var HeartbeatTimeout = function (message) {
  Error.captureStackTrace(this, this);
  this.message = message;
};

util.inherits(HeartbeatTimeout, Error);
HeartbeatTimeout.prototype.name = 'HeartbeatTimeout';

module.exports = HeartbeatTimeout;
