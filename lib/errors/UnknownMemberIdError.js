var util = require('util');

var UnknownMemberId = function (message) {
  Error.captureStackTrace(this, this);
  this.message = message;
};

util.inherits(UnknownMemberId, Error);
UnknownMemberId.prototype.name = 'UnknownMemberId';

module.exports = UnknownMemberId;
