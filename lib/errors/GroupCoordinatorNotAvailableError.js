var util = require('util');

var GroupCoordinatorNotAvailable = function (message) {
  Error.captureStackTrace(this, this);
  this.message = message;
};

util.inherits(GroupCoordinatorNotAvailable, Error);
GroupCoordinatorNotAvailable.prototype.name = 'GroupCoordinatorNotAvailable';

module.exports = GroupCoordinatorNotAvailable;
