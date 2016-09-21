var util = require('util');

var NotCoordinatorForGroup = function (message) {
  Error.captureStackTrace(this, this);
  this.message = message;
};

util.inherits(NotCoordinatorForGroup, Error);
NotCoordinatorForGroup.prototype.name = 'NotCoordinatorForGroup';

module.exports = NotCoordinatorForGroup;
