var util = require('util');

var GroupLoadInProgress = function (message) {
  Error.captureStackTrace(this, this);
  this.message = message;
};

util.inherits(GroupLoadInProgress, Error);
GroupLoadInProgress.prototype.name = 'GroupLoadInProgress';

module.exports = GroupLoadInProgress;
