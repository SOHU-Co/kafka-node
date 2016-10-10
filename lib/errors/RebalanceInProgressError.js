var util = require('util');

var RebalanceInProgress = function (message) {
  Error.captureStackTrace(this, this);
  this.message = message;
};

util.inherits(RebalanceInProgress, Error);
RebalanceInProgress.prototype.name = 'RebalanceInProgress';

module.exports = RebalanceInProgress;
