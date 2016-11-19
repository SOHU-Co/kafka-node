var util = require('util');

var IllegalGeneration = function (message) {
  Error.captureStackTrace(this, this);
  this.message = message;
};

util.inherits(IllegalGeneration, Error);
IllegalGeneration.prototype.name = 'IllegalGeneration';

module.exports = IllegalGeneration;
