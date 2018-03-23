var util = require('util');

var ClientIsClosing = function (message) {
  Error.captureStackTrace(this, this);
  this.message = message || 'Client is closing';
};

util.inherits(ClientIsClosing, Error);
ClientIsClosing.prototype.name = 'ClientIsClosing';

module.exports = ClientIsClosing;
