var util = require('util');

/**
 * The request was sent to a broker that was not the controller.
 *
 * @param {*} message A message describing the issue.
 *
 * @constructor
 */
var NotController = function (message) {
  Error.captureStackTrace(this, this);
  this.message = message;
};

util.inherits(NotController, Error);
NotController.prototype.name = 'NotController';

module.exports = NotController;
