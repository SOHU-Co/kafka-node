'use strict';

var util = require('util');
var NestedError = require('nested-error-stacks');

/**
 * The offset for the comsumer is invalid
 *
 * @param {String} message A message describing the problem with the fetching of offsets for the consumer
 *
 * @constructor
 */
var InvalidConsumerOffsetError = function (message, nested) {
  NestedError.apply(this, arguments);
};

util.inherits(InvalidConsumerOffsetError, NestedError);
InvalidConsumerOffsetError.prototype.name = 'InvalidConsumerOffsetError';

module.exports = InvalidConsumerOffsetError;
