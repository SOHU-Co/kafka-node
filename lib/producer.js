'use strict';

var util = require('util');
var BaseProducer = require('./baseProducer');

/** @inheritdoc */
function Producer (client, options) {
  BaseProducer.call(this, client, options, BaseProducer.PARTITIONER_TYPES.default);
}

util.inherits(Producer, BaseProducer);

Producer.PARTITIONER_TYPES = BaseProducer.PARTITIONER_TYPES;

module.exports = Producer;
