'use strict';

var BaseProducer = require('./baseProducer');

/** @inheritdoc */
function Producer(client, options) {
  BaseProducer.call(this, client, options, this.PARTITIONER_TYPES.default);
}

util.inherits(Producer, BaseProducer);

module.exports = Producer;
