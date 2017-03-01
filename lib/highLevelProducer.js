'use strict';

var util = require('util');
var BaseProducer = require('./baseProducer');

/** @inheritdoc */
function HighLevelProducer (client, options, customPartitioner) {
  BaseProducer.call(this, client, options, BaseProducer.PARTITIONER_TYPES.cyclic, customPartitioner);
}

util.inherits(HighLevelProducer, BaseProducer);

HighLevelProducer.PARTITIONER_TYPES = BaseProducer.PARTITIONER_TYPES;

module.exports = HighLevelProducer;
