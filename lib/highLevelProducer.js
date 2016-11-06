'use strict';

var util = require('util');
var BaseProducer = require('./baseProducer');

/** @inheritdoc */
function HighLevelProducer (client, options) {
  BaseProducer.call(this, client, options, this.PARTITIONER_TYPES.cyclic);
}

util.inherits(HighLevelProducer, BaseProducer);

module.exports = HighLevelProducer;
