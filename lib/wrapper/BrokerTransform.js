'use strict';

var util = require('util'),
	Transform = require('stream').Transform,
    KafkaBuffer = require('../batch/KafkaBuffer');

var BrokerTransform = function (options) {
    Transform.call(this, options);
    this.noAckBatchSize = options ? options.noAckBatchSize : null;
    this.noAckBatchAge = options ? options.noAckBatchAge : null;
    this._KafkaBuffer = new KafkaBuffer(this.noAckBatchSize, this.noAckBatchAge);
};

util.inherits(BrokerTransform, Transform);

BrokerTransform.prototype._transform = function (chunk, enc, done) {
    this._KafkaBuffer.addChunk(chunk, this._transformNext.bind(this))
	done();
};

BrokerTransform.prototype._transformNext = function () {
    this.push(this._KafkaBuffer.getBatch());
    this._KafkaBuffer.truncateBatch();
}

module.exports = BrokerTransform;
