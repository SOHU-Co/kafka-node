'use strict';

var KafkaBuffer = function (batchSize, batchAge) {
  this._batch_size = batchSize;
  this._batch_age = batchAge;
  this._batch_age_timer = null;
  this._buffer = null;
};

KafkaBuffer.prototype.addChunk = function (buffer, callback) {
  if (this._buffer == null) {
    this._buffer = Buffer.from(buffer);
  } else {
    this._buffer = Buffer.concat([this._buffer, buffer]);
  }

  if (typeof callback !== 'undefined' && callback != null) {
    if (
      this._batch_size == null ||
      this._batch_age == null ||
      (this._buffer && this._buffer.length > this._batch_size)
    ) {
      callback();
    } else {
      this._setupTimer(callback);
    }
  }
};

KafkaBuffer.prototype._setupTimer = function (callback) {
  var self = this;

  if (this._batch_age_timer != null) {
    clearTimeout(this._batch_age_timer);
  }

  this._batch_age_timer = setTimeout(function () {
    if (self._buffer && self._buffer.length > 0) {
      callback();
    }
  }, this._batch_age);
};

KafkaBuffer.prototype.getBatch = function () {
  return this._buffer;
};

KafkaBuffer.prototype.truncateBatch = function () {
  this._buffer = null;
};

module.exports = KafkaBuffer;
