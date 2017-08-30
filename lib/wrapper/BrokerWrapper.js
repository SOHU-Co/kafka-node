'use strict';

var BrokerReadable = require('./BrokerReadable');
var BrokerTransform = require('./BrokerTransform');

const util = require('util');
const EventEmitter = require('events');

var BrokerWrapper = function (socket, noAckBatchOptions, idleConnectionMs) {
  EventEmitter.call(this);
  this.socket = socket;
  this.idleConnectionMs = idleConnectionMs;

  var self = this;
  var readable = new BrokerReadable();
  var transform = new BrokerTransform(noAckBatchOptions);

  readable.pipe(transform);

  transform.on('readable', function () {
    var bulkMessage = null;
    self._lastWrite = Date.now();
    while ((bulkMessage = transform.read())) {
      // eslint-disable-line no-cond-assign
      self.socket.write(bulkMessage);
    }
  });

  this.readableSocket = readable;
};

util.inherits(BrokerWrapper, EventEmitter);

BrokerWrapper.prototype.getReadyEventName = function () {
  const lp = this.socket.longpolling ? '-longpolling' : '';
  return `${this.socket.addr}${lp}-ready`;
};

BrokerWrapper.prototype.isConnected = function () {
  return !this.socket.destroyed && !this.socket.closing && !this.socket.error;
};

BrokerWrapper.prototype.isIdle = function () {
  return Date.now() - this._lastWrite >= this.idleConnectionMs;
};

BrokerWrapper.prototype.write = function (buffer) {
  this._lastWrite = Date.now();
  this.socket.write(buffer);
};

BrokerWrapper.prototype.writeAsync = function (buffer) {
  this.readableSocket.push(buffer);
};

module.exports = BrokerWrapper;
