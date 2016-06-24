'use strict';

var BrokerReadable = require('./BrokerReadable');
var BrokerTransform = require('./BrokerTransform');

var BrokerWrapper = function (socket, noAckBatchOptions) {
  this.socket = socket;

  var self = this;
  var readable = new BrokerReadable();
  var transform = new BrokerTransform(noAckBatchOptions);

  readable.pipe(transform);

  transform.on('readable', function () {
    var bulkMessage = null;
    while (bulkMessage = transform.read()) { // eslint-disable-line no-cond-assign
      self.socket.write(bulkMessage);
    }
  });

  this.readableSocket = readable;
};

BrokerWrapper.prototype.write = function (buffer) {
  this.socket.write(buffer);
};

BrokerWrapper.prototype.writeAsync = function (buffer) {
  this.readableSocket.push(buffer);
};

module.exports = BrokerWrapper;
