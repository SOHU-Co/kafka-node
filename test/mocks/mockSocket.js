var util = require('util');
var EventEmitter = require('events').EventEmitter;

function FakeSocket () {
  EventEmitter.call(this);

  this.unref = function () {};

  this.end = function () {
    var self = this;
    setImmediate(function () {
      self.emit('end');
    });
  };
  this.close = function () {};
  this.setKeepAlive = function () {};
  this.destroy = function () {};
  this.write = function () {};
}

util.inherits(FakeSocket, EventEmitter);

module.exports = FakeSocket;
