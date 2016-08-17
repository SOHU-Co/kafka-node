var util = require('util');
var EventEmitter = require('events').EventEmitter;

function FakeSocket () {
  EventEmitter.call(this);

  this.end = function () {
    var self = this;
    setImmediate(function () {
      self.emit('end');
    });
  };
  this.close = function () {};
  this.setKeepAlive = function () {};
  this.destroy = function () {};
}

util.inherits(FakeSocket, EventEmitter);

module.exports = FakeSocket;
