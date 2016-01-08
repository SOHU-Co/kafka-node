'use strict';

var util = require('util'),
	Readable = require('stream').Readable;

var BrokerReadable = function (options) {
	Readable.call(this, options);
};

util.inherits(BrokerReadable, Readable);

BrokerReadable.prototype._read = function (size) {};

module.exports = BrokerReadable;
