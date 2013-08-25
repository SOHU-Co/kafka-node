'use strict';

var net = require('net'),
    util = require('util'),
    events = require('events'),
    Client = require('./client'),
    protocol = require('./protocol'),
    Message = protocol.Message,
    ProduceRequest = protocol.ProduceRequest,
    DEFAULTS = {
        requireAcks: 1,
        ackTimeoutMs: 100
    };


var Producer = function (host, port, clientId) {
    this.buildOptions(Array.prototype.slice.call(arguments, 4));
    this.connect(host, port, clientId);
}
util.inherits(Producer, events.EventEmitter);

Producer.prototype.buildOptions = function (args) {
    this.partitions = args.length < 1 ? DEFAULTS.partitions : args[0];
    this.requireAcks = args.length < 2 ? DEFAULTS.requireAcks : args[1];
    this.ackTimeoutMs = args.length < 3 ? DEFAULTS.ackTimeoutMs : args[2];
}

Producer.prototype.connect = function (host, port, clientId) {
    this.client = new Client(host, port, clientId);
    // emiter...
    var self = this;
    this.client.on('error', function (err) {
        self.emit('error', err);
    });
    this.client.on('close', function () {
        console.log('close');
    });
}

Producer.prototype.send = function (payloads, cb) {
   this.client.sendProduceRequest(this.buildPayloads(payloads), this.requireAcks, this.ackTimeoutMs, cb);
}

Producer.prototype.buildPayloads = function (payloads) {
    var groupedMessages = payloads.reduce(function (out, p) {
        var msg = new Message(0,0,'',p.message);
        p.partition = p.partition || 0;

        out[p.topic] = out[p.topic] || {};
        out[p.topic][p.partition] = out[p.topic][p.partition] || [];
        out[p.topic][p.partition].push(msg);
        return out;
    }, {});
    return payloads.map(function (p) {
        return new ProduceRequest(p.topic, p.partition, groupedMessages[p.topic][p.partition]);
    });
}

function noAcks() {
    return 'Not require ACK';
}
module.exports = Producer;
