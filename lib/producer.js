'use strict';

var net = require('net'),
    util = require('util'),
    events = require('events'),
    _ = require('underscore'),
    Client = require('./client'),
    protocol = require('./protocol'),
    Message = protocol.Message,
    ProduceRequest = protocol.ProduceRequest,
    DEFAULTS = {
        requireAcks: 1,
        ackTimeoutMs: 100
    };


var Producer = function (connectionString, clientId) {
    this.ready = false;
    this.clientId = clientId;
    this.connectionString = connectionString;
    this.buildOptions(Array.prototype.slice.call(arguments, 4));
    this.connect();
}
util.inherits(Producer, events.EventEmitter);

Producer.prototype.buildOptions = function (args) {
    this.partitions = args.length < 1 ? DEFAULTS.partitions : args[0];
    this.requireAcks = args.length < 2 ? DEFAULTS.requireAcks : args[1];
    this.ackTimeoutMs = args.length < 3 ? DEFAULTS.ackTimeoutMs : args[2];
}

Producer.prototype.connect = function () {
    this.client = new Client(this.connectionString, this.clientId);
    // emiter...
    var self = this;
    this.client.once('connect', function () {
        self.ready = true;
        self.emit('ready'); 
    });
    this.client.on('error', function (err) {
       self.emit('error', err);
    });
    this.client.on('close', function () {
        console.log('close');
    });
    this.client.on('ready', function () {
        self.ready = true;
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

Producer.prototype.createTopics = function (topics, async, cb) {
    var self = this;
    if (!this.ready) {
        setTimeout(function () {
            self.createTopics(topics, async, cb); 
        }, 100);
        return;
    }
    topics = typeof topic === 'string' ? [topics] : topics;
    if (typeof async === 'function' || typeof async === 'undefined') {
        cb = async;
        async = true;
    }
    this.client.loadMetadataForTopics(topics, function (err, resp) {
        if (async) return cb && cb(null, 'Requests sent');
        var topicMetadata = resp[1];
        // ommit existed topics
        var topicsNotExists = 
            _.pairs(topicMetadata)
            .filter(function (pairs) { return _.isEmpty(pairs[1]) })
            .map(function (pairs) { return pairs[0] });

        if (!topicsNotExists.length) return  cb && cb(null, 'All created');
        self.client.createTopics(topicsNotExists, function (err, created) {
            cb && cb(null, 'All created');
        });
    });
}

function noAcks() {
    return 'Not require ACK';
}
module.exports = Producer;
