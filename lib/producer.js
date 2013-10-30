'use strict';

var util = require('util'),
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

var Producer = function (client) {
    this.ready = false;
    this.client = client;
    this.buildOptions(Array.prototype.slice.call(arguments,2));
    this.connect();
}
util.inherits(Producer, events.EventEmitter);

Producer.prototype.buildOptions = function (args) {
    this.requireAcks = DEFAULTS.requireAcks;
    this.ackTimeoutMs = DEFAULTS.ackTimeoutMs;
}

Producer.prototype.connect = function () {
    // emiter...
    var self = this;
    this.ready = this.client.ready;
    if (this.ready) self.emit('ready');
    this.client.on('ready', function () {
        if (!self.ready) self.emit('ready'); 
        self.ready = true;
    });
    this.client.on('error', function (err) {
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
        p.partition = p.partition || 0;
        out[p.topic] = out[p.topic] || {};
        out[p.topic][p.partition] = out[p.topic][p.partition] || [];
        p.messages = _.isArray(p.messages) ? p.messages : [p.messages];
        p.messages.forEach(function (message) {
            var msg = new Message(0,0,'',message);
            out[p.topic][p.partition].push(msg); 
        });
        
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

    // first, load metadata to create topics
    this.client.loadMetadataForTopics(topics, function (err, resp) {
        if (async) return cb && cb(null, 'All requests sent');
        var topicMetadata = resp[1].metadata;
        // ommit existed topics
        var topicsNotExists = 
            _.pairs(topicMetadata)
            .filter(function (pairs) { return _.isEmpty(pairs[1]) })
            .map(function (pairs) { return pairs[0] });

        if (!topicsNotExists.length) return  cb && cb(null, 'All created');
        // check from zookeeper to make sure topic created
        self.client.createTopics(topicsNotExists, function (err, created) {
            cb && cb(null, 'All created');
        });
    });
}

function noAcks() {
    return 'Not require ACK';
}
module.exports = Producer;
