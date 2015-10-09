'use strict';

var util = require('util'),
    debug = require('debug')('kafka-node:HighLevelProducer'),
    events = require('events'),
    _ = require('lodash'),
    async = require('async'),
    Client = require('./client'),
    protocol = require('./protocol'),
    Message = protocol.Message,
    KeyedMessage = protocol.KeyedMessage,
    ProduceRequest = protocol.ProduceRequest,
    DEFAULTS = {
        requireAcks: 1,
        ackTimeoutMs: 100
    };

/**
 * Provides common functionality for a kafka producer
 *
 * @param {Client} client A kafka client object to use for the producer
 * @param {Object} [options] An object containing configuration options
 * @param {Number} [options.requireAcks=1] Configuration for when to consider a message as acknowledged.
 *      <li>0 = No ack required</li>
 *      <li>1 = Leader ack required</li>
 *      <li>-1 = All in sync replicas ack required</li>
 *
 * @param {Number} [options.ackTimeoutMs=100] The amount of time in milliseconds to wait for all acks before considered
 *      the message as errored
 *
 * @constructor
 */
var HighLevelProducer = function (client, options) {
    options = options || {};

    this.ready = false;
    this.client = client;

    this.requireAcks = options.requireAcks === undefined
        ? DEFAULTS.requireAcks
        : options.requireAcks;
    this.ackTimeoutMs = options.ackTimeoutMs === undefined
        ? DEFAULTS.ackTimeoutMs
        : options.ackTimeoutMs;

    this.buffer = [];
    this.bufferLimit = options.bufferLimit || 0;
    this.buffering = false;

    this.connect();
};

util.inherits(HighLevelProducer, events.EventEmitter);

HighLevelProducer.prototype.connect = function () {
    // emiter...
    var self = this;
    this.ready = this.client.ready;
    if (this.ready) self.emit('ready');
    this.client.on('ready', function () {
        if (!self.ready) {
            self.ready = true;
            self.emit('ready');
        }
    });
    this.client.on('brokersChanged', function () {
        this.topicMetadata = {};
        self.flushBuffer();
    });
    this.client.on('error', function (err) {
        var isConnectionError = err.code === 'EPIPE' || err.code === 'ECONNRESET' || err.code === 'ECONNREFUSED';
        var isBrokerNotAvailable = err.message === 'NotLeaderForPartition' || err.name === 'BrokerNotAvailableError'
        if (self.bufferLimit > 0 && (isConnectionError || isBrokerNotAvailable)) {
            self.buffering = true;
            debug(err); // all good here! producer will buffer ...
        } else {
            self.emit('error', err);
        }
    });
    this.client.on('close', function () {
    });
};

/**
 * Adds a message to the buffer for retrying when broker becomes available.
 */
HighLevelProducer.prototype.appendToBuffer = function (message) {
    this.buffering = true;
    this.buffer.push(message);
};

/**
 * Send all messages held in the memory buffer while the broker has been unavailable.
 */
HighLevelProducer.prototype.flushBuffer = function () {
    var self = this;
    var error = null;
    async.whilst(
        function () {
            return !error && self.buffer.length > 0;
        },
        function (cb) {
            var message = self.buffer.shift();
            self.buffering = false;
            self.send(message.payloads, function (err, response) {
                if (err) {
                    return cb(err);
                }
                message.cb(null, response);
                cb();
            });
        },
        function (err) {
            if (err) {
                error = err;
                debug('failed to flush buffer', err);
                return;
            }
            debug('buffer has been successfully flushed');
        }
    );
};

/**
 * Sends a new message or array of messages to a topic/partition
 * This will use the
 *
 * @see Client#sendProduceRequest for a more low level way to send messages to kafka
 *
 * @param {Array.<HighLevelProducer~sendPayload>} payloads An array of topic payloads
 * @param {HighLevelProducer~sendCallback} cb A function to call once the send has completed
 */
HighLevelProducer.prototype.send = function (payloads, cb) {
    var self = this,
        client = this.client,
        requireAcks = this.requireAcks,
        ackTimeoutMs = this.ackTimeoutMs;

    client.sendProduceRequest(this.buildPayloads(payloads), requireAcks, ackTimeoutMs, function producerSend(err, res) {
        if (err) {
            var isBrokerNotAvailable = err.message === 'NotLeaderForPartition' || err.name === 'BrokerNotAvailableError';
            if ((self.buffering || isBrokerNotAvailable) && self.bufferLimit > self.buffer.length) {
                self.appendToBuffer({payloads: payloads, cb: cb});
            } else {
                cb(err);
            }
        } else {
            cb(null, res);
        }
    });
};

HighLevelProducer.prototype.buildPayloads = function (payloads) {
    var self = this;
    return payloads.map(function (p) {
        p.partition = p.hasOwnProperty('partition') ? p.partition : self.client.nextPartition(p.topic);
        p.attributes = p.attributes || 0;
        var messages = _.isArray(p.messages) ? p.messages : [p.messages];
        messages = messages.map(function (message) {
            if (message instanceof KeyedMessage) {
                return message;
            }
            return new Message(0, 0, '', message);
        });
        return new ProduceRequest(p.topic, p.partition, messages, p.attributes);
    });
};

HighLevelProducer.prototype.createTopics = function (topics, async, cb) {
    if (!this.ready) {
        return cb(new Error('Producer not ready!'));
    }

    this.client.createTopics(topics, async, cb);
};

HighLevelProducer.prototype.close = function (cb) {
    this.client.close(cb);
};

function noAcks() {
    return 'Not require ACK';
}

module.exports = HighLevelProducer;
