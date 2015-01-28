'use strict';

var util = require('util'),
    events = require('events'),
    _ = require('lodash'),
    Client = require('./client'),
    protocol = require('./protocol'),
    Message = protocol.Message,
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
    var useOptions = options || {};

    this.ready = false;
    this.client = client;

    this.requireAcks = useOptions.requireAcks || DEFAULTS.requireAcks
    this.ackTimeoutMs = useOptions.ackTimeoutMs || DEFAULTS.ackTimeoutMs

    this.connect();
}

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
    this.client.on('error', function (err) {
        self.emit('error', err);
    });
    this.client.on('close', function () {
    });
}

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
    this.client.sendProduceRequest(this.buildPayloads(payloads), this.requireAcks, this.ackTimeoutMs, cb);
}

HighLevelProducer.prototype.buildPayloads = function (payloads) {
    var self = this;
    return payloads.map(function (p) {
        p.partition = p.partition || self.client.nextPartition(p.topic);
        p.attributes = p.attributes || 0;
        var messages = _.isArray(p.messages) ? p.messages : [p.messages];
        messages = messages.map(function (message) {
            return new Message(0, 0, '', message);
        });
        return new ProduceRequest(p.topic, p.partition, messages, p.attributes);
    });
}

HighLevelProducer.prototype.createTopics = function (topics, async, cb) {
    if (!this.ready) {
        return cb(new Error('Producer not ready!'));
    }

    this.client.createTopics(topics, async, cb);
}

HighLevelProducer.prototype.close = function (cb) {
    this.client.close(cb);
}

function noAcks() {
    return 'Not require ACK';
}

module.exports = HighLevelProducer;

/**
 * The payload object for sending messages to kafka
 *
 * @typedef HighLevelProducer~sendPayload
 *
 * @property {String} topic The topic name to add to
 * @property {String|Array} messages A single message or array of messages
 */

/**
 * Callback format for HighLevelProducer#send
 *
 * @example <caption>Example of the `topics` parameter</caption>
 * //Inserted a single message into the `test` topic. The latest offset in that topic is 4
 * { test: { '0': 4 } }
 *
 * @callback HighLevelProducer~sendCallback
 *
 * @param {*} error The error, if one occurred, or null if not
 * @param {Object} topics An object containing each topic that was added to with the latest offset value
 */
