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
var Producer = function (client, options) {
    var useOptions = options || {};

    this.ready = false;
    this.client = client;

    this.requireAcks = useOptions.requireAcks || DEFAULTS.requireAcks
    this.ackTimeoutMs = useOptions.ackTimeoutMs || DEFAULTS.ackTimeoutMs

    this.connect();
}

util.inherits(Producer, events.EventEmitter);

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
 * @param {Array.<Producer~sendPayload>} payloads An array of topic payloads
 * @param {Producer~sendCallback} cb A function to call once the send has completed
 */
Producer.prototype.send = function (payloads, cb) {
    this.client.sendProduceRequest(this.buildPayloads(payloads), this.requireAcks, this.ackTimeoutMs, cb);
}

Producer.prototype.buildPayloads = function (payloads) {
    return payloads.map(function (p) {
        p.partition = p.partition || 0;
        var messages = _.isArray(p.messages) ? p.messages : [p.messages];
        messages = messages.map(function (message) {
            return new Message(0,0,'',message);
        });
        return new ProduceRequest(p.topic, p.partition, messages);
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
    topics = typeof topics === 'string' ? [topics] : topics;
    if (typeof async === 'function' && typeof cb === 'undefined') {
        cb = async;
        async = true;
    }

    // first, load metadata to create topics
    this.client.loadMetadataForTopics(topics, function (err, resp) {
        if (err) return cb && cb(err);
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

Producer.prototype.close = function (cb) {
    this.client.close(cb);
}

function noAcks() {
    return 'Not require ACK';
}
module.exports = Producer;

/**
 * The payload object for sending messages to kafka
 *
 * @typedef Producer~sendPayload
 *
 * @property {String} topic The topic name to add to
 * @property {String|Array} messages A single message or array of messages
 * @property {Number} [partition=0] The partition to add all messages to
 */

/**
 * Callback format for Producer#send
 *
 * @example <caption>Example of the `topics` parameter</caption>
 * //Inserted a single message into the `test` topic on partition `0`. The latest offset in that topic/partition is 4
 * { test: { '0': 4 } }
 *
 * @callback Producer~sendCallback
 *
 * @param {*} error The error, if one occurred, or null if not
 * @param {Object} topics An object containing each topic that was added to with the latest offset value for the
 *      partition that was used
 */
