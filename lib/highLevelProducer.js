'use strict';

var util = require('util'),
    events = require('events'),
    _ = require('lodash'),
    Client = require('./client'),
    protocol = require('./protocol'),
    Message = protocol.Message,
    KeyedMessage = protocol.KeyedMessage,
    ProduceRequest = protocol.ProduceRequest,
    partitioner = require('./partitioner'),
    DefaultPartitioner = partitioner.DefaultPartitioner,
    RandomPartitioner = partitioner.RandomPartitioner,
    CyclicPartitioner = partitioner.CyclicPartitioner,
    KeyedPartitioner = partitioner.KeyedPartitioner;

var PARTITIONER_TYPES = {
    default: 0,
    random: 1,
    cyclic: 2,
    keyed: 3
};

var DEFAULTS = {
    requireAcks: 1,
    ackTimeoutMs: 100,
    partitionerType: PARTITIONER_TYPES.cyclic
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

    if (options.partitionerType === PARTITIONER_TYPES.default) {
        this.partitioner = new DefaultPartitioner();
    } else if (options.partitionerType === PARTITIONER_TYPES.random) {
        this.partitioner = new RandomPartitioner();
    } else if (options.partitionerType === PARTITIONER_TYPES.keyed) {
        this.partitioner = new KeyedPartitioner();
    } else {
        this.partitioner = new CyclicPartitioner();
    }

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
    });
    this.client.on('error', function (err) {
        self.emit('error', err);
    });
    this.client.on('close', function () {
    });
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
    this.client.sendProduceRequest(this.buildPayloads(payloads, this.client.topicMetadata), this.requireAcks, this.ackTimeoutMs, cb);
};

HighLevelProducer.prototype.buildPayloads = function (payloads, topicMetadata) {
    var self = this;
    return payloads.map(function (p) {
        var topicPartitions = _.pluck(topicMetadata[p.topic], 'partition');
        p.partition = p.hasOwnProperty('partition') ? p.partition : self.partitioner.getPartition(topicPartitions, p.key);
        p.attributes = p.hasOwnProperty('attributes') ? p.attributes : 0;
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

HighLevelProducer.PARTITIONER_TYPES = PARTITIONER_TYPES;

module.exports = HighLevelProducer;
