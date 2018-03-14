'use strict';

var assert = require('assert');
var util = require('util');
var EventEmitter = require('events');
var _ = require('lodash');
var protocol = require('./protocol');
var Message = protocol.Message;
var KeyedMessage = protocol.KeyedMessage;
var ProduceRequest = protocol.ProduceRequest;
var partitioner = require('./partitioner');
var DefaultPartitioner = partitioner.DefaultPartitioner;
var RandomPartitioner = partitioner.RandomPartitioner;
var CyclicPartitioner = partitioner.CyclicPartitioner;
var KeyedPartitioner = partitioner.KeyedPartitioner;
var CustomPartitioner = partitioner.CustomPartitioner;

var PARTITIONER_TYPES = {
  default: 0,
  random: 1,
  cyclic: 2,
  keyed: 3,
  custom: 4
};

var PARTITIONER_MAP = {
  0: DefaultPartitioner,
  1: RandomPartitioner,
  2: CyclicPartitioner,
  3: KeyedPartitioner,
  4: CustomPartitioner
};

var DEFAULTS = {
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
 * @param {Number} [defaultPartitionType] The default partitioner type
 * @param {Object} [customPartitioner] a custom partitinoer to use of the form: function (partitions, key)
 * @constructor
 */
function BaseProducer (client, options, defaultPartitionerType, customPartitioner) {
  EventEmitter.call(this);
  options = options || {};

  this.ready = false;
  this.client = client;

  this.requireAcks = options.requireAcks === undefined ? DEFAULTS.requireAcks : options.requireAcks;
  this.ackTimeoutMs = options.ackTimeoutMs === undefined ? DEFAULTS.ackTimeoutMs : options.ackTimeoutMs;

  if (customPartitioner !== undefined && options.partitionerType !== PARTITIONER_TYPES.custom) {
    throw new Error('Partitioner Type must be custom if providing a customPartitioner.');
  } else if (customPartitioner === undefined && options.partitionerType === PARTITIONER_TYPES.custom) {
    throw new Error('No customer partitioner defined');
  }

  var partitionerType = PARTITIONER_MAP[options.partitionerType] || PARTITIONER_MAP[defaultPartitionerType];

  // eslint-disable-next-line
  this.partitioner = new partitionerType(customPartitioner);

  this.connect();
}

util.inherits(BaseProducer, EventEmitter);

BaseProducer.prototype.connect = function () {
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
    let topics = Object.keys(this.topicMetadata);
    this.refreshMetadata(topics, function (error) {
      if (error) {
        self.emit('error', error);
      }
    });
  });
  this.client.on('error', function (err) {
    self.emit('error', err);
  });
  this.client.on('close', function () {});
};

/**
 * Sends a new message or array of messages to a topic/partition
 * This will use the
 *
 * @see Client#sendProduceRequest for a more low level way to send messages to kafka
 *
 * @param {Array.<BaseProducer~sendPayload>} payloads An array of topic payloads
 * @param {BaseProducer~sendCallback} cb A function to call once the send has completed
 */
BaseProducer.prototype.send = function (payloads, cb) {
  var client = this.client;
  var requireAcks = this.requireAcks;
  var ackTimeoutMs = this.ackTimeoutMs;

  client.sendProduceRequest(this.buildPayloads(payloads, client.topicMetadata), requireAcks, ackTimeoutMs, cb);
};

BaseProducer.prototype.buildPayloads = function (payloads, topicMetadata) {
  const topicPartitionRequests = Object.create(null);
  payloads.forEach(p => {
    p.partition = p.hasOwnProperty('partition')
      ? p.partition
      : this.partitioner.getPartition(_.map(topicMetadata[p.topic], 'partition'), p.key);
    p.attributes = p.hasOwnProperty('attributes') ? p.attributes : 0;
    let messages = _.isArray(p.messages) ? p.messages : [p.messages];

    messages = messages.map(function (message) {
      if (message instanceof KeyedMessage) {
        return message;
      }
      return new Message(0, 0, p.key, message, p.timestamp || Date.now());
    });

    let key = p.topic + p.partition;
    let request = topicPartitionRequests[key];

    if (request == null) {
      topicPartitionRequests[key] = new ProduceRequest(p.topic, p.partition, messages, p.attributes);
    } else {
      assert(request.attributes === p.attributes);
      Array.prototype.push.apply(request.messages, messages);
    }
  });
  return _.values(topicPartitionRequests);
};

BaseProducer.prototype.createTopics = function (topics, async, cb) {
  if (!this.ready) {
    return cb(new Error('Producer not ready!'));
  }

  this.client.createTopics(topics, async, cb);
};

BaseProducer.prototype.close = function (cb) {
  this.client.close(cb);
};

BaseProducer.PARTITIONER_TYPES = PARTITIONER_TYPES;

module.exports = BaseProducer;
