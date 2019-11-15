'use strict';

const Duplex = require('stream').Duplex;
const KafkaClient = require('./kafkaClient');
const HighLevelProducer = require('./highLevelProducer');
const logger = require('./logging')('kafka-node:ProducerStream');
const _ = require('lodash');

const DEFAULTS = {
  kafkaClient: {
    kafkaHost: '127.0.0.1:9092'
  },
  producer: {
    partitionerType: 3
  }
};

const DEFAULT_HIGH_WATER_MARK = 100;

class ProducerStream extends Duplex {
  constructor (options) {
    if (options == null) {
      options = {};
    }

    super({ objectMode: true, decodeStrings: false, highWaterMark: options.highWaterMark || DEFAULT_HIGH_WATER_MARK });

    _.defaultsDeep(options, DEFAULTS);

    this.client = new KafkaClient(options.kafkaClient);
    this.producer = new HighLevelProducer(this.client, options.producer, options.producer.customPartitioner);
    this.producer.on('error', error => this.emit('error', error));
    this.bufferForReadable = [];
  }

  sendPayload (payload, callback) {
    let self = this;
    if (!_.isArray(payload)) {
      payload = [payload];
    }

    let innerCallback = function (err, data) {
      if (err) {
        return callback(err);
      }
      self.bufferForReadable.push(data);
      self._read();
      return callback(null, data);
    };

    if (!this.producer.ready) {
      this.producer.once('ready', () => this.producer.send(payload, innerCallback));
    } else {
      this.producer.send(payload, innerCallback);
    }
  }

  close (callback) {
    this.producer.close(callback);
  }

  _write (message, encoding, callback) {
    logger.debug('_write');
    this.sendPayload(message, callback);
  }

  _writev (chunks, callback) {
    logger.debug('_writev');
    const payload = _.map(chunks, 'chunk');
    this.sendPayload(payload, callback);
  }

  _read () {
    while (this.bufferForReadable.length) {
      if (!this.push(this.bufferForReadable.shift())) {
        break;
      }
    }
  }
}

module.exports = ProducerStream;
