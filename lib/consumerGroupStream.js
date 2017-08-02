'use strict';

const Readable = require('stream').Readable;
const ConsumerGroup = require('./consumerGroup');
const _ = require('lodash');
const logger = require('./logging')('kafka-node:ConsumerGroupStream');

const DEFAULT_HIGH_WATER_MARK = 100;

const DEFAULTS = {
  autoCommit: true
};

function convertToCommitPayload (messages) {
  const ret = [];
  _.forEach(messages, function (partitionOffset, topic) {
    _.forEach(partitionOffset, function (offset, partition) {
      if (offset != null) {
        ret.push({
          topic: topic,
          partition: partition,
          offset: offset,
          metadata: 'm'
        });
      }
    });
  });
  return ret;
}

class ConsumerGroupStream extends Readable {
  constructor (options, topics) {
    super({ objectMode: true, highWaterMark: options.highWaterMark || DEFAULT_HIGH_WATER_MARK });

    _.defaultsDeep(options || {}, DEFAULTS);

    this.autoCommit = options.autoCommit;

    options.connectOnReady = false;
    options.autoCommit = false;

    this.consumerGroup = new ConsumerGroup(options, topics);

    this.messageBuffer = [];
    this.commitQueue = {};

    this.consumerGroup.on('error', error => this.emit('error', error));
    this.consumerGroup.on('message', message => {
      this.messageBuffer.push(message);
      this.consumerGroup.pause();
    });
    this.consumerGroup.on('done', message => {
      setImmediate(() => this.transmitMessages());
    });
  }

  emit (event, value) {
    if (event === 'data' && !_.isEmpty(value)) {
      setImmediate(() => this.commit(value));
    }
    super.emit.apply(this, arguments);
  }

  _read () {
    logger.debug('_read called');
    if (!this.consumerGroup.ready) {
      logger.debug('consumerGroup is not ready, calling consumerGroup.connect');
      this.consumerGroup.connect();
    }
    this._reading = true;
    this.transmitMessages();
  }

  commit (message) {
    if (message.offset !== -1) {
      _.set(this.commitQueue, [message.topic, message.partition], message.offset);
    }

    if (!this.autoCommit || this.committing) {
      logger.debug('skipping committing');
      return;
    }
    this.committing = true;
    setTimeout(() => {
      logger.debug('setting committing to false');
      this.committing = false;
    }, this.consumerGroup.options.autoCommitIntervalMs);

    const commits = convertToCommitPayload(this.commitQueue);

    if (_.isEmpty(commits)) {
      logger.debug('commit ignored. no commits to make.');
      return;
    }

    logger.debug('committing', commits);

    this.consumerGroup.sendOffsetCommitRequest(commits, error => {
      if (error) {
        logger.error('commit request failed', error);
        this.emit('error', error);
        return;
      }
      for (let tp of commits) {
        if (_.get(this.commitQueue, [tp.topic, tp.partition]) === tp.offset) {
          this.commitQueue[tp.topic][tp.partition] = null;
        }
      }
    });
  }

  transmitMessages () {
    while (this._reading && this.messageBuffer.length > 0) {
      this._reading = this.push(this.messageBuffer.shift());
    }
    if (this.messageBuffer.length === 0 && this._reading) {
      this.consumerGroup.resume();
    }
  }

  close (callback) {
    this.consumerGroup.close(true, error => {
      callback(error);
      this.emit('close');
    });
  }

  _destroy () {
    this.consumerGroup.close(true, () => this.emit('close'));
  }
}

module.exports = ConsumerGroupStream;
