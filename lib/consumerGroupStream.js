'use strict';

const Readable = require('stream').Readable;
const ConsumerGroup = require('./consumerGroup');
const _ = require('lodash');
const logger = require('./logging')('kafka-node:ConsumerGroupStream');
const async = require('async');
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
    if (event === 'data' && this.autoCommit && !_.isEmpty(value)) {
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

  commit (message, force, callback) {
    if (message != null && message.offset !== -1) {
      _.set(this.commitQueue, [message.topic, message.partition], message.offset);
    }

    if (this.committing && !force) {
      logger.debug('skipping committing');
      return;
    }

    this.committing = true;

    if (!force) {
      this.autoCommitTimer = setTimeout(() => {
        logger.debug('setting committing to false');
        this.committing = false;
      }, this.consumerGroup.options.autoCommitIntervalMs);
    }

    const commits = convertToCommitPayload(this.commitQueue);

    if (_.isEmpty(commits)) {
      logger.debug('commit ignored. no commits to make.');
      return;
    }

    logger.debug('committing', commits);

    this.consumerGroup.sendOffsetCommitRequest(commits, error => {
      if (error) {
        logger.error('commit request failed', error);
        if (callback) {
          return callback(error);
        }
        this.emit('error', error);
        return;
      }
      for (let tp of commits) {
        if (_.get(this.commitQueue, [tp.topic, tp.partition]) === tp.offset) {
          this.commitQueue[tp.topic][tp.partition] = null;
        }
      }
      callback && callback(null);
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
    clearTimeout(this.autoCommitTimer);
    async.series(
      [
        callback => this.commit(null, true, callback),
        callback => {
          this.consumerGroup.close(true, () => {
            callback();
            this.emit('close');
          });
        }
      ],
      callback || _.noop
    );
  }

  _destroy () {
    this.close();
  }
}

module.exports = ConsumerGroupStream;
