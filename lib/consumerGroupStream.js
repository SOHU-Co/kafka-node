'use strict';

const Readable = require('stream').Readable;
const ConsumerGroup = require('./consumerGroup');
const _ = require('lodash');
const TimeoutError = require('./errors/TimeoutError');
const logger = require('./logging')('kafka-node:ConsumerGroupStream');
const async = require('async');
const DEFAULT_HIGH_WATER_MARK = 100;
const Denque = require('denque');

const DEFAULTS = {
  autoCommit: true,
  treatTimeoutsAsSuccess: false
};

function isTimeoutError (error) {
  return error && (error.errorCode === 7 || error instanceof TimeoutError);
}

class ConsumerGroupStream extends Readable {
  constructor (options, topics) {
    super({ objectMode: true, highWaterMark: options.highWaterMark || DEFAULT_HIGH_WATER_MARK });

    _.defaultsDeep(options || {}, DEFAULTS);
    const self = this;

    this.autoCommit = options.autoCommit;

    options.connectOnReady = false;
    options.autoCommit = false;
    const originalOnRebalance = options.onRebalance;
    options.onRebalance = function (isAlreadyMember, callback) {
      const autoCommit = _.once(function (err) {
        if (err) {
          callback(err);
        } else {
          self.commit(null, true, callback);
        }
      });
      if (typeof originalOnRebalance === 'function') {
        try {
          originalOnRebalance(isAlreadyMember, autoCommit);
        } catch (e) {
          autoCommit(e);
        }
      } else {
        autoCommit();
      }
    };

    this.options = options;
    this.consumerGroup = new ConsumerGroup(options, topics);

    this.messageBuffer = new Denque();
    this.commitQueue = {};
    this.commitGenericCallbacks = [];
    this.commitPartitionCallbacks = new Map();

    this.consumerGroup.on('error', error => this.emit('error', error));
    this.consumerGroup.on('connect', () => this.emit('connect'));
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
    if (typeof callback === 'function') {
      callback = _.once(callback);
      if (message == null) {
        this.commitGenericCallbacks.push(callback);
      } else {
        const key = message.topic + ':' + message.partition;
        let callbacks = this.commitPartitionCallbacks.get(key);
        if (callbacks == null) {
          callbacks = [];
          this.commitPartitionCallbacks.set(key, callbacks);
        }
        callbacks.push(callback);
      }
    }
    if (message != null && message.offset !== -1) {
      _.set(this.commitQueue, [message.topic, message.partition], message.offset + 1);
    }

    if (this.autoCommitPending && !force) {
      logger.debug('commit is pending, delaying');
      return;
    }

    this.commitQueued(force);
  }


  commitQueued (force) {
    if (!force) {
      if (!this.autoCommitPending) {
        this.autoCommitPending = true;
        this.autoCommitTimer = setTimeout(() => {
          logger.debug('registering auto commit to allow batching');
          this.autoCommitPending = false;

          this.commitQueued(true);
        }, this.consumerGroup.options.autoCommitIntervalMs);
        this.autoCommitTimer.unref();
      }
      return;
    }

    if (this.commitPendingPromise) {
      this.needsCommitWhenDone = true;
      return;
    }

    let commits = [];
    /**
     * @type {Map<string, Map<number, number>>}
     */
    let needsCommitted = new Map();
    _.forEach(this.commitQueue, function (partitionOffset, topic) {
      const topicMap = new Map();
      needsCommitted.set(topic, topicMap);
      _.forEach(partitionOffset, function (offset, partition) {
        if (offset != null) {
          topicMap.set(partition, offset);
          commits.push({
            topic: topic,
            partition: partition,
            offset: offset,
            metadata: 'm'
          });
        }
      });
    });
    function filterCommits () {
      commits = commits.filter(msg => {
        const topicMap = needsCommitted.get(msg.topic);
        return topicMap && topicMap.has(msg.partition);
      });
      return commits;
    }

    const genericCallbacks = this.commitGenericCallbacks;
    const partitionCallbacks = this.commitPartitionCallbacks;
    // These are pending, don't let another request send them too
    this.commitQueue = {};
    this.commitGenericCallbacks = [];
    this.commitPartitionCallbacks = new Map();

    if (_.isEmpty(commits)) {
      // we wouldn't have partition callbacks here if there was nothing to commit
      genericCallbacks.forEach(cb => cb(null));
      logger.debug('commit ignored. no commits to make.');
      return;
    }

    this.commitPendingPromise = new Promise((resolve, reject) => {
      logger.debug('committing', commits);
      const finalize = () => {
        this.commitPendingPromise = null;

        genericCallbacks.forEach(cb => cb(null));
        // Any remaining partition callbacks left did not get included in the commit result. Throw an error to them.
        partitionCallbacks.forEach((callbacks, key) => {
          callbacks.forEach(cb => cb(new Error('topic ' + key + ' did not get returned in commit response')));
        });
        resolve();
        if (this.needsCommitWhenDone) {
          this.needsCommitWhenDone = false;
          this.commitQueued(true);
        }
      };
      let hasRetried = false;
      const processOffsetCommitResponse = (error, topics) => {
        let shouldRetry = false;
        const isTimeout = isTimeoutError(error);
        if (isTimeout && hasRetried && this.options.treatTimeoutsAsSuccess) {
          logger.warn('2 timeout errors in a row, treating as success due to user choice');
          genericCallbacks.forEach(cb => cb(null));
          partitionCallbacks.forEach(callbacks => callbacks.forEach(cb => cb(null)));
          finalize();
          return;
        } else if (isTimeout && !hasRetried) {
          shouldRetry = true;
        } else if (error) {
          // Nothing known triggers this unless something in the request stack might throw an error
          logger.error('commit request failed', error);
          this.emit('error', error);
          partitionCallbacks.forEach(callbacks => {
            callbacks.forEach(cb => cb(error));
          });
          genericCallbacks.forEach(cb => cb(error));
          finalize();
          return;
        } else {
          Object.keys(topics).forEach(topic => {
            const partitions = topics[topic];
            const neededCommitsTopic = needsCommitted.get(topic);
            Object.keys(partitions).forEach(/** @type number */partition => {
              const partitionError = partitions[partition];
              const key = topic + ':' + partition;
              const callbacks = partitionCallbacks.get(key);
              function executeCallbacks (err) {
                if (callbacks) {
                  callbacks.forEach(cb => cb(err));
                }
                partitionCallbacks.delete(key);
                neededCommitsTopic.delete(partition);
                if (!neededCommitsTopic.size) {
                  needsCommitted.delete(topic);
                }
              }
              if (callbacks && callbacks.length) {
                const isReplicationTimeout = isTimeoutError(partitionError);
                if (isReplicationTimeout && hasRetried && this.options.treatTimeoutsAsSuccess) {
                  logger.warn('2 timeout errors in a row, treating as success due to user choice');
                  executeCallbacks(null, partitionError);
                } else if (isReplicationTimeout && !hasRetried) {
                  // This is a dangerous scenario. The offset is likely actually committed, but we timed out....
                  // If we allow this error to bubble up, it is likely wrong. Let's retry and hope for success.
                  shouldRetry = true;
                } else {
                  executeCallbacks(partitionError);
                }
              } else if (partitionError) {
                logger.error('partition error with no callback receiving it', partitionError);
                this.emit('error', partitionError);
                executeCallbacks(partitionError);
              }
            });
          });
        }
        if (shouldRetry) {
          hasRetried = true;
          logger.warn('retrying offset commit as some partitions timed out');
          setTimeout(() => {
            // Fetch needed topics to discover, did it succeed?
            const payloads = [];
            needsCommitted.forEach((partitions, topic) => {
              partitions.forEach((neededOffset, partition) => {
                payloads.push({topic: topic, partition: partition, time: -1});
              });
            });
            this.consumerGroup.getOffset().fetch(payloads, (error, result) => {
              if (error) {
                // We don't want to treat this as a total failure if fetch fails, just retry the commit,
                // then if that fails, that will report the error
                logger.error('got error on retry for offset commit, retrying in hopes it will succeed');
                this.consumerGroup.sendOffsetCommitRequest(filterCommits(), processOffsetCommitResponse);
                return;
              }
              if (result) {
                needsCommitted.forEach((partitions, topic) => {
                  partitions.forEach((neededOffset, partition) => {
                    const offset = _.head(result[topic][partition]);
                    if (offset >= neededOffset) {
                      const key = topic + ':' + partition;
                      logger.debug('offset did successfully commit, executing callbacks for ' + topic + ':' + partition);
                      const callbacks = partitionCallbacks.get(key);
                      callbacks.forEach(cb => cb());
                      partitionCallbacks.delete(key);
                    }
                  });
                });
              }
              if (partitionCallbacks.size) {
                // We still have offsets to retry, we need to refilter as some of them may of been successfully processed above
                this.consumerGroup.sendOffsetCommitRequest(filterCommits(), processOffsetCommitResponse);
              } else {
                finalize();
              }
            });
          }, 5000);
          return;
        }
        finalize();
      };
      this.consumerGroup.sendOffsetCommitRequest(commits, processOffsetCommitResponse);
    }).catch(error => {
      logger.error('error in commit request', error);
      partitionCallbacks.forEach(callbacks => {
        callbacks.forEach(cb => cb(error));
      });
      genericCallbacks.forEach(cb => cb(error));
    });
  }

  transmitMessages () {
    while (this._reading && !this.messageBuffer.isEmpty()) {
      this._reading = this.push(this.messageBuffer.shift());
    }
    if (this.messageBuffer.isEmpty() && this._reading) {
      this.consumerGroup.resume();
    }
  }

  close (callback) {
    clearTimeout(this.autoCommitTimer);
    async.series(
      [
        // We don't want to check auto commit as anything in the commit queue was already told to be committed
        callback => {
          this.commit(null, true, (err) => {
            if (err) {
              logger.error('Error in committing during close', err);
            }
            callback(null);
          });
        },
        callback => {
          this.consumerGroup.close(false, () => {
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
