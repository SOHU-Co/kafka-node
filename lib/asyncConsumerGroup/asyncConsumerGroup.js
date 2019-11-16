'use strict';

const EventEmitter = require('events');
const ConsumerGroup = require('../consumerGroup');
const PartitionProcessor = require('./partitionProcessor');
const CommitManager = require('./commitManager');
const debug = require('debug')('kafka-node:asyncConsumerGroup:AsyncConsumerGroup');

const DEFAULT_OPTIONS = {
  maxUncommittedMessages: 1,
  prefetchHighWatermark: 10000,
  prefetchLowWatermark: 1000,
  commitRetryOptions: {
    minTimeout: 100,
    maxTimeout: 5000,
    factor: 2.5,
    randomize: true
  },
  processRetryOptions: {
    minTimeout: 100,
    maxTimeout: 5000,
    factory: 2.5,
    retries: 5,
    randomize: true
  }
};

//
// TODO global and per-partition processing stats, commit stats, etc.
//
class AsyncConsumerGroup extends EventEmitter {
  constructor (options, topics) {
    super();

    if (!options) {
      options = {};
    }

    //
    // TODO if user-provided onRebalance is called, we should invoke it (or perhaps we could offer an alternative API)
    //
    const overrides = {
      paused: true,
      autoCommit: false,
      onRebalance: (isMember, cb) => this._onGroupRebalanceBegin(isMember, cb)
    };

    this.options = Object.assign({}, DEFAULT_OPTIONS, options, overrides);
    this.closing = false;
    this.closed = false;
    this.buffered = 0;
    this.idle = true;
    this.consuming = false;

    this.group = new InternalConsumerGroup(this.options, topics);
    this.group.on('error', err => this._onInternalError(err));

    this.commitManager = new CommitManager(this.group, this.options);
    this.commitManager.on('error', err => this._onInternalError(err));

    this.partitionProcessors = new Map();
  }

  consume (cb) {
    if (this.consuming) {
      throw new Error('consume() should only be called once');
    }

    this.consuming = true;

    this.commitManager.on('internal:commit-complete', info => this._onCommitComplete(info));
    this.commitManager.on('internal:commit-retry', err => this._onCommitRetry(err));
    this.group.on('rebalanced', () => this._onGroupRebalanceEnd());
    this.group.on('message', msg => this._onGroupMessage(msg, cb));
    this.group.on('done', () => this._onGroupBatchDone());
    this.group.resume();
  }

  close () {
    if (this.closing) {
      return Promise.reject(new Error('already closing/closed'));
    }

    debug('closing');

    this.closing = true;

    return new Promise((resolve, reject) => {
      this.group.pause();

      this._acquiesce()
        .then(() => this._closeGroup())
        .then(() => {
          debug('closed');
          resolve();
        })
        .catch(err => {
          reject(err);
        });
    });
  }

  _closeGroup () {
    return new Promise((resolve, reject) => {
      this.group.close(err => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  _acquiesce () {
    debug('pausing fetch requests while we acquiesce');
    this.group.pause();

    return this._acquiescePartitionProcessors()
      .then(() => this._acquiesceCommitManager());
  }

  _acquiescePartitionProcessors () {
    debug('waiting for partition processors to acquiesce');

    const promises = [];
    for (const partitionProcessor of this.partitionProcessors.values()) {
      promises.push(partitionProcessor.acquiesce());
    }
    return Promise.all(promises)
      .then(() => {
        //
        // once all partition processors have acquiesced, purge the existing map.
        // (no way to resume & we're about to rebalance or close anyway)
        //
        debug('partition processors have acquiesced');
        this.partitionProcessors = new Map();
      });
  }

  _acquiesceCommitManager () {
    debug('waiting for commit manager to acquiesce');

    return this.commitManager.acquiesce()
      .then(() => {
        debug('commit manager has acquiesced, purging cached offsets');

        this.commitManager.purge();
      });
  }

  _onGroupRebalanceBegin (isMember, cb) {
    debug('begin rebalance');

    //
    // FIXME presumably we have a limited amount of time to acquiesce.
    //
    const acquiesceThenCommitAndRebalance = () => {
      this._acquiesce()
        .then(() => cb())
        .catch(err => cb(err));
    };

    if (this.idle) {
      acquiesceThenCommitAndRebalance();
    } else {
      this.once('internal:idle', () => acquiesceThenCommitAndRebalance());
    }
  }

  _onGroupRebalanceEnd () {
    debug('end rebalance');

    if (!this.closing && this.group.paused && this.buffered < this.options.prefetchHighWatermark) {
      debug('resuming fetch after rebalance');
      this.group.resume();
    }
  }

  _onCommitComplete (info) {
    this.buffered -= info.messagesCommitted;
    if (!this.closing && this.group.paused && this.buffered <= this.options.prefetchLowWatermark) {
      debug('resuming fetch because message backlog is below prefetchLowWatermark');
      this.group.resume();
    }
  }

  _onCommitRetry (err) {
    //
    // folks can listen for this event if they want insight into failed commits
    //
    this.emit('commit-retry', err);
  }

  _onInternalError (err) {
    this.emit('error', err);
  }

  _onGroupMessage (msg, cb) {
    if (this.idle) {
      this.idle = false;
    }

    const topicPartition = `${msg.topic}/${msg.partition}`;
    let partitionProcessor = this.partitionProcessors.get(topicPartition);
    if (partitionProcessor == null) {
      partitionProcessor = new PartitionProcessor(msg.topic, msg.partition, cb, this.commitManager, this.options);
      partitionProcessor.on('error', err => this._onInternalError(err));
      this.partitionProcessors.set(topicPartition, partitionProcessor);
    }

    if (partitionProcessor.push(msg)) {
      this.buffered++;
      if (!this.group.paused && this.buffered >= this.options.prefetchHighWatermark) {
        debug('pausing fetch because message backlog exceeds prefetchHighWatermark');
        this.group.pause();
      }
    }
  }

  _onGroupBatchDone () {
    if (!this.idle) {
      this.idle = true;
      this.emit('internal:idle');
    }
  }
}

//
// We extend ConsumerGroup only to neuter the commit/autoCommit methods & to capture assigned partitions.
//
// FIXME we directly access assignedPartitions elsewhere, but would be cleaner to add an isAssignedPartition method.
//
class InternalConsumerGroup extends ConsumerGroup {
  constructor (options, topics) {
    super(options, topics);
    this.assignedPartitions = new Set();
  }

  commit (force, cb) {
    if (typeof force === 'function') {
      cb = force;
      force = false;
    }
    setImmediate(cb);
  }

  autoCommit (force, cb) {
    this.commit(force, cb);
  }

  handleSyncGroup (syncGroupResponse, callback) {
    debug('handling sync group');
    return super.handleSyncGroup(syncGroupResponse, (err, startFetch) => {
      this.assignedPartitions = new Set();
      if (!err) {
        for (const [topic, partitions] of Object.entries(syncGroupResponse.partitions)) {
          for (const partition of partitions) {
            this.assignedPartitions.add(`${topic}/${partition}`);
          }
        }
      }
      debug('consumer was assigned partitions', Array.from(this.assignedPartitions));

      callback(err, startFetch);
    });
  }
}

module.exports = AsyncConsumerGroup;
