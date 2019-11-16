'use strict';

const EventEmitter = require('events');
const retry = require('retry');
const debug = require('debug')('kafka-node:asyncConsumerGroup:CommitManager');

class CommitManager extends EventEmitter {
  constructor (group, options) {
    super();
    this.group = group;
    this.options = options;
    this.offsets = new Map();
    this.callbacks = [];
    this.uncommitted = 0;
    this.idle = true;
  }

  ack (message, cb) {
    let partitionOffsets = this.offsets.get(message.topic);
    if (!partitionOffsets) {
      partitionOffsets = new Map();
      this.offsets.set(message.topic, partitionOffsets);
    }

    partitionOffsets.set(message.partition, message.offset + 1);
    if (cb) {
      this.callbacks.push(cb);
    }

    this.uncommitted++;

    //
    // TODO need to support autoCommit-style intervals in addition to the more aggressive ConsumerGroupStream style too.
    //
    if (this.idle) {
      this._commitPending();
    }
  }

  acquiesce () {
    return new Promise(resolve => {
      if (this.idle) {
        return resolve();
      }
      this.once('internal:idle', () => resolve());
    });
  }

  purge () {
    if (!this.idle) {
      throw new Error('attempt to purge abandoned topic/partitions while consumer is committing');
    }

    this.offsets = new Map();
    const reason = new Error('commit offsets purged');
    for (const callback of this.callbacks) {
      try {
        callback(reason);
      } catch (err) {
        this.emit('error', err);
      }
    }
    this.callbacks = [];
  }

  _commitPending () {
    if (this.uncommitted === 0) {
      debug('became idle');

      this.idle = true;
      this.emit('internal:idle');
      return;
    }

    this.idle = false;

    //
    // if we don't retry "forever" we risk breaking the consumer, so override the user's preference here.
    //
    const overrides = {forever: true};
    const retryOptions = Object.assign({}, this.options.commitRetryOptions, overrides);
    const r = retry.operation(retryOptions);
    r.attempt(() => this._attemptCommit(r));
  }

  _attemptCommit (r) {
    //
    // Take a snapshot of commits + callbacks before running the commit so we can discern which callbacks arrived while
    // we were still committing.
    //
    const commits = this._buildCommits();
    const callbacks = this.callbacks.slice();
    const count = this.uncommitted;
    debug('attempting offset commit for %d messages:', count, commits);
    this.group.sendOffsetCommitRequest(commits, err => {
      if (err) {
        debug('commit attempt failed, retrying', err);
        this.emit('internal:commit-retry', err);
        r.retry(err);
        return;
      }

      debug('commit successful, processing callbacks');
      for (const callback of callbacks) {
        try {
          callback();
        } catch (err) {
          this.emit('error', err);
        }
      }
      this.uncommitted -= count;
      this.callbacks = this.callbacks.slice(callbacks.length);

      this.emit('internal:commit-complete', {messagesCommitted: count});

      //
      // New commits may have arrived while we were committing.
      //
      setImmediate(() => this._commitPending());
    });
  }

  _buildCommits () {
    const commits = [];
    for (const [topic, partitionCommits] of this.offsets.entries()) {
      for (const [partition, offset] of partitionCommits.entries()) {
        const metadata = 'm';
        commits.push({topic, partition, offset, metadata});
      }
    }
    return commits;
  }
}

module.exports = CommitManager;
