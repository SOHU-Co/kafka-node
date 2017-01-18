'use strict';

const stream = require('stream');
const Writable = stream.Writable;

const _ = require('lodash');

var DEFAULTS = {
  // Auto commit config
  autoCommit: true,
  autoCommitIntervalMs: 5000,
  autoCommitMsgCount: 100
};

class CommitStream extends Writable {

  constructor (client, topics, groupId, options) {
    options.objectMode = true;
    options.highWaterMark = 1;
    super(options);

    this.options = _.defaults((options || {}), DEFAULTS);
    this.client = client;
    this.topicPartionOffsets = this.buildTopicData(_.cloneDeep(topics));

    this.committing = false;
    this.groupId = groupId;

    this.autoCommit = options.autoCommit;
    this.autoCommitMsgCount = options.autoCommitMsgCount;
    this.autoCommitIntervalMs = options.autoCommitIntervalMs;

    this.autoCommitIntervalTimer = null;

    if (this.autoCommit && this.autoCommitIntervalMs) {
      this.autoCommitIntervalTimer = setInterval(function () {
        this.commit();
      }.bind(this), this.autoCommitIntervalMs);
    }

    this.messageCount = 0;
  }

  _write (data, encoding, done) {
    let topicUpdate = {};
    topicUpdate[data.topic] = {};
    topicUpdate[data.topic][data.partition] = data.offset;
    this.updateOffsets(topicUpdate);
    this.messageCount++;
    if (this.autoCommit && this.messageCount === this.autoCommitMsgCount) {
      this.messageCount = 0;
      return this.commit(done);
    }
    done();
  }

  buildTopicData (topicPartions) {
    return topicPartions.map(function (partion) {
      if (typeof partion !== 'object') partion = { topic: partion };
      partion.partition = partion.partition || 0;
      partion.offset = partion.offset || 0;
      // Metadata can be arbitrary
      partion.metadata = 'm';
      return partion;
    });
  }

  /**
   * @param {Object} topics - An object containing topic offset data keyed by topic
   *   with keys for partion containing the offset last seen.
   */
  updateOffsets (topics, initing) {
    this.topicPartionOffsets.forEach(function (p) {
      if (!_.isEmpty(topics[p.topic]) && topics[p.topic][p.partition] !== undefined) {
        var offset = topics[p.topic][p.partition];
        if (offset === -1) offset = 0;
        // Note, we track the offset of the next message we want to see,
        // not the most recent message we have seen.
        if (!initing) p.offset = offset + 1;
        else p.offset = offset;
      }
    });
  }

  /**
   * Clear the autocommit interval of this commitStream if set.
   */
  clearInterval () {
    clearInterval(this.autoCommitIntervalTimer);
  }

  commit (cb) {
    let self = this;

    if (!cb) {
      cb = function noop () {};
    }

    if (self.committing) {
      return cb(null, 'Commit in progress');
    }

    let topicPartionOffsets = self.topicPartionOffsets;

    let commits = topicPartionOffsets.filter(function (partition) { return partition.offset !== 0; });

    if (commits.length) {
      self.committing = true;
      self.client.sendOffsetCommitRequest(self.groupId, commits, function () {
        self.emit('commitComplete', { group: self.groupId, commits });
        self.committing = false;
        cb.apply(this, arguments);
      });
    } else {
      cb(null, 'Nothing to be committed');
    }
  }

}

module.exports = CommitStream;
