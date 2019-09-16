'use strict';

const stream = require('stream');
const Transform = stream.Transform;

const _ = require('lodash');

var DEFAULTS = {
  // Auto commit config
  autoCommit: true,
  autoCommitIntervalMs: 5000,
  autoCommitMsgCount: 100,
  // Whether to act as a transform stream and emit the events we observe.
  // If we write all data this stream will fill its buffer and then provide
  // backpressure preventing our continued reading.
  passthrough: false
};

class CommitStream extends Transform {
  constructor (client, topics, groupId, options) {
    options = options || {};
    let parentOptions = _.defaults({ highWaterMark: options.highWaterMark }, { objectMode: true });
    super(parentOptions);

    this.options = _.defaults(options || {}, DEFAULTS);
    this.client = client;
    this.topicPartionOffsets = this.buildTopicData(_.cloneDeep(topics));

    this.committing = false;
    this.groupId = groupId;

    this.autoCommit = options.autoCommit;
    this.autoCommitMsgCount = options.autoCommitMsgCount;
    this.autoCommitIntervalMs = options.autoCommitIntervalMs;

    this.autoCommitIntervalTimer = null;

    if (this.autoCommit && this.autoCommitIntervalMs) {
      this.autoCommitIntervalTimer = setInterval(
        function () {
          this.commit();
        }.bind(this),
        this.autoCommitIntervalMs
      );
    }

    this.messageCount = 0;
  }

  /**
   * Extend Transform::on() to act as a pipe if someone consumes data from us.
   */
  on (eventName) {
    if (eventName === 'data') {
      this.options.passthrough = true;
    }
    super.on.apply(this, arguments);
  }

  /**
   * Extend Transform::pipe() to act as a pipe if someone consumes data from us.
   */
  pipe () {
    this.options.passthrough = true;
    super.pipe.apply(this, arguments);
  }

  _transform (data, encoding, done) {
    let topicUpdate = {};
    let self = this;
    topicUpdate[data.topic] = {};
    topicUpdate[data.topic][data.partition] = data.offset;
    self.updateOffsets(topicUpdate);
    self.messageCount++;
    const doneWrapper = function () {
      // We need to act as a through stream if we are not
      // purely a terminal write stream.
      if (self.options.passthrough) {
        return done(null, data);
      }
      done();
    };
    if (self.autoCommit && self.messageCount === self.autoCommitMsgCount) {
      self.messageCount = 0;
      return self.commit(doneWrapper);
    }
    doneWrapper();
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
   * @param {Object} topics - An object containing topic offset data keyed by
   *   topic with keys for partion containing the offset last seen.
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

    let commits = topicPartionOffsets.filter(function (partition) {
      return partition.offset !== 0;
    });

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
