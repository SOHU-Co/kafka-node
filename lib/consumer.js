'use strict';

var util = require('util');
var _ = require('lodash');
var events = require('events');
var logger = require('./logging')('kafka-node:Consumer');
var utils = require('./utils');

var DEFAULTS = {
  groupId: 'kafka-node-group',
  // Auto commit config
  autoCommit: true,
  autoCommitIntervalMs: 5000,
  // Fetch message config
  fetchMaxWaitMs: 100,
  fetchMinBytes: 1,
  fetchMaxBytes: 1024 * 1024,
  fromOffset: false,
  encoding: 'utf8'
};

var nextId = (function () {
  var id = 0;
  return function () {
    return id++;
  };
})();

var Consumer = function (client, topics, options) {
  if (!topics) {
    throw new Error('Must have payloads');
  }

  utils.validateTopics(topics);

  this.fetchCount = 0;
  this.client = client;
  this.options = _.defaults((options || {}), DEFAULTS);
  this.ready = false;
  this.paused = this.options.paused;
  this.id = nextId();
  this.payloads = this.buildPayloads(topics);
  this.connect();
  this.encoding = this.options.encoding;

  if (this.options.groupId) {
    utils.validateConfig('options.groupId', this.options.groupId);
  }
};
util.inherits(Consumer, events.EventEmitter);

Consumer.prototype.buildPayloads = function (payloads) {
  var self = this;
  return payloads.map(function (p) {
    if (typeof p !== 'object') p = { topic: p };
    p.partition = p.partition || 0;
    p.offset = p.offset || 0;
    p.maxBytes = self.options.fetchMaxBytes;
    p.metadata = 'm'; // metadata can be arbitrary
    return p;
  });
};

Consumer.prototype.connect = function () {
  var self = this;
  // Client already exists
  this.ready = this.client.ready;
  if (this.ready) this.init();

  this.client.on('ready', function () {
    logger.debug('consumer ready');
    if (!self.ready) self.init();
    self.ready = true;
  });

  this.client.on('error', function (err) {
    logger.error('client error %s', err.message);
    self.emit('error', err);
  });

  this.client.on('close', function () {
    logger.debug('connection closed');
  });

  this.client.on('brokersChanged', function () {
    var topicNames = self.payloads.map(function (p) {
      return p.topic;
    });

    this.refreshMetadata(topicNames, function (err) {
      if (err) return self.emit('error', err);
      self.fetch();
    });
  });
  // 'done' will be emit when a message fetch request complete
  this.on('done', function (topics) {
    self.updateOffsets(topics);
    setImmediate(function () {
      self.fetch();
    });
  });
};

Consumer.prototype.init = function () {
  if (!this.payloads.length) {
    return;
  }

  var self = this;
  var topics = self.payloads.map(function (p) { return p.topic; });

  self.client.topicExists(topics, function (err) {
    if (err) {
      return self.emit('error', err);
    }

    if (self.options.fromOffset) {
      return self.fetch();
    }

    self.fetchOffset(self.payloads, function (err, topics) {
      if (err) {
        return self.emit('error', err);
      }

      self.updateOffsets(topics, true);
      self.fetch();
    });
  });
};

/*
 * Update offset info in current payloads
 * @param {Object} Topic-partition-offset
 * @param {Boolean} Don't commit when initing consumer
 */
Consumer.prototype.updateOffsets = function (topics, initing) {
  this.payloads.forEach(function (p) {
    if (!_.isEmpty(topics[p.topic]) && topics[p.topic][p.partition] !== undefined) {
      var offset = topics[p.topic][p.partition];
      if (offset === -1) offset = 0;
      if (!initing) p.offset = offset + 1;
      else p.offset = offset;
    }
  });

  if (this.options.autoCommit && !initing) {
    this.autoCommit(false, function (err) {
      err && logger.debug('auto commit offset', err);
    });
  }
};

function autoCommit (force, cb) {
  if (arguments.length === 1) {
    cb = force;
    force = false;
  }

  if (this.committing && !force) return cb(null, 'Offset committing');

  this.committing = true;
  setTimeout(function () {
    this.committing = false;
  }.bind(this), this.options.autoCommitIntervalMs);

  var payloads = this.payloads;
  if (this.pausedPayloads) payloads = payloads.concat(this.pausedPayloads);

  var commits = payloads.filter(function (p) { return p.offset !== 0; });
  if (commits.length) {
    this.client.sendOffsetCommitRequest(this.options.groupId, commits, cb);
  } else {
    cb(null, 'Nothing to be committed');
  }
}
Consumer.prototype.commit = Consumer.prototype.autoCommit = autoCommit;

Consumer.prototype.fetch = function () {
  if (!this.ready || this.paused) return;
  this.client.sendFetchRequest(this, this.payloads, this.options.fetchMaxWaitMs, this.options.fetchMinBytes);
};

Consumer.prototype.fetchOffset = function (payloads, cb) {
  this.client.sendOffsetFetchRequest(this.options.groupId, payloads, cb);
};

Consumer.prototype.addTopics = function (topics, cb, fromOffset) {
  fromOffset = !!fromOffset;
  var self = this;
  if (!this.ready) {
    setTimeout(function () {
      self.addTopics(topics, cb, fromOffset);
    }
    , 100);
    return;
  }

  // The default is that the topics is a string array of topic names
  var topicNames = topics;

  // If the topics is actually an object and not string we assume it is an array of payloads
  if (typeof topics[0] === 'object') {
    topicNames = topics.map(function (p) { return p.topic; });
  }

  this.client.addTopics(
    topicNames,
    function (err, added) {
      if (err) return cb && cb(err, added);

      var payloads = self.buildPayloads(topics);
      var reFetch = !self.payloads.length;

      if (fromOffset) {
        payloads.forEach(function (p) {
          self.payloads.push(p);
        });
        if (reFetch) self.fetch();
        cb && cb(null, added);
        return;
      }

      // update offset of topics that will be added
      self.fetchOffset(payloads, function (err, offsets) {
        if (err) return cb(err);
        payloads.forEach(function (p) {
          var offset = offsets[p.topic][p.partition];
          if (offset === -1) offset = 0;
          p.offset = offset;
          self.payloads.push(p);
        });
        if (reFetch) self.fetch();
        cb && cb(null, added);
      });
    }
  );
};

Consumer.prototype.removeTopics = function (topics, cb) {
  topics = typeof topics === 'string' ? [topics] : topics;
  this.payloads = this.payloads.filter(function (p) {
    return !~topics.indexOf(p.topic);
  });

  this.client.removeTopicMetadata(topics, cb);
};

Consumer.prototype.close = function (force, cb) {
  this.ready = false;
  if (typeof force === 'function') {
    cb = force;
    force = false;
  }

  if (force) {
    this.commit(force, function (err) {
      if (err) {
        return cb(err);
      }
      this.client.close(cb);
    }.bind(this));
  } else {
    this.client.close(cb);
  }
};

Consumer.prototype.setOffset = function (topic, partition, offset) {
  this.payloads.every(function (p) {
    if (p.topic === topic && p.partition == partition) { // eslint-disable-line eqeqeq
      p.offset = offset;
      return false;
    }
    return true;
  });
};

Consumer.prototype.pause = function () {
  this.paused = true;
};

Consumer.prototype.resume = function () {
  this.paused = false;
  this.fetch();
};

Consumer.prototype.pauseTopics = function (topics) {
  if (!this.pausedPayloads) this.pausedPayloads = [];
  pauseOrResume(this.payloads, this.pausedPayloads, topics);
};

Consumer.prototype.resumeTopics = function (topics) {
  if (!this.pausedPayloads) this.pausedPayloads = [];
  var reFetch = !this.payloads.length;
  pauseOrResume(this.pausedPayloads, this.payloads, topics);
  reFetch = reFetch && this.payloads.length;
  if (reFetch) this.fetch();
};

function pauseOrResume (payloads, nextPayloads, topics) {
  if (!topics || !topics.length) return;

  for (var i = 0, j = 0, l = payloads.length; j < l; i++, j++) {
    if (isInTopics(payloads[i])) {
      nextPayloads.push(
        payloads.splice(i, 1)[0]
      );
      i--;
    }
  }

  function isInTopics (p) {
    return topics.some(function (topic) {
      if (typeof topic === 'string') {
        return p.topic === topic;
      } else {
        return p.topic === topic.topic && p.partition === topic.partition;
      }
    });
  }
}

module.exports = Consumer;
