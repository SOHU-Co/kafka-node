'use strict';

var debug = require('debug')('kafka-node:ConsumerGroup');
var util = require('util');
var retry = require('retry');
var highLevelConsumer = require('./highLevelConsumer');
var Client = require('./client');
var _ = require('lodash');
var async = require('async');
var validateConfig = require('./utils').validateConfig;
var GroupCoordinatorNotAvailable = require('./errors/GroupCoordinatorNotAvailableError');
var NotCoordinatorForGroup = require('./errors/NotCoordinatorForGroupError');
var IllegalGeneration = require('./errors/IllegalGenerationError');
var GroupLoadInProgress = require('./errors/GroupLoadInProgressError');
var UnknownMemberId = require('./errors/UnknownMemberIdError');
var RebalanceInProgress = require('./errors/RebalanceInProgressError');
var assert = require('assert');
var roundRobin = require('./assignment/roundrobin');

var DEFAULTS = {
  groupId: 'kafka-node-group',
  // Auto commit config
  autoCommit: true,
  autoCommitIntervalMs: 5000,
  // Fetch message config
  fetchMaxWaitMs: 100,
  paused: false,
  maxNumSegments: 1000,
  fetchMinBytes: 1,
  fetchMaxBytes: 1024 * 1024,
  maxTickMessages: 1000,
  fromOffset: false,
  sessionTimeout: 10000,
  retries: 10,
  retryFactor: 1.8,
  connectOnReady: true,
  protocol: {
    name: 'roundrobin',
    version: 0
  }
};

function ConsumerGroup (memberOptions, topics) {
  var self = this;
  this.options = _.defaults((memberOptions || {}), DEFAULTS);
  this.client = new Client(memberOptions.host, memberOptions.id);
  this.topics = topics;
  this.options.protocol.subscription = topics || [];

  if (this.options.connectOnReady) {
    this.client.once('ready', this.connect.bind(this));
  }

  this.client.on('error', function (err) {
    debug('Error from %j', err);
    self.emit('error', err);
  });

  this.client.on('reconnect', function (lastError) {
    self.fetch();
  });

  // 'done' will be emit when a message fetch request complete
  this.on('done', function (topics) {
    self.updateOffsets(topics);
    if (!self.paused) {
      setImmediate(function () {
        self.fetch();
      });
    }
  });

  if (this.options.groupId) {
    validateConfig('options.groupId', this.options.groupId);
  }

  this.isLeader = false;
  this.coordinatorId = null;
  this.generationId = null;
  this.ready = false;
}

util.inherits(ConsumerGroup, highLevelConsumer);

ConsumerGroup.prototype.setCoordinatorId = function (coordinatorId) {
  this.client.coordinatorId = String(coordinatorId);
};

ConsumerGroup.prototype.assignPartitions = function (protocol, groupMembers, callback) {
  debug('Assigning Partitions to members', groupMembers);
  debug('Using group protocol', protocol);

  var self = this;
  var topics = _(groupMembers).pluck('subscription').flatten().uniq().value();

  async.waterfall([
    function (callback) {
      debug('loadingMetadata for topics:', topics);
      self.client.loadMetadataForTopics(topics, callback);
    },

    function (metadataResponse, callback) {
      var metadataResponse = metadataResponse[1].metadata;
      var metadata = mapTopicToPartitions(metadataResponse);
      debug('mapTopicToPartitions', metadata);
      callback(null, metadata);
    },

    function (metadata, callback) {
      roundRobin.assign(metadata, groupMembers, callback);
    }
  ], callback);
};

function mapTopicToPartitions (metadata) {
  // var metadata = metadataResponse[1].metadata;
  return _.mapValues(metadata, Object.keys);
}

ConsumerGroup.prototype.connect = function () {
  if (this.connecting) {
    debug('Connect ignored. Currently connecting.');
    return;
  }

  debug('Connecting %s', this.client.clientId);
  var self = this;

  this.connecting = true;

  async.waterfall([
    function (callback) {
      if (self.client.coordinatorId) {
        return callback(null, null);
      }
      self.client.sendGroupCoordinatorRequest(self.options.groupId, callback);
    },

    function (coordinatorInfo, callback) {
      debug('GroupCoordinator Response:', coordinatorInfo);
      if (coordinatorInfo) {
        self.setCoordinatorId(coordinatorInfo.coordinatorId);
      }
      self.client.sendJoinGroupRequest(self.options.groupId, self.memberId, self.options.sessionTimeout, [self.options.protocol], callback);
    },

    function (joinGroupResponse, callback) {
      debug('joinGroupResponse %j from %s', joinGroupResponse, self.client.clientId);

      self.isLeader = (joinGroupResponse.leaderId === joinGroupResponse.memberId);
      self.generationId = joinGroupResponse.generationId;
      self.memberId = joinGroupResponse.memberId;

      var groupAssignment;
      if (self.isLeader) {
        // assign partitions
        return self.assignPartitions(joinGroupResponse.groupProtocol, joinGroupResponse.members, callback);
      }
      callback(null, groupAssignment);
    },

    function (groupAssignment, callback) {
      debug('SyncGroup Request from %s', self.memberId);
      self.client.sendSyncGroupRequest(self.options.groupId, self.generationId, self.memberId, groupAssignment, callback);
    },

    function (syncGroupResponse, callback) {
      debug('SyncGroup Response');
      if (syncGroupResponse.partitions.length) {
        debug('%s owns', self.memberId, syncGroupResponse.partitions);

        self.fetchOffset(groupPartitionsByTopic(syncGroupResponse.partitions), function (error, offsets) {
          if (error) {
            return callback(error);
          }

          debug('%s fetchOffset Response: %j', self.client.clientId, offsets);

          self.topicPayloads = [];

          var payloads = self.buildPayloads(syncGroupResponse.partitions);

          payloads.forEach(function (p) {
            var offset = offsets[p.topic][p.partition];
            if (offset === -1) offset = 0;
            p.offset = offset;
            self.topicPayloads.push(p);
          });
          callback(null, true);
        });
      } else { // no partitions assigned
        callback(null, false);
      }
    }
  ], function (error, startFetch) {
    self.connecting = false;
    if (error) {
      return self.tryToRecoverFrom(error, 'connect');
    }

    self.ready = true;
    self.lastError = null;

    debug('generationId', self.generationId);

    if (startFetch) {
      self.fetch();
    }
    self.emit('connect');
    self.startHearbeats();
  });
};

/*
Converts:

  [
    {topic: 'test', partition: 0},
    {topic: 'test', partition: 1},
    {topic: 'Bob', partition: 0}
  ]

Into:

  {
    test: [0, 1],
    bob: [0]
  }

*/
function groupPartitionsByTopic (topicPartitions) {
  assert(Array.isArray(topicPartitions));

  debug('groupPartitionsByTopic', topicPartitions);

  return topicPartitions.reduce(function (result, tp) {
    if (!(tp.topic in result)) {
      result[tp.topic] = [tp.partition];
    } else {
      result[tp.topic].push(tp.partition);
    }
    return result;
  }, {});
}

var recoverableErrors = [
  {
    errors: [GroupCoordinatorNotAvailable, IllegalGeneration, GroupLoadInProgress, RebalanceInProgress]
  },
  {
    errors: [NotCoordinatorForGroup],
    handler: function () {
      this.client.coordinatorId = null;
    }
  },
  {
    errors: [UnknownMemberId],
    handler: function () {
      this.memberId = null;
    }
  }
];

function isErrorInstanceOf (error, errors) {
  return errors.some(function (errorClass) {
    return error instanceof errorClass;
  });
}

ConsumerGroup.prototype.tryToRecoverFrom = function (error, source) {
  this.ready = false;
  this.stopHeartbeats();

  var retryTimeout = false;
  var retry = recoverableErrors.some(function (recoverableItem) {
    if (isErrorInstanceOf(error, recoverableItem.errors)) {
      recoverableItem.handler && recoverableItem.handler.call(this, error);
      return true;
    }
    return false;
  }, this);

  if (retry) {
    retryTimeout = this.getRetryTimeout(error);
  }

  if (retry && retryTimeout) {
    debug('RECOVERY from %s: %s retrying', source, this.memberId || this.client.clientId, error);
    this.scheduleReconnect(retryTimeout);
  } else {
    this.emit('error', error);
  }
  this.lastError = error;
};

ConsumerGroup.prototype.getRetryTimeout = function (error) {
  assert(error);
  if (!this._timeouts) {
    this._timeouts = retry.timeouts({
      retries: this.options.retries,
      factor: this.options.retryFactor
    });
  }

  if (this._retryIndex == null || this.lastError == null ||
      error.errorCode !== this.lastError.errorCode) {
    this._retryIndex = 0;
  }

  var index = this._retryIndex++;
  if (index >= this._timeouts.length) {
    return false;
  }
  return this._timeouts[index];
};

ConsumerGroup.prototype.scheduleReconnect = function (timeout) {
  assert(timeout);
  var self = this;
  this.reconnectTimer = setTimeout(function () {
    self.connect();
  }, timeout);
};

ConsumerGroup.prototype.startHearbeats = function () {
  assert(this.options.sessionTimeout - 1200 > 0);
  assert(this.ready, 'consumerGroup is not ready');

  var self = this;
  this.sendHeartbeat();
  this.hearbeatInterval = setInterval(function () {
    self.sendHeartbeat();
  }, this.options.sessionTimeout - 1200);
  debug('%s started heartbeats', this.memberId);
};

ConsumerGroup.prototype.stopHeartbeats = function () {
  this.hearbeatInterval && clearInterval(this.hearbeatInterval);
};

ConsumerGroup.prototype.leaveGroup = function (callback) {
  this.stopHeartbeats();
  this.client.sendLeaveGroupRequest(this.options.groupId, this.memberId, callback);
};

ConsumerGroup.prototype.sendHeartbeat = function () {
  assert(this.memberId, 'invalid memberId');
  assert(this.generationId >= 0, 'invalid generationId');
  debug('%s ❤️', this.memberId);
  var self = this;
  this.client.sendHeartbeatRequest(this.options.groupId, this.generationId, this.memberId, function (error) {
    if (error) {
      debug('Heartbeat error:', error);
      self.tryToRecoverFrom(error, 'heartbeat');
    }
    debug('%s 💚', self.memberId, error);
  });
};

ConsumerGroup.prototype.fetchOffset = function (payloads, cb) {
  debug('in fetchOffset payloads: %j', payloads);
  this.client.sendOffsetFetchV1Request(this.options.groupId, payloads, cb);
};

ConsumerGroup.prototype.autoCommit = function (force, cb) {
  if (arguments.length === 1) {
    cb = force;
    force = false;
  }

  if (this.committing && !force) return cb(null, 'Offset committing');

  this.committing = true;
  setTimeout(function () {
    this.committing = false;
  }.bind(this), this.options.autoCommitIntervalMs);

  var commits = this.topicPayloads.filter(function (p) { return p.offset !== 0; });

  if (commits.length) {
    this.client.sendOffsetCommitV2Request(this.options.groupId, this.generationId,
      this.memberId, commits, cb);
  } else {
    cb(null, 'Nothing to be committed');
  }
};

ConsumerGroup.prototype.commit = ConsumerGroup.prototype.autoCommit;

ConsumerGroup.prototype.close = function (force, cb) {
  var self = this;
  this.ready = false;

  this.stopHeartbeats();

  if (typeof force === 'function') {
    cb = force;
    force = false;
  }

  async.series([
    function (callback) {
      if (force) {
        self.commit(true, callback);
        return;
      }
      callback(null);
    },
    function (callback) {
      self.leaveGroup(callback);
    },
    function (callback) {
      self.client.close(callback);
    },
  ], cb);
};

module.exports = ConsumerGroup;
