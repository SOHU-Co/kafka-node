'use strict';

var debug = require('debug')('kafka-node:ConsumerGroup');
var util = require('util');
var highLevelConsumer = require('./highLevelConsumer');
var Client = require('./client');
var _ = require('lodash');
var async = require('async');
var validateConfig = require('./utils').validateConfig;
var ConsumerGroupRecovery = require('./ConsumerGroupRecovery');
var createTopicPartitionList = require('./utils').createTopicPartitionList;

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

  this.recovery = new ConsumerGroupRecovery(this);

  if (this.options.connectOnReady) {
    this.client.once('ready', this.connect.bind(this));
  }

  this.client.on('error', function (err) {
    debug('Error from %s', self.client.clientId, err);
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
      var metadata = mapTopicToPartitions(metadataResponse[1].metadata);
      debug('mapTopicToPartitions', metadata);
      callback(null, metadata);
    },

    function (metadata, callback) {
      roundRobin.assign(metadata, groupMembers, callback);
    }
  ], callback);
};

function mapTopicToPartitions (metadata) {
  return _.mapValues(metadata, Object.keys);
}

ConsumerGroup.prototype.handleJoinGroup = function (joinGroupResponse, callback) {
  debug('joinGroupResponse %j from %s', joinGroupResponse, this.client.clientId);

  this.isLeader = (joinGroupResponse.leaderId === joinGroupResponse.memberId);
  this.generationId = joinGroupResponse.generationId;
  this.memberId = joinGroupResponse.memberId;

  var groupAssignment;
  if (this.isLeader) {
    // assign partitions
    return this.assignPartitions(joinGroupResponse.groupProtocol, joinGroupResponse.members, callback);
  }
  callback(null, groupAssignment);
};

ConsumerGroup.prototype.handleSyncGroup = function (syncGroupResponse, callback) {
  debug('SyncGroup Response');
  var self = this;
  var ownedTopics = Object.keys(syncGroupResponse.partitions);
  if (ownedTopics.length) {
    debug('%s owns topics: ', self.client.clientId, syncGroupResponse.partitions);

    self.fetchOffset(syncGroupResponse.partitions, function (error, offsets) {
      if (error) {
        return callback(error);
      }

      debug('%s fetchOffset Response: %j', self.client.clientId, offsets);

      self.topicPayloads = [];

      var payloads = self.buildPayloads(createTopicPartitionList(syncGroupResponse.partitions));
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
};

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
      self.handleJoinGroup(joinGroupResponse, callback);
    },

    function (groupAssignment, callback) {
      debug('SyncGroup Request from %s', self.memberId);
      self.client.sendSyncGroupRequest(self.options.groupId, self.generationId, self.memberId, groupAssignment, callback);
    },

    function (syncGroupResponse, callback) {
      self.handleSyncGroup(syncGroupResponse, callback);
    }
  ], function (error, startFetch) {
    self.connecting = false;
    if (error) {
      return self.recovery.tryToRecoverFrom(error, 'connect');
    }

    self.ready = true;
    self.recovery.clearError();

    debug('generationId', self.generationId);

    if (startFetch) {
      self.fetch();
    }
    self.emit('connect');
    self.startHearbeats();
  });
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

  debug('%s started heartbeats', this.client.clientId);
  var self = this;
  this.sendHeartbeat();
  this.hearbeatInterval = setInterval(function () {
    self.sendHeartbeat();
  }, this.options.sessionTimeout - 1200);
};

ConsumerGroup.prototype.stopHeartbeats = function () {
  this.hearbeatInterval && clearInterval(this.hearbeatInterval);
};

ConsumerGroup.prototype.leaveGroup = function (callback) {
  debug('%s leaving group', this.client.clientId);
  var self = this;
  this.stopHeartbeats();
  if (self.generationId != null) {
    this.client.sendLeaveGroupRequest(this.options.groupId, this.memberId, function (error) {
      self.generationId = null;
      callback(error);
    });
  } else {
    callback(null);
  }
};

ConsumerGroup.prototype.sendHeartbeat = function () {
  assert(this.memberId, 'invalid memberId');
  assert(this.generationId >= 0, 'invalid generationId');
  debug('%s ❤️  ->', this.client.clientId);
  var self = this;
  this.client.sendHeartbeatRequest(this.options.groupId, this.generationId, this.memberId, function (error) {
    if (error) {
      debug('Heartbeat error:', error);
      self.recovery.tryToRecoverFrom(error, 'heartbeat');
    }
    debug('%s 💚 <-', self.client.clientId, error);
  });
};

ConsumerGroup.prototype.fetchOffset = function (payloads, cb) {
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

  if (commits.length && this.generationId && this.memberId) {
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
    }
  ], cb);
};

module.exports = ConsumerGroup;
