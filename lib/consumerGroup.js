'use strict';

var debug = require('debug')('kafka-node:ConsumerGroup');
var util = require('util');
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

  this.client.on('ready', this.connect.bind(this));
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
      var metadata = mapTopicToPartitions(metadataResponse);
      debug('mapTopicToPartitions', metadata);
      callback(null, metadata);
    },

    function (metadata, callback) {
      roundRobin.assign(metadata, groupMembers, callback);
    }
  ], callback);
};

function mapTopicToPartitions (metadataResponse) {
  var metadata = metadataResponse[1].metadata;
  return _.mapValues(metadata, Object.keys);
}

ConsumerGroup.prototype.connect = function () {
  debug('connecting');
  var self = this;

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
      debug('SyncGroup Response %s assignment: %j', self.memberId, syncGroupResponse);
      if (syncGroupResponse.partitions.length) {
        self.fetchOffset(syncGroupResponse.partitions, function (error, offsets) {
          if (error) {
            return callback(error);
          }

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
    if (error) {
      return self.tryToRecoverFrom(error);
    }

    self.ready = true;

    if (startFetch) {
      self.fetch();
    }
    self.emit('connect');
    self.startHearbeats();
  });
};

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

ConsumerGroup.prototype.tryToRecoverFrom = function (error) {
  this.ready = false;
  this.stopHeartbeats();

  var retry = recoverableErrors.some(function (recoverableItem) {
    if (isErrorInstanceOf(error, recoverableItem.errors)) {
      recoverableItem.handler && recoverableItem.handler.call(this, error);
      return true;
    }
    return false;
  }, this);

  if (retry) {
    debug('RECOVERY: %s retrying', this.memberId, error);
    this.scheduleReconnect();
  } else {
    this.emit('error', error);
  }
};

ConsumerGroup.prototype.scheduleReconnect = function () {
  var self = this;
  this.reconnectTimer = setTimeout(function () {
    self.connect();
  }, 1000);
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

ConsumerGroup.prototype.sendHeartbeat = function () {
  assert(this.memberId, 'invalid memberId');
  assert(this.generationId >= 0, 'invalid generationId');
  debug('%s ❤️', this.memberId);
  var self = this;
  this.client.sendHeartbeatRequest(this.options.groupId, this.generationId, this.memberId, function (error) {
    if (error) {
      debug('Heartbeat error:', error);
      self.tryToRecoverFrom(error);
    }
    debug('%s 💚', self.memberId);
  });
};

ConsumerGroup.prototype.fetchOffset = function (payloads, cb) {
  debug('in fetchOffset payloads: %j', payloads);
  this.client.sendOffsetFetchV1Request(this.options.groupId, payloads, cb);
};

module.exports = ConsumerGroup;
