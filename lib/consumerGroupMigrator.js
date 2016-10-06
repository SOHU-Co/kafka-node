'use strict';

var assert = require('assert');
var util = require('util');
var async = require('async');
var debug = require('debug')('kafka-node:ConsumerGroupMigrator');
var zookeeper = require('node-zookeeper-client');
var _ = require('lodash');
var EventEmitter = require('events').EventEmitter;

function ConsumerGroupMigrator (consumerGroup) {
  EventEmitter.call(this);
  assert(consumerGroup);
  var self = this;
  this.consumerGroup = consumerGroup;
  this.client = consumerGroup.client;

  if (consumerGroup.options.migrateRolling) {
    this.zk = zookeeper.createClient(consumerGroup.client.connectionString);
    this.zk.on('connected', function () {
      self.filterByExistingZkTopics(function (error, topics) {
        if (error) {
          return self.emit('error', error);
        }

        if (topics.length) {
          self.checkForOwnersAndListenForChange(topics);
        } else {
          debug('No HLC topics exist in zookeeper.');
          self.connectConsumerGroup();
        }
      });
    });

    this.on('noOwnersForTopics', function () {
      debug('No owners for topics reported.');
      self.connectConsumerGroup();
    });

    this.on('topicOwnerChange', _.debounce(function (topics) {
      self.checkForOwnersAndListenForChange(topics);
    }, 250));

    this.zk.connect();
  } else {
    this.connectConsumerGroup();
  }
}

util.inherits(ConsumerGroupMigrator, EventEmitter);

ConsumerGroupMigrator.prototype.connectConsumerGroup = function () {
  debug('%s connecting consumer group', this.client.clientId);
  var self = this;
  if (this.client.ready) {
    this.consumerGroup.connect();
  } else {
    this.client.once('ready', function () {
      self.consumerGroup.connect();
    });
  }
  this.zk && this.zk.close();
};

ConsumerGroupMigrator.prototype.filterByExistingZkTopics = function (callback) {
  var self = this;
  var path = '/consumers/' + this.consumerGroup.options.groupId + '/owners/';

  async.filter(this.consumerGroup.topics, function (topic, cb) {
    var topicPath = path + topic;
    debug('%s checking zk path %s', self.client.clientId, topicPath);
    self.zk.exists(topicPath, function (error, stat) {
      if (error) {
        return callback(error);
      }
      cb(stat);
    });
  }, function (result) {
    callback(null, result);
  });
};

ConsumerGroupMigrator.prototype.checkForOwnersAndListenForChange = function (topics) {
  var self = this;
  var path = '/consumers/' + this.consumerGroup.options.groupId + '/owners/';
  var ownedPartitions = 0;

  debug('%s listening for changes in topics: ', this.client.clientId, topics);

  async.each(topics,
    function (topic, callback) {
      self.zk.getChildren(path + topic,
        function (event) {
          self.emit('topicOwnerChange', topics, topic);
        },
        function (error, children, stats) {
          if (error) {
            return callback(error);
          }
          ownedPartitions += children.length;
          callback(null);
        });
    },
    function (error) {
      if (error) {
        return self.emit('error', error);
      }
      if (ownedPartitions === 0) {
        self.emit('noOwnersForTopics');
      } else {
        debug('%s %d partitions are owned by old HLC... waiting...', self.client.clientId, ownedPartitions);
      }
    }
  );
};

ConsumerGroupMigrator.prototype.saveHighLevelConsumerOffsets = function (topicPartitionList, callback) {
  var self = this;
  this.client.sendOffsetFetchRequest(this.consumerGroup.options.groupId, topicPartitionList, function (error, results) {
    debug('sendOffsetFetchRequest response:', results, error);
    if (error) {
      return callback(error);
    }
    self.offsets = results;
    callback(null);
  });
};

ConsumerGroupMigrator.prototype.getOffset = function (tp, defaultOffset) {
  return _.get(this.offsets, [tp.topic, tp.partition], defaultOffset);
};

module.exports = ConsumerGroupMigrator;
