'use strict';

const assert = require('assert');
const debug = require('debug')('kafka-node:ConsumerGroupMigrator');
const zookeeper = require('node-zookeeper-client');
const _ = require('lodash');

function ConsumerGroupMigrator (consumerGroup) {
  assert(consumerGroup);
  this.consumerGroup = consumerGroup;
  this.client = consumerGroup.client;
  this.client.once('ready', this.consumerGroup.connect.bind(this.consumerGroup));
  this.savedOffsets = false;

  if (consumerGroup.options.migrateRolling) {
    this.zk = zookeeper.createClient(consumerGroup.client.connectionString);
  }
}

ConsumerGroupMigrator.prototype.saveHighLevelConsumerOffsets = function (topicPartitionList, callback) {
  if (this.savedOffsets) {
    return callback(null);
  }

  var self = this;
  this.client.sendOffsetFetchRequest(this.consumerGroup.options.groupId, topicPartitionList, function (error, results) {
    debug('sendOffsetFetchRequest response:', results, error);
    if (error) {
      return callback(error);
    }
    self.offsets = results;
    self.savedOffsets = true;
    callback(null);
  });
};

ConsumerGroupMigrator.prototype.getOffset = function (tp, defaultOffset) {
  return _.get(this.offsets, [tp.topic, tp.partition], defaultOffset);
};

module.exports = ConsumerGroupMigrator;
