'use strict';

const assert = require('assert');
const util = require('util');
const async = require('async');
const logger = require('./logging')('kafka-node:ConsumerGroupMigrator');
const zookeeper = require('node-zookeeper-client');
const _ = require('lodash');
const EventEmitter = require('events').EventEmitter;
const NUMER_OF_TIMES_TO_VERIFY = 4;
const VERIFY_WAIT_TIME_MS = 1500;

function ConsumerGroupMigrator (consumerGroup) {
  EventEmitter.call(this);
  assert(consumerGroup);
  const self = this;
  this.consumerGroup = consumerGroup;
  this.client = consumerGroup.client;
  var verified = 0;

  if (consumerGroup.options.migrateRolling) {
    this.zk = zookeeper.createClient(consumerGroup.client.connectionString, { retries: 10 });
    this.zk.on('connected', function () {
      self.filterByExistingZkTopics(function (error, topics) {
        if (error) {
          return self.emit('error', error);
        }

        if (topics.length) {
          self.checkForOwnersAndListenForChange(topics);
        } else {
          logger.debug('No HLC topics exist in zookeeper.');
          self.connectConsumerGroup();
        }
      });
    });

    this.on('noOwnersForTopics', function (topics) {
      logger.debug('No owners for topics %s reported.', topics);
      if (++verified <= NUMER_OF_TIMES_TO_VERIFY) {
        logger.debug(
          '%s verify %d of %d HLC has given up ownership by checking again in %d',
          self.client.clientId,
          verified,
          NUMER_OF_TIMES_TO_VERIFY,
          VERIFY_WAIT_TIME_MS
        );

        setTimeout(function () {
          self.checkForOwners(topics);
        }, VERIFY_WAIT_TIME_MS);
      } else {
        self.connectConsumerGroup();
      }
    });

    this.on(
      'topicOwnerChange',
      _.debounce(function (topics) {
        verified = 0;
        self.checkForOwnersAndListenForChange(topics);
      }, 250)
    );

    this.zk.connect();
  } else {
    this.connectConsumerGroup();
  }
}

util.inherits(ConsumerGroupMigrator, EventEmitter);

ConsumerGroupMigrator.prototype.connectConsumerGroup = function () {
  logger.debug('%s connecting consumer group', this.client.clientId);
  const self = this;
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
  const self = this;
  const path = '/consumers/' + this.consumerGroup.options.groupId + '/owners/';

  async.filter(
    this.consumerGroup.topics,
    function (topic, cb) {
      const topicPath = path + topic;
      logger.debug('%s checking zk path %s', self.client.clientId, topicPath);
      self.zk.exists(topicPath, function (error, stat) {
        if (error) {
          return callback(error);
        }
        cb(stat);
      });
    },
    callback
  );
};

ConsumerGroupMigrator.prototype.checkForOwnersAndListenForChange = function (topics) {
  this.checkForOwners(topics, true);
};

ConsumerGroupMigrator.prototype.checkForOwners = function (topics, listenForChange) {
  const self = this;
  const path = '/consumers/' + this.consumerGroup.options.groupId + '/owners/';
  var ownedPartitions = 0;

  function topicWatcher (event) {
    self.emit('topicOwnerChange', topics);
  }

  async.each(
    topics,
    function (topic, callback) {
      const args = [path + topic];

      if (listenForChange) {
        logger.debug('%s listening for changes in topic %s', self.client.clientId, topic);
        args.push(topicWatcher);
      }

      args.push(function (error, children, stats) {
        if (error) {
          return callback(error);
        }
        ownedPartitions += children.length;
        callback(null);
      });

      self.zk.getChildren.apply(self.zk, args);
    },
    function (error) {
      if (error) {
        return self.emit('error', error);
      }
      if (ownedPartitions === 0) {
        self.emit('noOwnersForTopics', topics);
      } else {
        logger.debug('%s %d partitions are owned by old HLC... waiting...', self.client.clientId, ownedPartitions);
      }
    }
  );
};

ConsumerGroupMigrator.prototype.saveHighLevelConsumerOffsets = function (topicPartitionList, callback) {
  const self = this;
  this.client.sendOffsetFetchRequest(this.consumerGroup.options.groupId, topicPartitionList, function (error, results) {
    logger.debug('sendOffsetFetchRequest response:', results, error);
    if (error) {
      return callback(error);
    }
    self.offsets = results;
    callback(null);
  });
};

ConsumerGroupMigrator.prototype.getOffset = function (tp, defaultOffset) {
  const offset = _.get(this.offsets, [tp.topic, tp.partition], defaultOffset);
  if (offset === -1) {
    return defaultOffset;
  }
  return offset;
};

module.exports = ConsumerGroupMigrator;
