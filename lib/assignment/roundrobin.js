'use strict';

var _ = require('lodash');
var groupPartitionsByTopic = require('../utils').groupPartitionsByTopic;
var logger = require('../logging')('kafka-node:Roundrobin');
var VERSION = 0;

function assignRoundRobin (topicPartition, groupMembers, callback) {
  logger.debug('topicPartition: %j', topicPartition);
  logger.debug('groupMembers: %j', groupMembers);
  var _members = _(groupMembers).map('id');
  var members = _members.value().sort();
  logger.debug('members', members);
  var assignment = _members.reduce(function (obj, id) {
    obj[id] = [];
    return obj;
  }, {});

  var subscriberMap = groupMembers.reduce(function (subscribers, member) {
    subscribers[member.id] = member.subscription;
    return subscribers;
  }, {});

  logger.debug('subscribers', subscriberMap);

  // layout topic/partitions pairs into a list
  var topicPartitionList = _(topicPartition).map(function (partitions, topic) {
    return partitions.map(function (partition) {
      return {
        topic: topic,
        partition: partition
      };
    });
  }).flatten().value();
  logger.debug('round robin on topic partition pairs: ', topicPartitionList);

  var assigner = cycle(members);

  topicPartitionList.forEach(function (tp) {
    var topic = tp.topic;
    while (!_.includes(subscriberMap[assigner.peek()], topic)) {
      assigner.next();
    }
    assignment[assigner.next()].push(tp);
  });

  var ret = _.map(assignment, function (value, key) {
    var ret = {};
    ret.memberId = key;
    ret.topicPartitions = groupPartitionsByTopic(value);
    ret.version = VERSION;
    return ret;
  });

  callback(null, ret);
}

function cycle (arr) {
  var index = -1;
  var len = arr.length;
  return {
    peek: function () {
      return arr[(index + 1) % len];
    },
    next: function () {
      index = ++index % len;
      return arr[index];
    }
  };
}

module.exports = {
  assign: assignRoundRobin,
  name: 'roundrobin',
  version: VERSION
};
