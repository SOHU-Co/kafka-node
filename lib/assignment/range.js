'use strict';

const logger = require('../logging')('kafka-node:Range');
const VERSION = 0;
const _ = require('lodash');
const groupPartitionsByTopic = require('../utils').groupPartitionsByTopic;

function assignRange (topicPartition, groupMembers, callback) {
  logger.debug('topicPartition: %j', topicPartition);
  var assignment = _(groupMembers).map('id').reduce(function (obj, id) {
    obj[id] = [];
    return obj;
  }, {});

  const topicMemberMap = topicToMemberMap(groupMembers);
  for (var topic in topicMemberMap) {
    if (!topicMemberMap.hasOwnProperty(topic)) {
      continue;
    }
    logger.debug('For topic %s', topic);
    topicMemberMap[topic].sort();
    logger.debug('   members: ', topicMemberMap[topic]);

    var numberOfPartitionsForTopic = topicPartition[topic].length;
    logger.debug('   numberOfPartitionsForTopic', numberOfPartitionsForTopic);

    var numberOfMembersForTopic = topicMemberMap[topic].length;
    logger.debug('   numberOfMembersForTopic', numberOfMembersForTopic);

    var numberPartitionsPerMember = Math.floor(numberOfPartitionsForTopic / numberOfMembersForTopic);
    logger.debug('   numberPartitionsPerMember', numberPartitionsPerMember);

    var membersWithExtraPartition = numberOfPartitionsForTopic % numberOfMembersForTopic;
    var topicPartitionList = createTopicPartitionArray(topic, numberOfPartitionsForTopic);

    for (var i = 0, n = numberOfMembersForTopic; i < n; i++) {
      var start = numberPartitionsPerMember * i + Math.min(i, membersWithExtraPartition);
      var length = numberPartitionsPerMember + (i + 1 > membersWithExtraPartition ? 0 : 1);
      var assignedTopicPartitions = assignment[topicMemberMap[topic][i]];
      assignedTopicPartitions.push.apply(assignedTopicPartitions, topicPartitionList.slice(start, start + length));
    }
  }

  logger.debug(assignment);

  callback(null, convertToAssignmentList(assignment, VERSION));
}

function convertToAssignmentList (assignment, version) {
  return _.map(assignment, function (value, key) {
    return {
      memberId: key,
      topicPartitions: groupPartitionsByTopic(value),
      version: version
    };
  });
}

function createTopicPartitionArray (topic, numberOfPartitions) {
  return _.times(numberOfPartitions, function (n) {
    return {
      topic: topic,
      partition: n
    };
  });
}

function topicToMemberMap (groupMembers) {
  return groupMembers.reduce(function (result, member) {
    member.subscription.forEach(function (topic) {
      if (topic in result) {
        result[topic].push(member.id);
      } else {
        result[topic] = [member.id];
      }
    });
    return result;
  }, {});
}

module.exports = {
  assign: assignRange,
  name: 'range',
  version: VERSION
};
