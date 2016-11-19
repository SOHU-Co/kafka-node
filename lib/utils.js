var assert = require('assert');
var InvalidConfigError = require('./errors/InvalidConfigError');
var legalChars = new RegExp('^[a-zA-Z0-9._-]*$');
const allowedTopicLength = 249;

function validateConfig (property, value) {
  if (!legalChars.test(value)) {
    throw new InvalidConfigError([property, value, "is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'"].join(' '));
  }
}

function validateTopicNames (topics) {
  // Rewriting same validations done by Apache Kafka team for topics
  // iterating over topics
  topics.some(function (topic) {
    if (topic.length <= 0) {
      throw new InvalidConfigError('topic name is illegal, cannot be empty');
    }
    if (topic === '.' || topic === '..') {
      throw new InvalidConfigError('topic name cannot be . or ..');
    }
    if (topic.length > allowedTopicLength) {
      throw new InvalidConfigError(`topic name is illegal, cannot be longer than ${allowedTopicLength} characters`);
    }
    if (!legalChars.test(topic)) {
      throw new InvalidConfigError(`topic name ${topic} is illegal, contains a character other than ASCII alphanumerics .,_ and -`);
    }
  });
  return true;
}

function validateTopics (topics) {
  if (topics.some(function (topic) {
    if ('partition' in topic) {
      return typeof topic.partition !== 'number';
    }
    return false;
  })) {
    throw new InvalidConfigError('Offset must be a number and can not contain characters');
  }
}

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
  return topicPartitions.reduce(function (result, tp) {
    if (!(tp.topic in result)) {
      result[tp.topic] = [tp.partition];
    } else {
      result[tp.topic].push(tp.partition);
    }
    return result;
  }, {});
}

/*
Converts:
  {
    test: [0, 1],
    bob: [0]
  }

Into a topic partition payload:
  [
    {topic: 'test', partition: 0},
    {topic: 'test', partition: 1},
    {topic: 'Bob', partition: 0}
  ]
*/
function createTopicPartitionList (topicPartitions) {
  var tpList = [];
  for (var topic in topicPartitions) {
    if (!topicPartitions.hasOwnProperty(topic)) {
      continue;
    }
    topicPartitions[topic].forEach(function (partition) {
      tpList.push({
        topic: topic,
        partition: partition
      });
    });
  }
  return tpList;
}

module.exports = {
  validateConfig: validateConfig,
  validateTopics: validateTopics,
  groupPartitionsByTopic: groupPartitionsByTopic,
  createTopicPartitionList: createTopicPartitionList,
  validateTopicNames: validateTopicNames
};
