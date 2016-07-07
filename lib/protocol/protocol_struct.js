'use strict';

function createStruct () {
  var args = arguments[0];
  return function () {
    for (var i = 0; i < args.length; i++) {
      this[args[i]] = arguments[i];
    }
  };
}

var KEYS = {
  FetchRequest: ['topic', 'partition', 'offset', 'maxBytes'],
  FetchResponse: ['topic', 'fetchPartitions'],
  OffsetCommitRequest: ['topic', 'partition', 'offset', 'metadata', 'committing', 'autoCommitIntervalMs'],
  OffsetCommitResponse: [],
  TopicAndPartition: ['topic', 'partition'],
  PartitionMetadata: ['topic', 'partition', 'leader', 'replicas', 'isr'],
  Message: ['magic', 'attributes', 'key', 'value'],
  ProduceRequest: ['topic', 'partition', 'messages', 'attributes'],
  Request: ['payloads', 'encoder', 'decoder', 'callback']
};

var ERROR_CODE = {
  '0': 'NoError',
  '-1': 'Unknown',
  '1': 'OffsetOutOfRange',
  '2': 'InvalidMessage',
  '3': 'UnknownTopicOrPartition',
  '4': 'InvalidMessageSize',
  '5': 'LeaderNotAvailable',
  '6': 'NotLeaderForPartition',
  '7': 'RequestTimedOut',
  '8': 'BrokerNotAvailable',
  '9': 'ReplicaNotAvailable',
  '10': 'MessageSizeTooLarge',
  '11': 'StaleControllerEpochCode',
  '12': 'OffsetMetadataTooLargeCode'
};

var REQUEST_TYPE = {
  produce: 0,
  fetch: 1,
  offset: 2,
  metadata: 3,
  leader: 4,
  stopReplilca: 5,
  offsetCommit: 8,
  offsetFetch: 9
};

Object.keys(KEYS).forEach(function (o) { exports[o] = createStruct(KEYS[o]); });
exports.KEYS = KEYS;
exports.ERROR_CODE = ERROR_CODE;
exports.REQUEST_TYPE = REQUEST_TYPE;
exports.KeyedMessage = function KeyedMessage (key, value) {
  exports.Message.call(this, 0, 0, key, value);
};
