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
  Message: ['magic', 'attributes', 'key', 'value', 'timestamp'],
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
  '12': 'OffsetMetadataTooLargeCode',
  '14': 'GroupLoadInProgress',
  '15': 'GroupCoordinatorNotAvailable',
  '16': 'NotCoordinatorForGroup',
  '17': 'InvalidTopic',
  '18': 'RecordListTooLarge',
  '19': 'NotEnoughReplicas',
  '20': 'NotEnoughReplicasAfterAppend',
  '21': 'InvalidRequiredAcks',
  '22': 'IllegalGeneration',
  '23': 'InconsistentGroupProtocol',
  '25': 'UnknownMemberId',
  '26': 'InvalidSessionTimeout',
  '27': 'RebalanceInProgress',
  '28': 'InvalidCommitOffsetSize',
  '29': 'TopicAuthorizationFailed',
  '30': 'GroupAuthorizationFailed',
  '31': 'ClusterAuthorizationFailed',
  '41': 'NotController',
  '42': 'InvalidRequest'
};

var GROUP_ERROR = {
  GroupCoordinatorNotAvailable: require('../errors/GroupCoordinatorNotAvailableError'),
  IllegalGeneration: require('../errors/IllegalGenerationError'),
  NotCoordinatorForGroup: require('../errors/NotCoordinatorForGroupError'),
  GroupLoadInProgress: require('../errors/GroupLoadInProgressError'),
  UnknownMemberId: require('../errors/UnknownMemberIdError'),
  RebalanceInProgress: require('../errors/RebalanceInProgressError'),
  NotController: require('../errors/NotControllerError')
};

var REQUEST_TYPE = {
  produce: 0,
  fetch: 1,
  offset: 2,
  metadata: 3,
  leader: 4,
  stopReplica: 5,
  updateMetadata: 6,
  controlledShutdown: 7,
  offsetCommit: 8,
  offsetFetch: 9,
  groupCoordinator: 10,
  joinGroup: 11,
  heartbeat: 12,
  leaveGroup: 13,
  syncGroup: 14,
  describeGroups: 15,
  listGroups: 16,
  saslHandshake: 17,
  apiVersions: 18,
  createTopics: 19,
  deleteTopics: 20,
  describeConfigs: 32,
  saslAuthenticate: 36
};

Object.keys(KEYS).forEach(function (o) {
  exports[o] = createStruct(KEYS[o]);
});
exports.KEYS = KEYS;
exports.ERROR_CODE = ERROR_CODE;
exports.GROUP_ERROR = GROUP_ERROR;
exports.REQUEST_TYPE = REQUEST_TYPE;
exports.KeyedMessage = function KeyedMessage (key, value) {
  exports.Message.call(this, 0, 0, key, value, Date.now());
};
