'use strict';

const p = require('./protocol');
const _ = require('lodash');

const API_MAP = {
  produce: [
    [p.encodeProduceRequest, p.decodeProduceResponse],
    [p.encodeProduceV1Request, p.decodeProduceV1Response],
    [p.encodeProduceV2Request, p.decodeProduceV2Response]
  ],
  fetch: [[p.encodeFetchRequest, p.decodeFetchResponse]],
  offset: [[p.encodeOffsetRequest, p.decodeOffsetResponse]],
  metadata: [[p.encodeMetadataRequest, p.decodeMetadataResponse]],
  leader: null,
  stopReplica: null,
  updateMetadata: null,
  controlledShutdown: null,
  offsetCommit: [
    // decode response should be the same for versions 0-2
    [p.encodeOffsetCommitRequest, p.decodeOffsetCommitResponse],
    [p.encodeOffsetCommitV1Request, p.decodeOffsetCommitResponse],
    [p.encodeOffsetCommitV2Request, p.decodeOffsetCommitResponse]
  ],
  offsetFetch: [
    [p.encodeOffsetFetchRequest, p.decodeOffsetFetchResponse],
    [p.encodeOffsetFetchV1Request, p.decodeOffsetFetchV1Response]
  ],
  groupCoordinator: [[p.encodeGroupCoordinatorRequest, p.decodeGroupCoordinatorResponse]],
  joinGroup: [[p.encodeJoinGroupRequest, p.decodeJoinGroupResponse]],
  heartbeat: [[p.encodeGroupHeartbeatRequest, p.decodeGroupHeartbeatResponse]],
  leaveGroup: [[p.encodeLeaveGroupRequest, p.decodeLeaveGroupResponse]],
  syncGroup: [[p.encodeJoinGroupRequest, p.decodeJoinGroupResponse]],
  describeGroups: [[p.encodeDescribeGroups, p.decodeDescribeGroups]],
  listGroups: [[p.encodeListGroups, p.decodeListGroups]],
  saslHandshake: null,
  apiVersions: [[p.encodeVersionsRequest, p.decodeVersionsResponse]],
  createTopics: null,
  deleteTopics: null
};

// Since versions API isn't around until 0.10 we need to hardcode the supported API versions for 0.9 here
const API_SUPPORTED_IN_KAFKA_0_9 = {
  produce: {
    min: 0,
    max: 1,
    usable: 1
  },
  offsetCommit: {
    min: 0,
    max: 2,
    usable: 2
  },
  offsetFetch: {
    min: 0,
    max: 1,
    usable: 1
  }
};

module.exports = {
  apiMap: API_MAP,
  maxSupport: _.mapValues(API_MAP, function (api) {
    return api != null ? api.length - 1 : null;
  }),
  baseSupport: Object.assign(
    _.mapValues(API_MAP, function (api) {
      return api != null ? { min: 0, max: 0, usable: 0 } : null;
    }),
    API_SUPPORTED_IN_KAFKA_0_9
  )
};
