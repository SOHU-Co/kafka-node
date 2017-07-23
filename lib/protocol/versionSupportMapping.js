'use strict';

const p = require('./protocol');
const _ = require('lodash');

const API_MAP = {
  produce: [[p.encodeProduceRequest, p.decodeProduceResponse]],
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
  describeGroups: null,
  listGroups: null,
  saslHandshake: null,
  apiVersions: [[p.encodeVersionsRequest, p.decodeVersionsResponse]],
  createTopics: null,
  deleteTopics: null
};

module.exports = {
  apiMap: API_MAP,
  maxSupported: _.mapValues(API_MAP, function (api) {
    return api != null ? api.length - 1 : null;
  })
};
