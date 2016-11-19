'use strict';

var Binary = require('binary');
var Buffermaker = require('buffermaker');
var _ = require('lodash');
var crc32 = require('buffer-crc32');
var protocol = require('./protocol_struct');
var getCodec = require('../codec');
var REQUEST_TYPE = protocol.REQUEST_TYPE;
var ERROR_CODE = protocol.ERROR_CODE;
var GROUP_ERROR = protocol.GROUP_ERROR;
var PartitionMetadata = protocol.PartitionMetadata;

var API_VERSION = 0;
var REPLICA_ID = -1;
var GROUPS_PROTOCOL_TYPE = 'consumer';

function groupByTopic (payloads) {
  return payloads.reduce(function (out, p) {
    out[p.topic] = out[p.topic] || {};
    out[p.topic][p.partition] = p;
    return out;
  }, {});
}

function encodeRequestWithLength (request) {
  return new Buffermaker()
    .Int32BE(request.length)
    .string(request)
    .make();
}

function encodeRequestHeader (clientId, correlationId, apiKey, apiVersion) {
  return new Buffermaker()
    .Int16BE(apiKey)
    .Int16BE(apiVersion || API_VERSION)
    .Int32BE(correlationId)
    .Int16BE(clientId.length)
    .string(clientId);
}

function encodeFetchRequest (maxWaitMs, minBytes) {
  return function encodeFetchRequest (clientId, correlationId, payloads) {
    return _encodeFetchRequest(clientId, correlationId, payloads, maxWaitMs, minBytes);
  };
}

function decodeTopics (decodePartitions) {
  return function (end, vars) {
    if (--vars.topicNum === 0) end();
    this.word16bs('topic')
      .tap(function (vars) {
        this.buffer('topic', vars.topic);
        vars.topic = vars.topic.toString();
      })
      .word32bs('partitionNum')
      .loop(decodePartitions);
  };
}

function _encodeFetchRequest (clientId, correlationId, payloads, maxWaitMs, minBytes) {
  payloads = groupByTopic(payloads);
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.fetch);
  var topics = Object.keys(payloads);

  request.Int32BE(REPLICA_ID)
    .Int32BE(maxWaitMs)
    .Int32BE(minBytes)
    .Int32BE(topics.length);

  topics.forEach(function (topic) {
    request.Int16BE(topic.length)
      .string(topic);

    var partitions = _.pairs(payloads[topic]).map(function (pairs) { return pairs[1]; });
    request.Int32BE(partitions.length);
    partitions.forEach(function (p) {
      request.Int32BE(p.partition)
        .Int64BE(p.offset)
        .Int32BE(p.maxBytes);
    });
  });

  return encodeRequestWithLength(request.make());
}

function decodeFetchResponse (cb, maxTickMessages) {
  return function (resp) {
    return _decodeFetchResponse(resp, cb, maxTickMessages);
  };
}

function createGroupError (errorCode) {
  if (errorCode == null || errorCode === 0) {
    return null;
  }

  var error = ERROR_CODE[errorCode];
  if (error in GROUP_ERROR) {
    error = new GROUP_ERROR[error]('Kafka Error Code: ' + errorCode);
  } else {
    error = new Error(error);
  }
  error.errorCode = errorCode;
  return error;
}

function _decodeFetchResponse (resp, cb, maxTickMessages) {
  var topics = {};
  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word32bs('topicNum')
    .loop(decodeTopics(decodePartitions));

  function decodePartitions (end, vars) {
    if (--vars.partitionNum === 0) end();
    topics[vars.topic] = topics[vars.topic] || {};
    this.word32bs('partition')
      .word16bs('errorCode')
      .word64bs('highWaterOffset')
      .word32bs('messageSetSize')
      .tap(function (vars) {
        this.buffer('messageSet', vars.messageSetSize);
        if (vars.errorCode !== 0) {
          return cb({ topic: vars.topic, partition: vars.partition, message: ERROR_CODE[vars.errorCode] });
        }
        var messageSet = decodeMessageSet(vars.topic, vars.partition, vars.messageSet, cb, maxTickMessages);
        if (messageSet.length) {
          var offset = messageSet[messageSet.length - 1];
          topics[vars.topic][vars.partition] = offset;
        }
      });
  }
  cb && cb(null, 'done', topics);
}

function decodeMessageSet (topic, partition, messageSet, cb, maxTickMessages) {
  var set = [];
  var messageCount = 0;
  while (messageSet.length > 0) {
    var cur = 8 + 4 + 4 + 1 + 1 + 4 + 4;
    Binary.parse(messageSet)
      .word64bs('offset')
      .word32bs('messageSize')
      .tap(function (vars) {
        if (vars.messageSize > (messageSet.length - 12)) {
          vars.partial = true;
        }
      })
      .word32bs('crc')
      .word8bs('magicByte')
      .word8bs('attributes')
      .word32bs('key')
      .tap(function (vars) {
        if (vars.key === -1) return;
        cur += vars.key;
        this.buffer('key', vars.key);
      })
      .word32bs('value')
      .tap(function (vars) {
        if (vars.value !== -1) {
          cur += vars.value;
          this.buffer('value', vars.value);
        } else {
          vars.value = null;
        }

        if (!vars.partial && vars.offset !== null) {
          messageCount++;
          set.push(vars.offset);
          if (!cb) return;
          var codec = getCodec(vars.attributes);
          if (!codec) {
            return cb(null, 'message', {
              topic: topic,
              value: vars.value,
              offset: vars.offset,
              partition: partition,
              key: vars.key
            });
          }
          codec.decode(vars.value, function (error, inlineMessageSet) {
            if (error) return; // Not sure where to report this
            decodeMessageSet(topic, partition, inlineMessageSet, cb, maxTickMessages);
          });
        }
      });
    // Defensive code around potential denial of service
    if (maxTickMessages && messageCount > maxTickMessages) break;
    messageSet = messageSet.slice(cur);
  }
  return set;
}

function encodeMetadataRequest (clientId, correlationId, topics) {
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.metadata);
  request.Int32BE(topics.length);
  topics.forEach(function (topic) {
    request.Int16BE(topic.length)
      .string(topic);
  });
  return encodeRequestWithLength(request.make());
}

function decodeMetadataResponse (resp) {
  var brokers = {};
  var out = {};
  var topics = {};
  var errors = [];
  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word32bs('brokerNum')
    .loop(decodeBrokers)
    .word32bs('topicNum')
    .loop(_decodeTopics);

  function decodeBrokers (end, vars) {
    if (vars.brokerNum-- === 0) return end();
    this.word32bs('nodeId')
      .word16bs('host')
      .tap(function (vars) {
        this.buffer('host', vars.host);
        vars.host = vars.host.toString();
      })
      .word32bs('port')
      .tap(function (vars) {
        brokers[vars.nodeId] = { nodeId: vars.nodeId, host: vars.host, port: vars.port };
      });
  }

  function _decodeTopics (end, vars) {
    if (vars.topicNum-- === 0) return end();
    this.word16bs('topicError')
      .word16bs('topic')
      .tap(function (vars) {
        this.buffer('topic', vars.topic);
        vars.topic = vars.topic.toString();
      })
      .word32bs('partitionNum')
      .tap(function (vars) {
        if (vars.topicError !== 0) {
          return errors.push(ERROR_CODE[vars.topicError]);
        }
        this.loop(decodePartitions);
      });
  }

  function decodePartitions (end, vars) {
    if (vars.partitionNum-- === 0) return end();
    topics[vars.topic] = topics[vars.topic] || {};
    this.word16bs('errorCode')
      .word32bs('partition')
      .word32bs('leader')
      .word32bs('replicasNum')
      .tap(function (vars) {
        var buffer = this.buffer('replicas', vars.replicasNum * 4).vars.replicas;
        this.vars.replicas = bufferToArray(vars.replicasNum, buffer);
      })
      .word32bs('isrNum')
      .tap(function (vars) {
        var buffer = this.buffer('isr', vars.isrNum * 4).vars.isr;
        this.vars.isr = bufferToArray(vars.isrNum, buffer);
        if (vars.errorCode === 0 || vars.errorCode === 9) {
          topics[vars.topic][vars.partition] =
            new PartitionMetadata(vars.topic, vars.partition, vars.leader, vars.replicas, vars.isr);
        } else {
          errors.push(ERROR_CODE[vars.errorCode]);
        }
      });
  }

  if (!_.isEmpty(errors)) out.error = errors;
  out.metadata = topics;
  return [brokers, out];
}

function bufferToArray (num, buffer) {
  var ret = [];
  for (var i = 0; i < num; i++) {
    ret.push(Binary.parse(buffer).word32bs('r').vars.r);
    buffer = buffer.slice(4);
  }
  return ret;
}

function encodeOffsetCommitRequest (group) {
  return function (clientId, correlationId, payloads) {
    return _encodeOffsetCommitRequest(clientId, correlationId, group, payloads);
  };
}

function encodeOffsetCommitV2Request (clientId, correlationId, group, generationId, memberId, payloads) {
  payloads = groupByTopic(payloads);
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.offsetCommit, 2);
  var topics = Object.keys(payloads);

  request.Int16BE(group.length).string(group)
    .Int32BE(generationId)
    .Int16BE(memberId.length).string(memberId)
    .Int64BE(-1)
    .Int32BE(topics.length);

  topics.forEach(function (topic) {
    request.Int16BE(topic.length)
      .string(topic);

    var partitions = _.pairs(payloads[topic]).map(function (pairs) { return pairs[1]; });
    request.Int32BE(partitions.length);
    partitions.forEach(function (p) {
      request.Int32BE(p.partition)
        .Int64BE(p.offset)
        .Int16BE(p.metadata.length)
        .string(p.metadata);
    });
  });

  return encodeRequestWithLength(request.make());
}

function _encodeOffsetCommitRequest (clientId, correlationId, group, payloads) {
  payloads = groupByTopic(payloads);
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.offsetCommit);
  var topics = Object.keys(payloads);

  request.Int16BE(group.length)
    .string(group)
    .Int32BE(topics.length);

  topics.forEach(function (topic) {
    request.Int16BE(topic.length)
      .string(topic);

    var partitions = _.pairs(payloads[topic]).map(function (pairs) { return pairs[1]; });
    request.Int32BE(partitions.length);
    partitions.forEach(function (p) {
      request.Int32BE(p.partition)
        .Int64BE(p.offset)
        .Int16BE(p.metadata.length)
        .string(p.metadata);
    });
  });

  return encodeRequestWithLength(request.make());
}

function decodeOffsetCommitResponse (resp) {
  var topics = {};
  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word32bs('topicNum')
    .loop(decodeTopics(decodePartitions));

  function decodePartitions (end, vars) {
    if (--vars.partitionNum === 0) end();
    topics[vars.topic] = topics[vars.topic] || {};
    this.word32bs('partition')
      .word16bs('errorcode')
      .tap(function (vars) {
        topics[vars.topic]['partition'] = vars.partition;
        topics[vars.topic]['errorCode'] = vars.errorcode;
      });
  }
  return topics;
}

function encodeProduceRequest (requireAcks, ackTimeoutMs) {
  return function (clientId, correlationId, payloads) {
    return _encodeProduceRequest(clientId, correlationId, payloads, requireAcks, ackTimeoutMs);
  };
}

function _encodeProduceRequest (clientId, correlationId, payloads, requireAcks, ackTimeoutMs) {
  payloads = groupByTopic(payloads);
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.produce);
  var topics = Object.keys(payloads);
  request.Int16BE(requireAcks)
    .Int32BE(ackTimeoutMs)
    .Int32BE(topics.length);

  topics.forEach(function (topic) {
    request.Int16BE(topic.length)
      .string(topic);

    var reqs = _.pairs(payloads[topic]).map(function (pairs) { return pairs[1]; });
    request.Int32BE(reqs.length);
    reqs.forEach(function (p) {
      var messageSet = encodeMessageSet(p.messages);
      request.Int32BE(p.partition)
        .Int32BE(messageSet.length)
        .string(messageSet);
    });
  });

  return encodeRequestWithLength(request.make());
}

function encodeMessageSet (messageSet) {
  var buffer = new Buffermaker();
  messageSet.forEach(function (message) {
    var msg = encodeMessage(message);
    buffer.Int64BE(0)
      .Int32BE(msg.length)
      .string(msg);
  });
  return buffer.make();
}

function encodeMessage (message) {
  var m = new Buffermaker()
    .Int8(message.magic)
    .Int8(message.attributes);

  var key = message.key;
  if (key) {
    m.Int32BE(message.key.length);
    m.string(message.key);
  } else {
    m.Int32BE(-1);
  }

  var value = message.value;

  if (value !== null && value !== undefined) {
    if (Buffer.isBuffer(value)) {
      m.Int32BE(value.length);
    } else {
      if (typeof value !== 'string') value = value.toString();
      m.Int32BE(Buffer.byteLength(value));
    }
    m.string(value);
  } else {
    m.Int32BE(-1);
  }
  m = m.make();
  var crc = crc32.signed(m);
  return new Buffermaker()
    .Int32BE(crc)
    .string(m)
    .make();
}

function decodeProduceResponse (resp) {
  var topics = {};
  var error;
  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word32bs('topicNum')
    .loop(decodeTopics(decodePartitions));

  function decodePartitions (end, vars) {
    if (--vars.partitionNum === 0) end();
    topics[vars.topic] = topics[vars.topic] || {};
    this.word32bs('partition')
      .word16bs('errorCode')
      .word64bs('offset')
      .tap(function (vars) {
        if (vars.errorCode) {
          error = new Error(ERROR_CODE[vars.errorCode]);
        } else {
          topics[vars.topic][vars.partition] = vars.offset;
        }
      });
  }
  return error || topics;
}

function encodeOffsetFetchRequest (group) {
  return function (clientId, correlationId, payloads) {
    return _encodeOffsetFetchRequest(clientId, correlationId, group, payloads);
  };
}

function encodeOffsetFetchV1Request (clientId, correlationId, group, payloads) {
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.offsetFetch, 1);
  var topics = Object.keys(payloads);

  request.Int16BE(group.length)
    .string(group)
    .Int32BE(topics.length);

  topics.forEach(function (topic) {
    request.Int16BE(topic.length).string(topic)
      .Int32BE(payloads[topic].length);

    payloads[topic].forEach(function (p) {
      request.Int32BE(p);
    });
  });

  return encodeRequestWithLength(request.make());
}

function _encodeOffsetFetchRequest (clientId, correlationId, group, payloads) {
  payloads = groupByTopic(payloads);
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.offsetFetch);
  var topics = Object.keys(payloads);

  request.Int16BE(group.length)
    .string(group)
    .Int32BE(topics.length);

  topics.forEach(function (topic) {
    request.Int16BE(topic.length)
      .string(topic);

    var partitions = _.pairs(payloads[topic]).map(function (pairs) { return pairs[1]; });
    request.Int32BE(partitions.length);
    partitions.forEach(function (p) {
      request.Int32BE(p.partition);
    });
  });

  return encodeRequestWithLength(request.make());
}

function decodeOffsetFetchResponse (resp) {
  var topics = {};
  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word32bs('topicNum')
    .loop(decodeTopics(decodePartitions));

  function decodePartitions (end, vars) {
    if (--vars.partitionNum === 0) end();
    topics[vars.topic] = topics[vars.topic] || {};
    this.word32bs('partition')
      .word64bs('offset')
      .word16bs('metadata')
      .tap(function (vars) {
        if (vars.metadata === -1) {
          return;
        }

        this.buffer('metadata', vars.metadata);
      })
      .word16bs('errorCode')
      .tap(function (vars) {
        topics[vars.topic][vars.partition] = vars.errorCode === 0 ? vars.offset : -1;
      });
  }
  return topics;
}

function decodeOffsetFetchV1Response (resp) {
  var topics = {};
  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word32bs('topicNum')
    .loop(decodeTopics(decodePartitions));

  function decodePartitions (end, vars) {
    if (--vars.partitionNum === 0) end();
    topics[vars.topic] = topics[vars.topic] || {};
    this.word32bs('partition')
      .word64bs('offset')
      .word16bs('metadata')
      .tap(function (vars) {
        if (vars.metadata === -1) {
          return;
        }

        this.buffer('metadata', vars.metadata);
      })
      .word16bs('errorCode')
      .tap(function (vars) {
        if (vars.metadata.length === 0 && vars.offset === 0) {
          topics[vars.topic][vars.partition] = -1;
        } else {
          topics[vars.topic][vars.partition] = vars.errorCode === 0 ? vars.offset : -1;
        }
      });
  }
  return topics;
}

function encodeOffsetRequest (clientId, correlationId, payloads) {
  payloads = groupByTopic(payloads);
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.offset);
  var topics = Object.keys(payloads);

  request.Int32BE(REPLICA_ID)
    .Int32BE(topics.length);

  topics.forEach(function (topic) {
    request.Int16BE(topic.length)
      .string(topic);

    var partitions = _.pairs(payloads[topic]).map(function (pairs) { return pairs[1]; });
    request.Int32BE(partitions.length);
    partitions.forEach(function (p) {
      request.Int32BE(p.partition)
        .Int64BE(p.time)
        .Int32BE(p.maxNum);
    });
  });

  return encodeRequestWithLength(request.make());
}

function decodeOffsetResponse (resp) {
  var topics = {};
  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word32bs('topicNum')
    .loop(decodeTopics(decodePartitions));

  function decodePartitions (end, vars) {
    if (--vars.partitionNum === 0) end();
    topics[vars.topic] = topics[vars.topic] || {};
    this.word32bs('partition')
      .word16bs('errorCode')
      .word32bs('offsetNum')
      .loop(decodeOffsets);
  }

  function decodeOffsets (end, vars) {
    if (--vars.offsetNum <= 0) end();
    topics[vars.topic][vars.partition] = topics[vars.topic][vars.partition] || [];
    this.word64bs('offset')
      .tap(function (vars) {
        if (vars.offset != null) topics[vars.topic][vars.partition].push(vars.offset);
      });
  }
  return topics;
}

function encodeGroupCoordinatorRequest (clientId, correlationId, groupId) {
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.groupCoordinator);
  request.Int16BE(groupId.length).string(groupId);
  return encodeRequestWithLength(request.make());
}

function encodeGroupHeartbeat (clientId, correlationId, groupId, generationId, memberId) {
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.heartbeat);
  request
    .Int16BE(groupId.length).string(groupId)
    .Int32BE(generationId)
    .Int16BE(memberId.length).string(memberId);
  return encodeRequestWithLength(request.make());
}

function decodeGroupHeartbeat (resp) {
  var result = null;
  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word16bs('errorCode')
    .tap(function (vars) {
      result = createGroupError(vars.errorCode);
    });
  return result;
}

function decodeGroupCoordinatorResponse (resp) {
  var result;
  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word16bs('errorCode')
    .word32bs('coordinatorId')
    .word16bs('coordinatorHost')
    .tap(function (vars) {
      this.buffer('coordinatorHost', vars.coordinatorHost);
      vars.coordinatorHost = vars.coordinatorHost.toString();
    })
    .word32bs('coordinatorPort')
    .tap(function (vars) {
      if (vars.errorCode !== 0) {
        result = createGroupError(vars.errorCode);
        return;
      }

      result = {
        coordinatorHost: vars.coordinatorHost,
        coordinatorPort: vars.coordinatorPort,
        coordinatorId: vars.coordinatorId
      };
    });
  return result;
}

/*

ProtocolType => "consumer"

ProtocolName => AssignmentStrategy
  AssignmentStrategy => string

ProtocolMetadata => Version Subscription UserData
  Version => int16
  Subscription => [Topic]
    Topic => string
  UserData => bytes
*/

function encodeGroupProtocol (protocol) {
  this
    .Int16BE(protocol.name.length).string(protocol.name)
    .string(_encodeProtocolData(protocol));
}

function _encodeProtocolData (protocol) {
  var protocolByte = new Buffermaker()
    .Int16BE(protocol.version)
    .Int32BE(protocol.subscription.length);
  protocol.subscription.forEach(function (topic) {
    protocolByte.Int16BE(topic.length).string(topic);
  });

  if (protocol.userData) {
    var userDataStr = JSON.stringify(protocol.userData);
    var data = new Buffer(userDataStr, 'utf8');
    protocolByte.Int32BE(data.length).string(data);
  } else {
    protocolByte.Int32BE(-1);
  }

  return encodeRequestWithLength(protocolByte.make());
}

function decodeSyncGroupResponse (resp) {
  var result;
  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word16bs('errorCode')
    .tap(function (vars) {
      result = createGroupError(vars.errorCode);
    })
    .word32bs('memberAssignment')
    .tap(function (vars) {
      if (result) {
        return;
      }
      this.buffer('memberAssignment', vars.memberAssignment);
      result = decodeMemberAssignment(vars.memberAssignment);
    });

  return result;
}

/*
MemberAssignment => Version PartitionAssignment
  Version => int16
  PartitionAssignment => [Topic [Partition]]
    Topic => string
    Partition => int32
  UserData => bytes
*/

function decodeMemberAssignment (assignmentBytes) {
  var assignment = {
    partitions: {}
  };

  Binary.parse(assignmentBytes)
    .word16bs('version')
    .tap(function (vars) {
      assignment.version = vars.version;
    })
    .word32bs('partitionAssignment')
    .loop(function (end, vars) {
      if (vars.partitionAssignment-- === 0) return end();

      var topic;
      var partitions = [];

      this.word16bs('topic')
        .tap(function (vars) {
          this.buffer('topic', vars.topic);
          topic = vars.topic.toString();
        })
        .word32bs('partitionsNum')
        .loop(function (end, vars) {
          if (vars.partitionsNum-- === 0) return end();
          this.word32bs('partition').tap(function (vars) {
            partitions.push(vars.partition);
          });
        });
      assignment.partitions[topic] = partitions;
    })
    .word32bs('userData')
    .tap(function (vars) {
      if (vars.userData == null || vars.userData === -1) {
        return;
      }
      this.buffer('userData', vars.userData);
      try {
        assignment.userData = JSON.parse(vars.userData.toString());
      } catch (e) {
        assignment.userData = 'JSON Parse error';
      }
    });

  return assignment;
}

function encodeSyncGroupRequest (clientId, correlationId, groupId, generationId, memberId, groupAssignment) {
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.syncGroup);
  request
    .Int16BE(groupId.length).string(groupId)
    .Int32BE(generationId)
    .Int16BE(memberId.length).string(memberId);

  if (groupAssignment && groupAssignment.length) {
    request.Int32BE(groupAssignment.length);
    groupAssignment.forEach(function (assignment) {
      request.Int16BE(assignment.memberId.length).string(assignment.memberId)
        .string(_encodeMemberAssignment(assignment));
    });
  } else {
    request.Int32BE(0);
  }

  return encodeRequestWithLength(request.make());
}

function _encodeMemberAssignment (assignment) {
  var numberOfTopics = Object.keys(assignment.topicPartitions).length;

  var assignmentByte = new Buffermaker()
    .Int16BE(assignment.version)
    .Int32BE(numberOfTopics);

  for (var tp in assignment.topicPartitions) {
    if (!assignment.topicPartitions.hasOwnProperty(tp)) {
      continue;
    }
    var partitions = assignment.topicPartitions[tp];
    assignmentByte.Int16BE(tp.length).string(tp)
      .Int32BE(partitions.length);

    partitions.forEach(function (partition) {
      assignmentByte.Int32BE(partition);
    });
  }

  if (assignment.userData) {
    var userDataStr = JSON.stringify(assignment.userData);
    var data = new Buffer(userDataStr, 'utf8');
    assignmentByte.Int32BE(data.length).string(data);
  } else {
    assignmentByte.Int32BE(-1);
  }

  return encodeRequestWithLength(assignmentByte.make());
}

function encodeLeaveGroupRequest (clientId, correlationId, groupId, memberId) {
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.leaveGroup);
  request
    .Int16BE(groupId.length).string(groupId)
    .Int16BE(memberId.length).string(memberId);

  return encodeRequestWithLength(request.make());
}

function decodeLeaveGroupResponse (resp) {
  var error = null;
  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word16bs('errorCode')
    .tap(function (vars) {
      error = createGroupError(vars.errorCode);
    });
  return error;
}

// {
//   name: '', // string
//   subscription: [/* topics */],
//   version: 0, // integer
//   userData: {} //arbitary
// }

/*
JoinGroupRequest => GroupId SessionTimeout MemberId ProtocolType GroupProtocols
  GroupId => string
  SessionTimeout => int32
  MemberId => string
  ProtocolType => string
  GroupProtocols => [ProtocolName ProtocolMetadata]
    ProtocolName => string
    ProtocolMetadata => bytes
*/

function encodeJoinGroupRequest (clientId, correlationId, groupId, memberId, sessionTimeout, groupProtocols) {
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.joinGroup);
  request
    .Int16BE(groupId.length).string(groupId)
    .Int32BE(sessionTimeout)
    .Int16BE(memberId.length).string(memberId)
    .Int16BE(GROUPS_PROTOCOL_TYPE.length).string(GROUPS_PROTOCOL_TYPE)
    .Int32BE(groupProtocols.length);

  groupProtocols.forEach(encodeGroupProtocol.bind(request));

  return encodeRequestWithLength(request.make());
}

/*

v0 and v1 supported in 0.9.0 and greater
JoinGroupResponse => ErrorCode GenerationId GroupProtocol LeaderId MemberId Members
  ErrorCode => int16
  GenerationId => int32
  GroupProtocol => string
  LeaderId => string
  MemberId => string
  Members => [MemberId MemberMetadata]
    MemberId => string
    MemberMetadata => bytes
*/
function decodeJoinGroupResponse (resp) {
  var result = {
    members: []
  };

  var error;

  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word16bs('errorCode')
    .tap(function (vars) {
      error = createGroupError(vars.errorCode);
    })
    .word32bs('generationId')
    .tap(function (vars) {
      result.generationId = vars.generationId;
    })
    .word16bs('groupProtocol')
    .tap(function (vars) {
      this.buffer('groupProtocol', vars.groupProtocol);
      result.groupProtocol = vars.groupProtocol = vars.groupProtocol.toString();
    })
    .word16bs('leaderId')
    .tap(function (vars) {
      this.buffer('leaderId', vars.leaderId);
      result.leaderId = vars.leaderId = vars.leaderId.toString();
    })
    .word16bs('memberId')
    .tap(function (vars) {
      this.buffer('memberId', vars.memberId);
      result.memberId = vars.memberId = vars.memberId.toString();
    })
    .word32bs('memberNum')
    .loop(function (end, vars) {
      if (error) {
        return end();
      }

      if (vars.memberNum-- === 0) return end();
      var memberMetadata;
      this
        .word16bs('groupMemberId').tap(function (vars) {
          this.buffer('groupMemberId', vars.groupMemberId);
          vars.memberId = vars.groupMemberId.toString();
        })
        .word32bs('memberMetadata').tap(function (vars) {
          if (vars.memberMetadata > -1) {
            this.buffer('memberMetadata', vars.memberMetadata);
            memberMetadata = decodeGroupData(this.vars.memberMetadata);
            memberMetadata.id = vars.memberId;
            result.members.push(memberMetadata);
          }
        });
    });

  return error || result;
}

function decodeGroupData (resp) {
  var topics = [];
  var parsed = Binary.parse(resp)
    .word16bs('version')
    .word32bs('subscriptionNum')
    .loop(function decodeSubscription (end, vars) {
      if (vars.subscriptionNum-- === 0) return end();
      this.word16bs('topic').tap(function () {
        this.buffer('topic', vars.topic);
        topics.push(vars.topic.toString());
      });
    })
    .word32bs('userData')
    .tap(function (vars) {
      if (vars.userData === -1) {
        vars.userData = undefined;
        return;
      }
      this.buffer('userData', vars.userData);
      try {
        vars.userData = JSON.parse(vars.userData.toString());
      } catch (error) {
        vars.userData = 'JSON parse error';
      }
    })
    .vars;

  return {
    subscription: topics,
    version: parsed.version,
    userData: parsed.userData
  };
}

exports.encodeFetchRequest = encodeFetchRequest;
exports.decodeFetchResponse = decodeFetchResponse;
exports.encodeOffsetCommitRequest = encodeOffsetCommitRequest;
exports.encodeOffsetCommitV2Request = encodeOffsetCommitV2Request;
exports.decodeOffsetCommitResponse = decodeOffsetCommitResponse;
exports.encodeOffsetFetchRequest = encodeOffsetFetchRequest;
exports.encodeOffsetFetchV1Request = encodeOffsetFetchV1Request;
exports.decodeOffsetFetchResponse = decodeOffsetFetchResponse;
exports.decodeOffsetFetchV1Response = decodeOffsetFetchV1Response;
exports.encodeMetadataRequest = encodeMetadataRequest;
exports.decodeMetadataResponse = decodeMetadataResponse;
exports.encodeProduceRequest = encodeProduceRequest;
exports.decodeProduceResponse = decodeProduceResponse;
exports.encodeOffsetRequest = encodeOffsetRequest;
exports.decodeOffsetResponse = decodeOffsetResponse;
exports.encodeMessageSet = encodeMessageSet;
exports.encodeJoinGroupRequest = encodeJoinGroupRequest;
exports.decodeJoinGroupResponse = decodeJoinGroupResponse;
exports.encodeGroupCoordinatorRequest = encodeGroupCoordinatorRequest;
exports.decodeGroupCoordinatorResponse = decodeGroupCoordinatorResponse;
exports.encodeGroupHeartbeat = encodeGroupHeartbeat;
exports.decodeGroupHeartbeat = decodeGroupHeartbeat;
exports.encodeSyncGroupRequest = encodeSyncGroupRequest;
exports.decodeSyncGroupResponse = decodeSyncGroupResponse;
exports.encodeLeaveGroupRequest = encodeLeaveGroupRequest;
exports.decodeLeaveGroupResponse = decodeLeaveGroupResponse;
