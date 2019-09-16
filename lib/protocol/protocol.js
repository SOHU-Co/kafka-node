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
const API_KEY_TO_NAME = _.invert(REQUEST_TYPE);
const MessageSizeTooLarge = require('../errors/MessageSizeTooLargeError');
const SaslAuthenticationError = require('../errors/SaslAuthenticationError');
const InvalidRequestError = require('../errors/InvalidRequestError');
const async = require('async');

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
  return new Buffermaker().Int32BE(request.length).string(request).make();
}

function encodeRequestHeader (clientId, correlationId, apiKey, apiVersion) {
  return new Buffermaker()
    .Int16BE(apiKey)
    .Int16BE(apiVersion || API_VERSION)
    .Int32BE(correlationId)
    .Int16BE(clientId.length)
    .string(clientId);
}

function encodeSaslHandshakeRequest (clientId, correlationId, apiVersion, mechanism) {
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.saslHandshake, apiVersion);
  request.Int16BE(mechanism.length).string(mechanism.toUpperCase());
  return encodeRequestWithLength(request.make());
}

function decodeSaslHandshakeResponse (resp) {
  var mechanisms = [];
  var errorCode = null;

  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word16bs('errorCode')
    .tap(function (vars) {
      errorCode = vars.errorCode;
    })
    .word32bs('numMechanisms')
    .loop(_decodeMechanisms);

  function _decodeMechanisms (end, vars) {
    if (vars.numMechanisms-- === 0) {
      return end();
    }
    this
      .word16be('mechanismSize')
      .tap(function (vars) {
        this.buffer('mechanism', vars.mechanismSize);
        mechanisms.push(vars.mechanism);
      });
  }
  if (errorCode == null || errorCode === 0) {
    return mechanisms;
  }
  return new SaslAuthenticationError(errorCode, 'Handshake failed.');
}

function encodeSaslAuthenticateRequest (clientId, correlationId, apiVersion, saslOpts) {
  //
  // FIXME From the Kafka protocol docs:
  //       If SaslHandshakeRequest version is v0, a series of SASL client and server tokens
  //       corresponding to the mechanism are sent as opaque packets without wrapping the
  //       messages with Kafka protocol headers. If SaslHandshakeRequest version is v1, the
  //       SaslAuthenticate request/response are used, where the actual SASL tokens are
  //       wrapped in the Kafka protocol.
  //
  var username = saslOpts.username || '';
  var password = saslOpts.password || '';
  var authBytes = null;
  if (saslOpts.mechanism.toUpperCase() === 'PLAIN') {
    authBytes =
      (new Buffermaker())
        .string(username).Int8(0)
        .string(username).Int8(0)
        .string(password)
        .make();
  } else {
    return new Error('unsupported SASL auth type: ' + saslOpts.mechanism.toUpperCase());
  }

  if (apiVersion === 0) {
    return encodeRequestWithLength(authBytes);
  }

  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.saslAuthenticate, 0);
  request.Int32BE(authBytes.length).string(authBytes);
  return encodeRequestWithLength(request.make());
}

function decodeSaslAuthenticateResponse (resp) {
  var errorCode = null;
  var errorMessage = null;
  var authBytes = null;
  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word16bs('errorCode')
    .word16bs('errorMessageLength')
    .tap(function (vars) {
      errorCode = vars.errorCode;
      this.buffer('errorMessage', vars.errorMessageLength);
      errorMessage = vars.errorMessage.toString();
    })
    .word32bs('authBytesLength')
    .tap(function (vars) {
      this.buffer('authBytes', vars.authBytesLength);
      authBytes = vars.authBytes.toString();
    });
  if (errorCode == null || errorCode === 0) {
    return authBytes;
  }
  return new SaslAuthenticationError(errorCode, errorMessage);
}

function encodeFetchRequest (maxWaitMs, minBytes) {
  return function encodeFetchRequest (clientId, correlationId, payloads) {
    return _encodeFetchRequest(clientId, correlationId, payloads, maxWaitMs, minBytes);
  };
}

function encodeFetchRequestV1 (maxWaitMs, minBytes) {
  return function encodeFetchRequest (clientId, correlationId, payloads) {
    return _encodeFetchRequest(clientId, correlationId, payloads, maxWaitMs, minBytes, 1);
  };
}

function encodeFetchRequestV2 (maxWaitMs, minBytes) {
  return function encodeFetchRequest (clientId, correlationId, payloads) {
    return _encodeFetchRequest(clientId, correlationId, payloads, maxWaitMs, minBytes, 2);
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

function _encodeFetchRequest (clientId, correlationId, payloads, maxWaitMs, minBytes, version) {
  payloads = groupByTopic(payloads);
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.fetch, version);
  var topics = Object.keys(payloads);

  request.Int32BE(REPLICA_ID).Int32BE(maxWaitMs).Int32BE(minBytes).Int32BE(topics.length);

  topics.forEach(function (topic) {
    request.Int16BE(topic.length).string(topic);

    var partitions = _.toPairs(payloads[topic]).map(function (pairs) {
      return pairs[1];
    });
    request.Int32BE(partitions.length);
    partitions.forEach(function (p) {
      request.Int32BE(p.partition).Int64BE(p.offset).Int32BE(p.maxBytes);
    });
  });

  return encodeRequestWithLength(request.make());
}

function decodeFetchResponse (cb, maxTickMessages) {
  return function (resp) {
    return _decodeFetchResponse(resp, cb, maxTickMessages, 0);
  };
}

function decodeFetchResponseV1 (cb, maxTickMessages) {
  return function (resp) {
    return _decodeFetchResponse(resp, cb, maxTickMessages, 1);
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

function _decodeFetchResponse (resp, cb, maxTickMessages, version) {
  if (!cb) {
    throw new Error('Missing callback');
  }

  var topics = {};
  var events = [];
  let eventCount = 0;

  cb(null, 'processingfetch');
  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .tap(function () {
      if (version >= 1) {
        this.word32bs('throttleTime');
      }
    })
    .word32bs('topicNum')
    .loop(decodeTopics(decodePartitions));

  // Parsing is all sync, but emitting events is async due to compression
  // At this point, the enqueue chain should be populated in order, and at
  // the end, the 'topics' map above should be fully populated.
  // topics is not ready for use until all events are processed, as an async
  // decompression may add more offsets to the topic map.
  async.series(events, (err) => {
    if (err) {
      cb(err);
    }
    cb(null, 'done', topics);
  });

  function decodePartitions (end, vars) {
    if (--vars.partitionNum === 0) end();
    topics[vars.topic] = topics[vars.topic] || {};
    let hadError = false;
    this.word32bs('partition')
      .word16bs('errorCode')
      .word64bs('highWaterOffset')
      .word32bs('messageSetSize')
      .tap(function (vars) {
        this.buffer('messageSet', vars.messageSetSize);
        const errorCode = vars.errorCode;
        const topic = vars.topic;
        const partition = vars.partition;

        if (errorCode !== 0) {
          return events.push((next) => {
            const err = new Error(ERROR_CODE[errorCode]);
            err.topic = topic;
            err.partition = partition;
            cb(err);
            next();
          });
        }

        const highWaterOffset = vars.highWaterOffset;
        const { magicByte } = Binary.parse(vars.messageSet).skip(16).word8bs('magicByte').vars;

        if (magicByte != null && magicByte > 1) {
          return events.push((next) => {
            const err = new Error('Not a message set. Magic byte is ' + magicByte);
            err.topic = topic;
            err.partition = partition;
            cb(err);
            next();
          });
        }

        decodeMessageSet(
          topic,
          partition,
          vars.messageSet,
          (enqueuedTask) => {
            events.push(enqueuedTask);
            if (maxTickMessages && ++eventCount >= maxTickMessages) {
              eventCount = 0;
              events.push((next) => process.nextTick(next));
            }
          },
          (err, data) => {
            if (err) {
              hadError = true;
            } else if (hadError) {
              return; // Once we've had an error on this partition, don't emit any more messages
            }
            if (!err && data) {
              topics[topic][partition] = data.offset;

              cb(null, 'message', data);
            } else {
              cb(err);
            }
          },
          highWaterOffset,
          topics
        );
      });
  }
}

function decodeMessageSet (topic, partition, messageSet, enqueue, emit, highWaterOffset, topics, lastOffset) {
  const messageSetSize = messageSet.length;
  // TODO: this is broken logic. It overwrites previous partitions HWO.
  // Need to refactor this on next major API bump
  topics[topic].highWaterOffset = highWaterOffset;

  let innerMessages = [];

  while (messageSet.length > 0) {
    var cur = 8 + 4 + 4 + 1 + 1 + 4 + 4;
    var partial = false;
    Binary.parse(messageSet)
      .word64bs('offset')
      .word32bs('messageSize')
      .tap(function (vars) {
        if (vars.messageSize > messageSet.length - 12) {
          partial = true;
        }
      })
      .word32bs('crc')
      .word8bs('magicByte')
      .word8bs('attributes')
      .tap(function (vars) {
        if (vars.magicByte > 0) {
          this.word64bs('timestamp');
          cur += 8;
        }
      })
      .word32bs('key')
      .tap(function (vars) {
        if (vars.key === -1) {
          vars.key = null;
          return;
        }
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

        if (vars.attributes === 0 && vars.messageSize > messageSetSize) {
          const offset = vars.offset;
          return enqueue(next => {
            emit(
              new MessageSizeTooLarge({
                topic: topic,
                offset: offset,
                partition: partition
              })
            );
            next(null);
          });
        }

        if (!partial && vars.offset !== null) {
          const offset = vars.offset;
          const value = vars.value;
          const key = vars.key;
          const magicByte = vars.magicByte;
          var codec = getCodec(vars.attributes);
          if (!codec) {
            const message = {
              topic: topic,
              value: value,
              offset: offset,
              partition: partition,
              highWaterOffset: highWaterOffset,
              key: key
            };

            if (vars.timestamp) {
              message.timestamp = new Date(vars.timestamp);
            }

            if (lastOffset != null) {
              // need to fix offset skipping enqueue till later
              innerMessages.push(message);
              return;
            }

            enqueue((next) => {
              emit(null, message);
              next(null);
            });
            return;
          }
          enqueue((next) => {
            codec.decode(value, function (error, inlineMessageSet) {
              if (error) {
                emit(error);
                next(null);
                return;
              }
              decodeMessageSet(topic, partition, inlineMessageSet, (cb) => {
                cb(_.noop);
              }, emit, highWaterOffset, topics, magicByte === 1 ? offset : null);

              // Delay 1 tick as this isn't counted to max tick messages, give a breather
              process.nextTick(() => next(null));
            });
          });
        }
      });
    // Skip decoding binary left in the message set if partial message detected
    if (partial) break;
    messageSet = messageSet.slice(cur);
  }

  if (lastOffset != null && innerMessages.length) {
    // contains inner messages, need to fix up offsets
    let len = innerMessages.length - 1;
    for (const message of innerMessages) {
      const offset = lastOffset - len--;
      message.offset = offset;
      enqueue((next) => {
        emit(null, message);
        next(null);
      });
    }
  }
}

function encodeMetadataRequest (clientId, correlationId, topics) {
  return _encodeMetadataRequest(clientId, correlationId, topics, 0);
}

function decodeMetadataResponse (resp) {
  return _decodeMetadataResponse(resp, 0);
}

function encodeMetadataV1Request (clientId, correlationId, topics) {
  return _encodeMetadataRequest(clientId, correlationId, topics, 1);
}

function decodeMetadataV1Response (resp) {
  return _decodeMetadataResponse(resp, 1);
}

function _encodeMetadataRequest (clientId, correlationId, topics, version) {
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.metadata, version);

  // In version 0 an empty array will fetch all topics.
  // In version 1+ a null value (-1) will fetch all topics. An empty array returns no topics.
  // This adds support for maintaining version 0 behaviour in client regardless of kafka version ([] = fetch all topics).
  if (version > 0 && ((Array.isArray(topics) && topics.length === 0) || topics === null)) {
    request.Int32BE(-1);
    return encodeRequestWithLength(request.make());
  }

  // Handle case where null is provided but version requested was 0 (not supported).
  // Can happen if the api versions requests fails and fallback api support is used.
  if (version === 0 && topics === null) {
    topics = [];
  }

  request.Int32BE(topics.length);
  topics.forEach(function (topic) {
    request.Int16BE(topic.length).string(topic);
  });
  return encodeRequestWithLength(request.make());
}

function _decodeMetadataResponse (resp, version) {
  var brokers = {};
  var out = {};
  var topics = {};
  var controllerId = -1;
  var errors = [];
  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word32bs('brokerNum')
    .loop(decodeBrokers)
    .tap(function (vars) {
      if (version < 1) {
        return;
      }

      this.word32bs('controllerId');
      controllerId = vars.controllerId;
    })
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
        if (version < 1) {
          return;
        }

        this.word16bs('rack');
        if (vars.rack === -1) {
          vars.rack = '';
        } else {
          this.buffer('rack', vars.rack);
          vars.rack = vars.rack.toString();
        }
      })
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

        if (version < 1) {
          return;
        }

        this.word8bs('isInternal');
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
          topics[vars.topic][vars.partition] = new PartitionMetadata(
            vars.topic,
            vars.partition,
            vars.leader,
            vars.replicas,
            vars.isr
          );
        } else {
          errors.push(ERROR_CODE[vars.errorCode]);
        }
      });
  }

  if (!_.isEmpty(errors)) out.error = errors;
  out.metadata = topics;

  if (version > 0) {
    out.clusterMetadata = {
      controllerId
    };
  }

  return [brokers, out];
}

function encodeCreateTopicRequest (clientId, correlationId, topics, timeoutMs) {
  return _encodeCreateTopicRequest(clientId, correlationId, topics, timeoutMs, 0);
}

function encodeCreateTopicRequestV1 (clientId, correlationId, topics, timeoutMs) {
  return _encodeCreateTopicRequest(clientId, correlationId, topics, timeoutMs, 1);
}

function _encodeCreateTopicRequest (clientId, correlationId, topics, timeoutMs, version) {
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.createTopics, version);
  request.Int32BE(topics.length);
  topics.forEach(function (topic) {
    request.Int16BE(topic.topic.length).string(topic.topic);

    // When replica assignment is used, partitions and replication factor must be unset (-1)
    if (topic.replicaAssignment) {
      request.Int32BE(-1);
      request.Int16BE(-1);
    } else {
      request.Int32BE(topic.partitions);
      request.Int16BE(topic.replicationFactor);
    }

    if (topic.replicaAssignment) {
      request.Int32BE(topic.replicaAssignment.length);
      topic.replicaAssignment.forEach(function (replicaAssignment) {
        request.Int32BE(replicaAssignment.partition);
        request.Int32BE(replicaAssignment.replicas.length);
        replicaAssignment.replicas.forEach(function (replica) {
          request.Int32BE(replica);
        });
      });
    } else {
      request.Int32BE(0);
    }

    if (topic.configEntries) {
      request.Int32BE(topic.configEntries.length);
      topic.configEntries.forEach(function (config) {
        request.Int16BE(config.name.length).string(config.name);

        if (config.value == null) {
          request.Int16BE(-1);
        } else {
          request.Int16BE(config.value.length).string(config.value);
        }
      });
    } else {
      request.Int32BE(0);
    }
  });
  request.Int32BE(timeoutMs);

  // Validate only is 1+
  if (version > 0) {
    request.Int8(0);
  }

  return encodeRequestWithLength(request.make());
}

function decodeCreateTopicResponse (resp) {
  return _decodeCreateTopicResponse(resp, 0);
}

function decodeCreateTopicResponseV1 (resp) {
  return _decodeCreateTopicResponse(resp, 1);
}

function _decodeCreateTopicResponse (resp, version) {
  var topicErrorResponses = [];
  var error;

  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word32bs('topicNum')
    .loop(decodeTopics);

  function decodeTopics (end, vars) {
    if (vars.topicNum-- === 0) return end();

    this.word16bs('topic')
      .tap(function (vars) {
        this.buffer('topic', vars.topic);
        vars.topic = vars.topic.toString();
      })
      .word16bs('errorCode')
      .tap(function (vars) {
        if (version > 0) {
          this.word16bs('errorMessage');

          if (vars.errorMessage !== -1) {
            this.buffer('errorMessage', vars.errorMessage);
            vars.errorMessage = vars.errorMessage.toString();
          }
        }

        if (vars.errorCode === 0) {
          return;
        }

        // Errors that are not related to the actual topic(s) but the entire request
        // (like timeout and sending the request to a non-controller)
        if (vars.errorCode === 7 || vars.errorCode === 41) {
          error = createGroupError(vars.errorCode);
          return;
        }

        if (version > 0) {
          topicErrorResponses.push({
            topic: vars.topic,
            error: vars.errorMessage
          });
        } else {
          topicErrorResponses.push({
            topic: vars.topic,
            error: 'received error code ' + vars.errorCode + ' for topic'
          });
        }
      });
  }

  return error || topicErrorResponses;
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
  return encodeOffsetCommitGenericRequest(clientId, correlationId, group, generationId, memberId, payloads, 2);
}

function encodeOffsetCommitV1Request (clientId, correlationId, group, generationId, memberId, payloads) {
  return encodeOffsetCommitGenericRequest(clientId, correlationId, group, generationId, memberId, payloads, 1);
}

function encodeOffsetCommitGenericRequest (
  clientId,
  correlationId,
  group,
  generationId,
  memberId,
  payloads,
  apiVersion
) {
  payloads = groupByTopic(payloads);
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.offsetCommit, apiVersion);
  var topics = Object.keys(payloads);

  request
    .Int16BE(group.length)
    .string(group)
    .Int32BE(generationId)
    .Int16BE(memberId.length)
    .string(memberId)
    .Int64BE(-1)
    .Int32BE(topics.length);

  topics.forEach(function (topic) {
    request.Int16BE(topic.length).string(topic);

    var partitions = _.toPairs(payloads[topic]).map(function (pairs) {
      return pairs[1];
    });
    request.Int32BE(partitions.length);
    partitions.forEach(function (p) {
      request.Int32BE(p.partition).Int64BE(p.offset).Int16BE(p.metadata.length).string(p.metadata);
    });
  });

  return encodeRequestWithLength(request.make());
}

function _encodeOffsetCommitRequest (clientId, correlationId, group, payloads) {
  payloads = groupByTopic(payloads);
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.offsetCommit);
  var topics = Object.keys(payloads);

  request.Int16BE(group.length).string(group).Int32BE(topics.length);

  topics.forEach(function (topic) {
    request.Int16BE(topic.length).string(topic);

    var partitions = _.toPairs(payloads[topic]).map(function (pairs) {
      return pairs[1];
    });
    request.Int32BE(partitions.length);
    partitions.forEach(function (p) {
      request.Int32BE(p.partition).Int64BE(p.offset).Int16BE(p.metadata.length).string(p.metadata);
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
    this.word32bs('partition').word16bs('errorcode').tap(function (vars) {
      topics[vars.topic]['partition'] = vars.partition;
      topics[vars.topic]['errorCode'] = vars.errorcode;
    });
  }
  return topics;
}

function encodeProduceRequest (requireAcks, ackTimeoutMs) {
  return function (clientId, correlationId, payloads) {
    return _encodeProduceRequest(clientId, correlationId, payloads, requireAcks, ackTimeoutMs, 0);
  };
}

function encodeProduceV1Request (requireAcks, ackTimeoutMs) {
  return function (clientId, correlationId, payloads) {
    return _encodeProduceRequest(clientId, correlationId, payloads, requireAcks, ackTimeoutMs, 1);
  };
}

function encodeProduceV2Request (requireAcks, ackTimeoutMs) {
  return function (clientId, correlationId, payloads) {
    return _encodeProduceRequest(clientId, correlationId, payloads, requireAcks, ackTimeoutMs, 2);
  };
}

function _encodeProduceRequest (clientId, correlationId, payloads, requireAcks, ackTimeoutMs, apiVersion) {
  payloads = groupByTopic(payloads);
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.produce, apiVersion);
  var topics = Object.keys(payloads);
  request.Int16BE(requireAcks).Int32BE(ackTimeoutMs).Int32BE(topics.length);

  topics.forEach(function (topic) {
    request.Int16BE(topic.length).string(topic);

    var reqs = _.toPairs(payloads[topic]).map(function (pairs) {
      return pairs[1];
    });
    request.Int32BE(reqs.length);
    reqs.forEach(function (p) {
      var messageSet = encodeMessageSet(p.messages, apiVersion === 2 ? 1 : 0);
      request.Int32BE(p.partition).Int32BE(messageSet.length).string(messageSet);
    });
  });

  return encodeRequestWithLength(request.make());
}

function encodeMessageSet (messageSet, magic) {
  var buffer = new Buffermaker();
  messageSet.forEach(function (message) {
    var msg = encodeMessage(message, magic);
    buffer.Int64BE(0).Int32BE(msg.length).string(msg);
  });
  return buffer.make();
}

function encodeMessage (message, magic) {
  if (magic == null) {
    magic = 0;
  }
  var m = new Buffermaker().Int8(magic).Int8(message.attributes);

  // Add timestamp support for new messages
  if (magic === 1) {
    m.Int64BE(message.timestamp);
  }

  var key = message.key;
  setValueOnBuffer(m, key);

  var value = message.value;
  setValueOnBuffer(m, value);

  m = m.make();
  var crc = crc32.signed(m);
  return new Buffermaker().Int32BE(crc).string(m).make();
}

function setValueOnBuffer (buffer, value) {
  if (value != null) {
    if (Buffer.isBuffer(value)) {
      buffer.Int32BE(value.length);
    } else {
      if (typeof value !== 'string') value = value.toString();
      buffer.Int32BE(Buffer.byteLength(value));
    }
    buffer.string(value);
  } else {
    buffer.Int32BE(-1);
  }
}

function decodeProduceV1Response (resp) {
  var topics = {};
  var error;
  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word32bs('topicNum')
    .loop(decodeTopics(decodePartitions))
    .word32bs('throttleTime');

  function decodePartitions (end, vars) {
    if (--vars.partitionNum === 0) end();
    topics[vars.topic] = topics[vars.topic] || {};
    this.word32bs('partition').word16bs('errorCode').word64bs('offset').tap(function (vars) {
      if (vars.errorCode) {
        error = new Error(ERROR_CODE[vars.errorCode]);
      } else {
        topics[vars.topic][vars.partition] = vars.offset;
      }
    });
  }
  return error || topics;
}

function decodeProduceV2Response (resp) {
  var topics = {};
  var error;
  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word32bs('topicNum')
    .loop(decodeTopics(decodePartitions))
    .word32bs('throttleTime');

  function decodePartitions (end, vars) {
    if (--vars.partitionNum === 0) end();
    topics[vars.topic] = topics[vars.topic] || {};
    this.word32bs('partition').word16bs('errorCode').word64bs('offset').word64bs('timestamp').tap(function (vars) {
      if (vars.errorCode) {
        error = new Error(ERROR_CODE[vars.errorCode]);
      } else {
        topics[vars.topic][vars.partition] = vars.offset;
      }
    });
  }
  return error || topics;
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
    this.word32bs('partition').word16bs('errorCode').word64bs('offset').tap(function (vars) {
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

  request.Int16BE(group.length).string(group).Int32BE(topics.length);

  topics.forEach(function (topic) {
    request.Int16BE(topic.length).string(topic).Int32BE(payloads[topic].length);

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

  request.Int16BE(group.length).string(group).Int32BE(topics.length);

  topics.forEach(function (topic) {
    request.Int16BE(topic.length).string(topic);

    var partitions = _.toPairs(payloads[topic]).map(function (pairs) {
      return pairs[1];
    });
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

  request.Int32BE(REPLICA_ID).Int32BE(topics.length);

  topics.forEach(function (topic) {
    request.Int16BE(topic.length).string(topic);

    var partitions = _.toPairs(payloads[topic]).map(function (pairs) {
      return pairs[1];
    });
    request.Int32BE(partitions.length);
    partitions.forEach(function (p) {
      request.Int32BE(p.partition).Int64BE(p.time).Int32BE(p.maxNum);
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
    this.word32bs('partition').word16bs('errorCode').word32bs('offsetNum').loop(decodeOffsets);
  }

  function decodeOffsets (end, vars) {
    if (--vars.offsetNum <= 0) end();
    topics[vars.topic][vars.partition] = topics[vars.topic][vars.partition] || [];
    this.word64bs('offset').tap(function (vars) {
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

function encodeGroupHeartbeatRequest (clientId, correlationId, groupId, generationId, memberId) {
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.heartbeat);
  request.Int16BE(groupId.length).string(groupId).Int32BE(generationId).Int16BE(memberId.length).string(memberId);
  return encodeRequestWithLength(request.make());
}

function decodeGroupHeartbeatResponse (resp) {
  var result = null;
  Binary.parse(resp).word32bs('size').word32bs('correlationId').word16bs('errorCode').tap(function (vars) {
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
  this.Int16BE(protocol.name.length).string(protocol.name).string(_encodeProtocolData(protocol));
}

function _encodeProtocolData (protocol) {
  var protocolByte = new Buffermaker().Int16BE(protocol.version).Int32BE(protocol.subscription.length);
  protocol.subscription.forEach(function (topic) {
    protocolByte.Int16BE(topic.length).string(topic);
  });

  if (protocol.userData) {
    var userDataStr = JSON.stringify(protocol.userData);
    var data = Buffer.from(userDataStr, 'utf8');
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
  request.Int16BE(groupId.length).string(groupId).Int32BE(generationId).Int16BE(memberId.length).string(memberId);

  if (groupAssignment && groupAssignment.length) {
    request.Int32BE(groupAssignment.length);
    groupAssignment.forEach(function (assignment) {
      request
        .Int16BE(assignment.memberId.length)
        .string(assignment.memberId)
        .string(_encodeMemberAssignment(assignment));
    });
  } else {
    request.Int32BE(0);
  }

  return encodeRequestWithLength(request.make());
}

function _encodeMemberAssignment (assignment) {
  var numberOfTopics = Object.keys(assignment.topicPartitions).length;

  var assignmentByte = new Buffermaker().Int16BE(assignment.version).Int32BE(numberOfTopics);

  for (var tp in assignment.topicPartitions) {
    if (!assignment.topicPartitions.hasOwnProperty(tp)) {
      continue;
    }
    var partitions = assignment.topicPartitions[tp];
    assignmentByte.Int16BE(tp.length).string(tp).Int32BE(partitions.length);

    partitions.forEach(function (partition) {
      assignmentByte.Int32BE(partition);
    });
  }

  if (assignment.userData) {
    var userDataStr = JSON.stringify(assignment.userData);
    var data = Buffer.from(userDataStr, 'utf8');
    assignmentByte.Int32BE(data.length).string(data);
  } else {
    assignmentByte.Int32BE(-1);
  }

  return encodeRequestWithLength(assignmentByte.make());
}

function encodeLeaveGroupRequest (clientId, correlationId, groupId, memberId) {
  var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.leaveGroup);
  request.Int16BE(groupId.length).string(groupId).Int16BE(memberId.length).string(memberId);

  return encodeRequestWithLength(request.make());
}

function decodeLeaveGroupResponse (resp) {
  var error = null;
  Binary.parse(resp).word32bs('size').word32bs('correlationId').word16bs('errorCode').tap(function (vars) {
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
    .Int16BE(groupId.length)
    .string(groupId)
    .Int32BE(sessionTimeout)
    .Int16BE(memberId.length)
    .string(memberId)
    .Int16BE(GROUPS_PROTOCOL_TYPE.length)
    .string(GROUPS_PROTOCOL_TYPE)
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
      this.word16bs('groupMemberId')
        .tap(function (vars) {
          this.buffer('groupMemberId', vars.groupMemberId);
          vars.memberId = vars.groupMemberId.toString();
        })
        .word32bs('memberMetadata')
        .tap(function (vars) {
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
    }).vars;

  return {
    subscription: topics,
    version: parsed.version,
    userData: parsed.userData
  };
}

function encodeDescribeGroups (clientId, correlationId, groups) {
  const request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.describeGroups);

  request.Int32BE(groups.length);
  groups.forEach(groupId => {
    request.Int16BE(groupId.length).string(groupId);
  });

  return encodeRequestWithLength(request.make());
}

function decodeDescribeGroups (resp) {
  let results = {};
  let error;

  Binary.parse(resp).word32bs('size').word32bs('correlationId').word32bs('describeNum').loop(decodeDescriptions);

  function decodeDescriptions (end, vars) {
    if (vars.describeNum-- === 0) return end();

    let described = { members: [] };
    this.word16bs('errorCode')
      .tap(vars => {
        error = createGroupError(vars.errorCode);
      })
      .word16bs('groupId')
      .tap(vars => {
        this.buffer('groupId', vars.groupId);
        described.groupId = vars.groupId.toString();
      })
      .word16bs('state')
      .tap(vars => {
        this.buffer('state', vars.state);
        described.state = vars.state.toString();
      })
      .word16bs('protocolType')
      .tap(vars => {
        this.buffer('protocolType', vars.protocolType);
        described.protocolType = vars.protocolType.toString();
      })
      .word16bs('protocol')
      .tap(vars => {
        this.buffer('protocol', vars.protocol);
        described.protocol = vars.protocol.toString();

        // keep this for error cases
        results[described.groupId] = described;
      })
      .word32bs('membersNum')
      .loop(function decodeGroupMembers (end, vars) {
        if (vars.membersNum-- === 0) return end();
        let member = {};

        this.word16bs('memberId')
          .tap(vars => {
            this.buffer('memberId', vars.memberId);
            member.memberId = vars.memberId.toString();
          })
          .word16bs('clientId')
          .tap(vars => {
            this.buffer('clientId', vars.clientId);
            member.clientId = vars.clientId.toString();
          })
          .word16bs('clientHost')
          .tap(vars => {
            this.buffer('clientHost', vars.clientHost);
            member.clientHost = vars.clientHost.toString();
          })
          .word32bs('memberMetadata')
          .tap(vars => {
            if (vars.memberMetadata > -1) {
              this.buffer('memberMetadata', vars.memberMetadata);
              let memberMetadata = decodeGroupData(vars.memberMetadata);
              memberMetadata.id = member.memberId;
              member.memberMetadata = memberMetadata;
            }
          })
          .word32bs('memberAssignment')
          .tap(vars => {
            this.buffer('memberAssignment', vars.memberAssignment);
            member.memberAssignment = decodeMemberAssignment(vars.memberAssignment);
            described.members.push(member);

            results[described.groupId] = described;
          });
      });
  }

  return error || results;
}

function encodeListGroups (clientId, correlationId) {
  return encodeRequestWithLength(encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.listGroups).make());
}

function decodeListGroups (resp) {
  let groups = {};
  let error;

  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word16bs('errorCode')
    .tap(vars => {
      error = createGroupError(vars.errorCode);
    })
    .word32bs('groupNum')
    .loop(function (end, vars) {
      if (vars.groupNum-- === 0) return end();

      this.word16bs('groupId')
        .tap(function (vars) {
          this.buffer('groupId', vars.groupId);
          vars.groupId = vars.groupId.toString();
        })
        .word16bs('protocolType')
        .tap(function (vars) {
          this.buffer('protocolType', vars.protocolType);
          groups[vars.groupId] = vars.protocolType.toString();
        });
    });

  return error || groups;
}

function encodeVersionsRequest (clientId, correlationId) {
  return encodeRequestWithLength(encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.apiVersions).make());
}

function decodeVersionsResponse (resp) {
  const MAX_SUPPORT_VERSION = require('./protocolVersions').maxSupport;
  const versions = Object.create(null);
  let error = null;

  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word16bs('errorCode')
    .tap(function (vars) {
      error = createGroupError(vars.errorCode);
    })
    .word32bs('apiNum')
    .loop(function (end, vars) {
      if (vars.apiNum-- === 0 || error) return end();

      let apiKey, minVersion, maxVersion;

      this.word16bs('apiKey')
        .tap(vars => {
          apiKey = vars.apiKey;
        })
        .word16bs('minVersion')
        .tap(vars => {
          minVersion = vars.minVersion;
        })
        .word16bs('maxVersion')
        .tap(vars => {
          maxVersion = vars.maxVersion;
        });

      const apiName = apiKey in API_KEY_TO_NAME ? API_KEY_TO_NAME[apiKey] : apiKey;

      versions[apiName] = {
        min: minVersion,
        max: maxVersion,
        usable: MAX_SUPPORT_VERSION[apiName] != null ? Math.min(MAX_SUPPORT_VERSION[apiName], maxVersion) : false
      };
    });
  return error || versions;
}

function encodeDescribeConfigsRequest (clientId, correlationId, payload) {
  return _encodeDescribeConfigsRequest(clientId, correlationId, payload, 0);
}

function encodeDescribeConfigsRequestV1 (clientId, correlationId, payload) {
  return _encodeDescribeConfigsRequest(clientId, correlationId, payload, 1);
}

function encodeDescribeConfigsRequestV2 (clientId, correlationId, payload) {
  return _encodeDescribeConfigsRequest(clientId, correlationId, payload, 2);
}

function _encodeDescribeConfigsRequest (clientId, correlationId, payload, apiVersion) {
  let request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.describeConfigs, apiVersion);
  const resources = payload.resources;
  request.Int32BE(resources.length);
  resources.forEach(function (resource) {
    request.Int8(resource.resourceType);
    request.Int16BE(resource.resourceName.length);
    request.string(resource.resourceName);
    if (resource.configNames === null || (Array.isArray(resource.configNames) && resource.configNames.length === 0)) {
      request.Int32BE(-1); // -1 will fetch all config entries for the resource
    } else {
      request.Int32BE(resource.configNames.length);
      resource.configNames.forEach(function (configName) {
        request.Int16BE(configName.length).string(configName);
      });
    }
  });
  if (apiVersion > 0) {
    request.Int8(payload.includeSynonyms || 0);
  }

  return encodeRequestWithLength(request.make());
}

function decodeDescribeConfigsResponse (resp) {
  return _decodeDescribeConfigsResponse(resp, 0);
}

function decodeDescribeConfigsResponseV1 (resp) {
  return _decodeDescribeConfigsResponse(resp, 1);
}

function decodeDescribeConfigsResponseV2 (resp) {
  return _decodeDescribeConfigsResponse(resp, 2);
}

function _decodeDescribeConfigsResponse (resp, apiVersion) {
  let resources = [];
  let error;
  Binary.parse(resp)
    .word32bs('size')
    .word32bs('correlationId')
    .word32bs('throttleTime')
    .word32bs('resourceNum')
    .loop(decodeResources);

  function decodeResources (end, vars) {
    if (vars.resourceNum-- === 0) return end();
    let resource = { configEntries: [] };

    this.word16bs('errorCode')
      .word16bs('errorMessage')
      .tap(function (vars) {
        if (vars.errorCode !== 0) {
          if (vars.errorMessage === -1) {
            vars.errorMessage = '';
          } else {
            this.buffer('errorMessage', vars.errorMessage);
            vars.errorMessage = vars.errorMessage.toString();
          }
          error = vars.errorCode === 42 ? new InvalidRequestError(vars.errorMessage) : createGroupError(vars.errorCode);
        }
      })
      .word8bs('resourceType')
      .word16bs('resourceName')
      .tap(function (vars) {
        this.buffer('resourceName', vars.resourceName);
        resource.resourceType = vars.resourceType.toString();
        resource.resourceName = vars.resourceName.toString();
      })
      .word32bs('configEntriesNum')
      .loop(decodeConfigEntries)
      .tap(function () {
        resources.push(resource);
      });

    function decodeConfigEntries (end, vars) {
      if (vars.configEntriesNum === -1 || vars.configEntriesNum-- === 0) return end();
      let configEntry = { synonyms: [] };

      this.word16bs('configName')
        .tap(function (vars) {
          this.buffer('configName', vars.configName);
          configEntry.configName = vars.configName.toString();
        })
        .word16bs('configValue')
        .tap(function (vars) {
          if (vars.configValue === -1) {
            vars.configValue = '';
          } else {
            this.buffer('configValue', vars.configValue);
            vars.configValue = vars.configValue.toString();
          }
          configEntry.configValue = vars.configValue;
        })
        .word8bs('readOnly')
        .word8bs('configSource')
        .word8bs('isSensitive')
        .tap(function () {
          if (apiVersion > 0) {
            this.word32bs('configSynonymsNum');
            this.loop(decodeConfigSynonyms);
          }
        })
        .tap(function (vars) {
          configEntry.readOnly = Boolean(vars.readOnly);
          configEntry.configSource = vars.configSource;
          configEntry.isSensitive = Boolean(vars.isSensitive);
          resource.configEntries.push(configEntry);
        });

      function decodeConfigSynonyms (end, vars) {
        if (vars.configSynonymsNum === -1 || vars.configSynonymsNum-- === 0) return end();
        let configSynonym = {};
        this.word16bs('configSynonymName')
          .tap(function (vars) {
            this.buffer('configSynonymName', vars.configSynonymName);
            configSynonym.configSynonymName = vars.configSynonymName.toString();
          })
          .word16bs('configSynonymValue')
          .tap(function (vars) {
            if (vars.configSynonymValue === -1) {
              configSynonym.configSynonymValue = '';
            } else {
              this.buffer('configSynonymValue', vars.configSynonymValue);
              configSynonym.configSynonymValue = vars.configSynonymValue.toString();
            }
          })
          .word8bs('configSynonymSource')
          .tap(function () {
            configSynonym.configSynonymSource = vars.configSynonymSource.toString();
            configEntry.synonyms.push(configSynonym);
          });
      }
    }
  }
  return error || resources;
}

exports.encodeSaslHandshakeRequest = encodeSaslHandshakeRequest;
exports.decodeSaslHandshakeResponse = decodeSaslHandshakeResponse;
exports.encodeSaslAuthenticateRequest = encodeSaslAuthenticateRequest;
exports.decodeSaslAuthenticateResponse = decodeSaslAuthenticateResponse;

exports.encodeFetchRequest = encodeFetchRequest;
exports.decodeFetchResponse = decodeFetchResponse;
exports.encodeFetchRequestV1 = encodeFetchRequestV1;
exports.decodeFetchResponseV1 = decodeFetchResponseV1;
exports.encodeFetchRequestV2 = encodeFetchRequestV2;

exports.encodeOffsetCommitRequest = encodeOffsetCommitRequest;
exports.encodeOffsetCommitV1Request = encodeOffsetCommitV1Request;
exports.encodeOffsetCommitV2Request = encodeOffsetCommitV2Request;
exports.decodeOffsetCommitResponse = decodeOffsetCommitResponse;

exports.encodeOffsetFetchRequest = encodeOffsetFetchRequest;
exports.encodeOffsetFetchV1Request = encodeOffsetFetchV1Request;
exports.decodeOffsetFetchResponse = decodeOffsetFetchResponse;
exports.decodeOffsetFetchV1Response = decodeOffsetFetchV1Response;
exports.encodeMetadataRequest = encodeMetadataRequest;
exports.decodeMetadataResponse = decodeMetadataResponse;
exports.encodeMetadataV1Request = encodeMetadataV1Request;
exports.decodeMetadataV1Response = decodeMetadataV1Response;

exports.encodeCreateTopicRequest = encodeCreateTopicRequest;
exports.encodeCreateTopicRequestV1 = encodeCreateTopicRequestV1;
exports.decodeCreateTopicResponse = decodeCreateTopicResponse;
exports.decodeCreateTopicResponseV1 = decodeCreateTopicResponseV1;

exports.encodeProduceRequest = encodeProduceRequest;
exports.encodeProduceV1Request = encodeProduceV1Request;
exports.encodeProduceV2Request = encodeProduceV2Request;
exports.decodeProduceResponse = decodeProduceResponse;
exports.decodeProduceV1Response = decodeProduceV1Response;
exports.decodeProduceV2Response = decodeProduceV2Response;

exports.encodeOffsetRequest = encodeOffsetRequest;
exports.decodeOffsetResponse = decodeOffsetResponse;
exports.encodeMessageSet = encodeMessageSet;
exports.encodeJoinGroupRequest = encodeJoinGroupRequest;
exports.decodeJoinGroupResponse = decodeJoinGroupResponse;
exports.encodeGroupCoordinatorRequest = encodeGroupCoordinatorRequest;
exports.decodeGroupCoordinatorResponse = decodeGroupCoordinatorResponse;
exports.encodeGroupHeartbeatRequest = encodeGroupHeartbeatRequest;
exports.decodeGroupHeartbeatResponse = decodeGroupHeartbeatResponse;
exports.encodeSyncGroupRequest = encodeSyncGroupRequest;
exports.decodeSyncGroupResponse = decodeSyncGroupResponse;
exports.encodeLeaveGroupRequest = encodeLeaveGroupRequest;
exports.decodeLeaveGroupResponse = decodeLeaveGroupResponse;
exports.encodeDescribeGroups = encodeDescribeGroups;
exports.decodeDescribeGroups = decodeDescribeGroups;
exports.encodeListGroups = encodeListGroups;
exports.decodeListGroups = decodeListGroups;
exports.encodeVersionsRequest = encodeVersionsRequest;
exports.decodeVersionsResponse = decodeVersionsResponse;
exports.encodeDescribeConfigsRequest = encodeDescribeConfigsRequest;
exports.encodeDescribeConfigsRequestV1 = encodeDescribeConfigsRequestV1;
exports.encodeDescribeConfigsRequestV2 = encodeDescribeConfigsRequestV2;
exports.decodeDescribeConfigsResponse = decodeDescribeConfigsResponse;
exports.decodeDescribeConfigsResponseV1 = decodeDescribeConfigsResponseV1;
exports.decodeDescribeConfigsResponseV2 = decodeDescribeConfigsResponseV2;
