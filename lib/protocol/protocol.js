'use strict';

var Binary = require('binary'),
    Buffermaker = require('buffermaker'),
    _  = require('lodash'),
    crc32 = require('buffer-crc32'),
    protocol = require('./protocol_struct'),
    KEYS = protocol.KEYS,
    REQUEST_TYPE = protocol.REQUEST_TYPE,
    ERROR_CODE = protocol.ERROR_CODE,
    FetchResponse = protocol.FetchResponse,
    liberrors = require('./../errors'),
    PartitionMetadata = protocol.PartitionMetadata;

var API_VERSION = 0,
    REPLICA_ID = -1,
    CODEC_NONE = 0,
    CODEC_GZIP = 1,
    CODEC_SNAPPY = 2;

function groupByTopic(payloads) {
    return payloads.reduce(function (out, p) { 
        out[p.topic] = out[p.topic] || {};
        out[p.topic][p.partition] = p;
        return out;
    }, {});
}

function encodeRequestWithLength(request) {
    return new Buffermaker()
        .Int32BE(request.length)
        .string(request)
        .make();
}

function encodeRequestHeader(clientId, correlationId, apiKey) {
    return new Buffermaker()
        .Int16BE(apiKey)
        .Int16BE(API_VERSION)
        .Int32BE(correlationId)
        .Int16BE(clientId.length)
        .string(clientId);
}

function encodeFetchRequest(maxWaitMs, minBytes) {
    return function encodeFetchRequest(clientId, correlationId, payloads) {
        return _encodeFetchRequest(clientId, correlationId, payloads, maxWaitMs, minBytes);
    }
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

    }
}

function _encodeFetchRequest(clientId, correlationId, payloads, maxWaitMs, minBytes) {
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

        var partitions = _.pairs(payloads[topic]).map(function (pairs) { return pairs[1] });
        request.Int32BE(partitions.length);
        partitions.forEach(function (p) {
            request.Int32BE(p.partition)
                .Int64BE(p.offset)
                .Int32BE(p.maxBytes);
        });
    });

    return encodeRequestWithLength(request.make());
}

function decodeFetchResponse(cb) {
    return function (resp) {
       return _decodeFetchResponse(resp, cb);
    }
}

function _decodeFetchResponse(resp, cb) {
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
                this.buffer('messageSet', vars.messageSetSize)
                if (vars.errorCode !== 0) 
                    cb({ topic: vars.topic, partition: vars.partition, message: ERROR_CODE[vars.errorCode] });
                var messageSet = decodeMessageSet(vars.topic, vars.partition, vars.messageSet, cb);
                if (messageSet.length) {
                    var offset = messageSet[messageSet.length-1];
                    topics[vars.topic][vars.partition] = offset;
                }
            });
    }
    cb && cb(null, 'done', topics);
}

function decodeMessageSet(topic, partition, messageSet, cb) {
    var set = [];
    while (messageSet.length > 0) {
        var cur = 8 + 4 + 4 + 1 + 1 + 4 + 4;
        Binary.parse(messageSet)
            .word64bs('offset')
            .word32bs('messageSize')
            .tap(function (vars) {
                if (vars.messageSize > (messageSet.length - 12)) 
                    vars.partial = true;
            })
            .word32bs('crc')
            .word8bs('magicByte')
            .word8bs('attributes')
            .word32bs('key')
            .tap(function (vars) {
                if (vars.key === -1)  return;
                cur += vars.key;
                this.buffer('key', vars.key);
            })
            .word32bs('value')
            .tap(function (vars) {
                if (vars.value !== -1) {
                    cur += vars.value;
                    this.buffer('value', vars.value);
                    vars.value = vars.value.toString();
                }

                if (!vars.partial && vars.offset !== null) {
                    cb && cb(null, 'message', { topic: topic, value: vars.value, offset: vars.offset, partition: partition, key: vars.key });
                    set.push(vars.offset);
                }
            });
        messageSet = messageSet.slice(cur);
    }
    return set;
}

function encodeMetadataRequest(clientId, correlationId, topics) {
    var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.metadata);
    request.Int32BE(topics.length);
    topics.forEach(function (topic) {
        request.Int16BE(topic.length)
                .string(topic);
    });
    return encodeRequestWithLength(request.make());
}

function decodeMetadataResponse(resp) {
    var brokers = {}, out = { response: {} }, topics = null, errors = null;
    Binary.parse(resp)
        .word32bs('size')
        .word32bs('correlationId')
        .word32bs('brokerNum')
        .loop(decodeBrokers)
        .word32bs('topicNum')
        .loop(_decodeTopics);
    
    function decodeBrokers(end, vars) {
        if (vars.brokerNum-- === 0) return end();
        this.word32bs('nodeId')
            .word16bs('host')
            .tap(function (vars) { 
                this.buffer('host', vars.host)
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
                    errors = errors || new liberrors.TopicsError();
                    return errors._addTopic(vars, vars.topicError, ERROR_CODE[vars.topicError])
                    }
                this.loop(decodePartitions);
            });
    }

    function decodePartitions (end, vars) {
        if (vars.partitionNum-- === 0) return end();
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
                if (vars.errorCode !== 0) {
                    errors = errors || new liberrors.TopicsError();
                    errors._addTopic(vars, vars.errorCode, ERROR_CODE[vars.errorCode])
                    }
                else {
                    topics = topics || {};
                    topics[vars.topic] = topics[vars.topic] || {};
                    topics[vars.topic][vars.partition] = new PartitionMetadata(vars.topic, vars.partition, vars.leader, vars.replicas, vars.isr);
                    }
            });
    }
    
    out.response.error = errors;
    out.response.success = [brokers, { metadata : topics }];

    return out;
}

function bufferToArray(num, buffer) {
    var ret = []
    for(var i=0; i< num; i++) {
        ret.push(Binary.parse(buffer).word32bs('r').vars.r);
        buffer = buffer.slice(4);
    }
    return ret;
}

function encodeOffsetCommitRequest(group) {
    return function (clientId, correlationId, payloads) {
        return _encodeOffsetCommitRequest(clientId, correlationId, group, payloads);
    }
}

function _encodeOffsetCommitRequest(clientId, correlationId, group, payloads) {
    payloads = groupByTopic(payloads);
    var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.offsetCommit);
    var topics = Object.keys(payloads);

    request.Int16BE(group.length)
            .string(group)
            .Int32BE(topics.length);

    topics.forEach(function (topic) {
        request.Int16BE(topic.length)
                .string(topic);

        var partitions = _.pairs(payloads[topic]).map(function (pairs) { return pairs[1] });
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

function decodeOffsetCommitResponse(resp) {
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

function encodeProduceRequest(requireAcks, ackTimeoutMs) {
    return function (clientId, correlationId, payloads) {
        return _encodeProduceRequest(clientId, correlationId, payloads, requireAcks, ackTimeoutMs);
    }
}

function _encodeProduceRequest(clientId, correlationId, payloads, requireAcks, ackTimeoutMs) {
    payloads = groupByTopic(payloads);
    var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.produce);
    var topics = Object.keys(payloads);
    request.Int16BE(requireAcks)
        .Int32BE(ackTimeoutMs)
        .Int32BE(topics.length);

    topics.forEach(function (topic) {
        request.Int16BE(topic.length)
               .string(topic);

        var reqs = _.pairs(payloads[topic]).map(function (pairs) { return pairs[1] });
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

function encodeMessageSet(messageSet) {
    var buffer = new Buffermaker();
    messageSet.forEach(function (message) {
        var msg = encodeMessage(message);
        buffer.Int64BE(0)
            .Int32BE(msg.length)
            .string(msg);
    });
    return buffer.make();
}

function encodeMessage(message) {
    var m = new Buffermaker()
        .Int8(message.magic)
        .Int8(message.attributes)
        .Int32BE(message.key.length)
        .string(message.key)
        .Int32BE(Buffer.isBuffer(message.value) ? message.value.length : Buffer.byteLength(message.value))
        .string(message.value).make();
    var crc = crc32.signed(m);
    return new Buffermaker()
        .Int32BE(crc)
        .string(m)
        .make();
}

function decodeProduceResponse(resp) {
    var out = { response: {} };
    var errors = null;
    var topics = null;
    Binary.parse(resp)
        .word32bs('size')
        .word32bs('correlationId')
        .word32bs('topicNum')
        .loop(decodeTopics(decodePartitions));

    function decodePartitions (end, vars) {
        if (--vars.partitionNum === 0) end();
        this.word32bs('partition')
            .word16bs('errorCode')
            .word64bs('offset')
            .tap(function (vars) {
                if (vars.errorCode !== 0) {
                    errors = errors || new liberrors.TopicsPartitionsError();
                    errors._addTopic(vars, vars.errorCode, ERROR_CODE[vars.errorCode]);
                } else {
                    topics = topics || {};
                    topics[vars.topic] = topics[vars.topic] || { partitions: {} };
                    topics[vars.topic].partitions[vars.partition] = {};
                    topics[vars.topic].partitions[vars.partition].offset = vars.offset;
                }
            });
    }
    out.response.error = errors;
    out.response.success = topics;

    return out;
}

function encodeOffsetFetchRequest(group) {
    return function (clientId, correlationId, payloads) {
        return _encodeOffsetFetchRequest(clientId, correlationId, group, payloads);
    }
}

function _encodeOffsetFetchRequest(clientId, correlationId, group, payloads) {
    payloads = groupByTopic(payloads);
    var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.offsetFetch);
    var topics = Object.keys(payloads);

    request.Int16BE(group.length)
            .string(group)
            .Int32BE(topics.length);

    topics.forEach(function (topic) {
        request.Int16BE(topic.length)
                .string(topic);

        var partitions = _.pairs(payloads[topic]).map(function (pairs) { return pairs[1] });
        request.Int32BE(partitions.length);
        partitions.forEach(function (p) {
            request.Int32BE(p.partition)
        });
    });

    return encodeRequestWithLength(request.make());
}

function decodeOffsetFetchResponse(resp) {
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
                    return
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

function encodeOffsetRequest(clientId, correlationId, payloads) {
    payloads = groupByTopic(payloads);
    var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.offset);
    var topics = Object.keys(payloads);

    request.Int32BE(REPLICA_ID)
            .Int32BE(topics.length);

    topics.forEach(function (topic) {
        request.Int16BE(topic.length)
                .string(topic);

        var partitions = _.pairs(payloads[topic]).map(function (pairs) { return pairs[1] });
        request.Int32BE(partitions.length);
        partitions.forEach(function (p) {
            request.Int32BE(p.partition)
                    .DoubleBE(p.time)
                    .Int32BE(p.maxNum);
        });
    });

    return encodeRequestWithLength(request.make());
}

function decodeOffsetResponse(resp) {
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
        if (--vars.offsetNum === 0) end();
        topics[vars.topic][vars.partition] = topics[vars.topic][vars.partition] || [];
        this.word64bs('offset')
            .tap(function (vars) {
                topics[vars.topic][vars.partition].push(vars.offset);
            });
    }
    return topics;
}

exports.encodeFetchRequest = encodeFetchRequest;
exports.decodeFetchResponse = decodeFetchResponse;
exports.encodeOffsetCommitRequest = encodeOffsetCommitRequest;
exports.decodeOffsetCommitResponse = decodeOffsetCommitResponse;
exports.encodeOffsetFetchRequest = encodeOffsetFetchRequest;
exports.decodeOffsetFetchResponse = decodeOffsetFetchResponse;
exports.encodeMetadataRequest = encodeMetadataRequest;
exports.decodeMetadataResponse = decodeMetadataResponse;
exports.encodeProduceRequest = encodeProduceRequest;
exports.decodeProduceResponse = decodeProduceResponse;
exports.encodeOffsetRequest = encodeOffsetRequest;
exports.decodeOffsetResponse = decodeOffsetResponse;
