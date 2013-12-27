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
        .UInt32BE(request.length)
        .string(request)
        .make();
}

function encodeRequestHeader(clientId, correlationId, apiKey) {
    return new Buffermaker()
        .UInt16BE(apiKey)
        .UInt16BE(API_VERSION)
        .UInt32BE(correlationId)
        .UInt16BE(clientId.length)
        .string(clientId);
}

function encodeFetchRequest(maxWaitMs, minBytes) {
    return function (clientId, correlationId, payloads) {
        return _encodeFetchRequest(clientId, correlationId, payloads, maxWaitMs, minBytes);
    }
}

function decodeTopics (decodePartitions) {
    return function (end, vars) {
    if (--vars.topicNum === 0) end();
    this.word16bu('topic')
        .tap(function (vars) {
            this.buffer('topic', vars.topic);
            vars.topic = vars.topic.toString();
        })
        .word32bu('partitionNum')
        .loop(decodePartitions);

    }
}

function _encodeFetchRequest(clientId, correlationId, payloads, maxWaitMs, minBytes) {
    payloads = groupByTopic(payloads);
    var request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.fetch);
    var topics = Object.keys(payloads);

    request.Int32BE(REPLICA_ID)
        .UInt32BE(maxWaitMs)
        .UInt32BE(minBytes)
        .UInt32BE(topics.length);

    topics.forEach(function (topic) {
        request.UInt16BE(topic.length)
            .string(topic);

        var partitions = _.pairs(payloads[topic]).map(function (pairs) { return pairs[1] });
        request.UInt32BE(partitions.length);
        partitions.forEach(function (p) {
            request.UInt32BE(p.partition)
                .Int64BE(p.offset)
                .UInt32BE(p.maxBytes);
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
        .word32bu('size')
        .word32bu('correlationId')
        .word32bu('topicNum')
        .loop(decodeTopics(decodePartitions));

    function decodePartitions (end, vars) {
        if (--vars.partitionNum === 0) end();
        topics[vars.topic] = topics[vars.topic] || {};
        this.word32bu('partition')
            .word16bu('errorCode')
            .word64bu('highWaterOffset')
            .word32bu('messageSetSize')
            .tap(function (vars) {
                this.buffer('messageSet', vars.messageSetSize)
                if (vars.errorCode !== 0) 
                    cb({ topic: vars.topic, partition: vars.partition, message:  ERROR_CODE[vars.errorCode] });
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
            .word64bu('offset')
            .word32bu('messageSize')
            .tap(function (vars) {
                if (vars.messageSize > (messageSet.length - 12)) 
                    vars.partial = true;
            })
            .word32bu('crc')
            .word8bu('magicByte')
            .word8bu('attributes')
            .word32bs('key')
            .tap(function (vars) {
                if (vars.key == -1)  return;
                cur += vars.key;
                this.buffer('key', vars.key);
            })
            .word32bu('value')
            .tap(function (vars) {
                if (vars.value == -1)  return;
                cur += vars.value;
                this.buffer('value', vars.value);
                vars.value = vars.value.toString();
                if (!vars.partial && vars.offset !== null) {
                    cb && cb(null, 'message', { topic: topic, value: vars.value, offset: vars.offset, partition: partition });  
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
var i=0;
function decodeMetadataResponse(resp) {
    var brokers = {}, out = {}, topics = {}, errors = [];
    Binary.parse(resp)
        .word32bu('size')
        .word32bu('correlationId')
        .word32bu('brokerNum')
        .loop(decodeBrokers)
        .word32bu('topicNum')
        .loop(_decodeTopics);
    
    function decodeBrokers(end, vars) {
        if (vars.brokerNum-- === 0) return end();
        this.word32bu('nodeId')
            .word16bu('host')
            .tap(function (vars) { 
                this.buffer('host', vars.host)
                vars.host = vars.host.toString();
            })
            .word32bu('port')
            .tap(function (vars) {
                brokers[vars.nodeId] = { nodeId: vars.nodeId, host: vars.host, port: vars.port };
            });
    }

    function _decodeTopics (end, vars) {
        if (vars.topicNum-- === 0) return end();
        this.word16bu('topicError')
            .word16bu('topic')
            .tap(function (vars) {
                this.buffer('topic', vars.topic);
                vars.topic = vars.topic.toString(); 
            })
            .word32bu('partitionNum')
            .tap(function (vars) {
                if (vars.topicError !== 0)
                    return errors.push(ERROR_CODE[vars.topicError]);
                this.loop(decodePartitions);
            });
    }

    function decodePartitions (end, vars) {
        if (vars.partitionNum-- === 0) return end();
        topics[vars.topic] = topics[vars.topic] || {};
        this.word16bu('errorCode')
            .word32bu('partition')
            .word32bu('leader')
            .word32bu('replicasNum')
            .tap(function (vars) {
                var buffer = this.buffer('replicas', vars.replicasNum * 4).vars.replicas; 
                this.vars.replicas = bufferToArray(vars.replicasNum, buffer);
            })
            .word32bu('isrNum')
            .tap(function (vars) {
                var buffer = this.buffer('isr', vars.isrNum * 4).vars.isr;
                this.vars.isr = bufferToArray(vars.isrNum, buffer);
                if (vars.errorCode !==0)
                    errors.push(ERROR_CODE[vars.errorCode]);
                else
                    topics[vars.topic][vars.partition] = 
                        new PartitionMetadata(vars.topic, vars.partition, vars.leader, vars.replicas, vars.isr);
            });
    }
    
    if (!_.isEmpty(errors)) out.error = errors;
    out.metadata = topics;
    return [brokers, out];
}

function bufferToArray(num, buffer) {
    var ret = []
    for(var i=0; i< num; i++) {
        ret.push(Binary.parse(buffer).word32bu('r').vars.r);
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

    request.UInt16BE(group.length)
            .string(group)
            .UInt32BE(topics.length);

    topics.forEach(function (topic) {
        request.UInt16BE(topic.length)
                .string(topic);

        var partitions = _.pairs(payloads[topic]).map(function (pairs) { return pairs[1] });
        request.UInt32BE(partitions.length);
        partitions.forEach(function (p) {
            request.UInt32BE(p.partition)
                    .Int64BE(p.offset)
                    .UInt16BE(p.metadata.length)
                    .string(p.metadata);
        });
    });

    return encodeRequestWithLength(request.make());
}

function decodeOffsetCommitResponse(resp) {
    var topics = {};
    Binary.parse(resp)
        .word32bu('size')
        .word32bu('correlationId')
        .word32bu('topicNum')
        .loop(decodeTopics(decodePartitions));

    function decodePartitions (end, vars) {
        if (--vars.partitionNum === 0) end();
        topics[vars.topic] = topics[vars.topic] || {};
        this.word32bu('partition')
            .word16bu('errorcode')
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
    request.UInt16BE(requireAcks)
        .UInt32BE(ackTimeoutMs)
        .UInt32BE(topics.length);

    topics.forEach(function (topic) {
        request.UInt16BE(topic.length)
               .string(topic);

        var reqs = _.pairs(payloads[topic]).map(function (pairs) { return pairs[1] });
        request.UInt32BE(reqs.length);
        reqs.forEach(function (p) {
            var messageSet = encodeMessageSet(p.messages);
            request.UInt32BE(p.partition)
                .UInt32BE(messageSet.length)
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
            .UInt32BE(msg.length)
            .string(msg);
    });
    return buffer.make();
}

function encodeMessage(message) {
    var m = new Buffermaker()
        .UInt8(message.magic)
        .UInt8(message.attributes)
        .UInt32BE(message.key.length)
        .string(message.key)
        .UInt32BE(message.value.length)
        .string(message.value).make();
    var crc = crc32.unsigned(m);
    return new Buffermaker()
        .UInt32BE(crc)
        .string(m)
        .make();
}

function decodeProduceResponse(resp) {
    var topics = {};
    Binary.parse(resp)
        .word32bu('size')
        .word32bu('correlationId')
        .word32bu('topicNum')
        .loop(decodeTopics(decodePartitions));
    
    function decodePartitions (end, vars) {
        if (--vars.partitionNum === 0) end();
        topics[vars.topic] = topics[vars.topic] || {};
        this.word32bu('partition')
            .word16bu('errorCode')
            .word64bu('offset')
            .tap(function (vars) {
                topics[vars.topic][vars.partition] = vars.offset;
            });
    }
    return topics;
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

    request.UInt16BE(group.length)
            .string(group)
            .UInt32BE(topics.length);

    topics.forEach(function (topic) {
        request.UInt16BE(topic.length)
                .string(topic);

        var partitions = _.pairs(payloads[topic]).map(function (pairs) { return pairs[1] });
        request.UInt32BE(partitions.length);
        partitions.forEach(function (p) {
            request.UInt32BE(p.partition)
        });
    });

    return encodeRequestWithLength(request.make());
}

function decodeOffsetFetchResponse(resp) {
    var topics = {};
    Binary.parse(resp)
        .word32bu('size')
        .word32bu('correlationId')
        .word32bu('topicNum')
        .loop(decodeTopics(decodePartitions));

    function decodePartitions (end, vars) {
        if (--vars.partitionNum === 0) end();
        topics[vars.topic] = topics[vars.topic] || {};
        this.word32bu('partition')
            .word64bu('offset')
            .word16bu('metadata')
            .tap(function (vars) {
                this.buffer('metadata', vars.metadata);
            })
            .word16bu('errorCode')
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
            .UInt32BE(topics.length);

    topics.forEach(function (topic) {
        request.UInt16BE(topic.length)
                .string(topic);

        var partitions = _.pairs(payloads[topic]).map(function (pairs) { return pairs[1] });
        request.UInt32BE(partitions.length);
        partitions.forEach(function (p) {
            request.UInt32BE(p.partition)
                    .DoubleBE(p.time)
                    .UInt32BE(p.maxNum);
        });
    });

    return encodeRequestWithLength(request.make());
}

function decodeOffsetResponse(resp) {
    var topics = {};
    Binary.parse(resp)
        .word32bu('size')
        .word32bu('correlationId')
        .word32bu('topicNum')
        .loop(decodeTopics(decodePartitions));

    function decodePartitions (end, vars) {
        if (--vars.partitionNum === 0) end();
        topics[vars.topic] = topics[vars.topic] || {};
        this.word32bu('partition')
            .word16bu('errorCode')
            .word32bu('offsetNum')
            .loop(decodeOffsets);
    }

    function decodeOffsets (end, vars) {
        if (--vars.offsetNum === 0) end();
        topics[vars.topic][vars.partition] = topics[vars.topic][vars.partition] || [];
        this.word64bu('offset')
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
