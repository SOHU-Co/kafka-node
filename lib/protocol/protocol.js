'use strict';

var Binary = require('binary'),
    Buffermaker = require('buffermaker'),
    _  = require('lodash'),
    crc32 = require('buffer-crc32'),
    protocol = require('./protocol_struct'),
    getCodec = require('../codec'),
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

function decodeFetchResponse(cb, maxTickMessages) {
    return function (resp) {
        return _decodeFetchResponse(resp, cb, maxTickMessages);
    }
}

function _decodeFetchResponse(resp, cb, maxTickMessages) {
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
                    cb({ topic: vars.topic, partition: vars.partition, message:  ERROR_CODE[vars.errorCode] });
                var messageSet = decodeMessageSet(vars.topic, vars.partition, vars.messageSet, cb, maxTickMessages);
                if (messageSet.length) {
                    var offset = messageSet[messageSet.length-1];
                    topics[vars.topic][vars.partition] = offset;
                }
            });
    }
    cb && cb(null, 'done', topics);
}

function decodeMessageSet(topic, partition, messageSet, cb, maxTickMessages) {
    var set = [];
    var messageCount = 0;
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
                    codec.decode(vars.value, function(error, inlineMessageSet) {
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
    var brokers = {}, out = {}, topics = {}, errors = [];
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
                if (vars.topicError !== 0)
                    return errors.push(ERROR_CODE[vars.topicError]);
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
                if (vars.errorCode ==0 || vars.errorCode ==9)
                    topics[vars.topic][vars.partition] =
                        new PartitionMetadata(vars.topic, vars.partition, vars.leader, vars.replicas, vars.isr);
                else
                    errors.push(ERROR_CODE[vars.errorCode]);

            });
    }

    if (!_.isEmpty(errors)) out.error = errors;
    out.metadata = topics;
    return [brokers, out];
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
        .string(message.key);
    var value = message.value;

    if (value !== null) {
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

function decodeProduceResponse(resp) {
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
            .word64bs('offset')
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
                .Int64BE(p.time)
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
        if (--vars.offsetNum <= 0) end();
        topics[vars.topic][vars.partition] = topics[vars.topic][vars.partition] || [];
        this.word64bs('offset')
            .tap(function (vars) {
                if (vars.offset != null) topics[vars.topic][vars.partition].push(vars.offset);
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
exports.encodeMessageSet = encodeMessageSet;
