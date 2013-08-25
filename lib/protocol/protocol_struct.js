'use strict';

function createStruct() {
    var args = arguments[0];
    return function () {
        for (var i=0; i<args.length; i++) {
            this[args[i]] = arguments[i];
        }
    }
}

var KEYS = {
    FetchRequest: ['topic', 'partition', 'offset', 'maxBytes'],
    FetchResponse: ['topic', 'fetchPartitions'],
    OffsetCommitRequest: ['topic', 'partition', 'offset', 'metadata'],
    OffsetCommitResponse: [],
    TopicAndPartition: ['topic', 'partition'],
    PartitionMetadata: ['topic', 'partition', 'leader', 'replicas', 'isr'],
    Message: ['magic', 'attributes', 'key', 'value'],
    ProduceRequest: ['topic', 'partition', 'messages'],
    Request: ['payloads', 'encoder', 'decoder', 'callback']
}

Object.keys(KEYS).forEach(function (o) { exports[o] = createStruct(KEYS[o]) });
exports.KEYS = KEYS;
