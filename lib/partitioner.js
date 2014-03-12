var _ = require('lodash');

'use strict';

function DefaultPartitioner() {
    this.getPartition = function (partitions) {
        if (partitions && _.isArray(partitions) && partitions.length > 0) {
            return partitions[0];
        } else {
            return 0;
        }
    }
}

function RandomPartitioner() {
    this.getPartition = function (partitions) {
        return partitions[Math.floor(Math.random() * partitions.length)];
    }
}

function KeyedPartitioner() {
    // Taken from oid package (Dan Bornstein)
    // Copyright The Obvious Corporation.
    function hashCode(string) {
        var hash = 0;
        var length = string.length;

        for (var i = 0; i < length; i++) {
            hash = ((hash * 31) + string.charCodeAt(i)) & 0x7fffffff;
        }

        return (hash === 0) ? 1 : hash;
    }

    this.getPartition = function (partitions, key) {
        key = key || ''

        var index = hashCode(key) % partitions.length;
        return partitions[index];
    }
}

exports.DefaultPartitioner = DefaultPartitioner;
exports.RandomPartitioner = RandomPartitioner;
exports.KeyedPartitioner = KeyedPartitioner;