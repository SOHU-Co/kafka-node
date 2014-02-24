'use strict';

function DefaultPartitioner() {
    this.getPartition = function () {
        return 0;
    }
}

function RandomPartitioner() {
    this.getPartition = function (partitions) {
        return partitions[Math.floor(Math.random() * partitions.length)];
    }
}

function KeyedPartitioner() {
    // taken from oid package (Dan Bornstein)
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