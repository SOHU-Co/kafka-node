﻿var util = require('util');
var _ = require('lodash');

'use strict';

var Partitioner = function () {
}

var DefaultPartitioner = function () {
}
util.inherits(DefaultPartitioner, Partitioner);

DefaultPartitioner.prototype.getPartition = function (partitions) {
    if (partitions && _.isArray(partitions) && partitions.length > 0) {
        return partitions[0];
    } else {
        return 0;
    }
}

var RandomPartitioner = function () {
}
util.inherits(RandomPartitioner, Partitioner);

RandomPartitioner.prototype.getPartition = function (partitions) {
    return partitions[Math.floor(Math.random() * partitions.length)];
}

var KeyedPartitioner = function () {
}
util.inherits(KeyedPartitioner, Partitioner);

// Taken from oid package (Dan Bornstein)
// Copyright The Obvious Corporation.
KeyedPartitioner.prototype.hashCode = function(string) {
    var hash = 0;
    var length = string.length;

    for (var i = 0; i < length; i++) {
        hash = ((hash * 31) + string.charCodeAt(i)) & 0x7fffffff;
    }

    return (hash === 0) ? 1 : hash;
}

KeyedPartitioner.prototype.getPartition = function (partitions, key) {
    key = key || ''

    var index = this.hashCode(key) % partitions.length;
    return partitions[index];
}

exports.DefaultPartitioner = DefaultPartitioner;
exports.RandomPartitioner = RandomPartitioner;
exports.KeyedPartitioner = KeyedPartitioner;