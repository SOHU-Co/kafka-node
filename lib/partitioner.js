'use strict';

const util = require('util');
const _ = require('lodash');

function Partitioner () { }

function DefaultPartitioner () {
  Partitioner.call(this);
}
util.inherits(DefaultPartitioner, Partitioner);

DefaultPartitioner.prototype.getPartition = function (partitions) {
  if (partitions && _.isArray(partitions) && partitions.length > 0) {
    return partitions[0];
  } else {
    return 0;
  }
};

function CyclicPartitioner () {
  Partitioner.call(this);
  this.c = 0;
}
util.inherits(CyclicPartitioner, Partitioner);

CyclicPartitioner.prototype.getPartition = function (partitions) {
  if (_.isEmpty(partitions)) return 0;
  return partitions[this.c++ % partitions.length];
};

function RandomPartitioner () {
  Partitioner.call(this);
}
util.inherits(RandomPartitioner, Partitioner);

RandomPartitioner.prototype.getPartition = function (partitions) {
  return partitions[Math.floor(Math.random() * partitions.length)];
};

function KeyedPartitioner () {
  Partitioner.call(this);
}
util.inherits(KeyedPartitioner, Partitioner);

// Taken from oid package (Dan Bornstein)
// Copyright The Obvious Corporation.
KeyedPartitioner.prototype.hashCode = function (stringOrBuffer) {
  let hash = 0;
  if (stringOrBuffer) {
    const string = stringOrBuffer.toString();
    const length = string.length;

    for (let i = 0; i < length; i++) {
      hash = ((hash * 31) + string.charCodeAt(i)) & 0x7fffffff;
    }
  }

  return (hash === 0) ? 1 : hash;
};

KeyedPartitioner.prototype.getPartition = function (partitions, key) {
  key = key || '';

  const index = this.hashCode(key) % partitions.length;
  return partitions[index];
};

function CustomPartitioner (partitioner) {
  Partitioner.call(this);
  this.getPartition = partitioner;
}
util.inherits(CustomPartitioner, Partitioner);

module.exports.DefaultPartitioner = DefaultPartitioner;
module.exports.CyclicPartitioner = CyclicPartitioner;
module.exports.RandomPartitioner = RandomPartitioner;
module.exports.KeyedPartitioner = KeyedPartitioner;
module.exports.CustomPartitioner = CustomPartitioner;
