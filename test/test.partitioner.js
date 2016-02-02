'use strict';

var _ = require('lodash'),
    kafka = require('..'),
    DefaultPartitioner = kafka.DefaultPartitioner,
    RandomPartitioner = kafka.RandomPartitioner,
    CyclicPartitioner = kafka.CyclicPartitioner,
    KeyedPartitioner = kafka.KeyedPartitioner;

function getPartitions(partitioner, partitions, count) {
    var arr = [];
    for (var i = 0; i < count; i++) {
        arr.push(partitioner.getPartition(partitions));
    }
    return arr;
}

describe('Partitioner', function () {
    describe('DefaultPartitioner', function ()  {
        var partitioner = new DefaultPartitioner();

        describe('#getPartition', function () {
            it('should always return the first partition', function () {
              var partitions = _.uniq(getPartitions(partitioner, [0, 1], 100));
              partitions.should.have.length(1);
              partitions.should.include(0);
            });
        });
    });

    describe('RandomPartitioner', function ()  {
        var partitioner = new RandomPartitioner();

        describe('#getPartition', function () {
            it('should return partitions within the existing ones', function () {
                var partitions = _.uniq(getPartitions(partitioner, [0, 1], 100));
                partitions.should.have.length(2);
                partitions.should.include(0);
                partitions.should.include(1);
            });
        });
    });

    describe('CyclicPartitioner', function ()  {
        var partitioner = new CyclicPartitioner();

        describe('#getPartition', function () {
            it('should return partitions cycling throw the existing ones', function () {
                var partitions = getPartitions(partitioner, [0, 1, 2], 6);
                partitions.should.have.length(6);
                partitions[0].should.equal(0);
                partitions[1].should.equal(1);
                partitions[2].should.equal(2);
                partitions[3].should.equal(0);
                partitions[4].should.equal(1);
                partitions[5].should.equal(2);
            });
        });
    });

    describe('KeyedPartitioner', function ()  {
        var partitioner = new KeyedPartitioner();

        describe('#getPartition', function () {
            it('should return partitions based on a given key', function () {
                var partitions = [partitioner.getPartition([0, 1], '12345'), partitioner.getPartition([0, 1], '123')];
                partitions.should.have.length(2);
                partitions[0].should.equal(1);
                partitions[1].should.equal(0);
            });
        });
    });
});
