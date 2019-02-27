'use strict';

const range = require('../../lib/assignment/range');
const _ = require('lodash');
const should = require('should');

describe('Range Assignment', function () {
  const topicPartition = {
    RebalanceTopic: ['0', '1', '2'],
    RebalanceTest: ['0', '1', '2']
  };

  const groupMembers = [
    {
      subscription: ['RebalanceTopic', 'RebalanceTest'],
      version: 0,
      id: 'consumer1'
    },
    {
      subscription: ['RebalanceTopic', 'RebalanceTest'],
      version: 0,
      id: 'consumer2'
    }
  ];

  it('should have required fields', function () {
    range.should.have.property('assign').which.is.a.Function;
    range.name.should.be.eql('range');
    range.version.should.be.eql(0);
  });

  it('should partition two topics of three partitions between two consumers', function (done) {
    range.assign(topicPartition, groupMembers, function (error, result) {
      should(error).be.empty;
      const consumer1 = _.head(result);
      consumer1.memberId.should.eql('consumer1');
      Object.keys(consumer1.topicPartitions).should.eql(['RebalanceTopic', 'RebalanceTest']);
      consumer1.topicPartitions['RebalanceTest'].should.eql([0, 1]);
      consumer1.topicPartitions['RebalanceTopic'].should.eql([0, 1]);

      const consumer2 = _.last(result);
      consumer2.memberId.should.eql('consumer2');
      Object.keys(consumer2.topicPartitions).should.eql(['RebalanceTopic', 'RebalanceTest']);
      consumer2.topicPartitions['RebalanceTest'].should.eql([2]);
      consumer2.topicPartitions['RebalanceTopic'].should.eql([2]);

      done();
    });
  });

  it('should partition two topics of three partitions between three consumers', function (done) {
    const gm = groupMembers.slice(0);
    gm.push({
      subscription: ['RebalanceTopic', 'RebalanceTest'],
      version: 0,
      id: 'consumer3'
    });

    range.assign(topicPartition, gm, function (error, result) {
      should(error).be.empty;
      const consumer1 = _.head(result);
      consumer1.memberId.should.eql('consumer1');
      Object.keys(consumer1.topicPartitions).should.eql(['RebalanceTopic', 'RebalanceTest']);
      consumer1.topicPartitions['RebalanceTest'].should.eql([0]);
      consumer1.topicPartitions['RebalanceTopic'].should.eql([0]);

      const consumer2 = result[1];
      consumer2.memberId.should.eql('consumer2');
      Object.keys(consumer2.topicPartitions).should.eql(['RebalanceTopic', 'RebalanceTest']);
      consumer2.topicPartitions['RebalanceTest'].should.eql([1]);
      consumer2.topicPartitions['RebalanceTopic'].should.eql([1]);

      const consumer3 = _.last(result);
      consumer3.memberId.should.eql('consumer3');
      Object.keys(consumer3.topicPartitions).should.eql(['RebalanceTopic', 'RebalanceTest']);
      consumer3.topicPartitions['RebalanceTest'].should.eql([2]);
      consumer3.topicPartitions['RebalanceTopic'].should.eql([2]);

      done();
    });
  });

  it('should partition two topics of three partitions between four consumers', function (done) {
    const gm = groupMembers.slice(0);
    gm.push(
      {
        subscription: ['RebalanceTopic', 'RebalanceTest'],
        version: 0,
        id: 'consumer3'
      },
      {
        subscription: ['RebalanceTopic', 'RebalanceTest'],
        version: 0,
        id: 'consumer4'
      }
    );

    range.assign(topicPartition, gm, function (error, result) {
      should(error).be.empty;
      const consumer1 = _.head(result);
      consumer1.memberId.should.eql('consumer1');
      Object.keys(consumer1.topicPartitions).should.eql(['RebalanceTopic', 'RebalanceTest']);
      consumer1.topicPartitions['RebalanceTest'].should.eql([0]);
      consumer1.topicPartitions['RebalanceTopic'].should.eql([0]);

      const consumer2 = result[1];
      consumer2.memberId.should.eql('consumer2');
      Object.keys(consumer2.topicPartitions).should.eql(['RebalanceTopic', 'RebalanceTest']);
      consumer2.topicPartitions['RebalanceTest'].should.eql([1]);
      consumer2.topicPartitions['RebalanceTopic'].should.eql([1]);

      const consumer4 = _.last(result);
      consumer4.memberId.should.eql('consumer4');
      Object.keys(consumer4.topicPartitions).should.eql([]);
      done();
    });
  });
});
