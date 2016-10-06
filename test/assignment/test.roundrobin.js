'use strict';

var roundRobin = require('../../lib/assignment/roundrobin');
var _ = require('lodash');
var should = require('should');

describe('Round Robin Assignment', function () {
  var topicPartition = {
    'RebalanceTopic': [
      '0',
      '1',
      '2'
    ],
    'RebalanceTest': [
      '0',
      '1',
      '2'
    ]
  };

  it('should have required fields', function () {
    roundRobin.assign.should.be.function;
    roundRobin.name.should.be.eql('roundrobin');
    roundRobin.version.should.be.eql(0);
  });

  it('should distribute two topics three partitions to two consumers ', function (done) {
    var groupMembers = [
      {
        'subscription': [
          'RebalanceTopic',
          'RebalanceTest'
        ],
        'version': 0,
        'id': 'consumer1'
      },
      {
        'subscription': [
          'RebalanceTopic',
          'RebalanceTest'
        ],
        'version': 0,
        'id': 'consumer2'
      }
    ];

    roundRobin.assign(topicPartition, groupMembers, function (error, assignment) {
      should(error).be.empty;
      var consumer1 = _.first(assignment);
      consumer1.memberId.should.eql('consumer1');
      Object.keys(consumer1.topicPartitions).should.eql(['RebalanceTopic', 'RebalanceTest']);
      consumer1.topicPartitions['RebalanceTest'].should.eql(['1']);
      consumer1.topicPartitions['RebalanceTopic'].should.eql(['0', '2']);

      var consumer2 = _.last(assignment);
      consumer2.memberId.should.eql('consumer2');
      Object.keys(consumer2.topicPartitions).should.eql(['RebalanceTopic', 'RebalanceTest']);
      consumer2.topicPartitions['RebalanceTest'].should.eql(['0', '2']);
      consumer2.topicPartitions['RebalanceTopic'].should.eql(['1']);
      done();
    });
  });

  it('should distribute two topics three partitions to three consumers', function (done) {
    var groupMembers = [
      {
        'subscription': [
          'RebalanceTopic',
          'RebalanceTest'
        ],
        'version': 0,
        'id': 'consumer1'
      },
      {
        'subscription': [
          'RebalanceTopic',
          'RebalanceTest'
        ],
        'version': 0,
        'id': 'consumer3'
      },
      {
        'subscription': [
          'RebalanceTopic',
          'RebalanceTest'
        ],
        'version': 0,
        'id': 'consumer2'
      }
    ];

    roundRobin.assign(topicPartition, groupMembers, function (error, assignment) {
      should(error).be.empty;
      assignment = _.sortBy(assignment, 'memberId');

      var consumer1 = _.first(assignment);
      consumer1.memberId.should.eql('consumer1');
      Object.keys(consumer1.topicPartitions).should.eql(['RebalanceTopic', 'RebalanceTest']);
      consumer1.topicPartitions['RebalanceTest'].should.eql(['0']);
      consumer1.topicPartitions['RebalanceTopic'].should.eql(['0']);

      var consumer2 = assignment[1];
      consumer2.memberId.should.eql('consumer2');
      Object.keys(consumer2.topicPartitions).should.eql(['RebalanceTopic', 'RebalanceTest']);
      consumer2.topicPartitions['RebalanceTest'].should.eql(['1']);
      consumer2.topicPartitions['RebalanceTopic'].should.eql(['1']);

      var consumer3 = _.last(assignment);
      consumer3.memberId.should.eql('consumer3');
      Object.keys(consumer3.topicPartitions).should.eql(['RebalanceTopic', 'RebalanceTest']);
      consumer3.topicPartitions['RebalanceTest'].should.eql(['2']);
      consumer3.topicPartitions['RebalanceTopic'].should.eql(['2']);
      done();
    });
  });

  it('should distribute two topics three partitions to four consumers', function (done) {
    var groupMembers = [
      {
        'subscription': [
          'RebalanceTopic',
          'RebalanceTest'
        ],
        'version': 0,
        'id': 'consumer1'
      },
      {
        'subscription': [
          'RebalanceTopic',
          'RebalanceTest'
        ],
        'version': 0,
        'id': 'consumer3'
      },
      {
        'subscription': [
          'RebalanceTopic',
          'RebalanceTest'
        ],
        'version': 0,
        'id': 'consumer2'
      },
      {
        'subscription': [
          'RebalanceTopic',
          'RebalanceTest'
        ],
        'version': 0,
        'id': 'consumer4'
      }
    ];

    roundRobin.assign(topicPartition, groupMembers, function (error, assignment) {
      should(error).be.empty;
      assignment = _.sortBy(assignment, 'memberId');

      var consumer1 = _.first(assignment);
      consumer1.memberId.should.eql('consumer1');
      Object.keys(consumer1.topicPartitions).should.eql(['RebalanceTopic', 'RebalanceTest']);
      consumer1.topicPartitions['RebalanceTest'].should.eql(['1']);
      consumer1.topicPartitions['RebalanceTopic'].should.eql(['0']);

      var consumer2 = assignment[1];
      consumer2.memberId.should.eql('consumer2');
      Object.keys(consumer2.topicPartitions).should.eql(['RebalanceTopic', 'RebalanceTest']);
      consumer2.topicPartitions['RebalanceTest'].should.eql(['2']);
      consumer2.topicPartitions['RebalanceTopic'].should.eql(['1']);

      var consumer3 = assignment[2];
      consumer3.memberId.should.eql('consumer3');
      Object.keys(consumer3.topicPartitions).should.eql(['RebalanceTopic']);
      consumer3.topicPartitions['RebalanceTopic'].should.eql(['2']);

      var consumer4 = _.last(assignment);
      consumer4.memberId.should.eql('consumer4');
      Object.keys(consumer4.topicPartitions).should.eql(['RebalanceTest']);
      done();
    });
  });
});
