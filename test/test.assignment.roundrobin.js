'use strict';

var roundRobin = require('../lib/assignment/roundrobin');

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

  it('should distribute 2 topics 3 partitions to 2 consumers ', function (done) {
    var groupMembers = [
      {
        'subscription': [
          'RebalanceTopic',
          'RebalanceTest'
        ],
        'version': 0,
        'id': 'consumer1-17e0d02e-5017-4c67-9a68-01730fc1076d'
      },
      {
        'subscription': [
          'RebalanceTopic',
          'RebalanceTest'
        ],
        'version': 0,
        'id': 'consumer2-b69f7c71-a7bb-4690-87d5-2aa928a8567b'
      }
    ];

    roundRobin.assign(topicPartition, groupMembers, function (error, assignment) {
      assignment[0].memberId.should.eql('consumer1-17e0d02e-5017-4c67-9a68-01730fc1076d');
      assignment[0].topicPartitions[0].topic.should.eql('RebalanceTopic');
      assignment[0].topicPartitions[0].partition.should.eql('0');

      assignment[0].topicPartitions[1].topic.should.eql('RebalanceTopic');
      assignment[0].topicPartitions[1].partition.should.eql('2');

      assignment[0].topicPartitions[2].topic.should.eql('RebalanceTest');
      assignment[0].topicPartitions[2].partition.should.eql('1');

      assignment[1].memberId.should.eql('consumer2-b69f7c71-a7bb-4690-87d5-2aa928a8567b');
      assignment[1].topicPartitions[0].topic.should.eql('RebalanceTopic');
      assignment[1].topicPartitions[0].partition.should.eql('1');

      assignment[1].topicPartitions[1].topic.should.eql('RebalanceTest');
      assignment[1].topicPartitions[1].partition.should.eql('0');

      assignment[1].topicPartitions[2].topic.should.eql('RebalanceTest');
      assignment[1].topicPartitions[2].partition.should.eql('2');
      done(error);
    });
  });

  it('should evenly distribute 2 topics 3 partitions to 3 consumers', function (done) {
    var groupMembers = [
      {
        'subscription': [
          'RebalanceTopic',
          'RebalanceTest'
        ],
        'version': 0,
        'id': 'consumer1-17e0d02e-5017-4c67-9a68-01730fc1076d'
      },
      {
        'subscription': [
          'RebalanceTopic',
          'RebalanceTest'
        ],
        'version': 0,
        'id': 'consumer3-9c0dd101-ec2f-4b83-91da-d64899c4b5dd'
      },
      {
        'subscription': [
          'RebalanceTopic',
          'RebalanceTest'
        ],
        'version': 0,
        'id': 'consumer2-b69f7c71-a7bb-4690-87d5-2aa928a8567b'
      }
    ];

    roundRobin.assign(topicPartition, groupMembers, function (error, assignment) {
      assignment[0].memberId.should.eql('consumer1-17e0d02e-5017-4c67-9a68-01730fc1076d');
      assignment[0].topicPartitions[0].topic.should.eql('RebalanceTopic');
      assignment[0].topicPartitions[0].partition.should.eql('0');

      assignment[0].topicPartitions[1].topic.should.eql('RebalanceTest');
      assignment[0].topicPartitions[1].partition.should.eql('0');

      assignment[1].memberId.should.eql('consumer3-9c0dd101-ec2f-4b83-91da-d64899c4b5dd');
      assignment[1].topicPartitions[0].topic.should.eql('RebalanceTopic');
      assignment[1].topicPartitions[0].partition.should.eql('2');

      assignment[1].topicPartitions[1].topic.should.eql('RebalanceTest');
      assignment[1].topicPartitions[1].partition.should.eql('2');

      assignment[2].memberId.should.eql('consumer2-b69f7c71-a7bb-4690-87d5-2aa928a8567b');
      assignment[2].topicPartitions[0].topic.should.eql('RebalanceTopic');
      assignment[2].topicPartitions[0].partition.should.eql('1');

      assignment[2].topicPartitions[1].topic.should.eql('RebalanceTest');
      assignment[2].topicPartitions[1].partition.should.eql('1');

      done(error);
    });
  });
});
