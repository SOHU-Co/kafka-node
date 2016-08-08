'use strict';

var Zookeeper = require('../lib/zookeeper').Zookeeper;
var host = process.env['KAFKA_TEST_HOST'] || '';
var zk;
var uuid = require('node-uuid');

// Helper method
function randomId () {
  return Math.floor(Math.random() * 10000);
}

describe('Zookeeper', function () {
  beforeEach(function () {
    zk = new Zookeeper(host);
  });

  afterEach(function () {
    zk.close();
  });

  describe('when init success', function () {
    it('should emit init event', function (done) {
      zk.once('init', function (brokers) {
        Object.keys(brokers).length.should.eql(1);
        done();
      });
    });
  });

  describe('#listPartitions', function () {
    function createTopicWithPartitions (topic, numberOfPartitions, cb) {
      var trans = zk.client.transaction().create('/brokers/topics/' + topic).create('/brokers/topics/' + topic + '/partitions');

      for (var i = 0; i < numberOfPartitions; i++) {
        trans.create('/brokers/topics/' + topic + '/partitions/' + i);
      }

      trans.commit(function (error, results) {
        if (error) {
          return cb(error);
        }
        cb(null, results);
      });
    }

    it('should trigger partitionsChanged event when partition is deleted', function (done) {
      var topic = uuid.v4();
      zk.on('partitionsChanged', done);
      createTopicWithPartitions(topic, 3, function (error) {
        if (error) {
          return done(error);
        }

        zk.listPartitions(topic);
        zk.client.remove('/brokers/topics/' + topic + '/partitions/1', function (error) {
          if (error) {
            done(error);
          }
        });
      });
    });

    it('should trigger partitionsChanged event when partition is added', function (done) {
      var topic = uuid.v4();
      zk.on('partitionsChanged', done);
      createTopicWithPartitions(topic, 3, function (error) {
        if (error) {
          return done(error);
        }

        zk.listPartitions(topic);
        zk.client.create('/brokers/topics/' + topic + '/partitions/3', function (error) {
          if (error) {
            done(error);
          }
        });
      });
    });
  });

  describe('#listBrokers', function () {
    describe('when client not init', function () {
      it('should return all brokers', function (done) {
        zk.inited = false;
        zk.listBrokers(function (brokers) {
          Object.keys(brokers).length.should.eql(1);
          done();
        });
      });
    });

    describe('when client inited', function () {
      it('should return all brokers and emit brokersChanged event', function (done) {
        var count = 0;
        zk.inited = true;

        zk.listBrokers(function (brokers) {
          Object.keys(brokers).length.should.eql(1);
          if (++count === 2) done();
        });

        zk.on('brokersChanged', function (brokers) {
          Object.keys(brokers).length.should.eql(1);
          if (++count === 2) done();
        });
      });
    });
  });

  describe('#unregisterConsumer', function () {
    it('removes the consumer ID node if it exists yields true if successful', function (done) {
      var groupId = 'awesomeFakeGroupId';
      var consumerId = 'fabulousConsumerId';
      zk.registerConsumer(groupId, consumerId, [{topic: 'fake-topic'}],
        function (error) {
          if (error) {
            return done(error);
          }

          zk.unregisterConsumer(groupId, consumerId, function (error, result) {
            result.should.be.true;
            done(error);
          });
        });
    });

    it('yields false if the consumer ID node does not exists', function (done) {
      zk.unregisterConsumer('fakeTestGroupId', 'fakeConsumerId', function (error, result) {
        result.should.be.false;
        done(error);
      });
    });
  });

  describe('#topicExists', function () {
    it('should return false when topic not exist', function (done) {
      zk.topicExists('_not_exist_topic_test', function (err, existed, topic) {
        existed.should.not.be.ok;
        topic.should.equal('_not_exist_topic_test');
        done(err);
      });
    });
  });

  describe('#getConsumersPerTopic', function () {
    it('gets all consumers per topic', function (done) {
      var groupId = 'myGroup' + randomId();
      var consumerId = 'myConsumer' + randomId();
      var topic = '_exist_topic_3_test';

      zk.registerConsumer(groupId, consumerId, [{topic: topic}],
        function (error) {
          if (error) {
            return done(error);
          }

          zk.getConsumersPerTopic(groupId, function (error, consumerPerTopic) {
            if (error) {
              return done(error);
            }

            Object.keys(consumerPerTopic.consumerTopicMap)[0].should.equal(consumerId);
            consumerPerTopic.consumerTopicMap[consumerId][0].should.equal(topic);

            Object.keys(consumerPerTopic.topicConsumerMap)[0].should.equal(topic);
            consumerPerTopic.topicConsumerMap[topic][0].should.equal(consumerId);

            Object.keys(consumerPerTopic.topicPartitionMap)[0].should.equal(topic);
            consumerPerTopic.topicPartitionMap[topic][0].should.equal('0');
            done();
          });
        });
    });
  });

  describe('#addPartitionOwnership', function () {
    it('it adds ownership', function (done) {
      var groupId = 'myGroup' + randomId();
      var consumerId = 'myConsumer' + randomId();
      var topic = '_exist_topic_3_test';

      zk.addPartitionOwnership(consumerId, groupId, topic, 0, function (error) {
        done(error);
      });
    });
  });

  describe('#checkPartitionOwnership', function () {
    it('it fails if the path does not exist', function (done) {
      var groupId = 'awesomeFakeGroupId';
      var consumerId = 'fabulousConsumerId';
      var topic = 'fake-topic';

      zk.checkPartitionOwnership(consumerId, groupId, topic, 0,
        function (error) {
          error.should.equal("Path wasn't created");
          done();
        });
    });

    it('it finds the ownership', function (done) {
      var groupId = 'myGroup' + randomId();
      var consumerId = 'myConsumer' + randomId();
      var topic = '_exist_topic_3_test';

      zk.addPartitionOwnership(consumerId, groupId, topic, 0, function (error) {
        if (error) {
          done(error);
        }

        zk.checkPartitionOwnership(consumerId, groupId, topic, 0,
          function (error) {
            done(error);
          });
      });
    });

    it('it does not find ownership for the wrong consumer', function (done) {
      var groupId = 'myGroup' + randomId();
      var consumerId = 'myConsumer' + randomId();
      var topic = '_exist_topic_3_test';

      zk.addPartitionOwnership(consumerId, groupId, topic, 0, function (error) {
        if (error) {
          done(error);
        }

        zk.checkPartitionOwnership('notMyConsumer', groupId, topic, 0,
          function (error) {
            error.should.equal('Consumer not registered notMyConsumer');
            done();
          });
      });
    });
  });
});
