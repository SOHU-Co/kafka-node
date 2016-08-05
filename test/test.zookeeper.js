'use strict';

var Zookeeper = require('../lib/zookeeper').Zookeeper;
var host = process.env['KAFKA_TEST_HOST'] || '';
var zk;

describe('Zookeeper', function () {
  before(function () {
    zk = new Zookeeper(host);
  });

  describe('when init success', function () {
    it('should emit init event', function (done) {
      zk.once('init', function (brokers) {
        Object.keys(brokers).length.should.eql(1);
        done();
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
});
