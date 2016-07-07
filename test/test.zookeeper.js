'use strict';

var libPath = process.env['kafka-cov'] ? '../lib-cov/' : '../lib/';
var Zookeeper = require(libPath + 'zookeeper').Zookeeper;
var host = process.env['KAFKA_TEST_HOST'] || '';
var zk;

/*
 *  To run the test, you should ensure:
 *   - at least 2 broker running
 */

xdescribe('Zookeeper', function () {
  before(function () {
    zk = new Zookeeper(host);
  });

  describe('when init success', function () {
    it('should emit init event', function (done) {
      var zk = new Zookeeper(host);
      zk.on('init', function (brokers) {
        Object.keys(brokers).length.should.above(1);
        done();
      });
    });
  });

  describe('#listBrokers', function () {
    describe('when client not init', function () {
      it('should return all brokers', function (done) {
        zk.inited = false;
        zk.listBrokers(function (brokers) {
          Object.keys(brokers).length.should.above(1);
          done();
        });
      });
    });

    describe('when client inited', function () {
      it('should return all brokers and emit brokersChanged event', function (done) {
        var count = 0;
        zk.inited = true;

        zk.listBrokers(function (brokers) {
          Object.keys(brokers).length.should.above(1);
          if (++count === 2) done();
        });

        zk.on('brokersChanged', function (brokers) {
          Object.keys(brokers).length.should.above(1);
          if (++count === 2) done();
        });
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
