'use strict';
var util = require('util');
var EventEmitter = require('events');
var sinon = require('sinon');

var libPath = '../lib/';

var HighLevelConsumer = require(libPath + 'highLevelConsumer');

function FakeZookeeper () {
  EventEmitter.call(this);

  this.checkPartitionOwnership = function (id, groupId, topic, partition, cb) {
    setImmediate(cb);
  };
  this.deletePartitionOwnership = function (groupId, topic, partition, cb) {
    setImmediate(cb);
  };
  this.addPartitionOwnership = function (id, groupId, topic, partition, cb) {
    setImmediate(cb);
  };
  this.registerConsumer = function (groupId, id, payloads, cb) {
    setImmediate(cb);
  };
  this.listConsumers = function (groupId) {};
  this.getConsumersPerTopic = function (groupId, cb) {
    var consumerTopicMap = {};
    consumerTopicMap[groupId] = [ 'fake-topic' ];
    var topicConsumerMap = { 'fake-topic': [ groupId ] };
    var topicPartitionMap = { 'fake-topic': [ '0', '1', '2' ] };
    var map = {
      consumerTopicMap: consumerTopicMap,
      topicConsumerMap: topicConsumerMap,
      topicPartitionMap: topicPartitionMap
    };

    setImmediate(cb, null, map);
  };
}
util.inherits(FakeZookeeper, EventEmitter);

function FakeClient () {
  EventEmitter.call(this);

  this.zk = new FakeZookeeper();

  this.topicExists = function (topics, cb) {
    setImmediate(cb);
  };
  this.refreshMetadata = function (topicNames, cb) {
    setImmediate(cb);
  };
  this.sendOffsetCommitRequest = function (groupId, commits, cb) {
    setImmediate(cb);
  };
  this.sendFetchRequest = function (consumer, payloads, fetchMaxWaitMs, fetchMinBytes, maxTickMessages) {};
  this.sendOffsetFetchRequest = function (groupId, payloads, cb) {
    setImmediate(cb);
  };
  this.sendOffsetRequest = function (payloads, cb) {
    setImmediate(cb);
  };
  this.addTopics = function (topics, cb) {
    setImmediate(cb);
  };
  this.removeTopicMetadata = function (topics, cb) {
    setImmediate(cb);
  };
  this.close = function (cb) {
    setImmediate(cb);
  };
}
util.inherits(FakeClient, EventEmitter);

describe('HighLevelConsumer', function () {
  describe('rebalance', function () {
    var client,
      highLevelConsumer,
      sandbox;

    beforeEach(function () {
      client = new FakeClient();

      highLevelConsumer = new HighLevelConsumer(
        client,
        [ {topic: 'fake-topic'} ]
      );

      clearTimeout(highLevelConsumer.checkPartitionOwnershipInterval);

      sandbox = sinon.sandbox.create();
    });

    afterEach(function () {
      highLevelConsumer.close(function () {});
      sandbox.restore();
      client = null;
      highLevelConsumer = null;
    });

    it('should emit rebalanced event and clear rebalancing flag only after offsets are updated', function (done) {
      client.emit('ready');

      highLevelConsumer.on('registered', function () {
        // verify rebalancing is false until rebalance finishes
        var refreshMetadataStub = sandbox.stub(client, 'refreshMetadata', function (topicNames, cb) {
          highLevelConsumer.rebalancing.should.be.true;
          setImmediate(cb);
        });

        var sendOffsetFetchRequestStub = sandbox.stub(client, 'sendOffsetFetchRequest', function (groupId, payloads, cb) {
          highLevelConsumer.rebalancing.should.be.true;
          // wait for the results
          setImmediate(function () {
            // verify again before the callback
            highLevelConsumer.rebalancing.should.be.true;
            cb();
          });
        });

        highLevelConsumer.on('rebalanced', function () {
          refreshMetadataStub.calledOnce.should.be.true;
          sendOffsetFetchRequestStub.calledOnce.should.be.true;
          sinon.assert.callOrder(refreshMetadataStub, sendOffsetFetchRequestStub);
          highLevelConsumer.rebalancing.should.be.false;
          done();
        });
      });

      highLevelConsumer.on('error', function (err) {
        done(err);
      });
    });

    it('should emit error and clear rebalancing flag if fetchOffset failed', function (done) {
      client.emit('ready');

      highLevelConsumer.on('registered', function () {
        sandbox.stub(client, 'sendOffsetFetchRequest', function (groupId, payloads, cb) {
          setImmediate(cb, new Error('Fetching offset failed'));
        });

        highLevelConsumer.on('rebalanced', function () {
          done(new Error('rebalance is not expected to succeed'));
        });
      });

      highLevelConsumer.on('error', function (err) {
        if (err.name === 'FailedToRebalanceConsumerError' && err.message === 'Fetching offset failed') {
          done();
        } else {
          done(err);
        }
      });
    });

    it('should ignore fetch calls from "done" event handler during rebalance', function (done) {
      client.emit('ready');

      var sendFetchRequestSpy = sandbox.spy(client, 'sendFetchRequest');
      var fetchSpy = sandbox.spy(highLevelConsumer, 'fetch');

      highLevelConsumer.on('registered', function () {
        client.sendOffsetFetchRequest = function (groupId, payloads, cb) {
          // simulate a done event before offset fetch returns
          highLevelConsumer.ready = true;
          highLevelConsumer.paused = false;
          highLevelConsumer.emit('done', {});

          setTimeout(function () {
            cb();
          }, 100);
        };

        highLevelConsumer.on('rebalanced', function () {
          if (fetchSpy.callCount !== 2) {
            done(fetchSpy.callCount.should.equal(2));
            return;
          }

          if (!sendFetchRequestSpy.calledOnce) {
            done(new Error('client.sendFetchRequest expected to be called only once'));
            return;
          }

          done();
        });
      });

      highLevelConsumer.on('error', function (err) {
        done(err);
      });
    });
  });
});
