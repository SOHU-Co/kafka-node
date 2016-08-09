'use strict';

var sinon = require('sinon');
var HighLevelConsumer = require('../lib/highLevelConsumer');
var FakeClient = require('./mocks/mockClient');
var should = require('should');
var InvalidConfigError = require('../lib/errors/InvalidConfigError');

describe('HighLevelConsumer', function () {
  describe('#close', function (done) {
    var client, consumer, leaveGroupStub, commitStub, clientCloseSpy;

    beforeEach(function () {
      client = new FakeClient();
      consumer = new HighLevelConsumer(client, [], {groupId: 'mygroup'});
      leaveGroupStub = sinon.stub(consumer, '_leaveGroup').yields();
      commitStub = sinon.stub(consumer, 'commit').yields();
      clientCloseSpy = sinon.spy(client, 'close');
    });

    it('should leave consumer group, commit and then close', function (done) {
      consumer.close(true, function (error) {
        consumer.closing.should.be.true;
        consumer.ready.should.be.false;

        sinon.assert.calledOnce(leaveGroupStub);
        sinon.assert.calledOnce(commitStub);
        sinon.assert.calledOnce(clientCloseSpy);
        sinon.assert.callOrder(leaveGroupStub, commitStub, clientCloseSpy);
        done(error);
      });
    });

    it('should leave consumer group, and then close', function (done) {
      consumer.close(false, function (error) {
        consumer.closing.should.be.true;
        consumer.ready.should.be.false;

        sinon.assert.calledOnce(leaveGroupStub);
        sinon.assert.calledOnce(clientCloseSpy);
        sinon.assert.callOrder(leaveGroupStub, clientCloseSpy);
        done(error);
      });
    });

    it('should leave consumer group, and then close (single callback argument)', function (done) {
      consumer.close(function (error) {
        consumer.closing.should.be.true;
        consumer.ready.should.be.false;

        sinon.assert.calledOnce(leaveGroupStub);
        sinon.assert.calledOnce(clientCloseSpy);
        sinon.assert.callOrder(leaveGroupStub, clientCloseSpy);
        done(error);
      });
    });
  });

  describe('#_leaveGroup', function () {
    var client, consumer, unregisterSpy, releasePartitionsStub;

    beforeEach(function () {
      client = new FakeClient();
      unregisterSpy = sinon.spy(client.zk, 'unregisterConsumer');
      consumer = new HighLevelConsumer(client, [], {groupId: 'mygroup'});
      releasePartitionsStub = sinon.stub(consumer, '_releasePartitions').yields();
    });

    it('should releases partitions and unregister it self', function (done) {
      consumer.topicPayloads = [{topic: 'fake-topic', partition: 0, offset: 0, maxBytes: 1048576, metadata: 'm'}];
      consumer._leaveGroup(function (error) {
        sinon.assert.calledOnce(unregisterSpy);
        sinon.assert.calledOnce(releasePartitionsStub);
        done(error);
      });
    });

    it('should only unregister it self', function (done) {
      consumer.topicPayloads = [];
      consumer._leaveGroup(function (error) {
        sinon.assert.notCalled(releasePartitionsStub);
        sinon.assert.calledOnce(unregisterSpy);
        done(error);
      });
    });
  });

  describe('validate groupId', function () {
    function validateThrowsInvalidConfigError (groupId) {
      should.throws(function () {
        var client = new FakeClient();
        // eslint-disable-next-line no-new
        new HighLevelConsumer(client, [ { topic: 'some_topic' } ], {groupId: groupId});
      }, InvalidConfigError);
    }

    function validateDoesNotThrowInvalidConfigError (groupId) {
      should.doesNotThrow(function () {
        var client = new FakeClient();
        // eslint-disable-next-line no-new
        new HighLevelConsumer(client, [ { topic: 'some_topic' } ], {groupId: groupId});
      });
    }

    it('should throws an error on invalid group IDs', function () {
      validateThrowsInvalidConfigError('myGroupId:12345');
      validateThrowsInvalidConfigError('myGroupId,12345');
      validateThrowsInvalidConfigError('myGroupId"12345"');
      validateThrowsInvalidConfigError('myGroupId?12345');
    });

    it('should not throw on valid group IDs', function () {
      validateDoesNotThrowInvalidConfigError('myGroupId.12345');
      validateDoesNotThrowInvalidConfigError('something_12345');
      validateDoesNotThrowInvalidConfigError('myGroupId-12345');
    });
  });

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

    describe('pending rebalances', function () {
      it('should initalize pending rebalances to zero', function () {
        highLevelConsumer.pendingRebalances.should.be.eql(0);
      });

      function verifyPendingRebalances (event, done) {
        sandbox.stub(client, 'refreshMetadata', function (topicNames, callback) {
          highLevelConsumer.rebalancing.should.be.true;
          highLevelConsumer.pendingRebalances.should.be.eql(0);
          event();
          highLevelConsumer.pendingRebalances.should.be.eql(1);
          setImmediate(callback);
        });
        client.emit('ready');
        highLevelConsumer.once('rebalanced', function () {
          sandbox.restore();
          highLevelConsumer.pendingRebalances.should.be.eql(1);
          highLevelConsumer.on('rebalanced', done);
        });
      }

      it('should queue brokersChanged events during a rebalance', function (done) {
        verifyPendingRebalances(function () {
          client.emit('brokersChanged');
        }, done);
      });
      it('should queue consumersChanged rebalance events during a rebalance', function (done) {
        verifyPendingRebalances(function () {
          client.zk.emit('consumersChanged');
        }, done);
      });
      it('should queue partitionsChanged rebalance events during a rebalance', function (done) {
        verifyPendingRebalances(function () {
          client.zk.emit('partitionsChanged');
        }, done);
      });
    });

    it('should emit rebalanced event and clear rebalancing flag only after offsets are updated', function (done) {
      client.emit('ready');

      sinon.stub(highLevelConsumer, 'rebalanceAttempt', function (oldTopicPayloads, cb) {
        highLevelConsumer.topicPayloads = [{topic: 'fake-topic', partition: 0, offset: 0, maxBytes: 1048576, metadata: 'm'}];
        cb();
      });

      // verify rebalancing is false until rebalance finishes
      var refreshMetadataStub = sandbox.stub(client, 'refreshMetadata', function (topicNames, cb) {
        highLevelConsumer.rebalancing.should.be.true;
        setImmediate(cb);
      });

      highLevelConsumer.on('registered', function () {
        var sendOffsetFetchRequestStub = sandbox.stub(client, 'sendOffsetFetchRequest', function (groupId, payloads, cb) {
          highLevelConsumer.rebalancing.should.be.true;
          // wait for the results
          setImmediate(function () {
            // verify again before the callback
            highLevelConsumer.rebalancing.should.be.true;
            cb(null, [{topic: 'fake-topic', partition: 0, offset: 0, maxBytes: 1048576, metadata: 'm'}]);
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

      sinon.stub(highLevelConsumer, 'rebalanceAttempt', function (oldTopicPayloads, cb) {
        highLevelConsumer.topicPayloads = [{topic: 'fake-topic', partition: 0, offset: 0, maxBytes: 1048576, metadata: 'm'}];
        cb();
      });

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

      sinon.stub(highLevelConsumer, 'rebalanceAttempt', function (oldTopicPayloads, cb) {
        highLevelConsumer.topicPayloads = [{topic: 'fake-topic', partition: 0, offset: 0, maxBytes: 1048576, metadata: 'm'}];
        cb();
      });

      var sendFetchRequestSpy = sandbox.spy(client, 'sendFetchRequest');
      var fetchSpy = sandbox.spy(highLevelConsumer, 'fetch');

      sandbox.stub(client, 'sendOffsetFetchRequest', function (groupId, payloads, cb) {
        highLevelConsumer.rebalancing.should.be.true;
        highLevelConsumer.ready = true;
        highLevelConsumer.paused = false;
        highLevelConsumer.emit('done', {});
        // wait for the results
        setImmediate(function () {
          // verify again before the callback
          highLevelConsumer.rebalancing.should.be.true;
          cb(null, [{topic: 'fake-topic', partition: 0, offset: 0, maxBytes: 1048576, metadata: 'm'}]);
        });
      });

      highLevelConsumer.on('registered', function () {
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
