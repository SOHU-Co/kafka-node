'use strict';

var sinon = require('sinon');
var HighLevelConsumer = require('../lib/highLevelConsumer');
var FakeClient = require('./mocks/mockClient');
var should = require('should');
var InvalidConfigError = require('../lib/errors/InvalidConfigError');
var FailedToRegisterConsumerError = require('../lib/errors/FailedToRegisterConsumerError');

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
      clearInterval(consumer.checkPartitionOwnershipInterval);
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
    var clock;

    beforeEach(function () {
      clock = sinon.useFakeTimers();
    });

    afterEach(function () {
      clock.restore();
    });

    function validateThrowsInvalidConfigError (groupId) {
      var consumer;
      should.throws(function () {
        var client = new FakeClient();
        consumer = new HighLevelConsumer(client, [ { topic: 'some_topic' } ], {groupId: groupId});
      }, InvalidConfigError);
      consumer && consumer.close();
    }

    function validateDoesNotThrowInvalidConfigError (groupId) {
      var consumer;
      should.doesNotThrow(function () {
        var client = new FakeClient();
        consumer = new HighLevelConsumer(client, [ { topic: 'some_topic' } ], {groupId: groupId});
      });
      consumer.close();
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

  describe('Reregister consumer on zookeeper reconnection', function () {
    var client, consumer, sandbox;
    var async = require('async');

    beforeEach(function () {
      client = new FakeClient();

      consumer = new HighLevelConsumer(
        client,
        [ {topic: 'fake-topic'} ],
        {groupId: 'zkReconnect-Test'}
      );

      sandbox = sinon.sandbox.create();
    });

    afterEach(function () {
      consumer.close();
      sandbox.restore();
      consumer = null;
    });

    it('should try to register the consumer and emit error on failure', function (done) {
      sandbox.stub(consumer, 'registerConsumer').yields(new Error('failed'));
      consumer.once('error', function (error) {
        error.should.be.an.instanceOf(FailedToRegisterConsumerError);
        error.message.should.be.eql('Failed to register consumer on zkReconnect');
        done();
      });
      client.emit('zkReconnect');
    });

    it('should register the consumer and emit registered on success', function (done) {
      sandbox.stub(consumer, 'registerConsumer').yields(null);
      async.parallel([
        function (callback) {
          consumer.once('registered', callback);
        },
        function (callback) {
          consumer.once('rebalancing', callback);
        }
      ], function () {
        sinon.assert.calledOnce(consumer.registerConsumer);
        done();
      });
      client.emit('zkReconnect');
    });
  });

  describe('#setOffset', function () {
    var client, highLevelConsumer;

    beforeEach(function () {
      client = new FakeClient();

      highLevelConsumer = new HighLevelConsumer(
        client,
        [ {topic: 'fake-topic'} ]
      );

      clearTimeout(highLevelConsumer.checkPartitionOwnershipInterval);
      highLevelConsumer.topicPayloads = [
        {topic: 'fake-topic', partition: '0', offset: 0, maxBytes: 1048576, metadata: 'm'},
        {topic: 'fake-topic', partition: '1', offset: 0, maxBytes: 1048576, metadata: 'm'}
      ];
    });

    it('should setOffset correctly given partition is a string', function () {
      highLevelConsumer.setOffset('fake-topic', '0', 86);
      highLevelConsumer.topicPayloads[0].offset.should.be.eql(86);

      highLevelConsumer.setOffset('fake-topic', '1', 23);
      highLevelConsumer.topicPayloads[1].offset.should.be.eql(23);
    });

    it('should setOffset correctly if given partition is a number', function () {
      highLevelConsumer.setOffset('fake-topic', 0, 67);
      highLevelConsumer.topicPayloads[0].offset.should.be.eql(67);

      highLevelConsumer.setOffset('fake-topic', 1, 98);
      highLevelConsumer.topicPayloads[1].offset.should.be.eql(98);
    });
  });

  describe('ensure partition ownership and registration', function () {
    var client, consumer, sandbox;
    var twentySeconds = 20000;

    beforeEach(function () {
      sandbox = sinon.sandbox.create();
      sandbox.useFakeTimers();
      client = new FakeClient();

      consumer = new HighLevelConsumer(
        client,
        [ {topic: 'fake-topic'} ]
      );
    });

    afterEach(function () {
      consumer.close();
      sandbox.restore();
      client = null;
      consumer = null;
    });

    it('should emit an FailedToRegisterConsumerError when ownership changes', function (done) {
      consumer.topicPayloads = [
        {topic: 'fake-topic', partition: '0', offset: 0, maxBytes: 1048576, metadata: 'm'},
        {topic: 'fake-topic', partition: '1', offset: 0, maxBytes: 1048576, metadata: 'm'}
      ];

      var checkPartitionOwnershipStub = sandbox.stub(client.zk, 'checkPartitionOwnership');

      checkPartitionOwnershipStub.withArgs(consumer.id, consumer.options.groupId, consumer.topicPayloads[0].topic,
        consumer.topicPayloads[0].partition).yields(new Error('not owned'));

      consumer.on('error', function (error) {
        error.should.be.an.instanceOf(FailedToRegisterConsumerError);
        error.message.should.be.eql('Error: not owned');
        sinon.assert.calledOnce(checkPartitionOwnershipStub);
        done();
      });
      sandbox.clock.tick(twentySeconds);
    });

    it('should emit an FailedToRegisterConsumerError when no longer registered', function (done) {
      consumer.topicPayloads = [
        {topic: 'fake-topic', partition: '0', offset: 0, maxBytes: 1048576, metadata: 'm'},
        {topic: 'fake-topic', partition: '1', offset: 0, maxBytes: 1048576, metadata: 'm'}
      ];

      sandbox.stub(client.zk, 'checkPartitionOwnership').yields();
      sandbox.stub(client.zk, 'isConsumerRegistered').yields(null, false);

      consumer.on('error', function (error) {
        error.should.be.an.instanceOf(FailedToRegisterConsumerError);
        error.message.should.be.eql('Error: Consumer ' + consumer.id + ' is not registered in group kafka-node-group');
        sinon.assert.calledOnce(client.zk.isConsumerRegistered);
        done();
      });
      sandbox.clock.tick(twentySeconds);
    });

    it('should emit an FailedToRegisterConsumerError when registered check fails', function (done) {
      consumer.topicPayloads = [
        {topic: 'fake-topic', partition: '0', offset: 0, maxBytes: 1048576, metadata: 'm'},
        {topic: 'fake-topic', partition: '1', offset: 0, maxBytes: 1048576, metadata: 'm'}
      ];

      sandbox.stub(client.zk, 'checkPartitionOwnership').yields();
      sandbox.stub(client.zk, 'isConsumerRegistered').yields(new Error('CONNECTION_LOSS[-4]'));

      consumer.on('error', function (error) {
        error.should.be.an.instanceOf(FailedToRegisterConsumerError);
        error.nested.should.be.an.instanceOf(Error);
        error.nested.message.should.be.eql('CONNECTION_LOSS[-4]');
        sinon.assert.calledOnce(client.zk.isConsumerRegistered);
        sinon.assert.calledTwice(client.zk.checkPartitionOwnership);
        done();
      });
      sandbox.clock.tick(twentySeconds);
    });

    it('should not emit an error if partition ownership checks succeeds', function (done) {
      consumer.topicPayloads = [
        {topic: 'fake-topic', partition: '0', offset: 0, maxBytes: 1048576, metadata: 'm'},
        {topic: 'fake-topic', partition: '1', offset: 0, maxBytes: 1048576, metadata: 'm'}
      ];

      var checkPartitionOwnershipStub = sandbox.stub(client.zk, 'checkPartitionOwnership').yields();
      sandbox.stub(client.zk, 'isConsumerRegistered').yields(null, true);

      sandbox.clock.tick(twentySeconds);
      sandbox.clock.restore();
      setImmediate(function () {
        sinon.assert.calledTwice(checkPartitionOwnershipStub);
        done();
      });
    });
  });

  describe('Verify no duplicate messages are being consumed', function () {
    this.timeout(26000);
    var _ = require('lodash');
    var Client = require('../lib/Client');
    var Producer = require('../lib/Producer');
    var uuid = require('node-uuid');
    var host = process.env['KAFKA_TEST_HOST'] || '';
    var topic = 'DuplicateMessageTest';
    var numberOfMessages = 20000;

    var highLevelConsumer;

    function sendUUIDMessages (times, topic, done) {
      var client = new Client(host, uuid.v4());
      var producer = new Producer(client, { requireAcks: 1 });

      producer.on('ready', function () {
        var messages = _.times(times, uuid.v4);
        producer.send([{topic: topic, messages: messages}], done);
      });
    }

    beforeEach(function (done) {
      sendUUIDMessages(numberOfMessages, topic, done);
    });

    afterEach(function (done) {
      highLevelConsumer.close(true, done);
    });

    [1 * 1024, 10 * 1024, 11 * 1024, 12 * 1024, 12300, 12350, 12370, 12371, 12372, 12373, 12375, 12385, 12400, 12500, 12.5 * 1024,
      13 * 1024, 15 * 1024, 20 * 1024, 50 * 1024, 100 * 1024, 1024 * 1024].forEach(verifyNoDupes);

    function verifyNoDupes (fetchMaxBytes) {
      it('should not receive duplicate messages for ' + numberOfMessages + ' messages using fetchMaxBytes: ' + fetchMaxBytes, function (done) {
        var client = new Client(host, uuid.v4());
        highLevelConsumer = new HighLevelConsumer(client, [ {topic: topic} ], {fetchMaxWaitMs: 10, fetchMaxBytes: fetchMaxBytes});
        var map = Object.create(null);
        var count = 0;

        highLevelConsumer.on('message', function (message) {
          if (map[message.value]) {
            done('duplicate message');
            return;
          }
          map[message.value] = true;

          if (++count === numberOfMessages) {
            done();
          }
        });
      });
    }
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
