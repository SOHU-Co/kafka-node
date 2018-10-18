'use strict';

const sinon = require('sinon');
const should = require('should');
const ConsumerGroup = require('../lib/consumerGroup');
const sendMessage = require('./helpers/sendMessage');
const _ = require('lodash');
const host = process.env['KAFKA_TEST_HOST'] || '';
const proxyquire = require('proxyquire').noCallThru();
const EventEmitter = require('events').EventEmitter;

const uuid = require('uuid');
const async = require('async');
const BrokerWrapper = require('../lib/wrapper/BrokerWrapper');
const FakeSocket = require('./mocks/mockSocket');

describe('ConsumerGroup', function () {
  describe('#constructor', function () {
    var ConsumerGroup;
    var fakeClient = sinon.stub().returns(new EventEmitter());

    beforeEach(function () {
      ConsumerGroup = proxyquire('../lib/consumerGroup', {
        './client': fakeClient
      });
    });

    it('should pass batch ConsumerGroup option to Client', function () {
      const batch = {
        noAckBatchAge: 10000
      };

      // eslint-disable-next-line no-new
      new ConsumerGroup(
        {
          host: 'myhost',
          id: 'myClientId',
          batch: batch,
          connectOnReady: false
        },
        'SampleTopic'
      );

      sinon.assert.calledWithExactly(fakeClient, 'myhost', 'myClientId', undefined, batch, undefined);
    });

    it('should pass zookeeper ConsumerGroup option to Client', function () {
      const zkOptions = {
        sessionTimeout: 10000
      };

      // eslint-disable-next-line no-new
      new ConsumerGroup(
        {
          host: 'myhost',
          id: 'myClientId',
          zk: zkOptions,
          connectOnReady: false
        },
        'SampleTopic'
      );

      sinon.assert.calledWithExactly(fakeClient, 'myhost', 'myClientId', zkOptions, undefined, undefined);
    });

    it('should setup SSL ConsumerGroup option ssl is true', function () {
      // eslint-disable-next-line no-new
      new ConsumerGroup(
        {
          host: 'myhost',
          id: 'myClientId',
          ssl: true,
          connectOnReady: false
        },
        'SampleTopic'
      );
      sinon.assert.calledWithExactly(fakeClient, 'myhost', 'myClientId', undefined, undefined, {});
    });

    it('should pass SSL client options through ConsumerGroup option', function () {
      const ssl = { rejectUnauthorized: false };
      // eslint-disable-next-line no-new
      new ConsumerGroup(
        {
          host: 'myhost',
          id: 'myClientId',
          ssl: ssl,
          connectOnReady: false
        },
        'SampleTopic'
      );
      sinon.assert.calledWithExactly(fakeClient, 'myhost', 'myClientId', undefined, undefined, ssl);
    });

    it('should throw an error if using an invalid outOfRangeOffset', function () {
      [true, false, '', 0, 1, 'blah'].forEach(offset => {
        should.throws(() => {
          // eslint-disable-next-line no-new
          new ConsumerGroup({
            outOfRangeOffset: offset
          });
        });
      });
    });

    it('should not throw an error if using an valid outOfRangeOffset', function () {
      ['earliest', 'latest', 'none'].forEach(offset => {
        should.doesNotThrow(() => {
          // eslint-disable-next-line no-new
          new ConsumerGroup(
            {
              outOfRangeOffset: offset,
              connectOnReady: false
            },
            'TestTopic'
          );
        });
      });
    });

    it('should throw an error if using an invalid fromOffset', function () {
      [true, false, '', 0, 1, 'blah'].forEach(offset => {
        should.throws(() => {
          // eslint-disable-next-line no-new
          new ConsumerGroup({
            fromOffset: offset
          });
        });
      });
    });

    it('should not throw an error if using an valid fromOffset', function () {
      ['earliest', 'latest', 'none'].forEach(offset => {
        should.doesNotThrow(() => {
          // eslint-disable-next-line no-new
          new ConsumerGroup(
            {
              fromOffset: offset,
              connectOnReady: false
            },
            'TestTopic'
          );
        });
      });
    });
  });

  describe('Topic partition change detection', function () {
    let ConsumerGroup = null;
    let consumerGroup = null;
    let sandbox = null;

    const fakeClient = new EventEmitter();
    fakeClient.loadMetadataForTopics = function () {};

    const FakeClient = function () {
      return fakeClient;
    };

    beforeEach(function () {
      sandbox = sinon.sandbox.create();
      ConsumerGroup = proxyquire('../lib/consumerGroup', {
        './client': FakeClient
      });

      consumerGroup = new ConsumerGroup(
        {
          host: 'gibberish',
          connectOnReady: false
        },
        'TestTopic'
      );
    });

    afterEach(function () {
      sandbox.restore();
    });

    describe('#scheduleTopicPartitionCheck', function () {
      let clock;
      beforeEach(function () {
        clock = sandbox.useFakeTimers();
      });

      it('should only have one schedule pending', function () {
        const cgMock = sandbox.mock(consumerGroup);
        consumerGroup.isLeader = true;
        cgMock
          .expects('_checkTopicPartitionChange')
          .once()
          .yields(null, true);
        cgMock.expects('commit').never();
        cgMock
          .expects('leaveGroup')
          .once()
          .yields(null);
        cgMock.expects('connect').once();

        consumerGroup.scheduleTopicPartitionCheck();
        consumerGroup.scheduleTopicPartitionCheck();

        clock.tick(30000);

        cgMock.verify();
      });

      it('should only schedule a check if consumer is a leader', function () {
        const cgMock = sandbox.mock(consumerGroup);

        consumerGroup.isLeader = false;

        cgMock.expects('_checkTopicPartitionChange').never();
        cgMock.expects('leaveGroup').never();
        cgMock.expects('connect').never();
        cgMock.expects('commit').never();

        consumerGroup.scheduleTopicPartitionCheck();
        clock.tick(30000);
        cgMock.verify();
      });
    });

    describe('#_checkTopicPartitionChange', function () {
      it('should yield false when the topic/partition length are the same', function (done) {
        sandbox.stub(consumerGroup.client, 'loadMetadataForTopics').yields(null, [
          0,
          {
            metadata: {
              aTopic: {
                '0': {},
                '1': {}
              },
              'foo.bar.topic': {
                '0': {},
                '1': {}
              },
              existingTopic: {
                '0': {},
                '1': {},
                '2': {}
              }
            }
          }
        ]);

        consumerGroup.topicPartitionLength = {
          aTopic: 2,
          'foo.bar.topic': 2,
          existingTopic: 3
        };

        consumerGroup.topics = ['aTopic', 'existingTopic', 'foo.bar.topic'];

        consumerGroup._checkTopicPartitionChange(function (error, changed) {
          sinon.assert.calledOnce(consumerGroup.client.loadMetadataForTopics);
          should(changed).be.false;
          done(error);
        });
      });

      it('should yield true when the topic/partition length are different', function (done) {
        sandbox.stub(consumerGroup.client, 'loadMetadataForTopics').yields(null, [
          0,
          {
            metadata: {
              'non.existant.topic': {
                '0': {},
                '1': {}
              },
              existingTopic: {
                '0': {},
                '1': {},
                '2': {}
              }
            }
          }
        ]);

        consumerGroup.topicPartitionLength = {
          'non.existant.topic': 0,
          existingTopic: 3
        };

        consumerGroup.topics = ['existingTopic', 'non.existant.topic'];

        consumerGroup._checkTopicPartitionChange(function (error, changed) {
          sinon.assert.calledOnce(consumerGroup.client.loadMetadataForTopics);
          should(changed).be.true;
          done(error);
        });
      });
    });
  });

  describe('Broker offline recovery', function () {
    let sandbox = null;
    let consumerGroup = null;
    let fakeClient = null;
    let ConsumerGroup = null;
    let clock = null;

    fakeClient = function () {
      return new EventEmitter();
    };

    before(function () {
      ConsumerGroup = proxyquire('../lib/consumerGroup', {
        './client': fakeClient
      });

      consumerGroup = new ConsumerGroup(
        {
          host: 'gibberish',
          connectOnReady: false
        },
        'TestTopic'
      );
    });

    beforeEach(function () {
      sandbox = sinon.sandbox.create();

      consumerGroup.client.refreshMetadata = sandbox.stub().yields(null);
      clock = sandbox.useFakeTimers();
    });

    afterEach(function () {
      sandbox.restore();
    });

    it('should refreshMetadata and connect when broker changes', function () {
      sandbox.stub(consumerGroup, 'connect');
      sandbox.spy(consumerGroup, 'pause');

      consumerGroup.ready = false;
      consumerGroup.reconnectTimer = null;
      consumerGroup.connecting = undefined;

      consumerGroup.client.emit('brokersChanged');
      clock.tick(50);
      consumerGroup.client.emit('brokersChanged');
      clock.tick(100);
      consumerGroup.client.emit('brokersChanged');
      clock.tick(200);

      sinon.assert.calledThrice(consumerGroup.pause);
      sinon.assert.calledOnce(consumerGroup.client.refreshMetadata);
      sinon.assert.calledOnce(consumerGroup.connect);
    });

    it('should not try to connect when broker changes and already connected', function () {
      sandbox.stub(consumerGroup, 'connect');
      sandbox.spy(consumerGroup, 'pause');
      sandbox.stub(consumerGroup, 'fetch');

      consumerGroup.ready = true;
      consumerGroup.reconnectTimer = null;
      consumerGroup.connecting = undefined;

      consumerGroup.client.emit('brokersChanged');
      clock.tick(200);

      sinon.assert.notCalled(consumerGroup.connect);
      sinon.assert.calledOnce(consumerGroup.pause);
      sinon.assert.calledOnce(consumerGroup.fetch);
      sinon.assert.calledOnce(consumerGroup.client.refreshMetadata);
      consumerGroup.paused.should.be.false;
    });

    it('should try to connect when broker changes and a reconnect is scheduled', function () {
      let stub = sandbox.stub(consumerGroup, 'connect');
      sinon.stub(global, 'clearTimeout');

      consumerGroup.ready = false;
      consumerGroup.reconnectTimer = 1234;
      consumerGroup.connecting = undefined;

      consumerGroup.client.emit('brokersChanged');
      clock.tick(200);

      should(consumerGroup.reconnectTimer).be.null;
      sinon.assert.calledOnce(clearTimeout);
      sinon.assert.calledOnce(stub);
      global.clearTimeout.restore();
    });
  });

  describe('Offset Out Of Range', function () {
    const InvalidConsumerOffsetError = require('../lib/errors/InvalidConsumerOffsetError');

    let ConsumerGroup = null;
    let consumerGroup = null;
    let sandbox = null;
    let fakeClient = null;

    beforeEach(function () {
      sandbox = sinon.sandbox.create();

      fakeClient = sandbox.stub().returns(new EventEmitter());

      ConsumerGroup = proxyquire('../lib/consumerGroup', {
        './client': fakeClient
      });

      consumerGroup = new ConsumerGroup(
        {
          host: host,
          connectOnReady: false,
          sessionTimeout: 8000,
          heartbeatInterval: 250,
          retryMinTimeout: 250,
          heartbeatTimeoutMs: 200
        },
        'TestTopic'
      );
    });

    afterEach(function () {
      sandbox.restore();
    });

    it('should emit error on outOfRangeOffset event if outOfRangeOffset option is none', function (done) {
      sandbox.spy(consumerGroup, 'pause');
      consumerGroup.on('error', function (error) {
        error.should.be.an.instanceOf(InvalidConsumerOffsetError);
        sinon.assert.calledOnce(consumerGroup.pause);
        done();
      });

      consumerGroup.options.outOfRangeOffset = 'none';
      consumerGroup.emit('offsetOutOfRange', { topic: 'test-topic', partition: '0' });
    });

    it('should emit error if fetchOffset fails', function (done) {
      const TOPIC_NAME = 'test-topic';

      sandbox.stub(consumerGroup, 'resume');

      consumerGroup.once('error', function (error) {
        error.should.be.an.instanceOf(InvalidConsumerOffsetError);
        sinon.assert.notCalled(consumerGroup.resume);
        sinon.assert.notCalled(consumerGroup.setOffset);
        done();
      });

      consumerGroup.offset = { fetch: function () {} };
      sandbox.spy(consumerGroup, 'setOffset');
      sandbox.stub(consumerGroup.offset, 'fetch').yields(new Error('something went wrong'));

      consumerGroup.topicPayloads = [
        {
          topic: TOPIC_NAME,
          partition: '0',
          offset: 1
        }
      ];

      consumerGroup.options.outOfRangeOffset = 'latest';
      consumerGroup.emit('offsetOutOfRange', { topic: TOPIC_NAME, partition: '0' });
    });

    it('should set offset to latest if outOfRangeOffset is latest', function (done) {
      const TOPIC_NAME = 'test-topic';
      const NEW_OFFSET = 657;

      sandbox.stub(consumerGroup, 'resume').callsFake(function () {
        sinon.assert.calledOnce(consumerGroup.pause);
        sinon.assert.calledWithExactly(
          consumerGroup.offset.fetch,
          [{ topic: TOPIC_NAME, partition: '0', time: -1 }],
          sinon.match.func
        );
        sinon.assert.calledWithExactly(consumerGroup.setOffset, TOPIC_NAME, '0', NEW_OFFSET);
        consumerGroup.topicPayloads[0].offset.should.be.eql(NEW_OFFSET);
        done();
      });

      consumerGroup.offset = { fetch: function () {} };

      sandbox.spy(consumerGroup, 'setOffset');
      sandbox.stub(consumerGroup, 'pause');

      sandbox.stub(consumerGroup.offset, 'fetch').yields(null, {
        'test-topic': {
          '0': [NEW_OFFSET]
        }
      });

      consumerGroup.topicPayloads = [
        {
          topic: TOPIC_NAME,
          partition: '0',
          offset: 1
        }
      ];

      consumerGroup.options.outOfRangeOffset = 'latest';
      consumerGroup.emit('offsetOutOfRange', { topic: TOPIC_NAME, partition: '0' });
    });

    it('should set offset to earliest if outOfRangeOffset is earliest', function (done) {
      const TOPIC_NAME = 'test-topic';
      const NEW_OFFSET = 500;

      sandbox.stub(consumerGroup, 'resume').callsFake(function () {
        sinon.assert.calledWithExactly(
          consumerGroup.offset.fetch,
          [{ topic: TOPIC_NAME, partition: '0', time: -2 }],
          sinon.match.func
        );
        sinon.assert.calledWithExactly(consumerGroup.setOffset, TOPIC_NAME, '0', NEW_OFFSET);
        consumerGroup.topicPayloads[0].offset.should.be.eql(NEW_OFFSET);
        done();
      });

      consumerGroup.offset = { fetch: function () {} };

      sandbox.spy(consumerGroup, 'setOffset');

      sandbox.stub(consumerGroup.offset, 'fetch').yields(null, {
        'test-topic': {
          '0': [NEW_OFFSET]
        }
      });

      consumerGroup.topicPayloads = [
        {
          topic: TOPIC_NAME,
          partition: '0',
          offset: 1
        }
      ];

      consumerGroup.options.outOfRangeOffset = 'earliest';
      consumerGroup.emit('offsetOutOfRange', { topic: TOPIC_NAME, partition: '0' });
    });
  });

  describe('#reconnectIfNeeded', function () {
    let cg;
    beforeEach(function () {
      cg = {
        connect: sinon.stub(),
        fetch: sinon.stub()
      };
    });

    it('should do nothing if there is a pending connect', function () {
      cg.connecting = true;

      ConsumerGroup.prototype.reconnectIfNeeded.call(cg);

      sinon.assert.notCalled(cg.connect);
      sinon.assert.notCalled(cg.fetch);
      should(cg.paused).be.false;
    });

    it('should call connect if not ready and not connecting', function () {
      cg.connecting = false;
      cg.ready = false;

      ConsumerGroup.prototype.reconnectIfNeeded.call(cg);

      sinon.assert.calledOnce(cg.connect);
      sinon.assert.notCalled(cg.fetch);
      should(cg.paused).be.false;
    });

    it('should call connect if not ready and not connecting and clear existing reconnectTimer', function () {
      cg.connecting = false;
      cg.ready = false;

      cg.reconnectTimer = {};

      sinon.stub(global, 'clearTimeout');

      ConsumerGroup.prototype.reconnectIfNeeded.call(cg);

      sinon.assert.calledOnce(cg.connect);
      sinon.assert.calledOnce(clearTimeout);
      sinon.assert.notCalled(cg.fetch);
      should(cg.paused).be.false;
      should(cg.reconnectTimer).be.null;
      global.clearTimeout.restore();
    });

    it('should call fetch if ready and not connecting', function () {
      cg.connecting = false;
      cg.ready = true;

      ConsumerGroup.prototype.reconnectIfNeeded.call(cg);

      sinon.assert.notCalled(cg.connect);
      sinon.assert.calledOnce(cg.fetch);
      should(cg.paused).be.false;
    });
  });

  describe('#clearPendingFetches', function () {
    it('should set waiting to false and clear the callback queue', function () {
      const pendingSocket = new FakeSocket();
      pendingSocket.waiting = true;
      const longPollingBrokers = {
        '1': new BrokerWrapper(pendingSocket),
        '2': new BrokerWrapper(new FakeSocket())
      };
      const fakeClient = {
        getBrokers: sinon.stub().returns(longPollingBrokers),
        clearCallbackQueue: sinon.stub()
      };

      ConsumerGroup.prototype.clearPendingFetches.call({
        client: fakeClient
      });

      sinon.assert.calledWithExactly(fakeClient.getBrokers, true);
      sinon.assert.calledWithExactly(fakeClient.clearCallbackQueue, pendingSocket);
      pendingSocket.waiting.should.be.false;
    });
  });

  describe('#close', function () {
    let sandbox, consumerGroup;

    beforeEach(function () {
      sandbox = sinon.sandbox.create();
      consumerGroup = new ConsumerGroup(
        {
          host: host,
          connectOnReady: false,
          sessionTimeout: 8000,
          heartbeatInterval: 250,
          retryMinTimeout: 250,
          heartbeatTimeoutMs: 200
        },
        'TestTopic'
      );
    });

    afterEach(function (done) {
      consumerGroup.close(function () {
        sandbox.restore();
        done();
      });
    });

    it('make an attempt to leave the group but do not error out when it fails', function (done) {
      const NotCoordinatorForGroup = require('../lib/errors/NotCoordinatorForGroupError');
      sandbox.stub(consumerGroup, 'leaveGroup').yields(new NotCoordinatorForGroup());
      consumerGroup.connect();
      consumerGroup.once('connect', function () {
        consumerGroup.close(done);
      });
    });

    it('should not throw an exception when closing immediately after an UnknownMemberId error', function (done) {
      const UnknownMemberId = require('../lib/errors/UnknownMemberIdError');
      sandbox.stub(consumerGroup.client, 'sendHeartbeatRequest').yields(new UnknownMemberId('test'));

      should.doesNotThrow(function () {
        consumerGroup.connect();
        consumerGroup.once('connect', function () {
          consumerGroup.close(done);
        });
      });
    });
  });

  describe('Long running fetches', function () {
    let consumerGroup;

    beforeEach(function (done) {
      consumerGroup = new ConsumerGroup(
        {
          host: host,
          groupId: 'longFetchSimulation'
        },
        'TestTopic'
      );
      consumerGroup.once('connect', done);
    });

    afterEach(function (done) {
      consumerGroup.close(done);
    });

    it('should not throw out of bounds', function (done) {
      should.doesNotThrow(function () {
        consumerGroup.pause();
        consumerGroup.client.correlationId = 2147483647;
        consumerGroup.resume();
        setImmediate(done);
      });
    });
  });

  describe('Sending Heartbeats', function () {
    var consumerGroup, sandbox;

    beforeEach(function () {
      consumerGroup = new ConsumerGroup(
        {
          host: host,
          connectOnReady: false
        },
        'TestTopic'
      );

      sandbox = sinon.sandbox.create();
      sandbox.stub(consumerGroup, 'sendHeartbeat').returns({
        verifyResolved: sandbox.stub().returns(true)
      });
      sandbox.useFakeTimers();
    });

    afterEach(function () {
      sandbox.restore();
    });

    it('throws exception if consumerGroup is not ready', function () {
      should.throws(function () {
        consumerGroup.startHeartbeats();
      });
    });

    it('should not continue to send heartbeats if last one never resolved', function () {
      consumerGroup.ready = true;
      sinon.assert.notCalled(consumerGroup.sendHeartbeat);

      consumerGroup.sendHeartbeat.restore();

      const verifyResolvedStub = sandbox.stub().returns(false);
      sandbox.stub(consumerGroup, 'sendHeartbeat').returns({
        verifyResolved: verifyResolvedStub
      });

      consumerGroup.options.heartbeatInterval = 3000;

      consumerGroup.startHeartbeats();
      sinon.assert.calledOnce(consumerGroup.sendHeartbeat);

      sandbox.clock.tick(2000);
      sinon.assert.calledOnce(consumerGroup.sendHeartbeat);

      sandbox.clock.tick(1000);
      sinon.assert.calledOnce(consumerGroup.sendHeartbeat);

      sandbox.clock.tick(1000);
      sinon.assert.calledOnce(consumerGroup.sendHeartbeat);
    });

    it('should use heartbeatInterval passed into options', function () {
      consumerGroup.ready = true;
      sinon.assert.notCalled(consumerGroup.sendHeartbeat);
      consumerGroup.options.heartbeatInterval = 3000;

      consumerGroup.startHeartbeats();

      sinon.assert.calledOnce(consumerGroup.sendHeartbeat);
      sandbox.clock.tick(2000);
      sinon.assert.calledOnce(consumerGroup.sendHeartbeat);
      sandbox.clock.tick(1000);
      sinon.assert.calledTwice(consumerGroup.sendHeartbeat);
      sandbox.clock.tick(3000);
      sinon.assert.calledThrice(consumerGroup.sendHeartbeat);
    });
  });

  describe('#handleSyncGroup', function () {
    var consumerGroup, sandbox;
    beforeEach(function () {
      consumerGroup = new ConsumerGroup(
        {
          host: host,
          connectOnReady: false
        },
        'TestTopic'
      );
      sandbox = sinon.sandbox.create();
    });

    afterEach(function () {
      sandbox.restore();
    });

    describe('HLC Migrator', function () {
      it('should not fetch from migrator or latestOffset if all offsets have saved previously', function (done) {
        consumerGroup.options.fromOffset = 'latest';
        consumerGroup.migrator = {
          saveHighLevelConsumerOffsets: sandbox.stub()
        };

        const syncGroupResponse = {
          partitions: {
            TestTopic: [0, 2, 3, 4]
          }
        };

        const fetchOffsetResponse = {
          TestTopic: {
            0: 10,
            2: 0,
            3: 9,
            4: 6
          }
        };

        sandbox.stub(consumerGroup, 'fetchOffset').yields(null, fetchOffsetResponse);
        sandbox.stub(consumerGroup, 'saveDefaultOffsets').yields(null);

        consumerGroup.handleSyncGroup(syncGroupResponse, function (error, ownsPartitions) {
          ownsPartitions.should.be.true;
          sinon.assert.calledWith(consumerGroup.fetchOffset, syncGroupResponse.partitions);

          sinon.assert.notCalled(consumerGroup.saveDefaultOffsets);
          sinon.assert.notCalled(consumerGroup.migrator.saveHighLevelConsumerOffsets);

          const topicPayloads = _(consumerGroup.topicPayloads);

          topicPayloads.find({ topic: 'TestTopic', partition: 0 }).offset.should.be.eql(10);
          topicPayloads.find({ topic: 'TestTopic', partition: 2 }).offset.should.be.eql(0);
          topicPayloads.find({ topic: 'TestTopic', partition: 3 }).offset.should.be.eql(9);
          topicPayloads.find({ topic: 'TestTopic', partition: 4 }).offset.should.be.eql(6);
          done(error);
        });
      });

      it('should fetch from migrator and latestOffset if some offsets have not been saved previously', function (done) {
        const ConsumerGroupMigrator = require('../lib/consumerGroupMigrator');
        consumerGroup.options.fromOffset = 'latest';
        consumerGroup.migrator = new ConsumerGroupMigrator(consumerGroup);

        sandbox.stub(consumerGroup.migrator, 'saveHighLevelConsumerOffsets').yields(null);

        const syncGroupResponse = {
          partitions: {
            TestTopic: [0, 2, 3, 4]
          }
        };

        const defaultOffsets = {
          TestTopic: {
            0: 10,
            2: 20,
            4: 5000
          }
        };

        const migrateOffsets = {
          TestTopic: {
            0: 10,
            2: 20,
            4: 5000
          }
        };

        const fetchOffsetResponse = {
          TestTopic: {
            0: 10,
            2: -1,
            3: 9,
            4: -1
          }
        };

        consumerGroup.defaultOffsets = defaultOffsets;
        consumerGroup.migrator.offsets = migrateOffsets;
        sandbox.stub(consumerGroup, 'fetchOffset').yields(null, fetchOffsetResponse);
        sandbox.stub(consumerGroup, 'saveDefaultOffsets').yields(null);

        consumerGroup.handleSyncGroup(syncGroupResponse, function (error, ownsPartitions) {
          ownsPartitions.should.be.true;
          sinon.assert.calledWith(consumerGroup.fetchOffset, syncGroupResponse.partitions);

          sinon.assert.calledOnce(consumerGroup.saveDefaultOffsets);
          sinon.assert.calledOnce(consumerGroup.migrator.saveHighLevelConsumerOffsets);

          const topicPayloads = _(consumerGroup.topicPayloads);

          topicPayloads.find({ topic: 'TestTopic', partition: 0 }).offset.should.be.eql(10);
          topicPayloads.find({ topic: 'TestTopic', partition: 2 }).offset.should.be.eql(20);
          topicPayloads.find({ topic: 'TestTopic', partition: 3 }).offset.should.be.eql(9);
          topicPayloads.find({ topic: 'TestTopic', partition: 4 }).offset.should.be.eql(5000);
          done(error);
        });
      });
    });

    describe('options.fromOffset is "none"', function () {
      it('should yield error when there is not saved offsets', function (done) {
        consumerGroup.options.fromOffset = 'none';
        const syncGroupResponse = {
          partitions: {
            TestTopic: [0, 2, 3, 4]
          }
        };

        const fetchOffsetResponse = {
          TestTopic: {
            0: 10,
            2: -1,
            3: -1,
            4: -1
          }
        };

        sandbox.stub(consumerGroup, 'fetchOffset').yields(null, fetchOffsetResponse);
        sandbox.stub(consumerGroup, 'saveDefaultOffsets').yields(null);
        consumerGroup.handleSyncGroup(syncGroupResponse, function (error, ownsPartitions) {
          should(ownsPartitions).be.undefined;
          error.should.be.a.Error;
          done();
        });
      });
    });

    describe('options.fromOffset is "latest"', function () {
      it('should not fetch latestOffset if all offsets have saved previously', function (done) {
        consumerGroup.options.fromOffset = 'latest';

        const syncGroupResponse = {
          partitions: {
            TestTopic: [0, 2, 3, 4]
          }
        };

        const fetchOffsetResponse = {
          TestTopic: {
            0: 10,
            2: 0,
            3: 9,
            4: 6
          }
        };

        sandbox.stub(consumerGroup, 'fetchOffset').yields(null, fetchOffsetResponse);
        sandbox.stub(consumerGroup, 'saveDefaultOffsets').yields(null);

        consumerGroup.handleSyncGroup(syncGroupResponse, function (error, ownsPartitions) {
          ownsPartitions.should.be.true;
          sinon.assert.calledWith(consumerGroup.fetchOffset, syncGroupResponse.partitions);
          sinon.assert.notCalled(consumerGroup.saveDefaultOffsets);

          const topicPayloads = _(consumerGroup.topicPayloads);

          topicPayloads.find({ topic: 'TestTopic', partition: 0 }).offset.should.be.eql(10);
          topicPayloads.find({ topic: 'TestTopic', partition: 2 }).offset.should.be.eql(0);
          topicPayloads.find({ topic: 'TestTopic', partition: 3 }).offset.should.be.eql(9);
          topicPayloads.find({ topic: 'TestTopic', partition: 4 }).offset.should.be.eql(6);
          done(error);
        });
      });

      it('should use latestOffset if offsets have never been saved', function (done) {
        consumerGroup.options.fromOffset = 'latest';

        const syncGroupResponse = {
          partitions: {
            TestTopic: [0, 2, 3, 4]
          }
        };

        const fetchOffsetResponse = {
          TestTopic: {
            0: 10,
            2: -1,
            3: -1,
            4: -1
          }
        };

        const defaultOffsets = {
          TestTopic: {
            0: 10,
            2: 3,
            4: 5000
          }
        };

        consumerGroup.defaultOffsets = defaultOffsets;
        sandbox.stub(consumerGroup, 'fetchOffset').yields(null, fetchOffsetResponse);
        sandbox.stub(consumerGroup, 'saveDefaultOffsets').yields(null);

        consumerGroup.handleSyncGroup(syncGroupResponse, function (error, ownsPartitions) {
          ownsPartitions.should.be.true;
          sinon.assert.calledWith(consumerGroup.fetchOffset, syncGroupResponse.partitions);
          sinon.assert.calledOnce(consumerGroup.saveDefaultOffsets);

          const topicPayloads = _(consumerGroup.topicPayloads);

          topicPayloads.find({ topic: 'TestTopic', partition: 3 }).offset.should.be.eql(0);
          topicPayloads.find({ topic: 'TestTopic', partition: 2 }).offset.should.be.eql(3);
          topicPayloads.find({ topic: 'TestTopic', partition: 0 }).offset.should.be.eql(10);
          topicPayloads.find({ topic: 'TestTopic', partition: 4 }).offset.should.be.eql(5000);
          done(error);
        });
      });
    });

    it('should yield false when there are no partitions owned', function (done) {
      consumerGroup.handleSyncGroup(
        {
          partitions: {}
        },
        function (error, ownsPartitions) {
          ownsPartitions.should.be.false;
          done(error);
        }
      );
    });
  });

  describe('KafkaClient support', function () {
    it('should throw error if migration option is used with KafkaClient', function () {
      should.throws(function () {
        // eslint-disable-next-line no-new
        new ConsumerGroup(
          {
            kafkaHost: 'localhost:9092',
            migrateHLC: true
          },
          'TestTopic'
        );
      });
    });
  });

  function sendRandomByteMessage (bytes, topic, done) {
    const crypto = require('crypto');
    const buffer = crypto.randomBytes(bytes);

    sendMessage(buffer, topic, done);

    return buffer;
  }

  describe('fetchMaxBytes', function () {
    let topic, consumerGroup;
    beforeEach(function (done) {
      topic = uuid.v4();
      sendRandomByteMessage(2048, topic, done);
    });

    afterEach(function (done) {
      consumerGroup.close(done);
    });

    it('should throw an error when message exceeds maximum fetch size', function (done) {
      consumerGroup = new ConsumerGroup(
        {
          kafkaHost: '127.0.0.1:9092',
          groupId: uuid.v4(),
          fetchMaxBytes: 1024,
          fromOffset: 'earliest'
        },
        topic
      );

      consumerGroup.once('error', function (error) {
        error.should.be.an.instanceOf(require('../lib/errors/MessageSizeTooLargeError'));
        done();
      });

      consumerGroup.once('message', function (message) {
        done(new Error('should not receive a message'));
      });
    });
  });

  describe('#scheduleReconnect', function () {
    var consumerGroup, sandbox;

    beforeEach(function () {
      consumerGroup = new ConsumerGroup(
        {
          host: host,
          connectOnReady: false
        },
        'TestTopic'
      );

      sandbox = sinon.sandbox.create();
      sandbox.stub(consumerGroup, 'connect');
    });

    afterEach(function () {
      sandbox.restore();
    });

    it('should throw an exception if passed in a empty timeout', function () {
      should.throws(function () {
        consumerGroup.scheduleReconnect(0);
      });

      should.throws(function () {
        consumerGroup.scheduleReconnect(null);
      });

      should.throws(function () {
        consumerGroup.scheduleReconnect(undefined);
      });
    });

    it('should call connect method when scheduled timer is up', function () {
      sandbox.useFakeTimers();
      should(consumerGroup.reconnectTimer).be.undefined;
      consumerGroup.scheduleReconnect(1000);
      should(consumerGroup.reconnectTimer).not.be.empty;
      sandbox.clock.tick(1000);
      should(consumerGroup.reconnectTimer).be.null;
      sinon.assert.calledOnce(consumerGroup.connect);
    });

    it('should only allow one connect to be scheduled at a time', function () {
      sandbox.useFakeTimers();
      consumerGroup.scheduleReconnect(2000);
      sandbox.clock.tick(1000);
      consumerGroup.scheduleReconnect(2000);
      consumerGroup.scheduleReconnect(2000);
      consumerGroup.scheduleReconnect(2000);
      sandbox.clock.tick(2000);
      should(consumerGroup.reconnectTimer).be.null;
      sinon.assert.calledOnce(consumerGroup.connect);

      sandbox.clock.tick(10000);
      sinon.assert.calledOnce(consumerGroup.connect);
    });
  });

  it('should not commit when autoCommit is disabled', function (done) {
    const topic = uuid.v4();
    const groupId = uuid.v4();

    const messages = _.times(5, function () {
      return uuid.v4();
    });

    function addMessages (done) {
      const Client = require('../lib/client');
      const Producer = require('../lib/producer');

      const client = new Client(host);
      const producer = new Producer(client);

      async.series(
        [
          function (callback) {
            client.createTopics([topic], true, callback);
          },
          function (callback) {
            if (producer.ready) {
              return callback();
            }
            producer.once('ready', callback);
          },
          function (callback) {
            producer.send([{ topic: topic, messages: messages }], callback);
          },
          function (callback) {
            producer.close(callback);
          }
        ],
        done
      );
    }

    function confirmMessages (done) {
      const left = messages.slice();
      const consumerGroup = new ConsumerGroup(
        {
          fromOffset: 'earliest',
          groupId: groupId,
          sessionTimeout: 8000,
          autoCommit: false
        },
        topic
      );

      async.series(
        [
          function (callback) {
            consumerGroup.on('message', function (data) {
              _.pull(left, data.value);

              if (left.length === 0) {
                callback();
              }
            });
          },
          function (callback) {
            consumerGroup.close(callback);
          }
        ],
        done
      );
    }

    async.series([addMessages, confirmMessages, confirmMessages], done);
  });

  describe('#addTopics', function () {
    let topic, newTopic, testMessage, consumerGroup;

    before('create the topics', done => {
      topic = uuid.v4();
      newTopic = uuid.v4();
      testMessage = uuid.v4();
      consumerGroup = new ConsumerGroup(
        {
          kafkaHost: host + ':9092',
          groupId: uuid.v4()
        },
        topic
      );
      consumerGroup.once('connect', () => {
        consumerGroup.client.createTopics([topic, newTopic], done);
      });
    });

    after('close consumer group', done => {
      consumerGroup.close(done);
    });

    it('should fetch messages from the topic added', done => {
      let messages = [];
      consumerGroup.on('message', message => {
        messages.push(message);
        if (messages.length === 2) {
          messages.should.containDeep([
            {
              topic: topic,
              value: testMessage
            },
            {
              topic: newTopic,
              value: testMessage
            }
          ]);
          done();
        }
      });
      consumerGroup.addTopics([newTopic], (error, result) => {
        should(error).be.null;
        result.should.be.eql(`Add Topics ${newTopic} Successfully`);
        consumerGroup.once('connect', () => {
          sendMessage(testMessage, topic, () => {});
          sendMessage(testMessage, newTopic, () => {});
        });
      });
    });
  });

  describe('#removeTopics', function () {
    let topic, newTopic, testMessage, consumerGroup;

    before('create the topics', done => {
      topic = uuid.v4();
      newTopic = uuid.v4();
      testMessage = uuid.v4();
      consumerGroup = new ConsumerGroup(
        {
          kafkaHost: host + ':9092',
          groupId: uuid.v4()
        },
        [topic, newTopic]
      );
      consumerGroup.once('connect', () => {
        consumerGroup.client.createTopics([topic, newTopic], done);
      });
    });

    after('close consumer group', done => {
      consumerGroup.close(done);
    });

    it('should not fetch messages from the topic removed', done => {
      let messages = [];
      consumerGroup.on('message', message => {
        messages.push(message);
        if (messages.length === 1) {
          messages.should.containDeep([
            {
              topic: topic,
              value: testMessage
            }
          ]);
          done();
        }
      });
      consumerGroup.removeTopics([newTopic], (error, result) => {
        should(error).be.null;
        result.should.be.eql(`Remove Topics ${newTopic} Successfully`);
        consumerGroup.once('connect', () => {
          sendMessage(testMessage, newTopic, () => {});
          sendMessage(testMessage, topic, () => {});
        });
      });
    });
  });
});
