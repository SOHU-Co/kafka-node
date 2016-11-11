'use strict';

const sinon = require('sinon');
const should = require('should');
const ConsumerGroup = require('../lib/consumerGroup');
const host = process.env['KAFKA_TEST_HOST'] || '';
const proxyquire = require('proxyquire').noCallThru();
const EventEmitter = require('events').EventEmitter;
const _ = require('lodash');

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
      new ConsumerGroup({
        host: 'myhost',
        id: 'myClientId',
        batch: batch,
        connectOnReady: false
      }, 'SampleTopic');

      sinon.assert.calledWithExactly(fakeClient, 'myhost', 'myClientId', undefined, batch, undefined);
    });

    it('should pass zookeeper ConsumerGroup option to Client', function () {
      const zkOptions = {
        sessionTimeout: 10000
      };

      // eslint-disable-next-line no-new
      new ConsumerGroup({
        host: 'myhost',
        id: 'myClientId',
        zk: zkOptions,
        connectOnReady: false
      }, 'SampleTopic');

      sinon.assert.calledWithExactly(fakeClient, 'myhost', 'myClientId', zkOptions, undefined, undefined);
    });

    it('should setup SSL ConsumerGroup option ssl is true', function () {
      // eslint-disable-next-line no-new
      new ConsumerGroup({
        host: 'myhost',
        id: 'myClientId',
        ssl: true,
        connectOnReady: false
      }, 'SampleTopic');
      sinon.assert.calledWithExactly(fakeClient, 'myhost', 'myClientId', undefined, undefined, {});
    });

    it('should pass SSL client options through ConsumerGroup option', function () {
      const ssl = { rejectUnauthorized: false };
      // eslint-disable-next-line no-new
      new ConsumerGroup({
        host: 'myhost',
        id: 'myClientId',
        ssl: ssl,
        connectOnReady: false
      }, 'SampleTopic');
      sinon.assert.calledWithExactly(fakeClient, 'myhost', 'myClientId', undefined, undefined, ssl);
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
          new ConsumerGroup({
            fromOffset: offset,
            connectOnReady: false
          }, 'TestTopic');
        });
      });
    });
  });

  describe('#close', function () {
    let sandbox, consumerGroup;

    beforeEach(function () {
      sandbox = sinon.sandbox.create();
      consumerGroup = new ConsumerGroup({
        host: host,
        connectOnReady: false,
        sessionTimeout: 8000,
        heartbeatInterval: 250,
        retryMinTimeout: 250
      }, 'TestTopic');
    });

    afterEach(function () {
      sandbox.restore();
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

  describe('#sendHeartbeats', function () {
    var consumerGroup, sandbox;

    beforeEach(function () {
      consumerGroup = new ConsumerGroup({
        host: host,
        connectOnReady: false
      }, 'TestTopic');

      sandbox = sinon.sandbox.create();
      sandbox.stub(consumerGroup, 'sendHeartbeat');
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

    it('should use heartbeatInterval passed into options', function () {
      consumerGroup.ready = true;
      sinon.assert.notCalled(consumerGroup.sendHeartbeat);
      consumerGroup.options.heartbeatInterval = 3000;

      consumerGroup.startHearbeats();

      sinon.assert.calledOnce(consumerGroup.sendHeartbeat);
      sandbox.clock.tick(2000);
      sinon.assert.calledOnce(consumerGroup.sendHeartbeat);
      sandbox.clock.tick(1000);
      sinon.assert.calledTwice(consumerGroup.sendHeartbeat);
      sandbox.clock.tick(3000);
      sinon.assert.calledThrice(consumerGroup.sendHeartbeat);
    });

    it('should use calculated heartbeatInterval if heartbeatInterval options is omitted', function () {
      consumerGroup.ready = true;
      consumerGroup.startHearbeats();
      sinon.assert.calledOnce(consumerGroup.sendHeartbeat);

      sandbox.clock.tick(9000);
      sinon.assert.calledOnce(consumerGroup.sendHeartbeat);

      sandbox.clock.tick(1000);
      sinon.assert.calledTwice(consumerGroup.sendHeartbeat);
    });
  });

  describe('#handleSyncGroup', function () {
    var consumerGroup, sandbox;
    beforeEach(function () {
      consumerGroup = new ConsumerGroup({
        host: host,
        connectOnReady: false
      }, 'TestTopic');
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

          topicPayloads.find({topic: 'TestTopic', partition: 0}).offset.should.be.eql(10);
          topicPayloads.find({topic: 'TestTopic', partition: 2}).offset.should.be.eql(0);
          topicPayloads.find({topic: 'TestTopic', partition: 3}).offset.should.be.eql(9);
          topicPayloads.find({topic: 'TestTopic', partition: 4}).offset.should.be.eql(6);
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

          topicPayloads.find({topic: 'TestTopic', partition: 0}).offset.should.be.eql(10);
          topicPayloads.find({topic: 'TestTopic', partition: 2}).offset.should.be.eql(20);
          topicPayloads.find({topic: 'TestTopic', partition: 3}).offset.should.be.eql(9);
          topicPayloads.find({topic: 'TestTopic', partition: 4}).offset.should.be.eql(5000);
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

          topicPayloads.find({topic: 'TestTopic', partition: 0}).offset.should.be.eql(10);
          topicPayloads.find({topic: 'TestTopic', partition: 2}).offset.should.be.eql(0);
          topicPayloads.find({topic: 'TestTopic', partition: 3}).offset.should.be.eql(9);
          topicPayloads.find({topic: 'TestTopic', partition: 4}).offset.should.be.eql(6);
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

          topicPayloads.find({topic: 'TestTopic', partition: 3}).offset.should.be.eql(0);
          topicPayloads.find({topic: 'TestTopic', partition: 2}).offset.should.be.eql(3);
          topicPayloads.find({topic: 'TestTopic', partition: 0}).offset.should.be.eql(10);
          topicPayloads.find({topic: 'TestTopic', partition: 4}).offset.should.be.eql(5000);
          done(error);
        });
      });
    });

    it('should yield false when there are no partitions owned', function (done) {
      consumerGroup.handleSyncGroup({
        partitions: {}
      }, function (error, ownsPartitions) {
        ownsPartitions.should.be.false;
        done(error);
      });
    });
  });

  describe('#scheduleReconnect', function () {
    var consumerGroup, sandbox;

    beforeEach(function () {
      consumerGroup = new ConsumerGroup({
        host: host,
        connectOnReady: false
      }, 'TestTopic');

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
});
