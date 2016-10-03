'use strict';

const sinon = require('sinon');
const should = require('should');
const ConsumerGroup = require('../lib/ConsumerGroup');
const host = process.env['KAFKA_TEST_HOST'] || '';
const proxyquire = require('proxyquire').noCallThru();
const EventEmitter = require('events').EventEmitter;

describe('ConsumerGroup', function () {
  describe('#constructor', function () {
    var ConsumerGroup;
    var fakeClient = sinon.stub().returns(new EventEmitter());

    beforeEach(function () {
      ConsumerGroup = proxyquire('../lib/ConsumerGroup', {
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
      should(consumerGroup.reconnectTimer).exist;
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
