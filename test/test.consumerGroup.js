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
        connnectOnReady: false
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
        connnectOnReady: false
      }, 'SampleTopic');

      sinon.assert.calledWithExactly(fakeClient, 'myhost', 'myClientId', zkOptions, undefined, undefined);
    });

    it('should setup SSL ConsumerGroup option ssl is true', function () {
      // eslint-disable-next-line no-new
      new ConsumerGroup({
        host: 'myhost',
        id: 'myClientId',
        ssl: true,
        connnectOnReady: false
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
        connnectOnReady: false
      }, 'SampleTopic');
      sinon.assert.calledWithExactly(fakeClient, 'myhost', 'myClientId', undefined, undefined, ssl);
    });
  });

  describe('#scheduleReconnect', function () {
    var consumerGroup, sandbox;

    beforeEach(function () {
      consumerGroup = new ConsumerGroup({
        host: host,
        connnectOnReady: false
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
