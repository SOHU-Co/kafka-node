'use strict';

var sinon = require('sinon');
var should = require('should');
var ConsumerGroup = require('../lib/ConsumerGroup');
var host = process.env['KAFKA_TEST_HOST'] || '';

describe('ConsumerGroup', function () {
  describe('#scheduleReconnect', function () {
    var consumerGroup, sandbox;

    beforeEach(function () {
      consumerGroup = new ConsumerGroup({
        host: host,
        connnectOnReady: false
      }, ['TestTopic']);

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
