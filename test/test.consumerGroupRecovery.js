'use strict';

const should = require('should');
const _ = require('lodash');
const sinon = require('sinon');
const ConsumerGroupRecovery = require('../lib/consumerGroupRecovery');
const GroupCoordinatorNotAvailable = require('../lib/errors/GroupCoordinatorNotAvailableError');
const GroupLoadInProgress = require('../lib/errors/GroupLoadInProgressError');
const HeartbeatTimeout = require('../lib/errors/HeartbeatTimeoutError');
const TimeoutError = require('../lib/errors/TimeoutError');
const BrokerNotAvailableError = require('../lib/errors').BrokerNotAvailableError;
const EventEmitter = require('events');

describe('ConsumerGroupRecovery', function () {
  var consumerGroupRecovery, fakeConsumerGroup;

  beforeEach(function () {
    fakeConsumerGroup = new EventEmitter();
    fakeConsumerGroup.client = new EventEmitter();
    fakeConsumerGroup.scheduleReconnect = () => {
      throw new Error('should be stubbed!');
    };
    Object.assign(fakeConsumerGroup, {
      stopHeartbeats: sinon.stub(),
      options: {
        retries: 10,
        retryFactor: 1.8,
        retryMinTimeout: 1000
      }
    });
    consumerGroupRecovery = new ConsumerGroupRecovery(fakeConsumerGroup);
  });

  describe('#tryToRecoverFrom', function () {
    it('should emit error on the client when calling trying to recover from a unknown error', function (done) {
      var testError = new Error('My test error');

      fakeConsumerGroup.once('error', function (error) {
        error.should.be.eql(testError);
        done();
      });

      consumerGroupRecovery.tryToRecoverFrom(testError, 'test');

      sinon.assert.calledOnce(fakeConsumerGroup.stopHeartbeats);
      fakeConsumerGroup.ready.should.be.false;
      consumerGroupRecovery.lastError.should.be.eql(testError);
    });

    it('should try to recover from a BrokerNotAvailableError', function () {
      const brokerNotAvailableError = new BrokerNotAvailableError('test error');

      fakeConsumerGroup.client.coordinatorId = 1234;

      fakeConsumerGroup.once('error', function (error) {
        error.should.not.be.eql(brokerNotAvailableError);
      });

      sinon.stub(fakeConsumerGroup, 'scheduleReconnect');

      consumerGroupRecovery.tryToRecoverFrom(brokerNotAvailableError, 'test');

      sinon.assert.calledOnce(fakeConsumerGroup.stopHeartbeats);
      fakeConsumerGroup.ready.should.be.false;
      consumerGroupRecovery.lastError.should.be.eql(brokerNotAvailableError);

      sinon.assert.calledOnce(fakeConsumerGroup.scheduleReconnect);
      should(fakeConsumerGroup.client.coordinatorId).be.undefined;
    });

    it('should try to recover from a temporary network error', function () {
      const fakeNetworkError = new Error('read ETIMEDOUT');
      fakeNetworkError.code = fakeNetworkError.errno = 'ETIMEDOUT';

      fakeConsumerGroup.once('error', function (error) {
        error.should.not.be.eql(fakeNetworkError);
      });

      sinon.stub(fakeConsumerGroup, 'scheduleReconnect');

      consumerGroupRecovery.tryToRecoverFrom(fakeNetworkError, 'test');

      sinon.assert.calledOnce(fakeConsumerGroup.stopHeartbeats);
      fakeConsumerGroup.ready.should.be.false;
      consumerGroupRecovery.lastError.should.be.eql(fakeNetworkError);

      sinon.assert.calledOnce(fakeConsumerGroup.scheduleReconnect);
    });

    it('should try to recover from a request timeout', function () {
      const fakeNetworkError = new TimeoutError('request timeout');

      fakeConsumerGroup.once('error', function (error) {
        error.should.not.be.eql(fakeNetworkError);
      });

      sinon.stub(fakeConsumerGroup, 'scheduleReconnect');

      consumerGroupRecovery.tryToRecoverFrom(fakeNetworkError, 'test');

      sinon.assert.calledOnce(fakeConsumerGroup.stopHeartbeats);
      fakeConsumerGroup.ready.should.be.false;
      consumerGroupRecovery.lastError.should.be.eql(fakeNetworkError);

      sinon.assert.calledOnce(fakeConsumerGroup.scheduleReconnect);
    });

    it('should try to recover from a HeartbeatTimeout', function () {
      const heartbeatTimeout = new HeartbeatTimeout('test error');

      fakeConsumerGroup.once('error', function (error) {
        error.should.not.be.eql(heartbeatTimeout);
      });

      sinon.stub(fakeConsumerGroup, 'scheduleReconnect');

      consumerGroupRecovery.tryToRecoverFrom(heartbeatTimeout, 'test');

      sinon.assert.calledOnce(fakeConsumerGroup.stopHeartbeats);
      fakeConsumerGroup.ready.should.be.false;
      consumerGroupRecovery.lastError.should.be.eql(heartbeatTimeout);

      sinon.assert.calledOnce(fakeConsumerGroup.scheduleReconnect);
    });
  });

  describe('#getRetryTimeout', function () {
    it('should reset backoff timeouts when calling with different error', function () {
      var error = new GroupCoordinatorNotAvailable();
      error.errorCode = 15;

      var differentError = new GroupLoadInProgress();
      differentError.errorCode = 14;

      consumerGroupRecovery.options.retries = 5;

      var results = _.times(3, function () {
        var ret = consumerGroupRecovery.getRetryTimeout(error);
        consumerGroupRecovery.lastError = error;
        return ret;
      });

      consumerGroupRecovery._timeouts.should.have.length(5);

      results.should.be.length(3);
      results.forEach(function (result, index) {
        result.should.eql(consumerGroupRecovery._timeouts[index]);
      });

      results = _.times(5, function () {
        var ret = consumerGroupRecovery.getRetryTimeout(differentError);
        consumerGroupRecovery.lastError = differentError;
        return ret;
      });

      consumerGroupRecovery._timeouts.should.have.length(5);

      results.should.be.length(5);
      results.forEach(function (result, index) {
        result.should.eql(consumerGroupRecovery._timeouts[index]);
      });
    });

    it('should return backoff timeouts when calling same error', function () {
      var error = new GroupCoordinatorNotAvailable();
      error.errorCode = 15;

      consumerGroupRecovery.options.retries = 5;

      var results = _.times(6, function () {
        var ret = consumerGroupRecovery.getRetryTimeout(error);
        consumerGroupRecovery.lastError = error;
        return ret;
      });

      consumerGroupRecovery._timeouts.should.have.length(5);
      results.pop().should.be.false;
      results.forEach(function (result, index) {
        result.should.eql(consumerGroupRecovery._timeouts[index]);
      });
    });

    it('should initalize timeout array when invoked', function () {
      var error = new GroupCoordinatorNotAvailable();
      error.errorCode = 15;

      should(consumerGroupRecovery._timeouts).be.undefined;

      var retryTime = consumerGroupRecovery.getRetryTimeout(error);

      consumerGroupRecovery._timeouts.should.have.length(10);
      retryTime.should.be.eql(consumerGroupRecovery._timeouts[0]);
    });

    it('should throw if empty arguments', function () {
      should.throws(function () {
        consumerGroupRecovery.getRetryTimeout();
      });

      should.throws(function () {
        consumerGroupRecovery.getRetryTimeout(null);
      });
    });
  });
});
