'use strict';

var should = require('should');
var _ = require('lodash');
var ConsumerGroupRecovery = require('../lib/ConsumerGroupRecovery');
var GroupCoordinatorNotAvailable = require('../lib/errors/GroupCoordinatorNotAvailableError');
var GroupLoadInProgress = require('../lib/errors/GroupLoadInProgressError');

describe('ConsumerGroupRecovery', function () {
  describe('#getRetryTimeout', function () {
    var consumerGroupRecovery;

    beforeEach(function () {
      consumerGroupRecovery = new ConsumerGroupRecovery({
        options: {
          retries: 10,
          retryFactor: 1.8
        }
      });
    });

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
