var should = require('should');
var ConsumerGroup = require('../lib/ConsumerGroup');
var host = process.env['KAFKA_TEST_HOST'] || '';
var _ = require('lodash');
var GroupCoordinatorNotAvailable = require('../lib/errors/GroupCoordinatorNotAvailableError');
var GroupLoadInProgress = require('../lib/errors/GroupLoadInProgressError');

'use strict';

describe('ConsumerGroup', function () {
  describe('#getRetryTimeout', function () {
    var consumerGroup;

    beforeEach(function () {
      consumerGroup = new ConsumerGroup({
        host: host,
        groupId: 'TestGroup',
        connectOnReady: false
      });
    });

    it('should reset backoff timeouts when calling with different error', function () {
      var error = new GroupCoordinatorNotAvailable();
      error.errorCode = 15;

      var differentError = new GroupLoadInProgress();
      differentError.errorCode = 14;

      consumerGroup.options.retries = 5;

      var results = _.times(3, function () {
        var ret = consumerGroup.getRetryTimeout(error);
        consumerGroup.lastError = error;
        return ret;
      });

      consumerGroup._timeouts.should.have.length(5);

      results.should.be.length(3);
      results.forEach(function (result, index) {
        result.should.eql(consumerGroup._timeouts[index]);
      });

      results = _.times(5, function () {
        var ret = consumerGroup.getRetryTimeout(differentError);
        consumerGroup.lastError = differentError;
        return ret;
      });

      consumerGroup._timeouts.should.have.length(5);

      results.should.be.length(5);
      results.forEach(function (result, index) {
        result.should.eql(consumerGroup._timeouts[index]);
      });
    });

    it('should return backoff timeouts when calling same error', function () {
      var error = new GroupCoordinatorNotAvailable();
      error.errorCode = 15;

      consumerGroup.options.retries = 5;

      var results = _.times(6, function () {
        var ret = consumerGroup.getRetryTimeout(error);
        consumerGroup.lastError = error;
        return ret;
      });

      consumerGroup._timeouts.should.have.length(5);
      results.pop().should.be.false;
      results.forEach(function (result, index) {
        result.should.eql(consumerGroup._timeouts[index]);
      });
    });

    it('should initalize timeout array when invoked', function () {
      var error = new GroupCoordinatorNotAvailable();
      error.errorCode = 15;

      should(consumerGroup._timeouts).be.undefined;

      var retryTime = consumerGroup.getRetryTimeout(error);

      consumerGroup._timeouts.should.have.length(10);
      retryTime.should.be.eql(consumerGroup._timeouts[0]);
    });

    it('should throw if empty arguments', function () {
      should.throws(function () {
        consumerGroup.getRetryTimeout();
      });

      should.throws(function () {
        consumerGroup.getRetryTimeout(null);
      });
    });
  });
});
