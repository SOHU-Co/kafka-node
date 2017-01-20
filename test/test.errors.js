'use strict';

const errors = require('../lib/errors');

describe('Test Errors', function () {
  it('should have right number of consumer group errors', function () {
    errors.ConsumerGroupErrors.length.should.be.eql(7);
  });
});
