'use strict';

const createLogger = require('../lib/logging');
const logging = require('../logging');
const sinon = require('sinon');

describe('logging', function () {
  it('should expose a setLoggerProvider function', function () {
    logging.setLoggerProvider.should.be.instanceOf(Function);
  });

  it('should create logger via custom logger provider', function () {
    const provider = sinon.stub();
    const loggerName = 'kafka-node:consumer';
    const loggerImpl = {};
    provider.withArgs(loggerName).returns(loggerImpl);
    logging.setLoggerProvider(provider);

    const logger = createLogger(loggerName);

    logger.should.equal(loggerImpl);
  });
});
