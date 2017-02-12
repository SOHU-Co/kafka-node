'use strict';

const debug = require('debug');

let loggerProvider = debugLoggerProvider;

module.exports = exports = function getLogger (name) {
  return loggerProvider(name);
};

exports.setLoggerProvider = function setLoggerProvider (provider) {
  loggerProvider = provider;
};

function debugLoggerProvider (name) {
  let logger = debug(name);
  logger = logger.bind(logger);

  return {
    debug: logger,
    info: logger,
    warn: logger,
    error: logger
  };
}
