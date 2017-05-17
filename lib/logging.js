'use strict';

var debug = require('debug');

var loggerProvider = debugLoggerProvider;

module.exports = exports = function getLogger (name) {
  return loggerProvider(name);
};

exports.setLoggerProvider = function setLoggerProvider (provider) {
  loggerProvider = provider;
};

function debugLoggerProvider (name) {
  var logger = debug(name);
  logger = logger.bind(logger);

  return {
    debug: logger,
    info: logger,
    warn: logger,
    error: logger
  };
}
