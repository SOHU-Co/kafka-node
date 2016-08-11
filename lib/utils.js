var InvalidConfigError = require('./errors/InvalidConfigError');
var legalChars = new RegExp('^[a-zA-Z0-9._-]*$');

function validateConfig (property, value) {
  if (!legalChars.test(value)) {
    throw new InvalidConfigError([property, value, "is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'"].join(' '));
  }
}

function validateTopics (topics) {
  if (topics.some(function (topic) {
    if ('partition' in topic) {
      return typeof topic.partition !== 'number';
    }
    return false;
  })) {
    throw new InvalidConfigError('Offset must be a number and can not contain characters');
  }
}

module.exports = {
  validateConfig: validateConfig,
  validateTopics: validateTopics
};
