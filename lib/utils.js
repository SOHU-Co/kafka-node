var InvalidConfigError = require('./errors/InvalidConfigError');
var legalChars = new RegExp('^[a-zA-Z0-9._-]*$');

function validateConfig (property, value) {
  if (!legalChars.test(value)) {
    throw new InvalidConfigError([property, value, "is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'"].join(' '));
  }
}

module.exports = {
  validateConfig: validateConfig
};
