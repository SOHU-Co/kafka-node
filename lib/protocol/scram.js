const crypto = require('crypto');

function hi (config, authConfig) {
  const { password, salt, iterations } = config;
  return crypto.pbkdf2Sync(password, salt, iterations, authConfig.length, authConfig.digest);
}

function hmac (key, data, authConfig) {
  return crypto.createHmac(authConfig.digest, key).update(data).digest();
}

function hash (data, authConfig) {
  return crypto.createHash(authConfig.digest).update(data).digest();
}

function h (data, authConfig) {
  return crypto
    .createHash(authConfig.digest)
    .update(data)
    .digest();
}

function normalizePassword (password) {
  return password.toString('utf-8');
}

function nonce () {
  return crypto.randomBytes(16)
    .toString('base64')
    .replace(/\+/g, '-') // Convert '+' to '-'
    .replace(/\//g, '_') // Convert '/' to '_'
    .replace(/=+$/, '') // Remove ending '='
    .toString('ascii');
}

function xor (left, right) {
  let leftBuffer = Buffer.from(left);
  let rightBuffer = Buffer.from(right);
  let leftLength = Buffer.byteLength(leftBuffer);
  let rightLength = Buffer.byteLength(rightBuffer);

  if (leftLength !== rightLength) {
    return new Error('Error while authentication (xor buffer length)');
  }

  let result = Buffer.alloc(leftLength);
  for (let i = 0; i < leftLength; i++) {
    result[i] = leftBuffer[i] ^ rightBuffer[i];
  }

  return result;
}

const G2_HEADER = 'n,,';

exports.hi = hi;
exports.hmac = hmac;
exports.hash = hash;
exports.h = h;
exports.normalizePassword = normalizePassword;
exports.nonce = nonce;
exports.xor = xor;
exports.G2_HEADER = G2_HEADER;
