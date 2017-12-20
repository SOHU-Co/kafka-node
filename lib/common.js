'use strict';

const createBuffer = function (data, encoding) {
  if (typeof Buffer.from === 'function') {
    return Buffer.from(data, encoding);
  }
  return new Buffer(data, encoding);
};

module.exports = {
  createBuffer: createBuffer
};
