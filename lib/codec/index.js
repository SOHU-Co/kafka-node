'use strict';

var zlib = require('zlib');
var snappyCodec = require('./snappy');

var gzipCodec = {
  encode: zlib.gzip,
  decode: zlib.gunzip
};

var codecs = [
  null,
  gzipCodec,
  snappyCodec
];

function getCodec (attributes) {
  return codecs[attributes & 3] || null;
}

module.exports = getCodec;
