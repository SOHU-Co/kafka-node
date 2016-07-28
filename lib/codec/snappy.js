'use strict';

var optional = require('optional');
var async = require('async');
var snappy = optional('snappy');

if (snappy == null) {
  var unavailableCodec = function unavailableCodec () {
    throw new Error('Snappy codec is not installed');
  };
  module.exports = {
    encode: unavailableCodec,
    decode: unavailableCodec
  };
} else {
  var SNAPPY_MAGIC_BYTES = [130, 83, 78, 65, 80, 80, 89, 0];
  var SNAPPY_MAGIC_BYTES_LEN = SNAPPY_MAGIC_BYTES.length;
  var SNAPPY_MAGIC = new Buffer(SNAPPY_MAGIC_BYTES).toString('hex');

  exports.encode = snappy.compress;
  exports.decode = decodeSnappy;
}

function isChunked (buffer) {
  var prefix = buffer.toString('hex', 0, SNAPPY_MAGIC_BYTES_LEN);
  return prefix === SNAPPY_MAGIC;
}

// Ported from:
// https://github.com/Shopify/sarama/blob/a3e2437d6d26cda6b2dc501dbdab4d3f6befa295/snappy.go
function decodeSnappy (buffer, cb) {
  if (isChunked(buffer)) {
    var pos = 16;
    var max = buffer.length;
    var encoded = [];
    var size;

    while (pos < max) {
      size = buffer.readUInt32BE(pos);
      pos += 4;
      encoded.push(buffer.slice(pos, pos + size));
      pos += size;
    }
    return async.mapSeries(encoded, snappy.uncompress,
      function (err, decodedChunks) {
        if (err) return cb(err);
        return cb(null, Buffer.concat(decodedChunks));
      }
    );
  }
  return snappy.uncompress(buffer, cb);
}
