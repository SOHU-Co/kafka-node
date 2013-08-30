var crypto = require('crypto')
    , alpha_num_chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXTZabcdefghiklmnopqrstuvwxyz'.split('');

var util = {};

util.md5 = function (text) {
    return crypto.createHash('md5').update(text).digest('hex');
};

/**
 * generate a random string of length
 * randomString(2048 + Math.random() * 2048);  //2048-4096
 * @param length
 * @returns {string}
 */
util.randomString = function (length) {
    if (!length) {
        length = Math.floor(Math.random() * alpha_num_chars.length);
    }

    var str = '';
    for (var i = 0; i < length; i++) {
        str += alpha_num_chars[Math.floor(Math.random() * alpha_num_chars.length)];
    }
    return str;
};


module.exports = util;
