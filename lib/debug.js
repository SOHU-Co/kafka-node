'use strict';
var debug = null;

try {
	debug = require('debug')('kafka-node');
} catch( err ) {
	debug = function() {};
}

module.exports = debug;