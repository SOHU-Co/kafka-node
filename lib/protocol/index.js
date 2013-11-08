'use strict';

var _ = require('lodash'),
    struct = require('./protocol_struct'),
    protocol = require('./protocol');

exports = _.extend(exports, struct, protocol);
