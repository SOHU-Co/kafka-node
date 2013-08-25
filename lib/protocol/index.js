'use strict';

var _ = require('underscore'),
    struct = require('./protocol_struct'),
    protocol = require('./protocol');

exports = _.extend(exports, struct, protocol);
