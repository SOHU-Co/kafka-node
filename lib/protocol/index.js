'use strict';

var _ = require('lodash');
var struct = require('./protocol_struct');
var protocol = require('./protocol');

exports = _.extend(exports, struct, protocol);
