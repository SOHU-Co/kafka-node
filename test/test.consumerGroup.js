'use strict';

var should = require('should');
var ConsumerGroup = require('../lib/ConsumerGroup');
var host = process.env['KAFKA_TEST_HOST'] || '';
var _ = require('lodash');
var GroupCoordinatorNotAvailable = require('../lib/errors/GroupCoordinatorNotAvailableError');
var GroupLoadInProgress = require('../lib/errors/GroupLoadInProgressError');


describe('ConsumerGroup', function () {

});
