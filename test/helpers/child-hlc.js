'use strict';

var kafka = require('../../');
var HighLevelConsumer = kafka.HighLevelConsumer;
var Client = kafka.Client;
var argv = require('optimist').argv;
var topic = argv.topic || 'topic1';
var uuid = require('node-uuid');
var host = process.env['KAFKA_TEST_HOST'] || '';
var client = new Client(host, 'child-' + uuid.v4(), {sessionTimeout: 5000});
var topics = [{topic: topic}];
var options = { autoCommit: true, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 };
var debug = require('debug')('kafka-node:Child-HLC');

if (argv.groupId) {
  options.groupId = argv.groupId;
}

if (argv.consumerId) {
  options.id = argv.consumerId;
}

var consumer = new HighLevelConsumer(client, topics, options);

consumer.on('message', function (message) {
  var out = {
    id: consumer.id,
    message: message
  };
  process.send(out);
});

consumer.on('error', function (err) {
  debug('error', err);
});

consumer.on('rebalanced', function () {
  debug('%s rebalanced!', consumer.id);
  sendEvent('rebalanced');
});

consumer.on('rebalancing', function () {
  debug('%s is rebalancing', consumer.id);
});

consumer.on('registered', function () {
  debug('%s registered', consumer.id);
  sendEvent('registered');
});

function sendEvent (event) {
  process.send({
    id: consumer.id,
    event: event
  });
}

function close (signal) {
  return function () {
    debug('closing the consumer (%s) [%s].', signal, consumer.id);
    consumer.close(true, function () {
      process.exit();
    });
  };
}

process.once('SIGINT', close('SIGINT'));
process.once('SIGTERM', close('SIGTERM'));
process.once('SIGABRT', close('SIGABRT'));
process.once('disconnect', close('disconnect'));
