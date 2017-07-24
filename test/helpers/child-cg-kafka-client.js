'use strict';

var kafka = require('../../');
var ConsumerGroup = kafka.ConsumerGroup;
var argv = require('optimist').argv;
var topic = argv.topic || 'topic1';
var options = {
  kafkaHost: '127.0.0.1:9092',
  autoCommit: true,
  fetchMaxWaitMs: 1000,
  fetchMaxBytes: 1024 * 1024,
  sessionTimeout: 8000,
  heartbeatInterval: 250,
  retryMinTimeout: 250,
  versions: {
    requestTimeout: 150
  }
};
var debug = require('debug')('kafka-node:Child-ConsumerGroup');

if (argv.groupId) {
  options.groupId = argv.groupId;
}

if (argv.consumerId) {
  options.id = argv.consumerId;
}

var consumer = new ConsumerGroup(options, [topic]);

consumer.on('message', function (message) {
  var out = {
    id: consumer.client.clientId,
    message: message
  };
  process.send(out);
});

consumer.on('error', function (err) {
  debug('error', err);
});

consumer.on('rebalanced', function () {
  debug('%s rebalanced!', consumer.client.clientId);
  sendEvent('rebalanced');
});

consumer.on('rebalancing', function () {
  debug('%s is rebalancing', consumer.client.clientId);
});

function sendEvent (event) {
  process.send({
    id: consumer.client.clientId,
    event: event
  });
}

function close (signal) {
  return function () {
    debug('closing the consumer (%s) [%s].', signal, consumer.client.clientId);
    consumer.close(true, function () {
      process.exit();
    });
  };
}

process.once('SIGINT', close('SIGINT'));
process.once('SIGTERM', close('SIGTERM'));
process.once('SIGABRT', close('SIGABRT'));
process.once('disconnect', close('disconnect'));
