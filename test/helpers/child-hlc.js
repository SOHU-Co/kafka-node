'use strict';

var kafka = require('../../');
var HighLevelConsumer = kafka.HighLevelConsumer;
var Client = kafka.Client;
var argv = require('optimist').argv;
var topic = argv.topic || 'topic1';
var client = new Client('localhost:2181');
var topics = [{topic: topic}];
var options = { autoCommit: true, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 };

if (argv.groupId) {
  options.groupId = argv.groupId;
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
  console.log('error', err);
});

consumer.on('rebalanced', function () {
  console.log('rebalanced!');
});

function close () {
  console.log('closing the consumer');
  consumer.close(true, function () {
    process.exit();
  });
}

process.once('SIGINT', close);
process.once('SIGTERM', close);
process.once('SIGABRT', close);
process.once('disconnect', close);
