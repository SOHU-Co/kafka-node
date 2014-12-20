'use strict';

var kafka = require('../kafka');
var HighLevelConsumer = kafka.HighLevelConsumer;
var Offset = kafka.Offset;
var Client = kafka.Client;
var argv = require('optimist').argv;
var topic = argv.topic || 'topic1';
var client = new Client('localhost:2181');
var topics = [ { topic: topic }];
var options = { autoCommit: true, fromBeginning: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024*1024 };
var consumer = new HighLevelConsumer(client, topics, options);
var offset = new Offset(client);

consumer.on('message', function (message) {
    console.log(this.id, message);
});
consumer.on('error', function (err) {
    console.log('error', err);
});
consumer.on('offsetOutOfRange', function (topic) {
    topic.maxNum = 2;
    offset.fetch([topic], function (err, offsets) {
        var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
        consumer.setOffset(topic.topic, topic.partition, min);
    });
});
