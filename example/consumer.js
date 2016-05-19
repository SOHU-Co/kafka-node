'use strict';

var kafka = require('..');
var Consumer = kafka.Consumer;
var Offset = kafka.Offset;
var Client = kafka.Client;
var argv = require('optimist').argv;
var topic = argv.topic || 'topic1';

var client = new Client('localhost:2181');
var topics = [
        {topic: topic, partition: 0}
    ],
    options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 500*1024*1024, encoding: 'buffer' };

var consumer = new Consumer(client, topics, options);
var offset = new Offset(client);

var start = Date.now();
consumer.on('message', function (message) {
    console.log("message - offset:", message.offset, "| length:", message.value.length);
    console.log("receive msg after (ms):", Date.now() - start);
});

consumer.on('error', function (err) {
    console.log('error', err);
});

/*
* If consumer get `offsetOutOfRange` event, fetch data from the smallest(oldest) offset
*/
consumer.on('offsetOutOfRange', function (topic) {
    topic.maxNum = 2;
    offset.fetch([topic], function (err, offsets) {
        var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
        consumer.setOffset(topic.topic, topic.partition, min);
    });
});
