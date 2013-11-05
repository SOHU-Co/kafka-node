'use strict';

var kafka = require('../kafka');
var Consumer = kafka.Consumer;
var Producer = kafka.Producer;
var Offset = kafka.Offset;
var Client = kafka.Client;

var client = new Client();
var topics = [
        {topic: 'topic2'},
        {topic: 'topic1'},
        {topic: 't2'},
        {topic: 'topic3'} 
    ],
    options = { autoCommit: false, fromBeginning: false, fetchMaxWaitMs: 10000 };

function createConsumer() {
    var consumer = new Consumer(client, topics, options);
    var offset = new Offset(client);
    consumer.on('message', function (message) {
        console.log(this.id, message);
    });
    consumer.on('error', function (err) {
        console.log('error', err);
    });
    consumer.on('offsetOutOfRange', function (topic) {
        console.log(topic); 
        topic.maxNum = 2;
        offset.fetch([topic], function (err, offsets) {
            var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
            consumer.setOffset(topic.topic, topic.partition, min);
        });
    })
}

createConsumer();
