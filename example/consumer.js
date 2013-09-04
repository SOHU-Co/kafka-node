'use strict';

var kafka = require('../kafka');
var Consumer = kafka.Consumer;
var Producer = kafka.Producer;
var Client = kafka.Client;

var client = new Client();
var consumer = new Consumer(client, 
    [{topic: 'topic1', autoCommit: false},{topic: 'topic2', autoCommit: false},{topic: 'topic74'}],
    'group0');

consumer.on('message', function (message) {
    console.log('group0--->',message);
});
consumer.on('error', function (err) {
    console.log(err);
})
