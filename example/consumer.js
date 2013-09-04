'use strict';

var kafka = require('../kafka');
var Consumer = kafka.Consumer;
var Producer = kafka.Producer;
var Client = kafka.Client;

var client = new Client();
var consumer = new Consumer(client, 
    [{topic: 'topic3', autoCommit: false},{topic: 'topic80', autoCommit: false},{topic: 'topic74'}],
    'group0');

consumer.on('message', function (message) {
    console.log('group0--->',message.value);
});
consumer.on('error', function (err) {
    console.log(err);
})

setTimeout(function () {
    var consumer1 = new Consumer(client, [{ topic: 'topic3', autoCommit: false }], 'group1');

    consumer1.on('message', function (message) {
        console.log('group1--->',message.value);
    });
    consumer1.on('error', function (err) {
        //console.log('--------->',err);
    })
}, 3000);
