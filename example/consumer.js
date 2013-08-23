'use strict';

var Consumer = require('../lib/consumer');
var Producer = require('../lib/producer');

var consumer = new Consumer('localhost-t', 'localhost', 9092, 't-topic', [0]);
consumer.on('message', function (messages) {
    console.log('hihihihi',messages);
});

/*
var producer = new Producer('test-client','localhost',9092, 't-topic');
setInterval(function () {
producer.send('hello world',function (data) {
    console.log('send success....');
});

}, 100);
*/

//consumer.metadataForTopic()
