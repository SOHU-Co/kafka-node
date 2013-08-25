'use strict';

var Consumer = require('../lib/consumer');
var Producer = require('../lib/producer');

var consumer = new Consumer([{topic: '10i-0'}, {topic: '11'}]);
consumer.on('message', function (messages) {
    console.log(messages.length);
});
consumer.on('error', function (err) {
    console.log('--------->',err);
})
