'use strict';

var Consumer = require('../lib/consumer');
var Producer = require('../lib/producer');

var consumer = new Consumer([{topic: 'topic3'},{topic: 'topic80'},{topic: 'topic74'}]);
consumer.on('message', function (messages) {
    console.log(messages.toString());
});
consumer.on('error', function (err) {
    //console.log('--------->',err);
})
