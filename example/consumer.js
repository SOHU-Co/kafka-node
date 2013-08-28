'use strict';

var Consumer = require('../lib/consumer');
var Producer = require('../lib/producer');

var consumer = new Consumer([{topic: 'topic'}]);
consumer.on('message', function (messages) {
    console.log(messages.toString());
});
consumer.on('error', function (err) {
    //console.log('--------->',err);
})

consumer.createTopics(['nnnnnn', 'mmm8'],false, function (err,resp) {
    console.log(resp);
});
