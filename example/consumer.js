'use strict';

var kafka = require('../kafka');
var Consumer = kafka.Consumer;
var Producer = kafka.Producer;
var Client = kafka.Client;

var client = new Client();
var topics = [
        {topic: 'topic1'},
        {topic: 'new_1'} 
    ],
    options = { autoCommit: false, fromBeginning: false, fetchMaxWaitMs: 5000 };

function createConsumer() {
    var consumer = new Consumer(client, topics, options);
    consumer.on('message', function (message) {
        console.log(this.id, message);
    });
    consumer.on('error', function (err) {
        console.log(err);
    });
    consumer.on('offsetOutOfRange', function (err) {
        console.log(this.id, err);
    })
}

createConsumer();
