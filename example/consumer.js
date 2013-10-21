'use strict';
/*
require('nodetime').profile({
    accountKey: '5bc30ec5e93c4bb817c647e6f14f528f912e706a', 
    appName: 'Node.js Application'
});
*/

//var agent = require('webkit-devtools-agent');
//require('look').start();

var kafka = require('../kafka');
var Consumer = kafka.Consumer;
var Producer = kafka.Producer;
var Client = kafka.Client;

var client = new Client('192.168.105.113:2181/kafka0.8');
var topics = [
        //{topic: 'topic1'},
        //{topic: 't2'},
        //{topic: 'topic75'},
        //{topic: 'new_1'}, 
        //
        {topic: '1-news'} 
    ],
    options = { autoCommit: false, fromBeginning: false, fetchMaxWaitMs: 5000, groupId: 'hello' };


function createConsumer() {
    var consumer = new Consumer(client, topics, options);
    consumer.on('message', function (message) {
        //console.log(this.id, message);
    });
    consumer.on('error', function (err) {
        console.log(err);
    });
    consumer.on('offsetOutOfRange', function (err) {
        console.log(this.id, err);
    })
}

createConsumer();

//setTimeout(createConsumer, 400);

//setTimeout(createConsumer, 400);
