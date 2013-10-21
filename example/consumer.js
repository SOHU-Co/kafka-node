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

var client = new Client();
var topics = [
        //{topic: 'topic1'},
        //{topic: 'topic2'},
        //{topic: 'topic75'},
        //{topic: 'new_1'}, 
        //
        {topic: 'none_1'} 
    ],
    options = { autoCommit: false, fromBeginning: false, fetchMaxWaitMs: 30000 , groupId: 'bug_group' };


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

setTimeout(createConsumer, 400);

setTimeout(createConsumer, 400);
