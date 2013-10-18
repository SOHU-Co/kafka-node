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
var consumer = new Consumer(
    client, 
    [
        //{topic: 'topic1'},
        //{topic: 'topic2'},
        //{topic: 'topic75'},
        {topic: 'new_1'}, 
        {topic: 'none_1'} 
    ],
    { autoCommit: true, fromBeginning: false, fetchMaxWaitMs: 3000 }
);

consumer.on('message', function (message) {
    console.log(message);
});
consumer.on('error', function (err) {
    console.log(err);
});
consumer.on('offsetOutOfRange', function (err) {
    console.log(err);
})
