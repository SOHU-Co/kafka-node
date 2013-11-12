'use strict';

var kafka = require('../kafka');
var Client = kafka.Client;
var client = new Client();
var total = 50000;
var assert = require('assert');
var count = 0;

client.on('error', function () {
    console.log(arguments);
}).on('ready', function () {
    console.log('ready');
    test();
});

function fetch (cb) {
    client.loadMetadataForTopics([
        't2',
        'topic2'],
        cb );
}

function test() {
    console.time('fetch');
    for (var i=0; i<total; i++) {
        fetch(function (err, data) {
            if (++count === total) console.timeEnd('fetch');    
        });
    }
}

