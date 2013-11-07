'use strict';

var kafka = require('../kafka');
var Client = kafka.Client;
var Offset = kafka.Offset;
var offset = new Offset(new Client());
var total = 30000;
var count = 0;

function fetch (cb) {
    offset.fetch([
        {topic: 't2', partition: 0, maxNum: 2},
        { topic: 'topic2', offset: 100, partition: 0 }],
        cb );
}

function commit (cb) {
    offset.commit('group-offset',
        [ 
            { topic: 't2', offset: 10, partition: 0 },
            { topic: 'topic2', offset: 100, partition: 0 },
        ], cb);
}

function test() {
    console.time('fetch');
    for (var i=0; i<total; i++) {
        fetch(function (err, data) {
            if (++count === total) console.timeEnd('fetch');    
        });
    }
}

fetch(function () { 
    test();
});
