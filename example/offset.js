'use strict';

var kafka = require('../kafka');
var Client = kafka.Client;
var Offset = kafka.Offset;
var offset = new Offset(new Client());
var total = 1000;
var count = 0;

function fetch () {
    offset.fetch([{topic: 't2', partition: 0, maxNum: 2}], function (err, data) {
        if (err) console.log(err)
        console.log('offset', data);
    });
}

function commit (cb) {
    offset.commit('group-offset',
        [ 
            { topic: 'topic1', offset: 100, partition: 0 },
            { topic: 'topic2', offset: 100, partition: 0 },
            { topic: 't2', offset: 8, partition: 0 }
        ], cb);
}

console.time('commit');
for (var i=0; i<total; i++) {
    commit(function () {
        if (++count === total) console.timeEnd('commit');    
    });
}
