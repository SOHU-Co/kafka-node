'use strict';
var Binary = require('binary');
var protocol = require('../../lib/protocol');

var topics = [
    { topic: 'topic1', partition: 0 },
    { topic: 'topic2', partition: 0 },
    { topic: 'topic3', partition: 0 }
];


function encode () {
    return protocol.encodeOffsetFetchRequest('group')('clientId', 8, topics);
}

console.time('encode');
for (var i=0; i<30000; i++) {
    var data = encode();
    var result = test(data);
}

console.timeEnd('encode');

function test(resp) {
    return Binary.parse(resp)
        .word32bu('size')
        .word16bu('apikey')
        .word16bu('version')
        .word32bu('id')
        .word16bu('clientId')
        .tap(function (vars) {
            this.buffer('clientId', vars.clientId);
        })
        .word16bu('group')
        .tap(function (vars) {
            this.buffer('group', vars.group);
        })
        .word32bu('topicNum')
        .loop(function (end, vars) {
            if (--vars.topicNum === 0) end();
            this.word16bu('topic')
                .tap(function (vars) {
                    this.buffer('topic', vars.topic);
                })
                .word32bu('partitionNum')
                .loop(function (end, vars) {
                    if (--vars.partitionNum === 0) end();
                    this.word32bu('partition');
                });
        }).vars;
}
