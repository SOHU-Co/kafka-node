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
for (var i=0; i<50000; i++) {
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
        .loop(decodeTopics).vars;
    
    function decodeTopics (end, vars) {
        if (--vars.topicNum === 0) end();
        this.word16bu('topic')
            .tap(function (vars) {
                this.buffer('topic', vars.topic);
            })
            .word32bu('partitionNum')
            .loop(decodePartitions); 
    }

    function decodePartitions (end, vars) {
        if (this.eof()) end();
        this.word32bu('partition'); 
    }
}

function test1(resp) {
    var cur = 4 + 2 + 2 + 4 + 2;
    var vars = Binary.parse(resp)
        .word32bu('size')
        .word16bu('apikey')
        .word16bu('version')
        .word32bu('id')
        .word16bu('clientId')
        .tap(function (vars) {
            cur += vars.clientId;
            this.buffer('clientId', vars.clientId);
        })
        .word16bu('group')
        .tap(function (vars) {
            cur += vars.group;
            this.buffer('group', vars.group);
        })
        .word32bu('topicNum').vars;
        
    cur += 2 + 4;
    resp = resp.slice(cur);
    for (var i=0; i<vars.topicNum; i++) {
        cur = 2;
        var topic = Binary.parse(resp)
            .word16bu('topic')
            .tap(function (vars) {
                cur += vars.topic;
                this.buffer('topic', vars.topic);
            })
            .word32bu('partitionNum').vars;
        resp = resp.slice(cur + 4);

        for (var j=0; j<topic.partitionNum; j++) {
            Binary.parse(resp)
                .word32bu('partition');
            resp = resp.slice(4);
        }
    }

    return vars;
}
