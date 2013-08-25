'use strict';

var Consumer = require('../lib/consumer'),
    Producer = require('../lib/producer'),
    Client = require('../lib/client');
    
var letters = 'abcdefghijklmnopqrstuvwxyz',
    upcaseLetters = letters.toUpperCase(),
    numbers = '1234567890';

var dictionary = letters + upcaseLetters + numbers;

function createMsg() {
    var digit = 2048 + Math.floor(Math.random()*2048);  
    var charsNum = dictionary.length;
    var n = Math.floor(digit/charsNum);
    var n1 = digit%charsNum;

    var ret = ''
    for(var i=0;i<n;i++) {
        ret += dictionary;
    }
    return ret +  dictionary.slice(n1);
}

var repeat = 5000,
    msgsPerTopic = 512,
    messages = [];

var client = new Client('localhost', 9092);
var producer = new Producer('producer', 'localhost', 9092);

producer.send('helloworld', ['i am a new'], function (data) {
    //console.log(data);
});



function multiMetadata() {
    for(var i=0; i<100; i++) {
        client.loadMetadataForTopics(['99noworld'], function (data) {
            console.log(data);
        });        
    }
}

// send
function batchSend() {
    var messages = []
    for(var i=0; i< 512; i++) {
        messages.push(createMsg());
    }
    producer.send(messages, function (data) {
        console.log('batch', data);
    });
}

var count = 0;
function eachSend0() {
    count ++;
    var msg = createMsg();
    //console.log(msg);
    producer.send('hi-topic', [count+''], function () {
        console.log('each....')
        if (count < 512) {
        process.nextTick(function () {
            eachSend();
        }); 
    }     
    });
}

function eachSend() {
    for(var i=0; i<12200; i++) {
        
        //var msg = createMsg();
    //console.log(msg);
        producer.send('00000-topic', [i+''], function(data) {
                console.log('===============================>',data);
            }(i));
    }
}

function fetch() {
    var consumer = new Consumer('consumer', client, 'hello-topic');
    consumer.on('message', function (data) {
        console.log(data);
    });
}

//multiMetadata();
eachSend();
//fetch();
