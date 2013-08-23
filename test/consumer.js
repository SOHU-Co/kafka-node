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

console.log(digit, n, n1)
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
var producer = new Producer('producer', 'localhost', 9092, 'hello-topic');

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
function fake() {
    count ++;
    var msg = createMsg();
    //console.log(msg);
    producer.send([count+''], function () {
        
    });

    if (count < 512) {
        process.nextTick(function () {
            eachSend();
        }); 
    }
}

function eachSend() {
    for(var i=0; i<512; i++) {
        producer.send([createMsg()], function() {});
    }
}

function fetch() {
    var consumer = new Consumer('umer', 'localhost', 9092, 'magictopic');
    consumer.on('message', function (data) {
        var msg = data[0] || '';
        console.log('------------------------------>', msg);
    });
}

//eachSend();
fetch();
