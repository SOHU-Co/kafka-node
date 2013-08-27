var Producer = require('../lib/producer');

var producer = new Producer();

var letters = 'abcdefghijklmnopqrstuvwxyz',
    upcaseLetters = letters.toUpperCase(),
    numbers = '1234567890';

var dictionary = letters + upcaseLetters + numbers;

function createMsg() {
    var digit = 2048 + Math.floor(Math.random() * 2048);
    var charsNum = dictionary.length;
    var n = Math.floor(digit / charsNum);
    var n1 = digit % charsNum;

    var ret = ''
    for (var i = 0; i < n; i++) {
        ret += dictionary;
    }
    return ret + dictionary.slice(n1);
}
producer.on('ready', function () {
console.log('i am ready');
    for (var i = 0; i < 20; i++) {
        producer.send([
            {topic: 'topic', message: 'woo fuck you' + 1 + 'coolmessage' }
        ], function (err, data) {
            if (err) console.log(err);
        });
    }
});

