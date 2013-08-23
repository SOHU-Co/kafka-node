var Producer = require('../lib/producer'),
    lineByLineReader = require('line-by-line');

var respTimes = 0;

var lr = new lineByLineReader('data.txt');
lr.on('line', function (line) {
    sendLine(line)
});

//192.168.105.223
var producer = new Producer();

function sendLine(line) {
    var topic = line.slice(0, line.indexOf(' '));
    var msg = line.slice(line.indexOf(' ') + 1);
    producer.send([{topic: topic, message: msg}], function (data) {
        console.log('success....>', data.length);
    });    
}
