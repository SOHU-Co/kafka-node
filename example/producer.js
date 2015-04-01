var kafka = require('..');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var Client = kafka.Client;
var client = new Client('localhost:2181');
var argv = require('optimist').argv;
var topic = argv.topic || 'topic1';
var p = argv.p || 0;
var a = argv.a || 0;
var producer = new Producer(client, { requireAcks: 1 });

producer.on('ready', function () {
    var message = 'a message';
    var keyedMessage = new KeyedMessage('keyed', 'a keyed message');

    producer.send([
        { topic: topic, partition: p, messages: [message, keyedMessage], attributes: a }
    ], function (err, result) {
        console.log(err || result);
        process.exit();
    });
});

producer.on('error', function (err) {
    console.log('error', err)
});
