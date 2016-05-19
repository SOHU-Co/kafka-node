var kafka = require('..');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var Client = kafka.Client;
var client = new Client('localhost:2181');
var argv = require('optimist').argv;
var topic = argv.topic || 'topic1';
var p = argv.p || 0;
var a = argv.a || 0;

//to test the SmartBuffer class
var produceLargeMessage = argv.largeMsg && argv.largeMessage == "true";
var messageSizeInMb = +(produceLargeMessage ? argv.msgSizeInMb : undefined);

//to create a new topic
var createTopic = argv.createTopic && argv.createTopic == "true";
var producer = new Producer(client, { requireAcks: 1 });

producer.on('ready', function () {
    if (createTopic) {
        producer.createTopics([topic], false, function (err, data) {
            console.log(err || data);
            process.exit();
        });
    }
    else {
        var message = "";
        var keyedMessage;
        if (produceLargeMessage && messageSizeInMb > 0) {
            message = (new Array(sizeInMb * 1024 * 1024)).join("x");
        }
        else {
            message = 'a new message';
            keyedMessage = new KeyedMessage('keyed', 'a keyed message');
        }
        var msgs = keyedMessage ? [message, keyedMessage] : [message];
        producer.send([
            { topic: topic, partition: p, messages: msgs, attributes: a }
        ], function (err, result) {
            console.log(err || result);
            process.exit();
        });
    }
});

producer.on('error', function (err) {
    console.log('error', err)
});