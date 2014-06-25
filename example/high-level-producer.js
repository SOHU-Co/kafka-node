var kafka = require('../kafka');
var HighLevelProducer = kafka.HighLevelProducer;
var Client = kafka.Client;
var client = new Client();
var argv = require('optimist').argv;
var topic = argv.topic || 'topic1';
var count = 3, rets = 0;
var producer = new HighLevelProducer(client);

producer.on('ready', function () {
    send('hello');
    setTimeout(function () {
        send('world');
        send('world');
    }, 2000);
});

function send(message) {
    producer.send([
        {topic: topic, messages: [message] }
    ], function (err, data) {
        if (err) console.log(arguments);
        if (++rets === count) process.exit();
    });
}