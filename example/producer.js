var kafka = require('../kafka'),
    Producer = kafka.Producer,
    KeyedMessage = kafka.KeyedMessage,
    Client = kafka.Client,
    client = new Client('localhost:2181');

var argv = require('optimist').argv;
var topic = argv.topic || 'topic1';
var p = argv.p || 0;
var a = argv.a || 0;
var count = argv.count || 1, rets = 0;
var producer = new Producer(client);

producer.on('ready', function () {
   send([
     'hello',
     new KeyedMessage('keyed', 'keyed message')
   ]);
});

producer.on('error', function (err) {
    console.log('error', err)
})

function send(messages) {
    for (var i = 0; i < count; i++) {
        producer.send([
            {topic: topic, messages: messages , partition: p, attributes: a}
        ], function (err, data) {
            if (err) console.log(arguments);
            if (++rets === count) process.exit();
        });
    }
}
