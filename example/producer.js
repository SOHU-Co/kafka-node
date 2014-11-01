var kafka = require('../kafka'),
    Producer = kafka.Producer,
    Client = kafka.Client,
    client = new Client('localhost:2181');

var argv = require('optimist').argv;
var topic = argv.topic || 'topic1';
var p = argv.p || 0;
var count = argv.count || 1, rets = 0;
var producer = new Producer(client);

producer.on('ready', function () {
   send('hello');
});

producer.on('error', function (err) {
    console.log('error', err)
})

function send(message) {
    for (var i = 0; i < count; i++) {
        producer.send([
            {topic: topic, messages: [message] , partition: p}
        ], function (err, data) {
            if (err) console.log(arguments);
            if (++rets === count) process.exit();
        });
    }
}
