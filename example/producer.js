var kafka = require('../kafka'),
    Producer = kafka.Producer,
    Client = kafka.Client,
    client = new Client(null, null, { metadataRetries: 3, metadataRetriesBackoffMs: 1000, zookeeper: { spinDelay: 1000, connectionRetries: 2 } });

var argv = require('optimist').argv;
var topic = argv.topic || 'topic1';
var p = argv.p || 0;
var count = argv.count || 1, rets = 0;
var producer = new Producer(client, { metadataRefreshIntervalMs: 60000 });
var producerKeyed = new Producer(client, { partitionerType: 2 });

client.on('error', function (err) {
   console.log ('An error has occured: ' + err )
});

producer.on('ready', function () {
   send('hello');
});

producerKeyed.on('ready', function () {
    sendWithKey('hello', 'key');
});

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

function sendWithKey(message, key) {
    for (var i = 0; i < count; i++) {
        producerKeyed.send([
            { key: key, topic: topic, messages: [message] }
        ], function (err, data) {
            if (err) console.log(arguments);
            if (++rets === count) process.exit();
        });
    }
}
