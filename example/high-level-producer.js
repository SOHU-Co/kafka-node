var kafka = require('..');
var HighLevelProducer = kafka.HighLevelProducer;
var Client = kafka.Client;
var client = new Client();
var argv = require('optimist').argv;
var topic = argv.topic || 'topic1';
var count = 10;
var rets = 0;
var producer = new HighLevelProducer(client);

producer.on('ready', function () {
  setInterval(send, 1000);
});

producer.on('error', function (err) {
  console.log('error', err);
});

function send () {
  var message = new Date().toString();
  producer.send([
    {topic: topic, messages: [message]}
  ], function (err, data) {
    if (err) console.log(err);
    else console.log('send %d messages', ++rets);
    if (rets === count) process.exit();
  });
}
