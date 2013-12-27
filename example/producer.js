var kafka = require('../kafka'),
    Producer = kafka.Producer,
    Client = kafka.Client,
    client = new Client();

var argv = require('optimist').argv;
var topic = argv.topic || 'topic1';

var producer = new Producer(client);

var count = 1, rets = 0;
producer.on('ready', function () {
   //setInterval(send, 1000); 
   send('hello');
});

function createMessage (n) {
    var length = n * 1024;
    var data = 'start';

    for (var i=0; i<length; i++)
        data = data + 'a';

    return data + 'end';
}

function send(message) {
    message = message || createMessage();
    for (var i = 0; i < count; i++) {
        producer.send([
            {topic: topic, messages: [message] }
        ], function (err, data) {
            if (err) console.log(arguments);
            if (++rets === count) process.exit();
        });
    }
}
