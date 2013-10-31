var kafka = require('../kafka');
var Consumer = kafka.Consumer;
var Producer = kafka.Producer;
var Client = kafka.Client;

var client = new Client();

function createTopics() {
    var producer = new Producer(client);
    producer.createTopics(['new_100'],false, function (err, data) {
        console.log(data);
    });
}

function addTopics() {
    var consumer = new Consumer(client,[{topic: 'topic7'}]);
    consumer.on('message', function (msg) { console.log(msg) });

    consumer.addTopics(
        [{topic: 'topic5'},
        {topic: 'topic4'},
        {topic: 'topic3'}],
        function (err, data) {
            if (err) console.log(err);
            console.log(data);}
    );
}

function removeTopics() {
    var consumer = new Consumer(client,[{topic: 'topic7'},{topic: 'topic3'},{topic: 'topic4'}]);
    consumer.removeTopics(['topic3', 'topic4'], function (err, data) {
        console.log(data);
    }); 
}

function exit() {
    consumer.commit(function (err, data) {
        console.log(data);
        process.exit();
    });
}

//addTopics();

//setTimeout(removeTopics, 5000);

createTopics();
