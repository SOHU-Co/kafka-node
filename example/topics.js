var Producer = require('../lib/producer'),
    Consumer = require('../lib/consumer');


function createTopics() {
    var producer = new Producer('localhost:2181')
    producer.createTopics(['t17', 't18', 't19', 't20'],false, function (err, data) {
        console.log(data);
    });
}

function addTopics() {
    var consumer = new Consumer([{topic: 'topic'}]);
    //consumer.on('error', function (err) { console.log(err) });
    consumer.on('message', function (msg) { console.log(msg) });
    consumer.addTopics(
        [{topic: 't18'},
        {topic: 't19'},
        {topic: 't20'},
        {topic: 't17'}],
        function (err, data) {
            if (err) console.log(err);
            console.log(data);}
    );
}

addTopics();
