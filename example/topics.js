var Producer = require('../lib/producer'),
    Consumer = require('../lib/consumer');


function createTopics() {
    var producer = new Producer('localhost:2181')
    producer.createTopics(['t17', 't18', 't19', 't21'],false, function (err, data) {
        console.log(data);
    });
}

var consumer = new Consumer([{topic: 'topic7'}]);
function addTopics() {
    //consumer.on('error', function (err) { console.log(err) });
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
    consumer.removeTopics(['topic3', 'topic4'], function (err, data) {
        console.log(data);
    }); 
}

addTopics();

setTimeout(removeTopics, 5000);

//createTopics();
