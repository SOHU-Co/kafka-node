var Producer = require('../lib/producer')
    , Client = require('../lib/client')
    , util = require('./util.js')
    , config = require('./config.json');


var client = new Client(config.zookeeper)
    , producer = new Producer(client)
    , start = 0
    , total = config.topicNum
    , end = total + start
    , topics = []
    , step = 500
    , count = 0;

function createTopics() {
    producer.createTopics(
        topics,
        false,
        function () {
            count += step;
            console.log('created:', count);
            if (count >= total) {
                console.log('complete! created:', total);
                process.exit();
            }
        }
    );

    topics = []
}

producer.on('ready', function () {
    for (var i = start; i < end; i++) {
        topics.push(util.md5(i.toString()));
        !(topics.length % step) && createTopics()
    }
    topics.length && createTopics()
});

