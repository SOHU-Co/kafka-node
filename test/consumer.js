var Consumer = require('../lib/consumer')
    , util = require('./util.js')
    , config = require('./config.json');

var consumer = new Consumer([{topic: 'topic3', autoCommit: false}], config.zookeeper)
    , total = config.topicNum * config.msgNum
    , topics = []
    , step = 100
    , firstTopics = true
    , count = 0;

function onMessage(message) {
    if (!(++count % step)) {
        //console.log('msg count:', count);
        console.log(message.topic, message.offset)
    }
    if (count == total*2) {
        console.log('complete!');
        //process.exit()
    }
    if (count > total*2) {
       console.log(message.offset)
    }
}
consumer.on('message', onMessage);
consumer.on('error', function (err) {
    console.log(err)
});

function subTopics() {
    consumer.addTopics(topics, function () {
        if (firstTopics) {
            firstTopics = false;
            //consumer.fetch();
            console.log('fetch start!')
        }
    });
    topics = [];
}

// test script
for (var i = 0; i < config.topicNum; i++) {
    topics.push({topic: util.md5(i.toString()), autoCommit: false});
    !(i % step) && subTopics()
}

topics.length && subTopics();

