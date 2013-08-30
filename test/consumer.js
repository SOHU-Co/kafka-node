var Consumer = require('../lib/consumer')
    , util = require('./util.js')
    , config = require('./config.json');

var consumer = new Consumer([], config.zookeeper)
    , total = config.topicNum * config.msgNum
    , topics = []
    , step = 7999
    , firstTopics = true
    , count = 0;

function onMessage(data) {
    if (!(++count % step)) {
        console.log('msg count:', count);
    }
    if (count == total) {
        console.log('complete!');
//        process.exit()
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
            consumer.fetch();
            console.log('fetch start!')
        }
    });
    topics = [];
}

// test script
for (var i = 0; i < config.topicNum; i++) {
    topics.push({topic: util.md5(i.toString()), autoCommit: false});
    /*
     if (!(i % step)) {
     subTopics()
     }
     */
}

topics.length && subTopics();

