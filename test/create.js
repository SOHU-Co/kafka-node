var Producer = require('../lib/producer')
    , util = require('./util.js')
    , config = require('./config.json');

var producer = new Producer('10.16.1.218:2181/kafka0.8')
    , start = 0
    , total = 15000
    , end = total + start
    , topics = []
    , topicsLength = 1
    , count = 0;

producer.on('ready', function () {
    for (var i = start; i < end; i++) {
        topics.push(util.md5(i.toString()));
        if (!(topics.length % topicsLength) || count == total) {
            producer.createTopics(
                topics,
                false,
                function (err, data) {
                    count += topicsLength;
                    console.log('created:', count);
                    if (count >= total) {
                        console.log('complete! created:', total);
                        process.exit();
                    }
                });
            topics = []
        }
    }
});

