var Producer = require('../lib/producer')
    , config = require('./config.json')
    , lineByLineReader = require('line-by-line');

var producer = new Producer(config.zookeeper)
    , total = config.topicNum * config.msgNum
    , count = 0;

producer.on('ready', function () {
    var lr = new lineByLineReader(config.dataSource);
    lr.on('line', function (line) {
        var topic = line.slice(0, line.indexOf(' '));
        var msg = line.slice(line.indexOf(' ') + 1);
        producer.send([
            {topic: topic, message: msg}
        ], function (err, data) {
            if (err) console.log(err);
            if (++count == total) {
                console.log('complete!');
                process.exit();
            } else if (!(count % 10000))
                console.log('sended:', count)
        });
    });
});


