var Producer = require('../lib/producer')
    , Client = require('../lib/client')
    , config = require('./config.json')
    , cluster = require('cluster')
    , numCPUs = require('os').cpus().length
    , lineByLineReader = require('line-by-line');

if (cluster.isMaster) {
    // Fork workers.
    for (var i = 0; i < numCPUs; i++) {
        cluster.fork();
    }

    cluster.on('exit', function (worker) {
        console.log('worker ' + worker.process.pid + ' exit');
    });
} else {
    // Workers can share any TCP connection
    // In this case its a HTTP server
    var client = new Client(config.zookeeper)
        , producer = new Producer(client)
        , total = config.topicNum * config.msgNum
        , count = 0;
    producer.on('ready', function () {
        var lr = new lineByLineReader(config.dataSource);
        lr.on('line', function (line) {
            var topic = line.slice(0, line.indexOf(' '));
            var msg = line.slice(line.indexOf(' ') + 1);
            producer.send([
                {topic: topic, messages: msg}
            ], function (err) {
                if (err) console.log(err);
                if (++count == total) {
                    process.exit();
                } else if (!(count % 10000))
                    console.log('sended:', count)
            });
        });
    });
}


