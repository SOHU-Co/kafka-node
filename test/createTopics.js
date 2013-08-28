var Producer = require('../lib/producer')
    , config = require('./config.json');

var producer = new Producer('localhost')
    , start = 19000
    , total = 5000
    , end = total + start
    , count = 0;

producer.on('ready', function () {
    console.log('i am ready');
    for (var i = start; i < end; i++) {
        producer.send([
            {topic: i + '', message: 'hello'}
        ], function (err, data) {
//            if (err) console.log(err);
            if (++count == total) {
                console.log('complete!');
                process.exit();
            } else if (!(count % 100))
                console.log('created:', count)
        })
    }
});


/*
 var zookeeper = require('node-zookeeper-client');
 var client = zookeeper.createClient('localhost:2181');
 var path = '/brokers/topics/mytopic';

 function getData(client, path) {
 client.getData(
 path,
 function (event) {
 console.log('Got event: %s', event);
 getData(client, path);
 },
 function (error, data, stat) {
 if (error) {
 console.log('Error occurred when getting data: %s.', error);
 return;
 }

 console.log(
 'Node: %s has data: %s, version: %d',
 path,
 data ? data.toString() : undefined,
 stat.version
 );
 }
 );
 }

 client.once('connected', function () {
 console.log('Connected to ZooKeeper.');
 getData(client, path);
 });

 client.connect();
 */
