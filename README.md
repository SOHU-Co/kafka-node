Kafka node client
================

This is nodejs client for Kafka-0.8 with zookeeper integration
# API
## Producer
### Producer(connectionString, clientId)
* `connectionString`: zookeeper connection string, default `localhost:2181`
* `clientId`: This is a user supplied identifier for the client application, default `kafka-node-client`

``` js
    var producer = new require('./kafka').Producer();
```

### send(payloads, cb)
* `payloads`: **Array**,array of `ProduceRequest`, `ProduceRequest` is a JSON object like:

``` js
    {
       topic: 'topicName',
       message: 'message body',
       partition: '0', //default 0
    }
```

* `cb`: **Function**, the callback
Example:

``` js
    var producer = new require('./kafka').Producer(),
        payloads = [
            { topic: 'topic1', message: 'hi', partition: 0 },
            { topic: 'topic2', message: 'hello' }
        ];
    producer.on('ready', function () {
        producer.send(payloads, function (err, data) {
            console.log(data);
        });
    })
```

### createTopics(topics, async, cb)
This method is used to create topics in kafka server, only work when kafka server set `auto.create.topics.enable` true, our client simply send a metadata request to let server auto crate topics. when `async` set false, this method not return util all topics are created, otherwise return immediately.
* `topics`: **Array**,array of topics
* `async`: **Boolean**,async or sync
* `cb`: **Function**,the callback
Example:

``` js
    var producer = new require('./kafka').Producer();
    // Create topics sync
    producer.createTopics(['t','t1'],false, function (err, data) {
        console.log(data);
    });
    // Create topics async
    producer.createTopics(['t'], true, function () {err, data});
    producer.createTopics(['t'], function (err, data) {});// Simply omit 2nd arg
```

## Consumer
### Consumer(payloads, connectionString, goupId, clientId)
* `payloads`: **Array**,array of `FetchRequest`, `FetchRequest` is a JSON object like:

``` js
    {
       topic: 'topicName',
       partition: '0', //default 0
       autoCommit: true, //default true
       commitIntervalMs: 5000 //default 5s
    }
```

* `groupId`: *String*, consumet group id, deafult `kafka-node-group`
* `clientId`: This is a user supplied identifier for the client application, default `kafka-node-client`
Example:

``` js
    var Consumer = require('./kafka').Consumer,
        consumer = new Consumer(
            [
                { topic: 't', partition: 0 }, { topic: 't1', partition: 1 }
            ],
            'my-group'
        );
```

### on('message', onMessage);
* `onMessage`: **Function**, callback when new message comes
Example:

``` js
    consumer.on('message', function (message) {
        console.log(message);
    });
```

### on('error', function (err) {})
### addTopics(topics, cb)
Add topics to current consumer, if any topic to be added not exists, return error
* `topics`, **Array**, array of topics to add
* `cb`, **Function**,the callback
Example:

``` js
    consumer.addTopics(['t1', 't2'], function (err, added) {
    });
```

### removeTopics(topics, cb)
* `topics`, **Array**, array of topics to remove 
* `cb`, **Function**,the callback
Example:

``` js
    consumer.removeTopics(['t1', 't2'], function (err, removed) {
    });
```

### commit(cb)
Commit current offset of current topics manually, this method should be called when a consumer leaves
* `cb`, **Function**, the callback
Example:

``` js
consumer.commit(function(err, data) {
});
```
