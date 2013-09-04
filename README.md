Kafka node client
================

This is a nodejs client for Kafka-0.8 with zookeeper integration.
# API
## Client
### Client(connectionString, clientId)
* `connectionString`: zookeeper connection string, default `localhost:2181/kafka0.8`
* `clientId`: This is a user supplied identifier for the client application, default `kafka-node-client`

## Producer
### Producer(client)
* `client`: client which keep connect with kafka server.

``` js
var kafka = require('kafka'),
    Producer = kafka.Producer,
    client = new kafka.Client(),
    producer = new Producer(client);
```

### send(payloads, cb)
* `payloads`: **Array**,array of `ProduceRequest`, `ProduceRequest` is a JSON object like:

``` js
{
   topic: 'topicName',
   messages: ['message body'],// multi messages should be a array, single message can be just a string
   partition: '0', //default 0
}
```

* `cb`: **Function**, the callback

Example:

``` js
var kafka = require('kafka'),
    Producer = kafka.Producer,
    client = new kafka.Client(),
    producer = new Producer(client),
    payloads = [
        { topic: 'topic1', messages: 'hi', partition: 0 },
        { topic: 'topic2', messages: ['hello', 'world'] }
    ];
producer.on('ready', function () {
    producer.send(payloads, function (err, data) {
        console.log(data);
    });
})
```

### createTopics(topics, async, cb)
This method is used to create topics in kafka server, only work when kafka server set `auto.create.topics.enable` true, our client simply send a metadata request to let server auto crate topics. when `async` set false, this method does not return util all topics are created, otherwise return immediately.

* `topics`: **Array**,array of topics
* `async`: **Boolean**,async or sync
* `cb`: **Function**,the callback

Example:

``` js
var kafka = require('kafka'),
    Producer = kafka.Producer,
    client = new kafka.Client(),
    producer = new Producer(client);
// Create topics sync
producer.createTopics(['t','t1'], false, function (err, data) {
    console.log(data);
});
// Create topics async
producer.createTopics(['t'], true, function (err, data) {});
producer.createTopics(['t'], function (err, data) {});// Simply omit 2nd arg
```

## Consumer
### Consumer(client, payloads, goupId, fetchMaxWaitMs, fetchMinBytes)
* `client`: client which keep connect with kafka server.
* `payloads`: **Array**,array of `FetchRequest`, `FetchRequest` is a JSON object like:

``` js
{
   topic: 'topicName',
   partition: '0', //default 0
   autoCommit: true, //default true
   commitIntervalMs: 5000 //default 5s
}
```

* `groupId`: *String*, consumer group id, deafult `kafka-node-group`
* `fetchMaxWaitTime`: *Number*, the max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued, default 1000ms
* `fetchMinBytes`: *Number*, this is the minimum number of bytes of messages that must be available to give a response, default 1 byte

Example:

``` js
var kafka = require('kafka'),
    Consumer = kafka.Consumer,
    client = new kafka.Client(),
    consumer = new Consumer(
        client,
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
Commit offset of the current topics manually, this method should be called when a consumer leaves

* `cb`, **Function**, the callback

Example:

``` js
consumer.commit(function(err, data) {
});
```

### close(force)
* `force`, **Boolean**, if set true, it force commit current offset before close, default false

Example

```js
consumer.close(true);
```
