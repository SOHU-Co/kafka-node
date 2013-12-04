Kafka-node
==========

Kafka-node is a nodejs client with zookeeper integration for apache Kafka, only support the latest version of kafka 0.8 which is still under development, so this module
is `not production ready` so far.
Zookeeper does the following jobs:

* Load broker metadata from zookeeper before we can communicate with kafka server
* Watch broker state, if broker changed, client will refresh broker and topic metadata stored in client

# Install kafka
Follow the [instructions](https://cwiki.apache.org/KAFKA/kafka-08-quick-start.html) on the Kafka wiki to build Kafka 0.8 and get a test broker up and running.

# API
## Client
### Client(connectionString, clientId)
* `connectionString`: zookeeper connection string, default `localhost:2181/kafka0.8`
* `clientId`: This is a user supplied identifier for the client application, default `kafka-node-client`

## Producer
### Producer(client)
* `client`: client which keep connect with kafka server.

``` js
var kafka = require('kafka-node'),
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
var kafka = require('kafka-node'),
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
var kafka = require('kafka-node'),
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
### Consumer(client, payloads, options)
* `client`: client which keep connect with kafka server.
* `payloads`: **Array**,array of `FetchRequest`, `FetchRequest` is a JSON object like:

``` js
{
   topic: 'topicName',
   partition: '0', //default 0 
   offset: 0, //default 0 
}
```

* `options`: options for consumer,

```js
{
    groupId: 'kafka-node-group',//consumer group id, deafult `kafka-node-group`
    // Auto commit config 
    autoCommit: true,
    autoCommitIntervalMs: 5000,
    // The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued, default 100ms
    fetchMaxWaitMs: 100,
    // This is the minimum number of bytes of messages that must be available to give a response, default 1 byte
    fetchMinBytes: 1,
    // The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
    fetchMaxBytes: 1024 * 10, 
    // If set true, consumer will fetch message from the given offset in the payloads 
    fromOffset: false
}
```
Example:

``` js
var kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    client = new kafka.Client(),
    consumer = new Consumer(
        client,
        [
            { topic: 't', partition: 0 }, { topic: 't1', partition: 1 }
        ],
        {
            autoCommit: false
        }
    );
```

### on('message', onMessage);
By default, we will consume message from the last committed offset of the current group

* `onMessage`: **Function**, callback when new message comes

Example:

``` js
consumer.on('message', function (message) {
    console.log(message);
});
```

### on('error', function (err) {})


### on('offsetOutOfRange', function (err) {})


### addTopics(topics, cb)
Add topics to current consumer, if any topic to be added not exists, return error
* `topics`: **Array**, array of topics to add
* `cb`: **Function**,the callback

Example:

``` js
consumer.addTopics(['t1', 't2'], function (err, added) {
});
```

### removeTopics(topics, cb)
* `topics`: **Array**, array of topics to remove 
* `cb`: **Function**, the callback

Example:

``` js
consumer.removeTopics(['t1', 't2'], function (err, removed) {
});
```

### commit(cb)
Commit offset of the current topics manually, this method should be called when a consumer leaves

* `cb`: **Function**, the callback

Example:

``` js
consumer.commit(function(err, data) {
});
```

### setOffset(topic, partition, offset)
Set offset of the given topic

* `topic`: **String**

* `partition`: **Number**

* `offset`: **Number**

Example:

``` js
consumer.setOffset('topic', 0, 0); 
```

### close(force)
* `force`: **Boolean**, if set true, it force commit current offset before close, default false

Example

```js
consumer.close(true);
```

## Offset 
### Offset(client)
* `client`: client which keep connect with kafka server.

### fetch(payloads, cb)

* `payloads`: **Array**,array of `OffsetRequest`, `OffsetRequest` is a JSON object like:

``` js
{
   topic: 'topicName',
   partition: '0', //default 0
   time: Date.now(), //used to ask for all messages before a certain time (ms), not support negative,default Date.now() 
   maxNum: 1 //default 1
}
```

* `cb`: *Function*, the callback

Example

```js
var kafka = require('kafka'),
    client = new kafka.Client(),
    offset = new kafka.Offset(client);
    offset.fetch([
        { tiopic: 't', partition: 0, time: Date.now(), maxNum: 1 } 
    ], function (err, data) {
        // data
        // { 't': { '0': [999] } }
    });
```

### commit(groupId, payloads, cb)
* `groupId`: consumer group
* `payloads`: **Array**,array of `OffsetCommitRequest`, `OffsetCommitRequest` is a JSON object like:

``` js
{
   topic: 'topicName',
   partition: '0', //default 0
   offset: 1,
   metadata: 'm', //default 'm'
}
```

Example

```js
var kafka = require('kafka-node'),
    client = new kafka.Client(),
    offset = new kafka.Offset(client);
    offset.commit('groupId', [
        { tiopic: 't', partition: 0, offset: 10 } 
    ], function (err, data) {
    });
```

# Todo
* Compression: gzip & snappy

# LICENSE - "MIT"
Copyright (c) 2013 Sohu.com

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions: 

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
