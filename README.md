Kafka-node
==========

[![NPM](https://nodei.co/npm/kafka-node.png)](https://nodei.co/npm/kafka-node/)
[![NPM](https://nodei.co/npm-dl/kafka-node.png?height=3)](https://nodei.co/npm/kafka-node/)
[![Build Status](https://travis-ci.org/SOHU-Co/kafka-node.svg?branch=master)](https://travis-ci.org/SOHU-Co/kafka-node)
[![Coverage Status](https://coveralls.io/repos/github/SOHU-Co/kafka-node/badge.svg?branch=master)](https://coveralls.io/github/SOHU-Co/kafka-node?branch=master)

Kafka-node is a Node.js client with Zookeeper integration for Apache Kafka 0.8.1 and later.

# Table of Contents
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Features](#features)
- [Install Kafka](#install-kafka)
- [API](#api)
  - [Client](#client)
  - [Producer](#producer)
  - [HighLevelProducer](#highlevelproducer)
  - [Consumer](#consumer)
  - [HighLevelConsumer](#highlevelconsumer)
  - [Offset](#offset)
- [Troubleshooting / FAQ](#troubleshooting--faq)
  - [HighLevelProducer with KeyedPartitioner errors on first send](#highlevelproducer-with-keyedpartitioner-errors-on-first-send)
  - [How do I debug an issue?](#how-do-i-debug-an-issue)
  - [For a new consumer how do I start consuming from the latest message in a partition?](#for-a-new-consumer-how-do-i-start-consuming-from-the-latest-message-in-a-partition)
  - [FailedToRebalanceConsumerError: Exception: NODE_EXISTS[-110]](#failedtorebalanceconsumererror-exception-node_exists-110)
  - [HighLevelConsumer does not consume on all partitions](#highlevelconsumer-does-not-consume-on-all-partitions)
  - [How to throttle messages / control the concurrency of processing messages](#how-to-throttle-messages--control-the-concurrency-of-processing-messages)
  - [How do I consume binary data?](#how-do-i-consume-binary-data)
- [Running Tests](#running-tests)
- [LICENSE - "MIT"](#license---mit)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Features
* Consumer and High Level Consumer
* Producer and High Level Producer
* Manage topic Offsets
* SSL connections to brokers (Kafka 0.9+)

# Install Kafka
Follow the [instructions](http://kafka.apache.org/documentation.html#quickstart) on the Kafka wiki to build Kafka 0.8 and get a test broker up and running.

# API
## Client
### Client(connectionString, clientId, [zkOptions], [noAckBatchOptions], [sslOptions])
* `connectionString`: Zookeeper connection string, default `localhost:2181/`
* `clientId`: This is a user-supplied identifier for the client application, default `kafka-node-client`
* `zkOptions`: **Object**, Zookeeper options, see [node-zookeeper-client](https://github.com/alexguan/node-zookeeper-client#client-createclientconnectionstring-options)
* `noAckBatchOptions`: **Object**, when requireAcks is disabled on Producer side we can define the batch properties, 'noAckBatchSize' in bytes and 'noAckBatchAge' in milliseconds. The default value is `{ noAckBatchSize: null, noAckBatchAge: null }` and it acts as if there was no batch
* `sslOptions`: **Object**, options to be passed to the tls broker sockets, ex. { rejectUnauthorized: false } (Kafka +0.9)

### close(cb)
Closes the connection to Zookeeper and the brokers so that the node process can exit gracefully.

* `cb`: **Function**, the callback

## Producer
### Producer(client, [options])
* `client`: client which keeps a connection with the Kafka server.
* `options`: options for producer,

```js
{
    // Configuration for when to consider a message as acknowledged, default 1
    requireAcks: 1,
    // The amount of time in milliseconds to wait for all acks before considered, default 100ms
    ackTimeoutMs: 100,
    // Partitioner type (default = 0, random = 1, cyclic = 2, keyed = 3), default 0
    partitionerType: 2
}
```

``` js
var kafka = require('kafka-node'),
    Producer = kafka.Producer,
    client = new kafka.Client(),
    producer = new Producer(client);
```

### Events

- `ready`: this event is emitted when producer is ready to send messages.
- `error`: this is the error event propagates from internal client, producer should always listen it.

### send(payloads, cb)
* `payloads`: **Array**,array of `ProduceRequest`, `ProduceRequest` is a JSON object like:

``` js
{
   topic: 'topicName',
   messages: ['message body'], // multi messages should be a array, single message can be just a string or a KeyedMessage instance
   key: 'theKey', // only needed when using keyed partitioner
   partition: 0, // default 0
   attributes: 2 // default: 0
}
```

* `cb`: **Function**, the callback

`attributes` controls compression of the message set. It supports the following values:

* `0`: No compression
* `1`: Compress using GZip
* `2`: Compress using snappy

Example:

```js
var kafka = require('kafka-node'),
    Producer = kafka.Producer,
    KeyedMessage = kafka.KeyedMessage,
    client = new kafka.Client(),
    producer = new Producer(client),
    km = new KeyedMessage('key', 'message'),
    payloads = [
        { topic: 'topic1', messages: 'hi', partition: 0 },
        { topic: 'topic2', messages: ['hello', 'world', km] }
    ];
producer.on('ready', function () {
    producer.send(payloads, function (err, data) {
        console.log(data);
    });
});

producer.on('error', function (err) {})
```
> ⚠️**WARNING**: Batch multiple messages of the same topic/partition together as an array on the `messages` attribute otherwise you may lose messages!

### createTopics(topics, async, cb)
This method is used to create topics on the Kafka server. It only works when `auto.create.topics.enable`, on the Kafka server, is set to true. Our client simply sends a metadata request to the server which will auto create topics. When `async` is set to false, this method does not return until all topics are created, otherwise it returns immediately.

* `topics`: **Array**, array of topics
* `async`: **Boolean**, async or sync
* `cb`: **Function**, the callback

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


## HighLevelProducer
### HighLevelProducer(client, [options])
* `client`: client which keeps a connection with the Kafka server. Round-robins produce requests to the available topic partitions
* `options`: options for producer,

```js
{
    // Configuration for when to consider a message as acknowledged, default 1
    requireAcks: 1,
    // The amount of time in milliseconds to wait for all acks before considered, default 100ms
    ackTimeoutMs: 100,
    // Partitioner type (default = 0, random = 1, cyclic = 2, keyed = 3), default 2
    partitionerType: 3
}
```

``` js
var kafka = require('kafka-node'),
    HighLevelProducer = kafka.HighLevelProducer,
    client = new kafka.Client(),
    producer = new HighLevelProducer(client);
```

### Events

- `ready`: this event is emitted when producer is ready to send messages.
- `error`: this is the error event propagates from internal client, producer should always listen it.

### send(payloads, cb)
* `payloads`: **Array**,array of `ProduceRequest`, `ProduceRequest` is a JSON object like:

``` js
{
   topic: 'topicName',
   messages: ['message body'], // multi messages should be a array, single message can be just a string,
   key: 'theKey', // only needed when using keyed partitioner
   attributes: 1
}
```

* `cb`: **Function**, the callback

Example:

``` js
var kafka = require('kafka-node'),
    HighLevelProducer = kafka.HighLevelProducer,
    client = new kafka.Client(),
    producer = new HighLevelProducer(client),
    payloads = [
        { topic: 'topic1', messages: 'hi' },
        { topic: 'topic2', messages: ['hello', 'world'] }
    ];
producer.on('ready', function () {
    producer.send(payloads, function (err, data) {
        console.log(data);
    });
});
```
> ⚠️**WARNING**: Batch multiple messages of the same topic/partition together as an array on the `messages` attribute otherwise you may lose messages!

### createTopics(topics, async, cb)
This method is used to create topics on the Kafka server. It only work when `auto.create.topics.enable`, on the Kafka server, is set to true. Our client simply sends a metadata request to the server which will auto create topics. When `async` is set to false, this method does not return until all topics are created, otherwise it returns immediately.

* `topics`: **Array**,array of topics
* `async`: **Boolean**,async or sync
* `cb`: **Function**,the callback

Example:

``` js
var kafka = require('kafka-node'),
    HighLevelProducer = kafka.HighLevelProducer,
    client = new kafka.Client(),
    producer = new HighLevelProducer(client);
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
* `client`: client which keeps a connection with the Kafka server. **Note**: it's recommend that create new client for different consumers.
* `payloads`: **Array**,array of `FetchRequest`, `FetchRequest` is a JSON object like:

``` js
{
   topic: 'topicName',
   offset: 0, //default 0
}
```

* `options`: options for consumer,

```js
{
    groupId: 'kafka-node-group',//consumer group id, default `kafka-node-group`
    // Auto commit config
    autoCommit: true,
    autoCommitIntervalMs: 5000,
    // The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued, default 100ms
    fetchMaxWaitMs: 100,
    // This is the minimum number of bytes of messages that must be available to give a response, default 1 byte
    fetchMinBytes: 1,
    // The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
    fetchMaxBytes: 1024 * 1024,
    // If set true, consumer will fetch message from the given offset in the payloads
    fromOffset: false,
    // If set to 'buffer', values will be returned as raw buffer objects.
    encoding: 'utf8'
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
By default, we will consume messages from the last committed offset of the current group

* `onMessage`: **Function**, callback when new message comes

Example:

``` js
consumer.on('message', function (message) {
    console.log(message);
});
```

### on('error', function (err) {})


### on('offsetOutOfRange', function (err) {})


### addTopics(topics, cb, fromOffset)
Add topics to current consumer, if any topic to be added not exists, return error
* `topics`: **Array**, array of topics to add
* `cb`: **Function**,the callback
* `fromOffset`: **Boolean**, if true, the consumer will fetch message from the specified offset, otherwise it will fetch message from the last commited offset of the topic.

Example:

``` js
consumer.addTopics(['t1', 't2'], function (err, added) {
});

or

consumer.addTopics([{ topic: 't1', offset: 10 }], function (err, added) {
}, true);
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

### pause()
Pause the consumer. ***Calling `pause` does not automatically stop messages from being emitted.*** This is because pause just stops the kafka consumer fetch loop. Each iteration of the fetch loop can obtain a batch of messages (limited by `fetchMaxBytes`).

### resume()
Resume the consumer. Resumes the fetch loop.

### pauseTopics(topics)
Pause specify topics

```
consumer.pauseTopics([
    'topic1',
    { topic: 'topic2', partition: 0 }
]);
```

### resumeTopics(topics)
Resume specify topics

```
consumer.resumeTopics([
    'topic1',
    { topic: 'topic2', partition: 0 }
]);
```

### close(force, cb)
* `force`: **Boolean**, if set to true, it forces the consumer to commit the current offset before closing, default `false`

Example

```js
consumer.close(true, cb);
consumer.close(cb); //force is disabled
```

## HighLevelConsumer
### HighLevelConsumer(client, payloads, options)
* `client`: client which keeps a connection with the Kafka server.
* `payloads`: **Array**,array of `FetchRequest`, `FetchRequest` is a JSON object like:

``` js
{
   topic: 'topicName'
}
```

* `options`: options for consumer,

```js
{
    // Consumer group id, default `kafka-node-group`
    groupId: 'kafka-node-group',
    // Optional consumer id, defaults to groupId + uuid
    id: 'my-consumer-id',
    // Auto commit config
    autoCommit: true,
    autoCommitIntervalMs: 5000,
    // The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued, default 100ms
    fetchMaxWaitMs: 100,
    // This is the minimum number of bytes of messages that must be available to give a response, default 1 byte
    fetchMinBytes: 1,
    // The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
    fetchMaxBytes: 1024 * 1024,
    // If set true, consumer will fetch message from the given offset in the payloads
    fromOffset: false,
    // If set to 'buffer', values will be returned as raw buffer objects.
    encoding: 'utf8'
}
```
Example:

``` js
var kafka = require('kafka-node'),
    HighLevelConsumer = kafka.HighLevelConsumer,
    client = new kafka.Client(),
    consumer = new HighLevelConsumer(
        client,
        [
            { topic: 't' }, { topic: 't1' }
        ],
        {
            groupId: 'my-group'
        }
    );
```

### on('message', onMessage);
By default, we will consume messages from the last committed offset of the current group

* `onMessage`: **Function**, callback when new message comes

Example:

``` js
consumer.on('message', function (message) {
    console.log(message);
});
```

### on('error', function (err) {})


### on('offsetOutOfRange', function (err) {})

### addTopics(topics, cb, fromOffset)
Add topics to current consumer, if any topic to be added not exists, return error
* `topics`: **Array**, array of topics to add
* `cb`: **Function**,the callback
* `fromOffset`: **Boolean**, if true, the consumer will fetch message from the specified offset, otherwise it will fetch message from the last commited offset of the topic.

Example:

``` js
consumer.addTopics(['t1', 't2'], function (err, added) {
});

or

consumer.addTopics([{ topic: 't1', offset: 10 }], function (err, added) {
}, true);
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

### pause()
Pause the consumer. ***Calling `pause` does not automatically stop messages from being emitted.*** This is because pause just stops the kafka consumer fetch loop. Each iteration of the fetch loop can obtain a batch of messages (limited by `fetchMaxBytes`).

### resume()
Resume the consumer. Resumes the fetch loop.

### close(force, cb)
* `force`: **Boolean**, if set to true, it forces the consumer to commit the current offset before closing, default `false`

Example:

```js
consumer.close(true, cb);
consumer.close(cb); //force is disabled
```

## Offset
### Offset(client)
* `client`: client which keeps a connection with the Kafka server.

### events
* `ready`: when zookeeper is ready
* `connect` when broker is ready

### fetch(payloads, cb)
Fetch the available offset of a specific topic-partition

* `payloads`: **Array**,array of `OffsetRequest`, `OffsetRequest` is a JSON object like:

``` js
{
   topic: 'topicName',
   partition: 0, //default 0
   // time:
   // Used to ask for all messages before a certain time (ms), default Date.now(),
   // Specify -1 to receive the latest offsets and -2 to receive the earliest available offset.
   time: Date.now(),
   maxNum: 1 //default 1
}
```

* `cb`: *Function*, the callback

Example

```js
var kafka = require('kafka-node'),
    client = new kafka.Client(),
    offset = new kafka.Offset(client);
    offset.fetch([
        { topic: 't', partition: 0, time: Date.now(), maxNum: 1 }
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
   partition: 0, //default 0
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
        { topic: 't', partition: 0, offset: 10 }
    ], function (err, data) {
    });
```

### fetchCommits(groupid, payloads, cb)
Fetch the last committed offset in a topic of a specific consumer group

* `groupId`: consumer group
* `payloads`: **Array**,array of `OffsetFetchRequest`, `OffsetFetchRequest` is a JSON object like:

``` js
{
   topic: 'topicName',
   partition: 0 //default 0
}
```

Example

```js
var kafka = require('kafka-node'),
    client = new kafka.Client(),
    offset = new kafka.Offset(client);
    offset.fetchCommits('groupId', [
        { topic: 't', partition: 0 }
    ], function (err, data) {
    });
```

### fetchLatestOffsets(topics, cb)

Example

```js
	var partition = 0;
	var topic = 't';
	offset.fetchLatestOffsets([topic], function (error, offsets) {
		if (error)
			return handleError(error);
		console.log(offsets[topic][partition]);
	});
```


# Troubleshooting / FAQ

## HighLevelProducer with KeyedPartitioner errors on first send

Error:

```
BrokerNotAvailableError: Could not find the leader
```

Call `client.refreshMetadata()` before sending the first message. Reference issue [#354](https://github.com/SOHU-Co/kafka-node/issues/354)



## How do I debug an issue?
This module uses the [debug module](https://github.com/visionmedia/debug) so you can just run below before starting your app.

```bash
export DEBUG=kafka-node:*
```


## For a new consumer how do I start consuming from the latest message in a partition?

1. Call `offset.fetchLatestOffsets` to get fetch the latest offset
2. Consume from returned offset

Reference issue [#342](https://github.com/SOHU-Co/kafka-node/issues/342)


## FailedToRebalanceConsumerError: Exception: NODE_EXISTS[-110]

This error can occur when a HLC is killed and restarted quickly. The ephemeral nodes linked to the previous session are not relinquished in zookeeper when `SIGINT` is sent and instead relinquished when zookeeper session timeout is reached. The timeout can be adjusted using the `sessionTimeout` zookeeper option when the `Client` is created (the default is 30000ms).

Example handler:

```js
process.on('SIGINT', function () {
    highLevelConsumer.close(true, function () {
        process.exit();
    });
});
```

Alternatively, you to avoid this issue entirely by omitting the HLC's `id` and a unique one will be generated for you.

Reference issue [#90](https://github.com/SOHU-Co/kafka-node/issues/90)

## HighLevelConsumer does not consume on all partitions

Your partition will be stuck if the `fetchMaxBytes` is smaller than the message produced.  Increase `fetchMaxBytes` value should resolve this issue.

Reference to issue [#339](https://github.com/SOHU-Co/kafka-node/issues/339) 

## How to throttle messages / control the concurrency of processing messages

1. Create a `async.queue` with message processor and concurrency of one (the message processor itself is wrapped with `setImmediate` so it will not freeze up the event loop)
2. Set the `queue.drain` to resume the consumer
3. The handler for consumer's `message` event pauses the consumer and pushes the message to the queue.

## How do I consume binary data?

In the consumer set the `encoding` option to `buffer` there not settings on producers needed for this.

Reference to issue [#470](https://github.com/SOHU-Co/kafka-node/issues/470)

# Running Tests

### Install Docker

On the Mac install [Docker for Mac](https://docs.docker.com/engine/installation/mac/).

### Start Docker and Run Tests

```bash
npm test
```

### Stop Docker

```bash
npm run stopDocker
```


# LICENSE - "MIT"
Copyright (c) 2015 Sohu.com

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
