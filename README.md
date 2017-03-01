Kafka-node
==========

[![Build Status](https://travis-ci.org/SOHU-Co/kafka-node.svg?branch=master)](https://travis-ci.org/SOHU-Co/kafka-node)
[![Coverage Status](https://coveralls.io/repos/github/SOHU-Co/kafka-node/badge.svg?branch=master)](https://coveralls.io/github/SOHU-Co/kafka-node?branch=master)

[![NPM](https://nodei.co/npm/kafka-node.png)](https://nodei.co/npm/kafka-node/)
[![NPM](https://nodei.co/npm-dl/kafka-node.png?height=3)](https://nodei.co/npm/kafka-node/)


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
  - [ConsumerGroup](#consumergroup)
  - [Offset](#offset)
- [Troubleshooting / FAQ](#troubleshooting--faq)
  - [HighLevelProducer with KeyedPartitioner errors on first send](#highlevelproducer-with-keyedpartitioner-errors-on-first-send)
  - [How do I debug an issue?](#how-do-i-debug-an-issue)
  - [How do I get a list of all topics?](#how-do-i-get-a-list-of-all-topics)
  - [For a new consumer how do I start consuming from the latest message in a partition?](#for-a-new-consumer-how-do-i-start-consuming-from-the-latest-message-in-a-partition)
  - [FailedToRebalanceConsumerError: Exception: NODE_EXISTS[-110]](#failedtorebalanceconsumererror-exception-node_exists-110)
  - [HighLevelConsumer does not consume on all partitions](#highlevelconsumer-does-not-consume-on-all-partitions)
  - [How to throttle messages / control the concurrency of processing messages](#how-to-throttle-messages--control-the-concurrency-of-processing-messages)
  - [How do I produce and consume binary data?](#how-do-i-produce-and-consume-binary-data)
  - [What are these node-gyp and snappy errors?](#what-are-these-node-gyp-and-snappy-errors)
  - [How do I configure the log output?](#how-do-i-configure-the-log-output)
- [Running Tests](#running-tests)
- [LICENSE - "MIT"](#license---mit)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Features
* Consumer and High Level Consumer
* Producer and High Level Producer
* Manage topic Offsets
* SSL connections to brokers (Kafka 0.9+)
* Consumer Groups managed by Kafka coordinator (Kafka 0.9+)

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
### Producer(client, [options], [customPartitioner])
* `client`: client which keeps a connection with the Kafka server.
* `options`: options for producer,

```js
{
    // Configuration for when to consider a message as acknowledged, default 1
    requireAcks: 1,
    // The amount of time in milliseconds to wait for all acks before considered, default 100ms
    ackTimeoutMs: 100,
    // Partitioner type (default = 0, random = 1, cyclic = 2, keyed = 3, custom = 4), default 0
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
### HighLevelProducer(client, [options], [customPartitioner])
* `client`: client which keeps a connection with the Kafka server. Round-robins produce requests to the available topic partitions
* `options`: options for producer,

```js
{
    // Configuration for when to consider a message as acknowledged, default 1
    requireAcks: 1,
    // The amount of time in milliseconds to wait for all acks before considered, default 100ms
    ackTimeoutMs: 100,
    // Partitioner type (default = 0, random = 1, cyclic = 2, keyed = 3, custom = 4), default 2
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
⚠️ ***This consumer has been deprecated in the latest version of Kafka (0.10.1) and is likely to be removed in the future. Please use the ConsumerGroup instead.***

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

## ConsumerGroup

The new consumer group uses Kafka broker coordinators instead of Zookeeper to manage consumer groups. This is supported in **Kafka version 0.9** and above only.

### Coming from the highLevelConsumer

API is very similar to `HighLevelConsumer` with some exceptions noted below:

* In an effort to make the API simpler you no longer need to create a `client` this is done inside the `ConsumerGroup`
* consumer ID do not need to be defined. There's a new ID to represent consumers called *member ID* and this is assigned to consumer after joining the group
* Offsets, group members, and ownership details are not stored in Zookeeper
* `ConsumerGroup` does not emit a `registered` event

### ConsumerGroup(options, topics)

```js
var options = {
  host: 'zookeeper:2181',
  zk : undefined,   // put client zk settings if you need them (see Client)
  batch: undefined, // put client batch settings if you need them (see Client)
  ssl: true, // optional (defaults to false) or tls options hash
  groupId: 'ExampleTestGroup',
  sessionTimeout: 15000,
  // An array of partition assignment protocols ordered by preference.
  // 'roundrobin' or 'range' string for built ins (see below to pass in custom assignment protocol)
  protocol: ['roundrobin'],

  // Offsets to use for new groups other options could be 'earliest' or 'none' (none will emit an error if no offsets were saved)
  // equivalent to Java client's auto.offset.reset
  fromOffset: 'latest', // default

  // how to recover from OutOfRangeOffset error (where save offset is past server retention) accepts same value as fromOffset
  outOfRangeOffset: 'earliest', // default
  migrateHLC: false,    // for details please see Migration section below
  migrateRolling: true
};

var consumerGroup = new ConsumerGroup(options, ['RebalanceTopic', 'RebalanceTest']);

// Or for a single topic pass in a string

var consumerGroup = new ConsumerGroup(options, 'RebalanceTopic');
```

### Custom Partition Assignment Protocol

You can pass a custom assignment strategy to the `protocol` array with the interface:

#### string :: name
#### integer :: version
#### object :: userData
#### function :: assign (topicPartition, groupMembers, callback)
**topicPartition**

```json
{
  "RebalanceTopic": [
    "0",
    "1",
    "2"
  ],
  "RebalanceTest": [
    "0",
    "1",
    "2"
  ]
}
```

**groupMembers**

```json
[
  {
    "subscription": [
      "RebalanceTopic",
      "RebalanceTest"
    ],
    "version": 0,
    "id": "consumer1-8db1b117-61c6-4f91-867d-20ccd1ad8b3d"
  },
  {
    "subscription": [
      "RebalanceTopic",
      "RebalanceTest"
    ],
    "version": 0,
    "id": "consumer3-bf2d11f4-1c73-4a39-b498-cfe76eb65bea"
  },
  {
    "subscription": [
      "RebalanceTopic",
      "RebalanceTest"
    ],
    "version": 0,
    "id": "consumer2-9781058e-fad4-40e8-a69c-69afbae05184"
  }
]
```

**callback(error, result)**

***result***

```json
[
  {
    "memberId": "consumer3-bf2d11f4-1c73-4a39-b498-cfe76eb65bea",
    "topicPartitions": {
      "RebalanceTopic": [
        "2"
      ],
      "RebalanceTest": [
        "2"
      ]
    },
    "version": 0
  },
  {
    "memberId": "consumer2-9781058e-fad4-40e8-a69c-69afbae05184",
    "topicPartitions": {
      "RebalanceTopic": [
        "1"
      ],
      "RebalanceTest": [
        "1"
      ]
    },
    "version": 0
  },
  {
    "memberId": "consumer1-8db1b117-61c6-4f91-867d-20ccd1ad8b3d",
    "topicPartitions": {
      "RebalanceTopic": [
        "0"
      ],
      "RebalanceTest": [
        "0"
      ]
    },
    "version": 0
  }
]
```


### Auto migration from the v0.8 based highLevelConsumer

We have two options for automatic migration from existing `highLevelConsumer` group. This is useful to preserve the previous committed offsets for your group.

We support two use cases:

1. You have downtime and your old HLC consumers are no longer available
2. Where the old HLC group is still up and working and you are doing a rolling deploy with zero downtime

For case 1 use below setting:

```js
{
	migrateHLC: true, // default is false
	migrateRolling: false // default is true
}
```

For case 2 setting `migrateRolling` to `true` will allow the ConsumerGroup to start monitoring `zk` nodes for when topic ownership are relinquished by the old HLC consumer. Once this is done the ConsumerGroup will connect and the previous HLC offsets from zookeeper will be migrated automatically to the new Kafka broker based coordinator.

* Group name should be consistent with old highLevelConsumer
* Should never overwrite existing offsets
* Only offsets for Topics that were once in the highLevelConsumer will be migrated over offsets for new topics will follow the `fromOffset` setting


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

### fetchEarliestOffsets(topics, cb)

Example

```js
	var partition = 0;
	var topic = 't';
	offset.fetchEarliestOffsets([topic], function (error, offsets) {
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

## How do I get a list of all topics?

Call `client.loadMetadataForTopics` with a blank topic array to get the entire list of available topics (and available brokers).

```js
client.once('connect', function () {
	client.loadMetadataForTopics([], function (error, results) {
	  if (error) {
	  	return console.error(error);
	  }
	  console.log('%j', _.get(results, '1.metadata'));
	});
});
```

## For a new consumer how do I start consuming from the latest message in a partition?

If you are using the new `ConsumerGroup` simply set `'latest'` to `fromOffset` option.

Otherwise:

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

Alternatively, you can avoid this issue entirely by omitting the HLC's `id` and a unique one will be generated for you.

Reference issue [#90](https://github.com/SOHU-Co/kafka-node/issues/90)

## HighLevelConsumer does not consume on all partitions

Your partition will be stuck if the `fetchMaxBytes` is smaller than the message produced.  Increase `fetchMaxBytes` value should resolve this issue.

Reference to issue [#339](https://github.com/SOHU-Co/kafka-node/issues/339)

## How to throttle messages / control the concurrency of processing messages

1. Create a `async.queue` with message processor and concurrency of one (the message processor itself is wrapped with `setImmediate` so it will not freeze up the event loop)
2. Set the `queue.drain` to resume the consumer
3. The handler for consumer's `message` event pauses the consumer and pushes the message to the queue.

## How do I produce and consume binary data?

### Consume
In the consumer set the `encoding` option to `buffer`.

### Produce
Set the `messages` attribute in the `payload` to a `Buffer`. `TypedArrays` such as `Uint8Array` are not supported and need to be converted to a `Buffer`.

```js
{
 messages: Buffer.from(data.buffer)
}
```

Reference to issue [#470](https://github.com/SOHU-Co/kafka-node/issues/470) [#514](https://github.com/SOHU-Co/kafka-node/issues/514)

## What are these node-gyp and snappy errors?

Snappy is a optional compression library. Windows users have reported issues with installing it while running `npm install`. It's **optional** in kafka-node and can be skipped by using the `--no-optional` flag (though errors from it should not fail the install).

```bash
npm install kafka-node --no-optional --save
```

Keep in mind if you try to use snappy without installing it `kafka-node` will throw a runtime exception.

## How do I configure the log output?

By default, `kafka-node` uses [debug](https://github.com/visionmedia/debug) to log important information. To integrate `kafka-node`'s log output into an application, it is possible to set a logger provider. This enables filtering of log levels and easy redirection of output streams.

### What is a logger provider?

A logger provider is a function which takes the name of a logger and returns a logger implementation. For instance, the following code snippet shows how a logger provider for the global `console` object could be written:

```javascript
function consoleLoggerProvider (name) {
  // do something with the name
  return {
    debug: console.debug.bind(console),
    info: console.info.bind(console),
    warn: console.warn.bind(console),
    error: console.error.bind(console)
  };
}
```

The logger interface with its `debug`, `info`, `warn` and `error` methods expects format string support as seen in `debug` or the JavaScript `console` object. Many commonly used logging implementations cover this API, e.g. bunyan, pino or winston.

### How do I set a logger provider?

For performance reasons, initialization of the `kafka-node` module creates all necessary loggers. This means that custom logger providers need to be set *before requiring the `kafka-node` module*. The following example shows how this can be done:

```javascript
// first configure the logger provider
const kafkaLogging = require('kafka-node/logging');
kafkaLogging.setLoggerProvider(consoleLoggerProvider);

// then require kafka-node and continue as normal
const kafka = require('kafka-node');
```

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
