Kafka-node
==========

[![Build Status](https://travis-ci.org/SOHU-Co/kafka-node.svg?branch=master)](https://travis-ci.org/SOHU-Co/kafka-node)
[![Coverage Status](https://coveralls.io/repos/github/SOHU-Co/kafka-node/badge.svg?branch=master)](https://coveralls.io/github/SOHU-Co/kafka-node?branch=master)

[![NPM](https://nodei.co/npm/kafka-node.png)](https://nodei.co/npm/kafka-node/)
<!--[![NPM](https://nodei.co/npm-dl/kafka-node.png?height=3)](https://nodei.co/npm/kafka-node/)-->


Kafka-node is a Node.js client for Apache Kafka 0.9 and later.

# Table of Contents
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Features](#features)
- [Install Kafka](#install-kafka)
- [API](#api)
  - [KafkaClient](#kafkaclient)
  - [Producer](#producer)
  - [HighLevelProducer](#highlevelproducer)
  - [ProducerStream](#producerstream)
  - [Consumer](#consumer)
  - [ConsumerStream](#consumerstream)
  - [ConsumerGroup](#consumergroup)
  - [ConsumerGroupStream](#consumergroupstream)
  - [Offset](#offset)
  - [Admin](#admin)
- [Troubleshooting / FAQ](#troubleshooting--faq)
  - [HighLevelProducer with KeyedPartitioner errors on first send](#highlevelproducer-with-keyedpartitioner-errors-on-first-send)
  - [How do I debug an issue?](#how-do-i-debug-an-issue)
  - [For a new consumer how do I start consuming from the latest message in a partition?](#for-a-new-consumer-how-do-i-start-consuming-from-the-latest-message-in-a-partition)
  - [ConsumerGroup does not consume on all partitions](#consumergroup-does-not-consume-on-all-partitions)
  - [How to throttle messages / control the concurrency of processing messages](#how-to-throttle-messages--control-the-concurrency-of-processing-messages)
  - [How do I produce and consume binary data?](#how-do-i-produce-and-consume-binary-data)
  - [What are these node-gyp and snappy errors?](#what-are-these-node-gyp-and-snappy-errors)
  - [How do I configure the log output?](#how-do-i-configure-the-log-output)
  - [Error: Not a message set. Magic byte is 2](#error-not-a-message-set-magic-byte-is-2)
- [Running Tests](#running-tests)
- [LICENSE - "MIT"](#license---mit)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Features
* Consumer
* Producer and High Level Producer
* Node Stream Producer (Kafka 0.9+)
* Node Stream Consumers (ConsumerGroupStream Kafka 0.9+)
* Manage topic Offsets
* SSL connections to brokers (Kafka 0.9+)
* SASL/PLAIN Authentication (Kafka 0.10+)
* Consumer Groups managed by Kafka coordinator (Kafka 0.9+)
* Connect directly to brokers (Kafka 0.9+)
* Administrative APIs
	* List Groups
	* Describe Groups
	* Create Topics

# Install Kafka
Follow the [instructions](http://kafka.apache.org/documentation.html#quickstart) on the Kafka wiki to build Kafka and get a test broker up and running.

# API

## KafkaClient

New KafkaClient connects directly to Kafka brokers.

### Options
* `kafkaHost` : A string of kafka broker/host combination delimited by comma for example: `kafka-1.us-east-1.myapp.com:9093,kafka-2.us-east-1.myapp.com:9093,kafka-3.us-east-1.myapp.com:9093` default: `localhost:9092`.
* `connectTimeout` : in ms it takes to wait for a successful connection before moving to the next host default: `10000`
* `requestTimeout` : in ms for a kafka request to timeout default: `30000`
* `autoConnect` : automatically connect when KafkaClient is instantiated otherwise you need to manually call `connect` default: `true`
* `connectRetryOptions` : object hash that applies to the initial connection. see [retry](https://www.npmjs.com/package/retry) module for these options.
* `idleConnection` : allows the broker to disconnect an idle connection from a client (otherwise the clients continues to O after being disconnected). The value is elapsed time in ms without any data written to the TCP socket. default: 5 minutes
* `reconnectOnIdle` : when the connection is closed due to client idling, client will attempt to auto-reconnect. default: true
* `maxAsyncRequests` : maximum async operations at a time toward the kafka cluster. default: 10
* `sslOptions`: **Object**, options to be passed to the tls broker sockets, ex. `{ rejectUnauthorized: false }` (Kafka 0.9+)
* `sasl`: **Object**, SASL authentication configuration (only SASL/PLAIN is currently supported), ex. `{ mechanism: 'plain', username: 'foo', password: 'bar' }` (Kafka 0.10+)

### Example

```javascript
const client = new kafka.KafkaClient({kafkaHost: '10.3.100.196:9092'});
```

## Producer
### Producer(KafkaClient, [options], [customPartitioner])
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
    client = new kafka.KafkaClient(),
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
   key: 'theKey', // string or buffer, only needed when using keyed partitioner
   partition: 0, // default 0
   attributes: 2, // default: 0
   timestamp: Date.now() // <-- defaults to Date.now() (only available with kafka v0.10+)
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
    client = new kafka.KafkaClient(),
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

### createTopics(topics, cb)
This method is used to create topics on the Kafka server. It requires Kafka 0.10+.

* `topics`: **Array**, array of topics
* `cb`: **Function**, the callback

Example:

``` js
var kafka = require('kafka-node');
var client = new kafka.KafkaClient();

var topicsToCreate = [{
  topic: 'topic1',
  partitions: 1,
  replicationFactor: 2
},
{
  topic: 'topic2',
  partitions: 5,
  replicationFactor: 3,
  // Optional set of config entries
  configEntries: [
    {
      name: 'compression.type',
      value: 'gzip'
    },
    {
      name: 'min.compaction.lag.ms',
      value: '50'
    }
  ],
  // Optional explicit partition / replica assignment
  // When this property exists, partitions and replicationFactor properties are ignored
  replicaAssignment: [
    {
      partition: 0,
      replicas: [3, 4]
    },
    {
      partition: 1,
      replicas: [2, 1]
    }
  ]
}];

client.createTopics(topicsToCreate, (error, result) => {
  // result is an array of any errors if a given topic could not be created
});

```

## HighLevelProducer
### HighLevelProducer(KafkaClient, [options], [customPartitioner])
* `client`: client which keeps a connection with the Kafka server. Round-robins produce requests to the available topic partitions
* `options`: options for producer,

```js
{
    // Configuration for when to consider a message as acknowledged, default 1
    requireAcks: 1,
    // The amount of time in milliseconds to wait for all acks before considered, default 100ms
    ackTimeoutMs: 100
}
```

``` js
var kafka = require('kafka-node'),
    HighLevelProducer = kafka.HighLevelProducer,
    client = new kafka.KafkaClient(),
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
   key: 'theKey', // string or buffer, only needed when using keyed partitioner
   attributes: 1,
   timestamp: Date.now() // <-- defaults to Date.now() (only available with kafka v0.10 and KafkaClient only)
}
```

* `cb`: **Function**, the callback

Example:

``` js
var kafka = require('kafka-node'),
    HighLevelProducer = kafka.HighLevelProducer,
    client = new kafka.KafkaClient(),
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

### createTopics(topics, async, cb)
This method is used to create topics on the Kafka server. It only work when `auto.create.topics.enable`, on the Kafka server, is set to true. Our client simply sends a metadata request to the server which will auto create topics. When `async` is set to false, this method does not return until all topics are created, otherwise it returns immediately.

* `topics`: **Array**,array of topics
* `async`: **Boolean**,async or sync
* `cb`: **Function**,the callback

Example:

``` js
var kafka = require('kafka-node'),
    HighLevelProducer = kafka.HighLevelProducer,
    client = new kafka.KafkaClient(),
    producer = new HighLevelProducer(client);
// Create topics sync
producer.createTopics(['t','t1'], false, function (err, data) {
    console.log(data);
});
// Create topics async
producer.createTopics(['t'], true, function (err, data) {});
producer.createTopics(['t'], function (err, data) {});// Simply omit 2nd arg
```

## ProducerStream

### ProducerStream (options)

#### Options
* `highWaterMark` size of write buffer (Default: 100)
* `kafkaClient` options see [KafkaClient](#kafkaclient)
* `producer` options for Producer see [HighLevelProducer](#highlevelproducer)

### Streams Example

In this example we demonstrate how to stream a source of data (from `stdin`) to kafka (`ExampleTopic` topic) for processing. Then in a separate instance (or worker process) we consume from that kafka topic and use a `Transform` stream to update the data and stream the result to a different topic using a `ProducerStream`.

> Stream text from `stdin` and write that into a Kafka Topic

```js
const Transform = require('stream').Transform;
const ProducerStream = require('./lib/producerStream');
const _ = require('lodash');
const producer = new ProducerStream();

const stdinTransform = new Transform({
  objectMode: true,
  decodeStrings: true,
  transform (text, encoding, callback) {
    text = _.trim(text);
    console.log(`pushing message ${text} to ExampleTopic`);
    callback(null, {
      topic: 'ExampleTopic',
      messages: text
    });
  }
});

process.stdin.setEncoding('utf8');
process.stdin.pipe(stdinTransform).pipe(producer);
```

> Use `ConsumerGroupStream` to read from this topic and transform the data and feed the result of into the `RebalanceTopic` Topic.

```js
const ProducerStream = require('./lib/producerStream');
const ConsumerGroupStream = require('./lib/consumerGroupStream');
const resultProducer = new ProducerStream();

const consumerOptions = {
  kafkaHost: '127.0.0.1:9092',
  groupId: 'ExampleTestGroup',
  sessionTimeout: 15000,
  protocol: ['roundrobin'],
  asyncPush: false,
  id: 'consumer1',
  fromOffset: 'latest'
};

const consumerGroup = new ConsumerGroupStream(consumerOptions, 'ExampleTopic');

const messageTransform = new Transform({
  objectMode: true,
  decodeStrings: true,
  transform (message, encoding, callback) {
    console.log(`Received message ${message.value} transforming input`);
    callback(null, {
      topic: 'RebalanceTopic',
      messages: `You have been (${message.value}) made an example of`
    });
  }
});

consumerGroup.pipe(messageTransform).pipe(resultProducer);
```

## Consumer
### Consumer(client, payloads, options)
* `client`: client which keeps a connection with the Kafka server. **Note**: it's recommend that create new client for different consumers.
* `payloads`: **Array**,array of `FetchRequest`, `FetchRequest` is a JSON object like:

``` js
{
   topic: 'topicName',
   offset: 0, //default 0
   partition: 0 // default 0
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
    encoding: 'utf8',
    keyEncoding: 'utf8'
}
```
Example:

``` js
var kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    client = new kafka.KafkaClient(),
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

## ConsumerStream

`Consumer` implemented using node's `Readable` stream interface. Read more about streams [here](https://nodejs.org/dist/v8.1.3/docs/api/stream.html#stream_readable_streams).

### Notes

* streams are consumed in chunks and in `kafka-node` each chunk is a kafka message
* a stream contains an internal buffer of messages fetched from kafka. By default the buffer size is `100` messages and can be changed through the `highWaterMark` option

### Compared to Consumer

Similar API as `Consumer` with some exceptions. Methods like `pause` and `resume` in `ConsumerStream` respects the toggling of flow mode in a Stream. In `Consumer` calling `pause()` just paused the fetch cycle and will continue to emit `message` events. Pausing in a `ConsumerStream` should immediately stop emitting `data` events.

### ConsumerStream(client, payloads, options)

## ConsumerGroup

### ConsumerGroup(options, topics)

```js
var options = {
  kafkaHost: 'broker:9092', // connect directly to kafka broker (instantiates a KafkaClient)
  batch: undefined, // put client batch settings if you need them
  ssl: true, // optional (defaults to false) or tls options hash
  groupId: 'ExampleTestGroup',
  sessionTimeout: 15000,
  // An array of partition assignment protocols ordered by preference.
  // 'roundrobin' or 'range' string for built ins (see below to pass in custom assignment protocol)
  protocol: ['roundrobin'],
  encoding: 'utf8', // default is utf8, use 'buffer' for binary data

  // Offsets to use for new groups other options could be 'earliest' or 'none' (none will emit an error if no offsets were saved)
  // equivalent to Java client's auto.offset.reset
  fromOffset: 'latest', // default
  commitOffsetsOnFirstJoin: true, // on the very first time this consumer group subscribes to a topic, record the offset returned in fromOffset (latest/earliest)
  // how to recover from OutOfRangeOffset error (where save offset is past server retention) accepts same value as fromOffset
  outOfRangeOffset: 'earliest', // default
  // Callback to allow consumers with autoCommit false a chance to commit before a rebalance finishes
  // isAlreadyMember will be false on the first connection, and true on rebalances triggered after that
  onRebalance: (isAlreadyMember, callback) => { callback(); } // or null
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

### commit(force, cb)
Commit offset of the current topics manually, this method should be called when a consumer leaves

* `force`: **Boolean**, force a commit even if there's a pending commit, default false (optional)
* `cb`: **Function**, the callback

Example:

``` js
consumer.commit(function(err, data) {
});
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

## ConsumerGroupStream

The `ConsumerGroup` wrapped with a `Readable` stream interface. Read more about consuming `Readable` streams [here](https://nodejs.org/dist/v8.1.3/docs/api/stream.html#stream_readable_streams).

Same notes in the Notes section of [ConsumerStream](#consumerstream) applies to this stream.

### Auto Commit

`ConsumerGroupStream` manages auto commits differently than `ConsumerGroup`. Whereas the `ConsumerGroup` would automatically commit offsets of fetched messages the `ConsumerGroupStream` will only commit offsets of consumed messages from the stream buffer. This will be better for most users since it more accurately represents what was actually "Consumed". The interval at which auto commit fires off is still controlled by the `autoCommitIntervalMs` option and this feature can be disabled by setting `autoCommit` to `false`.

### ConsumerGroupStream (consumerGroupOptions, topics)

* `consumerGroupOptions` same options to initialize a `ConsumerGroup`
* `topics` a single or array of topics to subscribe to

### commit(message, force, callback)
This method can be used to commit manually when `autoCommit` is set to `false`.

* `message` the original message or an object with `{topic, partition, offset}`
* `force` a commit even if there's a pending commit
* `callback` (*optional*)

### close(callback)
Closes the `ConsumerGroup`. Calls `callback` when complete. If `autoCommit` is enabled calling close will also commit offsets consumed from the buffer.

## Offset
### Offset(client)
* `client`: client which keeps a connection with the Kafka server.

### events
* `ready`: when all brokers are discovered
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
    client = new kafka.KafkaClient(),
    offset = new kafka.Offset(client);
    offset.fetch([
        { topic: 't', partition: 0, time: Date.now(), maxNum: 1 }
    ], function (err, data) {
        // data
        // { 't': { '0': [999] } }
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
    client = new kafka.KafkaClient(),
    offset = new kafka.Offset(client);
    offset.fetchCommitsV1('groupId', [
        { topic: 't', partition: 0 }
    ], function (err, data) {
    });
```

### fetchCommitsV1(groupid, payloads, cb)

Alias of `fetchCommits`.

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

## Admin

This class provides administrative APIs can be used to monitor and administer the Kafka cluster.

### Admin (KafkaClient)
* `kafkaClient`: client which keeps a connection with the Kafka server.

### listGroups(cb)

List the consumer groups managed by the kafka cluster.

* `cb`: **Function**, the callback

Example:

```js
const client = new kafka.KafkaClient();
const admin = new kafka.Admin(client); // client must be KafkaClient
admin.listGroups((err, res) => {
  console.log('consumerGroups', res);
});
```

Result:

```js
consumerGroups { 'console-consumer-87148': 'consumer',
  'console-consumer-2690': 'consumer',
  'console-consumer-7439': 'consumer'
}
```

### describeGroups(consumerGroups, cb)

Fetch consumer group information from the cluster. See result for detailed information.

* `consumerGroups`: **Array**, array of consumer groups (which can be gathered from `listGroups`)
* `cb`: **Function**, the callback

Example:

```js
admin.describeGroups(['console-consumer-2690'], (err, res) => {
  console.log(JSON.stringify(res,null,1));
})
```

Result:

```json
{
 "console-consumer-2690": {
  "members": [
   {
    "memberId": "consumer-1-20195e12-cb3b-4ba4-9076-e7da8ed0d57a",
    "clientId": "consumer-1",
    "clientHost": "/192.168.61.1",
    "memberMetadata": {
     "subscription": [
      "twice-tt"
     ],
     "version": 0,
     "userData": "JSON parse error",
     "id": "consumer-1-20195e12-cb3b-4ba4-9076-e7da8ed0d57a"
    },
    "memberAssignment": {
     "partitions": {
      "twice-tt": [
       0,
       1
      ]
     },
     "version": 0,
     "userData": "JSON Parse error"
    }
   }
  ],
  "error": null,
  "groupId": "console-consumer-2690",
  "state": "Stable",
  "protocolType": "consumer",
  "protocol": "range",
  "brokerId": "4"
 }
}
```

### listTopics(cb)

List the topics managed by the kafka cluster.

* `cb`: **Function**, the callback

Example:

```js
const client = new kafka.KafkaClient();
const admin = new kafka.Admin(client);
admin.listTopics((err, res) => {
  console.log('topics', res);
});
```

Result:

```js
[
  {
    "1001": {
      "nodeId": 1001,
      "host": "127.0.0.1",
      "port": 9092
    }
  },
  {
    "metadata": {
      "my-test-topic": {
        "0": {
          "topic": "my-test-topic",
          "partition": 0,
          "leader": 1001,
          "replicas": [
            1001
          ],
          "isr": [
            1001
          ]
        },
        "1": {
          "topic": "my-test-topic",
          "partition": 1,
          "leader": 1001,
          "replicas": [
            1001
          ],
          "isr": [
            1001
          ]
        }
      }
    },
    "clusterMetadata": {
      "controllerId": 1001
    }
  }
]
```

### createTopics(topics, cb)

```js
var topics = [{
  topic: 'topic1',
  partitions: 1,
  replicationFactor: 2
}];
admin.createTopics(topics, (err, res) => {
  // result is an array of any errors if a given topic could not be created
})
```

See [createTopics](#createtopicstopics-cb)

### describeConfigs(payload, cb)

Fetch the configuration for the specified resources. It requires Kafka 0.11+.

* `payload`: **Array**, array of resources
* `cb`: **Function**, the callback

Example:

```js
const resource = {
  resourceType: admin.RESOURCE_TYPES.topic,   // 'broker' or 'topic'
  resourceName: 'my-topic-name',
  configNames: []           // specific config names, or empty array to return all,
}

const payload = {
  resources: [resource],
  includeSynonyms: false   // requires kafka 2.0+
};

admin.describeConfigs(payload, (err, res) => {
  console.log(JSON.stringify(res,null,1));
})
```

Result:

```json
[
 {
  "configEntries": [
   {
    "synonyms": [],
    "configName": "compression.type",
    "configValue": "producer",
    "readOnly": false,
    "configSource": 5,
    "isSensitive": false
   },
   {
    "synonyms": [],
    "configName": "message.format.version",
    "configValue": "0.10.2-IV0",
    "readOnly": false,
    "configSource": 4,
    "isSensitive": false
   },
   {
    "synonyms": [],
    "configName": "file.delete.delay.ms",
    "configValue": "60000",
    "readOnly": false,
    "configSource": 5,
    "isSensitive": false
   },
   {
    "synonyms": [],
    "configName": "leader.replication.throttled.replicas",
    "configValue": "",
    "readOnly": false,
    "configSource": 5,
    "isSensitive": false
   },
   {
    "synonyms": [],
    "configName": "max.message.bytes",
    "configValue": "1000012",
    "readOnly": false,
    "configSource": 5,
    "isSensitive": false
   },
    ...
  ],
  "resourceType": "2",
  "resourceName": "my-topic-name"
 }
]

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

If you are using the new `ConsumerGroup` simply set `'latest'` to `fromOffset` option.

Otherwise:

1. Call `offset.fetchLatestOffsets` to get fetch the latest offset
2. Consume from returned offset

Reference issue [#342](https://github.com/SOHU-Co/kafka-node/issues/342)


## ConsumerGroup does not consume on all partitions

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

## Error: Not a message set. Magic byte is 2

If you are receiving this error in your consumer double check the `fetchMaxBytes` configuration. If set too low the broker could start sending fetch responses in RecordBatch format instead of MessageSet.


# Running Tests

### Install Docker

On the Mac install [Docker for Mac](https://docs.docker.com/engine/installation/mac/).

### Start Docker and Run Tests

```bash
npm test
```

### Using different versions of Kafka

Achieved using the `KAFKA_VERSION` environment variable.

```bash
# Runs "latest" kafka on docker hub*
npm test

# Runs test against other versions:

KAFKA_VERSION=0.9 npm test

KAFKA_VERSION=0.10 npm test

KAFKA_VERSION=0.11 npm test

KAFKA_VERSION=1.0 npm test

KAFKA_VERSION=1.1 npm test

KAFKA_VERSION=2.0 npm test
```

*See Docker hub [tags](https://hub.docker.com/r/wurstmeister/kafka/tags/) entry for which version is considered `latest`.

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
