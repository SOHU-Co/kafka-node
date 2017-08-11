'use strict';

const Transform = require('stream').Transform;
const ProducerStream = require('..').ProducerStream;
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

const ConsumerGroupStream = require('..').ConsumerGroupStream;
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
