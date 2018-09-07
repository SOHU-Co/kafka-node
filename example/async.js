'use strict';

const kafka = require('..');

const options = {
  kafkaHost: 'localhost:9092',
  groupId: 'kafka-node-demo'
};

function doSomethingAsync () {
  return new Promise(resolve => {
    setTimeout(() => resolve(), 50 + Math.random() * 200);
  });
}

const consumer = new kafka.AsyncConsumerGroup(options, 'kafka-node-demo');
consumer.on('error', err => {
  console.error('error: unexpected error from consumer', err);
});

//
// `consumer.consume(async msg => ...)` also works, automatically invoking
// ack() when the promise resolves.
//
consumer.consume(msg => {
  console.log(JSON.stringify(msg));

  //
  // just demonstrate that we're processing messages asynchronously
  //
  doSomethingAsync().then(() => msg.ack());
});

console.log('waiting for messages to arrive ¯\\_(ツ)_/¯');

function exitGracefully () {
  console.log('closing consumer');
  consumer.close()
    .then(() => {
      console.log('consumer closed, exiting');
      process.exit(0);
    });
}

process.once('SIGTERM', exitGracefully);
process.once('SIGINT', exitGracefully);
