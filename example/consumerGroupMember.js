var async = require('async');
var ConsumerGroup = require('..').ConsumerGroup;

var consumerOptions = {
  kafkaHost: '127.0.0.1:9092',
  groupId: 'ExampleTestGroup',
  sessionTimeout: 15000,
  protocol: ['roundrobin'],
  fromOffset: 'earliest' // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
};

var topics = ['RebalanceTopic', 'RebalanceTest'];

var consumerGroup = new ConsumerGroup(Object.assign({ id: 'consumer1' }, consumerOptions), topics);
consumerGroup.on('error', onError);
consumerGroup.on('message', onMessage);

var consumerGroup2 = new ConsumerGroup(Object.assign({ id: 'consumer2' }, consumerOptions), topics);
consumerGroup2.on('error', onError);
consumerGroup2.on('message', onMessage);
consumerGroup2.on('connect', function () {
  setTimeout(function () {
    consumerGroup2.close(true, function (error) {
      console.log('consumer2 closed', error);
    });
  }, 25000);
});

var consumerGroup3 = new ConsumerGroup(Object.assign({ id: 'consumer3' }, consumerOptions), topics);
consumerGroup3.on('error', onError);
consumerGroup3.on('message', onMessage);

function onError (error) {
  console.error(error);
  console.error(error.stack);
}

function onMessage (message) {
  console.log(
    '%s read msg Topic="%s" Partition=%s Offset=%d',
    this.client.clientId,
    message.topic,
    message.partition,
    message.offset
  );
}

process.once('SIGINT', function () {
  async.each([consumerGroup, consumerGroup2, consumerGroup3], function (consumer, callback) {
    consumer.close(true, callback);
  });
});
