'use strict';

var kafka = require('..');
var Client = kafka.Client;
var HighLevelProducer = kafka.HighLevelProducer;
var async = require('async');
var debug = require('debug')('kafka-node:Test-Rebalance');
var Childrearer = require('./helpers/Childrearer');
var uuid = require('node-uuid');
var _ = require('lodash');
var host = process.env['KAFKA_TEST_HOST'] || '';

describe('Integrated HLC Rebalance', function () {
  var producer;
  var topic = 'RebalanceTopic';
  var rearer;
  var groupId = 'rebal_group';

  function sendMessage (topic, message, done) {
    producer.send([ {topic: topic, messages: [message]} ], function () {
      debug('sent', message);
      done();
    });
  }

  before(function (done) {
    var client = new Client(host);
    producer = new HighLevelProducer(client);
    client.on('ready', function () {
      client.refreshMetadata([topic], function (data) {
        client.topicPartitions[topic].should.be.length(3);
        done();
      });
    });
  });

  beforeEach(function (done) {
    rearer = new Childrearer();

    // make sure there are no other consumers on this topic before starting test
    producer.client.zk.getConsumersPerTopic(groupId, function (error, data) {
      if (error && error.name === 'NO_NODE') {
        done();
      } else {
        data.consumerTopicMap.should.be.empty;
        data.topicConsumerMap.should.be.empty;
        data.topicPartitionMap.should.be.empty;
        done(error);
      }
    });
  });

  afterEach(function (done) {
    debug('killChildren');
    rearer.closeAll();
    setTimeout(done, 1000);
  });

  function sendMessages (messages, done) {
    async.eachSeries(messages, function (message, cb) {
      sendMessage(topic, message, cb);
    }, function (error, results) {
      if (error) {
        console.error('Send Error', error);
        return done(error);
      }
      debug('all messages sent');
    });
  }

  function getConsumerVerifier (messages, expectedPartitionsConsumed, expectedConsumersConsuming, done) {
    var processedMessages = 0;
    var partitionsConsumed = {};
    var consumedByConsumer = {};

    return function onData (data) {
      debug('From child %d %j', this._childNum, data);
      topic.should.be.eql(data.message.topic);
      if (~messages.indexOf(data.message.value)) {
        processedMessages++;
        partitionsConsumed[data.message.partition] = true;
        consumedByConsumer[data.id] = true;
      }
      if (processedMessages >= messages.length && Object.keys(partitionsConsumed).length === expectedPartitionsConsumed &&
        Object.keys(consumedByConsumer).length === expectedConsumersConsuming) {
        done();
      }
      console.log('processedMessages', processedMessages)
    };
  }

  function generateMessages (numberOfMessages, prefix) {
    return _.times(numberOfMessages, function () {
      return prefix + '-' + uuid.v4();
    });
  }

  it('verify two consumers consuming messages on all partitions', function (done) {
    var messages = generateMessages(3, 'verify 2 c');
    var numberOfConsumers = 2;

    var verify = getConsumerVerifier(messages, 3, numberOfConsumers, done);

    rearer.setVerifier(topic, groupId, verify);
    rearer.raise(numberOfConsumers);

    sendMessages(messages, done);
  });

  it('verify three consumers consuming messages on all partitions', function (done) {
    var messages = generateMessages(3, 'verify 3 c');
    var numberOfConsumers = 3;

    var verify = getConsumerVerifier(messages, 3, numberOfConsumers, done);

    rearer.setVerifier(topic, groupId, verify);
    rearer.raise(numberOfConsumers);

    sendMessages(messages, done);
  });

  it('verify three of four consumers are consuming messages on all partitions', function (done) {
    var messages = generateMessages(3, 'verify 4 c');

    var verify = getConsumerVerifier(messages, 3, 3, done);
    rearer.setVerifier(topic, groupId, verify);
    rearer.raise(4);

    sendMessages(messages, done);
  });

  it('verify one consumer consumes all messages on all partitions after one out of the two consumer is killed', function (done) {
    this.timeout(40000);
    var messages = generateMessages(4, 'verify 1 c 1 killed');
    var verify = getConsumerVerifier(messages, 3, 1, done);

    rearer.setVerifier(topic, groupId, verify);
    rearer.raise(2, function () {
      rearer.kill(1, function () {
        sendMessages(messages, done);
      });
    });
  });

  it('verify two consumer consumes all messages on all partitions after one out of the four consumers are killed', function (done) {
    this.timeout(40000);
    var messages = generateMessages(4, 'verify 2 c 2 killed');
    var verify = getConsumerVerifier(messages, 3, 2, done);

    rearer.setVerifier(topic, groupId, verify);
    rearer.raise(4, function () {
      rearer.kill(2, function () {
        sendMessages(messages, done);
      });
    });
  });

  it('verify three consumer consumes all messages on all partitions after three out of the six consumers are killed', function (done) {
    this.timeout(40000);
    var messages = generateMessages(4, 'verify 3 c 3 killed');
    var verify = getConsumerVerifier(messages, 3, 3, done);

    rearer.setVerifier(topic, groupId, verify);
    rearer.raise(6, function () {
      setTimeout(function () {
        rearer.kill(3, function () {
          sendMessages(messages, done);
        });
      }, 1000);
    });
  });
});
