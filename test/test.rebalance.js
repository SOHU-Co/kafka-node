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
    setTimeout(done, 500);
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
    var consumedByConsumer = {};
    var verified = _.once(done);

    return function onData (data) {
      debug('From child %d %j', this._childNum, data);
      topic.should.be.eql(data.message.topic);
      if (~messages.indexOf(data.message.value)) {
        processedMessages++;
        consumedByConsumer[data.id] = true;
      }
      if (processedMessages >= messages.length && Object.keys(consumedByConsumer).length === expectedConsumersConsuming) {
        verified();
      }
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
    rearer.raise(numberOfConsumers, function () {
      sendMessages(messages, done);
    });
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
    var messages = generateMessages(4, 'verify 1 c 1 killed');
    var verify = getConsumerVerifier(messages, 3, 1, done);

    rearer.setVerifier(topic, groupId, verify);
    rearer.raise(2, function () {
      rearer.kill(1, function () {
        sendMessages(messages, done);
      });
    }, 500);
  });

  it('verify two consumer consumes all messages on all partitions after two out of the four consumers are killed right away', function (done) {
    var messages = generateMessages(4, 'verify 4 c 2 killed');
    var verify = getConsumerVerifier(messages, 3, 2, done);

    rearer.setVerifier(topic, groupId, verify);
    rearer.raise(4, function () {
      rearer.kill(2, function () {
        sendMessages(messages, done);
      });
    });
  });

  it('verify three consumer consumes all messages on all partitions after one that is unassigned is killed', function (done) {
    var messages = generateMessages(3, 'verify 2 c 2 killed');
    var verify = getConsumerVerifier(messages, 3, 3, done);

    rearer.setVerifier(topic, groupId, verify);

    async.series([
      function (callback) {
        rearer.raise(3, callback);
      },
      function (callback) {
        setTimeout(callback, 1000);
      },
      function (callback) {
        rearer.raise(1, callback);
      },
      function (callback) {
        setTimeout(callback, 1000);
      },
      function (callback) {
        rearer.killFirst(callback);
      }
    ], function () {
      sendMessages(messages, done);
    });
  });

  it('verify two consumer consumes all messages on all partitions after two out of the four consumers are killed', function (done) {
    var messages = generateMessages(4, 'verify 2 c 2 killed');
    var verify = getConsumerVerifier(messages, 3, 2, done);

    rearer.setVerifier(topic, groupId, verify);
    rearer.raise(4, function () {
      rearer.kill(2, function () {
        sendMessages(messages, done);
      });
    }, 500);
  });

  it('verify three consumer consumes all messages on all partitions after three out of the six consumers are killed', function (done) {
    var messages = generateMessages(4, 'verify 3 c 3 killed');
    var verify = getConsumerVerifier(messages, 3, 3, done);

    rearer.setVerifier(topic, groupId, verify);
    rearer.raise(6, function () {
      rearer.kill(3, function () {
        sendMessages(messages, done);
      });
    }, 1000);
  });
});
