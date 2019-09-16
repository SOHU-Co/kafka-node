'use strict';

var kafka = require('..');
var Client = kafka.KafkaClient;
var Producer = kafka.Producer;
var async = require('async');
var debug = require('debug')('kafka-node:Test-Rebalance');
var Childrearer = require('./helpers/Childrearer');
var uuid = require('uuid');
var _ = require('lodash');

describe('Integrated Reblance', function () {
  describe('ConsumerGroup using Kafka Client', function () {
    testRebalance('test/helpers/child-cg-kafka-client');
  });
});

function testRebalance (forkPath) {
  var producer;
  var topic = 'RebalanceTopic';
  var rearer;
  var groupId = 'rebal_group';

  before(function (done) {
    if (process.env.TRAVIS) {
      return this.skip();
    }
    var client = new Client();
    producer = new Producer(client);
    client.on('ready', function () {
      client.refreshMetadata([topic], function (data) {
        // client.topicPartitions[topic].should.be.length(3);
        done();
      });
    });
  });

  beforeEach(function (done) {
    rearer = new Childrearer(forkPath);
    done();
  });

  afterEach(function (done) {
    debug('killChildren');
    rearer.closeAll(done);
  });

  function sendMessages (messages, done) {
    const payload = distributeMessages(messages);
    debug('Sending', payload);
    producer.send(payload, function (error) {
      if (error) {
        return done(error);
      }
      debug('all messages sent');
    });
  }

  function distributeMessages (messages) {
    const partitions = [0, 1, 2];
    var index = 0;
    var len = partitions.length;

    var partitionBuckets = partitions.map(function (partition) {
      return {
        topic: topic,
        messages: [],
        partition: partition
      };
    });

    messages.forEach(function (message) {
      partitionBuckets[index++ % len].messages.push(message);
    });

    return partitionBuckets;
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
      if (processedMessages >= messages.length) {
        var consumedBy = Object.keys(consumedByConsumer);
        if (consumedBy.length >= expectedConsumersConsuming) {
          verified();
        } else {
          verified(
            new Error(
              'Received messages but not by the expected ' +
                expectedConsumersConsuming +
                ' consumers: ' +
                JSON.stringify(consumedBy)
            )
          );
        }
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
    rearer.raise(
      2,
      function () {
        rearer.kill(1, function () {
          sendMessages(messages, done);
        });
      },
      500
    );
  });

  it('verify two consumer consumes all messages on all partitions after two out of the four consumers are killed right away', function (done) {
    var messages = generateMessages(3, 'verify 4 c 2 killed');
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
    var verify = getConsumerVerifier(messages, 3, 2, done);

    rearer.setVerifier(topic, groupId, verify);

    async.series(
      [
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
      ],
      function () {
        sendMessages(messages, done);
      }
    );
  });

  it('verify two consumer consumes all messages on all partitions after two out of the four consumers are killed', function (done) {
    var messages = generateMessages(3, 'verify 2 c 2 killed');
    var verify = getConsumerVerifier(messages, 3, 2, done);

    rearer.setVerifier(topic, groupId, verify);
    rearer.raise(
      4,
      function () {
        rearer.kill(2, function () {
          sendMessages(messages, done);
        });
      },
      500
    );
  });

  it('verify three consumer consumes all messages on all partitions after three out of the six consumers are killed', function (done) {
    var messages = generateMessages(3, 'verify 3 c 3 killed');
    var verify = getConsumerVerifier(messages, 3, 2, done);

    rearer.setVerifier(topic, groupId, verify);
    rearer.raise(
      6,
      function () {
        rearer.kill(3, function () {
          sendMessages(messages, done);
        });
      },
      1000
    );
  });
}
