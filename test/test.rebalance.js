var kafka = require('..');
var Client = kafka.Client;
var HighLevelProducer = kafka.HighLevelProducer;
var fork = require('child_process').fork;
var async = require('async');
var debug = require('debug')('kafka-node:Test-Rebalance');

var children = 0;

describe('Integrated HLC Rebalance', function () {
  var producer;
  var topic = 'RebalanceTopic';
  var child1, child2, child3, child4;

  function sendMessage (topic, message, done) {
    debug('sending ', message);
    producer.send([ {topic: topic, messages: [message]} ], function () {
      debug('sent', message);
      done();
    });
  }

  before(function (done) {
    var client = new Client();
    producer = new HighLevelProducer(client);
    client.on('ready', function () {
      client.refreshMetadata([topic], function (data) {
        client.topicPartitions[topic].should.be.length(3);
        done();
      });
    });
  });

  afterEach(function (done) {
    debug('killChildren');
    child1.kill();
    child2.kill();
    child3 && child3.kill();
    child4 && child4.kill();
    setTimeout(done, 1000);
  });

  function sendMessages (messages, done) {
    async.eachSeries(messages, function (message, cb) {
      sendMessage(topic, message, cb);
    }, function (error, results) {
      if (error) {
        console.error('Send Error', error);
        done(error);
      }
      done(null);
      debug('all sent');
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
      if (processedMessages === messages.length && Object.keys(partitionsConsumed).length === expectedPartitionsConsumed &&
        Object.keys(consumedByConsumer).length === expectedConsumersConsuming) {
        done();
      }
    };
  }

  it('verify two consumers consuming messages on all partitions', function (done) {
    var groupId = 'rebal_group';
    var messages = ['verify two consumers consuming 1', 'verify two consumers consuming 2', 'verify two consumers consuming 3'];

    var verify = getConsumerVerifier(messages, 3, 2, done);

    child1 = raiseChild(topic, groupId, verify);
    child2 = raiseChild(topic, groupId, verify);

    sendMessages(messages, function (error) {
      if (error) {
        done(error);
      }
    });
  });

  it('verify three consumers consuming messages on all partitions', function (done) {
    var groupId = 'rebal_group';
    var messages = ['verify 3 consumers consuming 1', 'verify 3 consumers consuming 2', 'verify 3 consumers consuming 3'];

    var verify = getConsumerVerifier(messages, 3, 3, done);

    child1 = raiseChild(topic, groupId, verify);
    child2 = raiseChild(topic, groupId, verify);
    child3 = raiseChild(topic, groupId, verify);

    sendMessages(messages, function (error) {
      if (error) {
        done(error);
      }
    });
  });

  it('verify three of four consumers are consuming messages on all partitions', function (done) {
    var groupId = 'rebal_group';
    var messages = ['verify 4 hlc consuming 1', 'verify 4 hlc consuming 2', 'verify 4 hlc consuming 3'];

    var verify = getConsumerVerifier(messages, 3, 3, done);

    child1 = raiseChild(topic, groupId, verify);
    child2 = raiseChild(topic, groupId, verify);
    child3 = raiseChild(topic, groupId, verify);
    child4 = raiseChild(topic, groupId, verify);

    sendMessages(messages, function (error) {
      if (error) {
        done(error);
      }
    });
  });

  it('verify one consumer consumes all messages on all partitions after one out of the two consumer is killed', function (done) {
    var groupId = 'rebal_group';

    var messages = ['verify one 1', 'verify one 2', 'verify one 3', 'verify one 4'];
    var verify = getConsumerVerifier(messages, 3, 1, done);

    child1 = raiseChild(topic, groupId, verify);
    child2 = raiseChild(topic, groupId, verify);

    setImmediate(function () {
      child2.kill();
      child2.once('close', function (code, signal) {
        debug('child %s killed %d %s', this._childNum, code, signal);
        sendMessages(messages, function (error) {
          if (error) {
            done(error);
          }
        });
      });
    });
  });
});

function raiseChild (topic, groupId, onMessage) {
  debug('forking child ', ++children);
  var child = fork('test/helpers/child-hlc', ['--groupId=' + groupId, '--topic=' + topic]);
  child._childNum = children;
  child.on('message', onMessage);
  return child;
}
