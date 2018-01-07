'use strict';

const uuid = require('uuid');
const createTopic = require('../docker/createTopic');
const sendMessage = require('./helpers/sendMessage');
const async = require('async');
const _ = require('lodash');
const sinon = require('sinon');

const ConsumerGroupStream = require('../lib/consumerGroupStream');

function createConsumerGroupStream (topic) {
  const consumerOptions = {
    kafkaHost: '127.0.0.1:9092',
    groupId: 'ExampleTestGroup',
    sessionTimeout: 15000,
    protocol: ['roundrobin'],
    asyncPush: false,
    id: 'consumer1',
    autoCommit: false,
    fromOffset: 'earliest'
  };
  return new ConsumerGroupStream(consumerOptions, topic);
}

describe('ConsumerGroupStream', function () {
  let topic;

  before(function () {
    topic = uuid.v4();
    return createTopic(topic, 1, 1);
  });

  describe('#close', function () {
    it('should not call consumerGroup with force option', function (done) {
      const consumerGroupStream = createConsumerGroupStream(topic);

      const closeSpy = sinon.spy(consumerGroupStream.consumerGroup, 'close');

      consumerGroupStream.close(function () {
        sinon.assert.calledOnce(closeSpy);
        sinon.assert.calledWithExactly(closeSpy, false, sinon.match.func);
        done();
      });
    });

    it('autoCommit false should close the consumer without committing offsets', function (done) {
      const messages = _.times(3, uuid.v4);
      let consumerGroupStream;

      async.series(
        [
          function (callback) {
            sendMessage(messages, topic, callback);
          },
          function (callback) {
            const messagesToRead = _.clone(messages);
            consumerGroupStream = createConsumerGroupStream(topic);
            consumerGroupStream.on('data', function (message) {
              _.pull(messagesToRead, message.value);
              if (messagesToRead.length === 0) {
                callback(null);
              }
            });
          },
          function (callback) {
            consumerGroupStream.close(callback);
          },
          function (callback) {
            const messagesToRead = _.clone(messages);
            consumerGroupStream = createConsumerGroupStream(topic);
            consumerGroupStream.on('data', function (message) {
              _.pull(messagesToRead, message.value);
              if (messagesToRead.length === 0) {
                callback(null);
              }
            });
          },
          function (callback) {
            consumerGroupStream.close(callback);
          }
        ],
        done
      );
    });
  });
});
