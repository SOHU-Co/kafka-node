'use strict';

const ProducerStream = require('../lib/producerStream');
const ConsumerGroup = require('../lib/consumerGroup');
const uuid = require('uuid');
const createTopic = require('../docker/createTopic');
const _ = require('lodash');
const async = require('async');

describe('Producer Stream', function () {
  let topic;

  before(function () {
    topic = uuid.v4();
    return createTopic(topic, 1, 1);
  });

  it('should stream to a topic and verify data was streamed to that topic', function (done) {
    const producerStream = new ProducerStream();

    const messages = [uuid.v4(), uuid.v4()];

    const consumer = new ConsumerGroup(
      {
        kafkaHost: '127.0.0.1:9092',
        groupId: uuid.v4(),
        fromOffset: 'earliest'
      },
      topic
    );

    consumer.on('message', function (message) {
      _.pull(messages, message.value);
      if (_.isEmpty(messages)) {
        async.parallel([callback => producerStream.close(callback), callback => consumer.close(callback)], done);
      }
    });

    producerStream.write({
      topic: topic,
      messages: messages
    });
  });
});
