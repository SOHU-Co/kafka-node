'use strict';

var kafka = require('..');
var HighLevelProducer = kafka.HighLevelProducer;
var uuid = require('uuid');
var KafkaClient = kafka.KafkaClient;
var KeyedMessage = kafka.KeyedMessage;
const _ = require('lodash');
const assert = require('assert');
const ConsumerGroup = kafka.ConsumerGroup;
const sendMessage = require('./helpers/sendMessage');
var client, producer, noAckProducer, producerKeyed;

describe('partitioner', function () {
  let topic, consumerGroup, messages;
  const createTopic = require('../docker/createTopic');

  beforeEach(function () {
    messages = _.times(3, uuid.v4);
    topic = uuid.v4();

    return createTopic(topic, 3, 1).then(function () {
      return new Promise(function (resolve, reject) {
        sendMessage(messages, topic, function (error) {
          if (error) {
            return reject(error);
          }
          resolve();
        });
      });
    });
  });

  afterEach(function (done) {
    consumerGroup && consumerGroup.close(done);
  });

  it('should distribute messages round-robin using cyclic', function (done) {
    consumerGroup = new ConsumerGroup(
      {
        kafkaHost: '127.0.0.1:9092',
        groupId: uuid.v4(),
        fromOffset: 'earliest'
      },
      topic
    );
    const offsets = [];
    consumerGroup.on('message', function (message) {
      offsets.should.not.containEql(message.offset);
      offsets.push(message.offset);
      _.pull(messages, message.value);
      if (messages.length === 0) {
        done();
      }
    });
  });
});

[
  {
    name: 'PLAINTEXT HighLevelProducer using KafkaClient',
    useKafkaClient: true
  },
  {
    name: 'SSL Producer using KafkaClient',
    useKafkaClient: true,
    sslOptions: {
      rejectUnauthorized: false
    },
    suiteTimeout: 30000
  }
].forEach(function (testParameters) {
  var TOPIC_POSTFIX = '_test_' + uuid.v4();
  var EXISTS_TOPIC_3 = '_exists_3' + TOPIC_POSTFIX;

  var sslOptions = testParameters.sslOptions;
  var suiteTimeout = testParameters.suiteTimeout;
  var suiteName = testParameters.name;

  describe(suiteName, function () {
    before(function (done) {
      if (suiteTimeout) {
        this.timeout(suiteTimeout);
      }
      const kafkaHost = 'localhost:' + (sslOptions != null ? '9093' : '9092');
      client = new KafkaClient({
        kafkaHost: kafkaHost,
        sslOptions: sslOptions
      });

      producer = new HighLevelProducer(client);
      noAckProducer = new HighLevelProducer(client, { requireAcks: 0 });
      producerKeyed = new HighLevelProducer(client, { partitionerType: HighLevelProducer.PARTITIONER_TYPES.keyed });

      producer.on('ready', function () {
        producerKeyed.on('ready', function () {
          producer.createTopics([EXISTS_TOPIC_3], true, function (err) {
            if (err) return done(err);
            done();
          });
        });
      });
    });

    after(function (done) {
      producer.close(done);
    });

    describe('#buildPayloads', function () {
      function assertMessage (result, topic, partition, value) {
        assert(
          _.chain(result)
            .find({ topic: topic, partition: partition })
            .result('messages')
            .map('value')
            .includes(value)
            .value(),
          `Value "${value}" is not in topic "${topic}" partition ${partition}`
        );
      }

      it('should normalize payload by topic parition', function () {
        const topicMetadata = {
          jolly: [0, 1, 2],
          christmas: [0, 1, 2],
          coal: [0]
        };

        const payload = [
          { topic: 'jolly', partition: 0, messages: 'jolly-test-0' },
          { topic: 'jolly', partition: 1, messages: 'jolly-test-1' },
          { topic: 'jolly', partition: 2, messages: 'jolly-test-2' },
          { topic: 'jolly', partition: 1, messages: 'jolly-test-3' },
          { topic: 'christmas', partition: 0, messages: 'christmas-test-0' },
          { topic: 'christmas', partition: 1, messages: 'christmas-test-1' },
          { topic: 'christmas', partition: 2, messages: 'christmas-test-2' },
          { topic: 'christmas', partition: 0, messages: 'christmas-test-3' },
          { topic: 'christmas', partition: 1, messages: 'christmas-test-4' },
          { topic: 'christmas', partition: 2, messages: 'christmas-test-5' },
          { topic: 'coal', partition: 0, messages: 'coal-test' }
        ];

        const requestPayload = producer.buildPayloads(payload, topicMetadata);

        Object.keys(requestPayload).length.should.be.eql(7);

        assertMessage(requestPayload, 'jolly', 0, 'jolly-test-0');
        assertMessage(requestPayload, 'jolly', 1, 'jolly-test-1');
        assertMessage(requestPayload, 'jolly', 1, 'jolly-test-3');
        assertMessage(requestPayload, 'jolly', 2, 'jolly-test-2');

        assertMessage(requestPayload, 'christmas', 0, 'christmas-test-0');
        assertMessage(requestPayload, 'christmas', 1, 'christmas-test-1');
        assertMessage(requestPayload, 'christmas', 2, 'christmas-test-2');
        assertMessage(requestPayload, 'christmas', 0, 'christmas-test-3');
        assertMessage(requestPayload, 'christmas', 1, 'christmas-test-4');
        assertMessage(requestPayload, 'christmas', 2, 'christmas-test-5');

        assertMessage(requestPayload, 'coal', 0, 'coal-test');
      });
    });

    describe('#send', function () {
      before(function (done) {
        // Ensure that first message gets the `0`
        producer.send([{ topic: EXISTS_TOPIC_3, messages: '_initial' }], function (err, message) {
          message.should.be.ok;
          message[EXISTS_TOPIC_3].should.have.property('0', 0);
          done(err);
        });
      });

      it('should send message successfully', function (done) {
        producer.send([{ topic: EXISTS_TOPIC_3, messages: 'hello kafka' }], function (err, message) {
          message.should.be.ok;
          message[EXISTS_TOPIC_3]['0'].should.be.above(0);
          done(err);
        });
      });

      it('should send buffer message successfully', function (done) {
        var message = Buffer.from('hello kafka');
        producer.send([{ topic: EXISTS_TOPIC_3, messages: message }], function (err, message) {
          message.should.be.ok;
          message[EXISTS_TOPIC_3]['0'].should.be.above(0);
          done(err);
        });
      });

      it('should send null message successfully', function (done) {
        var message = null;
        producer.send([{ topic: EXISTS_TOPIC_3, messages: message }], function (err, message) {
          message.should.be.ok;
          message[EXISTS_TOPIC_3]['0'].should.be.above(0);
          done(err);
        });
      });

      it('should convert none buffer message to string', function (done) {
        var message = -1;
        producer.send([{ topic: EXISTS_TOPIC_3, messages: message }], function (err, message) {
          message.should.be.ok;
          message[EXISTS_TOPIC_3]['0'].should.be.above(0);
          done(err);
        });
      });

      it('should send Message struct successfully', function (done) {
        var message = new KeyedMessage('test-key', 'test-message');
        producer.send([{ topic: EXISTS_TOPIC_3, messages: message }], function (err, message) {
          message.should.be.ok;
          message[EXISTS_TOPIC_3]['0'].should.be.above(0);
          done(err);
        });
      });

      it('should support multi messages in one topic', function (done) {
        producer.send([{ topic: EXISTS_TOPIC_3, messages: ['hello kafka', 'hello kafka'] }], function (err, message) {
          message.should.be.ok;
          message[EXISTS_TOPIC_3]['0'].should.be.above(0);
          done(err);
        });
      });

      it('should support snappy compression', function (done) {
        producer.send(
          [
            {
              topic: EXISTS_TOPIC_3,
              messages: ['hello kafka', 'hello kafka'],
              attributes: 2
            }
          ],
          function (err, message) {
            if (err) return done(err);
            message.should.be.ok;
            message[EXISTS_TOPIC_3]['0'].should.be.above(0);
            done();
          }
        );
      });

      it('should send message without ack', function (done) {
        noAckProducer.send(
          [
            {
              topic: EXISTS_TOPIC_3,
              messages: 'hello kafka'
            }
          ],
          function (err, message) {
            if (err) return done(err);
            message.result.should.equal('no ack');
            done();
          }
        );
      });

      it('should send message to specified partition even when producer configured with keyed partitioner', function (done) {
        producerKeyed.send([{ key: '12345', partition: 0, topic: EXISTS_TOPIC_3, messages: 'hello kafka' }], function (
          err,
          message
        ) {
          message.should.be.ok;
          message[EXISTS_TOPIC_3]['0'].should.be.above(0);
          done(err);
        });
      });
    });

    describe('#close', function () {
      var client, producer;

      before(function (done) {
        client = new KafkaClient();
        producer = new HighLevelProducer(client);
        producer.on('ready', done);
      });

      after(function (done) {
        producer.close(done);
      });

      it('should close successfully', function (done) {
        producer.close(done);
      });
    });
  });
});
