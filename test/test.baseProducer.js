'use strict';

const BaseProducer = require('../lib/baseProducer');
const ConsumerGroup = require('../lib/consumerGroup');
const KafkaClient = require('../lib/kafkaClient');
const Client = require('./mocks/mockClient');
const uuid = require('uuid');
const sinon = require('sinon');
const async = require('async');
const should = require('should');

describe('BaseProducer', function () {
  describe('ready event', function () {
    const KAFKA_HOST = 'localhost:9092';
    let client;
    before(function () {
      client = new KafkaClient({
        kafkaHost: KAFKA_HOST
      });
    });

    it('can listen on the ready event before the client is connected', function (done) {
      const producer = new BaseProducer(client, {}, BaseProducer.PARTITIONER_TYPES.default);
      producer.once('ready', function () {
        should(producer.ready).be.true;
        done();
      });
    });

    it('can listen on the ready event after the client is connected', function (done) {
      should(client.ready).be.true;
      const producer = new BaseProducer(client, {}, BaseProducer.PARTITIONER_TYPES.default);
      producer.once('ready', function () {
        should(producer.ready).be.true;
        done();
      });
    });
  });

  describe('encoding and decoding key attribute', function () {
    const KAFKA_HOST = 'localhost:9092';
    let consumerGroup, topic, producer;
    beforeEach(function (done) {
      topic = uuid.v4();

      const createTopic = require('../docker/createTopic');

      async.series(
        [
          function (callback) {
            createTopic(topic, 1, 1)
              .then(function () {
                callback(null);
              })
              .catch(error => callback(error));
          },
          function (callback) {
            const client = new KafkaClient({
              kafkaHost: KAFKA_HOST
            });

            producer = new BaseProducer(client, {}, BaseProducer.PARTITIONER_TYPES.default);
            producer.once('ready', function () {
              callback(null);
            });
          },
          function (callback) {
            consumerGroup = new ConsumerGroup(
              {
                kafkaHost: KAFKA_HOST,
                groupId: uuid.v4()
              },
              topic
            );
            consumerGroup.once('connect', function () {
              callback(null);
            });
          }
        ],
        done
      );
    });

    describe('gzip compression', function () {
      afterEach(function () {
        consumerGroup.on('error', function () {});
      });

      it('kafkaClient', function (done) {
        const messageValue = uuid.v4();
        const time = Date.now();
        producer.send(
          [
            {
              topic: topic,
              messages: messageValue,
              key: 'myKeyIsHere',
              attributes: 1,
              timestamp: time
            }
          ],
          function (error) {
            if (error) {
              done(error);
            }
          }
        );

        consumerGroup.on('message', function (message) {
          console.log(message);
          message.key.should.be.exactly('myKeyIsHere');
          message.value.should.be.exactly(messageValue);
          should(message.timestamp.getTime()).be.exactly(time);
          done();
        });
      });
    });

    afterEach(function (done) {
      producer.close();
      consumerGroup.close(done);
    });

    it('verify key string value makes it into the message', function (done) {
      producer.send(
        [
          {
            topic: topic,
            messages: 'this is my message',
            key: 'myKeyIsHere'
          }
        ],
        function (error) {
          if (error) {
            done(error);
          }
        }
      );

      consumerGroup.on('message', function (message) {
        message.key.should.be.exactly('myKeyIsHere');
        done();
      });
    });

    it('verify empty key string value makes it into the message', function (done) {
      producer.send(
        [
          {
            topic: topic,
            messages: 'this is my message',
            key: ''
          }
        ],
        function (error) {
          if (error) {
            done(error);
          }
        }
      );

      consumerGroup.on('message', function (message) {
        message.key.should.be.exactly('');
        done();
      });
    });

    it('verify key value of 0 makes it into the message', function (done) {
      producer.send(
        [
          {
            topic: topic,
            messages: 'this is my message',
            key: 0
          }
        ],
        function (error) {
          if (error) {
            done(error);
          }
        }
      );

      consumerGroup.on('message', function (message) {
        message.key.should.be.exactly('0');
        done();
      });
    });

    it('verify key value of null makes it into the message as null', function (done) {
      producer.send(
        [
          {
            topic: topic,
            messages: 'this is my message',
            key: null
          }
        ],
        function (error) {
          if (error) {
            done(error);
          }
        }
      );

      consumerGroup.on('message', function (message) {
        should(message.key).be.null;
        done();
      });
    });

    it('verify key value of undefined makes it into the message as null', function (done) {
      producer.send(
        [
          {
            topic: topic,
            messages: 'this is my message',
            key: undefined
          }
        ],
        function (error) {
          if (error) {
            done(error);
          }
        }
      );

      consumerGroup.on('message', function (message) {
        should(message.key).be.null;
        done();
      });
    });

    it('verify key value of buffer makes it into the message as untouched buffer', function (done) {
      const keyBuffer = Buffer.from('testing123');
      producer.send(
        [
          {
            topic: topic,
            messages: 'this is my message',
            key: keyBuffer
          }
        ],
        function (error) {
          if (error) {
            done(error);
          }
        }
      );

      consumerGroup.options.keyEncoding = 'buffer';

      consumerGroup.on('message', function (message) {
        should(message.key).not.be.empty;
        keyBuffer.equals(message.key).should.be.true;
        done();
      });
    });
  });

  describe('On Brokers Changed', function () {
    it('should emit error when refreshMetadata fails', function (done) {
      const fakeClient = new Client();
      fakeClient.topicMetadata = {};

      const producer = new BaseProducer(fakeClient, {}, BaseProducer.PARTITIONER_TYPES.default);

      producer.once('error', function (error) {
        error.should.be.an.instanceOf(Error);
        error.message.should.be.exactly('boo');
        done();
      });

      const myError = new Error('boo');
      const refreshMetadataStub = sinon.stub(fakeClient, 'refreshMetadata').yields(myError);

      fakeClient.emit('brokersChanged');

      sinon.assert.calledWith(refreshMetadataStub, []);
    });

    it('should call client.refreshMetadata when brokerChanges', function (done) {
      const fakeClient = new Client();

      fakeClient.topicMetadata = {
        MyTopic: [0],
        YourTopic: [0, 1, 2]
      };

      const producer = new BaseProducer(fakeClient, {}, BaseProducer.PARTITIONER_TYPES.default);

      producer.once('error', done);

      const refreshMetadataStub = sinon.stub(fakeClient, 'refreshMetadata').yields(null);

      fakeClient.emit('brokersChanged');

      fakeClient.topicMetadata.should.have.property('MyTopic');
      fakeClient.topicMetadata.should.have.property('YourTopic');
      sinon.assert.calledWith(refreshMetadataStub, ['MyTopic', 'YourTopic']);
      done();
    });
  });
});
