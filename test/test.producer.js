'use strict';

var kafka = require('..');
var Producer = kafka.Producer;
var uuid = require('uuid');
var Client = kafka.KafkaClient;
var KeyedMessage = kafka.KeyedMessage;
const async = require('async');

var client, producer, noAckProducer, producerKeyed;

[
  {
    name: 'PLAINTEXT Producer'
  },
  {
    name: 'SSL Producer',
    sslOptions: {
      rejectUnauthorized: false
    },
    suiteTimeout: 30000
  }
].forEach(function (testParameters) {
  var TOPIC_POSTFIX = '_test_' + Date.now();
  var EXISTS_TOPIC_3 = '_exists_3' + TOPIC_POSTFIX;

  var sslOptions = testParameters.sslOptions;
  var suiteTimeout = testParameters.suiteTimeout;
  var suiteName = testParameters.name;

  const kafkaHost = '127.0.0.1:' + (sslOptions == null ? '9092' : '9093');

  describe(suiteName, function () {
    before(function (done) {
      if (suiteTimeout) {
        this.timeout(suiteTimeout);
      }
      client = new Client({ kafkaHost, sslOptions });
      producer = new Producer(client);
      noAckProducer = new Producer(client, { requireAcks: 0 });
      producerKeyed = new Producer(client, { partitionerType: Producer.PARTITIONER_TYPES.keyed });

      async.series(
        [
          function (callback) {
            producer.once('ready', callback);
          },
          function (callback) {
            producer.createTopics([EXISTS_TOPIC_3], true, callback);
          }
        ],
        done
      );
    });

    after(function (done) {
      producer.close(done);
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

      it('should support gzip compression', function (done) {
        producer.send(
          [
            {
              topic: EXISTS_TOPIC_3,
              messages: ['hello kafka', 'hello kafka'],
              attributes: 1
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

      describe('Keyed Partitioner', function () {
        const createTopic = require('../docker/createTopic');
        const topicWithTwoPartitions = uuid.v4();
        let client, keyed;

        before(function () {
          return createTopic(topicWithTwoPartitions, 2, 1).then(function () {
            return new Promise(function (resolve, reject) {
              client = new Client({ kafkaHost, sslOptions });
              keyed = new Producer(client, { partitionerType: Producer.PARTITIONER_TYPES.keyed });
              client.once('ready', function () {
                client.refreshMetadata([topicWithTwoPartitions], function (error) {
                  if (error) {
                    return reject(error);
                  }
                  resolve();
                });
              });
            });
          });
        });

        after(function (done) {
          keyed.close(done);
        });

        it('should send message to partition determined by keyed partitioner', function (done) {
          keyed.send([{ key: '12345', topic: topicWithTwoPartitions, messages: 'hello kafka' }], function (
            err,
            message
          ) {
            message.should.be.ok;
            message[topicWithTwoPartitions].should.have.property('1', 0);
            done(err);
          });
        });
      });
    });

    describe('#close', function () {
      var client, producer;

      before(function (done) {
        client = new Client();
        producer = new Producer(client);
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
