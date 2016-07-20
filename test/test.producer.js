'use strict';

var kafka = require('..');
var Producer = kafka.Producer;
var uuid = require('node-uuid');
var Client = kafka.Client;
var KeyedMessage = kafka.KeyedMessage;

var client, producer, noAckProducer, producerKeyed;

var host = process.env['KAFKA_TEST_HOST'] || '';

// Helper method
function randomId () {
  return Math.floor(Math.random() * 10000);
}

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

  describe(suiteName, function () {
    before(function (done) {
      if (suiteTimeout) { this.timeout(suiteTimeout); }
      var clientId = 'kafka-node-client-' + uuid.v4();
      client = new Client(host, clientId, undefined, undefined, sslOptions);
      producer = new Producer(client);
      noAckProducer = new Producer(client, { requireAcks: 0 });
      producerKeyed = new Producer(client, { partitionerType: Producer.PARTITIONER_TYPES.keyed });

      producer.on('ready', function () {
        producerKeyed.on('ready', function () {
          producer.createTopics([EXISTS_TOPIC_3], false, function (err, created) {
            if (err) return done(err);
            setTimeout(done, 500);
          });
        });
      });
    });

    after(function (done) {
      producer.close(done);
    });

    describe(suiteName + ' socket recovery', function () {
      var emptyFn = function () {};
      var recoveryClient;
      var recoveryProducer;

      before(function (done) {
        var clientId = 'kafka-node-client-' + uuid.v4();
        recoveryClient = new Client(host, clientId, undefined, undefined, sslOptions);
        recoveryProducer = new Producer(recoveryClient);
        recoveryProducer.on('ready', done);
        // make sure uncaught errors do not fail the test
        recoveryProducer.on('error', emptyFn);
      });

      after(function (done) {
        recoveryProducer.close(done);
      });

      it('should recover from a socket issue', function (done) {
        var broker = recoveryClient.brokers[Object.keys(recoveryClient.brokers)[0]];
        recoveryClient.on('reconnect', done);
        broker.socket.emit('error', new Error('Mock error'));
        broker.socket.end();
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
        var message = new Buffer('hello kafka');
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
        producer.send([{
          topic: EXISTS_TOPIC_3,
          messages: ['hello kafka', 'hello kafka'],
          attributes: 2
        }], function (err, message) {
          if (err) return done(err);
          message.should.be.ok;
          message[EXISTS_TOPIC_3]['0'].should.be.above(0);
          done();
        });
      });

      it('should support gzip compression', function (done) {
        producer.send([{
          topic: EXISTS_TOPIC_3,
          messages: ['hello kafka', 'hello kafka'],
          attributes: 1
        }], function (err, message) {
          if (err) return done(err);
          message.should.be.ok;
          message[EXISTS_TOPIC_3]['0'].should.be.above(0);
          done();
        });
      });

      it('should send message without ack', function (done) {
        noAckProducer.send([{
          topic: EXISTS_TOPIC_3, messages: 'hello kafka'
        }], function (err, message) {
          if (err) return done(err);
          message.result.should.equal('no ack');
          done();
        });
      });

      it('should send message to specified partition even when producer configured with keyed partitioner', function (done) {
        producerKeyed.send([{ key: '12345', partition: 0, topic: EXISTS_TOPIC_3, messages: 'hello kafka' }], function (err, message) {
          message.should.be.ok;
          message[EXISTS_TOPIC_3]['0'].should.be.above(0);
          done(err);
        });
      });

      xit('should send message to partition determined by keyed partitioner', function (done) {
        producerKeyed.send([{ key: '12345', topic: EXISTS_TOPIC_3, messages: 'hello kafka' }], function (err, message) {
          message.should.be.ok;
          message[EXISTS_TOPIC_3].should.have.property('1', 0);
          done(err);
        });
      });
    });

    describe('#createTopics', function () {
      var client, producer;

      before(function (done) {
        client = new Client(host);
        producer = new Producer(client);
        producer.on('ready', done);
      });

      after(function (done) {
        producer.close(done);
      });

      it('should return All requests sent when async is true', function (done) {
        producer.createTopics(['_exist_topic_' + randomId() + '_test'], true, function (err, data) {
          data.should.equal('All requests sent');
          done(err);
        });
      });

      it('async should be true if not present', function (done) {
        producer.createTopics(['_exist_topic_' + randomId() + '_test'], function (err, data) {
          data.should.equal('All requests sent');
          done(err);
        });
      });

      it('should return All created when async is false', function (done) {
        producer.createTopics(['_exist_topic_' + randomId() + '_test'], false, function (err, data) {
          data.should.equal('All created');
          done(err);
        });
      });
    });

    describe('#close', function () {
      var client, producer;

      before(function (done) {
        client = new Client(host);
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
