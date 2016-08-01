'use strict';

var kafka = require('..');
var HighLevelProducer = kafka.HighLevelProducer;
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
    name: 'PLAINTEXT HighLevelProducer'
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
      producer = new HighLevelProducer(client);
      noAckProducer = new HighLevelProducer(client, { requireAcks: 0 });
      producerKeyed = new HighLevelProducer(client, { partitionerType: HighLevelProducer.PARTITIONER_TYPES.keyed });

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
    });

    describe('#createTopics', function () {
      var client, producer;

      before(function (done) {
        client = new Client(host);
        producer = new HighLevelProducer(client);
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
