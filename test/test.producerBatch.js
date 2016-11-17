'use strict';

var sinon = require('sinon');
var kafka = require('..');
var Producer = kafka.Producer;
var Client = kafka.Client;
var async = require('async');

var client, producer, batchClient, batchProducer, noAckProducer;

var TOPIC_POSTFIX = '_test_' + Date.now();
var EXISTS_TOPIC_4 = '_exists_4' + TOPIC_POSTFIX;
var BATCH_SIZE = 500;
var BATCH_AGE = 300;

var host = process.env['KAFKA_TEST_HOST'] || '';
var broker = null;

// Intermittently fails

describe('No Ack Producer', function () {
  before(function (done) {
    async.series({
      setupClient: function (callback) {
        client = new Client(host);
        batchClient = new Client(host, null, null, { noAckBatchSize: BATCH_SIZE, noAckBatchAge: BATCH_AGE });
        producer = new Producer(client);
        batchProducer = new Producer(batchClient);
        producer.on('ready', function () {
          producer.createTopics([EXISTS_TOPIC_4], true, function (err) {
            if (err) return callback(err);
            callback();
          });
          broker = Object.keys(client.brokers)[0];
        });
      },
      producerSend: function (callback) {
        producer.send([{ topic: EXISTS_TOPIC_4, messages: '_initial 1' }], function (err, message) {
          if (err) return callback(err);
          message.should.be.ok;
          message[EXISTS_TOPIC_4].should.have.property('0', 0);
          batchProducer.send([{ topic: EXISTS_TOPIC_4, messages: '_initial 2' }], function (err, message) {
            message.should.be.ok;
            message[EXISTS_TOPIC_4].should.have.property('0', 1);
            callback(err);
          });
        });
      // Ensure that first message gets the `0`
      }
    }, done);
  });

  after(function (done) {
    async.each([producer, batchProducer], function (producer, callback) {
      producer.close(callback);
    }, done);
  });

  describe('with no batch client', function () {
    before(function (done) {
      noAckProducer = new Producer(client, { requireAcks: 0 });
      done();
    });

    beforeEach(function () {
      this.sendSpy = sinon.spy(client.brokers[broker].socket, 'write');
    });

    afterEach(function () {
      this.sendSpy.restore();
    });

    it('should send message directly', function (done) {
      var self = this;
      noAckProducer.send([{
        topic: EXISTS_TOPIC_4, messages: 'hello kafka no batch'
      }], function (err, message) {
        if (err) return done(err);
        message.result.should.equal('no ack');
        self.sendSpy.args.length.should.be.equal(1);
        self.sendSpy.args[0].toString().should.containEql('hello kafka no batch');
        done();
      });
    });
  });

  describe('with batch client', function () {
    before(function (done) {
      noAckProducer = new Producer(batchClient, { requireAcks: 0 });
      done();
    });

    beforeEach(function () {
      this.sendSpy = sinon.spy(batchClient.brokers[broker].socket, 'write');
      this.clock = sinon.useFakeTimers();
    });

    afterEach(function () {
      this.sendSpy.restore();
      this.clock.restore();
    });

    it('should wait to send message 500 ms', function (done) {
      var self = this;
      noAckProducer.send([{
        topic: EXISTS_TOPIC_4, messages: 'hello kafka with batch'
      }], function (err, message) {
        if (err) return done(err);
        message.result.should.equal('no ack');
        self.sendSpy.args.length.should.be.equal(0);
        self.clock.tick(BATCH_AGE - 5);
        self.sendSpy.args.length.should.be.equal(0);
        self.clock.tick(10);
        self.sendSpy.args.length.should.be.equal(1);
        self.sendSpy.args[0].toString().should.containEql('hello kafka with batch');
        done();
      });
    });

    it('should send message once the batch max size is reached', function (done) {
      var self = this;
      var foo = '';
      for (var i = 0; i < BATCH_SIZE; i++) foo += 'X';
      foo += 'end of message';
      noAckProducer.send([{
        topic: EXISTS_TOPIC_4, messages: 'hello kafka with batch'
      }], function (err, message) {
        if (err) return done(err);
        message.result.should.equal('no ack');
        self.sendSpy.args.length.should.be.equal(0);
        noAckProducer.send([{
          topic: EXISTS_TOPIC_4, messages: foo
        }], function (err, message) {
          if (err) return done(err);
          message.result.should.equal('no ack');
          self.sendSpy.args.length.should.be.equal(1);
          self.sendSpy.args[0].toString().should.containEql('hello kafka with batch');
          self.sendSpy.args[0].toString().should.containEql('end of message');
          done();
        });
      });
    });
  });
});
