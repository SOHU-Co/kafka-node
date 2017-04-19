'use strict';

const kafka = require('..');
const Client = kafka.KafkaClient;
const sinon = require('sinon');
const EventEmitter = require('events');
const TimeoutError = require('../lib/errors/TimeoutError');

describe('Kafka Client', function () {
  it('should connect plaintext', function (done) {
    const client = new Client({
      kafkaHost: 'localhost:9092'
    });
    client.once('ready', done);
  });

  it('should error when connecting to an invalid host', function (done) {
    const client = new Client({
      kafkaHost: 'localhost:9094'
    });

    client.on('error', function (error) {
      error.code.should.be.eql('ECONNREFUSED');
      done();
    });
  });

  it('should connect SSL', function (done) {
    const client = new Client({
      kafkaHost: 'localhost:9093',
      sslOptions: {
        rejectUnauthorized: false
      }
    });
    client.once('ready', done);
  });

  describe('Verify Timeout', function () {
    let sandbox;

    beforeEach(function () {
      sandbox = sinon.sandbox.create();
    });

    afterEach(function () {
      sandbox.restore();
    });

    it('should timeout when connect is not emitted', function (done) {
      const clock = sandbox.useFakeTimers();
      const client = new Client({
        autoConnect: false,
        kafkaHost: 'localhost:9093',
        sslOptions: {
          rejectUnauthorized: false
        }
      });

      sandbox.stub(client, 'setupBroker').returns({
        socket: new EventEmitter()
      });

      client.connect();
      client.once('error', function (error) {
        error.should.be.an.instanceOf(TimeoutError);
        done();
      });

      clock.tick(10000);
    });
  });
});
