'use strict';

const kafka = require('..');
const Client = kafka.KafkaClient;
const sinon = require('sinon');
const TimeoutError = require('../lib/errors/TimeoutError');
const FakeSocket = require('./mocks/mockSocket');
const should = require('should');
const _ = require('lodash');

describe('Kafka Client', function () {
  describe('#parseHostList', function () {
    it('initial hosts should be parsed if single host is provided', function () {
      const client = new Client({
        autoConnect: false,
        kafkaHost: 'localhost:9092'
      });

      client.initialHosts.should.not.be.empty;
      client.initialHosts.length.should.be.eql(1);
      client.initialHosts[0].host.should.be.eql('localhost');
      client.initialHosts[0].port.should.be.eql('9092');
    });

    it('initial hosts should be parsed if multiple hosts are provided', function () {
      const client = new Client({
        autoConnect: false,
        kafkaHost: 'localhost:9092,127.0.0.1:9093,192.168.1.0:9094'
      });

      client.initialHosts.should.not.be.empty;
      client.initialHosts.should.be.eql([
        {
          host: 'localhost',
          port: '9092'
        },
        {
          host: '127.0.0.1',
          port: '9093'
        },
        {
          host: '192.168.1.0',
          port: '9094'
        }
      ]);
    });
  });

  describe('#wrapTimeoutIfNeeded', function () {
    let sandbox, wrapTimeoutIfNeeded, client, clock;

    beforeEach(function () {
      sandbox = sinon.sandbox.create();
      clock = sandbox.useFakeTimers();
      client = {
        unqueueCallback: sandbox.stub(),
        options: {
          requestTimeout: false
        }
      };
      wrapTimeoutIfNeeded = Client.prototype.wrapTimeoutIfNeeded.bind(client);
    });

    afterEach(function () {
      sandbox.restore();
    });

    it('should not wrap if there is not a this.options.requestTimeout', function () {
      const myFn = function () {};
      const retFn = wrapTimeoutIfNeeded(1, 1, myFn);
      myFn.should.be.exactly(retFn);
    });

    it('should not yield timeout if returned callback is called in time', function (done) {
      client.options.requestTimeout = 400;
      const retFn = wrapTimeoutIfNeeded(1, 1, done);
      retFn.should.not.be.exactly(done);
      clock.tick(300);
      retFn();
      clock.tick(300);
    });

    it('should yield timeout error if not called by timeout', function (done) {
      client.options.requestTimeout = 400;
      function callback (error) {
        error.should.be.an.instanceOf(Error);
        error.message.should.be.exactly('Request timed out after 400ms');
        sinon.assert.calledWithExactly(client.unqueueCallback, 1, 10);
        done();
      }
      const retFn = wrapTimeoutIfNeeded(1, 10, callback);
      retFn.should.not.be.exactly(callback);
      clock.tick(400);
      retFn(new Error('BAD'));
    });
  });

  describe('#setBrokerMetadata', function () {
    let clock;

    beforeEach(function () {
      clock = sinon.useFakeTimers();
    });

    afterEach(function () {
      clock.restore();
    });

    it('should set new brokerMetadata field on client no emit', function () {
      const client = new Client({
        autoConnect: false,
        kafkaHost: 'Kafka-1.us-east-1.myapp.com:9093'
      });

      const brokerMetadata = {
        '1': { nodeId: 1, host: 'Kafka-1.us-east-1.myapp.com', port: 9093 },
        '2': { nodeId: 2, host: 'Kafka-2.us-east-1.myapp.com', port: 9093 },
        '3': { nodeId: 3, host: 'Kafka-3.us-east-1.myapp.com', port: 9093 }
      };

      client.on('brokersChanged', function () {
        throw new Error('should not emit');
      });

      client.setBrokerMetadata(brokerMetadata);
      client.brokerMetadata.should.be.eql(brokerMetadata);
      client.brokerMetadataLastUpdate.should.be.eql(0);
      clock.tick(100);
    });

    it('should set same brokerMetadata field on client no emit', function () {
      const client = new Client({
        autoConnect: false,
        kafkaHost: 'Kafka-1.us-east-1.myapp.com:9093'
      });

      const brokerMetadata = {
        '1': { nodeId: 1, host: 'Kafka-1.us-east-1.myapp.com', port: 9093 },
        '2': { nodeId: 2, host: 'Kafka-2.us-east-1.myapp.com', port: 9093 },
        '3': { nodeId: 3, host: 'Kafka-3.us-east-1.myapp.com', port: 9093 }
      };

      client.brokerMetadata = brokerMetadata;
      should(client.brokerMetadataLastUpdate).be.empty;

      client.on('brokersChanged', function () {
        throw new Error('should not emit');
      });

      client.setBrokerMetadata(brokerMetadata);
      client.brokerMetadata.should.be.eql(brokerMetadata);
      client.brokerMetadataLastUpdate.should.be.eql(0);
      clock.tick(100);
    });

    it('should set different brokerMetadata field on client no emit', function (done) {
      const client = new Client({
        autoConnect: false,
        kafkaHost: 'Kafka-1.us-east-1.myapp.com:9093'
      });

      const brokerMetadata = {
        '1': { nodeId: 1, host: 'Kafka-1.us-east-1.myapp.com', port: 9093 },
        '2': { nodeId: 2, host: 'Kafka-2.us-east-1.myapp.com', port: 9093 },
        '3': { nodeId: 3, host: 'Kafka-3.us-east-1.myapp.com', port: 9093 }
      };

      client.brokerMetadata = _.clone(brokerMetadata);
      should(client.brokerMetadataLastUpdate).be.empty;

      delete brokerMetadata['1'];
      client.on('brokersChanged', done);

      client.setBrokerMetadata(brokerMetadata);
      client.brokerMetadata.should.be.eql(brokerMetadata);
      client.brokerMetadataLastUpdate.should.be.eql(0);
      clock.tick(100);
    });
  });

  describe('#connect', function () {
    it('should connect plaintext', function (done) {
      const client = new Client({
        kafkaHost: 'localhost:9092'
      });
      client.once('error', done);
      client.once('ready', function () {
        client.brokerMetadata.should.not.be.empty;
        done();
      });
    });

    it('should error when connecting to an invalid host', function (done) {
      const client = new Client({
        connectRetryOptions: {
          retries: 0
        },
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
      client.once('error', done);
      client.once('ready', function () {
        client.brokerMetadata.should.not.be.empty;
        done();
      });
    });
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
        connectRetryOptions: {
          retries: 0
        },
        autoConnect: false,
        kafkaHost: 'localhost:9093',
        sslOptions: {
          rejectUnauthorized: false
        }
      });

      sandbox.stub(client, 'setupBroker').returns({
        socket: new FakeSocket()
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
