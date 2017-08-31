'use strict';

const kafka = require('..');
const Client = kafka.KafkaClient;
const sinon = require('sinon');
const TimeoutError = require('../lib/errors/TimeoutError');
const TopicsNotExistError = require('../lib/errors/TopicsNotExistError');
const BrokerWrapper = require('../lib/wrapper/BrokerWrapper');
const FakeSocket = require('./mocks/mockSocket');
const should = require('should');
const _ = require('lodash');
const uuid = require('uuid');

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

  describe('Versions', function () {
    let client;
    before(function () {
      if (process.env.KAFKA_VERSION === '0.8') {
        this.skip();
      }
    });

    afterEach(function (done) {
      client.close(done);
    });

    describe('#initializeBroker', function () {
      it('should not call #getApiVersions if socket is longpolling', function (done) {
        client = new Client({ kafkaHost: '127.0.0.1:9092', autoConnect: false });
        const fakeBroker = new BrokerWrapper(new FakeSocket());
        const apiVersionsSpy = sinon.spy(client, 'getApiVersions');
        fakeBroker.socket.longpolling = true;
        client.initializeBroker(fakeBroker, function (error) {
          sinon.assert.notCalled(apiVersionsSpy);
          done(error);
        });
      });

      it('should not call #getApiVersions if versions is disabled', function (done) {
        client = new Client({ kafkaHost: '127.0.0.1:9092', autoConnect: false, versions: { disabled: true } });
        client.options.versions.disabled.should.be.true;
        const brokerInitSpy = sinon.spy(client, 'initializeBroker');
        const apiVersionsSpy = sinon.spy(client, 'getApiVersions');
        client.connect();
        client.once('connect', function () {
          sinon.assert.calledOnce(brokerInitSpy);
          sinon.assert.notCalled(apiVersionsSpy);
          brokerInitSpy.restore();
          apiVersionsSpy.restore();
          done();
        });
      });

      it('should call #getApiVersions if versions is enabled', function (done) {
        client = new Client({ kafkaHost: '127.0.0.1:9092', autoConnect: false });
        client.options.versions.disabled.should.be.false;
        const brokerInitSpy = sinon.spy(client, 'initializeBroker');
        const apiVersionsSpy = sinon.spy(client, 'getApiVersions');
        client.connect();
        client.once('connect', function () {
          sinon.assert.calledOnce(brokerInitSpy);
          sinon.assert.calledOnce(apiVersionsSpy);
          sinon.assert.callOrder(brokerInitSpy, apiVersionsSpy);
          brokerInitSpy.restore();
          apiVersionsSpy.restore();
          done();
        });
      });

      if (process.env.KAFKA_VERSION === '0.9') {
        it('should return base support mapping', function (done) {
          client = new Client({ kafkaHost: '127.0.0.1:9092' });
          client.once('connect', function () {
            const broker = client.brokerForLeader();
            broker.isConnected().should.be.true;
            broker.should.have.property('apiSupport');
            broker.apiSupport.should.be.type('object');
            _.forOwn(broker.apiSupport, function (support, api) {
              if (support === null) {
                return;
              }
              support.should.be.type('object');
              support.should.have.keys('min', 'max', 'usable');
            });
            done();
          });
        });
      }
    });

    describe('#getApiVersions', function () {
      beforeEach(function (done) {
        client = new Client({ kafkaHost: '127.0.0.1:9092' });
        client.once('connect', done);
      });

      if (process.env.KAFKA_VERSION === '0.9') {
        it('#getApiVersions failure for 0.9', function (done) {
          client.getApiVersions(client.brokerForLeader(), function (error, results) {
            error.should.be.an.instanceOf(TimeoutError);
            done();
          });
        });
      } else {
        it('#getApiVersions returns results', function (done) {
          client.getApiVersions(client.brokerForLeader(), function (error, results) {
            should(results).not.be.empty;
            _.forOwn(results, function (support, api) {
              if (support === null) {
                return;
              }
              support.should.have.keys('min', 'max', 'usable');
            });
            done(error);
          });
        });
      }
    });
  });

  describe('#deleteDisconnected', function () {
    let sandbox, client, fakeBroker;

    before(function () {
      sandbox = sinon.sandbox.create();
      client = new Client({ kafkaHost: '127.0.0.1:9092', autoConnect: false, requestTimeout: 4000 });
      fakeBroker = new BrokerWrapper(new FakeSocket());
      fakeBroker.socket.addr = '127.0.0.1:9092';
    });

    afterEach(function () {
      sandbox.restore();
    });

    it('should not delete if broker is still connected', function () {
      sandbox.spy(client, 'getBrokers');

      client.brokers[fakeBroker.socket.addr] = fakeBroker;

      sandbox.stub(fakeBroker, 'isConnected').returns(true);
      client.deleteDisconnected(fakeBroker);
      sinon.assert.notCalled(client.getBrokers);
      sinon.assert.calledOnce(fakeBroker.isConnected);

      client.brokers.should.have.property(fakeBroker.socket.addr);
    });

    it('should throw if the broker is key does not match the instance', function () {
      sandbox.spy(client, 'getBrokers');
      client.brokers[fakeBroker.socket.addr] = {};
      sandbox.stub(fakeBroker, 'isConnected').returns(false);

      should.throws(function () {
        client.deleteDisconnected(fakeBroker);
      });

      sinon.assert.calledOnce(client.getBrokers);
      sinon.assert.calledOnce(fakeBroker.isConnected);

      client.brokers.should.have.property(fakeBroker.socket.addr);
    });

    it('should delete broker if disconnected', function () {
      sandbox.spy(client, 'getBrokers');

      client.brokers[fakeBroker.socket.addr] = fakeBroker;

      sandbox.stub(fakeBroker, 'isConnected').returns(false);
      client.deleteDisconnected(fakeBroker);
      sinon.assert.calledOnce(client.getBrokers);
      sinon.assert.calledOnce(fakeBroker.isConnected);

      client.brokers.should.not.have.property(fakeBroker.socket.addr);
    });
  });

  describe('#waitUntilReady', function () {
    let sandbox, client, clock;

    before(function () {
      sandbox = sinon.sandbox.create();
      clock = sandbox.useFakeTimers();
      client = new Client({ kafkaHost: '127.0.0.1:9092', autoConnect: false, requestTimeout: 4000 });
    });

    afterEach(function () {
      sandbox.restore();
    });

    it('should yield error timeout if broker is not ready by requestTimeout', function (done) {
      const fakeBroker = new BrokerWrapper(new FakeSocket());
      const readyKey = 'broker.host-ready';

      sandbox.stub(fakeBroker, 'getReadyEventName').returns(readyKey);
      sandbox.spy(client, 'removeListener');
      sandbox.spy(client, 'once');

      client.waitUntilReady(fakeBroker, function (error) {
        error.should.not.be.empty;
        error.should.be.an.instanceOf(TimeoutError);
        sinon.assert.calledOnce(fakeBroker.getReadyEventName);
        sinon.assert.calledWith(client.removeListener, readyKey, sinon.match.func);
        sinon.assert.calledWith(client.once, readyKey, sinon.match.func);
        done();
      });

      clock.tick(client.options.requestTimeout + 1);
    });

    it('should yield if broker is ready before requestTimeout', function (done) {
      const fakeBroker = new BrokerWrapper(new FakeSocket());
      const readyKey = 'broker.host-ready';

      sandbox.stub(fakeBroker, 'getReadyEventName').returns(readyKey);
      sandbox.spy(client, 'removeListener');
      sandbox.spy(client, 'once');

      client.waitUntilReady(fakeBroker, function (error) {
        should(error).be.empty;
        sinon.assert.calledOnce(fakeBroker.getReadyEventName);
        sinon.assert.calledWith(client.once, readyKey, sinon.match.func);
        done();
      });

      clock.tick(client.options.requestTimeout - 1);
      client.emit(readyKey);
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
        error.should.be.an.instanceOf(TimeoutError);
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

    it('should set different brokerMetadata field on client emit', function (done) {
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
    let client;

    afterEach(function (done) {
      client.close(done);
    });

    it('should connect plaintext', function (done) {
      client = new Client({
        kafkaHost: 'localhost:9092'
      });
      client.once('error', done);
      client.once('ready', function () {
        client.brokerMetadata.should.not.be.empty;
        client.ready.should.be.true;
        done();
      });
    });

    it('should error when connecting to an invalid host', function (done) {
      client = new Client({
        connectRetryOptions: {
          retries: 0
        },
        kafkaHost: 'localhost:9094'
      });

      client.on('error', function (error) {
        client.ready.should.be.false;
        error.code.should.be.eql('ECONNREFUSED');
        done();
      });
    });

    it('should connect SSL', function (done) {
      client = new Client({
        kafkaHost: 'localhost:9093',
        sslOptions: {
          rejectUnauthorized: false
        }
      });
      client.once('error', done);
      client.once('ready', function () {
        client.ready.should.be.true;
        client.brokerMetadata.should.not.be.empty;
        done();
      });
    });
  });

  describe('#refreshBrokerMetadata', function () {
    let sandbox, client;

    beforeEach(function () {
      sandbox = sinon.sandbox.create();
      client = new Client({
        connectRetryOptions: {
          retries: 0
        },
        autoConnect: false,
        kafkaHost: 'localhost:9093',
        sslOptions: {
          rejectUnauthorized: false
        }
      });
    });

    afterEach(function () {
      sandbox.restore();
    });

    it('should refresh broker metadata using available broker', function (done) {
      const fakeBroker = new BrokerWrapper(new FakeSocket());
      const fakeDeadBroker = new BrokerWrapper(new FakeSocket());

      const metadata = [
        {
          '1': { nodeId: 1, host: 'Kafka-1.us-east-1.myapp.com', port: 9093 },
          '2': { nodeId: 2, host: 'Kafka-2.us-east-1.myapp.com', port: 9093 },
          '3': { nodeId: 3, host: 'Kafka-3.us-east-1.myapp.com', port: 9093 }
        },
        { metadata: {} }
      ];

      client.brokerMetadata = _.clone(metadata[0]);

      const newBrokerKey = `${metadata[0][2].host}:${metadata[0][2].port}`;
      const deadBrokerKey = `${metadata[0][1].host}:${metadata[0][1].port}`;

      client.brokers[deadBrokerKey] = fakeDeadBroker;
      client.brokers[newBrokerKey] = fakeBroker;

      delete metadata[0]['1'];

      Object.assign(fakeBroker.socket, metadata['2']);

      sandbox.stub(client, 'getAvailableBroker').yields(null, fakeBroker);
      sandbox.stub(client, 'loadMetadataFrom').yields(null, metadata);
      sandbox.spy(client, 'updateMetadatas');
      sandbox.spy(client, 'refreshBrokers');

      should(client.refreshingMetadata).be.empty;

      client.refreshBrokerMetadata(function (error) {
        sinon.assert.calledOnce(client.getAvailableBroker);
        sinon.assert.calledWith(client.loadMetadataFrom, fakeBroker, sinon.match.func);
        sinon.assert.calledWith(client.updateMetadatas, metadata);
        sinon.assert.calledOnce(client.refreshBrokers);
        sinon.assert.callOrder(
          client.getAvailableBroker,
          client.loadMetadataFrom,
          client.updateMetadatas,
          client.refreshBrokers
        );

        client.brokers.should.have.property(newBrokerKey).and.be.exactly(fakeBroker);
        client.brokers.should.not.have.property(deadBrokerKey);

        done(error);
      });
    });

    it('should emit an error', function (done) {
      const expectedError = new Error('Unable to find available brokers to try');
      sandbox.stub(client, 'getAvailableBroker').yields(expectedError);
      sandbox.stub(client, 'loadMetadataFrom');
      sandbox.stub(client, 'updateMetadatas');
      sandbox.stub(client, 'refreshBrokers');

      client.on('error', function (error) {
        error.should.be.an.instanceOf(Error);
        error.nested.should.be.eql(expectedError);
        done();
      });

      client.refreshBrokerMetadata();
    });

    it('should not perform refreshBrokerMetadata if one is in progress', function () {
      sandbox.stub(client, 'getAvailableBroker');
      sandbox.stub(client, 'loadMetadataFrom');
      sandbox.stub(client, 'updateMetadatas');
      sandbox.stub(client, 'refreshBrokers');
      client.refreshingMetadata = true;

      client.refreshBrokerMetadata();

      client.refreshingMetadata.should.be.true;
      sinon.assert.notCalled(client.getAvailableBroker);
      sinon.assert.notCalled(client.loadMetadataFrom);
      sinon.assert.notCalled(client.updateMetadatas);
      sinon.assert.notCalled(client.refreshBrokers);
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

      const fakeSocket = new FakeSocket();

      sandbox.spy(fakeSocket, 'destroy');
      sandbox.spy(fakeSocket, 'end');
      sandbox.spy(fakeSocket, 'unref');

      sandbox.stub(client, 'setupBroker').returns({
        socket: fakeSocket
      });

      client.connect();
      client.once('error', function (error) {
        error.should.be.an.instanceOf(TimeoutError);
        fakeSocket.closing.should.be.true;
        sinon.assert.callOrder(fakeSocket.end, fakeSocket.destroy, fakeSocket.unref);
        done();
      });

      clock.tick(10000);
    });
  });

  describe('#topicExists', function () {
    const createTopic = require('../docker/createTopic');
    let sandbox, client;

    beforeEach(function (done) {
      sandbox = sinon.sandbox.create();
      client = new Client({
        kafkaHost: 'localhost:9092'
      });
      client.once('ready', done);
    });

    afterEach(function (done) {
      sandbox.restore();
      client.close(done);
    });

    it('should not yield error when single topic exists', function (done) {
      const topic = uuid.v4();

      createTopic(topic, 1, 1).then(function () {
        client.topicExists([topic], done);
      });
    });

    it('should yield error when given group of topics do not exist', function (done) {
      sandbox.spy(client, 'loadMetadataForTopics');
      sandbox.spy(client, 'updateMetadatas');

      const nonExistantTopics = _.times(3, () => uuid.v4());

      client.topicExists(nonExistantTopics, function (error) {
        error.should.be.an.instanceOf(TopicsNotExistError);
        sinon.assert.calledOnce(client.updateMetadatas);
        sinon.assert.calledWith(client.loadMetadataForTopics, []);
        sinon.assert.callOrder(client.loadMetadataForTopics, client.updateMetadatas);
        error.topics.should.be.eql(nonExistantTopics);
        done();
      });
    });
  });
});
