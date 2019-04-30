'use strict';

const proxyquire = require('proxyquire');
const kafka = require('..');
const Client = kafka.KafkaClient;
const sinon = require('sinon');
const TimeoutError = require('../lib/errors/TimeoutError');
const TopicsNotExistError = require('../lib/errors/TopicsNotExistError');
const NotControllerError = require('../lib/errors/NotControllerError');
const SaslAuthenticationError = require('../lib/errors/SaslAuthenticationError');
const BrokerWrapper = require('../lib/wrapper/BrokerWrapper');
const FakeSocket = require('./mocks/mockSocket');
const should = require('should');
const _ = require('lodash');
const uuid = require('uuid');
const BufferList = require('bl');
const retry = require('retry');

describe('Kafka Client', function () {
  describe('Discover Group Coordinator', function () {
    let client;
    beforeEach(function (done) {
      client = new Client();
      client.once('connect', done);
    });

    afterEach(function (done) {
      client.close(done);
    });

    it('#sendGroupCoordinatorRequest', function (done) {
      var operation = retry.operation();
      operation.attempt(function () {
        client.sendGroupCoordinatorRequest('ExampleTopic', function (error, response) {
          if (operation.retry(error)) {
            return;
          }
          should(error).be.null;
          response.coordinatorPort.should.be.eql(9092);
          response.coordinatorHost.should.be.eql('127.0.0.1');
          done();
        });
      });
    });
  });

  describe('#handleReceivedData', function () {
    let socket;

    beforeEach(function () {
      socket = {
        buffer: new BufferList()
      };
    });

    it('should always consume entire response even if handlers are missing', function () {
      const fakeClient = {
        invokeResponseCallback: sinon.stub().returns(null)
      };

      sinon.spy(socket.buffer, 'consume');
      sinon
        .stub(socket.buffer, 'readUInt32BE')
        .onFirstCall()
        .returns(0);
      sinon.stub(socket.buffer, 'shallowSlice').returns({
        readUInt32BE: sinon.stub().returns(25)
      });

      socket.buffer.append(Uint8Array.from([0, 0, 0, 0]));
      Client.prototype.handleReceivedData.call(fakeClient, socket);
      sinon.assert.calledOnce(socket.buffer.shallowSlice);
      sinon.assert.calledOnce(socket.buffer.consume);
      sinon.assert.calledOnce(fakeClient.invokeResponseCallback);
      should(socket.waiting).be.empty;
    });

    it('should consume entire response if handlers are missing and set waiting to false for longpolling sockets', function () {
      const fakeClient = {
        invokeResponseCallback: sinon.stub().returns(null)
      };

      sinon.spy(socket.buffer, 'consume');
      sinon
        .stub(socket.buffer, 'readUInt32BE')
        .onFirstCall()
        .returns(0);
      sinon.stub(socket.buffer, 'shallowSlice').returns({
        readUInt32BE: sinon.stub().returns(25)
      });

      socket.longpolling = true;
      socket.waiting = true;

      socket.buffer.append(Uint8Array.from([0, 0, 0, 0]));
      Client.prototype.handleReceivedData.call(fakeClient, socket);
      sinon.assert.calledOnce(socket.buffer.shallowSlice);
      sinon.assert.calledOnce(socket.buffer.consume);
      sinon.assert.calledOnce(fakeClient.invokeResponseCallback);
      socket.waiting.should.be.false;
    });

    it('should early return when buffer is beyond offset', function () {
      const fakeClient = {
        invokeResponseCallback: function () {}
      };

      socket.buffer.append(Uint8Array.from([0, 0, 0]));

      const readSpy = sinon.spy(socket.buffer, 'readUInt32BE');
      Client.prototype.handleReceivedData.call(fakeClient, socket);
      sinon.assert.notCalled(readSpy);
    });

    it('should early return when buffer is empty', function () {
      const fakeClient = {
        invokeResponseCallback: function () {}
      };

      const readSpy = sinon.spy(socket.buffer, 'readUInt32BE');
      Client.prototype.handleReceivedData.call(fakeClient, socket);
      sinon.assert.notCalled(readSpy);
    });
  });

  describe('#parseHostList', function () {
    it('initial hosts should be parsed if single host is provided', function () {
      const client = new Client({
        autoConnect: false,
        kafkaHost: 'localhost:9092'
      });

      client.initialHosts.should.not.be.empty;
      client.initialHosts.length.should.be.eql(1);
      client.initialHosts[0].host.should.be.eql('localhost');
      client.initialHosts[0].port.should.be.eql(9092);
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
          port: 9092
        },
        {
          host: '127.0.0.1',
          port: 9093
        },
        {
          host: '192.168.1.0',
          port: 9094
        }
      ]);
    });
  });

  describe('#createBroker', function () {
    let sandbox, Client, mockSocket;
    beforeEach(function () {
      sandbox = sinon.sandbox.create();
      mockSocket = new FakeSocket();
      Client = proxyquire('../lib/kafkaClient', {
        net: {
          createConnection () {
            return mockSocket;
          }
        }
      });
    });

    afterEach(function () {
      sandbox.restore();
    });

    it('should not emit connect event when fails to initializeBroker', function () {
      const client = new Client({ autoConnect: false });
      sandbox.stub(client, 'initializeBroker').yields(new Error('fake error'));
      sandbox.spy(client, 'emit');

      client.createBroker('fakehost', 9092, true);

      mockSocket.emit('connect');
      sinon.assert.calledOnce(client.initializeBroker);
      sinon.assert.notCalled(client.emit);
    });

    it('should not emit reconnect event when fails to initializeBroker', function () {
      const client = new Client({ autoConnect: false });
      sandbox.stub(client, 'initializeBroker').yields(new Error('fake error'));
      sandbox.spy(client, 'emit');

      client.createBroker('fakehost', 9092, true);

      mockSocket.error = new Error('some socket error');

      mockSocket.emit('connect');
      sinon.assert.calledOnce(client.initializeBroker);
      sinon.assert.notCalled(client.emit);
    });

    it('should not reconnect when broker is no longer valid', function () {
      sandbox.useFakeTimers();
      const client = new Client({ autoConnect: false });
      client.brokerMetadata = {
        '1001': {
          host: 'localhost',
          port: 9092
        },
        '1002': {
          host: 'kafkaServer',
          port: 9092
        }
      };
      sandbox.stub(client, 'reconnectBroker');
      client.createBroker('fakehost', 9092, true);
      mockSocket.emit('end');
      sandbox.clock.tick(1000);
      sinon.assert.notCalled(client.reconnectBroker);
    });

    it('should try reconnecting when client is initializing', function () {
      sandbox.useFakeTimers();
      const client = new Client({ autoConnect: false });
      client.connecting = true;
      client.brokerMetadata = {};
      sandbox.stub(client, 'reconnectBroker');
      client.createBroker('fakehost', 9092, true);
      mockSocket.emit('end');
      sandbox.clock.tick(1000);
      sinon.assert.calledOnce(client.reconnectBroker);
    });

    it('should schedule refresh of metadata when socket is closed', function (done) {
      const client = new Client({ autoConnect: false });
      sandbox.stub(client, 'refreshBrokerMetadata').callsFake(done);
      client.createBroker('fakehost', 9092, true);
      mockSocket.emit('close', false);
    });

    it('should not schedule refresh of metadata when client is initalizing', function (done) {
      const client = new Client({ autoConnect: false });
      client.connecting = true;
      sandbox.stub(client, 'refreshBrokerMetadata');
      client.createBroker('fakehost', 9092, true);
      mockSocket.emit('close', false);
      setImmediate(function () {
        sinon.assert.notCalled(client.refreshBrokerMetadata);
        done();
      });
    });

    it('should not schedule metadata refresh when broker is closed due to being idle', function () {
      const client = new Client({ autoConnect: false });
      const brokerWrapper = client.createBroker('fakehost', 9092, true);

      sandbox.stub(brokerWrapper, 'isIdle').returns(true);
      sandbox.stub(client, 'refreshBrokerMetadata');
      sandbox.useFakeTimers();

      mockSocket.emit('close', false);
      sandbox.clock.tick();
      sinon.assert.notCalled(client.refreshBrokerMetadata);
    });

    it('should reconnect when broker is closed due to being idle', function () {
      const client = new Client({ autoConnect: false, reconnectOnIdle: true });
      const brokerWrapper = client.createBroker('fakehost', 9092, true);

      sandbox.stub(brokerWrapper, 'isIdle').returns(true);
      sandbox.stub(client, 'isValidBroker').returns(true);
      sandbox.stub(client, 'reconnectBroker');
      sandbox.useFakeTimers();

      mockSocket.emit('close', false);
      sandbox.clock.tick(1001);
      sinon.assert.calledWithExactly(client.reconnectBroker, mockSocket);
    });
  });

  describe('#sendRequest', function () {
    let sandbox;
    before(function () {
      sandbox = sinon.sandbox.create();
    });

    afterEach(function () {
      sandbox.restore();
    });

    it('should call refreshBrokerMetadata if broker is not connected', function (done) {
      const client = new Client({
        autoConnect: false,
        kafkaHost: 'localhost:9092'
      });

      const fakeSocket = new FakeSocket();
      fakeSocket.destroyed = true;

      const fakeBroker = new BrokerWrapper(fakeSocket);
      fakeBroker.apiSupport = {};

      const BrokerNotAvailableError = require('../lib/errors').BrokerNotAvailableError;

      sandbox.stub(client, 'leaderByPartition').returns('1001');
      sandbox.stub(client, 'brokerForLeader').returns(fakeBroker);
      sandbox.stub(client, 'refreshBrokerMetadata');

      const request = {
        type: 'produce',
        data: {
          payloads: [
            {
              topic: 'test-topic',
              partition: 0,
              messages: [
                {
                  magic: 0,
                  attributes: 0,
                  key: 'test-key',
                  value: 'test-message',
                  timestamp: 1511365962702
                }
              ]
            }
          ]
        },
        args: [1, 100],
        requireAcks: 1
      };

      client.sendRequest(request, function (error) {
        sinon.assert.calledOnce(client.refreshBrokerMetadata);
        error.should.not.be.empty;
        error.should.be.an.instanceOf(BrokerNotAvailableError);
        done();
      });
    });
  });

  describe('Versions', function () {
    let client;
    afterEach(function (done) {
      client.close(done);
    });

    describe('#initializeBroker', function () {
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
        kafkaHost: 'localhost:9095'
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

    describe('using SASL authentication', function () {
      before(function () {
        // these tests should not run again Kafka 0.8 & 0.9
        const supportsSaslPlain =
          !process.env.KAFKA_VERSION || (process.env.KAFKA_VERSION !== '0.8' && process.env.KAFKA_VERSION !== '0.9');
        if (!supportsSaslPlain) {
          this.skip();
        }
      });

      it('should connect SASL/PLAIN', function (done) {
        client = new Client({
          kafkaHost: 'localhost:9094',
          sasl: {
            mechanism: 'plain',
            username: 'kafkanode',
            password: 'kafkanode'
          },
          connectRetryOptions: {
            retries: 0
          }
        });
        client.once('error', done);
        client.once('ready', function () {
          client.ready.should.be.true;
          client.brokerMetadata.should.not.be.empty;
          done();
        });
      });

      it('should not connect SASL/PLAIN with bad credentials', function (done) {
        client = new Client({
          kafkaHost: 'localhost:9094',
          sasl: {
            mechanism: 'plain',
            username: 'kafkanode',
            password: 'badpasswd'
          },
          connectRetryOptions: {
            retries: 0
          }
        });
        client.once('error', function (err) {
          if (err instanceof SaslAuthenticationError) {
            // expected
            done();
          } else {
            done(err);
          }
        });
        client.once('ready', function () {
          var err = new Error('expected error!');
          done(err);
        });
      });
    });
  });

  describe('#updateMetadatas', function () {
    let client, sandbox;

    const updatedPartialMetadata = [
      null,
      {
        metadata: {
          topic1: {
            '0': {
              topic: 'topic1',
              partition: 0,
              leader: 1001
            },
            '1': {
              topic: 'topic1',
              partition: 1,
              leader: 1001
            },
            '2': {
              topic: 'topic1',
              partition: 2,
              leader: 1001
            }
          }
        }
      }
    ];

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

    it('should extend topicMetadata by default instead of replace', function () {
      client.topicMetadata = {
        topic0: {
          '0': {
            topic: 'topic0',
            partition: 0,
            leader: 1001
          }
        },
        topic1: {
          '0': {
            topic: 'topic1',
            partition: 0,
            leader: 1001
          }
        }
      };
      sandbox.stub(client, 'setBrokerMetadata');
      client.updateMetadatas(updatedPartialMetadata);
      client.topicMetadata.topic0.should.not.be.empty;
      client.topicMetadata.topic1.should.have.property('1', {
        topic: 'topic1',
        partition: 1,
        leader: 1001
      });
      client.topicMetadata.topic1.should.have.property('2', {
        topic: 'topic1',
        partition: 2,
        leader: 1001
      });
      sinon.assert.calledWithExactly(client.setBrokerMetadata, null);
    });

    it('should replace if second parameter replaceTopicMetadata is true', function () {
      client.topicMetadata = {
        topic0: {
          '0': {
            topic: 'topic0',
            partition: 0,
            leader: 1001
          }
        },
        topic1: {
          '0': {
            topic: 'topic1',
            partition: 0,
            leader: 1001
          }
        }
      };
      sandbox.stub(client, 'setBrokerMetadata');

      client.topicMetadata.should.not.be.empty;

      client.updateMetadatas(updatedPartialMetadata, true);

      client.topicMetadata.should.not.have.property('topic0');
      sinon.assert.calledWithExactly(client.setBrokerMetadata, null);
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

    it('should not perform refreshBrokerMetadata if consumer is closing', function () {
      sandbox.stub(client, 'getAvailableBroker');
      sandbox.stub(client, 'loadMetadataFrom');
      sandbox.stub(client, 'updateMetadatas');
      sandbox.stub(client, 'refreshBrokers');
      client.closing = true;

      client.refreshBrokerMetadata();

      client.closing.should.be.true;
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

  describe('#getListGroups', function () {
    let client;

    afterEach(function (done) {
      client.close(done);
    });

    it('should error if the client is not ready', function (done) {
      client = new Client({
        kafkaHost: 'localhost:9092',
        autoConnect: false
      });
      client.getListGroups(function (error, res) {
        error.should.be.an.instanceOf(Error);
        error.message.should.be.exactly('Client is not ready (getListGroups)');
        done();
      });
    });
  });

  describe('#createTopics', function () {
    let client;

    before(function () {
      if (process.env.KAFKA_VERSION === '0.9' || process.env.KAFKA_VERSION === '0.10') {
        return this.skip();
      }
    });

    beforeEach(function (done) {
      client = new Client({
        kafkaHost: 'localhost:9092'
      });
      client.once('ready', done);
    });

    afterEach(function (done) {
      client.close(done);
    });

    it('should create given topics', function (done) {
      const topic1 = uuid.v4();
      const topic1ReplicationFactor = 1;
      const topic1Partitions = 5;
      const topic2 = uuid.v4();
      const topic2ReplicationFactor = 1;
      const topic2Partitions = 1;

      client.createTopics(
        [
          {
            topic: topic1,
            partitions: topic1Partitions,
            replicationFactor: topic1ReplicationFactor
          },
          {
            topic: topic2,
            partitions: topic2Partitions,
            replicationFactor: topic2ReplicationFactor
          }
        ],
        (error, result) => {
          should.not.exist(error);
          result.should.be.empty;

          // Verify topics were properly created with partitions + replication factor by fetching metadata again
          const verifyPartitions = (topicMetadata, expectedPartitionCount, expectedReplicatonfactor) => {
            for (let i = 0; i < expectedPartitionCount; i++) {
              topicMetadata[i].partition.should.be.exactly(i);
              topicMetadata[i].replicas.length.should.be.exactly(expectedReplicatonfactor);
            }
          };

          client.loadMetadataForTopics([topic1, topic2], (error, result) => {
            should.not.exist(error);
            verifyPartitions(result[1].metadata[topic1], topic1Partitions, topic1ReplicationFactor);
            verifyPartitions(result[1].metadata[topic2], topic2Partitions, topic2ReplicationFactor);
            done();
          });
        }
      );
    });

    it('should return topic creation errors', function (done) {
      const topic = uuid.v4();
      // Only 1 broker is available under test, so a replication factor > 1 is not possible
      const topicReplicationFactor = 2;
      const topicPartitions = 5;

      client.createTopics(
        [
          {
            topic: topic,
            partitions: topicPartitions,
            replicationFactor: topicReplicationFactor
          }
        ],
        (error, result) => {
          should.not.exist(error);
          result.should.have.length(1);
          result[0].topic.should.be.exactly(topic);
          result[0].error.toLowerCase().should.startWith('replication factor: 2 larger than available brokers: 1');
          done();
        }
      );
    });

    it('should create topics with config entries', function (done) {
      const topic1 = uuid.v4();
      const topic1ReplicationFactor = 1;
      const topic1Partitions = 5;

      client.createTopics(
        [
          {
            topic: topic1,
            partitions: topic1Partitions,
            replicationFactor: topic1ReplicationFactor,
            configEntries: [
              {
                name: 'compression.type',
                value: 'gzip'
              },
              {
                name: 'min.compaction.lag.ms',
                value: '50'
              }
            ]
          }
        ],
        (error, result) => {
          should.not.exist(error);
          result.should.be.empty;

          const resource = {
            resourceType: 'topic',
            resourceName: topic1,
            configNames: []
          };

          const payload = {
            resources: [resource]
          };

          client.describeConfigs(payload, (error, result) => {
            should.not.exist(error);
            result[0].resourceName.should.be.exactly(topic1);
            result[0].configEntries
              .filter(c => {
                return c.configName === 'compression.type';
              })[0]
              .configValue.should.be.exactly('gzip');
            result[0].configEntries
              .filter(c => {
                return c.configName === 'min.compaction.lag.ms';
              })[0]
              .configValue.should.be.exactly('50');
            done();
          });
        }
      );
    });

    it('should return topic creation errors with invalid config entries', function (done) {
      const topic1 = uuid.v4();
      const topic1ReplicationFactor = 1;
      const topic1Partitions = 5;

      client.createTopics(
        [
          {
            topic: topic1,
            partitions: topic1Partitions,
            replicationFactor: topic1ReplicationFactor,
            configEntries: [
              {
                name: 'compression.ty',
                value: 'gzip'
              }
            ]
          }
        ],
        (error, result) => {
          should.not.exist(error);
          result.should.have.length(1);
          result[0].topic.should.be.exactly(topic1);
          result[0].error.toLowerCase().should.startWith('unknown topic config name: compression.ty');
          done();
        }
      );
    });

    it('should create topics with explicit replica assignments if both simple and explicit assignment is provided', function (done) {
      const topic1 = uuid.v4();
      const topic1ReplicationFactor = 1;
      const topic1Partitions = 5;

      client.createTopics(
        [
          {
            topic: topic1,
            partitions: topic1Partitions,
            replicationFactor: topic1ReplicationFactor,
            replicaAssignment: [
              {
                partition: 0,
                replicas: [1001]
              },
              {
                partition: 1,
                replicas: [1001]
              }
            ]
          }
        ],
        (error, result) => {
          should.not.exist(error);
          result.should.be.empty;

          client.loadMetadataForTopics([topic1], (error, result) => {
            should.not.exist(error);

            const topicMetadata = result[1].metadata[topic1];
            Object.keys(topicMetadata).should.have.length(2);
            topicMetadata['0'].partition.should.be.exactly(0);
            topicMetadata['0'].replicas.should.have.length(1);
            topicMetadata['1'].partition.should.be.exactly(1);
            topicMetadata['1'].replicas.should.have.length(1);
            done();
          });
        }
      );
    });

    it('should create topics with replica assignments if only explicit assignment is provided', function (done) {
      const topic1 = uuid.v4();

      client.createTopics(
        [
          {
            topic: topic1,
            replicaAssignment: [
              {
                partition: 0,
                replicas: [1001]
              },
              {
                partition: 1,
                replicas: [1001]
              }
            ]
          }
        ],
        (error, result) => {
          should.not.exist(error);
          result.should.be.empty;

          client.loadMetadataForTopics([topic1], (error, result) => {
            should.not.exist(error);

            const topicMetadata = result[1].metadata[topic1];
            Object.keys(topicMetadata).should.have.length(2);
            topicMetadata['0'].partition.should.be.exactly(0);
            topicMetadata['0'].replicas.should.have.length(1);
            topicMetadata['1'].partition.should.be.exactly(1);
            topicMetadata['1'].replicas.should.have.length(1);
            done();
          });
        }
      );
    });
  });

  describe('#wrapControllerCheckIfNeeded', function () {
    let client, sandbox;

    beforeEach(function (done) {
      if (process.env.KAFKA_VERSION === '0.9') {
        return this.skip();
      }

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

    it('should not wrap again if already wrapped', function () {
      const fn = _.noop;

      const wrapped = client.wrapControllerCheckIfNeeded('', [], fn);
      const secondWrapped = client.wrapControllerCheckIfNeeded('', [], wrapped);

      wrapped.should.be.exactly(secondWrapped);
    });

    it('should wrap if not already wrapped', function () {
      const fn = _.noop;

      const wrapped = client.wrapControllerCheckIfNeeded(_.noop, _.noop, [], fn);

      wrapped.should.not.be.exactly(fn);
    });

    it('should set controller id to null if NotControllerError was returned once', function () {
      const fn = _.noop;
      const requestType = 'createTopics';
      const wrapped = client.wrapControllerCheckIfNeeded(requestType, [], fn);
      const setControllerIdSpy = sandbox.spy(client, 'setControllerId');
      sandbox.stub(client, 'sendControllerRequest');

      wrapped(new NotControllerError('not controller'));

      sinon.assert.calledOnce(setControllerIdSpy);
      sinon.assert.alwaysCalledWithExactly(setControllerIdSpy, null);
    });

    it('should send controller request again if NotControllerError was returned once', function () {
      var args = [];
      const requestType = 'createTopics';
      const fn = _.noop;
      const wrapped = client.wrapControllerCheckIfNeeded(requestType, args, fn);
      const setControllerIdSpy = sandbox.spy(client, 'setControllerId');
      const sendControllerRequestSpy = sandbox.stub(client, 'sendControllerRequest');

      wrapped(new NotControllerError('not controller'));

      sinon.assert.calledOnce(setControllerIdSpy);
      sinon.assert.alwaysCalledWithExactly(setControllerIdSpy, null);
      sinon.assert.calledOnce(sendControllerRequestSpy);
      sinon.assert.alwaysCalledWithExactly(sendControllerRequestSpy, requestType, args, wrapped);
    });

    it('should set controller id to null and call original callback if NotControllerError was returned on second try', function () {
      const fnSpy = sandbox.spy();
      const requestType = 'createTopics';
      const wrapped = client.wrapControllerCheckIfNeeded(requestType, [], fnSpy);
      const setControllerIdSpy = sandbox.spy(client, 'setControllerId');
      sandbox.stub(client, 'sendControllerRequest');

      wrapped(new NotControllerError('not controller'));
      wrapped(new NotControllerError('not controller'));

      sinon.assert.calledTwice(setControllerIdSpy);
      sinon.assert.alwaysCalledWithExactly(setControllerIdSpy, null);
      sinon.assert.calledOnce(fnSpy);
    });

    it('should call original callback if another error was returned', function () {
      const fnSpy = sandbox.spy();
      const requestType = 'createTopics';
      const wrapped = client.wrapControllerCheckIfNeeded(requestType, [], fnSpy);
      const setControllerIdSpy = sandbox.spy(client, 'setControllerId');

      wrapped(new TimeoutError('operation timed out'));

      sinon.assert.notCalled(setControllerIdSpy);
      sinon.assert.calledOnce(fnSpy);
    });

    it('should call original callback if no error was returned', function () {
      const fnSpy = sandbox.spy();
      const requestType = 'createTopics';
      const wrapped = client.wrapControllerCheckIfNeeded(requestType, [], fnSpy);
      const setControllerIdSpy = sandbox.spy(client, 'setControllerId');
      const expectedResult = [];

      wrapped(null, expectedResult);

      sinon.assert.notCalled(setControllerIdSpy);
      sinon.assert.calledOnce(fnSpy);
      sinon.assert.alwaysCalledWith(fnSpy, null, expectedResult);
    });
  });

  describe('#loadMetadataForTopics', function () {
    it('should request metadata from correct broker after ready', function (done) {
      const client = new Client({ autoConnect: false });
      const brokerAddr = uuid.v4();

      const brokerForLeaderStub = sinon.stub(client, 'brokerForLeader');
      sinon.spy(client, 'waitUntilReady');

      const firstBroker = new BrokerWrapper(new FakeSocket());
      const secondBroker = new BrokerWrapper(new FakeSocket());

      firstBroker.socket.addr = brokerAddr;
      secondBroker.socket.addr = brokerAddr;

      brokerForLeaderStub.onFirstCall().returns(firstBroker);
      brokerForLeaderStub.onSecondCall().returns(secondBroker);

      client.connecting = true;
      client.loadMetadataForTopics([], function (error, result) {
        if (error) {
          return done(error);
        }
        sinon.assert.calledTwice(brokerForLeaderStub);
        done(null);
      });

      firstBroker.socket.destroyed = true;
      secondBroker.apiSupport = {
        metadata: {
          usable: 0
        }
      };

      sinon.stub(client, 'queueCallback').callsFake(function (socket, correlationId, coderAndCb) {
        setImmediate(function () {
          coderAndCb[1](null);
        });
      });

      client.emit(firstBroker.getReadyEventName());
    });
  });

  describe('#getController', function () {
    it('should use cached controller details even if controllerId is 0', function (done) {
      const client = new Client({ autoConnect: false });

      client.brokerMetadata = {
        '0': {
          host: 'fake-host',
          port: 1234
        }
      };

      client.clusterMetadata = {
        controllerId: 0
      };

      const broker = uuid.v4();

      sinon
        .stub(client, 'getBroker')
        .withArgs('fake-host', 1234)
        .returns(broker);

      const loadMetadataStub = sinon.stub(client, 'loadMetadata').yieldsAsync(null, [{}, {}]);

      client.getController(function (error, broker, controllerId) {
        sinon.assert.notCalled(loadMetadataStub);
        sinon.assert.calledOnce(client.getBroker);
        broker.should.be.eql(broker);
        done(error);
      });
    });
  });

  describe('#sendControllerRequest', function () {
    let client, sandbox;

    beforeEach(function (done) {
      if (process.env.KAFKA_VERSION === '0.9') {
        return this.skip();
      }

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

    it('should wrap callback', function () {
      const requestType = 'createTopics';
      const fakeBroker = new BrokerWrapper(new FakeSocket());
      sandbox.stub(client, 'getController').yields(null, fakeBroker, 1);
      sandbox.stub(client, 'queueCallback');
      const wrapControllerSpy = sandbox.spy(client, 'wrapControllerCheckIfNeeded');
      const callbackSpy = sandbox.spy();

      client.sendControllerRequest(requestType, [], callbackSpy);

      sinon.assert.calledOnce(wrapControllerSpy);
    });

    it('should be called twice when NotController error was returned', function () {
      const requestType = 'createTopics';
      const fakeBroker = new BrokerWrapper(new FakeSocket());
      sandbox.stub(client, 'getController').yields(null, fakeBroker, 1);
      sandbox.stub(client, 'sendRequestToBroker').callsFake((brokerId, requestType, args, callback) => {
        callback(new NotControllerError('not controller'));
      });
      const callbackSpy = sandbox.spy();
      const sendControllerRequestSpy = sandbox.spy(client, 'sendControllerRequest');

      client.sendControllerRequest(requestType, [], callbackSpy);

      sinon.assert.calledTwice(sendControllerRequestSpy);
    });

    it('should send request to controller', function () {
      const requestType = 'createTopics';
      const fakeBroker = new BrokerWrapper(new FakeSocket());
      sandbox.stub(client, 'getController').yields(null, fakeBroker, 1);
      const sendRequestSpy = sandbox.stub(client, 'sendRequestToBroker');
      const args = [];
      const callback = _.noop;

      client.sendControllerRequest(requestType, args, callback);

      sinon.assert.calledOnce(sendRequestSpy);
    });

    it('should return error if controller request fails', function () {
      const requestType = 'createTopics';
      const error = new TimeoutError('operation timed out');
      sandbox.stub(client, 'getController').yields(error);
      const callbackSpy = sandbox.spy();

      client.sendControllerRequest(requestType, null, callbackSpy);

      sinon.assert.calledOnce(callbackSpy);
      sinon.assert.alwaysCalledWithExactly(callbackSpy, error);
    });
  });
});
