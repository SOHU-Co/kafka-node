var host = process.env['KAFKA_TEST_HOST'] || '';
var kafka = require('..');
var Client = kafka.Client;
var uuid = require('node-uuid');
var should = require('should');
var FakeZookeeper = require('./mocks/mockZookeeper');
var InvalidConfigError = require('../lib/errors/InvalidConfigError');
var proxyquire = require('proxyquire');
var sinon = require('sinon');

describe('Client', function () {
  var client = null;

  describe('on brokersChanged', function () {
    var zk, Client, brokers;

    before(function () {
      brokers = {
        '1001': {
          endpoints: [ 'PLAINTEXT://127.0.0.1:9092', 'SSL://127.0.0.1:9093' ],
          host: '127.0.0.1',
          version: 2,
          port: 9092
        },
        '1002': {
          endpoints: [ 'PLAINTEXT://127.0.0.2:9092', 'SSL://127.0.0.2:9093' ],
          host: '127.0.0.2',
          version: 2,
          port: 9092
        }
      };

      zk = new FakeZookeeper();

      Client = proxyquire('../lib/Client', {
        './zookeeper': {
          Zookeeper: function () {
            return zk;
          }
        }
      });
    });

    it('should keep brokerProfiles in sync with broker changes', function () {
      var clientId = 'kafka-node-client-' + uuid.v4();
      client = new Client(host, clientId, undefined, undefined, {rejectUnauthorized: false});

      sinon.spy(client, 'setupBrokerProfiles');
      sinon.stub(client, 'createBroker').returns({
        socket: {
          close: function () {},
          end: function () {}
        }
      });

      zk.emit('init', brokers);
      client.brokerMetadata.should.have.property('1001');
      delete brokers['1001'];
      zk.emit('brokersChanged', brokers);
      client.brokerMetadata.should.not.have.property('1001');
      sinon.assert.calledTwice(client.setupBrokerProfiles);
    });
  });

  describe('#constructor', function () {
    function validateThrowsInvalidConfigError (clientId) {
      should.throws(function () {
        client = new Client(host, clientId);
      }, InvalidConfigError);
    }

    function validateDoesNotThrowInvalidConfigError (clientId) {
      should.doesNotThrow(function () {
        client = new Client(host, clientId);
      });
    }

    it('should throws an error on invalid client IDs', function () {
      validateThrowsInvalidConfigError('myClientId:12345');
      validateThrowsInvalidConfigError('myClientId,12345');
      validateThrowsInvalidConfigError('myClientId"12345"');
      validateThrowsInvalidConfigError('myClientId?12345');
    });

    it('should not throw on valid client IDs', function () {
      validateDoesNotThrowInvalidConfigError('myClientId.12345');
      validateDoesNotThrowInvalidConfigError('something_12345');
      validateDoesNotThrowInvalidConfigError('myClientId-12345');
    });
  });

  describe('non constructor', function () {
    beforeEach(function (done) {
      var clientId = 'kafka-node-client-' + uuid.v4();
      client = new Client(host, clientId, undefined, undefined, {rejectUnauthorized: false});
      client.once('connect', done);
    });

    describe('#reconnectBroker', function () {
      var emptyFn = function () {};

      it('should cache brokers correctly for SSL after reconnects', function (done) {
        client.on('error', emptyFn);
        var broker = client.brokers[Object.keys(client.brokers)[0]];
        broker.socket.emit('error', new Error('Mock error'));
        broker.socket.end();
        client.once('reconnect', function () {
          Object.keys(client.brokers).should.have.lengthOf(1);
          broker = client.brokers[Object.keys(client.brokers)[0]];
          broker.socket.emit('error', new Error('Mock error'));
          broker.socket.end();
          client.once('reconnect', function () {
            Object.keys(client.brokers).should.have.lengthOf(1);
            done();
          });
        });
      });
    });

    describe('#setupBrokerProfiles', function () {
      it('should contain SSL options', function () {
        should.exist(client.brokerProfiles);
        var brokerKey = host + ':9092';
        should(client.brokerProfiles).have.property(brokerKey);
        var profile = client.brokerProfiles[brokerKey];
        should(profile).have.property('host').and.be.exactly(host);
        should(profile).have.property('port').and.be.exactly(9092);
        should(profile).have.property('sslHost').and.be.exactly(host);
        should(profile).have.property('sslPort').and.be.exactly(9093);
      });
    });
  });
});
