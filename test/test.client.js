var host = process.env['KAFKA_TEST_HOST'] || '';
var kafka = require('..');
var Client = kafka.Client;
var uuid = require('node-uuid');
var should = require('should');

describe('Client', function () {
  var client = null;

  beforeEach(function (done) {
    var clientId = 'kafka-node-client-' + uuid.v4();
    client = new Client(host, clientId, undefined, undefined, {rejectUnauthorized: false});
    client.on('ready', done);
  });

  describe('#reconnectBroker', function () {
    var emptyFn = function () {};

    it('should cache brokers correctly for SSL after a reconnect', function (done) {
      client.on('error', emptyFn);
      var broker = client.brokers[Object.keys(client.brokers)[0]];
      broker.socket.emit('error', new Error('Mock error'));
      broker.socket.end();
      client.on('connect', function () {
        Object.keys(client.brokers).should.have.lengthOf(1);
        done();
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
