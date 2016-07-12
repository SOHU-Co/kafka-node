var host = process.env['KAFKA_TEST_HOST'] || '';
var kafka = require('..');
var Client = kafka.Client;
var uuid = require('node-uuid');
var should = require('should');

describe('Client', function () {
  var client = null;

  describe('#setupBrokerProfiles', function () {
    beforeEach(function (done) {
      var clientId = 'kafka-node-client-' + uuid.v4();
      client = new Client(host, clientId, undefined, undefined, {rejectUnauthorized: false});
      client.on('ready', done);
    });

    it('should contain SSL options', function () {
      should.exist(client.brokerProfiles);
      should(client.brokerProfiles).have.property('127.0.0.1:9092');
      var profile = client.brokerProfiles['127.0.0.1:9092'];
      should(profile).have.property('host').and.be.exactly('127.0.0.1');
      should(profile).have.property('port').and.be.exactly(9092);
      should(profile).have.property('sslHost').and.be.exactly('127.0.0.1');
      should(profile).have.property('sslPort').and.be.exactly(9093);
    });
  });
});
