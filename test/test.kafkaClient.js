'use strict';

const kafka = require('..');
const Client = kafka.KafkaClient;


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
      console.log('test error', error)
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
});
