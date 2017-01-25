'use strict';

var retry = require('retry');
var kafka = require('..');
var Client = kafka.Client;

var host = process.env['KAFKA_TEST_HOST'] || '';

var client = null;

before(function (done) {
  var TIMEOUT = 45000;
  this.timeout(TIMEOUT);

  var operation = retry.operation({
    retries: 200,
    factor: 1,
    minTimeout: 3000
  });

  operation.attempt(function () {
    client = new Client(host);
    console.log('Creating new client.');

    let timer = setTimeout(function () {
      console.log('Kafka not responding...');
      client.close(function () {
        operation.retry(new Error('No Response'));
      });
    }, 2000);

    client.on('connect', function () {
      clearTimeout(timer);
      console.log(client.brokerMetadata);
      done();
    });

    client.on('error', function (error) {
      clearTimeout(timer);
      if (error.name !== 'NO_NODE') {
        console.error('Unexpected error', error);
      } else {
        console.log('Kafka not ready yet...');
      }
      client.close(function () {
        operation.retry(error);
      });
    });
  });
});
