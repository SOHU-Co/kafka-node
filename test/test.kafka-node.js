'use strict';

var kafka = require('..');
var Client = kafka.KafkaClient;

const host = '127.0.0.1:9092';

var client = null;

before(function (done) {
  client = new Client({ kafkaHost: host });
  client.once('ready', done);
  client.once('error', done);
});
