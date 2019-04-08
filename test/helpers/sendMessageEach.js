'use strict';

const KafkaClient = require('../../lib/kafkaClient');
const HighLevelProducer = require('../../lib/highLevelProducer');
const async = require('async');

function sendMessage (message, topic, done) {
  var client = new KafkaClient({ kafkaHost: '127.0.0.1:9092' });
  var producer = new HighLevelProducer(client, { requireAcks: 1 });

  client.on('connect', function () {
    async.each(
      message,
      function (message, callback) {
        producer.send([{ topic: topic, messages: message, attributes: 1 }], callback);
      },
      function (error) {
        if (error) {
          done(error);
        } else {
          producer.close(function () {
            done(null);
          });
        }
      }
    );
  });
}

module.exports = sendMessage;
