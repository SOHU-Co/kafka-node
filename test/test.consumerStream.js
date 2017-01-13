var should = require('should');

var through2 = require('through2');

var libPath = process.env['KAFKA_COV'] ? '../lib-cov/' : '../lib/';
var ConsumerStream = require(libPath + 'consumerStream');
var Producer = require(libPath + 'producer');
var Client = require(libPath + 'client');

var client, consumer, producer;

var TOPIC_POSTFIX = '_test_' + Date.now();
var EXISTS_TOPIC_1 = '_exists_1' + TOPIC_POSTFIX;
var EXISTS_TOPIC_2 = '_exists_2' + TOPIC_POSTFIX;

var host = process.env['KAFKA_TEST_HOST'] || 'localhost';

before(function (done) {
  client = new Client(host);
  producer = new Producer(client);
  producer.on('ready', function () {
    producer.createTopics([
      EXISTS_TOPIC_1,
      EXISTS_TOPIC_2
    ], false, function (err, created) {
      if (err) return done(err);

      function useNewTopics () {
        var messages = [];
        for (var i = 1; i <= 100; i++) {
          messages.push('stream message ' + i);
        }
        producer.send([{ topic: EXISTS_TOPIC_1, messages }], done);
      }
      // Ensure leader selection happened
      setTimeout(useNewTopics, 400);
    });
  });
});

describe('ConsumerStream', function () {
  it('should emit message when get new message', function (done) {
    var topics = [ { topic: EXISTS_TOPIC_1 } ];
    var options = { autoCommit: false, groupId: '_groupId_1_test', fetchMaxBytes: 1024 };
    consumer = new ConsumerStream(client, topics, options);
    var eventCount = {
      message: 0,
      data: 0,
      pipe: 0
    };
    var getEventCounter = function (name, done) {
      return function () {
        eventCount[name]++;
        if (eventCount[name] === 100 && done) {
          return done();
        }
      };
    };
    consumer.on('message', getEventCounter('message'));
    consumer.on('data', getEventCounter('pipe'));
    var pipeCount = 0;
    consumer
      .pipe(through2.obj({highWaterMark: 101}, function (data, enc, cb) {
        pipeCount++;
        cb(null, data);
      }));
    consumer.on('data', getEventCounter('data', function () {
      should.exist(eventCount.message);
      eventCount.message.should.equal(100);
      eventCount.data.should.equal(100);
      pipeCount.should.equal(100);
      done();
    }));
  });
});
