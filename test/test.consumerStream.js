var should = require('should');

var through2 = require('through2');

var libPath = process.env['KAFKA_COV'] ? '../lib-cov/' : '../lib/';
var ConsumerStream = require(libPath + 'consumerStream');
var Producer = require(libPath + 'producer');
var Client = require(libPath + 'client');

var client, consumer, producer;

var TOPIC_POSTFIX = '_test_' + Date.now();
var EXISTS_TOPIC_1 = '_exists_1' + TOPIC_POSTFIX;
var APPEND_TOPIC_1 = '_append_1' + TOPIC_POSTFIX;

var host = process.env['KAFKA_TEST_HOST'] || '';

var writeEvents = function (done) {
  client = new Client(host);
  producer = new Producer(client);
  producer.on('ready', function () {
    var topicsReadyHandler = function (err, created) {
      if (err) return done(err);
      function useNewTopics () {
        var writeStuff = function (topic, numberOfMessages, cb) {
          var messages = [];
          for (var i = 1; i <= numberOfMessages; i++) {
            messages.push('stream message ' + i);
          }
          //console.log(messages);
          producer.send([{ topic: topic, messages }], cb);
        };
        writeStuff(EXISTS_TOPIC_1, 100, function() {
          writeStuff(APPEND_TOPIC_1, 20, done);
        });
      }
      // Ensure leader selection happened
      setTimeout(useNewTopics, 400);
    };
    producer.createTopics([
      EXISTS_TOPIC_1,
      APPEND_TOPIC_1
    ], false, topicsReadyHandler);
  });
};

before(writeEvents);

describe('ConsumerStream', function () {
  it.only('should continue polling for messages after consuming all messages', function (done) {
    console.log('starting test 2');
    var topics = [ { topic: EXISTS_TOPIC_1 } ];
    var options = { autoCommit: false, groupId: '_groupId_2_test' };
    consumer = new ConsumerStream(client, topics, options);
    var eventCount = {
      message: 0,
      data: 0,
      pipe: 0
    };
    var getEventCounter = function (name, number, done) {
      if (typeof number === 'function') {
        done = number;
        number = 100;
      }
      return function () {
        //console.log(arguments);
        eventCount[name]++;
        if (eventCount[name] === number && done) {
          return consumer.close(done);
        }
      };
    };
    consumer.on('data', getEventCounter('pipe'));
    //consumer.on('message', console.log);
    var pipeCount = 0;
    console.log('piping consumer');
    consumer
      .pipe(through2.obj({highWaterMark: 101}, function (data, enc, cb) {
        //console.log(data);
        pipeCount++;
        cb(null);
      }))
    consumer.on('data', getEventCounter('data', 20, function () {
      /*
      eventCount.message.should.equal(20);
      eventCount.data.should.equal(20);
      */
      pipeCount.should.equal(20);
      var count = 0;
      var interval, timeout;
      consumer.pipe(through2.obj(function (data, enc, cb) {
        count++;
        cb(null);
        // console.log('count', count);
        if (count === 17) {
          consumer.close(done);
        }
      }))
      writeEvents(function () {});
    }));
  });
  it('should emit both a \'message\' and a \'data\' event for each message', function (done) {
    var topics = [ { topic: EXISTS_TOPIC_1 } ];
    var options = { autoCommit: false, groupId: '_groupId_1_test', fetchMaxBytes: 1024 };
    consumer = new ConsumerStream(client, topics, options);
    var eventCount = {
      message: 0,
      data: 0,
      pipe: 0
    };
    var getEventCounter = function (name, number, done) {
      if (typeof number === 'function') {
        done = number;
        number = 100;
      }
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
      .pipe(through2.obj({highWaterMark: 8}, function (data, enc, cb) {
        pipeCount++;
        cb(null);
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
