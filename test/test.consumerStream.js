'use strict';
var should = require('should');

var through2 = require('through2');

var libPath = process.env['KAFKA_COV'] ? '../lib-cov/' : '../lib/';
var ConsumerStream = require(libPath + 'consumerStream');
var Producer = require(libPath + 'producer');
var Client = require(libPath + 'client');
var EventCounter = require('./helpers/EventCounter');

var TOPIC_POSTFIX = '_test_' + Date.now();
var EXISTS_TOPIC_1 = '_exists_1' + TOPIC_POSTFIX;
var APPEND_TOPIC_1 = '_append_1' + TOPIC_POSTFIX;

var host = process.env['KAFKA_TEST_HOST'] || '';

function createTopicAndProduceMessages(producer, topic, numberOfMessages, done) {
  if (!done) {
    done = function() {};
  }
  function topicsReady (err, created) {
    if (err) return done(err);
    setTimeout(useNewTopics, 400);
  };
  producer.createTopics([topic], function() {
      var messages = [];
      for (var i = 1; i <= numberOfMessages; i++) {
        messages.push('stream message ' + i);
      }
      producer.send([{ topic: topic, messages }], done);
  });
}



describe('ConsumerStream', function () {
  it('should emit both a \'message\' and a \'data\' event for each message', function (done) {
    var client = new Client(host);
    var producer = new Producer(client);
    producer.on('ready', function () {
      createTopicAndProduceMessages(producer, EXISTS_TOPIC_1, 100, function() {
        var topics = [ { topic: EXISTS_TOPIC_1 } ];
        // Here we set fetchMaxBytes to ensure that we're testing running
        // multiple fetches as the default 1024 * 1024 makes a single fetch.
        var options = { autoCommit: false, groupId: '_groupId_1_test', fetchMaxBytes: 512 };
        var consumer = new ConsumerStream(client, topics, options);
        var eventCounter = new EventCounter();
        consumer.on('message', eventCounter.createEventCounter('message'));
        consumer.on('data', eventCounter.createEventCounter('data'));
        var incrementPipeCount = eventCounter.createEventCounter('pipe', 100, function() {
          should.exist(eventCounter.events.message.events);
          var events = eventCounter.events;
          should.exist(events.message);
          events.message.events.length.should.equal(100);
          events.data.events.length.should.equal(100);
          events.pipe.events.length.should.equal(100);
          consumer.close(done);
        });
        consumer
          .pipe(through2.obj({highWaterMark: 8}, function (data, enc, cb) {
            incrementPipeCount(data);
            cb(null);
          }));
      });
    });
  });
  it('should continue polling for messages after consuming all existing messages', function (done) {
    var client = new Client(host);
    var producer = new Producer(client);
    producer.on('ready', function () {
      createTopicAndProduceMessages(producer, APPEND_TOPIC_1, 20, function() {
        var topics = [ { topic: APPEND_TOPIC_1 } ];
        var options = { autoCommit: false, groupId: '_groupId_2_test' };
        var consumer = new ConsumerStream(client, topics, options);
        var pipeCount = 0;
        var eventCounter = new EventCounter();
        var increment = eventCounter.createEventCounter('first', 20, function(error, events) {
          events.events.length.should.equal(20);
          events.events[0][0].value.should.equal('stream message 1');
          events.events[0][0].offset.should.equal(0);
          events.events[19][0].value.should.equal('stream message 20');
          events.events[19][0].offset.should.equal(19);
          var increment = eventCounter.createEventCounter('second', 20, function(error, events) {
            events.events.length.should.equal(20);
            events.events[0][0].value.should.equal('stream message 1');
            events.events[0][0].offset.should.equal(20);
            events.events[19][0].value.should.equal('stream message 20');
            events.events[19][0].offset.should.equal(39);
            consumer.close(done);
          });
          consumer.on('data', increment);
          createTopicAndProduceMessages(producer, APPEND_TOPIC_1, 20);
        });
        consumer
          .pipe(through2.obj(function (data, enc, cb) {
            increment(data);
            cb(null);
          }));
      });
    });
  });

});
