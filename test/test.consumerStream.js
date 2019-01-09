'use strict';
var should = require('should');

var through2 = require('through2');

var libPath = process.env['KAFKA_COV'] ? '../lib-cov/' : '../lib/';
var ConsumerStream = require(libPath + 'consumerStream');
var Producer = require(libPath + 'producer');
var Client = require(libPath + 'kafkaClient');
var EventCounter = require('./helpers/EventCounter');

const TOPIC_POSTFIX = '_test_' + Date.now();
const EXISTS_TOPIC_1 = '_exists_1' + TOPIC_POSTFIX;
const APPEND_TOPIC_1 = '_append_1' + TOPIC_POSTFIX;
const COMMIT_STREAM_TOPIC_1 = '_commit_stream_1' + TOPIC_POSTFIX;
const COMMIT_STREAM_TOPIC_2 = '_commit_stream_2' + TOPIC_POSTFIX;
const COMMIT_STREAM_TOPIC_3 = '_commit_stream_3' + TOPIC_POSTFIX;

function createTopicAndProduceMessages (producer, topic, numberOfMessages, done) {
  if (!done) {
    done = function () {};
  }
  producer.createTopics([topic], function () {
    var messages = [];
    for (var i = 1; i <= numberOfMessages; i++) {
      messages.push('stream message ' + i);
    }
    producer.send([{ topic: topic, messages }], done);
  });
}

describe('ConsumerStream', function () {
  it("should emit both a 'message' and a 'data' event for each message", function (done) {
    var client = new Client();
    var producer = new Producer(client);
    producer.once('ready', function () {
      createTopicAndProduceMessages(producer, EXISTS_TOPIC_1, 100, function () {
        var topics = [{ topic: EXISTS_TOPIC_1 }];
        // Here we set fetchMaxBytes to ensure that we're testing running
        // multiple fetches as the default 1024 * 1024 makes a single fetch.
        var options = { autoCommit: false, groupId: '_groupId_1_test', fetchMaxBytes: 512 };
        var consumer = new ConsumerStream(client, topics, options);
        var eventCounter = new EventCounter();
        consumer.on('message', eventCounter.createEventCounter('message'));
        consumer.on('data', eventCounter.createEventCounter('data'));
        var incrementPipeCount = eventCounter.createEventCounter('pipe', 100, function () {
          should.exist(eventCounter.events.message.events);
          var events = eventCounter.events;
          should.exist(events.message);
          events.message.events.length.should.equal(100);
          events.data.events.length.should.equal(100);
          events.pipe.events.length.should.equal(100);
          consumer.close(done);
        });
        consumer.pipe(
          through2.obj({ highWaterMark: 8 }, function (data, enc, cb) {
            incrementPipeCount(data);
            cb(null);
          })
        );
      });
    });
  });
  it('should continue polling for new messages appended after consuming all existing messages', function (done) {
    var client = new Client();
    var producer = new Producer(client);
    producer.once('ready', function () {
      createTopicAndProduceMessages(producer, APPEND_TOPIC_1, 20, function () {
        var topics = [{ topic: APPEND_TOPIC_1 }];
        var options = { autoCommit: false, groupId: '_groupId_2_test' };
        var consumer = new ConsumerStream(client, topics, options);
        var eventCounter = new EventCounter();
        var increment1 = eventCounter.createEventCounter('first', 20, function (error, firstEvents) {
          should.not.exist(error);
          firstEvents.events.length.should.equal(20);
          firstEvents.events[0][0].value.should.equal('stream message 1');
          firstEvents.events[0][0].offset.should.equal(0);
          firstEvents.events[19][0].value.should.equal('stream message 20');
          firstEvents.events[19][0].offset.should.equal(19);
          var increment2 = eventCounter.createEventCounter('second', 20, function (error, secondEvents) {
            should.not.exist(error);
            secondEvents.count.should.equal(20);
            secondEvents.events[0][0].value.should.equal('stream message 1');
            secondEvents.events[0][0].offset.should.equal(20);
            secondEvents.events[19][0].value.should.equal('stream message 20');
            secondEvents.events[19][0].offset.should.equal(39);
            consumer.close(done);
          });
          consumer.on('data', increment2);
          createTopicAndProduceMessages(producer, APPEND_TOPIC_1, 20);
        });
        consumer.pipe(
          through2.obj(function (data, enc, cb) {
            increment1(data);
            cb(null);
          })
        );
      });
    });
  });
  describe('CommitStream', function () {
    it('should instantiate a consumer stream and increment commit manually', function (done) {
      const groupId = '_commitStream_1_test';
      const topic = COMMIT_STREAM_TOPIC_1;
      var client = new Client();
      var producer = new Producer(client);
      producer.once('ready', function () {
        createTopicAndProduceMessages(producer, topic, 20, function () {
          var options = { autoCommit: false, groupId };
          var consumer = new ConsumerStream(client, [topic], options);
          var eventCounter = new EventCounter();
          let commitStream = consumer.createCommitStream({});
          var increment = eventCounter.createEventCounter('first', 20, function (error, events) {
            if (error) {
              throw error;
            }
            setImmediate(function () {
              commitStream.commit(function () {
                client.sendOffsetFetchRequest(groupId, commitStream.topicPartionOffsets, function (error, data) {
                  if (error) {
                    throw error;
                  }
                  data[topic][0].should.equal(20);
                  consumer.close(done);
                });
              });
            });
          });
          consumer
            .pipe(
              through2.obj(function (data, enc, cb) {
                increment();
                cb(null, data);
              })
            )
            .pipe(commitStream);
        });
      });
    });
    xit('should commit when the autocommit message count is reached', function (done) {
      const groupId = '_commitStream_2_test';
      const topic = COMMIT_STREAM_TOPIC_2;
      var client = new Client();
      var producer = new Producer(client);
      producer.once('ready', function () {
        createTopicAndProduceMessages(producer, topic, 20, function () {
          var options = { autoCommit: true, autoCommitIntervalMs: false, autoCommitMsgCount: 18, groupId };
          var consumer = new ConsumerStream(client, [topic], options);
          let commitStream = consumer.createCommitStream();
          commitStream.once('commitComplete', function (data) {
            client.sendOffsetFetchRequest(groupId, commitStream.topicPartionOffsets, function (error, data) {
              if (error) {
                throw error;
              }
              data[topic][0].should.equal(18);
              consumer.close(done);
            });
          });
          consumer
            .pipe(
              through2.obj(function (data, enc, cb) {
                cb(null, data);
              })
            )
            .pipe(commitStream);
        });
      });
    });
    xit('should autocommit after a given interval in milliseconds', function (done) {
      const groupId = '_commitStream_3_test';
      const topic = COMMIT_STREAM_TOPIC_3;
      var client = new Client();
      var producer = new Producer(client);
      producer.once('ready', function () {
        createTopicAndProduceMessages(producer, topic, 20, function () {
          var options = { autoCommit: true, autoCommitIntervalMs: 5, groupId };
          var consumer = new ConsumerStream(client, [topic], options);
          let commitStream = consumer.createCommitStream();
          commitStream.once('commitComplete', function (data) {
            client.sendOffsetFetchRequest(groupId, commitStream.topicPartionOffsets, function (error, data) {
              if (error) {
                throw error;
              }
              data[topic][0].should.equal(20);
              commitStream.clearInterval();
              consumer.close(done);
            });
          });
          consumer
            .pipe(
              through2.obj(function (data, enc, cb) {
                cb(null, data);
              })
            )
            .pipe(commitStream);
        });
      });
    });
  });
});
