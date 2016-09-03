var should = require('should');

// TODO: Remove - this is for debugging
var through2 = require('through2');

var libPath = process.env['KAFKA_COV'] ? '../lib-cov/' : '../lib/',
    ConsumerStream = require(libPath + 'consumerStream'),
    Producer = require(libPath + 'producer'),
    Offset = require(libPath + 'offset'),
    Client = require(libPath + 'client'),
    TopicsNotExistError = require(libPath + 'errors').TopicsNotExistError;

var client, consumer, producer, offset;

var TOPIC_POSTFIX = '_test_' + Date.now();
var EXISTS_TOPIC_1 = '_exists_1' + TOPIC_POSTFIX;
var EXISTS_TOPIC_2 = '_exists_2' + TOPIC_POSTFIX;
// var EXISTS_GZIP = '_exists_gzip'; // + TOPIC_POSTFIX;
// var EXISTS_SNAPPY = '_exists_snappy'; // + TOPIC_POSTFIX;

// Compression-friendly to be interesting
var SNAPPY_MESSAGE = new Array(20).join('snappy');

var host = process.env['KAFKA_TEST_HOST'] || '';
function noop() { console.log(arguments) }

function offsetOutOfRange (topic, consumer) {
    topic.maxNum = 2;
    offset.fetch([topic], function (err, offsets) {
        var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
        consumer.setOffset(topic.topic, topic.partition, min);
    });
}

before(function (done) {
    client = new Client(host);
    producer = new Producer(client);
    offset = new Offset(client);
    producer.on('ready', function () {
        producer.createTopics([
            EXISTS_TOPIC_1,
            EXISTS_TOPIC_2,
//          EXISTS_GZIP,
//          EXISTS_SNAPPY
        ], false, function (err, created) {
            if (err) return done(err);

            function useNewTopics() {
                var messages = [];
                var i = 0;
                for (var i = 1 ; i <= 100 ; i++) {
                  messages.push('stream message ' + i);
                }
                producer.send([{ topic: EXISTS_TOPIC_1, messages }], done);
            }
            // Ensure leader selection happened
            setTimeout(useNewTopics, 400);
        });
    });
});

after(function (done) {
  setTimeout(function() {
    consumer.close(done);
  }, 2000);
});



describe.only('ConsumerStream', function () {
    it('should emit message when get new message', function (done) {
        var topics = [ { topic: EXISTS_TOPIC_1 } ],
            options = { autoCommit: false, groupId: '_groupId_1_test' };
        consumer = new ConsumerStream(client, topics, options);
        var eventCount = {
            message: 0,
            data: 0,
        }
        var getEventCounter = function(name, done) {
            return function() {
                eventCount[name]++;
                if (eventCount[name] == 100 && done) {
                    return done();
                }
            };
        };
        consumer.on('message', getEventCounter('message'));
        consumer.on('data', getEventCounter('data', function() {
            eventCount.message.should.equal(100);
            eventCount.data.should.equal(100);
            done();
        }));
    });
});
