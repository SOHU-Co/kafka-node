'use strict';

var libPath = process.env['KAFKA_COV'] ? '../lib-cov/' : '../lib/',
    Consumer = require(libPath + 'consumer'),
    Producer = require(libPath + 'producer'),
    Offset = require(libPath + 'offset'),
    Client = require(libPath + 'client'),
    TopicsNotExistError = require(libPath + 'errors').TopicsNotExistError;

var client, consumer, producer, offset;

var TOPIC_POSTFIX = '_test_' + Date.now();
var EXISTS_TOPIC_1 = '_exists_1' + TOPIC_POSTFIX;
var EXISTS_TOPIC_2 = '_exists_2' + TOPIC_POSTFIX;
var EXISTS_GZIP = '_exists_gzip'; // + TOPIC_POSTFIX;
var EXISTS_SNAPPY = '_exists_snappy'; // + TOPIC_POSTFIX;

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

function createClient() {
    return new Client(host);
}

before(function (done) {
    client = createClient();
    producer = new Producer(client);
    offset = new Offset(client);
    producer.on('ready', function () {
        producer.createTopics([
            EXISTS_TOPIC_1,
            EXISTS_TOPIC_2,
            EXISTS_GZIP,
            EXISTS_SNAPPY
        ], false, function (err, created) {
            if (err) return done(err);

            function useNewTopics() {
                producer.send([
                    { topic: EXISTS_TOPIC_2, messages: 'hello kafka' },
                    { topic: EXISTS_GZIP, messages: 'hello gzip', attributes: 1 },
                    { topic: EXISTS_SNAPPY, messages: SNAPPY_MESSAGE, attributes: 2 }
                ], done);
            }
            // Ensure leader selection happened
            setTimeout(useNewTopics, 1000);
        });
    });
});

describe('Consumer', function () {

    describe('events', function () {
        it('should emit message when get new message', function (done) {
            var topics = [ { topic: EXISTS_TOPIC_2 } ],
                options = { autoCommit: false, groupId: '_groupId_1_test' };
            var consumer = new Consumer(client, topics, options);
            var count = 0;
            consumer.on('error', noop);
            consumer.on('offsetOutOfRange', function (topic) {
                offsetOutOfRange.call(null, topic, this);
            });
            consumer.on('message', function (message) {
                message.topic.should.equal(EXISTS_TOPIC_2);
                message.value.should.equal('hello kafka');
                message.partition.should.equal(0);
                offset.commit('_groupId_1_test', [message], function (err) {
                    if (count++ === 0) done(err);
                });
            });
        });

        it('should decode gzip messages', function (done) {
            var topics = [ { topic: EXISTS_GZIP } ],
                options = { autoCommit: false, groupId: '_groupId_gzip_test' };
            var consumer = new Consumer(createClient(), topics, options);
            var count = 0;
            consumer.on('error', noop);
            consumer.on('offsetOutOfRange', function (topic) {
                offsetOutOfRange.call(null, topic, this);
            });
            consumer.on('message', function (message) {
                message.topic.should.equal(EXISTS_GZIP);
                message.value.should.equal('hello gzip');
                offset.commit('_groupId_gzip_test', [message], function (err) {
                    if (count++ === 0) done(err);
                });
            });
        });

        it('should decode snappy messages', function (done) {
            var topics = [ { topic: EXISTS_SNAPPY } ],
                options = { autoCommit: false, groupId: '_groupId_snappy_test' };
            var consumer = new Consumer(createClient(), topics, options);
            var count = 0;
            consumer.on('error', noop);
            consumer.on('offsetOutOfRange', function (topic) {
                offsetOutOfRange.call(null, topic, this);
            });
            consumer.once('message', function (message) {
                message.topic.should.equal(EXISTS_SNAPPY);
                message.value.should.equal(SNAPPY_MESSAGE);
                offset.commit('_groupId_snappy_test', [message], function (err) {
                    if (count++ === 0) done(err);
                });
            });
        });

        it('should emit error when topic not exists', function (done) {
            var topics = [ { topic: '_not_exist_topic_1_test' } ],
                topicNames = topics.map(function (t) { return t.topic });
            var consumer = new Consumer(client, topics);
            consumer.on('error', function (error) {
                error.should.be.an.instanceOf(TopicsNotExistError)
                    .and.have.property('message', 'The topic(s) _not_exist_topic_1_test do not exist');
                done();
            });
        });

        it('should emit offsetOutOfRange when offset out of range', function (done) {
            var topics = [ { topic: EXISTS_TOPIC_1, offset: 100 } ],
                options = { fromOffset: true, autoCommit: false },
                count = 0;

            var client = new Client(host);
            var consumer = new Consumer(client, topics, options);
            consumer.on('offsetOutOfRange', function (topic) {
                topic.topic.should.equal(EXISTS_TOPIC_1);
                topic.partition.should.equal(0);
                topic.message.should.equal('OffsetOutOfRange');
                if (count++ === 0) done();
            });
            consumer.on('error', noop);
        });
    });

    describe('#addTopics', function () {
        describe('when topic need to added not exist', function () {
            it('should return error', function (done) {
                var options = { autoCommit: false, groupId: '_groupId_1_test' },
                    topics = ['_not_exist_topic_1_test'];
                var consumer = new Consumer(client, [], options);
                consumer.on('error', noop);
                consumer.addTopics(topics, function (err) {
                    err.topics.length.should.equal(1);
                    err.topics.should.eql(topics);
                    done();
                });
            });

            it('should return error when using payload as well', function (done) {
                var options = { autoCommit: false, groupId: '_groupId_1_test' },
                    topics = [{topic: '_not_exist_topic_1_test', offset: 42}];
                var consumer = new Consumer(client, [], options);
                consumer.on('error', noop);
                consumer.addTopics(topics, function (err) {
                    err.topics.length.should.equal(1);
                    err.topics.should.eql(topics.map(function(p) {return p.topic;}));
                    done();
                }, true);
            });
        });

        describe('when topic need to added exist', function () {
            it('should added successfully', function (done) {
                var options = { autoCommit: false, groupId: '_groupId_addTopics_test' },
                    topics = [EXISTS_TOPIC_2];
                var consumer = new Consumer(client, [], options);
                consumer.on('error', noop);
                consumer.addTopics(topics, function (err, data) {
                    data.should.eql(topics);
                    consumer.payloads.some(function (p) { return p.topic === topics[0] }).should.be.ok;
                    done();
                });
            });

            it('should add with given offset', function (done) {
                var options = { autoCommit: false, groupId: '_groupId_addTopics_test' },
                    topics = [{topic: EXISTS_TOPIC_2, offset: 42}];
                var consumer = new Consumer(client, [], options);
                consumer.on('error', noop);
                consumer.addTopics(topics, function (err, data) {
                    data.should.eql(topics.map(function(p) {return p.topic;}));
                    consumer.payloads.some(function (p) { return p.topic === topics[0].topic && p.offset === topics[0].offset; }).should.be.ok;
                    done();
                }, true);
            });
        });
    });

    describe('#removeTopics', function () {
        it('should remove topics successfully', function (done) {
            var options = { autoCommit: false, groupId: '_groupId_addTopics_test' },
                topics = [{ topic: EXISTS_TOPIC_2 }, { topic: EXISTS_TOPIC_1 }];
            var consumer = new Consumer(client, topics, options);
            consumer.on('error', noop);

            consumer.payloads.length.should.equal(2);
            consumer.removeTopics(EXISTS_TOPIC_2, function (err, removed) {
                removed.should.equal(1);
                consumer.payloads.length.should.equal(1);
                done(err);
            });
        });
    });

    describe('#setOffset', function () {
        it('should update the offset in consumer', function () {
            var options = { autoCommit: false, groupId: '_groupId_addTopics_test' },
                topics = [{ topic: EXISTS_TOPIC_2 }];
            var consumer = new Consumer(client, topics, options);
            consumer.on('error', noop);

            consumer.setOffset(EXISTS_TOPIC_2, 0, 100);
            consumer.payloads.filter(function (p) {
                return p.topic === EXISTS_TOPIC_2;
            })[0].offset.should.equal(100);
        });
    });

    describe('#commit', function () {
        it('should commit offset of current topics', function (done) {
            var topics = [ { topic: EXISTS_TOPIC_2 } ],
                options = { autoCommit: false, groupId: '_groupId_commit_test' };

            var client = new Client(host);
            var consumer = new Consumer(client, topics, options);
            var count = 0;
            consumer.on('error', noop);
            consumer.on('offsetOutOfRange', function (topic) {
                offsetOutOfRange.call(null, topic, this);
            });
            consumer.on('message', function (message) {
                consumer.commit(true, function (err) {
                    if (!err && count++ === 0) done(err);
                });
            });

        });
    });

    describe('#buildPayloads', function () {
        it('should set default config on the topic', function () {
            var defaults = {
                groupId: 'kafka-node-group',
                autoCommit: true,
                autoCommitMsgCount: 100,
                autoCommitIntervalMs: 5000,
                encoding: 'utf8',
                fetchMaxWaitMs: 100,
                fetchMinBytes: 1,
                fetchMaxBytes: 1024 * 1024,
                fromOffset: false
            };

            var consumer = new Consumer(client, []);
            consumer.options.should.eql(defaults);

            var topic = consumer.buildPayloads([{ topic: 'topic' }])[0];
            topic.partition.should.equal(0);
            topic.offset.should.equal(0);
            topic.metadata.should.equal('m');
            topic.maxBytes.should.equal(1024 * 1024);
        });

        it('should support custom options', function () {
            var options = {
                groupId: 'custom-group',
                autoCommit: false,
                autoCommitIntervalMs: 1000,
                fetchMaxWaitMs: 200,
                fetchMinBytes: 1,
                fetchMaxBytes: 1024,
                fromOffset: false
            };
            var consumer = new Consumer(client, [], options);
            consumer.options.should.equal(options);
            var topic = consumer.buildPayloads([{ topic: 'topic' }])[0];
            topic.partition.should.equal(0);
            topic.offset.should.equal(0);
            topic.metadata.should.equal('m');
            topic.maxBytes.should.equal(1024);
        });

        it('should return right payloads when arguments is string', function () {
            var consumer = new Consumer(client, []);
            var topics = ['topic'];
            var topic = consumer.buildPayloads(topics)[0];
            topic.should.be.an.instanceof(Object);
            topic.partition.should.equal(0);
            topic.offset.should.equal(0);
            topic.metadata.should.equal('m');
            topic.maxBytes.should.equal(1024 * 1024);
        });
    });

    describe('#close', function () {
        it('should close the consumer', function (done) {
            var client = new Client(host),
                topics = [ { topic: EXISTS_TOPIC_2 } ],
                options = { autoCommit: false, groupId: '_groupId_close_test' };

            var consumer = new Consumer(client, topics, options);
            consumer.once('message', function (message) {
                consumer.close(function (err) {
                    done(err);
                });
            });
        });

        it('should commit the offset if force', function (done) {
            var client = new Client(host),
                topics = [ { topic: EXISTS_TOPIC_2 } ],
                force = true,
                options = { autoCommit: false, groupId: '_groupId_close_test' };

            var consumer = new Consumer(client, topics, options);
            consumer.once('message', function (message) {
                consumer.close(force, function (err) {
                    done(err);
                });
            });
        });
    });

    describe('#pauseTopics|resumeTopics', function () {
        it('should pause or resume the topics', function (done) {
            var client = new Client(host);
            var topics = [
                {topic: EXISTS_TOPIC_1, partition: 0},
                {topic: EXISTS_TOPIC_1, partition: 1},
                {topic: EXISTS_TOPIC_2, partition: 0},
                {topic: EXISTS_TOPIC_2, partition: 1}
            ];
            var consumer = new Consumer(client, topics, {});
            consumer.on('error', function () {});

            function normalize (p) {
                return { topic: p.topic, partition: p.partition };
            }
            function compare (p1, p2) {
                return p1.topic === p2.topic
                    ? p1.partition - p2.partition
                    : p1.topic > p2.topic;
            }

            consumer.payloads.should.eql(topics);
            consumer.pauseTopics([EXISTS_TOPIC_1, { topic: EXISTS_TOPIC_2, partition: 0 }]);
            consumer.payloads.map(normalize).should.eql([{ topic: EXISTS_TOPIC_2, partition: 1 }]);

            consumer.resumeTopics([{topic: EXISTS_TOPIC_1, partition: 0}]);
            consumer.payloads.map(normalize).sort(compare).should.eql([
                {topic: EXISTS_TOPIC_1, partition: 0},
                {topic: EXISTS_TOPIC_2, partition: 1}
            ]);
            consumer.pausedPayloads.map(normalize).sort(compare).should.eql([
                {topic: EXISTS_TOPIC_1, partition: 1},
                {topic: EXISTS_TOPIC_2, partition: 0}
            ]);

            consumer.resumeTopics([EXISTS_TOPIC_1, EXISTS_TOPIC_2]);
            consumer.payloads.sort(compare).should.eql(topics);
            consumer.pausedPayloads.should.eql([]);
            consumer.once('message', function () {
                consumer.close(true, done);
            });
        });
    });
});
