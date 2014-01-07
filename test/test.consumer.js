'use strict';

var libPath = process.env['KAFKA_COV'] ? '../lib-cov/' : '../lib/',
    Consumer = require(libPath + 'consumer'),
    Producer = require(libPath + 'producer'),
    Offset = require(libPath + 'offset'),
    Client = require(libPath + 'client');

var client, consumer, producer, offset;

function noop() { console.log(arguments) }

function offsetOutOfRange (topic, consumer) {
    topic.maxNum = 2;
    offset.fetch([topic], function (err, offsets) {
        var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
        consumer.setOffset(topic.topic, topic.partition, min);
    });
}

before(function (done) {
    client = new Client();
    producer = new Producer(client);
    offset = new Offset(client);
    producer.on('ready', function () {
        producer.createTopics(['_exist_topic_1_test', '_exist_topic_2_test'], false, function (err, created) {
            producer.send([
                { topic: '_exist_topic_2_test', messages: 'hello kafka' }
            ], function (err) {
                done(err);
            });
        });
    });
});

describe('Consumer', function () {
    
    describe('events', function () {
        it ('should emit message when get new message', function (done) {
            var topics = [ { topic: '_exist_topic_2_test' } ],
                options = { autoCommit: false, groupId: '_groupId_1_test' };
            var consumer = new Consumer(client, topics, options);
            var count = 0;
            consumer.on('error', noop);
            consumer.on('offsetOutOfRange', function (topic) {
                offsetOutOfRange.call(null, topic, this);
            });
            consumer.on('message', function (message) {
                message.topic.should.equal('_exist_topic_2_test'); 
                //message.value.should.equal('hello kafka');
                message.partition.should.equal(0);
                offset.commit('_groupId_1_test', [message], function (err) {
                    if (count++ === 0) done(err); 
                });
            });
        });

        it('should emit error when topic not exists', function (done) {
            var topics = [ { topic: '_not_exist_topic_1_test' } ],
                topicNames = topics.map(function (t) { return t.topic });
            var consumer = new Consumer(client, topics);
            consumer.on('error', function (error) {
                error.should.equal('Topics ' + topicNames.toString() + ' not exists');
                done();
            });
        });

        it('should emit offsetOutOfRange when offset out of range', function (done) {
            var topics = [ { topic: '_exist_topic_1_test', offset: 100 } ],
                options = { fromOffset: true, autoCommit: false }, 
                count = 0;
            var consumer = new Consumer(client, topics, options);
            consumer.on('offsetOutOfRange', function (topic) {
                topic.topic.should.equal('_exist_topic_1_test');
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
                consumer.addTopics(topics, function (err, data) {
                    err.should.equal(1);
                    data.should.eql(topics);
                    done();
                });
            });
        });

        describe('when topic need to added exist', function () {
            it('should added successfully', function (done) {
                var options = { autoCommit: false, groupId: '_groupId_addTopics_test' },
                    topics = ['_exist_topic_2_test'];
                var consumer = new Consumer(client, [], options);
                consumer.on('error', noop);
                consumer.addTopics(topics, function (err, data) {
                    data.should.eql(topics);
                    consumer.payloads.some(function (p) { return p.topic === topics[0] }).should.be.ok;
                    done();
                });
            });
        });
    }); 
    
    describe('#removeTopics', function () {
        it('should remove topics successfully', function (done) {
            var options = { autoCommit: false, groupId: '_groupId_addTopics_test' },
                topics = [{ topic: '_exist_topic_2_test' }, { topic: '_exist_topic_1_test' }];
            var consumer = new Consumer(client, topics, options);
            consumer.on('error', noop);
            
            consumer.payloads.length.should.equal(2);
            consumer.removeTopics('_exist_topic_2_test', function (err, removed) {
                removed.should.equal(1);
                consumer.payloads.length.should.equal(1);
                done(err);
            });
        });
    });

    describe('#setOffset', function () {
        it('should update the offset in consumer', function () {
            var options = { autoCommit: false, groupId: '_groupId_addTopics_test' },
                topics = [{ topic: '_exist_topic_2_test' }];
            var consumer = new Consumer(client, topics, options);
            consumer.on('error', noop);

            consumer.setOffset('_exist_topic_2_test', 0, 100);
            consumer.payloads.filter(function (p) { 
                return p.topic === '_exist_topic_2_test'; 
            })[0].offset.should.equal(100);
        });
    }); 

    describe('#commit', function () {
        it('should commit offset of current topics', function (done) {
            var topics = [ { topic: '_exist_topic_2_test' } ],
                options = { autoCommit: false, groupId: '_groupId_commit_test' };

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
});
