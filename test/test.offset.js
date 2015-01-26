'use strict';

var libPath = process.env['kafka-cov'] ? '../lib-cov/' : '../lib/',
    Producer = require(libPath + 'producer'),
    Offset = require(libPath + 'offset'),
    Client = require(libPath + 'client');

var client, producer, offset;

var host = process.env['KAFKA_TEST_HOST'] || '';

before(function (done) {
    client = new Client(host);
    producer = new Producer(client);
    producer.on('ready', function () {
        producer.createTopics(['_exist_topic_3_test'], false, function (err, created) {
           done(err);
        });
    });

    offset = new Offset(client);
});

describe('Offset', function () {
    describe('#fetch', function () {
        it('should return offset of the topics', function (done) {
            var topic = '_exist_topic_3_test',
                topics = [ { topic: topic } ];
            offset.fetch(topics, function (err, data) {
                var offsets = data[topic][0];
                offsets.should.be.an.instanceOf(Array);
                offsets.length.should.equal(1);
                done(err);
            });
        });

        it('should return earliest offset of the topics', function (done) {
            var topic = '_exist_topic_3_test',
                topics = [ { topic: topic, time: -2 } ];
            offset.fetch(topics, function (err, data) {
                var offsets = data[topic][0];
                offsets.should.be.an.instanceOf(Array);
                offsets.length.should.above(0);
                done(err);
            });
        });

        it('should return latest offset of the topics', function (done) {
            var topic = '_exist_topic_3_test',
                topics = [ { topic: topic, time: -1 } ];
            offset.fetch(topics, function (err, data) {
                var offsets = data[topic][0];
                offsets.should.be.an.instanceOf(Array);
                offsets.length.should.above(0);
                done(err);
            });
        });
    });

    describe('#commit', function () {
        it('should commit successfully', function (done) {
            var topic = '_exist_topic_3_test',
                topics = [ { topic: topic, offset: 10 } ];
            offset.commit('_groupId_commit_test', topics, function (err, data) {
                data.should.be.ok;
                Object.keys(data)[0].should.equal(topic);
                done(err);
            });
        });
    });

    describe('#fetchCommits', function () {
        it('should get last commited offset of the consumer group', function (done) {
            var topic = '_exist_topic_3_test',
                topics = [ { topic: topic, offset: 10 } ];
            offset.fetchCommits('_groupId_commit_1_test', topics, function (err, data) {
                data.should.be.ok;
                Object.keys(data)[0].should.equal(topic);
                data[topic][0].should.equal(-1);
                done(err);
            });
        });
    });
});

