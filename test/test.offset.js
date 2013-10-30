'use strict';

var Offset = require('../lib/offset'),
    Producer = require('../lib/producer'),
    Client = require('../lib/client');

var client, producer, offset;

before(function (done) {
    client = new Client();
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
});

