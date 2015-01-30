'use strict';

var Producer = require('../lib/producer'),
    Client = require('../lib/client');

var client, producer;

var TOPIC_POSTFIX = '_test_' + Date.now();
var EXISTS_TOPIC_3 = '_exists_3' + TOPIC_POSTFIX;

var host = process.env['KAFKA_TEST_HOST'] || '';

// Helper method
function randomId () {
    return Math.floor(Math.random() * 10000)
}

before(function (done) {
    client = new Client(host);
    producer = new Producer(client);
    producer.on('ready', function () {
        producer.createTopics([EXISTS_TOPIC_3], false, function (err, created) {
            if(err) return done(err);
            setTimeout(done, 500);
        });
    });
});

describe('Producer', function () {
    describe('#send', function () {
        before(function(done) {
            // Ensure that first message gets the `0`
            producer.send([{ topic: EXISTS_TOPIC_3, messages: '_initial' }], function (err, message) {
                message.should.be.ok;
                message[EXISTS_TOPIC_3].should.have.property('0', 0);
                done(err);
            });
        });

        it('should send message successfully', function (done) {
            producer.send([{ topic: EXISTS_TOPIC_3, messages: 'hello kafka' }], function (err, message) {
                message.should.be.ok;
                message[EXISTS_TOPIC_3]['0'].should.be.above(0);
                done(err);
            });
        });

        it('should send buffer message successfully', function (done) {
            var message = new Buffer('hello kafka');
            producer.send([{ topic: EXISTS_TOPIC_3, messages: message }], function (err, message) {
                message.should.be.ok;
                message[EXISTS_TOPIC_3]['0'].should.be.above(0);
                done(err);
            });
        });

        it('should support multi messages in one topic', function (done) {
            producer.send([{ topic: EXISTS_TOPIC_3, messages: ['hello kafka', 'hello kafka'] }], function (err, message) {
                message.should.be.ok;
                message[EXISTS_TOPIC_3]['0'].should.be.above(0);
                done(err);
            });
        });

        it('should support snappy compression', function (done) {
            producer.send([{
                topic: EXISTS_TOPIC_3,
                messages: ['hello kafka', 'hello kafka'],
                attributes: 2
            }], function (err, message) {
                if (err) return done(err);
                message.should.be.ok;
                message[EXISTS_TOPIC_3]['0'].should.be.above(0);
                done();
            });
        });

        it('should support gzip compression', function (done) {
            producer.send([{
                topic: EXISTS_TOPIC_3,
                messages: ['hello kafka', 'hello kafka'],
                attributes: 1
            }], function (err, message) {
                if (err) return done(err);
                message.should.be.ok;
                message[EXISTS_TOPIC_3]['0'].should.be.above(0);
                done();
            });
        });
    });

    describe('#createTopics', function () {
        it('should return All requests sent when async is true', function (done) {
            producer.createTopics(['_exist_topic_'+ randomId() +'_test'], true, function (err, data) {
                data.should.equal('All requests sent');
                done(err);
            });
        });

        it('async should be true if not present', function (done) {
            producer.createTopics(['_exist_topic_'+ randomId() +'_test'], function (err, data) {
                data.should.equal('All requests sent');
                done(err);
            });
        });

        it('should return All created when async is false', function (done) {
            producer.createTopics(['_exist_topic_'+ randomId() +'_test'], false, function (err, data) {
                data.should.equal('All created');
                done(err);
            });
        });
    });

    describe('#close', function () {
        it('should close successfully', function (done) {
            producer.close(done);
        });
    });
});
