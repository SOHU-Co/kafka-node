'use strict';

var Producer = require('../lib/producer'),
    Client = require('../lib/client');

var client, producer;

// Helper method
function randomId () {
    return Math.floor(Math.random() * 10000)
}

before(function (done) {
    client = new Client();
    producer = new Producer(client);
    producer.on('ready', function () {
        producer.createTopics(['_exist_topic_3_test'], false, function (err, created) {
           done(); 
        });
    });
});

describe('Producer', function () {
    describe('#send', function () {
        it('should send message successfully', function (done) {
            producer.send([{ topic: '_exist_topic_3_test', messages: 'hello kafka' }], function (err, message) {
                message.should.be.ok;
                done(err);
            }); 
        });

        it('should support multi messages in one topic', function (done) {
            producer.send([{ topic: '_exist_topic_3_test', messages: ['hello kafka', 'hello kafka'] }], function (err, message) {
                message.should.be.ok;
                done(err);
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
});
