'use strict';

var Producer = require('../lib/producer'),
    Client = require('../lib/client'),
    Consumer = require('../lib/consumer'),
    liberrors =  require('../lib/errors'),
    config = require('./config');

var client, producer;

// Helper method
function randomId() {
    return Math.floor(Math.random() * 10000)
}

describe('Default Producer', function () {
    before(function (done) {
        client = new Client(config.zoo);
        producer = new Producer(client, { partitionerType: 0 });
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

            it('should send buffer message successfully', function (done) {
                var message = new Buffer('hello kafka');
                producer.send([{ topic: '_exist_topic_3_test', messages: message }], function (err, message) {
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
                producer.createTopics(['_exist_topic_' + randomId() + '_test'], true, function (err, data) {
                    data.should.equal('All requests sent');
                    done(err);
                });
            });

            it('async should be true if not present', function (done) {
                producer.createTopics(['_exist_topic_' + randomId() + '_test'], function (err, data) {
                    data.should.equal('All requests sent');
                    done(err);
                });
            });

            it('should return All created when async is false', function (done) {
                producer.createTopics(['_exist_topic_' + randomId() + '_test'], false, function (err, data) {
                    data.should.equal('All created');
                    done(err);
                });
            });
        });
    });
});

/*
 *  To run the test, you should ensure that topic _exist_topic_partitioned is created with 2 partitions:
 */
var topic;
describe('Partitioned Producer', function () {
    before(function (done) {
        topic = '_exist_topic_partitioned';
        client = new Client(config.zoo, null, { lazyBrokerConnection: false});
        producer = new Producer(client, { partitionerType: 2 });
        producer.on('ready', function () {
            producer.createTopics([topic], false, function (err, created) {
                done(err);
            });
        });
    });
    describe('Producer', function () {
        describe('#send', function () {
            this.timeout(5000);
            it('should support partitioning messages according to their key', function (done) {
                var msgs = [
                    { key: '0_key', topic: topic, messages: ['message_partition_0'] },
                    { key: '1_key', topic: topic, messages: ['message_partition_1','message_partition_1'] }
                ]
                producer.send(msgs, function (err, message) {
                    if (err) {
                        return done(err);
                    }

                    message.should.be.ok;
                    var offsetPartition0 = message[topic]['0'];
                    var offsetPartition1 = message[topic]['1'];
                    var consumer = new Consumer(client, [{ topic: topic, partition: 0, offset: offsetPartition0 }, { topic: topic, partition: 1, offset: offsetPartition1 }], { autoCommit: false, fromOffset: true });                    
                    var i = 0;
                    var messageCount = 3; // Since we are sending 1 message to partition 0 and two too partition 1

                    consumer.on('message', function (message, err) {
                        i++;
                        message.value.should.equal('message_partition_' + message.partition);
                        if (i == messageCount) {
                            done();
                        }
                    });
                });
            });
        });
    });
});

describe('Producer Errors', function () {
    before(function (done) {
        client = new Client(config.zoo);
        producer = new Producer(client, { requireAcks: 20, ackTimeoutMs:100 });
        producer.on('ready', function () {
            producer.createTopics(['_error_topic'], false, function (err, created) {
                done();
            });
        });
    });
    describe('Producer', function () {
        describe('#send', function () {
            it('should get an error with code 7 when producing requests with a requireAcks bigger than the total number of brokers.', function (done) {
                var msgs = [
                    { topic: '_error_topic', messages: ['message'], partitions:0 },
                ]
                producer.send(msgs, function (err, message) {
                    err.should.be.ok;
                    err.should.be.an.instanceof(liberrors.TopicsPartitionsError);
                    err.topics._error_topic.partitions['0'].errorCode.should.equal(7);
                    done();
                });
            });
        });
    });
});
