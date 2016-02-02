'use strict';

var sinon = require('sinon'),
    kafka = require('..'),
    Producer = kafka.Producer,
    Client = kafka.Client;

var client, producer, batchClient, batchProducer, noAckProducer;

var TOPIC_POSTFIX = '_test_' + Date.now();
var EXISTS_TOPIC_4 = '_exists_4' + TOPIC_POSTFIX;
var BATCH_SIZE = 500;
var BATCH_AGE = 300;

var host = process.env['KAFKA_TEST_HOST'] || '';

before(function (done) {
    client = new Client(host);
    batchClient = new Client(host, null, null, { noAckBatchSize: BATCH_SIZE, noAckBatchAge: BATCH_AGE });
    producer = new Producer(client);
    batchProducer = new Producer(batchClient);
    producer.on('ready', function () {
        producer.createTopics([EXISTS_TOPIC_4], false, function (err, created) {
            if(err) return done(err);
            setTimeout(done, 500);
        });
    });    
});

describe('No Ack Producer', function () {

    before(function(done) {
        // Ensure that first message gets the `0`
        producer.send([{ topic: EXISTS_TOPIC_4, messages: '_initial 1' }], function (err, message) {
            message.should.be.ok;
            message[EXISTS_TOPIC_4].should.have.property('0', 0);
            batchProducer.send([{ topic: EXISTS_TOPIC_4, messages: '_initial 2' }], function (err, message) {
                message.should.be.ok;
                message[EXISTS_TOPIC_4].should.have.property('0', 1);
                done(err);
            });
        });
    });

    describe('with no batch client', function () {

        before(function(done) {
            noAckProducer = new Producer(client, { requireAcks: 0 });
            done();
        });

        beforeEach(function() {
            this.sendSpy = sinon.spy(client.brokers[host + ":9092"].socket, 'write');
        });

        afterEach(function() {
            this.sendSpy.restore();
        });

        it('should send message directly', function (done) {
            var self = this;
            noAckProducer.send([{
                topic: EXISTS_TOPIC_4, messages: 'hello kafka no batch'
            }], function (err, message) {
                if (err) return done(err);
                message.result.should.equal('no ack');
                self.sendSpy.args.length.should.be.equal(1);
                self.sendSpy.args[0].toString().should.containEql('hello kafka no batch');
                done();
            });
        });
    });

    describe('with batch client', function () {

        before(function(done) {
            noAckProducer = new Producer(batchClient, { requireAcks: 0 });
            done();
        });

        beforeEach(function() {
            this.sendSpy = sinon.spy(batchClient.brokers[host + ":9092"].socket, 'write');
            this.clock = sinon.useFakeTimers();
        });

        afterEach(function() {
            this.sendSpy.restore();
            this.clock.restore();
        });

        it('should wait to send message 500 ms', function (done) {
            var self = this;
            noAckProducer.send([{
                topic: EXISTS_TOPIC_4, messages: 'hello kafka with batch'
            }], function (err, message) {
                if (err) return done(err);
                message.result.should.equal('no ack');
                self.sendSpy.args.length.should.be.equal(0);
                self.clock.tick(BATCH_AGE - 5);
                self.sendSpy.args.length.should.be.equal(0);
                self.clock.tick(10);
                self.sendSpy.args.length.should.be.equal(1);
                self.sendSpy.args[0].toString().should.containEql('hello kafka with batch');
                done();
            });
        });

        it('should send message once the batch max size is reached', function (done) {
            var self = this;
            var foo = "";
            for (var i = 0; i < BATCH_SIZE; i++) foo += "X" 
            foo += "end of message"
            noAckProducer.send([{
                topic: EXISTS_TOPIC_4, messages: 'hello kafka with batch'
            }], function (err, message) {
                if (err) return done(err);
                message.result.should.equal('no ack');
                self.sendSpy.args.length.should.be.equal(0);
                noAckProducer.send([{
                    topic: EXISTS_TOPIC_4, messages: foo
                }], function (err, message) {
                    if (err) return done(err);
                    message.result.should.equal('no ack');
                    self.sendSpy.args.length.should.be.equal(1);
                    self.sendSpy.args[0].toString().should.containEql('hello kafka with batch');
                    self.sendSpy.args[0].toString().should.containEql('end of message');
                    done();
                });
            });
        });

    });

});
