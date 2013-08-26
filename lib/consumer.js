'use strict';

var _ = require('underscore'),
    net = require('net'),
    util = require('util'),
    events = require('events'),
    Binary = require('binary'),
    protocol = require('./protocol'),
    Client = require('./client'),
    FetchRequest = protocol.FetchRequest,
    OffsetCommitRequest = protocol.OffsetCommitRequest;

var DEFAULTS = { 
    partition: [0], 
    // Auto commit config 
    autoCommit: true,
    autoCommitMsgCount: 100,
    autoCommitIntervalMs: 5000,
    // Fetch message config
    fetchMaxWaitMs: 100,
    fetchMinBytes: 4096,
    fetchMaxBytes: 1024 * 1024
};

var Consumer = function (payloads, host, port, groupId, clientId) {
    if (!payloads) throw new Error('Must have payloads');
    this.groupId = 'test_group';
    this.offsets = {};
    this.buildPayloads(payloads);
    this.connect();
}
util.inherits(Consumer, events.EventEmitter);

Consumer.prototype.buildPayloads = function (payloads) {
    //this.autoCommit = args.length < 6 ? DEFAULTS.autoCommit : args[5];
    //this.autoCommitMsgCount = args.length < 7 ? DEFAULTS.autoCommitMsgCount : args[6]; 
    //this.autoCommitIntervalMs = args.length < 8 ? DEFAULTS.autoCommitIntervalMs : args[7];
    this.payloads = payloads.map(function (p) {
       p.partition = p.partition || 0;
       p.offset = p.offset || 0;
       p.maxBytes = DEFAULTS.fetchMaxBytes;
       p.autoCommit = p.autoCommit || DEFAULTS.autoCommit;
       p.autoCommitIntervalMs = p.commitIntervalMs || DEFAULTS.autoCommitIntervalMs;
       if (p.autoCommit) p.metadata = 'metadata'; // metadata can be arbitrary
       return p;
    }); 
}

Consumer.prototype.connect = function () {
    this.client = new Client(this.host, this.port);
    // emiter...
    var self = this;
    this.client.on('connect', function () {
        process.nextTick(function () {
            self.fetch();
        });
    });
    this.client.on('error', function (err) {
        self.emit('error', err);
    });
    this.client.on('close', function () {
        console.log('close');
    });
    this.client.on('message', function (message) {
        self.emit('message', message);
    });
    this.client.on('done', function (topics) {
        self.updateOffsets(topics);
        process.nextTick(function() { 
            self.fetch();
        });
    });
}

Consumer.prototype.updateOffsets = function (topics) {
    var commits = [];
    this.payloads.forEach(function (p) {
        if (!_.isEmpty(topics[p.topic])) {
            var offset = topics[p.topic][p.partition];
            p.offset = offset + 1;
            if (p.autoCommit) commits.push(p);
        } else {
            p.offset += 1;
        }
    });
    //if (commits.length) this.commit(commits);
}

Consumer.prototype.fetch = function () {
    this.client.sendFetchRequest(this.payloads, DEFAULTS.fetchMaxWaitMs, DEFAULTS.fetchMinBytes);
}

Consumer.prototype.commit = function (payloads, cb) {
    cb = cb ||
         function (data) {
            console.log(data);
         }
    this.client.sendOffsetCommitRequest(this.groupId, payloads, cb);
}

module.exports = Consumer;
