'use strict';

var net = require('net'),
    util = require('util'),
    _ = require('underscore'),
    events = require('events'),
    Binary = require('binary'),
    Client = require('./client'),
    protocol = require('./protocol'),
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

var Consumer = function (payloads, connectionString, groupId, clientId) {
    if (!payloads) throw new Error('Must have payloads');
    this.groupId = groupId || 'test_group';
    this.clientId = clientId;
    this.connectionString = connectionString;
    this.ready = false;
    this.offsets = {};
    this.payloads = this.buildPayloads(payloads);
    this.connect();
}
util.inherits(Consumer, events.EventEmitter);

Consumer.prototype.buildPayloads = function (payloads) {
    return payloads.map(function (p) {
       p.partition = p.partition || 0;
       p.offset = p.offset || 0;
       p.maxBytes = DEFAULTS.fetchMaxBytes;
       p.autoCommit = p.autoCommit || DEFAULTS.autoCommit;
       p.autoCommitIntervalMs = p.commitIntervalMs || DEFAULTS.autoCommitIntervalMs;
       p.committing = false;
       if (p.autoCommit) p.metadata = 'm'; // metadata can be arbitrary
       return p;
    }); 
}

Consumer.prototype.connect = function () {
    this.client = new Client(this.connectionString, this.clientId);
    var self = this;
    this.client.on('ready', function () {
        self.ready = true;
    });
    this.client.once('connect', function () {
        process.nextTick(function () {
            self.fetchOffset(function (err, topics) {
                self.updateOffsets(topics, true);
                self.fetch();
            });
        });
    });
    this.client.on('error', function (err) {
        try {
            self.emit('error', err);
        } catch(err) {}
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

/* 
 * Update offset info in current payloads, if tirst is true, we don't commit the offset
 */
Consumer.prototype.updateOffsets = function (topics, first) {
    var commits = [];
    this.payloads.forEach(function (p) {
        if (!_.isEmpty(topics[p.topic])) {
            p.offset = topics[p.topic][p.partition];
            if (!first && p.autoCommit) commits.push(p);
        }
    });
    if (commits.length) this.autoCommit(commits);
}

Consumer.prototype.autoCommit = function (payloads) {
    payloads.forEach(function (p) {
        if (!p.committing) {
            setTimeout(function () { p.committing = false }, p.autoCommitIntervalMs);
            p.committing = true;
            this.commit([p]);
        }
    }.bind(this)); 
}

Consumer.prototype.commit = function (payloads, cb) {
    cb = cb ||
         function (err, data) {
            console.log('commited', data);
         }
    this.client.sendOffsetCommitRequest(this.groupId, payloads, cb);
}

Consumer.prototype.fetch = function () {
    var payloads = this.payloads.map(function (p) {
        return new FetchRequest(p.topic, p.partition, p.offset + 1, p.maxBytes);
    });
    this.client.sendFetchRequest(payloads, DEFAULTS.fetchMaxWaitMs, DEFAULTS.fetchMinBytes);
}

Consumer.prototype.fetchOffset = function (cb) {
    this.client.sendOffsetFetchRequest(this.groupId, this.payloads, cb);
}

Consumer.prototype.addTopics = function (topics, cb) {
    var self = this;
    if (!this.ready) {
        setTimeout(function () { 
            self.addTopics(topics,cb) }
        , 100);
        return;
    }
    this.client.addTopics(
        topics.map(function (t) { return t.topic }),
        function (err, added) {
            if (err) return cb(err);
            self.buildPayloads(topics).forEach(function (t) {
                self.payloads.push(t);
            });
            cb(null, added);
        }
    );     
}

module.exports = Consumer;
