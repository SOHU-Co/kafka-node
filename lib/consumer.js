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
    this.groupId = groupId || 'kafka-node-group';
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
       p.autoCommit = p.autoCommit === false ? false : DEFAULTS.autoCommit;
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
            self.fetchOffset(self.payloads, function (err, topics) {
                if (err) {
                    try {
                        self.emit('error', err);
                    } catch(e) {}
                    return;
                }
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
    // 'done' will be emit when a message fetch request complete
    this.client.on('done', function (topics) {
        self.updateOffsets(topics);
        //console.log(topics);
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

function autoCommit(payloads, cb) {
    if (typeof payloads === 'function' || typeof payloads === 'undefined') {
        cb = payloads;
        payloads = this.payloads;
    }
    payloads.forEach(function (p) {
        if (!p.committing) {
            setTimeout(function () { p.committing = false }, p.autoCommitIntervalMs);
            p.committing = true;
            this._commit([p], cb);
        }
    }.bind(this)); 
}

Consumer.prototype.commit = Consumer.prototype.autoCommit = autoCommit;

Consumer.prototype._commit = function (payloads, cb) {
    cb = cb ||
         function (err, data) {
            console.log('OffsetCommited', data);
         }
    this.client.sendOffsetCommitRequest(this.groupId, payloads, cb);
}

Consumer.prototype.fetch = function () {
    var payloads = this.payloads.map(function (p) {
        return new FetchRequest(p.topic, p.partition, p.offset + 1, p.maxBytes);
    });
    this.client.sendFetchRequest(payloads, DEFAULTS.fetchMaxWaitMs, DEFAULTS.fetchMinBytes);
}

Consumer.prototype.fetchOffset = function (payloads, cb) {
    var topics = {};
    this.client.sendOffsetFetchRequest(this.groupId, payloads, function (err, offsets) {
        if (err) return cb(err);
        _.extend(topics, offsets);
        // Waiting for all fetch request return
        if (Object.keys(topics).length < payloads.length) return;
        cb(null, topics);
    });
}

Consumer.prototype.addTopics = function (topics, cb) {
    var self = this;
    if (!this.ready) {
        setTimeout(function () { 
            self.addTopics(topics,cb) }
        , 100);
        return;
    }
    var topicNames = topics.map(function (t) { return t.topic });
    this.client.addTopics(
        topicNames,
        function (err, added) {
            if (err) return cb && cb(err);
            
            var payloads = self.buildPayloads(topics);
            // update offset of topics that will be added
            self.fetchOffset(payloads, function (err, offsets) {
                if (err) return cb(err);
                payloads.forEach(function (p) { 
                    p.offset = offsets[p.topic][p.partition];    
                    self.payloads.push(p);
                });
                cb && cb(null, added);
            });
        }
    );     
}

Consumer.prototype.removeTopics = function (topics, cb) {
    topics = typeof topic === 'string' ? [topics] : topics;      
    this.payloads = this.payloads.filter(function (p) { return topics.indexOf(p.topic) === -1 });
    this.client.removeTopicMetadata(topics);
    cb(null, 'topics removed');
}

module.exports = Consumer;
