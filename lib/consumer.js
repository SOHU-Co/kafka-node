'use strict';

var util = require('util'),
    _ = require('underscore'),
    events = require('events'),
    Client = require('./client'),
    protocol = require('./protocol'),
    FetchRequest = protocol.FetchRequest,
    OffsetCommitRequest = protocol.OffsetCommitRequest;

var DEFAULTS = { 
    groupId: 'kafka-node-group',
    // Auto commit config 
    autoCommit: true,
    autoCommitMsgCount: 100,
    autoCommitIntervalMs: 5000,
    // Fetch message config
    fetchMaxWaitMs: 100,
    fetchMinBytes: 1,
    fetchMaxBytes: 1024 * 10, 
    fromOffset: false
};

var id = 0;

var Consumer = function (client, topics, options) {
    if (!topics) throw new Error('Must have payloads');
    this.fetchCount = 0;
    this.client = client;
    this.options = _.defaults( (options||{}), DEFAULTS );
    this.ready = false;
    this.id = id++;
    this.payloads = this.buildPayloads(topics);
    this.connect();
}
util.inherits(Consumer, events.EventEmitter);

Consumer.prototype.buildPayloads = function (payloads) {
    return payloads.map(function (p) {
       p.partition = p.partition || 0;
       p.offset = p.offset || 0;
       p.maxBytes = DEFAULTS.fetchMaxBytes;
       p.metadata = 'm'; // metadata can be arbitrary
       return p;
    }); 
}

Consumer.prototype.connect = function () {
    //this.client = new Client(this.connectionString, this.clientId);
    var self = this;
    //Client already exists
    this.ready = this.client.ready;
    if (this.ready) this.init();

    this.client.on('ready', function () {
        if (!self.ready) self.init();
        self.ready = true;
    });

    this.client.on('error', function (err) {
        try {
            self.emit('error', err);
        } catch(err) {}
    });

    this.client.on('close', function () {
        console.log('close');
    });

    this.client.on('brokersChanged', function () {
        var topicNames = self.payloads.map(function (p) {
            return p.topic;
        });
        this.refreshMetadata(topicNames, function () {
            self.fetch();
        });
    });
    // 'done' will be emit when a message fetch request complete
    this.on('done', function (topics) {
        self.updateOffsets(topics);
        setImmediate(function() { 
            self.fetch();
        });
    });
}
var count = 0;

Consumer.prototype.init = function () {
    if (this.options.fromOffset)
        return this.fetch();
    this.fetchOffset(this.payloads, function (err, topics) {
        if (err) {
            try {
                this.emit('error', err);
            } catch(e) {}
            return;
        }
        this.updateOffsets(topics);
        this.fetch();
    }.bind(this));
}
/* 
 * Update offset info in current payloads
 */
Consumer.prototype.updateOffsets = function (topics) {
    this.payloads.forEach(function (p) {
        if (!_.isEmpty(topics[p.topic]))
            p.offset = topics[p.topic][p.partition] + 1;
    });
    if (this.options.autoCommit) this.autoCommit();
}

function autoCommit() {
    if (this.committing) return;
    this.committing = true;
    setTimeout(function () { 
        this.committing = false; 
    }.bind(this), this.options.autoCommitIntervalMs);

    var commits = this.payloads.reduce(function (out, p) { 
        if (p.offset !== 0)
            out.push(_.defaults({ offset: p.offset-1 }, p));
        return out;
    }, []);
    this._commit(commits); 
}

Consumer.prototype.commit = Consumer.prototype.autoCommit = autoCommit;

Consumer.prototype._commit = function (payloads, cb) {
    cb = cb ||
         function (err, data) {
            console.log('Offset commited', data);
         }
    this.client.sendOffsetCommitRequest(this.options.groupId, payloads, cb);
}

Consumer.prototype.fetch = function () {
    if (!this.ready) return;
    console.log(this.id, 'fetch count ------------------------------------------------------------------------------------------->', this.fetchCount);
    var maxBytes = null,
        fetchMaxWaitMs = this.options.fetchMaxWaitMs,
        payloads = this.payloads;

    if (this.fetchCount === 0) {
        maxBytes = 1024*1024;
        fetchMaxWaitMs = 100; 
        payloads = this.payloads.map(function (p) { return _.defaults({ maxBytes: maxBytes }, p) });
    }

    this.client.sendFetchRequest(this, payloads, fetchMaxWaitMs, this.options.fetchMinBytes);
}

Consumer.prototype.fetchOffset = function (payloads, cb) {
    var topics = {};
    this.client.sendOffsetFetchRequest(this.options.groupId, payloads, function (err, offsets) {
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
    this.payloads = this.payloads.filter(function (p) { 
        return topics.indexOf(p.topic) === -1;
    });
    this.client.removeTopicMetadata(topics, cb);
}

Consumer.prototype.close = function (force) {
    this.ready = false;
    if (force) this.commit(); 
}

Consumer.prototype.setOffset = function (topic, partition, offset) {
    this.payloads.every(function (p) {
        if (p.topic === topic && p.partition === partition) {
            p.offset = offset;
            return false;
        }
        return true;
    });
}

module.exports = Consumer;
