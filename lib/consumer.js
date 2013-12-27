'use strict';

var util = require('util'),
    _ = require('lodash'),
    events = require('events'),
    Client = require('./client'),
    protocol = require('./protocol'),
    Offset = require('./offset');

var DEFAULTS = { 
    groupId: 'kafka-node-group',
    // Auto commit config 
    autoCommit: true,
    autoCommitMsgCount: 100,
    autoCommitIntervalMs: 5000,
    // Fetch message config
    fetchMaxWaitMs: 100,
    fetchMinBytes: 1,
    fetchMaxBytes: 1024 * 1024, 
    fromOffset: false
};

var nextId = (function () {
    var id = 0;
    return function () {
        return id++;
    }
})();

var Consumer = function (client, topics, options) {
    if (!topics) throw new Error('Must have payloads');
    this.offset = new Offset(client);
    this.fetchCount = 0;
    this.client = client;
    this.options = _.defaults( (options||{}), DEFAULTS );
    this.ready = false;
    this.id = nextId();
    this.payloads = this.buildPayloads(topics);
    this.connect();
}
util.inherits(Consumer, events.EventEmitter);

Consumer.prototype.buildPayloads = function (payloads) {
    var self = this;
    return payloads.map(function (p) {
        if (typeof p !== 'object') p = { topic: p };
        p.partition = p.partition || 0;
        p.offset = p.offset || 0;
        p.maxBytes = self.options.fetchMaxBytes;
        p.metadata = 'm'; // metadata can be arbitrary
        return p;
    }); 
}

Consumer.prototype.connect = function () {
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

Consumer.prototype.init = function () {
    if (!this.payloads.length) return;

    var topics = this.payloads.map(function (p) { return p.topic; });
    this.client.topicExists(topics, function (err, topics) {
        if(err) return this.emit('error', 'Topics ' + topics.toString() + ' not exists');

        this.offset.fetch(this.payloads, function(err, data) {
            if (err) return this.emit('error', err);

            this.payloads.forEach(function (p) {
                p.offlineOffset = data[p.topic][p.partition][0]-1;
            }); 

            if (this.options.fromOffset)
                return this.fetch();

            this.fetchOffset(this.payloads, function (err, topics) {
                if (err) return this.emit('error', err);

                this.updateOffsets(topics);
                this.fetch();
            }.bind(this));
        }.bind(this));
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

function autoCommit(force, cb) {
    if (this.committing && !force) return cb && cb('Offset committing');

    this.committing = true;
    setTimeout(function () { 
        this.committing = false; 
    }.bind(this), this.options.autoCommitIntervalMs);

    var commits = this.payloads.reduce(function (out, p) { 
        if (p.offset !== 0)
            out.push(_.defaults({ offset: p.offset-1 }, p));
        else out.push(p);
        return out;
    }, []);

    this.client.sendOffsetCommitRequest(this.options.groupId, commits, cb);
}
Consumer.prototype.commit = Consumer.prototype.autoCommit = autoCommit;

Consumer.prototype.fetch = function () {
    if (!this.ready) return;
    this.client.sendFetchRequest(this, this.payloads, this.options.fetchMaxWaitMs, this.options.fetchMinBytes);
}

Consumer.prototype.fetchOffset = function (payloads, cb) {
    this.client.sendOffsetFetchRequest(this.options.groupId, payloads, cb);
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
        topics,
        function (err, added) {
            if (err) return cb && cb(err, added);
            
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
    topics = typeof topics === 'string' ? [topics] : topics;      
    this.payloads = this.payloads.filter(function (p) { 
        return !~topics.indexOf(p.topic);
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
