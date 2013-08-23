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
    if (!pyaloads) throw new Error('Must have payloads');
    this.groupId = 'test_group';
    this.topic = topic;
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
    this.client.on('message', function (messages) {
        self.emit('message', self.handleMessages(messages));
        process.nextTick(function() { 
            self.fetch()
        });
    });
}


Consumer.prototype.fetch = function (options) {
    //if (typeof options !== 'object') throw new Error('options should be a [Object]');
    this.client.sendFetchRequest(payloads, DEFAULTS.fetchMaxWaitMs, DEFAULTS.fetchMinBytes);
    options = _.defaults(options || {}, DEFAULTS);
    if (this.topicMetadata) {
        var payloads = this.payloadsByBroker();
        this.client.sendFetchRequest(payloads, options.fetchMaxWaitMs, options.fetchMinBytes);
    } else {
        this.client.loadMetadataForTopics([this.topic], function (metadatas) {
            this.topicMetadata = metadatas[1];
            this.client.brokerMetadata = metadatas[0];
            var payloads = this.payloadsByBroker(); 
            this.client.sendFetchRequest(payloads, options.fetchMaxWaitMs, options.fetchMinBytes);
        }.bind(this));     
    }
}

Consumer.prototype.payloadsByBroker = function () {
    return this.partitions.reduce(function (out, p) {
        var leader = this.leaderByPartition(p);
        out[leader] = out[leader] || [];
        out[leader].push(new FetchRequest(this.topic, p, this.offsets[p], 1024*1024));
        return out;
    }.bind(this), {});
}

Consumer.prototype.leaderByPartition = function (partition) {
    return this.topicMetadata[this.topic][partition].leader;
}

Consumer.prototype.handleMessages = function (resp) {
    var self = this;
    var messages = [];
    resp.forEach(function (r) {
        r.fetchPartitions.forEach(function (p) {
            var partition = p.partition;
            p.messageSet.forEach(function (message) {
                if(message.offset !== null) self.offsets[partition] = message.offset+1;
                messages.push(message.message.value.toString());
            });
        });
    });
    return messages;
}

Consumer.prototype.commitOffsets = function () {
    var reqs = this.partitions.reduce(function (reqs, p) {
        reqs.push(new OffsetCommitRequest(this.topic, p, this.offsets[p], ''));
        return reqs;
    }.bind(this), []);
    var reqs =  this.partitions.reduce(function (out, p) {
        var leader = this.leaderByPartition(p);
        out[leader] = out[leader] || [];
        out[leader].push(new FetchRequest(this.topic, p, this.offsets[p], 1024));
        return out;
    }.bind(this), {});

    var request = protocol.encodeOffsetCommitRequest(this.clientId,
                                                     ++this.correlationId,
                                                     this.group,
                                                     reqs); 
}

module.exports = Consumer;
