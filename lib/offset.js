'use strict';
var util = require('util'),
    _ = require('lodash'),
    events = require('events');

var Offset = function (client) {
    var self = this;
    this.client = client;
    this.ready = this.client.ready;
    this.client.on('ready', function () {
        self.ready = true;
        self.emit('ready');
    });
    this.client.once('connect', function () {
        self.emit('connect');
    });
    this.client.on('error', function (err) {
    });
};
util.inherits(Offset, events.EventEmitter);

Offset.prototype.fetch = function (payloads, cb) {
    if (!this.ready) {
        setTimeout(function () {
            this.fetch(payloads, cb);
        }.bind(this), 100);
        return;
    }
    this.client.sendOffsetRequest(this.buildPayloads(payloads), cb);
};

Offset.prototype.buildPayloads = function (paylaods) {
    return paylaods.map(function (pi) {
        var p = _.clone(pi, true);
        p.partition = pi.partition || 0;
        p.time = pi.time || Date.now();
        p.maxNum = pi.maxNum || 1;
        p.metadata = 'm'; // metadata can be arbitrary
        return p;
    });
};

Offset.prototype.commit = function (groupId, payloads, cb) {
    if (!this.ready) {
        setTimeout(function () {
            this.commit(groupId, payloads, cb);
        }.bind(this), 100);
        return;
    }
    this.client.sendOffsetCommitRequest(groupId, this.buildPayloads(payloads), cb);
};

Offset.prototype.fetchCommits = function (groupId, payloads, cb) {
    if (!this.ready) {
        setTimeout(function () {
            this.fetchCommits(groupId, payloads, cb);
        }.bind(this), 100);
        return;
    }
    this.client.sendOffsetFetchRequest(groupId, this.buildPayloads(payloads), cb);
};
module.exports = Offset;
