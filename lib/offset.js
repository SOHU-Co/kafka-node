'use strict';

var Offset = function (client) {
    this.client = client;
    this.ready = this.client.ready;
    this.client.once('connect', function () {
        this.ready = true;
    }.bind(this));
}

Offset.prototype.fetch = function (payloads, cb) {
    if (!this.ready) {
        setTimeout(function () {
            this.fetch(payloads, cb);
        }.bind(this), 100); 
        return;
    } 
    this.client.sendOffsetRequest(this.buildPayloads(payloads),cb);   
}

Offset.prototype.buildPayloads = function (paylaods) {
    return paylaods.map(function (p) {
        p.partition = p.partition || 0;
        p.time = p.time || Date.now();
        p.maxNum = p.maxNum || 1;
        p.metadata = 'm'; // metadata can be arbitrary
        return p;
    });
}

Offset.prototype.commit = function (groupId, payloads, cb) {
    if (!this.ready) {
       setTimeout(function () {
           this.commit(groupId, payloads, cb);
       }.bind(this), 100);
       return;
    }
    this.client.sendOffsetCommitRequest(groupId, this.buildPayloads(payloads), cb);
}
module.exports = Offset;
