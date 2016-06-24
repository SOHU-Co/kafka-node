'use strict';

var util = require('util');
var async = require('async');
var events = require('events');

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
    self.emit('error', err);
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

Offset.prototype.buildPayloads = function (payloads) {
  return payloads.map(function (p) {
    p.partition = p.partition || 0;
    p.time = p.time || Date.now();
    p.maxNum = p.maxNum || 1;
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

Offset.prototype.fetchLatestOffsets = function (topics, cb) {
  if (!this.ready) {
    setTimeout(function () {
      this.fetchLatestOffsets(topics, cb);
    }.bind(this), 100);
    return;
  }

  var client = this.client;
  async.waterfall([
    function (callback) {
      client.loadMetadataForTopics(topics, callback);
    },
    function (topicsMetaData, callback) {
      var payloads = [];
      var metaDatas = topicsMetaData[1].metadata;
      Object.keys(metaDatas).forEach(function (topicName) {
        var topic = metaDatas[topicName];
        Object.keys(topic).forEach(function (partition) {
          payloads.push({
            topic: topicName,
            partition: partition,
            time: -1
          });
        });
      });

      var offset = new Offset(client);
      offset.fetch(payloads, callback);
    },
    function (results, callback) {
      Object.keys(results).forEach(function (topicName) {
        var topic = results[topicName];

        Object.keys(topic).forEach(function (partitionName) {
          topic[partitionName] = topic[partitionName][0];
        });
      });
      callback(null, results);
    }
  ], cb);
};

module.exports = Offset;
