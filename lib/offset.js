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
    this.once('ready', () => this.fetch(payloads, cb));
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
    this.once('ready', () => this.commit(groupId, payloads, cb));
    return;
  }
  this.client.sendOffsetCommitRequest(groupId, this.buildPayloads(payloads), cb);
};

Offset.prototype.fetchCommits = function (groupId, payloads, cb) {
  if (!this.ready) {
    this.once('ready', () => this.fetchCommits(groupId, payloads, cb));
    return;
  }
  this.client.sendOffsetFetchRequest(groupId, this.buildPayloads(payloads), cb);
};

Offset.prototype.fetchLatestOffsets = function (topics, cb) {
  fetchOffsets(this, topics, cb, -1);
};

Offset.prototype.fetchEarliestOffsets = function (topics, cb) {
  fetchOffsets(this, topics, cb, -2);
};

// private helper
function fetchOffsets (offset, topics, cb, when) {
  if (!offset.ready) {
    if (when === -1) {
      offset.once('ready', () => offset.fetchLatestOffsets(topics, cb));
    } else if (when === -2) {
      offset.once('ready', () => offset.fetchEarliestOffsets(topics, cb));
    }
    return;
  }
  async.waterfall([
    callback => {
      offset.client.loadMetadataForTopics(topics, callback);
    },
    (topicsMetaData, callback) => {
      var payloads = [];
      var metaDatas = topicsMetaData[1].metadata;
      Object.keys(metaDatas).forEach(function (topicName) {
        var topic = metaDatas[topicName];
        Object.keys(topic).forEach(function (partition) {
          payloads.push({
            topic: topicName,
            partition: partition,
            time: when
          });
        });
      });

      if (payloads.length === 0) {
        return callback(new Error('Topic(s) does not exist'));
      }

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
}

module.exports = Offset;
