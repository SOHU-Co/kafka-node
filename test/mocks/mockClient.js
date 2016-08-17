var util = require('util');
var EventEmitter = require('events').EventEmitter;
var FakeZookeeper = require('./mockZookeeper');

function FakeClient () {
  EventEmitter.call(this);

  this.zk = new FakeZookeeper();

  this.topicExists = function (topics, cb) {
    setImmediate(cb);
  };
  this.refreshMetadata = function (topicNames, cb) {
    setImmediate(cb);
  };
  this.sendOffsetCommitRequest = function (groupId, commits, cb) {
    setImmediate(cb);
  };
  this.sendFetchRequest = function (consumer, payloads, fetchMaxWaitMs, fetchMinBytes, maxTickMessages) {};
  this.sendOffsetFetchRequest = function (groupId, payloads, cb) {
    setImmediate(cb);
  };
  this.sendOffsetRequest = function (payloads, cb) {
    setImmediate(cb);
  };
  this.addTopics = function (topics, cb) {
    setImmediate(cb);
  };
  this.removeTopicMetadata = function (topics, cb) {
    setImmediate(cb);
  };
  this.close = function (cb) {
    setImmediate(cb);
  };
}
util.inherits(FakeClient, EventEmitter);

module.exports = FakeClient;
