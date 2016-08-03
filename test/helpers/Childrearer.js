var EventEmitter = require('events');
var util = require('util');
var debug = require('debug')('kafka-node:Test-Childrearer');
var fork = require('child_process').fork;
var async = require('async');
var _ = require('lodash');

function Childrearer () {
  EventEmitter.call(this);
  this.children = [];
}

util.inherits(Childrearer, EventEmitter);

Childrearer.prototype.setVerifier = function (topic, groupId, verify) {
  this.topic = topic;
  this.groupId = groupId;
  this.verify = verify;
};

Childrearer.prototype.closeAll = function () {
  this.children.forEach(function (child) {
    child.kill();
  });
};

Childrearer.prototype.kill = function (children, callback) {
  var children = _.sample(this.children, children);
  async.each(children, function (child, callback) {
    child.once('close', function (code, signal){
      debug('child %s killed %d %s', this._childNum, code, signal);
      callback();
    });
    child.kill();
  }, callback);
}

Childrearer.prototype.raise = function (children, callback) {
  while (children--) {
    this.children.push(this._raiseChild());
  }

  if (callback) {
    async.each(this.children, function (child, callback) {
      child.once('message', function (data) {
        if (data.event === 'registered') {
          callback(null);
        } else {
          callback(new Error('not registered event: ' + data.event));
        }
      });
    }, callback);
  }
};

Childrearer.prototype._raiseChild = function () {
  var self = this;
  var childNumber = this.children.length + 1;
  debug('forking child %d', childNumber);
  var child = fork('test/helpers/child-hlc', ['--groupId=' + this.groupId, '--topic=' + this.topic]);
  child._childNum = childNumber;
  child.on('message', function (data) {
    if (data.message) {
      self.verify.call(this, data);
    }
  });
  return child;
};

module.exports = Childrearer;
