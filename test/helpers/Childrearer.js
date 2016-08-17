var EventEmitter = require('events').EventEmitter;
var util = require('util');
var debug = require('debug')('kafka-node:Test-Childrearer');
var fork = require('child_process').fork;
var async = require('async');
var _ = require('lodash');

function Childrearer () {
  EventEmitter.call(this);
  this.children = [];
  this.id = 0;
}

util.inherits(Childrearer, EventEmitter);

Childrearer.prototype.setVerifier = function (topic, groupId, verify) {
  this.topic = topic;
  this.groupId = groupId;
  this.verify = verify;
};

Childrearer.prototype.nextId = function () {
  return ++this.id;
};

Childrearer.prototype.closeAll = function () {
  this.children.forEach(function (child) {
    child.kill();
  });
};

Childrearer.prototype.kill = function (numberOfChildren, callback) {
  var children = _.sample(this.children, numberOfChildren);
  this._killEachChild(children, callback);
};

Childrearer.prototype.killLast = function (callback) {
  var child = _.last(this.children);
  this._killEachChild([child], callback);
};

Childrearer.prototype.killFirst = function (callback) {
  var child = _.first(this.children);
  this._killEachChild([child], callback);
};

Childrearer.prototype._killEachChild = function (children, callback) {
  var self = this;
  async.each(children, function (child, callback) {
    child.once('close', function (code, signal) {
      debug('child %s killed %d %s', this._childNum, code, signal);
      _.pull(self.children, this);
      callback();
    });
    child.kill();
  }, callback);
};

Childrearer.prototype.raise = function (children, callback, waitTime) {
  var newChildren = _.times(children, this._raiseChild, this);

  this.children = this.children.concat(newChildren);

  if (callback) {
    async.series([
      function (callback) {
        async.each(newChildren, function (child, callback) {
          child.once('message', function (data) {
            if (data.event === 'registered') {
              callback(null);
            } else {
              callback(new Error('unregistered event: ' + data.event));
            }
          });
        }, callback);
      },

      function (callback) {
        if (waitTime) {
          setTimeout(callback, waitTime);
        } else {
          callback();
        }
      }], callback
    );
  }
};

Childrearer.prototype._raiseChild = function () {
  var self = this;
  var childNumber = this.nextId();
  debug('forking child %d', childNumber);
  var child = fork('test/helpers/child-hlc', ['--groupId=' + this.groupId, '--topic=' + this.topic, '--consumerId=' + 'child_' + childNumber]);
  child._childNum = childNumber;
  child.on('message', function (data) {
    if (data.message) {
      self.verify.call(this, data);
    }
  });
  return child;
};

module.exports = Childrearer;
