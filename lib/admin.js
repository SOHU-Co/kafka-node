'use strict';

var KafkaClient = require('./kafkaClient');
var util = require('util');
var events = require('events');

var Admin = function (kafkaClient) {
  if (!(kafkaClient instanceof KafkaClient)) {
    throw new Error("'Admin' only accepts 'KafkaClient' for its kafka client.");
  }

  var self = this;
  this.client = kafkaClient;
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
util.inherits(Admin, events.EventEmitter);

Admin.prototype.listGroups = function (cb) {
  if (!this.ready) {
    this.once('ready', () => this.listGroups(cb));
    return;
  }
  this.client.getListGroups(cb);
};

Admin.prototype.describeGroups = function (consumerGroups, cb) {
  if (!this.ready) {
    this.once('ready', () => this.describeGroups(consumerGroups, cb));
    return;
  }
  this.client.getDescribeGroups(consumerGroups, cb);
};

module.exports = Admin;
