'use strict';

const KafkaClient = require('./kafkaClient');
const resources = require('./resources');
const util = require('util');
const EventEmitter = require('events');

function Admin (kafkaClient) {
  EventEmitter.call(this);
  if (!(kafkaClient instanceof KafkaClient)) {
    throw new Error("'Admin' only accepts 'KafkaClient' for its kafka client.");
  }

  var self = this;
  this.client = kafkaClient;
  this.RESOURCE_TYPES = resources.RESOURCE_TYPES;
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
}
util.inherits(Admin, EventEmitter);

Admin.prototype.listGroups = function (cb) {
  if (!this.ready) {
    this.once('ready', () => this.listGroups(cb));
    return;
  }
  this.client.getListGroups(cb);
};

Admin.prototype.listTopics = function (cb) {
  if (!this.ready) {
    this.once('ready', () => this.listTopics(cb));
    return;
  }
  this.client.loadMetadataForTopics([], cb);
};

Admin.prototype.describeGroups = function (consumerGroups, cb) {
  if (!this.ready) {
    this.once('ready', () => this.describeGroups(consumerGroups, cb));
    return;
  }
  this.client.getDescribeGroups(consumerGroups, cb);
};

Admin.prototype.createTopics = function (topics, cb) {
  if (!this.ready) {
    this.once('ready', () => this.client.createTopics(topics, cb));
    return;
  }
  this.client.createTopics(topics, cb);
};

Admin.prototype.describeConfigs = function (payload, cb) {
  if (!this.ready) {
    this.once('ready', () => this.describeConfigs(payload, cb));
    return;
  }
  this.client.describeConfigs(payload, cb);
};

module.exports = Admin;
