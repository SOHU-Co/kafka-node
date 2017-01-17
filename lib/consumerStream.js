'use strict';
var util = require('util');
var _ = require('lodash');
var Readable = require('stream').Readable;
var debug = require('debug');

var protocol = require('./protocol');

var DEFAULTS = {
  groupId: 'kafka-node-group',
  // Auto commit config
  autoCommit: true,
  autoCommitMsgCount: 100,
  autoCommitIntervalMs: 5000,
  // Fetch message config
  fetchMaxWaitMs: 100,
  fetchMinBytes: 1,
  fetchMaxBytes: 1024 * 1024,
  fromOffset: false,
  encoding: 'utf8'
};
var ConsumerStream = function (client, topics, options) {
  options.objectMode = true;
  options.highWaterMark = 100;
  this.highWaterMark = 100;
  Readable.call(this, options);
  if (!topics) {
    throw new Error('You must specify topics to subscribe to.');
  }
  // Whether we have sent a fetch request for which we have not yet received
  // all messages.
  this.fetchInFlight = false;
  this.fetchCount = 0;
  this.client = client;
  this.options = _.defaults((options || {}), DEFAULTS);
  this.ready = false;
  this.payloads = this.buildPayloads(topics);
  this.connect();
  this.encoding = this.options.encoding;
  this.emittedMessages = 0;
  this.messageBuffer = [];
  this._reading = false;
};
util.inherits(ConsumerStream, Readable);

// The older non-stream based consumer emitted `message` events rather
// than data events. This provides a backward compatibility layer for
// receiving message events instead.
ConsumerStream.prototype._emit = ConsumerStream.prototype.emit;
ConsumerStream.prototype.emit = function () {
  if (arguments[0] === 'data') {
    this._emit('message', arguments[1]);
  }
  this._emit.apply(this, arguments);
};

ConsumerStream.prototype._read = function () {
  this._reading = true;
  this.transmitMessages();
};

/**
 * Buffers the received message then checks to see if we shoudl send.
 */
ConsumerStream.prototype.handleMessage = function (message) {
  this.messageBuffer.push(message);
  this.transmitMessages();
};

ConsumerStream.prototype.transmitMessages = function () {
  //var keepGoing = !this.isPaused();
  while (this._reading && this.messageBuffer.length > 0) {
    this._reading = this.push(this.messageBuffer.shift());
  }
  if (this.messageBuffer.length === 0 && this._reading) {
    this.fetch();
  }
};

/**
 * Note, this is basically copied from client.sendFetchRequest().
 */
ConsumerStream.prototype.fetch = function () {
  var self = this;
  if (self.ready && !self.fetchInFlight) {
    self.fetchInFlight = true;
    var encoder = protocol.encodeFetchRequest(self.fetchMaxWaitMs, self.fetchMinBytes);
    var decoder = protocol.decodeFetchResponse(self.decodeCallback.bind(self), self.maxTickMessages);
    self.client.send(self.payloads, encoder, decoder, function (err) {
      self.fetchInFlight = false;
      if (err) {
        Array.prototype.unshift.call(arguments, 'error');
        self.emit.apply(self, arguments);
      }
      self.fetchInFlight = false;
      // TODO: Make this configurable.
      if (self.messageBuffer.length < 10) {
        setImmediate(self.fetch.bind(self));
      }
    });
  }
};

/**
 * The decode callback is invoked as data is decoded from the response.
 */
ConsumerStream.prototype.decodeCallback = function (err, type, message) {
  if (err) {
    switch (err.message) {
      case 'OffsetOutOfRange':
        return this.emit('offsetOutOfRange', err);
      case 'NotLeaderForPartition':
        return this.emit('brokersChanged');
      default:
        return this.emit('error', err);
    }
  }

  var encoding = this.options.encoding;

  if (type === 'message') {
    if (encoding !== 'buffer' && message.value) {
      message.value = message.value.toString(encoding);
    }
    this.handleMessage(message);
  } else {
    this.updateOffsets(message);
    if (this.messageBuffer.length === 0) {
      // If our message buffer was empty then it may be time to read again - read.
      this.fetch();
    }
    // TODO: Do I want this event? BC?
    // self.emit('done', message);
  }
};

ConsumerStream.prototype.connect = function () {
  var self = this;
  // Client already exists
  this.ready = this.client.ready;
  if (this.ready) this.init();

  this.client.on('ready', function () {
    // Should we actually fetch on connect and emit readable when we have at least some data ready?
    self.emit('readable');
    debug('consumer ready');
    if (!self.ready) self.init();
    self.ready = true;
  });

  this.client.on('error', function (err) {
    debug('client error %s', err.message);
    self.emit('error', err);
  });

  this.client.on('close', function () {
    debug('connection closed');
  });

  this.client.on('brokersChanged', function () {
    var topicNames = self.payloads.map(function (p) {
      return p.topic;
    });

    this.refreshMetadata(topicNames, function (err) {
      if (err) return self.emit('error', err);
    });
  });
};

ConsumerStream.prototype.updateOffsets = function (topics, initing) {
  this.payloads.forEach(function (p) {
    if (!_.isEmpty(topics[p.topic]) && topics[p.topic][p.partition] !== undefined) {
      var offset = topics[p.topic][p.partition];
      if (offset === -1) offset = 0;
      if (!initing) p.offset = offset + 1;
      else p.offset = offset;
    }
  });
};

ConsumerStream.prototype.close = function (force, cb) {
  this.ready = false;
  if (typeof force === 'function') {
    cb = force;
    force = false;
  }

  if (force) {
    this.commit(force, function (err) {
      this.emit('error', err);
      this.client.close(cb);
    }.bind(this));
  } else {
    this.client.close(cb);
  }
};

ConsumerStream.prototype.init = function () {
  if (!this.payloads.length) {
    return;
  }

  var self = this;
  var topics = self.payloads.map(function (p) { return p.topic; });

  self.client.topicExists(topics, function (err) {
    if (err) {
      return self.emit('error', err);
    }
  });
};

ConsumerStream.prototype.buildPayloads = function (payloads) {
  var self = this;
  return payloads.map(function (p) {
    if (typeof p !== 'object') p = { topic: p };
    p.partition = p.partition || 0;
    p.offset = p.offset || 0;
    p.maxBytes = self.options.fetchMaxBytes;
    p.metadata = 'm'; // metadata can be arbitrary
    return p;
  });
};

module.exports = ConsumerStream;
