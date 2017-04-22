'use strict';

const Client = require('./client');
const logger = require('./logging')('kafka-node:KafkaClient');
const async = require('async');
// const retry = require('retry');
const assert = require('assert');
const _ = require('lodash');
const util = require('util');
const net = require('net');
const BufferList = require('bl');
const tls = require('tls');
const BrokerWrapper = require('./wrapper/BrokerWrapper');
const errors = require('./errors');
const validateConfig = require('./utils').validateConfig;
const TimeoutError = require('./errors/TimeoutError');

const DEFAULTS = {
  kafkaHost: 'localhost:9092',
  connectTimeout: 10000,
  requestTimeout: 30000,
  ssl: false,
  autoConnect: true
};

const KafkaClient = function (options) {
  this.options = _.defaults((options || {}), DEFAULTS);

  this.sslOptions = this.options.sslOptions;
  this.ssl = !!this.sslOptions;

  if (this.options.ssl === true) {
    this.options.ssl = {};
  }

  if (this.options.clientId) {
    validateConfig('clientId', this.options.clientId);
  }

  this.clientId = this.options.clientId || 'kafka-node-client';
  this.noAckBatchOptions = this.noAckBatchOptions;
  this.brokers = {};
  this.longpollingBrokers = {};
  this.topicMetadata = {};
  this.topicPartitions = {};
  this.correlationId = 0;
  this._socketId = 0;
  this.cbqueue = {};
  this.brokerMetadata = {};
  this.ready = false;

  this.initialHosts = parseHostList(this.options.kafkaHost);

  if (this.options.autoConnect) {
    this.connect();
  }
};

util.inherits(KafkaClient, Client);

/*
{ '1001':
   { jmx_port: -1,
     timestamp: '1492521177416',
     endpoints: [ 'PLAINTEXT://127.0.0.1:9092', 'SSL://127.0.0.1:9093' ],
     host: '127.0.0.1',
     version: 2,
     port: '9092',
     id: '1001' } }

     vs

{ '1001': { nodeId: 1001, host: '127.0.0.1', port: 9093 } }

     */

function parseHost (hostString) {
  const piece = hostString.split(':');
  return {
    host: piece[0],
    port: piece[1]
  };
}

function parseHostList (hosts) {
  return hosts.split(',').map(parseHost);
}

KafkaClient.prototype.connect = function () {
  if (this.connecting) {
    logger.debug('connect request ignored. Client is currently connecting');
    return;
  }
  this.connecting = true;

  this.connectToBrokers(this.initialHosts, error => {
    this.connecting = false;
    if (error) {
      return this.emit('error', error);
    }
    this.emit('ready');
  });
};

KafkaClient.prototype.connectToBrokers = function (hosts, callback) {
  assert(hosts && hosts.length, 'No hosts to connect to');
  let index = 0;
  let ready = false;
  let errors = [];
  async.doWhilst(callback => {
    this.connectToBroker(hosts[index++], this.options.connectTimeout, error => {
      if (error) {
        logger.debug('failed to connect because of ', error);
        errors.push(error);
        callback(null);
        return;
      }
      errors.length = 0;
      ready = true;
      callback(null);
    });
  },
  () => !ready && index < hosts.length,
  () => {
    if (ready) {
      return callback(null);
    }
    if (errors.length) {
      callback(errors.pop());
    }
  });
};

KafkaClient.prototype.connectToBroker = function (broker, timeout, callback) {
  logger.debug(`Trying to connect to host: ${broker.host} port: ${broker.port}`);
  let connectTimer = null;

  const onError = (error) => {
    clearTimeout(connectTimer);
    connectTimer = null;
    this.closeBrokers(this.brokers);
    callback(error);
  };

  const brokerWrapper = this.setupBroker(broker.host, broker.port, false, this.brokers);
  const socket = brokerWrapper.socket;

  socket.once('connect', () => {
    logger.debug('broker socket connected');
    this.loadMetadataForTopics([], (error, result) => {
      if (error) {
        return callback(error);
      }
      this.brokerMetadata = result[0];
      this.updateMetadatas(result);
      clearTimeout(connectTimer);
      callback();
    });
  });

  socket.on('error', (error) => {
    logger.debug('Socket Error', error);
    onError(error);
  });

  connectTimer = setTimeout(() => {
    logger.debug('Connection timeout error');
    onError(new TimeoutError(`Connection timeout of ${timeout} exceeded`));
  }, timeout);
};

KafkaClient.prototype.wrapTimeoutIfNeeded = function (socketId, correlationId, callback) {
  if (this.options.requestTimeout === false) {
    return callback;
  }

  let timeoutId = null;

  const wrappedFn = function () {
    clear();
    callback.apply(null, arguments);
  };

  function clear () {
    clearTimeout(timeoutId);
    timeoutId = null;
  }

  timeoutId = setTimeout(() => {
    this.unqueueCallback(socketId, correlationId);
    callback(new Error(`Request timed out after ${this.options.requestTimeout}ms`));
    callback = _.noop;
  }, this.options.requestTimeout);

  return wrappedFn;
};

KafkaClient.prototype.queueCallback = function (socket, id, data) {
  data[1] = this.wrapTimeoutIfNeeded(socket.socketId, id, data[1]);
  Client.prototype.queueCallback.call(this, socket, id, data);
};

KafkaClient.prototype.createBroker = function (host, port, longpolling) {
  var self = this;
  var socket;
  if (self.ssl) {
    socket = tls.connect(port, host, self.sslOptions);
  } else {
    socket = net.createConnection(port, host);
  }
  socket.addr = host + ':' + port;
  socket.host = host;
  socket.port = port;
  socket.socketId = this.nextSocketId();
  if (longpolling) socket.longpolling = true;

  socket.on('connect', function () {
    var lastError = this.error;
    this.error = null;
    if (lastError) {
      this.waiting = false;
      self.emit('reconnect');
    } else {
      self.emit('connect');
    }
  });
  socket.on('error', function (err) {
    this.error = err;
    if (!self.connecting) {
      self.emit('socket_error', err);
    }
  });
  socket.on('close', function (hadError) {
    self.emit('close', this);
    if (hadError && this.error) {
      self.clearCallbackQueue(this, this.error);
    } else {
      self.clearCallbackQueue(this, new errors.BrokerNotAvailableError('Broker not available'));
    }
    retry(this);
  });
  socket.on('end', function () {
    retry(this);
  });
  socket.buffer = new BufferList();
  socket.on('data', function (data) {
    socket.buffer.append(data);
    self.handleReceivedData(socket);
  });
  socket.setKeepAlive(true, 60000);

  function retry (s) {
    if (s.retrying || s.closing) return;
    s.retrying = true;
    s.retryTimer = setTimeout(function () {
      if (s.closing) return;
      self.reconnectBroker(s);
    }, 1000);
  }
  return new BrokerWrapper(socket, this.noAckBatchOptions);
};

module.exports = KafkaClient;
