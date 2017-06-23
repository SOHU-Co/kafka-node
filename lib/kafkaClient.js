'use strict';

const Client = require('./client');
const logger = require('./logging')('kafka-node:KafkaClient');
// const async = require('async');
// const retry = require('retry');
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

function hostParse (hostString) {
  const piece = hostString.split(':');
  return {
    host: piece[0],
    port: piece[1]
  };
}

KafkaClient.prototype.connect = function () {
  if (this.connecting) {
    logger.debug('connect request ignored. Client is currently connecting');
    return;
  }
  this.connecting = true;
  const broker = hostParse(this.options.kafkaHost);
  this.connectToBroker(broker, this.options.connectTimeout, (error) => {
    if (error) {
      return this.emit('error', error);
    }
    this.emit('ready');
  });
};

KafkaClient.prototype.connectToBroker = function (broker, timeout, callback) {
  logger.debug(`Trying to connect to host: ${broker.host} port: ${broker.port}`);
  let connectTimer = null;

  this.once('connect', () => {
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
  const brokerWrapper = this.setupBroker(broker.host, broker.port, false, this.brokers);
  const socket = brokerWrapper.socket;

  console.log('attach socket error handler');
  socket.on('error', (error) => {
    logger.debug('Socket Error', error);
    clearTimeout(connectTimer);
    this.closeBrokers(this.brokers);
    callback(error);
  });

  connectTimer = setTimeout(() => {
    logger.debug('Connection timeout error');
    this.closeBrokers(this.brokers);
    callback(new TimeoutError(`Connection timeout of ${timeout} exceeded`));
  }, timeout);
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
