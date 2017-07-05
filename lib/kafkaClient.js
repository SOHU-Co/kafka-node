'use strict';

const Client = require('./client');
const logger = require('./logging')('kafka-node:KafkaClient');
const async = require('async');
const retry = require('retry');
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
const protocol = require('./protocol');
const NestedError = require('nested-error-stacks');

const DEFAULTS = {
  kafkaHost: 'localhost:9092',
  connectTimeout: 10000,
  requestTimeout: 30000,
  autoConnect: true,
  connectRetryOptions: {
    retries: 5,
    factor: 2,
    minTimeout: 1 * 1000,
    maxTimeout: 60 * 1000,
    randomize: true
  }
};

const KafkaClient = function (options) {
  this.options = _.defaults(options || {}, DEFAULTS);

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

  const connect = retry.operation(this.options.connectRetryOptions);

  connect.attempt(currentAttempt => {
    if (this.closing) {
      logger.debug('Client is closing abort retry');
      connect.stop();
      return;
    }

    logger.debug(`Connect attempt ${currentAttempt}`);
    this.connectToBrokers(this.initialHosts, error => {
      if (connect.retry(error)) {
        return;
      }

      this.connecting = false;

      if (error) {
        logger.debug('exhausted retries. Main error', connect.mainError());
        this.emit('error', connect.mainError());
        return;
      }

      this.loadMetadataForTopics([], (error, result) => {
        if (error) {
          logger.debug('loadMetadataForTopics after connect failed', error);
          return this.emit('error', error);
        }
        this.updateMetadatas(result);
        this.ready = true;
        this.emit('ready');
      });
    });
  });
};

KafkaClient.prototype.connectToBrokers = function (hosts, callback) {
  assert(hosts && hosts.length, 'No hosts to connect to');
  hosts = _.shuffle(hosts);
  let index = 0;
  let errors = [];
  let broker = null;
  async.doWhilst(
    callback => {
      this.connectToBroker(hosts[index++], (error, connectedBroker) => {
        if (error) {
          logger.debug('failed to connect because of ', error);
          errors.push(error);
          callback(null);
          return;
        }
        errors.length = 0;
        broker = connectedBroker;
        callback(null);
      });
    },
    () => !this.closing && !broker && index < hosts.length,
    () => {
      if (broker) {
        return callback(null, broker);
      }

      if (errors.length) {
        callback(errors.pop());
      } else {
        callback(new Error('client is closing?'));
      }
    }
  );
};

KafkaClient.prototype.connectToBroker = function (broker, callback) {
  const timeout = this.options.connectTimeout;
  logger.debug(`Trying to connect to host: ${broker.host} port: ${broker.port}`);
  let connectTimer = null;

  const onError = error => {
    if (socket.closing) {
      return;
    }
    clearTimeout(connectTimer);
    connectTimer = null;
    socket.closing = true;
    socket.end();
    socket.destroy();
    socket.unref();
    const brokerKey = `${broker.host}:${broker.port}`;
    delete this.brokers[brokerKey];
    callback(error);
  };

  const brokerWrapper = this.setupBroker(broker.host, broker.port, false, this.brokers);
  const socket = brokerWrapper.socket;

  socket.once('connect', () => {
    logger.debug('broker socket connected %j', broker);
    clearTimeout(connectTimer);
    callback(null, brokerWrapper);
  });

  socket.on('error', function (error) {
    logger.debug('Socket Error', error);
    onError(error);
  });

  connectTimer = setTimeout(function () {
    logger.debug('Connection timeout error with broker %j', broker);
    onError(new TimeoutError(`Connection timeout of ${timeout}ms exceeded`));
  }, timeout);
};

KafkaClient.prototype.setupBroker = function (host, port, longpolling, brokers) {
  var brokerKey = host + ':' + port;
  brokers[brokerKey] = this.createBroker(host, port, longpolling);
  return brokers[brokerKey];
};

// returns a connected broker
KafkaClient.prototype.getAvailableBroker = function (callback) {
  const brokers = this.getBrokers();
  const connectedBrokers = _.filter(brokers, function (broker) {
    return broker.isConnected();
  });

  if (connectedBrokers.length) {
    logger.debug('found %d connected broker(s)', connectedBrokers.length);
    return callback(null, _.sample(connectedBrokers));
  }

  let brokersToTry;

  if (_.isEmpty(brokers)) {
    brokersToTry = _.values(this.brokerMetadata);
  } else {
    const badBrokers = Object.keys(brokers);
    brokersToTry = _.filter(this.brokerMetadata, function (broker) {
      return !_.includes(badBrokers, `${broker.host}:${broker.port}`);
    });
  }

  if (_.isEmpty(brokersToTry)) {
    return callback(new Error('Unable to find available brokers to try'));
  }

  this.connectToBrokers(brokersToTry, callback);
};

KafkaClient.prototype.refreshBrokers = function () {
  var self = this;
  var validBrokers = _.map(this.brokerMetadata, function (broker) {
    return `${broker.host}:${broker.port}`;
  });

  function closeDeadBrokers (brokers) {
    var deadBrokerKeys = _.difference(Object.keys(brokers), validBrokers);
    if (deadBrokerKeys.length) {
      self.closeBrokers(
        deadBrokerKeys.map(function (key) {
          var broker = brokers[key];
          delete brokers[key];
          return broker;
        })
      );
    }
  }

  closeDeadBrokers(this.brokers);
  closeDeadBrokers(this.longpollingBrokers);
};

KafkaClient.prototype.refreshBrokerMetadata = function (callback) {
  if (this.refreshingMetadata) {
    return;
  }

  if (callback == null) {
    callback = _.noop;
  }

  this.refreshingMetadata = true;

  async.waterfall(
    [callback => this.getAvailableBroker(callback), (broker, callback) => this.loadMetadataFrom(broker, callback)],
    (error, result) => {
      this.refreshingMetadata = false;
      if (error) {
        callback(error);
        return this.emit('error', new NestedError('refreshBrokerMetadata failed', error));
      }
      this.updateMetadatas(result);
      this.refreshBrokers();
      callback(error);
    }
  );
};

Client.prototype.loadMetadataFrom = function (broker, cb) {
  assert(broker && broker.isConnected());
  var correlationId = this.nextId();
  var request = protocol.encodeMetadataRequest(this.clientId, correlationId, []);

  this.queueCallback(broker.socket, correlationId, [protocol.decodeMetadataResponse, cb]);
  broker.write(request);
};

KafkaClient.prototype.setBrokerMetadata = function (brokerMetadata) {
  assert(brokerMetadata, 'brokerMetadata is empty');
  const oldBrokerMetadata = this.brokerMetadata;
  this.brokerMetadata = brokerMetadata;
  this.brokerMetadataLastUpdate = Date.now();

  if (!_.isEmpty(oldBrokerMetadata) && !_.isEqual(oldBrokerMetadata, brokerMetadata)) {
    setImmediate(() => this.emit('brokersChanged'));
  }
};

KafkaClient.prototype.updateMetadatas = function (metadatas) {
  assert(metadatas && Array.isArray(metadatas) && metadatas.length === 2, 'metadata format is incorrect');
  logger.debug('updating metadatas');
  this.setBrokerMetadata(metadatas[0]);
  this.topicMetadata = metadatas[1].metadata;
};

KafkaClient.prototype.brokerForLeader = function (leader, longpolling) {
  var addr;
  var brokers = this.getBrokers(longpolling);
  // If leader is not give, choose the first broker as leader
  if (typeof leader === 'undefined') {
    if (!_.isEmpty(brokers)) {
      addr = Object.keys(brokers)[0];
      return brokers[addr];
    } else if (!_.isEmpty(this.brokerMetadata)) {
      leader = Object.keys(this.brokerMetadata)[0];
    } else {
      return;
    }
  }

  var broker = this.brokerMetadata[leader];

  if (!broker) {
    return;
  }

  addr = broker.host + ':' + broker.port;

  return brokers[addr] || this.setupBroker(broker.host, broker.port, longpolling, brokers);
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

  wrappedFn.timeoutId = timeoutId;

  return wrappedFn;
};

KafkaClient.prototype.queueCallback = function (socket, id, data) {
  data[1] = this.wrapTimeoutIfNeeded(socket.socketId, id, data[1]);
  Client.prototype.queueCallback.call(this, socket, id, data);
};

KafkaClient.prototype.close = function (callback) {
  logger.debug('close client');
  this.closing = true;
  this.closeBrokers(this.brokers);
  this.closeBrokers(this.longpollingBrokers);
  if (callback) {
    setImmediate(function () {
      callback(null);
    });
  }
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
    if (hadError) {
      self.clearCallbackQueue(
        this,
        this.error != null ? this.error : new errors.BrokerNotAvailableError('Broker not available')
      );
    } else {
      logger.debug(`clearing ${this.addr} callback queue without error`);
      self.clearCallbackQueue(this);
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
      logger.debug('reconnecting broker');
      self.reconnectBroker(s);
    }, 1000);
  }
  return new BrokerWrapper(socket, this.noAckBatchOptions);
};

KafkaClient.prototype.clearCallbackQueue = function (socket, error) {
  const socketId = socket.socketId;
  const longpolling = socket.longpolling;

  if (!this.cbqueue.hasOwnProperty(socketId)) {
    return;
  }

  const queue = this.cbqueue[socketId];

  if (!longpolling) {
    Object.keys(queue).forEach(function (key) {
      const handlers = queue[key];
      const cb = handlers[1];
      if (error) {
        cb(error);
      } else if (cb.timeoutId != null) {
        clearTimeout(cb.timeoutId);
      }
    });
  }
  delete this.cbqueue[socketId];
};

KafkaClient.prototype.topicExists = function (topics, callback) {
  this.loadMetadataForTopics([], (error, response) => {
    if (error) {
      return callback(error);
    }
    this.updateMetadatas(response);
    const missingTopics = _.difference(topics, Object.keys(this.topicMetadata));
    if (missingTopics.length === 0) {
      return callback(null);
    }
    callback(new errors.TopicsNotExistError(missingTopics));
  });
};

module.exports = KafkaClient;
