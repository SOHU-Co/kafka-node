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
const protocolVersions = require('./protocol/protocolVersions');
const baseProtocolVersions = protocolVersions.baseSupport;
const apiMap = protocolVersions.apiMap;
const NestedError = require('nested-error-stacks');
const getCodec = require('./codec');

const DEFAULTS = {
  kafkaHost: 'localhost:9092',
  connectTimeout: 10000,
  requestTimeout: 30000,
  idleConnection: 5 * 60 * 1000,
  autoConnect: true,
  versions: {
    disabled: false,
    requestTimeout: 500
  },
  connectRetryOptions: {
    retries: 5,
    factor: 2,
    minTimeout: 1 * 1000,
    maxTimeout: 60 * 1000,
    randomize: true
  }
};

const KafkaClient = function (options) {
  this.options = _.defaultsDeep(options || {}, DEFAULTS);

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

  callback = _.once(callback);

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

KafkaClient.prototype.wrapTimeoutIfNeeded = function (socketId, correlationId, callback, overrideTimeout) {
  if (this.options.requestTimeout === false && overrideTimeout == null) {
    return callback;
  }

  const timeout = overrideTimeout || this.options.requestTimeout;

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
    callback(new TimeoutError(`Request timed out after ${timeout}ms`));
    callback = _.noop;
  }, timeout);

  wrappedFn.timeoutId = timeoutId;

  return wrappedFn;
};

KafkaClient.prototype.queueCallback = function (socket, id, data) {
  data[1] = this.wrapTimeoutIfNeeded(socket.socketId, id, data[1], data[2]);
  Client.prototype.queueCallback.call(this, socket, id, data);
};

KafkaClient.prototype.getApiVersions = function (broker, cb) {
  if (!broker || !broker.isConnected()) {
    return cb(new errors.BrokerNotAvailableError('Broker not available (getApiVersions)'));
  }

  logger.debug(`Sending versions request to ${broker.socket.addr}`);

  const correlationId = this.nextId();
  const request = protocol.encodeVersionsRequest(this.clientId, correlationId);

  this.queueCallback(broker.socket, correlationId, [
    protocol.decodeVersionsResponse,
    cb,
    this.options.versions.requestTimeout
  ]);
  broker.write(request);
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

KafkaClient.prototype.initializeBroker = function (broker, callback) {
  if (!broker || !broker.isConnected()) {
    return callback(new errors.BrokerNotAvailableError('Broker not available (initializeBroker)'));
  }

  if (this.options.versions.disabled || broker.socket.longpolling) {
    callback(null);
    return;
  }

  this.getApiVersions(broker, (error, versions) => {
    if (error) {
      if (error instanceof TimeoutError) {
        logger.debug('getApiVersions request timedout probably less than 0.10 using base support');
        versions = baseProtocolVersions;
      } else {
        logger.error('ApiVersions failed with unexpected error', error);
        callback(error);
        return;
      }
    } else {
      logger.debug(`Received versions response from ${broker.socket.addr}`);
    }
    logger.debug('setting api support to %j', versions);
    broker.apiSupport = versions;
    callback(null);
  });
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

      self.initializeBroker(brokerWrapper, function (error) {
        if (error) {
          logger.error('error initialize broker after reconnect', error);
        } else {
          const readyEventName = brokerWrapper.getReadyEventName();
          self.emit(readyEventName);
        }
        self.emit('reconnect');
      });
    } else {
      self.initializeBroker(brokerWrapper, function (error) {
        if (error) {
          logger.error('error initialize broker after connect', error);
        } else {
          const readyEventName = brokerWrapper.getReadyEventName();
          self.emit(readyEventName);
        }
        self.emit('connect');
      });
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

  const brokerWrapper = new BrokerWrapper(socket, this.noAckBatchOptions, this.options.idleConnection);

  function retry (s) {
    if (s.retrying || s.closing) return;
    s.retrying = true;
    s.retryTimer = setTimeout(function () {
      if (s.closing) return;
      if (brokerWrapper.isIdle()) {
        logger.debug(`${self.clientId} to ${socket.addr} is idle not reconnecting`);
        s.closing = true;
        self.deleteDisconnected(brokerWrapper);
        return;
      }
      logger.debug(`${self.clientId} reconnecting to ${s.addr}`);
      self.reconnectBroker(s);
    }, 1000);
  }
  return brokerWrapper;
};

KafkaClient.prototype.deleteDisconnected = function (broker) {
  if (!broker.isConnected()) {
    const brokers = this.getBrokers(broker.socket.longpolling);
    const key = broker.socket.addr;
    assert(brokers[key] === broker);
    delete brokers[key];
  }
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

const encodeMessageSet = protocol.encodeMessageSet;
const Message = protocol.Message;

function compress (payloads, callback) {
  async.each(payloads, buildRequest, callback);

  function buildRequest (payload, cb) {
    const attributes = payload.attributes;
    const codec = getCodec(attributes);

    if (!codec) return cb(null);

    const innerSet = encodeMessageSet(payload.messages, 1);
    codec.encode(innerSet, function (err, message) {
      if (err) return cb(err);
      payload.messages = [new Message(0, attributes, payload.key, message)];
      cb(null);
    });
  }
}

function getSupportedForRequestType (broker, requestType) {
  assert(!_.isEmpty(broker.apiSupport), 'apiSupport is empty');
  const usable = broker.apiSupport[requestType].usable;

  logger.debug(`Using V${usable} of ${requestType}`);
  const combo = apiMap[requestType][usable];
  return {
    encoder: combo[0],
    decoder: combo[1]
  };
}

KafkaClient.prototype.waitUntilReady = function (broker, callback) {
  let timeoutId = null;

  function onReady () {
    logger.debug('broker is now ready');
    clearTimeout(timeoutId);
    timeoutId = null;
    callback(null);
  }

  const timeout = this.options.requestTimeout;
  const readyEventName = broker.getReadyEventName();

  timeoutId = setTimeout(() => {
    this.removeListener(readyEventName, onReady);
    callback(new TimeoutError(`Request timed out after ${timeout}ms`));
  }, timeout);

  this.once(readyEventName, onReady);
};

KafkaClient.prototype.sendRequest = function (request, callback) {
  logger.debug('sending request');
  const payloads = this.payloadsByLeader(request.data.payloads);

  logger.debug('grouped requests by %d brokers %j', Object.keys(payloads).length, Object.keys(payloads));

  const sendToBroker = async.ensureAsync((payload, leader, callback) => {
    const correlationId = this.nextId();

    const broker = this.brokerForLeader(leader);
    if (!broker || !broker.isConnected()) {
      callback(new errors.BrokerNotAvailableError('Broker not available (sendRequest)'));
      return;
    }

    const coder = getSupportedForRequestType(broker, request.type);

    const encoder = request.data.args != null ? coder.encoder.apply(null, request.data.args) : coder.encoder;
    const decoder = coder.decoder;

    const requestData = encoder(this.clientId, correlationId, payload);

    if (request.data.requireAcks === 0) {
      broker.writeAsync(requestData);
      callback(null, { result: 'no ack' });
    } else {
      this.queueCallback(broker.socket, correlationId, [decoder, callback]);
      broker.write(requestData);
    }
  });

  const ensureBrokerReady = async.ensureAsync((leader, callback) => {
    const broker = this.brokerForLeader(leader);
    if (broker.apiSupport == null) {
      logger.debug('missing apiSupport waiting until broker is ready...');
      this.waitUntilReady(broker, callback);
    } else {
      logger.debug('has apiSupport broker is ready');
      callback(null);
    }
  });

  async.mapValues(
    payloads,
    function (payload, leader, callback) {
      async.series(
        [
          function (callback) {
            ensureBrokerReady(leader, callback);
          },
          function (callback) {
            sendToBroker(payload, leader, callback);
          }
        ],
        function (error, results) {
          if (error) {
            return callback(error);
          }
          callback(null, _.last(results));
        }
      );
    },
    callback
  );
};

KafkaClient.prototype.leaderLessPayloads = function (payloads) {
  return _.filter(payloads, payload => !this.hasMetadata(payload.topic, payload.partition));
};

KafkaClient.prototype.verifyPayloadsHasLeaders = function (payloads, callback) {
  logger.debug('checking payload topic/partitions has leaders');

  const leaderLessPayloads = this.leaderLessPayloads(payloads);

  if (leaderLessPayloads.length === 0) {
    logger.debug('found leaders for all');
    return callback(null);
  }
  logger.debug('payloads has no leaders! Our metadata could be out of date try refreshingMetadata', leaderLessPayloads);
  this.refreshMetadata(_.map(leaderLessPayloads, 'topic'), error => {
    if (error) {
      return callback(error);
    }
    const payloadWithMissingLeaders = this.leaderLessPayloads(payloads);
    if (payloadWithMissingLeaders.length) {
      logger.error('leaders are still missing for %j', payloadWithMissingLeaders);
      callback(new errors.BrokerNotAvailableError('Could not find the leader'));
    } else {
      callback(null);
    }
  });
};

KafkaClient.prototype.sendProduceRequest = function (payloads, requireAcks, ackTimeoutMs, callback) {
  async.series(
    [
      function (callback) {
        logger.debug('compressing messages if needed');
        compress(payloads, callback);
      },
      callback => {
        this.verifyPayloadsHasLeaders(payloads, callback);
      },
      callback => {
        const request = {
          type: 'produce',
          data: {
            payloads: payloads,
            args: [requireAcks, ackTimeoutMs],
            requireAcks: requireAcks
          }
        };
        this.sendRequest(request, callback);
      }
    ],
    (err, result) => {
      if (err) {
        if (err.message === 'NotLeaderForPartition') {
          this.emit('brokersChanged');
        }
        callback(err);
      } else {
        callback(null, _.chain(result).last().reduce((accu, value) => _.merge(accu, value), {}).value());
      }
    }
  );
};

module.exports = KafkaClient;
