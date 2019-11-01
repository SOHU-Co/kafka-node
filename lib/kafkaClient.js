'use strict';

const Client = require('./baseClient');
const logger = require('./logging')('kafka-node:KafkaClient');
const EventEmitter = require('events');
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
const NotControllerError = require('./errors/NotControllerError');
const protocol = require('./protocol');
const protocolVersions = require('./protocol/protocolVersions');
const baseProtocolVersions = protocolVersions.baseSupport;
const apiMap = protocolVersions.apiMap;
const NestedError = require('nested-error-stacks');
const getCodec = require('./codec');
const resourceTypeMap = require('./resources').resourceTypeMap;

const DEFAULTS = {
  kafkaHost: 'localhost:9092',
  connectTimeout: 10000,
  requestTimeout: 30000,
  idleConnection: 5 * 60 * 1000,
  reconnectOnIdle: true,
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
  },
  maxAsyncRequests: 10,
  noAckBatchOptions: null
};

const KafkaClient = function (options) {
  EventEmitter.call(this); // Intentionally not calling Client to avoid constructor logic
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
  this.noAckBatchOptions = this.options.noAckBatchOptions;
  this.brokers = {};
  this.longpollingBrokers = {};
  this.topicMetadata = {};
  this.correlationId = 0;
  this._socketId = 0;
  /**
   * @type {Map<any, Map<any, any>>}
   */
  this.cbqueue = new Map();
  this.brokerMetadata = {};
  this.clusterMetadata = {};
  this.ready = false;
  this._timeouts = new Set();

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
  const ip = hostString.substring(0, hostString.lastIndexOf(':'));
  const port = +hostString.substring(hostString.lastIndexOf(':') + 1);
  const isIpv6 = ip.match(/\[(.*)\]/);
  const host = isIpv6 ? isIpv6[1] : ip;
  return {
    host,
    port
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

    async.series(
      [
        callback => {
          this.connectToBrokers(this.initialHosts, callback);
        },

        callback => {
          logger.debug('connected to socket, trying to load initial metadata');
          this.loadMetadataForTopics([], (error, result) => {
            if (error) {
              logger.debug('loadMetadataForTopics after connect failed', error);
              return callback(error);
            }
            this.updateMetadatas(result, true);
            callback(null);
          });
        }
      ],
      error => {
        if (connect.retry(error)) {
          return;
        }

        this.connecting = false;

        if (error) {
          logger.debug('exhausted retries. Main error', connect.mainError());
          this.emit('error', connect.mainError());
          return;
        }

        this.ready = true;
        this.emit('ready');
      }
    );
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
      if (this.closing) {
        return callback(new Error('client is closing'));
      }

      if (broker) {
        return callback(null, broker);
      }

      if (errors.length) {
        callback(errors.pop());
      } else {
        callback(new Error('failed to connect to brokers'));
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
    this._clearTimeout(connectTimer);
    callback(null, brokerWrapper);
  });

  socket.on('error', function (error) {
    logger.debug('Socket Error', error);
    onError(error);
  });

  connectTimer = this._createTimeout(function () {
    logger.debug('Connection timeout error with broker %j', broker);
    onError(new TimeoutError(`Connection timeout of ${timeout}ms exceeded`));
  }, timeout);
};

KafkaClient.prototype.getController = function (callback) {
  // Check for cached controller
  if (this.clusterMetadata.controllerId != null) {
    var controller = this.brokerMetadata[this.clusterMetadata.controllerId];
    var broker = this.getBroker(controller.host, controller.port);

    return callback(null, broker, this.clusterMetadata.controllerId);
  }

  // If cached controller is not available, refresh metadata
  this.loadMetadata((error, result) => {
    if (error) {
      return callback(error);
    }

    // No controller will be available if api version request timed out, or if kafka version is less than 0.10.
    if (!result[1].clusterMetadata || result[1].clusterMetadata.controllerId == null) {
      return callback(new errors.BrokerNotAvailableError('Controller broker not available'));
    }

    this.updateMetadatas(result);

    var controllerId = result[1].clusterMetadata.controllerId;
    var controllerMetadata = result[0][controllerId];

    var broker = this.getBroker(controllerMetadata.host, controllerMetadata.port);

    if (!broker || !broker.isConnected()) {
      this.refreshBrokerMetadata();
      return callback(new errors.BrokerNotAvailableError('Controller broker not available'));
    }

    return callback(null, broker, this.clusterMetadata.controllerId);
  });
};

KafkaClient.prototype.getBroker = function (host, port, longpolling) {
  const brokers = this.getBrokers(longpolling);

  var addr = host + ':' + port;
  return brokers[addr] || this.setupBroker(host, port, longpolling, brokers);
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
  if (this.refreshingMetadata || this.closing) {
    return;
  }

  if (callback == null) {
    callback = _.noop;
  }

  this.refreshingMetadata = true;

  logger.debug(`${this.clientId} refreshBrokerMetadata()`);

  async.waterfall(
    [callback => this.getAvailableBroker(callback), (broker, callback) => this.loadMetadataFrom(broker, callback)],
    (error, result) => {
      this.refreshingMetadata = false;
      if (error) {
        callback(error);
        return this.emit('error', new NestedError('refreshBrokerMetadata failed', error));
      }
      this.updateMetadatas(result, true);
      this.refreshBrokers();
      callback(error);
    }
  );
};

KafkaClient.prototype.loadMetadataFrom = function (broker, cb) {
  assert(broker && broker.isConnected());
  var correlationId = this.nextId();
  var request = protocol.encodeMetadataRequest(this.clientId, correlationId, []);

  this.sendWhenReady(broker, correlationId, request, protocol.decodeMetadataResponse, cb);
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

KafkaClient.prototype.setClusterMetadata = function (clusterMetadata) {
  assert(clusterMetadata, 'clusterMetadata is empty');
  this.clusterMetadata = clusterMetadata;
};

KafkaClient.prototype.setControllerId = function (controllerId) {
  if (!this.clusterMetadata) {
    this.clusterMetadata = {
      controllerId
    };

    return;
  }
  this.clusterMetadata.controllerId = controllerId;
};

KafkaClient.prototype.updateMetadatas = function (metadatas, replaceTopicMetadata) {
  assert(metadatas && Array.isArray(metadatas) && metadatas.length === 2, 'metadata format is incorrect');
  this.setBrokerMetadata(metadatas[0]);
  if (replaceTopicMetadata) {
    this.topicMetadata = metadatas[1].metadata;
  } else {
    _.extend(this.topicMetadata, metadatas[1].metadata);
  }

  if (metadatas[1].clusterMetadata) {
    this.setClusterMetadata(metadatas[1].clusterMetadata);
  }
  logger.debug(`${this.clientId} updated internal metadata`);
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

  return (
    brokers[addr] ||
    this.setupBroker(broker.host, broker.port, longpolling, brokers, err => {
      if (err) {
        this.emit('error', err);
      }
    })
  );
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

  logger.debug(`${this.clientId} sending versions request to ${broker.socket.addr}`);

  const correlationId = this.nextId();
  const request = protocol.encodeVersionsRequest(this.clientId, correlationId);

  this.queueCallback(broker.socket, correlationId, [
    protocol.decodeVersionsResponse,
    cb,
    this.options.versions.requestTimeout
  ]);
  broker.write(request);
};

KafkaClient.prototype.getListGroups = function (callback) {
  if (!this.ready) {
    return callback(new Error('Client is not ready (getListGroups)'));
  }
  const brokers = this.brokerMetadata;
  async.mapValuesLimit(
    brokers,
    this.options.maxAsyncRequests,
    (brokerMetadata, brokerId, cb) => {
      const broker = this.brokerForLeader(brokerId);
      if (!broker || !broker.isConnected()) {
        return cb(new errors.BrokerNotAvailableError('Broker not available (getListGroups)'));
      }

      const correlationId = this.nextId();
      const request = protocol.encodeListGroups(this.clientId, correlationId);
      this.sendWhenReady(broker, correlationId, request, protocol.decodeListGroups, cb);
    },
    (err, results) => {
      if (err) {
        callback(err);
        return;
      }
      results = _.values(results);
      callback(null, _.merge.apply({}, results));
    }
  );
};

KafkaClient.prototype.getDescribeGroups = function (groups, callback) {
  if (!this.ready) {
    return callback(new Error('Client is not ready (getDescribeGroups)'));
  }

  async.groupByLimit(
    groups,
    this.options.maxAsyncRequests,
    (group, cb) => {
      this.sendGroupCoordinatorRequest(group, (err, coordinator) => {
        cb(err || null, coordinator ? coordinator.coordinatorId : undefined);
      });
    },
    (err, results) => {
      if (err) {
        callback(err);
        return;
      }

      async.mapValuesLimit(
        results,
        this.options.maxAsyncRequests,
        (groups, coordinator, cb) => {
          const broker = this.brokerForLeader(coordinator);
          if (!broker || !broker.isConnected()) {
            return cb(new errors.BrokerNotAvailableError('Broker not available (getDescribeGroups)'));
          }

          const correlationId = this.nextId();
          const request = protocol.encodeDescribeGroups(this.clientId, correlationId, groups);
          this.sendWhenReady(broker, correlationId, request, protocol.decodeDescribeGroups, cb);
        },
        (err, res) => {
          if (err) {
            return callback(err);
          }

          callback(
            null,
            _.reduce(
              res,
              (result, describes, broker) => {
                _.each(describes, (values, consumer) => {
                  result[consumer] = values;
                  result[consumer].brokerId = broker;
                });
                return result;
              },
              {}
            )
          );
        }
      );
    }
  );
};

KafkaClient.prototype.close = function (callback) {
  logger.debug('close client');
  this.closing = true;
  this.closeBrokers(this.brokers);
  this.closeBrokers(this.longpollingBrokers);
  this._clearAllTimeouts();
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

  if (this.options.versions.disabled) {
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

    if (_.isEmpty(versions)) {
      return callback(new Error(`getApiVersions response was empty for broker: ${broker}`));
    }

    logger.debug('setting api support to %j', versions);
    broker.apiSupport = versions;

    if (this.options.sasl) {
      this.saslAuth(broker, err => {
        if (err) {
          return callback(err);
        }
        callback(null);
      });
    } else {
      callback(null);
    }
  });
};

KafkaClient.prototype.saslAuth = function (broker, callback) {
  const mechanism = this.options.sasl.mechanism.toUpperCase();
  const apiVersion = broker.apiSupport ? broker.apiSupport.saslHandshake.usable : undefined;
  if (typeof apiVersion !== 'number') {
    callback(new errors.SaslAuthenticationError(null, 'Broker does not support SASL authentication'));
    return;
  }

  async.waterfall(
    [
      callback => {
        logger.debug(`Sending SASL/${mechanism} handshake request to ${broker}`);

        const correlationId = this.nextId();
        const request = protocol.encodeSaslHandshakeRequest(this.clientId, correlationId, apiVersion, mechanism);

        this.queueCallback(broker.socket, correlationId, [protocol.decodeSaslHandshakeResponse, callback]);
        broker.write(request);
      },
      (enabledMechanisms, callback) => {
        logger.debug(`Sending SASL/${mechanism} authentication request to ${broker.socket.addr}`);

        const auth = this.options.sasl;
        const correlationId = this.nextId();
        const request = protocol.encodeSaslAuthenticateRequest(this.clientId, correlationId, apiVersion, auth);

        let decode = protocol.decodeSaslAuthenticateResponse;
        if (apiVersion === 0) {
          decode = _.identity;
          broker.socket.saslAuthCorrelationId = correlationId;
        }
        this.queueCallback(broker.socket, correlationId, [decode, callback]);
        broker.write(request);
      }
    ],
    (error, authBytes) => {
      if (!error) {
        broker.authenticated = true;
      }

      // TODO do stuff with authBytes
      callback(error);
    }
  );
};

KafkaClient.prototype.createBroker = function (host, port, longpolling) {
  logger.debug(`${this.clientId} createBroker ${host}:${port}`);
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
          logger.error('error initializing broker after reconnect', error);
          return;
        }
        const readyEventName = brokerWrapper.getReadyEventName();
        self.emit(readyEventName);
        self.emit('reconnect');
      });
    } else {
      self.initializeBroker(brokerWrapper, function (error) {
        if (error) {
          logger.error('error initializing broker after connect', error);
          if (error instanceof errors.SaslAuthenticationError) {
            self.emit('error', error);
          }
          return;
        }

        const readyEventName = brokerWrapper.getReadyEventName();
        self.emit(readyEventName);
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
    logger.debug(`${self.clientId} socket closed ${this.addr} (hadError: ${hadError})`);
    if (!hadError && self.closing) {
      logger.debug(`clearing ${this.addr} callback queue without error`);
      self.clearCallbackQueue(this);
    } else {
      let error = this.error;
      if (!error) {
        if (self.options.sasl && socket.saslAuthCorrelationId !== undefined) {
          delete socket.saslAuthCorrelationId;
          const message = 'Broker closed connection during SASL auth: bad credentials?';
          error = new errors.SaslAuthenticationError(null, message);
        } else {
          error = new errors.BrokerNotAvailableError('Broker not available (socket closed)');
          if (!self.connecting && !brokerWrapper.isIdle()) {
            logger.debug(`${self.clientId} schedule refreshBrokerMetadata()`);
            setImmediate(function () {
              self.refreshBrokerMetadata();
            });
          }
        }
      }
      self.clearCallbackQueue(this, error);
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

  const brokerWrapper = new BrokerWrapper(
    socket,
    this.noAckBatchOptions,
    this.options.idleConnection,
    this.options.sasl
  );

  function retry (s) {
    if (s.retrying || s.closing) return;
    s.retrying = true;
    s.retryTimer = setTimeout(function () {
      if (s.closing) return;
      if (!self.options.reconnectOnIdle && brokerWrapper.isIdle()) {
        logger.debug(`${self.clientId} to ${socket.addr} is idle not reconnecting`);
        s.closing = true;
        self.deleteDisconnected(brokerWrapper);
        return;
      }

      if (!self.isValidBroker(s)) {
        logger.debug(`${self.clientId} is not reconnecting to ${s.addr} invalid broker`);
        return;
      }

      logger.debug(`${self.clientId} reconnecting to ${s.addr}`);
      self.reconnectBroker(s);
    }, 1000);
  }
  return brokerWrapper;
};

KafkaClient.prototype.isValidBroker = function ({ host, port }) {
  return (
    this.connecting ||
    _(this.brokerMetadata)
      .values()
      .some({ host, port })
  );
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
  const queue = this.cbqueue.get(socketId);
  if (!queue) {
    return;
  }

  queue.forEach(function (handlers) {
    const cb = handlers[1];
    if (error) {
      cb(error);
    } else if (cb.timeoutId != null) {
      clearTimeout(cb.timeoutId);
    }
  });

  this.cbqueue.delete(socketId);
};

/**
 * Fetches metadata for brokers and cluster.
 * This includes an array containing each node (id, host and port).
 * Depending on kafka version, additional cluster information is available (controller id).
 * @param {loadMetadataCallback} cb Function to call once metadata is loaded.
 */
KafkaClient.prototype.loadMetadata = function (callback) {
  this.loadMetadataForTopics(null, callback);
};

/**
 * Fetches metadata for brokers and cluster.
 * This includes an array containing each node (id, host and port). As well as an object
 * containing the topic name, partition, leader number, replica count, and in sync replicas per partition.
 * Depending on kafka version, additional cluster information is available (controller id).
 * @param {Array} topics List of topics to fetch metadata for. An empty array ([]) will fetch all topics.
 * @param {loadMetadataCallback} callback Function to call once metadata is loaded.
 */
KafkaClient.prototype.loadMetadataForTopics = function (topics, callback) {
  const broker = this.brokerForLeader();

  if (!broker || !broker.isConnected()) {
    return callback(new errors.BrokerNotAvailableError('Broker not available (loadMetadataForTopics)'));
  }

  const ensureBrokerReady = (broker, cb) => {
    if (!broker.isReady()) {
      logger.debug('missing apiSupport waiting until broker is ready...(loadMetadataForTopics)');
      this.waitUntilReady(broker, cb);
    } else {
      cb(null);
    }
  };

  async.series(
    [
      cb => {
        ensureBrokerReady(broker, cb);
      },
      cb => {
        const broker = this.brokerForLeader();
        const correlationId = this.nextId();
        const supportedCoders = getSupportedForRequestType(broker, 'metadata');
        const request = supportedCoders.encoder(this.clientId, correlationId, topics);

        this.queueCallback(broker.socket, correlationId, [supportedCoders.decoder, cb]);
        broker.write(request);
      }
    ],
    (err, result) => {
      callback(err, result[1]);
    }
  );
};

/**
 * Creates one or more topics.
 * @param {Array} topics Array of topics with partition and replication factor to create.
 * @param {createTopicsCallback} callback Function to call once operation is completed.
 */
KafkaClient.prototype.createTopics = function (topics, callback) {
  // Calls with [string, string, ...] are forwarded to support previous versions
  if (topics.every(t => typeof t === 'string')) {
    return Client.prototype.createTopics.apply(this, arguments);
  }

  this.sendControllerRequest('createTopics', [topics, this.options.requestTimeout], callback);
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
  const brokerSupport = broker.apiSupport[requestType];

  if (!brokerSupport) {
    return null;
  }

  const usable = brokerSupport.usable;

  const combo = apiMap[requestType][usable];
  return {
    encoder: combo[0],
    decoder: combo[1]
  };
}

KafkaClient.prototype.waitUntilReady = function (broker, callback) {
  logger.debug('waitUntilReady ' + broker);
  let timeoutId = null;

  const onReady = () => {
    logger.debug('broker is now ready');

    if (timeoutId !== null) {
      this._clearTimeout(timeoutId);
      timeoutId = null;
    }

    callback(null);
  };

  const timeout = this.options.requestTimeout;
  const readyEventName = broker.getReadyEventName();

  if (timeout !== false) {
    timeoutId = this._createTimeout(() => {
      this.removeListener(readyEventName, onReady);
      this._timeouts.delete(timeoutId);
      callback(new TimeoutError(`Request timed out after ${timeout}ms`));
    }, timeout);
  }

  this.once(readyEventName, onReady);
};

KafkaClient.prototype._clearTimeout = function (timeoutId) {
  clearTimeout(timeoutId);
  this._timeouts.delete(timeoutId);
};

KafkaClient.prototype._clearAllTimeouts = function () {
  this._timeouts.forEach(function (timeoutId) {
    clearTimeout(timeoutId);
  });

  this._timeouts.clear();
};

KafkaClient.prototype._createTimeout = function (fn, timeout) {
  const timeoutId = setTimeout(fn, timeout);
  this._timeouts.add(timeoutId);
  return timeoutId;
};

KafkaClient.prototype.sendRequest = function (request, callback) {
  const payloads = this.payloadsByLeader(request.data.payloads);
  const longpolling = request.longpolling;

  const sendToBroker = async.ensureAsync((payload, leader, callback) => {
    const broker = this.brokerForLeader(leader, longpolling);
    if (!broker || !broker.isConnected()) {
      this.refreshBrokerMetadata();
      callback(new errors.BrokerNotAvailableError('Broker not available (sendRequest)'));
      return;
    }

    if (!broker.isReady()) {
      callback(new Error('Broker is not ready'));
      return;
    }

    if (longpolling) {
      if (broker.socket.waiting) {
        callback(null);
        return;
      }
      broker.socket.waiting = true;
    }

    const correlationId = this.nextId();
    const coder = getSupportedForRequestType(broker, request.type);

    const encoder = request.data.args != null ? coder.encoder.apply(null, request.data.args) : coder.encoder;
    const decoder =
      request.data.decoderArgs != null ? coder.decoder.apply(null, request.data.decoderArgs) : coder.decoder;

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
    const broker = this.brokerForLeader(leader, longpolling);
    if (!broker.isConnected()) {
      this.refreshBrokerMetadata();
      callback(new errors.BrokerNotAvailableError('Broker not available (sendRequest -> ensureBrokerReady)'));
      return;
    }
    if (!broker.isReady()) {
      logger.debug(`missing apiSupport waiting until broker is ready... (sendRequest ${request.type})`);
      this.waitUntilReady(broker, callback);
    } else {
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

/**
 * Sends a request to a specific broker by id
 */
KafkaClient.prototype.sendRequestToBroker = function (brokerId, requestType, args, callback) {
  const brokerMetadata = this.brokerMetadata[brokerId];

  if (!brokerMetadata) {
    return callback(new Error('No broker with id ' + brokerId));
  }

  const broker = this.getBroker(brokerMetadata.host, brokerMetadata.port);

  async.waterfall(
    [
      callback => {
        if (broker.isReady()) {
          return callback(null, broker);
        }

        this.waitUntilReady(broker, error => {
          callback(error, broker);
        });
      }
    ],
    (error, result) => {
      if (error) {
        return callback(error);
      }

      const broker = this.getBroker(brokerMetadata.host, brokerMetadata.port);
      const correlationId = this.nextId();
      const coder = getSupportedForRequestType(broker, requestType);

      if (!coder) {
        return callback(new errors.ApiNotSupportedError());
      }

      args.unshift(this.clientId, correlationId);
      const encoder = coder.encoder;
      const decoder = coder.decoder;
      const request = encoder.apply(null, args);

      this.sendWhenReady(broker, correlationId, request, decoder, callback);
    }
  );
};

KafkaClient.prototype.leaderLessPayloads = function (payloads) {
  return _.filter(payloads, payload => !this.hasMetadata(payload.topic, payload.partition));
};

KafkaClient.prototype.verifyPayloadsHasLeaders = function (payloads, callback) {
  const leaderLessPayloads = this.leaderLessPayloads(payloads);

  if (leaderLessPayloads.length === 0) {
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

KafkaClient.prototype.wrapControllerCheckIfNeeded = function (requestType, requestArgs, callback) {
  if (callback.isControllerWrapper) {
    return callback;
  }

  var hasBeenInvoked = false;

  const wrappedCallback = (error, result) => {
    if (error instanceof NotControllerError) {
      this.setControllerId(null);

      if (!hasBeenInvoked) {
        hasBeenInvoked = true;
        this.sendControllerRequest(requestType, requestArgs, wrappedCallback);
        return;
      }
    }

    callback(error, result);
  };

  wrappedCallback.isControllerWrapper = true;

  return wrappedCallback;
};

KafkaClient.prototype.sendControllerRequest = function (requestType, args, callback) {
  this.getController((error, controller, controllerId) => {
    if (error) {
      return callback(error);
    }

    const originalArgs = _.clone(args);
    const originalCallback = callback;
    callback = this.wrapControllerCheckIfNeeded(requestType, originalArgs, originalCallback);

    this.sendRequestToBroker(controllerId, requestType, args, callback);
  });
};

KafkaClient.prototype.sendFetchRequest = function (
  consumer,
  payloads,
  fetchMaxWaitMs,
  fetchMinBytes,
  maxTickMessages,
  callback
) {
  const memberId = consumer.memberId;
  const generationId = consumer.generationId;

  if (memberId == null && generationId == null) {
    Client.prototype.sendFetchRequest.apply(this, arguments);
    return;
  }

  payloads = _.cloneDeep(payloads);
  function stateValidator (unused, type, message) {
    const payloadMap = consumer.payloadMap;
    if (
      consumer.closing ||
      consumer.connecting ||
      consumer.rebalancing ||
      consumer.memberId !== memberId ||
      consumer.generationId !== generationId
    ) {
      logger.error(
        'ignoring message due to it being from an old group - memberId: ' + memberId,
        '!=' + consumer.memberId + ' - generationId: ' + generationId + '!=' + consumer.generationId
      );
      return false;
    }
    if (type === 'message') {
      const { topic, partition, offset } = message;
      if (!payloadMap[topic] || payloadMap[topic][partition] == null) {
        logger.error('received unexpected message', message, payloadMap);
        // We should have never received this in the first place
        return false;
      }

      if (offset == null || offset < payloadMap[topic][partition]) {
        // Kafka may send an older message than we expect (compressed messages, and other unknown reasons)
        return false;
      }
    }

    return true;
  }
  if (callback == null) {
    callback = _.noop;
  }

  async.series(
    [
      callback => {
        this.verifyPayloadsHasLeaders(payloads, callback);
      },
      callback => {
        const request = {
          type: 'fetch',
          longpolling: true,
          data: {
            payloads: payloads,
            args: [fetchMaxWaitMs, fetchMinBytes],
            decoderArgs: [this._createMessageHandler(consumer, stateValidator), maxTickMessages]
          }
        };

        this.sendRequest(request, callback);
      }
    ],
    callback
  );
};

KafkaClient.prototype.sendWhenReady = function (broker, correlationId, request, decode, cb) {
  const doSend = () => {
    this.queueCallback(broker.socket, correlationId, [decode, cb]);
    broker.write(request);
  };
  if (!broker.isReady()) {
    this.waitUntilReady(broker, doSend);
  } else {
    doSend();
  }
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
        if (err.message === 'NotLeaderForPartition' || err.message === 'UnknownTopicOrPartition') {
          this.emit('brokersChanged');
        }
        callback(err);
      } else {
        callback(
          null,
          _.chain(result)
            .last()
            .reduce((accu, value) => _.merge(accu, value), {})
            .value()
        );
      }
    }
  );
};

KafkaClient.prototype.handleReceivedData = function (socket) {
  if (socket.saslAuthCorrelationId !== undefined) {
    if (socket.buffer.length < 4) {
      // not enough data yet
      return;
    }

    const size = socket.buffer.readInt32BE(0);
    if (socket.buffer.length - 4 < size) {
      // still not enough data
      return;
    }

    const resp = socket.buffer.slice(4, 4 + size);
    this.invokeResponseCallback(socket, socket.saslAuthCorrelationId, resp);
    delete socket.saslAuthCorrelationId;
    socket.buffer.consume(size + 4);
  } else {
    return Client.prototype.handleReceivedData.call(this, socket);
  }
};

KafkaClient.prototype.describeConfigs = function (payload, callback) {
  if (!this.ready) {
    return callback(new Error('Client is not ready (describeConfigs)'));
  }
  let err;

  // Broker resource requests must go to the specific node
  // other requests can go to any node
  const brokerResourceRequests = [];
  const nonBrokerResourceRequests = [];

  _.forEach(payload.resources, function (resource) {
    if (resourceTypeMap[resource.resourceType] === undefined) {
      err = new Error(`Unexpected resource type ${resource.resourceType} for resource ${resource.resourceName}`);
      return false;
    } else {
      resource.resourceType = resourceTypeMap[resource.resourceType];
    }

    if (resource.resourceType === resourceTypeMap['broker']) {
      brokerResourceRequests.push(resource);
    } else {
      nonBrokerResourceRequests.push(resource);
    }
  });

  if (err) {
    return callback(err);
  }

  async.parallelLimit([
    (cb) => {
      if (nonBrokerResourceRequests.length > 0) {
        this.sendRequestToAnyBroker('describeConfigs', [{ resources: nonBrokerResourceRequests, includeSynonyms: payload.includeSynonyms }], cb);
      } else {
        cb(null, []);
      }
    },
    ...brokerResourceRequests.map(r => {
      return (cb) => {
        this.sendRequestToBroker(r.resourceName, 'describeConfigs', [{ resources: [r], includeSynonyms: payload.includeSynonyms }], cb);
      };
    })
  ], this.options.maxAsyncRequests, (err, result) => {
    if (err) {
      return callback(err);
    }

    callback(null, _.flatten(result));
  });
};

/**
 * Sends a request to any broker in the cluster
 */
KafkaClient.prototype.sendRequestToAnyBroker = function (requestType, args, callback) {
  // For now just select the first broker
  const brokerId = Object.keys(this.brokerMetadata)[0];
  this.sendRequestToBroker(brokerId, requestType, args, callback);
};

module.exports = KafkaClient;
