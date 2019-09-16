'use strict';

var util = require('util');
var _ = require('lodash');
var async = require('async');
var retry = require('retry');
var EventEmitter = require('events');
var errors = require('./errors');
var getCodec = require('./codec');
var protocol = require('./protocol');
var encodeMessageSet = protocol.encodeMessageSet;
var Message = protocol.Message;
var logger = require('./logging')('kafka-node:BaseClient');
var validateKafkaTopics = require('./utils').validateTopicNames;

const MAX_INT32 = 2147483647;

/**
 *
 * @constructor
 */
function Client () {
  throw new TypeError('BaseClient cannot be instantiated directly');
}

util.inherits(Client, EventEmitter);

Client.prototype.closeBrokers = function (brokers) {
  _.each(brokers, function (broker) {
    broker.socket.closing = true;
    broker.socket.end();
    setImmediate(function () {
      broker.socket.destroy();
      broker.socket.unref();
    });
  });
};

function decodeValue (encoding, value) {
  if (encoding !== 'buffer' && value != null) {
    return value.toString(encoding);
  }
  return value;
}

Client.prototype._createMessageHandler = function (consumer, stateValidator) {
  return (err, type, message) => {
    if (stateValidator && !stateValidator(err, type, message)) {
      return;
    }
    if (err) {
      if (err.message === 'OffsetOutOfRange') {
        return consumer.emit('offsetOutOfRange', err);
      } else if (err.message === 'NotLeaderForPartition' || err.message === 'UnknownTopicOrPartition') {
        return this.emit('brokersChanged');
      }

      return consumer.emit('error', err);
    }

    var encoding = consumer.options.encoding;
    const keyEncoding = consumer.options.keyEncoding;

    if (type === 'message') {
      message.value = decodeValue(encoding, message.value);
      message.key = decodeValue(keyEncoding || encoding, message.key);

      consumer.emit('message', message);
    } else {
      consumer.emit(type, message);
    }
  };
};

Client.prototype.sendFetchRequest = function (consumer, payloads, fetchMaxWaitMs, fetchMinBytes, maxTickMessages) {
  var encoder = protocol.encodeFetchRequest(fetchMaxWaitMs, fetchMinBytes);
  // TODO: state validator for HLC for ignoring stale fetch requests
  var decoder = protocol.decodeFetchResponse(this._createMessageHandler(consumer), maxTickMessages);

  this.send(payloads, encoder, decoder, function (err) {
    if (err) {
      Array.prototype.unshift.call(arguments, 'error');
      consumer.emit.apply(consumer, arguments);
    }
  });
};

Client.prototype.sendProduceRequest = function (payloads, requireAcks, ackTimeoutMs, cb) {
  var encoder = protocol.encodeProduceRequest(requireAcks, ackTimeoutMs);
  var decoder = protocol.decodeProduceResponse;
  var self = this;

  decoder.requireAcks = requireAcks;

  async.each(payloads, buildRequest, function (err) {
    if (err) return cb(err);
    self.send(payloads, encoder, decoder, function (err, result) {
      if (err) {
        if (err.message === 'NotLeaderForPartition' || err.message === 'UnknownTopicOrPartition') {
          self.emit('brokersChanged');
        }
        cb(err);
      } else {
        cb(null, result);
      }
    });
  });

  function buildRequest (payload, cb) {
    var attributes = payload.attributes;
    var codec = getCodec(attributes);

    if (!codec) return cb();

    var innerSet = encodeMessageSet(payload.messages);
    codec.encode(innerSet, function (err, message) {
      if (err) return cb(err);
      payload.messages = [new Message(0, attributes, '', message)];
      cb();
    });
  }
};

Client.prototype.sendOffsetCommitRequest = function (group, payloads, cb) {
  var encoder = protocol.encodeOffsetCommitRequest(group);
  var decoder = protocol.decodeOffsetCommitResponse;
  this.send(payloads, encoder, decoder, cb);
};

Client.prototype.sendOffsetCommitV2Request = function (group, generationId, memberId, payloads, cb) {
  var encoder = protocol.encodeOffsetCommitV2Request;
  var decoder = protocol.decodeOffsetCommitResponse;
  this.sendGroupRequest(encoder, decoder, arguments);
};

Client.prototype.sendOffsetFetchV1Request = function (group, payloads, cb) {
  var encoder = protocol.encodeOffsetFetchV1Request;
  var decoder = protocol.decodeOffsetFetchV1Response;
  this.sendGroupRequest(encoder, decoder, arguments);
};

Client.prototype.setCoordinatorIdAndSendOffsetFetchV1Request = function (group, payloads, cb) {
  this.sendGroupCoordinatorRequest(group, (err, coordinatorInfo) => {
    if (err) return cb(new errors.BrokerNotAvailableError('Broker not available'));
    this.coordinatorId = String(coordinatorInfo.coordinatorId);
    this.sendOffsetFetchV1Request(group, payloads, cb);
  });
};

Client.prototype.sendOffsetFetchRequest = function (group, payloads, cb) {
  var encoder = protocol.encodeOffsetFetchRequest(group);
  var decoder = protocol.decodeOffsetFetchResponse;
  this.send(payloads, encoder, decoder, cb);
};

Client.prototype.sendOffsetRequest = function (payloads, cb) {
  var encoder = protocol.encodeOffsetRequest;
  var decoder = protocol.decodeOffsetResponse;
  this.send(payloads, encoder, decoder, cb);
};

Client.prototype.refreshBrokerMetadata = function () {};

Client.prototype.sendWhenReady = function (broker, correlationId, request, decode, cb) {
  this.queueCallback(broker.socket, correlationId, [decode, cb]);
  broker.write(request);
};

Client.prototype.sendGroupRequest = function (encode, decode, requestArgs) {
  requestArgs = _.values(requestArgs);
  var cb = requestArgs.pop();
  var correlationId = this.nextId();

  requestArgs.unshift(this.clientId, correlationId);

  var request = encode.apply(null, requestArgs);
  var broker = this.brokerForLeader(this.coordinatorId);
  var brokerError = null;
  if (!broker) {
    brokerError = 'Could not find broker';
  } else if (!broker.isConnected()) {
    brokerError = 'Broker socket is closed' + (broker.socket.error ? ' - ' + broker.socket.error.message : '');
  }
  if (brokerError) {
    this.refreshBrokerMetadata();
    return cb(new errors.BrokerNotAvailableError('Broker not available: ' + brokerError));
  }

  this.sendWhenReady(broker, correlationId, request, decode, cb);
};

Client.prototype.sendGroupCoordinatorRequest = function (groupId, cb) {
  this.sendGroupRequest(protocol.encodeGroupCoordinatorRequest, protocol.decodeGroupCoordinatorResponse, arguments);
};

Client.prototype.sendJoinGroupRequest = function (groupId, memberId, sessionTimeout, groupProtocol, cb) {
  this.sendGroupRequest(protocol.encodeJoinGroupRequest, protocol.decodeJoinGroupResponse, arguments);
};

Client.prototype.sendSyncGroupRequest = function (groupId, generationId, memberId, groupAssignment, cb) {
  this.sendGroupRequest(protocol.encodeSyncGroupRequest, protocol.decodeSyncGroupResponse, arguments);
};

Client.prototype.sendHeartbeatRequest = function (groupId, generationId, memberId, cb) {
  this.sendGroupRequest(protocol.encodeGroupHeartbeatRequest, protocol.decodeGroupHeartbeatResponse, arguments);
};

Client.prototype.sendLeaveGroupRequest = function (groupId, memberId, cb) {
  this.sendGroupRequest(protocol.encodeLeaveGroupRequest, protocol.decodeLeaveGroupResponse, arguments);
};

/*
 *  Helper method
 *  topic in payloads may send to different broker, so we cache data util all request came back
 */
function wrap (payloads, cb) {
  var out = {};
  var count = Object.keys(payloads).length;

  return function (err, data) {
    // data: { topicName1: {}, topicName2: {} }
    if (err) return cb && cb(err);
    _.merge(out, data);
    count -= 1;
    // Waiting for all request return
    if (count !== 0) return;
    cb && cb(null, out);
  };
}

/**
 * Fetches metadata information for a topic
 * This includes an array containing a each zookeeper node, their nodeId, host name, and port. As well as an object
 * containing the topic name, partition, leader number, replica count, and in sync replicas per partition.
 *
 * @param {Array} topics An array of topics to load the metadata for
 * @param {Client~loadMetadataForTopicsCallback} cb Function to call once all metadata is loaded
 */
Client.prototype.loadMetadataForTopics = function (topics, cb) {
  var correlationId = this.nextId();
  var request = protocol.encodeMetadataRequest(this.clientId, correlationId, topics);
  var broker = this.brokerForLeader();

  if (!broker || !broker.isConnected()) {
    return cb(new errors.BrokerNotAvailableError('Broker not available'));
  }

  this.sendWhenReady(broker, correlationId, request, protocol.decodeMetadataResponse, cb);
};

Client.prototype.createTopics = function (topics, isAsync, cb) {
  topics = typeof topics === 'string' ? [topics] : topics;

  if (typeof isAsync === 'function' && typeof cb === 'undefined') {
    cb = isAsync;
    isAsync = true;
  }

  try {
    validateKafkaTopics(topics);
  } catch (e) {
    if (isAsync) return cb(e);
    throw e;
  }

  cb = _.once(cb);

  const getTopicsFromKafka = (topics, callback) => {
    this.loadMetadataForTopics(topics, function (error, resp) {
      if (error) {
        return callback(error);
      }
      callback(null, Object.keys(resp[1].metadata));
    });
  };

  const operation = retry.operation({ minTimeout: 200, maxTimeout: 2000 });

  operation.attempt(currentAttempt => {
    logger.debug('create topics currentAttempt', currentAttempt);
    getTopicsFromKafka(topics, function (error, kafkaTopics) {
      if (error) {
        if (operation.retry(error)) {
          return;
        }
      }

      logger.debug('kafka reported topics', kafkaTopics);
      const left = _.difference(topics, kafkaTopics);
      if (left.length === 0) {
        logger.debug(`Topics created ${kafkaTopics}`);
        return cb(null, kafkaTopics);
      }

      logger.debug(`Topics left ${left.join(', ')}`);
      if (!operation.retry(new Error(`Topics not created ${left}`))) {
        cb(operation.mainError());
      }
    });
  });

  if (!isAsync) {
    cb(null);
  }
};

Client.prototype.addTopics = function (topics, cb) {
  var self = this;
  this.topicExists(topics, function (err) {
    if (err) return cb(err);
    self.loadMetadataForTopics(topics, function (err, resp) {
      if (err) return cb(err);
      self.updateMetadatas(resp);
      cb(null, topics);
    });
  });
};

Client.prototype.nextId = function () {
  if (this.correlationId >= MAX_INT32) {
    this.correlationId = 0;
  }
  return this.correlationId++;
};

Client.prototype.nextSocketId = function () {
  return this._socketId++;
};

Client.prototype.refreshBrokers = function () {
  var self = this;
  var validBrokers = Object.keys(this.brokerProfiles);

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

Client.prototype.refreshMetadata = function (topicNames, cb) {
  var self = this;
  if (!topicNames.length) return cb();
  attemptRequestMetadata(topicNames, cb);

  function attemptRequestMetadata (topics, cb) {
    var operation = retry.operation({ minTimeout: 200, maxTimeout: 1000 });
    operation.attempt(function (currentAttempt) {
      logger.debug('refresh metadata currentAttempt', currentAttempt);
      self.loadMetadataForTopics(topics, function (err, resp) {
        err = err || resp[1].error;
        if (Array.isArray(err)) {
          err = new Error(String(err));
        }
        if (operation.retry(err)) {
          return;
        }
        if (err) {
          logger.debug('refresh metadata error', err.message);
          return cb(err);
        }
        self.updateMetadatas(resp);
        cb();
      });
    });
  }
};

Client.prototype.send = function (payloads, encoder, decoder, cb) {
  var self = this;
  var _payloads = payloads;
  // payloads: [ [metadata exists], [metadata not exists] ]
  payloads = this.checkMetadatas(payloads);
  if (payloads[0].length && !payloads[1].length) {
    this.sendToBroker(_.flatten(payloads), encoder, decoder, cb);
    return;
  }
  if (payloads[1].length) {
    var topicNames = payloads[1].map(function (p) {
      return p.topic;
    });
    this.loadMetadataForTopics(topicNames, function (err, resp) {
      if (err) {
        return cb(err);
      }

      var error = resp[1].error;
      if (error) {
        return cb(error);
      }

      self.updateMetadatas(resp);
      // check payloads again
      payloads = self.checkMetadatas(_payloads);
      if (payloads[1].length) {
        self.refreshBrokerMetadata();
        return cb(new errors.BrokerNotAvailableError('Could not find the leader'));
      }

      self.sendToBroker(payloads[1].concat(payloads[0]), encoder, decoder, cb);
    });
  }
};

Client.prototype.sendToBroker = function (payloads, encoder, decoder, cb) {
  var longpolling = encoder.name === 'encodeFetchRequest';
  payloads = this.payloadsByLeader(payloads);
  if (!longpolling) {
    cb = wrap(payloads, cb);
  }
  for (var leader in payloads) {
    if (!payloads.hasOwnProperty(leader)) {
      continue;
    }
    var correlationId = this.nextId();
    var broker = this.brokerForLeader(leader, longpolling);
    var brokerError = null;
    if (!broker) {
      brokerError = 'Could not find broker';
    } else if (!broker.isConnected()) {
      brokerError = 'Broker socket is closed' + (broker.socket.error ? ' - ' + broker.socket.error.message : '');
    }
    if (brokerError) {
      this.refreshBrokerMetadata();
      return cb(new errors.BrokerNotAvailableError('Broker not available: ' + brokerError), payloads[leader]);
    }

    if (longpolling) {
      if (broker.socket.waiting) {
        continue;
      }
      broker.socket.waiting = true;
    }
    var request = encoder(this.clientId, correlationId, payloads[leader]);

    if (decoder.requireAcks === 0) {
      broker.writeAsync(request);
      cb(null, { result: 'no ack' });
    } else {
      this.sendWhenReady(broker, correlationId, request, decoder, cb);
    }
  }
};

Client.prototype.checkMetadatas = function (payloads) {
  if (_.isEmpty(this.topicMetadata)) return [[], payloads];
  // out: [ [metadata exists], [metadata not exists] ]
  var out = [[], []];
  payloads.forEach(
    function (p) {
      if (this.hasMetadata(p.topic, p.partition)) out[0].push(p);
      else out[1].push(p);
    }.bind(this)
  );
  return out;
};

Client.prototype.hasMetadata = function (topic, partition) {
  var brokerMetadata = this.brokerMetadata;
  var leader = this.leaderByPartition(topic, partition);

  return leader !== undefined && brokerMetadata[leader];
};

Client.prototype.updateMetadatas = function (metadatas) {
  // _.extend(this.brokerMetadata, metadatas[0])
  _.extend(this.topicMetadata, metadatas[1].metadata);
  for (var topic in this.topicMetadata) {
    if (!this.topicMetadata.hasOwnProperty(topic)) {
      continue;
    }
    this.topicPartitions[topic] = Object.keys(this.topicMetadata[topic]).map(function (val) {
      return parseInt(val, 10);
    });
  }
};

Client.prototype.removeTopicMetadata = function (topics, cb) {
  topics.forEach(
    function (t) {
      if (this.topicMetadata[t]) delete this.topicMetadata[t];
    }.bind(this)
  );
  cb(null, topics.length);
};

Client.prototype.payloadsByLeader = function (payloads) {
  return payloads.reduce(
    function (out, p) {
      var leader = this.leaderByPartition(p.topic, p.partition);
      out[leader] = out[leader] || [];
      out[leader].push(p);
      return out;
    }.bind(this),
    {}
  );
};

Client.prototype.leaderByPartition = function (topic, partition) {
  var topicMetadata = this.topicMetadata;
  return topicMetadata[topic] && topicMetadata[topic][partition] && topicMetadata[topic][partition].leader;
};

Client.prototype.brokerForLeader = function (leader, longpolling) {
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

  var broker = _.find(this.brokerProfiles, { id: leader });

  if (!broker) {
    return;
  }

  addr = broker.host + ':' + broker.port;

  return brokers[addr] || this.setupBroker(broker.host, broker.port, longpolling, brokers);
};

Client.prototype.getBrokers = function (longpolling) {
  return longpolling ? this.longpollingBrokers : this.brokers;
};

Client.prototype.reconnectBroker = function (oldSocket) {
  oldSocket.retrying = false;
  if (oldSocket.error) {
    oldSocket.destroy();
  }
  var brokers = this.getBrokers(oldSocket.longpolling);
  var newBroker = this.setupBroker(oldSocket.host, oldSocket.port, oldSocket.longpolling, brokers);
  newBroker.socket.error = oldSocket.error;
};

Client.prototype.handleReceivedData = function (socket) {
  var buffer = socket.buffer;
  if (!buffer.length || buffer.length < 4) {
    return;
  }
  var size = buffer.readUInt32BE(0) + 4;

  if (buffer.length >= size) {
    if (socket.longpolling) {
      socket.waiting = false;
    }

    var resp = buffer.shallowSlice(0, size);
    var correlationId = resp.readUInt32BE(4);

    this.invokeResponseCallback(socket, correlationId, resp);
    buffer.consume(size);
  } else {
    return;
  }

  if (socket.buffer.length) {
    setImmediate(
      function () {
        this.handleReceivedData(socket);
      }.bind(this)
    );
  }
};

Client.prototype.invokeResponseCallback = function (socket, correlationId, resp) {
  var handlers = this.unqueueCallback(socket, correlationId);

  if (handlers) {
    var [decoder, cb] = handlers;
    var result = decoder(resp);
    if (result instanceof Error) {
      cb.call(this, result);
    } else {
      cb.call(this, null, result);
    }
  } else {
    logger.error(`missing handlers for Correlation ID: ${correlationId}`);
  }
};

Client.prototype.queueCallback = function (socket, id, data) {
  var socketId = socket.socketId;
  var queue = this.cbqueue.get(socketId);
  if (!queue) {
    queue = new Map();
    this.cbqueue.set(socketId, queue);
  }

  queue.set(id, data);
};

Client.prototype.unqueueCallback = function (socket, id) {
  var socketId = socket.socketId;

  var queue = this.cbqueue.get(socketId);
  try {
    if (!queue) {
      return null;
    }

    if (!queue.has(id)) {
      return null;
    }

    var result = queue.get(id);

    // cleanup socket queue
    queue.delete(id);

    return result;
  } finally {
    if (queue && !queue.size) {
      this.cbqueue.delete(socketId);
    }
  }
};

Client.prototype.clearCallbackQueue = function (socket, error) {
  var socketId = socket.socketId;
  var longpolling = socket.longpolling;

  var queue = this.cbqueue.get(socketId);
  if (!queue) {
    return;
  }

  if (!longpolling) {
    queue.forEach(function (handlers) {
      var cb = handlers[1];
      cb(error);
    });
  }

  this.cbqueue.delete(socketId);
};

module.exports = Client;
