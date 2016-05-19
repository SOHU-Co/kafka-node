'use strict';

var net  = require('net'),
    util = require('util'),
    _ = require('lodash'),
    async = require('async'),
    retry = require('retry'),
    events = require('events'),
    errors = require('./errors'),
    Binary = require('binary'),
    getCodec = require('./codec'),
    protocol = require('./protocol'),
    BrokerWrapper = require('./wrapper/BrokerWrapper'),
    encodeMessageSet = protocol.encodeMessageSet,
    Message = protocol.Message,
    zk = require('./zookeeper'),
    Zookeeper = zk.Zookeeper,
    debug = require('debug')('kafka-node:Client');
/**
 * Communicates with kafka brokers
 * Uses zookeeper to discover all the kafka brokers to connect to
 *
 * @example <caption>Non chrooted connection to a single zookeeper host</caption>
 * var client = new Client('localhost:2181')
 *
 * @example <caption>Chrooted connection to multiple zookeeper hosts</caption>
 * var client = new Client('localhost:2181,localhost:2182/exmaple/chroot
 *
 * @param {String} [connectionString='localhost:2181/kafka0.8'] A string containing a list of zookeeper hosts:port
 *      and the zookeeper chroot
 * @param {String} [clientId='kafka-node-client'] The client id to register with zookeeper, helpful for debugging
 * @param {Object} zkOptions Pass through options to the zookeeper client library
 *
 * @param {Object} noAckBatchOptions Batch buffer options for no ACK requirement producers
 * - noAckBatchOptions.noAckBatchSize Max batch size in bytes for the buffer before sending all data to broker
 * - noAckBatchOptions.noAckBatchAge Timeout max for the buffer to retain data before sending all data to broker
 *
 * @constructor
 */
var Client = function (connectionString, clientId, zkOptions, noAckBatchOptions) {
    if (this instanceof Client === false) {
        return new Client(connectionString, clientId);
    }

    this.connectionString = connectionString || 'localhost:2181/';
    this.clientId = clientId || 'kafka-node-client';
    this.zkOptions = zkOptions;
    this.noAckBatchOptions = noAckBatchOptions;
    this.brokers = {};
    this.longpollingBrokers = {};
    this.topicMetadata = {};
    this.topicPartitions = {};
    this.correlationId = 0;
    this._socketId = 0;
    this.cbqueue = {};
    this.brokerMetadata = {};
    this.ready = false;
    this.connect();
};

util.inherits(Client, events.EventEmitter);

Client.prototype.connect = function () {
    var zk = this.zk = new Zookeeper(this.connectionString, this.zkOptions);
    var self = this;
    zk.once('init', function (brokers) {
        self.ready = true;
        self.brokerMetadata = brokers;
        Object
            .keys(brokers)
            .some(function (key, index) {
                var broker = brokers[key];
                var addr = broker.host + ':' + broker.port;
                self.brokers[addr] = self.createBroker(broker.host, broker.port);
                // Only connect one broker
                return !index;
            });
        self.emit('ready');
    });
    zk.on('brokersChanged', function (brokerMetadata) {
        self.refreshBrokers(brokerMetadata);
        // Emit after a 3 seconds
        setTimeout(function () {
            self.emit('brokersChanged');
        }, 3000);
    });
    zk.once('disconnected', function () {
        if (!zk.closed) {
            zk.close();
            self.connect();
            self.emit('zkReconnect');
        }
    })
    zk.on('error', function (err) {
        self.emit('error', err);
    });
};

Client.prototype.close = function (cb) {
    this.closeBrokers(this.brokers);
    this.closeBrokers(this.longpollingBrokers);
    this.zk.close();
    cb && cb();
};

Client.prototype.closeBrokers = function (brokers) {
    _.each(brokers, function (broker) {
        broker.socket.closing = true;
        broker.socket.end();
    });
};

Client.prototype.sendFetchRequest = function (consumer, payloads, fetchMaxWaitMs, fetchMinBytes, maxTickMessages) {
    var self = this;
    var encoder = protocol.encodeFetchRequest(fetchMaxWaitMs, fetchMinBytes);
    var decoder = protocol.decodeFetchResponse(function (err, type, message) {
            if (err) {
                if (err.message === 'OffsetOutOfRange') {
                    return consumer.emit('offsetOutOfRange', err);
                } else if (err.message === 'NotLeaderForPartition') {
                    return self.emit('brokersChanged');
                }

                return consumer.emit('error', err);
            }

            var encoding = consumer.options.encoding;

            if (type === 'message') {
                if (encoding !== 'buffer' && message.value) {
                    message.value = message.value.toString(encoding);
                }

                consumer.emit('message', message);
            } else {
                consumer.emit('done', message);
            }
        }, maxTickMessages);

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
                if (err.message === 'NotLeaderForPartition') {
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
        codec.encode(innerSet, function(err, message) {
            if (err) return cb(err);
            payload.messages = [ new Message(0, attributes, '', message) ];
            cb();
        });
    }
};

Client.prototype.sendOffsetCommitRequest = function (group, payloads, cb) {
    var encoder = protocol.encodeOffsetCommitRequest(group),
        decoder = protocol.decodeOffsetCommitResponse;
    this.send(payloads, encoder, decoder, cb);
};

Client.prototype.sendOffsetFetchRequest = function (group, payloads, cb) {
    var encoder = protocol.encodeOffsetFetchRequest(group),
        decoder = protocol.decodeOffsetFetchResponse;
    this.send(payloads, encoder, decoder, cb);
};

Client.prototype.sendOffsetRequest = function (payloads, cb) {
    var encoder = protocol.encodeOffsetRequest,
        decoder = protocol.decodeOffsetResponse;
    this.send(payloads, encoder, decoder, cb);
};

/*
 *  Helper method
 *  topic in paylods may send to different broker, so we cache data util all request came back
 */
function wrap(payloads, cb) {
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
    }
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

    if (!broker || !broker.socket || broker.socket.error) {
        return cb(new errors.BrokerNotAvailableError('Broker not available'));
    }

    this.queueCallback(broker.socket, correlationId, [protocol.decodeMetadataResponse, cb]);
    broker.write(request);
};

Client.prototype.createTopics = function (topics, isAsync, cb) {
    topics = typeof topics === 'string' ? [topics] : topics;

    if (typeof isAsync === 'function' && typeof cb === 'undefined') {
        cb = isAsync;
        isAsync = true;
    }
    var self = this;

    // first, load metadata to create topics
    this.loadMetadataForTopics(topics, function (err, resp) {
        if (err) return cb(err);
        if (isAsync) return cb(null, 'All requests sent');
        var topicMetadata = resp[1].metadata;
        // ommit existed topics
        var existed = Object.keys(topicMetadata);
        var topicsNotExists = topics.filter(function (topic) {
            return !~existed.indexOf(topic);
        });

        function attemptCreateTopics (topics, cb) {
            var operation = retry.operation({ minTimeout: 200, maxTimeout: 2000 });
            operation.attempt(function(currentAttempt) {
                debug('create topics currentAttempt', currentAttempt);
                self.loadMetadataForTopics(topics, function (err, resp) {
                    if (resp) {
                        var topicMetadata = resp[1].metadata;
                        var created = Object.keys(topicMetadata).length === topics.length;
                        if (!created) err = new Error('Topic creation pending');
                    }
                    if (operation.retry(err)) {
                        return;
                    }

                    cb(err, 'All created');
                });
            });
        }

        if (!topicsNotExists.length) return cb(null, 'All created');

        debug('create topic by sending metadata request');
        attemptCreateTopics(topicsNotExists, cb);
    });
};

/**
 * Checks to see if a given array of topics exists
 *
 * @param {Array} topics An array of topic names to check
 *
 * @param {Client~topicExistsCallback} cb A function to call after all topics have been checked
 */
Client.prototype.topicExists = function (topics, cb) {
    var notExistsTopics = [];
    var self = this;

    async.each(topics, checkZK, function (err) {
      if (err) return cb(err);
      if (notExistsTopics.length) return cb(new errors.TopicsNotExistError(notExistsTopics));
      cb();
    });

    function checkZK (topic, cb) {
        self.zk.topicExists(topic, function (err, existed, topic) {
            if (err) return cb(err);
            if (!existed) notExistsTopics.push(topic);
            cb();
        });
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
    return this.correlationId++;
};

Client.prototype.nextSocketId = function () {
    return this._socketId++;
};

Client.prototype.refreshBrokers = function (brokerMetadata) {
    var self = this;
    this.brokerMetadata = brokerMetadata;
    deleteDeadBrokers(this.brokers);
    deleteDeadBrokers(this.longpollingBrokers);
    function deleteDeadBrokers (brokers) {
        Object.keys(brokers).filter(function (k) {
            return !~_.values(brokerMetadata).map(function (b) { return b.host + ':' + b.port }).indexOf(k);
        }).forEach(function (deadKey) {
            self.closeBrokers([brokers[deadKey]]);
            delete brokers[deadKey];
        }.bind(this));
    }
};

Client.prototype.refreshMetadata = function (topicNames, cb) {
    var self = this;
    if (!topicNames.length) return cb();
    attemptRequestMetadata(topicNames, cb);

    function attemptRequestMetadata (topics, cb) {
        var operation = retry.operation({ minTimeout: 200, maxTimeout: 1000 });
        operation.attempt(function(currentAttempt) {
            debug('refresh metadata currentAttempt', currentAttempt);
            self.loadMetadataForTopics(topics, function (err, resp) {
                err = err || resp[1].error;
                if (operation.retry(err)) {
                    return;
                }
                if (err) {
                    debug('refresh metadata error', err.message)
                    return cb(err);
                }
                self.updateMetadatas(resp);
                cb();
            });
        });
    }
};

Client.prototype.send = function (payloads, encoder, decoder, cb) {
    var self = this, _payloads = payloads;
    // payloads: [ [metadata exists], [metadata not exists] ]
    payloads = this.checkMetadatas(payloads);
    if (payloads[0].length && !payloads[1].length) {
        this.sendToBroker(_.flatten(payloads), encoder, decoder, cb);
        return;
    }
    if (payloads[1].length) {
        var topicNames = payloads[1].map(function (p) { return p.topic; });
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
        var correlationId = this.nextId();
        var request = encoder.call(null, this.clientId, correlationId, payloads[leader]);
        var broker = this.brokerForLeader(leader, longpolling);
        if (!broker || !broker.socket || broker.socket.error || broker.socket.closing) {
            return cb(new errors.BrokerNotAvailableError('Could not find the leader'), payloads[leader]);
        }

        if (longpolling) {
            if (broker.socket.waitting) continue;
            broker.socket.waitting = true;
        }

        if (decoder.requireAcks == 0) {
            broker.writeAsync(request);
            cb(null, { result: 'no ack' });
        } else {
            this.queueCallback(broker.socket, correlationId, [decoder, cb]);
            broker.write(request);
        }
    }
};

Client.prototype.checkMetadatas = function (payloads) {
    if (_.isEmpty(this.topicMetadata)) return [ [],payloads ];
    // out: [ [metadata exists], [metadata not exists] ]
    var out = [ [], [] ];
    payloads.forEach(function (p) {
        if (this.hasMetadata(p.topic, p.partition)) out[0].push(p)
        else out[1].push(p)
    }.bind(this));
    return out;
};

Client.prototype.hasMetadata = function (topic, partition) {
    var brokerMetadata = this.brokerMetadata;
    var leader = this.leaderByPartition(topic, partition);

    return (leader !== undefined) && brokerMetadata[leader];
};

Client.prototype.updateMetadatas = function (metadatas) {
    // _.extend(this.brokerMetadata, metadatas[0]);
    _.extend(this.topicMetadata, metadatas[1].metadata);
    for(var topic in this.topicMetadata) {
        this.topicPartitions[topic] = Object.keys(this.topicMetadata[topic]).map(function(val) {
            return parseInt(val, 10);
        });
    }
};

Client.prototype.removeTopicMetadata = function (topics, cb) {
    topics.forEach(function (t) {
        if (this.topicMetadata[t]) delete this.topicMetadata[t];
    }.bind(this));
    cb(null, topics.length);
};

Client.prototype.payloadsByLeader = function (payloads) {
    return payloads.reduce(function (out, p) {
        var leader = this.leaderByPartition(p.topic, p.partition);
        out[leader] = out[leader] || [];
        out[leader].push(p);
        return out;
    }.bind(this), {});
};

Client.prototype.leaderByPartition = function (topic, partition) {
    var topicMetadata = this.topicMetadata;
    return topicMetadata[topic] && topicMetadata[topic][partition] && topicMetadata[topic][partition]['leader'];
};

Client.prototype.brokerForLeader = function (leader, longpolling) {
    var brokers = longpolling ? this.longpollingBrokers : this.brokers;
    // If leader is not give, choose the first broker as leader
    if (typeof leader === 'undefined') {
        if (!_.isEmpty(brokers)) {
            var addr = Object.keys(brokers)[0];
            return brokers[addr];
        } else if (!_.isEmpty(this.brokerMetadata)) {
            leader = Object.keys(this.brokerMetadata)[0];
        } else {
            this.emit('error', new errors.BrokerNotAvailableError('Could not find a broker'));
            return;
        }
    }
    var metadata = this.brokerMetadata[leader],
        addr = metadata.host + ':' + metadata.port;
    brokers[addr] = brokers[addr] || this.createBroker(metadata.host, metadata.port, longpolling);
    return brokers[addr];
};

Client.prototype.createBroker = function connect(host, port, longpolling) {
    var self = this;
    var socket = net.createConnection(port, host);
    socket.addr = host + ':' + port;
    socket.host = host;
    socket.port = port;
    socket.socketId = this.nextSocketId();
    if (longpolling) socket.longpolling = true;

    socket.on('connect', function () {
        var lastError = this.error;
        this.error = null;
        if (lastError) {
            this.waitting = false;
            self.emit('reconnect');
        } else {
            self.emit('connect');
        }
    });
    socket.on('error', function (err) {
        this.error = err;
        self.emit('error', err);
    });
    socket.on('close', function (had_error) {
        self.emit('close', this);
        if (had_error && this.error) {
            self.clearCallbackQueue(this, this.error);
        }
        else {
            self.clearCallbackQueue(this, new errors.BrokerNotAvailableError('Broker not available'));
        }
        retry(this);
    });
    socket.on('end', function () {
        retry(this);
    });
    socket.buffer = new Buffer([]);
    socket.on('data', function (data) {
        this.buffer = Buffer.concat([this.buffer, data]);
        self.handleReceivedData(this);
    });
    socket.setKeepAlive(true, 60000);

    function retry(s) {
        if(s.retrying || s.closing) return;
        s.retrying = true;
        s.retryTimer = setTimeout(function () {
            s.retrying = false;
            s.connect(s.port, s.host);
        }, 1000);
    }
    return new BrokerWrapper(socket, this.noAckBatchOptions);
};

Client.prototype.handleReceivedData = function (socket) {
    var vars = Binary.parse(socket.buffer).word32bu('size').word32bu('correlationId').vars,
        size = vars.size + 4,
        correlationId = vars.correlationId;
    if (socket.buffer.length >= size) {
        var resp = socket.buffer.slice(0, size);
        var handlers = this.unqueueCallback(socket, correlationId);

        if (!handlers) return;
        var decoder = handlers[0];
        var cb = handlers[1];
        var result = decoder(resp);
        (result instanceof Error)
            ? cb.call(this, result)
            : cb.call(this, null, result);
        socket.buffer = socket.buffer.slice(size);
        if (socket.longpolling) socket.waitting = false;
    } else { return }

    if (socket.buffer.length)
        setImmediate(function () { this.handleReceivedData(socket);}.bind(this));
};

Client.prototype.queueCallback = function (socket, id, data) {
    var socketId = socket.socketId;
    var queue;

    if (this.cbqueue.hasOwnProperty(socketId)) {
        queue = this.cbqueue[socketId];
    }
    else {
        queue = {};
        this.cbqueue[socketId] = queue;
    }

    queue[id] = data;
};

Client.prototype.unqueueCallback = function (socket, id) {
    var socketId = socket.socketId;

    if (!this.cbqueue.hasOwnProperty(socketId)) {
        return null;
    }

    var queue = this.cbqueue[socketId];
    if (!queue.hasOwnProperty(id)) {
        return null;
    }

    var result = queue[id];

    // cleanup socket queue
    delete queue[id];
    if (!Object.keys(queue).length) {
        delete this.cbqueue[socketId];
    }

    return result;
};

Client.prototype.clearCallbackQueue = function (socket, error) {
    var socketId = socket.socketId;
    var longpolling = socket.longpolling;

    if (!this.cbqueue.hasOwnProperty(socketId)) {
        return;
    }

    var queue = this.cbqueue[socketId];

    if (!longpolling) {
        Object.keys(queue).forEach(function (key) {
            var handlers = queue[key];
            var cb = handlers[1];
            cb(error);
        });
    }
    delete this.cbqueue[socketId];
};

module.exports = Client;
