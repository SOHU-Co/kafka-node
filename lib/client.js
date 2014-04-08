'use strict';

var net  = require('net'),
    util = require('util'),
    _ = require('lodash'),
    events = require('events'),
    Binary = require('binary'),
    protocol = require('./protocol'),
    liberrors = require('./errors'),
    Zookeeper = require('./zookeeper');

var DEFAULTS = {
    metadataRetries: 3,
    metadataRetriesBackoffMs: 100,
    lazyBrokerConnection: true
};

var Client = function (connectionString, clientId, options) {
    if (this instanceof Client === false) return new Client(connectionString, clientId); 
    this.connectionString = connectionString || 'localhost:2181/kafka0.8';
    this.clientId = clientId || 'kafka-node-client';
    this.brokers = {};
    this.longpollingBrokers = {};
    this.topicMetadata = {};
    this.topicPartitions = {};
    this.correlationId = 0;
    this.cbqueue = {};
    this.brokerMetadata = {}
    this.ready = false;
    this.options = _.defaults((options||{}), DEFAULTS);
    this.metadataRetriesAttempts = 0;

    this.connect();
}
util.inherits(Client, events.EventEmitter);

Client.prototype.connect = function (options) {
    var zk = this.zk = new Zookeeper(this.connectionString, this.options && this.options.zookeeper);
    var self = this;
    zk.once('init', function (broker) {
        self.ready = true;

        if (Object.keys(broker).length && self.options.lazyBrokerConnection) {
            broker = _.pick(broker, Object.keys(broker)[0]);
            }

        self.brokerMetadata = broker;
        for (var b in broker) {
            var addr = broker[b].host + ':' + broker[b].port;
            self.brokers[addr] = self.createBroker(broker[b].host, broker[b].port);
        }
        self.emit('ready');
    });
    zk.on('brokersChanged', function (brokerMetadata) {
        self.refreshBrokers(brokerMetadata);
        self.emit('brokersChanged');
    });
    zk.on('error', function (err) {
        self.emit('error', err);
    });
}

Client.prototype.close = function (cb) {
    this.closeBrokers(this.brokers);
    this.closeBrokers(this.longpollingBrokers);
    this.zk.client.close();
    cb && cb();
}

Client.prototype.closeBrokers = function (brokers) {
    _.each(brokers, function (broker) {
        broker.closing = true;
        broker.end();
    });
}

Client.prototype.sendFetchRequest = function (consumer, payloads, fetchMaxWaitMs, fetchMinBytes) {
    var topics = {},
        count = payloads.length,
        offsetError = false,
        encoder = protocol.encodeFetchRequest(fetchMaxWaitMs, fetchMinBytes),
        decoder = protocol.decodeFetchResponse(function (err, type, value) {
            if (err) {
                if (err.message === 'OffsetOutOfRange') {
                    offsetError = true;
                    return consumer.emit('offsetOutOfRange', err);
                }
                return consumer.emit('error', err);
            }
            
            if (type === 'message') {
                consumer.emit('message', value);
            } else {
                consumer.emit('done', value);
            }
        });

        this.send(payloads, encoder, decoder, function (err) {
            if (err) {
                Array.prototype.unshift.call(arguments, 'error')
                consumer.emit.apply(consumer, arguments);
            }
        });
}

Client.prototype.sendProduceRequest = function (payloads, requireAcks, ackTimeoutMs, cb) {
    var encoder = protocol.encodeProduceRequest(requireAcks, ackTimeoutMs),
        decoder = protocol.decodeProduceResponse;
    this.send(payloads, encoder, decoder, wrapProduceRequest(payloads,cb));

    // If no acknowledgment is required, broker will not send a response.
    !requireAcks && cb && cb();
}

Client.prototype.sendOffsetCommitRequest = function (group, payloads, cb) {
    var encoder = protocol.encodeOffsetCommitRequest(group),
        decoder = protocol.decodeOffsetCommitResponse;
    this.send(payloads, encoder, decoder, wrap(payloads,cb));
}

Client.prototype.sendOffsetFetchRequest = function (group, payloads, cb) {
    var encoder = protocol.encodeOffsetFetchRequest(group),
        decoder = protocol.decodeOffsetFetchResponse;
    this.send(payloads, encoder, decoder, wrap(payloads,cb));
}

Client.prototype.sendOffsetRequest = function (payloads, cb) {
    var encoder = protocol.encodeOffsetRequest,
        decoder = protocol.decodeOffsetResponse;
    this.send(payloads, encoder, decoder, wrap(payloads, cb));
}

/*
 *  Helper method
 *  topic in paylods may send to different broker, so we cache data util all request came back
 */
function wrap(payloads, cb) {
    var out = {};
    var count = payloads.length;
    return function (err, data) {
        // data: { topicName1: {}, topicName2: {} }
        if (err) return cb(err);
        _.merge(out, data);
        count -= Object.keys(data).length;
        // Waiting for all request return
        if (count !== 0) return;
        cb && cb(null, out);
    }
}

function wrapProduceRequest(payloads, cb) {
    var out = null;
    var outErr = null;
    var count = payloads.length;
    return function (err, data) {
        // data: { topicName1: {}, topicName2: {} }

        // If error is of type TopicsPartitionsError we should wait for responses from other brokers before returning error.
        if ((err) && !(err instanceof liberrors.TopicsPartitionsError)) {
            return cb(err);
        }

        if (data) {
            // In case we have a payload resembling the following:
            //     { topic:'topic0', messages: ['hello'], partition:'0'},
            //     { topic:'topic0', messages: ['hello'], partition:'1'}
            // If the 2 partitions have the same leader / broker, we should ensure the client callback is called.
            // The count in the original wrap function should be decreased by 2 in this case and not 1; we need to look at the count of the updated partitions per topic and not the topic count.
            _.forIn(data, function (topicData, topicKey) {
                count -= Object.keys(topicData.partitions).length;
            });

            out = out ? _.merge(out, data) : data;
        }

        if (err) {
            _.forIn(err.topics, function (topicData, topicKey) {
                count -= Object.keys(topicData.partitions).length;
            });

            outErr = outErr ? _.merge(outErr, err) : err;
        }

        // Waiting for all request return
        if (count !== 0) return;
        cb && cb(outErr, out);
    }
}

/**
 * List broker and metadata information for one or more topics.
 * @param {String[]} an array of topic names.
 * @param {Client~loadMetadataForTopicsCallback} cb - The callback that returns the response.
 */
Client.prototype.loadMetadataForTopics = function (topics, cb) {

    if (!_.isArray(topics)) {
        return cb(new liberrors.InvalidArgumentFormatError('topics', 'topics should be an array of topic names (string(s)).'));
    }

    var self = this;
    var cbb = function (err, resp) {
        if (err) {
            if (self.metadataRetriesAttempts < self.options.metadataRetries) {
                self.metadataRetriesAttempts += 1;

                console.log('loadMetadataForTopics failed with error %s; retry attempt %d of %d', err || resp[1].error, self.metadataRetriesAttempts, self.options.metadataRetries);

                setTimeout(function () {
                    self.loadMetadataForTopics(topics, cb)
                }, self.options.metadataRetriesBackoffMs)
                return;
            }
        }

        self.metadataRetriesAttempts = 0;
        cb(err, resp);
    }

    var correlationId = this.nextId();
    var request = protocol.encodeMetadataRequest(this.clientId, correlationId, topics);
    var broker = this.brokerForLeader();
    if (!broker) {
        return cbb(new liberrors.TopicsBrokerNotAvailableError(topics));
    }
    this.cbqueue[correlationId] = [protocol.decodeMetadataResponse, cbb];
    broker && broker.write(request);
}

/**
 * Callback for loadMetadataForTopics function
 * @callback Client~loadMetadataForTopicsCallback
 * @param {err} An error that can be one of the following types.
 *   {InvalidArgumentFormatError} Argument topic must be an array of strings.
 *   {TopicsError} One or more topics' have encountered an error while running the requested operation.
 *   {TopicsBrokerNotAvailableError} Broker disconnected or cannot be reached.
 * @param {string[]} response: an array with 2 indexes, 0 containing the brokers and 1 the metadata of the topics.
 */


Client.prototype.createTopics = function (topics, cb) {
    var created = 0;
    topics.forEach(function (topic) {
        this.zk.topicExists(topic, function (existed, reply) {
            if (existed && ++created === topics.length) cb(null, topics.length); 
        }, true);
    }.bind(this));
}

Client.prototype.topicExists = function (topics, cb) {
    var errors = [], count = 0;
    topics.forEach(function (topic) {
        this.zk.topicExists(topic, function (existed, reply) {
            if (!existed) errors.push(reply); 
            if (++count === topics.length) cb(errors.length, errors);
        });
    }.bind(this));
}

Client.prototype.addTopics = function (topics, cb) {
    var self = this;
    this.topicExists(topics, function (err, errors) {
        if (err) return cb(err, errors);
        self.loadMetadataForTopics(topics, function (err, resp) {
            if (err) return cb(err);
            self.updateMetadatas(resp);
            cb(null, topics);
        });
    });
}

Client.prototype.nextId = function () {
    return this.correlationId++;
}

Client.prototype.nextPartition = (function cycle() {
    var c = 0;
    return function (topic) {
        return this.topicPartitions[topic][ c++ % this.topicPartitions[topic].length ];
    }
})();

Client.prototype.refreshBrokers = function (brokerMetadata) {
    this.brokerMetadata = brokerMetadata;
    Object.keys(this.brokers).filter(function (k) {
        return !~_.values(brokerMetadata).map(function (b) { return b.host + ':' + b.port }).indexOf(k);
    }).forEach(function (deadKey) {
        delete this.brokers[deadKey];
    }.bind(this));
}

Client.prototype.refreshMetadata = function (topicNames, cb) {
    var self = this;
    topicNames = topicNames || Object.keys(self.topicMetadata);
    if (!topicNames.length) return cb();
    self.loadMetadataForTopics(topicNames, function (err, resp) {
        if (err) return cb(err);
        if (resp[1].error) return cb(resp[1].error);
        self.updateMetadatas(resp);
        cb();
    });
}

Client.prototype.send = function (payloads, encoder, decoder, cb) {
    var self = this;
    // payloads: [ [metadata exists], [metadata not exists] ]
    payloads = this.checkMetadatas(payloads);
    if (payloads[0].length && !payloads[1].length) {
        this.sendToBrokers(_.flatten(payloads), encoder, decoder, cb);
        return;
    }
    if (payloads[1].length) {
        var topicNames = payloads[1].map(function (p) { return p.topic; });
        this.loadMetadataForTopics(topicNames, function (err, resp) {
            if (err) return cb(err);
            var error = resp[1].error;
            if (error) return cb(error);
            self.updateMetadatas(resp);
            self.sendToBrokers(payloads[1].concat(payloads[0]), encoder, decoder, cb);
        }); 
    }
}

Client.prototype.sendToBrokers = function (payloads, encoder, decoder, cb) {
    payloads = this.payloadsByLeader(payloads, cb);
    if (payloads instanceof Error) {
        return cb(payloads);
    }
    var longpolling = encoder.name === 'encodeFetchRequest';
    for (var leader in payloads) {
        var broker = this.brokerForLeader(leader, longpolling);
        if (longpolling) {
            if (broker.waitting) continue;
            broker.waitting = true;
        }
        this.sendToBroker(broker, payloads[leader], encoder, decoder, cb);
    }
}

Client.prototype.sendToBroker = function (broker, payloads, encoder, decoder, cb) {
    var self = this;
    if (broker.connected) {
        var correlationId = this.nextId();
        var request = encoder.call(null, this.clientId, correlationId, payloads);

        this.cbqueue[correlationId] = [decoder, cb];
        broker && broker.write(request);
    } else {
        // If broker is connecting, retry.
        if (broker._connecting) {
            setTimeout(function () {
                self.sendToBroker(broker, payloads, encoder, decoder, cb);
            }, 100);
        } else {
            var error = new liberrors.TopicsPartitionsError();
            payloads.forEach(function (payload) {
                error._addTopic(payload, 5, protocol.ERROR_CODE[5]); //LeaderNotAvailable
            });

            return cb(error);
        }
    }
}

Client.prototype.checkMetadatas = function (payloads) {
    if (_.isEmpty(this.topicMetadata)) return [ [],payloads ];
    // out: [ [metadata exists], [metadta not exists] ]
    var out = [ [], [] ];
    payloads.forEach(function (p) {
        if (this.hasMetadata(p.topic, p.partition)) out[0].push(p)
        else out[1].push(p)
    }.bind(this));
    return out;
}

Client.prototype.hasMetadata = function (topic, partition) {
    var brokerMetadata = this.brokerMetadata,
        topicMetadata = this.topicMetadata;
    var leader = topicMetadata[topic] && topicMetadata[topic][partition] && topicMetadata[topic][partition]['leader'];
    
    return (leader !== undefined) && brokerMetadata[leader];
}

Client.prototype.updateMetadatas = function (metadatas) {
    _.extend(this.brokerMetadata, metadatas[0]);
    _.extend(this.topicMetadata, metadatas[1].metadata);
    for(var topic in this.topicMetadata) {
        this.topicPartitions[topic] = Object.keys(this.topicMetadata[topic]);
    }
}

Client.prototype.removeTopicMetadata = function (topics, cb) {
    topics.forEach(function (t) {
        if (this.topicMetadata[t]) delete this.topicMetadata[t];
    }.bind(this));
    cb(null, topics.length);
}

Client.prototype.payloadsByLeader = function (payloads) {
    var error = null;
    var result = payloads.reduce(function (out, p) {
        var leader = this.leaderByPartition(p.topic, p.partition);
        if (leader instanceof Error) {
            error = error ? _.merge(error, leader) : leader;
            return;
        }
        if (!error) {
            out[leader] = out[leader] || [];
            out[leader].push(p);
            return out;
        }
    }.bind(this), {});

    return error || result;
}

Client.prototype.leaderByPartition = function (topic, partition) {
    // Return error if attempting to write to a non-existing partition.
    if (this.topicMetadata[topic][partition]) {
        return this.topicMetadata[topic][partition].leader;
    } else {
        var error = new liberrors.TopicsError();
        error._addTopic({ topic: topic, partition: partition }, 3, protocol.ERROR_CODE[3]); // UnknownTopicOrPartition
        return error;
    }
}

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
            this.emit('error', 'Broker not available');
            return;
        }
    } 
    var metadata = this.brokerMetadata[leader],
        addr = metadata.host + ':' + metadata.port;
    brokers[addr] = brokers[addr] || this.createBroker(metadata.host, metadata.port, longpolling);
    return brokers[addr];
}

Client.prototype.createBroker = function connect(host, port, longpolling) {
    console.log('Broker ' + host + ':' + port + ' creating connection.');
    var self = this;
    var socket = net.createConnection(port, host);
    socket.addr = host + ':' + port;
    socket.host = host;
    socket.port = port;
    socket.connected = false;
    if (longpolling) socket.longpolling = true;

    socket.on('connect', function () { 
        console.log ('Broker ' + host + ':' + port + ' has connected.');
        this.connected = true;
        self.emit('connect');
    });
    socket.on('error', function (err) { 
        self.emit('error', 'Broker ' + host + ':' + port + ' has encountered an error: ' + err);
    });
    socket.on('close', function (err) {
        // When a broker disconnects, remove it from list of brokers.
        // Eventually zookeeper will detect the disconnection and the topics' metadata will be refreshed; the topic partition leaders will be updated to reflect the changes.
        var addr = this.host + ':' + this.port;
        if (self.brokers[addr]) {
            delete self.brokers[addr]
            }

        this.connected = false;
        self.emit('close');
    });
    socket.buffer = new Buffer([]);
    socket.on('data', function (data) {
        this.buffer = Buffer.concat([this.buffer, data]);
        self.handleReceivedData(this);
    });
    socket.setKeepAlive(true, 60000);

    return socket;
}

Client.prototype.handleReceivedData = function (socket) {
        var vars = Binary.parse(socket.buffer).word32bu('size').word32bu('correlationId').vars,
            size = vars.size + 4,
            correlationId = vars.correlationId;
        if (socket.buffer.length >= size) {
            var resp = socket.buffer.slice(0, size);
            var handlers = this.cbqueue[correlationId],
                decoder = handlers[0],
                cb = handlers[1];
            // Delete callback functions after finish a request
            delete this.cbqueue[correlationId];

            var decoded = decoder(resp);
            // ProduceResponse and MetadataResponse decoders return the results in a format different than the other decoders (wrapped in a 'response' object to differentiate it from other decoders).
            // TODO: standarize return object with other decoders.
            if (decoded && decoded.response) {
                cb.call(this, decoded.response.error, decoded.response.success);
            } else {
                cb.call(this, null, decoded);
            }

            socket.buffer = socket.buffer.slice(size);
            if (socket.longpolling) socket.waitting = false;
        } else { return }

    if (socket.buffer.length) 
        setImmediate(function () { this.handleReceivedData(socket);}.bind(this));
}

module.exports = Client;
