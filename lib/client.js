'use strict';

var net  = require('net'),
    util = require('util'),
    _ = require('underscore'),
    events = require('events'),
    Binary = require('binary'),
    protocol = require('./protocol'),
    Zookeeper = require('./zookeeper');

var Client = function (connectionString, clientId) {
    if (this instanceof Client === false) return new Client(connectionString, clientId); 
    this.connectionString = connectionString || 'localhost:2181/kafka0.8';
    this.clientId = clientId || 'kafka-node-client';
    this.brokers = {}
    this.topicMetadata = {};
    this.topicPartitions = {};
    this.correlationId = 0;
    this.cbqueue = {};
    this.brokerMetadata = {}
    this.ready = false;
    this.connect();
}
util.inherits(Client, events.EventEmitter);

Client.prototype.connect = function () {
    var zk = this.zk = new Zookeeper(this.connectionString);
    var self = this;
    zk.once('init', function (broker) {
        self.ready = true;
        self.emit('ready');
        self.brokerMetadata = broker;
        for (var b in broker)
            self.createBroker(broker[b].host, broker[b].port);
    });
    zk.on('brokersChanged', function (brokers) {
        console.log('Brokers changed')
        self.refreshBrokers(brokers);
        setTimeout(function () {
            self.emit('brokersChanged');
        }, 5000);
    });
}

Client.prototype.createBroker = function connect(host, port) {
    var self = this;
    var socket = net.createConnection(port, host);
    socket.addr = host + ':' + port;
    socket.on('connect', function () { 
        console.log('client connected'); 
        process.nextTick(function () {
            self.emit('connect');
        });
    });
    socket.on('error', function (err) { 
        console.log(err);
        self.emit('error', err);    
    });
    socket.on('close', function (err) {
        self.emit('close');
        console.log('close',err);
    });
    socket.on('end', function () {
        console.log('end');
    });
    socket.buffer = new Buffer([]);
    //this.buffer = { data: new Buffer('') };
    socket.on('data', function (data) {
global.r++;
//console.log(global.r)
        this.buffer = Buffer.concat([this.buffer, data]);
        self.handleReceivedData(this);
    });
    socket.setKeepAlive(true, 60000);
    this.brokers[host + ':' + port] = socket;
    return socket;
}

Client.prototype.sendProduceRequest = function (payloads, requireAcks, ackTimeoutMs, cb) {
    var encoder = protocol.encodeProduceRequest(requireAcks, ackTimeoutMs),
        decoder = protocol.decodeProduceResponse;
    this.send(payloads, encoder, decoder, cb);
}

Client.prototype.sendFetchRequest = function (consumer, payloads, fetchMaxWaitMs, fetchMinBytes) {
    var topics = {},
        encoder = protocol.encodeFetchRequest(fetchMaxWaitMs, fetchMinBytes),
        decoder = protocol.decodeFetchResponse(function (err, type, value) {
            if (err) return consumer.emit('error', err);
            if (type === 'message') {
                consumer.emit('message', value);
            } else {
                _.extend(topics, value);
                if (Object.keys(topics).length < payloads.length) return;
                consumer.emit('done', topics);
            }
        });
    this.send(payloads, encoder, decoder, function (err) {
        if (err) consumer.emit('error', err);
    }); 
}

Client.prototype.sendOffsetCommitRequest = function (group, payloads, cb) {
    var encoder = protocol.encodeOffsetCommitRequest(group),
        decoder = protocol.decodeOffsetCommitResponse;
    this.send(payloads, encoder, decoder, cb);
}

Client.prototype.sendOffsetFetchRequest = function (group, payloads, cb) {
    var encoder = protocol.encodeOffsetFetchRequest(group),
        decoder = protocol.decodeOffsetFetchResponse;
    this.send(payloads, encoder, decoder, cb);
}

Client.prototype.loadMetadataForTopics = function (topics, cb) {
    var correlationId = this.nextId();
    var request = protocol.encodeMetadataRequest(this.clientId, correlationId, topics);
    this.cbqueue[correlationId] = [protocol.decodeMetadataResponse, cb];
    global.m++;
    this.brokerForLeader().write(request); 
}

Client.prototype.createTopics = function (topics, cb) {
    var created = 0;
    topics.forEach(function (topic) {
        this.zk.topicExists(topic, function (err, reply) {
            if (!err && ++created === topics.length) cb(null, topics.length); 
        });
    }.bind(this));
}

Client.prototype.addTopics = function (topics, cb) {
    var count = 0,
        self = this;
    topics.forEach(function (topic) {
        self.zk.topicExists(topic, function (err, reply) {
            if (err) return cb(topic + ' not exists');
            if (++count === topics.length) {
                self.loadMetadataForTopics(topics, function (err, resp) {
                    self.updateMetadatas(resp);
                    cb(null, topics.length);
                });            
            }
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
        return _.values(brokerMetadata).map(function (b) { return b.host + ':' + b.port }).indexOf(k) === -1;
    }).forEach(function (deadKey) {
        delete this.brokers[deadKey];
    }.bind(this));
}

Client.prototype.refreshMetadata = function (topicNames, cb) {
    topicNames = topicNames || Object.keys(this.topicMetadata);
    if (!topicNames.length) return;
    this.loadMetadataForTopics(topicNames, function (err, resp) {
        this.updateMetadatas(resp);
        cb && cb();
    }.bind(this));
}

Client.prototype.send = function (payloads, encoder, decoder, cb) {
    var self = this;
    // payloads: [ [metadata exists], [metadta not exists] ]
    payloads = this.checkMetadatas(payloads);
    if (payloads[0].length && !payloads[1].length) {
        this.sendToBroker(_.flatten(payloads), encoder, decoder, cb);
        return;
    }
    if (payloads[1].length) {
        var topicNames = payloads[1].map(function (p) { return p.topic; });
        this.loadMetadataForTopics(topicNames, function (err, resp) {
            if (err) return cb(err);
            var error = resp[1].error;
            if (error) return cb(error);
            self.updateMetadatas(resp);
            self.sendToBroker(payloads[1].concat(payloads[0]), encoder, decoder, cb);
        }); 
    }
}

Client.prototype.sendToBroker = function (payloads, encoder, decoder, cb) {
    payloads = this.payloadsByLeader(payloads);
    for (var leader in payloads) {
        var correlationId = this.nextId();
        var request = encoder.call(null, this.clientId, correlationId, payloads[leader]);
        var broker = this.brokerForLeader(leader); 
        this.cbqueue[correlationId] = [decoder, cb];
global.s++;
        broker.write(request);
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
    var leader = topicMetadata && topicMetadata[topic] && topicMetadata[topic][partition];
    
    return leader && brokerMetadata[leader];
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
    cb(null, topics.length + ' topics removed');
}

Client.prototype.payloadsByLeader = function (payloads) {
    return payloads.reduce(function (out, p) {
        var leader = this.leaderByPartition(p.topic, p.partition);
        out[leader] = out[leader] || [];
        out[leader].push(p);
        return out;
    }.bind(this), {});
}

Client.prototype.leaderByPartition = function (topic, partition) {
    return this.topicMetadata[topic][partition].leader;
}

Client.prototype.brokerForLeader = function (leader) {
    // If leader is not give, choose the first broker as leader
    if (typeof leader === 'undefined' && !_.isEmpty(this.brokers)) {
        var addr = Object.keys(this.brokers)[0];
        return this.brokers[addr];
    } 
    leader = leader || Object.keys(this.brokerMetadata)[0];
    var metadata = this.brokerMetadata[leader],
        addr = metadata.host + ':' + metadata.port;
    return this.brokers[addr] || this.createBroker(metadata.host, metadata.port);
}



var global = {s:0, r:0, m:0, d: 0};
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
global.d++;
//if (global.s<35000) console.log('s:',global.s,'r:', global.r, 'm:', global.m, 'd:', global.d)
        cb.call(this, null, decoder(resp));
        socket.buffer = socket.buffer.slice(size);
    } else { return }

    if (socket.buffer.length) this.handleReceivedData(socket);
}

module.exports = Client;
