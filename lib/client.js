'use strict';

var net  = require('net'),
    util = require('util'),
    _ = require('underscore'),
    events = require('events'),
    Binary = require('binary'),
    protocol = require('./protocol'),
    Zookeeper = require('./zookeeper'),
    TopicAndPartition = protocol.TopicAndPartition,
    Message = protocol.Message,
    ProduceRequest = protocol.ProduceRequest;

var Client = function (connectionString, clientId) {
    if (this instanceof Client === false) return new Client(host, port, clientId); 
    events.EventEmitter.call(this);
    this.connectionString = connectionString || 'localhost:2181';
    this.clientId = clientId || 'kafka-node-client';
    this.brokers = {}
    this.topicMetadata = {};
    this.topicPartitions = {};
    this.correlationId = 0;
    this.cbqueue = {};
    this.brokerMetadata = {}
    this.connect();
}
util.inherits(Client, events.EventEmitter);

Client.prototype.connect = function () {
    var zk = this.zk = new Zookeeper(this.connectionString);
    var self = this;
    zk.once('init', function (broker) {
        self.emit('ready');
        self.brokerMetadata = broker;
        for (var b in broker)
            self.createBroker(broker[b].host, broker[b].port);
    });
    zk.on('brokersChanged', function (brokers) {
        self.refreshBrokers(brokers);
        self.refreshMetadata();
    });
}

Client.prototype.createBroker = function connect(host, port) {
    var self = this;
    var socket = net.createConnection(port, host);
    socket.addr = host + ':' + port;
    socket.on('connect', function () { 
        console.log('client connected'); 
        self.emit('connect');
    });
    socket.on('error', function (err) { 
        console.log(err);
        self.emit('error', err);    
    });
    socket.on('close', function (err) {
        console.log('close',err);
    });
    socket.on('end', function (err) {
        console.log('end', err);
    });
    socket.buffer = new Buffer([]);
    //this.buffer = { data: new Buffer('') };
    socket.on('data', function (data) {
global.r++;
        this.buffer = Buffer.concat([this.buffer, data]);
        self.handleReceivedData(this);
        //self.test(data);
    });
    socket.setKeepAlive(true, 1000);
    this.brokers[host + ':' + port] = socket;
    return socket;
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

Client.prototype.refreshBrokers = function (brokers) {
    this.brokerMetadata = brokers;
    Object.keys(this.brokers).filter(function (k) {
        return _.values(brokers).map(function (b) { return b.host + ':' + b.port }).indexOf(k) === -1;
    }).forEach(function (deadKey) {
        delete this.brokers[deadKey];
    }.bind(this));
}

Client.prototype.refreshMetadata = function () {
    var topicNames = Object.keys(this.topicMetadata);
    if (!topicNames.length) return;
    this.loadMetadataForTopics(topicNames, function (err, resp) {
        this.updateMetadatas(resp);
    }.bind(this));
}

Client.prototype.checkMetadatas = function (payloads) {
    if ( _.isEmpty(this.topicMetadata) ) return [ [],payloads ]; 
    // out: [ [metadata exists], [metadta not exists] ]
    var out = [ [], [] ];
    payloads.forEach(function (p) {
        if (this.hasMetadata(p.topic, p.partition)) out[0].push(p)
        else out[1].push(p)
    }.bind(this)); 
    return out;
}

Client.prototype.hasMetadata = function (topic, partition, topicMetadata) {
    topicMetadata = topicMetadata || this.topicMetadata;
    return topicMetadata && topicMetadata[topic] && topicMetadata[topic][partition];
}

Client.prototype.updateMetadatas = function (metadatas) {
    this.brokerMetadata =  _.extend(this.brokerMetadata, metadatas[0]);
    this.topicMetadata =  _.extend(this.topicMetadata, metadatas[1]);
    for(var topic in metadatas[1]) {
        this.topicPartitions[topic] = Object.keys(metadatas[1][topic]);
    }
}

Client.prototype.removeTopicMetadata = function (topics) {
    topics.forEach(function (t) {
        if (this.topicMetadata[t]) delete this.topicMetadata[t];
    }.bind(this));
}

Client.prototype.send = function (payloads, encoder, decoder, cb) {
    var self = this;
    payloads = this.checkMetadatas(payloads);
    if (payloads[0].length && !payloads[1].length) {
        this.sendToBroker(_.flatten(payloads), encoder, decoder, cb);
        return;
    }
    if (payloads[1].length) {
        var topicNames = payloads[1].map(function (p) { return p.topic; });
        this.loadMetadataForTopics(topicNames, function (err, resp) {
            if (err) return cb(err);
            console.log(resp);

            var metadata = resp[1],
                error = false;
            payloads[1].forEach(function (p) {
                if (!self.hasMetadata(p.topic, p.partition, metadata)) error = true;
            });
            if (error) return cb('error');
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
    // If leader is not give, choose the first as leader
    leader = leader || Object.keys(this.brokerMetadata)[0];
    var metadata = this.brokerMetadata[leader],
        addr = metadata.host + ':' + metadata.port;
    return this.brokers[addr] || this.createBroker(metadata.host, metadata.port);
}

Client.prototype.sendFetchRequest = function (payloads, fetchMaxWaitMs, fetchMinBytes) {
    var encoder = protocol.encodeFetchRequest(fetchMaxWaitMs, fetchMinBytes),
        decoder = protocol.decodeFetchResponse(function (err, type, value) {
            if (err) 
                this.emit('error', err);
            else
                type === 'message' ? this.emit('message', value)
                                   : this.emit('done', value);
        }.bind(this));
    this.send(payloads, encoder, decoder, 'message');
}

Client.prototype.sendProduceRequest = function (payloads, requireAcks, ackTimeoutMs, cb) {
    var encoder = protocol.encodeProduceRequest(requireAcks, ackTimeoutMs),
        decoder = protocol.decodeProduceResponse;
    this.send(payloads, encoder, decoder, cb);
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
        var reply = decoder(resp);
        var errorCode = reply ? reply.errorCode : null;
        if (typeof cb === 'function')
            errorCode ? cb.call(this, ERROR_CODE[errorCode]) : cb.call(this, null, reply);
        socket.buffer = socket.buffer.slice(size);
    } else { return }

    if (socket.buffer.length) this.handleReceivedData(socket);
}

module.exports = Client;
