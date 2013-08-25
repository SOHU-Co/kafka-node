'use strict';

var net  = require('net'),
    util = require('util'),
    events = require('events'),
    Binary = require('binary'),
    _ = require('underscore'),
    protocol = require('./protocol'),
    TopicAndPartition = protocol.TopicAndPartition,
    Message = protocol.Message,
    ProduceRequest = protocol.ProduceRequest;

var Client = function (host, port, clientId) {
    if (this instanceof Client === false) return new Client(host, port, clientId); 
    events.EventEmitter.call(this);
    this.host = host || 'localhost';
    this.port = port || 9092;
    this.clientId = clientId || 'node-client';
    this.brokers = {}
    this.topicMetadata = {};
    this.topicPartitions = {};
    this.correlationId = 0;
    this.cbqueue = {};
    // Default broker
    this.brokerMetadata = {
        'default': {
            host: this.host,
            port: this.port
        } 
    }
    this.connect();
}
util.inherits(Client, events.EventEmitter);

function connect(addr, host, port) {
    var self = this;
    var socket = net.createConnection(port || this.port, host || this.host);
    socket.on('connect', function () { 
        console.log('client connected'); 
        self.emit('connect');
    });
    socket.on('error', function (err) { 
        console.log(err);
        self.emit('error', err);    
    });
    socket.on('close', function (err) {
        console.log(err);
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
    var _default = this.host + ':' + this.port;
    this.brokers[addr || _default] = socket;
    return socket;
}
var count=0;
Client.prototype.connect = Client.prototype.createBroker = connect;

Client.prototype.nextId = function () {
    return this.correlationId++;
}

Client.prototype.nextPartition = (function cycle() {
    var c = 0;
    return function (topic) {
        return this.topicPartitions[topic][ c++ % this.topicPartitions[topic].length ];
    }
})();

Client.prototype.send = function (payloads, encoder, decoder, cb) {
    var self = this;
    payloads = this.checkMetadatas(payloads);
    if (payloads[0].length && !payloads[1].length) {
        this.sendToBroker(_.flatten(payloads), encoder, decoder, cb);
        return;
    }
    if (payloads[1].length) {
        var topicNames = payloads[1].map(function (p) { return p.topic; });
        this.loadMetadataForTopics(topicNames, function (resp) {
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

function blank (obj) {
    for (var o in obj) return false;
    return true;
}

Client.prototype.checkMetadatas = function (payloads) {
    if ( blank(this.topicMetadata) ) return [ [],payloads ]; 
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
    var metadata = this.brokerMetadata[leader],
        addr = metadata.host + ':' + metadata.port;
    return this.brokers[addr] || this.createBroker(addr, metadata.host, metadata.port);
}

Client.prototype.sendFetchRequest = function (payloads, fetchMaxWaitMs, fetchMinBytes) {
    var encoder = protocol.encodeFetchRequest(fetchMaxWaitMs, fetchMinBytes),
        decoder = protocol.decodeFetchResponse(function (err, type, value) {
            if (err) { 
                this.emit('error', err);
            } else {
                type === 'message' ? this.emit('message', value)
                                   : this.emit('done', value);
            }
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
        decoder = protocol.decodeOddsetCommitResponse;
    this.send(payloads, encoder, decoder, cb);
}

Client.prototype.loadMetadataForTopics = function (topics, cb) {
    var correlationId = this.nextId();
    var request = protocol.encodeMetadataRequest(this.clientId, correlationId, topics);
    this.cbqueue[correlationId] = 
        [protocol.decodeMetadataResponse, cb];
    global.m++;
    this.brokerForLeader('default').write(request); 
}

Client.prototype.updateMetadatas = function (metadatas) {
    this.brokerMetadata =  _.extend(this.brokerMetadata, metadatas[0]);
    this.topicMetadata =  _.extend(this.topicMetadata, metadatas[1]);
    for(var topic in metadatas[1]) {
        this.topicPartitions[topic] = Object.keys(metadatas[1][topic]);
    }
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
if (global.s<35000) console.log('s:',global.s,'r:', global.r, 'm:', global.m, 'd:', global.d)
        typeof cb === 'function' ?  cb.call(this, decoder(resp)) : decoder(resp);
        socket.buffer = socket.buffer.slice(size);
    } else { return }

    if (socket.buffer.length) this.handleReceivedData(socket);
}

module.exports = Client;
