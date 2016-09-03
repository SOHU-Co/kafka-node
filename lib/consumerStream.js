'use strict';
var util = require('util');
var _ = require('lodash');
var Readable = require('stream').Readable;
var debug = require('debug');

var protocol = require('./protocol');

var DEFAULTS = {
    groupId: 'kafka-node-group',
    // Auto commit config
    autoCommit: true,
    autoCommitMsgCount: 100,
    autoCommitIntervalMs: 5000,
    // Fetch message config
    fetchMaxWaitMs: 100,
    fetchMinBytes: 1,
    fetchMaxBytes: 1024 * 1024,
    fromOffset: false,
    encoding: 'utf8'
}

// WTF, why?
var nextId = (function () {
    var id = 0;
    return function () {
        return id++;
    }
})();

var ConsumerStream = function (client, topics, options) {
    options.objectMode = true;
    Readable.call(this, options);
    if (!topics) {
        throw new Error('Must have payloads');
    }
    this.fetchCount = 0;
    this.client = client;
    this.options = _.defaults( (options||{}), DEFAULTS );
    this.ready = false;
    this.paused = this.options.paused;
    this.id = nextId();
    this.payloads = this.buildPayloads(topics);
    this.connect();
    this.encoding = this.options.encoding;
    this.emittedMessages = 0;
};
util.inherits(ConsumerStream, Readable);

/**
 * Note, this is basically copied from client.sendFetchRequest().
 */
ConsumerStream.prototype._read = function (bytes) {
    var self = this;
    var encoder = protocol.encodeFetchRequest(this.fetchMaxWaitMs, this.fetchMinBytes);
    var decoder = protocol.decodeFetchResponse(this.decoder.bind(this), this.maxTickMessages);
    this.client.send(this.payloads, encoder, decoder, function (err) {
        if (err) {
          console.log('error', err);
            Array.prototype.unshift.call(arguments, 'error');
            this.emit.apply(consumer, arguments);
        }
    });
};

ConsumerStream.prototype.decoder = function (err, type, message) {
    if (err) {
        if (err.message === 'OffsetOutOfRange') {
            return this.emit('offsetOutOfRange', err);
        } else if (err.message === 'NotLeaderForPartition') {
            return self.emit('brokersChanged');
        }
        return this.emit('error', err);
    }

    var encoding = this.encoding;

    if (type === 'message') {
        if (encoding !== 'buffer' && message.value) {
            message.value = message.value.toString(encoding);
        }
        this.emit('message', message);
        this.push(message);
    } else {
        this.emit('done', message);
    }
};

ConsumerStream.prototype.connect = function () {
    var self = this;
    // Client already exists
    this.ready = this.client.ready;
    if (this.ready) this.init();

    this.client.on('ready', function () {
        self.emit('readable');
        debug('consumer ready');
        if (!self.ready) self.init();
        self.ready = true;
    });

    this.client.on('error', function (err) {
        debug('client error %s', err.message);
        self.emit('error', err);
    });

    this.client.on('close', function () {
        debug('connection closed');
    });

    this.client.on('brokersChanged', function () {
        var topicNames = self.payloads.map(function (p) {
            return p.topic;
        });

        this.refreshMetadata(topicNames, function (err) {
            if (err) return self.emit('error', err);
            self.fetch();
        });
    });
    // TODO: Refactor this whole "done" thing - it's bullshit.
    // 'done' will be emit when a message fetch request complete
    this.on('done', function (topics) {
        //self.updateOffsets(topics);
        setImmediate(function () {
            self.fetch();
        });
    });
}

ConsumerStream.prototype.fetch = function () {
    if (!this.ready || this.paused) return;
    this.client.sendFetchRequest(this, this.payloads, this.options.fetchMaxWaitMs, this.options.fetchMinBytes);
};

ConsumerStream.prototype.close = function (force, cb) {
    this.ready = false;
    if (typeof force === 'function') {
        cb = force;
        force = false;
    }

    if (force) {
        this.commit(force, function (err) {
            this.client.close(cb);
        }.bind(this));
    } else {
        this.client.close(cb);
    }
};

ConsumerStream.prototype.init = function () {
    if (!this.payloads.length) {
        return;
    }

    var self = this;
    var topics = self.payloads.map(function (p) { return p.topic; });

    self.client.topicExists(topics, function (err) {
        if (err) {
            return self.emit('error', err);
        }

        /*
        // TODO: REMOVE THIS!
        if (self.options.fromOffset)
            return self.fetch();

        self.fetchOffset(self.payloads, function (err, topics) {
            if (err) {
                return self.emit('error', err);
            }

            self.updateOffsets(topics, true);
            self.fetch();
        });
        */
    });
};

ConsumerStream.prototype.buildPayloads = function (payloads) {
    var self = this;
    return payloads.map(function (p) {
        if (typeof p !== 'object') p = { topic: p };
        p.partition = p.partition || 0;
        p.offset = p.offset || 0;
        p.maxBytes = self.options.fetchMaxBytes;
        p.metadata = 'm'; // metadata can be arbitrary
        return p;
    });
};


module.exports = ConsumerStream;
