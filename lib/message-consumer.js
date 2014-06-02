'use strict';

var Consumer = require('./consumer'),
    events = require('events'),
    concurrent = require('./concurrent'),
    async = require('async'),
    util = require('util'),
    dirname = require('path').dirname,
    zookeeper = require('node-zookeeper-client'),
    os = require('os'),
    _ = require('lodash'),
    debug = require('debug')('kafka-node:message-consumer'),
    errors = require('./errors');

/*
 * Partition-agnostic message-oriented consumer
 * Represents a single message stream across multiple partitions
 * TODO deal with degenerate cases (0 brokers, empty topics, etc.)
 * client: Client
 * topics: [String]
 * options: same as for Consumer
 * autoCommit is disabled
 */
function MessageConsumer (client, initialTopics, options) {
    var self = this;
    this._initialTopics = initialTopics.slice(0);
    this._topicStreamCounts = {};
    this.ready = false;
    this.closing = false;
    this.options = { // https://kafka.apache.org/documentation.html#consumerconfigs
        useCommitApi: true,
        likeScala: true,
        rebalanceMaxRetries: Infinity, // TODO move to ZK
        rebalanceBackoffMs: 2000,
        maxPartitionQueueSize: 1, // must be positive
    };
    options = _.extend({}, options, { autoCommit: false }); // XXX needs to run filter
    this.ephemerals = {};
    this.rebalanceLock = concurrent.lock();
    client.on('error', function (err) {
        // this error is forwarded to this.consumer already
    });
    this.consumer = new Consumer(client, [], options);
    this.lastReceivedOffset = {}; // key => int
    this.lastCommittedOffset = {}; // key => int
    this.messageByOffset = {}; // key => int => message payload
    this.pendingCommits = {}; // key => int
    this.partition = {}; // key => EventEmitter
    var origFetch = this.consumer.fetch;
    this.consumer.fetch = function () {
        this.payloads.forEach(function (payload) {
            var key = canonicalPayloadString(payload);
            payload.busy = self.lastCommittedOffset[key] + self.options.maxPartitionQueueSize <= self.lastReceivedOffset[key];
        });
        origFetch.apply(this, arguments);
    };
    this.consumer.on('error', function (err) {
        if (err instanceof errors.BrokerNotAvailableError) {
            throw err;
        }
        debug('re-throwing error', err);
        throw err;
    });
    debug('registered handlers');
    this.consumerId = this.consumer.options.groupId + '_' + os.hostname() + '-' + Date.now() + '-' + randUint32String();
    this.consumer.on('message', function (message) {
        var key = canonicalPayloadString(message);
        console.assert(!(self.lastReceivedOffset[key] >= message.offset));
        console.assert((self.lastReceivedOffset.hasOwnProperty(key) + self.lastCommittedOffset.hasOwnProperty(key) + self.messageByOffset.hasOwnProperty(key)) % 3 === 0);
        self.lastReceivedOffset[key] = message.offset;
        if (!self.lastCommittedOffset.hasOwnProperty(key)) { // we want to emitMessage
            self.lastCommittedOffset[key] = message.offset - 1; // by someone else
        }
        var map = self.messageByOffset[key] = (self.messageByOffset[key] || {});
        map[message.offset] = message;
        if (self.lastCommittedOffset[key] == message.offset - 1) {
            emitMessage(key, message.offset);
        }
        function emitMessage(key, offset) {
            var message = self.messageByOffset[key][offset];
            self.emit('message', message, function () {
                message.metadata = '';
                self.pendingCommits[key] = (self.pendingCommits[key] || 0) + 1;
                self.partition[key] = self.partition[key] || new events.EventEmitter();
                self.commit(message, function (err) {
                    if (err) {
                        console.error('commit failed', err);
                    } // pretend we committed it
                    if (--self.pendingCommits[key] === 0) {
                        self.partition[key].emit('noCommits');
                        delete self.pendingCommits[key];
                        delete self.partition[key];
                    }
                    self.lastCommittedOffset[key] = offset;
                    debug(self.lastCommittedOffset[key], '<', offset, '<=', self.lastReceivedOffset[key]);
                    if (self.lastReceivedOffset[key] == offset) { // no more
                        delete self.lastReceivedOffset[key]; // free memory
                        delete self.lastCommittedOffset[key];
                        delete self.messageByOffset[key];
                        return;
                    } else {
                        console.assert(self.lastReceivedOffset[key] > offset, 'offset = ' + offset + '; self.lastReceivedOffset[' + key + '] = ' + self.lastReceivedOffset[key]);
                        setImmediate(emitMessage, key, offset + 1);
                    }
                });
            });
        }
    });
    this._connect(function (err) {
        if (err) {
            console.error('could not connect', err);
        } else {
            debug('MessageConsumer: initialized!');
        }
    });
    MessageConsumer.onKilled(function (signal, done) {
        self.close(done);
    });
}
util.inherits(MessageConsumer, events.EventEmitter);

MessageConsumer.prototype.onBeforeFetch = function (cb) {
    debug('waiting for before fetch');
    if (this.consumer.fetching) {
        this.consumer.once('done', function () {
            debug('got it!');
            cb(); // discard topics map
        });
    } else {
        cb();
    }
};

MessageConsumer.prototype.onPartitionIdle = function (payload, cb) {
    var key = canonicalPayloadString(payload);
    debug('waiting for partition idle', key);
    if (this.pendingCommits[key]) {
        this.partition[key].once('idle', cb);
    } else {
        cb();
    }
};

/*
 * self.thunk('foo', 42) is equivalent to function (cb) { return self.foo(42, cb); }
 */
MessageConsumer.prototype.thunk = function (method) {
    var self = this, args = Array.prototype.slice.call(arguments);
    args.shift(); // => method
    return function (cb) {
        args.push(cb);
        var str = util.inspect(args);
        if (debug.enabled) {
            console.log('this.' + method + '(' + str.substring(2, str.length - 2) + ')');
        }
        return self[method].apply(self, args);
    };
};

MessageConsumer.prototype.whenReady = function (cb /* () */) {
    if (this.consumer.client.ready) {
        cb();
    } else {
        this.consumer.client.once('ready', cb);
    }
};

// zookeeper convenience

MessageConsumer.prototype.zk = function () {
    return this.consumer.client.zk.client;
};

MessageConsumer.prototype.getChildren = function (path, watcher, callback) {
    this.zk().getChildren(path, watcher, callback);
};

MessageConsumer.prototype.getData = function (path, watcher, callback) {
    this.zk().getData(path, watcher, callback);
};

MessageConsumer.prototype.setData = function (path, data, version, callback) {
    this.zk().setData(path, data, version, callback);
};

MessageConsumer.prototype.create = function (path, data, acls, mode, callback) {
    var self = this;
    var myMode = [data, acls, mode].filter(function (myMode) {
        return !(myMode instanceof Buffer || myMode instanceof Array || myMode instanceof Function);
    })[0];
    if (myMode === zookeeper.CreateMode.EPHEMERAL) {
        this.ephemerals[path] = true;
    }
    var cb = arguments[arguments.length-1];
    this.zk().create(path, data, acls, mode, function (err) {
        if (err && err.name === 'NO_NODE') {
            console.warn('Creating parent node for', path);
            return self.zk().mkdirp(dirname(path), function (err) {
                if (err && err.name !== 'NODE_EXISTS') {
                    console.error('Error creating node', path, err);
                    return cb(err);
                }
                return self.create(path, data, acls, mode, cb);
            });
        }
        return cb.apply(this, arguments);
    });
};

MessageConsumer.prototype.mkdirp = function (path, data, acls, mode, callback) {
    return this.zk().mkdirp(path, data, acls, mode, callback);
};

MessageConsumer.prototype.remove = function (path, version, callback) {
    var self = this;
    if (version instanceof Function) {
        callback = version;
        version = -1;
    }
    this.zk().remove(path, version, function (err) {
        delete self.ephemerals[path];
        return callback.apply(this, arguments);
    });
};

MessageConsumer.prototype.watchData = function (path, callback) {
    var self = this;
    this.getData(path, function (event) {
        switch (event.name) {
            case 'NODE_DELETED':
                return;
            case 'NODE_DATA_CHANGED':
                self.watchData(path, callback);
                return;
            default:
                console.warn('watchData unknown event', event);
        }
    }, callback);
};

MessageConsumer.prototype.getOffset = function (payload, cb) {
    this.getData('/consumers/' + this.consumer.options.groupId + '/offsets/' + payload.topic + '/' + topic.partition, cb);
};

MessageConsumer.prototype.setOffset = function (payload, cb) {
    this.setData('/consumers/' + this.consumer.options.groupId + '/offsets/' + payload.topic + '/' + payload.partition, payload.offset, cb);
};

/*
 * Deletes a partition's ownership
 * payload must not be in this.consumer.payloads
 */
MessageConsumer.prototype.givePartition = function (payload, cb) {
    var self = this;
    this.onPartitionIdle(payload, function () {
        self.remove('/consumers/' + self.consumer.options.groupId + '/owners/' + payload.topic + '/' + payload.partition, function (err) {
            if (err && err.name === 'NO_NODE') {
                return cb();
            }
            return cb.apply(this, arguments);
        });
    });
};

/*
 * Adds a partition's ownership
 * payload must not be in this.consumer.payloads
 */
MessageConsumer.prototype.takePartition = function (payload, cb) {
    var self = this;
    var path = '/consumers/' + this.consumer.options.groupId + '/owners/' + payload.topic + '/' + payload.partition;
    this.create(path, new Buffer(this.consumerId), zookeeper.CreateMode.EPHEMERAL, function (err) {
        if (err) {
            debug('take partition', path, 'failed');
        } else {
            self.consumer.payloads.push(payload);
        }
        return cb.apply(this, arguments);
    });
};

MessageConsumer.prototype.watchChildren = function (path, callback) {
    var self = this;
    this.getChildren(path, function (event) {
        switch (event.name) {
            case 'NODE_DELETED':
                return;
            case 'NODE_CHILDREN_CHANGED':
                self.watchChildren(path, callback);
                return;
            default:
                console.warn('watchChildren unknown event', event);
        }
    }, function (err) {
        if (err && err.name === 'NO_NODE') {
            console.warn('Creating %s to watch', path);
            self.mkdirp(path, function (err) {
                if (err && err.name !== 'NODE_EXISTS') {
                    console.log('Error watching %s: %s', path, err);
                    return callback(err);
                }
                self.watchChildren(path, callback);
            });
            return;
        }
        return callback.apply(this, arguments);
    });
};

// client convenience

MessageConsumer.prototype.fetchOffset = function (payloads, callback) {
    var self = this;
    try {
        this.consumer.fetchOffset(payloads, callback);
    } catch (err) {
        if (err instanceof errors.BrokerNotAvailableError) {
            console.warn('no brokers, waiting for broker');
            return this.consumer.client.once('brokersChanged', function () {
                self.fetchOffset(payloads, callback);
            });
        }
        console.error('fetchOffset error', err);
        throw err;
    }
};

/*
 * commits a series of commits
 * commit: payloads to commit
 * failure: called on failed payloads
 * cb: callback
 */
MessageConsumer.prototype.commit = // TODO batching
MessageConsumer.prototype.sendCommit = function (commit, failure /* (failedPayload) optional */, cb /* (err) */) {
    var self = this;
    var tasks = [];
    if (!(commit instanceof Array)) {
        if (!cb) {
            cb = failure;
            failure = function() {};
        }
        return this.sendCommit([commit], failure, function (err, results) {
            return cb(err, results && results[0]);
        });
    }
    if (self.options.useCommitApi) { // doesn't normalize results
        tasks.push(function (cb) {
            self.consumer.client.sendOffsetCommitRequest(self.consumer.options.groupId, commit, function (err) {
                if (err) {
                    commit.forEach(failure);
                }
                return cb.apply(this, arguments);
            });
        });
    } else {
        tasks = commit.map(function (payload) {
            return function (cb) {
                self.setOffset(payload, function (err) {
                    if (err) {
                        failure(payload);
                    }
                    return cb.apply(this, arguments);
                });
            };
        });
    }
    async.parallel(tasks, cb);
}

// initial connection

MessageConsumer.prototype._registerConsumer = function (cb /* (err) */) {
    var self = this;
    this.watchChildren('/consumers/' + self.consumer.options.groupId + '/ids', function (err, children, stat) {
        if (err) {
            console.log('registration failed');
            cb && cb(err);
            return self.emit('error', err);
        }
        console.log('peers:', children);
        if (cb) {
            cb(null, children);
            cb = null;
        } else { // cb triggers this later
            self._invalidateBalance();
        }
    });
};

MessageConsumer.prototype._connect = function (cb /* (err) */) {
    async.series([
        this.thunk('whenReady'),
        this.thunk('_registerConsumer'),
        this.thunk('addTopics', this._initialTopics),
    ], cb);
};

// topic management

MessageConsumer.prototype.addTopics = function (topics, cb /* (err) */) {
    var self = this, count = this._topicStreamCounts;
    topics.forEach(function (topic) {
        count[topic] = (count[topic] || 0) + 1;
        console.assert(count[topic] == 1, 'cannot multiply subscribe stream to topic');
    });
    self.consumer.client.topicExists(topics, function (err, topics) {
        if (err) {
            cb(err);
            return self.emit('error', new errors.TopicsNotExistError(topics));
        }
        self._sendTopicStreamCounts(cb);
    });
};

MessageConsumer.prototype._sendTopicStreamCounts = function (cb /* (err) */) {
    var path = '/consumers/' + this.consumer.options.groupId + '/ids/' + this.consumerId, data = new Buffer(JSON.stringify({
        version: 1,
        subscription: this._topicStreamCounts,
        pattern: 'static',
        timestamp: Date.now().toString(),
    }));
    if (this.ready) {
        this.setData(path, data, cb);
    } else {
        this.create(path, data, zookeeper.CreateMode.EPHEMERAL, cb);
    }
};

function randUint32String() { // scala client doesn't allow leading 0?
    return (Math.random().toString(16) + '00000000').substr(2, 8);
}

// partitioning

MessageConsumer.prototype._invalidateBalance = function () { // TODO add delay?
    var self = this;
    this.syncedRebalance(function (err) { // this is what scala does, but it's slightly different from what the spec says
        if (err) {
            console.error('rebalance failed', err);
        } else {
            console.log('rebalanced with', self.consumer.payloads.length, 'payloads');
        }
    });
};

MessageConsumer.prototype.syncedRebalance = function (cb) {
    var self = this;
    if (this.closing) {
        debug('closing, not rebalancing');
        return;
    }
    var rebalance = self.rebalanceLock.cancels(self.rebalance.bind(self));
    this.rebalanceLock.cancellable = !self.options.likeScala; // scala just lets the rebalance fail
    debug('queueing rebalance');
    this.rebalanceLock(function (cb) {
        debug('got rebalance lock');
        if (self.options.likeScala) {
            concurrent.retryWithDelay(self.options.rebalanceMaxRetries, self.options.rebalanceBackoffMs, rebalance, cb, function (cb) { // scala gives up all partitions on failure
                console.info('Rebalancing attempt failed. Clearing the cache before the next rebalancing operation is triggered');
                concurrent.parallelLocked(self.consumer.payloads.map(function (payload) {
                    return self.thunk('givePartition', payload);
                }), cb); // let it fail
            });
        } else {
            concurrent.retry(self.options.rebalanceMaxRetries, rebalance, cb);
        }
    }, cb);
};

MessageConsumer.prototype.glob = function (path, transform /* (path, child) optional */, cb) {
    var self = this;
    if (!cb) {
        cb = transform;
        transform = null;
    }
    this.getChildren(path, function(err, children, stat) {
        debug('glob getChildren', err, children);
        if (err) {
            return cb(err);
        }
        async.map(children, function (child, cb) {
            self.getData(transform ? transform(path, child) : path + '/' + child, function (err, data) {
                cb(err, { child: child, data: data });
            });
        }, cb);
    });
};

function canonicalPayloadString(payload) {
    return payload.topic + '.' + payload.partition; // partition is always int or none
};

MessageConsumer.prototype.rebalance = function (cb /* (err) */) {
    var self = this;
    debug('rebalancing');
    var topics = Object.keys(this._topicStreamCounts);
    async.parallel([
        this.thunk('glob', '/consumers/' + this.consumer.options.groupId + '/ids'),
        function (cb) {
            async.map(topics, function (topic, cb) {
                self.getChildren('/brokers/topics/' + topic + '/partitions', cb);
            }, cb);
        },
    ], function (err, vals) {
        debug('trying to rebalance', err, vals);
        if (err) {
            return cb(err);
        }
        var peers = vals[0], partitions = vals[1], consumersPerTopicMap = {}, partitionsPerTopicMap = {};
        for (var i = 0; i < topics.length; ++i) {
            consumersPerTopicMap[topics[i]] = [];
        }
        peers.forEach(function (peer) {
            var obj;
            try {
                obj = JSON.parse(peer.data.toString());
            } catch (err) {
                console.warn('skipping peer', peer);
                return;
            }
            Object.keys(obj.subscription).forEach(function (topic) {
                (consumersPerTopicMap[topic] || []).push(peer.child);
            });
        });

        // copy from https://github.com/apache/kafka/blob/c66e408b244de52f1c5c5bbd7627aa1f028f9a87/core/src/main/scala/kafka/consumer/ZookeeperConsumerConnector.scala#L447
        for (var i = 0; i < topics.length; ++i) {
            partitionsPerTopicMap[topics[i]] = partitions[i].sort(function (a, b) {
                return a - b; // String => Number coercion
            });
        }
        var oldPayloads = self.consumer.payloads, newPayloads = [];
        topics.forEach(function (topic) {
            var curConsumers = consumersPerTopicMap[topic];
            var curPartitions = partitionsPerTopicMap[topic].map(Number);
            var nPartsPerConsumer = curPartitions.length / curConsumers.length | 0;
            var nConsumersWithExtraPart = curPartitions.length % curConsumers.length;
            //console.info('Consumer ' + self.consumerId + ' rebalancing the following partitions: ' + curPartitions + ' for topic ' + topic + ' with consumers: ' + curConsumers);
            // we only support 1 message stream, so we don't loop through the thread set
            var consumerThreadId = self.consumerId;
            var myConsumerPosition = curConsumers.indexOf(consumerThreadId);
            console.assert(myConsumerPosition >= 0, 'cannot find ' + consumerThreadId + ' in ' + curConsumers); // TODO fix race condition on exit
            var startPart = nPartsPerConsumer * myConsumerPosition + Math.min(myConsumerPosition, nConsumersWithExtraPart);
            var nParts = nPartsPerConsumer + (myConsumerPosition + 1 > nConsumersWithExtraPart ? 0 : 1);

            /**
             * Range-partition the sorted partitions to consumers for better locality.
             * The first few consumers pick up an extra partition, if any.
             */
            if (nParts <= 0) {
                console.warn('No broker partitions consumed by consumer thread ' + consumerThreadId + ' for topic ' + topic);
            } else {
                for (var i = startPart; i < startPart + nParts; ++i) {
                    var partition = curPartitions[i];
                    //console.info(consumerThreadId + ' attempting to claim partition ' + partition);
                    newPayloads.push({
                        topic: topic,
                        partition: partition,
                        maxBytes: self.consumer.options.fetchMaxBytes,
                        metadata: '',
                        offset: -1,
                    });

                }
            }
        });
        // end copied algorithm

        var give = [], take = [], intersection = [], gift = {};
        if (self.options.likeScala) {
            give = oldPayloads;
            take = newPayloads;
            give.forEach(function (p) {
                gift[canonicalPayloadString(p)] = p;
            });
        } else {
            oldPayloads.forEach(function (payload) {
                var key = canonicalPayloadString(payload)
                gift[key] = payload;
            });
            newPayloads = newPayloads.map(function (payload) {
                var key = canonicalPayloadString(payload)
                if (gift[key]) {
                    intersection.push(payload = gift[key]);
                    delete gift[key];
                } else {
                    take.push(payload);
                }
                return payload;
            });
            for (var key in gift) {
                if (gift.hasOwnProperty(key)) {
                    give.push(gift[key]);
                }
            }
        }

        potluck = self.rebalanceLock.cancels(potluck);
        if (self.options.likeScala) {
            if (give.length || take.length) {
                potluck(cb);
            } else {
                debug('rebalance would not change anything');
                cb();
            }
        } else {
            concurrent.retryWithDelay(self.options.rebalanceMaxRetries, self.options.rebalanceBackoffMs, potluck, cb);
        }

        function potluck (cb) { // never self.consumer.payloads.push(newPayload)
            async.series([
                self.onBeforeFetch.bind(self),
                function (cb) { // parallel?
                    debug('waiting for commits');
                    self.consumer.payloads = intersection;
                    cb(); // XXX
                },
                function (cb) { // parallel?
                    debug('giving ' + give.length + ' partition(s)');
                    if (!give.length) {
                        return cb();
                    }
                    concurrent.parallelLocked(give.map(function (payload) {
                        return self.thunk('givePartition', payload);
                    }), cb);
                },
                function (cb) { // commit ownership
                    debug('taking ' + take.length + ' partition(s)');
                    if (!take.length) {
                        return cb();
                    }
                    concurrent.parallelLocked(take.map(function (payload) {
                        var thunk = self.rebalanceLock.cancels(self.thunk('takePartition', payload));
                        if (self.options.likeScala) {
                            return thunk;
                        }
                        return function (cb) {
                            concurrent.retry(self.options.rebalanceMaxAttempts, thunk, cb);
                        };
                    }), cb);
                },
                function (cb) { // get offsets
                    if (debug.enabled) {
                        console.assert(newPayloads.map(canonicalPayloadString).sort().toString() === self.consumer.payloads.map(canonicalPayloadString).sort().toString(), { newPayloads: newPayloads, payloads: self.consumer.payloads });
                    }
                    var payloads = newPayloads.filter(function (payload) {
                        return payload.offset === -1; // cached offsets are valid
                    });
                    debug('loading ' + payloads.length + ' partition(s)');
                    if (!payloads.length) {
                        return cb();
                    }
                    if (self.options.useCommitApi) {
                        self.fetchOffset(payloads, function (err, offsets) {
                            if (err) {
                                return cb(err);
                            }
                            payloads.forEach(function (p) {
                                p.offset = offsets[p.topic][p.partition];
                            });
                            cb();
                        });
                    } else {
                        async.parallel(payloads.map(function (payload) {
                            return function (cb) {
                                self.getOffset(payload, function (err, offsetString) {
                                    payload.offset = parseInt(offsetString);
                                    if (isNaN(payload.offset)) {
                                        payload.offset = -1;
                                    }
                                    debug('payload offset:', payload.offset);
                                    cb();
                                });
                            };
                        }), cb);
                    }
                },
                function (cb) {
                    debug('got offsets', { newPayloads: newPayloads, payloads: self.consumer.payloads });
                    var virginPayloads = newPayloads.filter(function (payload) {
                        if (payload.offset >= 0) {
                            ++payload.offset; // https://cwiki.apache.org/confluence/display/KAFKA/Keyed+Messages+Proposal
                            return false;
                        }
                        if (payload.offset === -1) {
                            return true;
                        }
                        console.error('invalid payload offset', payload);
                        cb(new Error('invalid payload offset in ' + JSON.stringify(payload)));
                        return true;
                    });
                    if (!virginPayloads.length) {
                        return cb();
                    }
                    debug('new payloads (seek to start)', virginPayloads);
                    virginPayloads = JSON.parse(JSON.stringify(virginPayloads));
                    virginPayloads.forEach(function (payload) {
                        payload.time = -2;
                        payload.maxNum = 1;
                    });
                    self.consumer.client.sendOffsetRequest(virginPayloads, function (err, offsets) {
                        debug('got offset request', err, offsets);
                        newPayloads.forEach(function (payload) {
                            var offset = ((offsets[payload.topic] || {})[payload.partition] || [])[0];
                            if (offset >= 0) {
                                payload.offset = offset;
                            }
                            if (offset < 0) {
                                console.error('bad offset response', offsets);
                                err = new Error('bad offset response: ' + JSON.stringify(offsets)); // XXX remove this
                            }
                        });
                        if (err) {
                            return cb(err);
                        }
                        cb();
                    });
                },
                function (cb) {
                    //console.log('consuming at', self.consumer.payloads);
                    console.assert(self.consumer.payloads.every(function (payload) { return payload.offset >= 0; }), self.consumer.payloads);
                    self.consumer.ready = true; // wait?
                    self.consumer.ensureFetching();
                    cb();
                },
            ].map(self.rebalanceLock.cancels), cb);
        }
    });
};

var signum = {
    SIGINT: 2,
    SIGTERM: 15,
    uncaughtException: 8 - 128, // uncomment to catch exceptions
};
MessageConsumer.onKilled = function (cb) {
    var handlers = [];
    MessageConsumer.onKilled = function (cb) {
        handlers.push(cb);
    };
    MessageConsumer.onKilled(cb);
    var dying = false;
    Object.keys(signum).forEach(function (signal) {
        process.on(signal, function (arg) {
            var code = 128 + signum[signal];
            if (signal === 'uncaughtException') {
                signal = arg;
            }
            console.info('caught', signal);
            if (dying) {
                return;
            }
            dying = true;
            concurrent.parallelLocked(handlers.map(function (handler) {
                return function (cb) {
                    if (handler.length == 2) {
                        handler(signal, function (err) {
                            if (err) {
                                console.error('error', err, 'on', signal, err.stack);
                            }
                            cb(); // finish regardless
                        });
                    } else {
                        handler(signal);
                        cb();
                    }
                };
            }), function () { // never an error
                process.exit(code);
            });
        });
    });
};

MessageConsumer.prototype._fireSale = function (cb) {
    var self = this;
    concurrent.parallelLocked(Object.keys(self.ephemerals).map(function (path) {
        return function (cb) {
            self.remove(path, cb);
        };
    }), cb);
};

MessageConsumer.prototype.close = function (cb) {
    var self = this;
    this.closing = true;
    var oldCb = cb;
    cb = function (err) {
        if (err) {
            console.error('close error', err, new Error().stack);
        }
        return oldCb.apply(this, arguments);
    };
    async.series([
        self.onBeforeFetch.bind(self),
        function (cb) {
            self.consumer.ready = self.ready = false;
            cb();
        },
        self._fireSale.bind(self),
        function (cb) {
            return self.consumer.close(true, cb);
        },
    ], cb);
};

module.exports = MessageConsumer;
