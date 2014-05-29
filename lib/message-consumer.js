'use strict';

var Consumer = require('./consumer'),
    events = require('events'),
    async = require('async'),
    util = require('util'),
    dirname = require('path').dirname,
    zookeeper = require('node-zookeeper-client'),
    os = require('os'),
    errors = require('./errors');

/*
 * Partition-agnostic message-oriented consumer
 * Represents a single message stream across multiple partitions
 * TODO deal with degenerate cases (0 brokers, empty topics, etc.)
 * client: Client
 * topics: [String]
 * options: same as for Consumer
 */
function MessageConsumer (client, initialTopics, options) {
    var self = this;
    this._initialTopics = initialTopics.slice(0);
    this._topicStreamCounts = {};
    this.ready = false;
    this.options = { // https://kafka.apache.org/documentation.html#consumerconfigs
        useCommitApi: true,
        likeScala: true,
        rebalanceMaxRetries: Infinity, // TODO move to ZK
        rebalanceBackoffMs: 2000,
    };
    this.ephemerals = {};
    this.fetching = false;
    this.rebalanceLock = makeLock();
    client.on('error', function (err) {
        // this error is forwarded to this.consumer already
    });
    this.consumer = new Consumer(client, [], options);
    this.consumer.on('error', function (err) {
        if (err instanceof errors.BrokerNotAvailableError) {
            throw err;
        }
        console.log('re-throwing error', err);
        throw err;
    });
    console.log('registered handlers');
    this.consumerId = this.consumer.options.groupId + '_' + os.hostname() + '-' + Date.now() + '-' + randUint32String();
    this.consumer.on('message', function() {
        self.emit.apply(self, ['message'].concat(Array.prototype.slice.call(arguments)));
    });
    this._connect(function (err) {
        if (err) {
            console.error('could not connect', err);
        } else {
            console.log('MessageConsumer: initialized!');
        }
    });
    MessageConsumer.onKilled(function (signal, done) {
        self.close(done);
    });
}
util.inherits(MessageConsumer, events.EventEmitter);

MessageConsumer.prototype.onBeforeFetch = function (cb) {
    if (this.fetching) {
        this.consumer.once('done', function () {
            cb(); // discard topics map
        });
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
        //console.log('this.' + method + '(' + str.substring(2, str.length - 2) + ')');
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
    this.remove('/consumers/' + this.consumer.options.groupId + '/owners/' + payload.topic + '/' + payload.partition, function (err) {
        if (err && err.name === 'NO_NODE') {
            return cb();
        }
        return cb.apply(this, arguments);
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
            console.log('take partition', path, 'failed');
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

MessageConsumer.prototype._invalidateBalance = function () { // TODO add delay
    this.syncedRebalance(function (err) { // this is what scala does, but it's slightly different from what the spec says
        if (err) {
            console.error('rebalance failed', err);
        } else {
            console.log('rebalanced');
        }
    });
};

MessageConsumer.prototype.syncedRebalance = function (cb) { // XXX sync
    var self = this;
    console.log('queueing rebalance');
    this.rebalanceLock(function (cb) {
        console.log('got rebalance lock');
        if (self.options.likeScala) {
            retryWithDelay(self.options.rebalanceMaxRetries, self.options.rebalanceBackoffMs, self.rebalance.bind(self), cb, function (cb) { // scala gives up all partitions on failure
                console.info('Rebalancing attempt failed. Clearing the cache before the next rebalancing operation is triggered');
                async.parallel(self.consumer.payloads.map(function (payload) {
                    return self.thunk('givePartition', payload);
                }), cb);
            });
        } else {
            async.retry(self.options.rebalanceMaxRetries, self.rebalance.bind(self), cb);
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
    var topics = Object.keys(this._topicStreamCounts);
    async.parallel([
        this.thunk('glob', '/consumers/' + this.consumer.options.groupId + '/ids'),
        function (cb) {
            async.map(topics, function (topic, cb) {
                self.getChildren('/brokers/topics/' + topic + '/partitions', cb);
            }, cb);
        },
    ], function (err, vals) {
        if (err) {
            cb(err);
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
            console.assert(myConsumerPosition >= 0, 'cannot find ' + consumerThreadId + ' in ' + curConsumers); // XXX happens on exit
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
            newPayloads.forEach(function (payload) {
                var key = canonicalPayloadString(payload)
                if (gift[key]) {
                    intersection.push(payload);
                    delete gift[key];
                } else {
                    take.push(payload);
                }
            });
            for (var key in gift) {
                if (gift.hasOwnProperty(key)) {
                    give.push(gift[key]);
                }
            }
            //console.log('give', give, 'take', take);
        }

        if (self.options.likeScala) {
            if (give.length || take.length) {
                potluck(cb);
            } else {
                console.log('rebalance would not change anything');
                cb();
            }
        } else {
            retryWithDelay(self.options.rebalanceMaxRetries, self.options.rebalanceBackoffMs, potluck, cb);
        }

        function potluck (cb) {
            async.series([
                function (cb) {
                    var commit = JSON.parse(JSON.stringify(give)).filter(function (payload) {
                        return --payload.offset >= 0;
                    });
                    console.log('saving ' + commit.length + ' partition(s), filtered', give.length - commit.length);
                    self.onBeforeFetch(function () {
                        console.log('committing gifts');
                        self.consumer.payloads = intersection;
                        var tasks = [];
                        if (self.options.useCommitApi) {
                            if (commit.length) {
                                tasks.push(function (cb) {
                                    self.consumer.client.sendOffsetCommitRequest(self.consumer.options.groupId, commit, function (err) {
                                        if (err) {
                                            self.consumer.payloads = oldPayloads;
                                        }
                                        return cb.apply(this, arguments);
                                    });
                                });
                            }
                        } else {
                            tasks = commit.map(function (payload) {
                                return function (cb) {
                                    self.setOffset(payload, function (err) {
                                        if (err) {
                                            self.consumer.payloads.push(payload);
                                            // give.splice(give.indexOf(payload), 1); // will not be used
                                        }
                                        return cb.apply(this, arguments);
                                    });
                                };
                            });
                        }
                        async.parallel(tasks, cb);
                    });
                },
                function (cb) {
                    console.log('giving ' + give.length + ' partitions');
                    async.parallel(give.map(function (payload) {
                        return self.thunk('givePartition', payload);
                    }), cb);
                },
                function (cb) { // commit ownership
                    console.log('taking ' + take.length + ' partitions');
                    async.parallel(take.map(function (payload) {
                        var thunk = self.thunk('takePartition', payload);
                        if (self.options.likeScala) {
                            return thunk;
                        }
                        return function (cb) {
                            async.retry(self.options.rebalanceMaxAttempts, thunk, cb);
                        };
                    }), cb);
                },
                function (cb) { // get offsets
                    console.log('loading ' + take.length + ' partitions');
                    if (self.options.useCommitApi) {
                        self.fetchOffset(take, function (err, offsets) {
                            if (err) {
                                return cb(err);
                            }
                            take.forEach(function (p) {
                                p.offset = offsets[p.topic][p.partition];
                            });
                            cb();
                        });
                    } else {
                        async.parallel(take.map(function (payload) {
                            return function (cb) {
                                self.getOffset(payload, function (err, offsetString) {
                                    payload.offset = parseInt(offsetString);
                                    if (isNaN(payload.offset)) {
                                        payload.offset = -1;
                                    }
                                    console.log('payload offset:', payload.offset);
                                    cb();
                                });
                            };
                        }), cb);
                    }
                },
                function (cb) {
                    console.log('got offsets');
                    var virginPayloads = take.filter(function (payload) {
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
                    console.log('new payloads (seek to start)', virginPayloads);
                    virginPayloads = JSON.parse(JSON.stringify(virginPayloads));
                    virginPayloads.forEach(function (payload) {
                        payload.time = -2;
                        payload.maxNum = 1;
                    });
                    self.consumer.client.sendOffsetRequest(virginPayloads, function (err, offsets) {
                        console.log('got offset request', err, offsets);
                        take.forEach(function (payload) {
                            var offset = ((offsets[payload.topic] || {})[payload.partition] || [])[0];
                            if (!(offset >= 0)) {
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
                    console.log('consuming at', newPayloads);
                    self.consumer.ready = true;
                    self.fetching = true;
                    self.consumer.fetch();
                    cb();
                },
            ], cb);
        }
    });
};

var signum = {
    SIGINT: 2,
    SIGTERM: 15,
    //uncaughtException: 8 - 128, // uncomment to catch exceptions
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
            async.parallel(handlers.map(function (handler) {
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
    async.parallel(Object.keys(self.ephemerals).map(function (path) {
        return function (cb) {
            self.remove(path, cb);
        };
    }), cb);
};

MessageConsumer.prototype.close = function (cb) {
    var self = this;
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
            self.ready = false;
            cb();
        },
        self._fireSale.bind(self),
        function (cb) {
            return self.consumer.close(true, cb);
        },
    ], cb);
};

function retryWithDelay (times, delay, task, cb, failure) {
    failure = failure || function (cb) { cb(); };
    async.retry(times, function (cb) {
        task(function (err) {
            var thiz = this, argumentz = arguments;
            if (err) {
                failure(function () {
                    setTimeout(function () {
                        cb.apply(thiz, argumentz);
                    }, delay);
                });
            } else {
                cb.apply(this, arguments);
            }
        });
    }, cb);
}

function makeLock () {
    var waiters = {}, start = 0, end = 0, locked = false;
    return function (txn, cb) {
        if (locked) {
            waiters[end++] = acquired;
        } else {
            acquired();
        }
        function acquired () {
            locked = true;
            try {
                txn(function (err) {
                    locked = false;
                    cb && cb.apply(this, arguments);
                    if (start < end) {
                        setImmediate(waiters[start++]); // same guarantee as java
                    }
                });
            } catch (err) {
                if (locked) {
                    locked = false;
                    cb && cb.apply(this, arguments);
                }
                if (start < end) {
                    setImmediate(waiters[start++]); // same guarantee as java
                }
                throw err;
            }
        }
    };
}

module.exports = MessageConsumer;
