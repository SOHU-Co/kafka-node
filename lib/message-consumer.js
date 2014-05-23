'use strict';

var Consumer = require('./consumer'),
    events = require('events'),
    async = require('async'),
    util = require('util'),
    zookeeper = require('node-zookeeper-client'),
    os = require('os'),
    errors = require('./errors');

/*
 * Partition-agnostic message-oriented consumer
 * Represents a single message stream across multiple partitions
 * client: Client
 * topics: [String]
 * options: same as for Consumer
 */
function MessageConsumer (client, initialTopics, options) {
    var self = this;
    this._initialTopics = initialTopics.slice(0);
    this._topicStreamCounts = {};
    this.ready = false;
    this.consumer = new Consumer(client, [], options);
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
}
util.inherits(MessageConsumer, events.EventEmitter);

/*
 * self.thunk('foo', 42) is equivalent to function (cb) { return self.foo(42, cb); }
 */
MessageConsumer.prototype.thunk = function (method) {
    var self = this, args = Array.prototype.slice.call(arguments);
    args.shift(); // => method
    return function (cb) {
        args.push(cb);
        var str = util.inspect(args);
        console.log('this.' + method + '(' + str.substring(2, str.length - 2) + ')');
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
    return this.zk().getChildren(path, watcher, callback);
};

MessageConsumer.prototype.getData = function (path, watcher, callback) {
    return this.zk().getData(path, watcher, callback);
};

MessageConsumer.prototype.setData = function (path, data, version, callback) {
    return this.zk().setData(path, data, version, callback);
};

MessageConsumer.prototype.create = function (path, data, acls, mode, callback) {
    var self = this;
    var myMode = [data, acls, mode].filter(function (myMode) {
        return !(myMode instanceof Buffer || myMode instanceof Array || myMode instanceof Function);
    })[0];
    if (myMode === zookeeper.CreateMode.EPHEMERAL) {
        MessageConsumer.onKilled(function (signal, done) {
            self.zk().remove(path, done);
        });
    }
    return this.zk().create(path, data, acls, mode, callback);
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
    }, callback);
};

// initial connection

MessageConsumer.prototype._registerConsumer = function (cb /* (err) */) {
    var self = this;
    async.parallel([
        function (cb) {
            self.watchChildren('/consumers/' + self.consumer.options.groupId + '/ids', function (err, children, stat) {
                console.log('peers:', children);
                if (cb) {
                    cb(null, children);
                    cb = null;
                } else {
                    self._invalidateBalance();
                }
            });
        },
        //this.thunk('getChildren', '/brokers/ids', function (event) {
        //    console.log('broker registry event', event); // TODO get /brokers/topics/test/partitions/*
        //}),
    ], function(err, vals) {
        if (err) {
            console.log('registration failed');
            cb(err);
            return self.emit('error', err);
        } else {
            //console.log('peers', vals[0][0], 'brokers', vals[1][0]); // XXX pause when no brokers
            console.log('connected to peers', vals[0][0]);
            cb();
        }
    }, cb);
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
    async.retry(3, this.rebalance.bind(this), function (err) {
        if (err) {
            console.error('rebalance failed', err);
        } else {
            console.log('rebalanced');
        }
    });
};

MessageConsumer.prototype.glob = function (path, transform /* (path, child) optional */, cb) {
    var self = this;
    if (!cb) {
        cb = transform;
        transform = null;
    }
    this.getChildren(path, function(err, children, stat) {
        async.map(children, function (child, cb) {
            self.getData(transform ? transform(path, child) : path + '/' + child, function (err, data) {
                cb(err, { child: child, data: data });
            });
        }, cb);
    });
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
            var obj = JSON.parse(peer.data.toString());
            Object.keys(obj.subscription).forEach(function (topic) {
                consumersPerTopicMap[topic].push(peer.child);
            });
        });

        // closeFetchers
        //
        // releasePartitionOwnership

        // copy from https://github.com/apache/kafka/blob/c66e408b244de52f1c5c5bbd7627aa1f028f9a87/core/src/main/scala/kafka/consumer/ZookeeperConsumerConnector.scala#L447
        for (var i = 0; i < topics.length; ++i) {
            partitionsPerTopicMap[topics[i]] = partitions[i].sort(function (a, b) {
                return a - b; // String => Number coercion
            });
        }
        topics.forEach(function (topic) {
            var curConsumers = consumersPerTopicMap[topic];
            var curPartitions = partitionsPerTopicMap[topic].map(Number);
            var nPartsPerConsumer = curPartitions.length / curConsumers.length | 0;
            var nConsumersWithExtraPart = curPartitions.length % curConsumers.length;
            console.info('Consumer ' + self.consumerId + ' rebalancing the following partitions: ' + curPartitions + ' for topic ' + topic + ' with consumers: ' + curConsumers);
            // we only support 1 message stream, so we don't loop through the thread set
            var consumerThreadId = self.consumerId;
            var myConsumerPosition = curConsumers.indexOf(consumerThreadId);
            console.assert(myConsumerPosition >= 0);
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
                    console.info(consumerThreadId + ' attempting to claim partition ' + partition);
                    // addPartitionTopicInfo(currentTopicRegistry, topicDirs, partition, topic, consumerThreadId);
                    // partitionOwnershipDecision += (topic, partition) => consumerThreadId
                }
            }
        });
        cb();
    });
}

var signum = {
    SIGINT: 2,
    SIGTERM: 15,
};
MessageConsumer.onKilled = function (cb) {
    var handlers = [];
    MessageConsumer.onKilled = function (cb) {
        handlers.push(cb);
    };
    MessageConsumer.onKilled(cb);
    var dying = false;
    Object.keys(signum).forEach(function (signal) {
        process.on(signal, function () {
            if (dying) {
                return;
            }
            dying = true;
            async.parallel(handlers.map(function (handler) {
                return function (cb) {
                    if (handler.length == 2) {
                        handler(signal, function (err) {
                            if (err) {
                                console.error('error', err, 'on', signal);
                            }
                            cb(); // finish regardless
                        });
                    } else {
                        handler(signal);
                        cb();
                    }
                };
            }), function () { // never an error
                process.exit(128 + signum[signal]);
            });
        });
    });
};

module.exports = MessageConsumer;
