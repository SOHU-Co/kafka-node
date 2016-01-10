'use strict';

var util = require('util'),
    _ = require('lodash'),
    events = require('events'),
    Client = require('./client'),
    protocol = require('./protocol'),
    uuid = require('node-uuid'),
    Offset = require('./offset'),
    async = require("async"),
    errors = require('./errors'),
    zk = require('./zookeeper'),
    ZookeeperConsumerMappings = zk.ZookeeperConsumerMappings,
    retry = require('retry'),
    debug = require('debug')('kafka-node:HighLevelConsumer');

var DEFAULTS = {
    groupId: 'kafka-node-group',
    // Auto commit config
    autoCommit: true,
    autoCommitMsgCount: 100,
    autoCommitIntervalMs: 5000,
    // Fetch message config
    fetchMaxWaitMs: 100,
    paused: false,
    maxNumSegments: 1000,
    fetchMinBytes: 1,
    fetchMaxBytes: 1024 * 1024,
    maxTickMessages: 1000,
    fromOffset: false
};

var nextId = (function () {
    var id = 0;
    return function () {
        return id++;
    }
})();

var HighLevelConsumer = function (client, topics, options) {
    if (!topics) {
        throw new Error('Must have payloads');
    }
    this.fetchCount = 0;
    this.client = client;
    this.options = _.defaults((options || {}), DEFAULTS);
    this.initialised = false;
    this.ready = false;
    this.paused = this.options.paused;
    this.rebalancing = false;
    this.id = this.options.id || this.options.groupId + '_' + uuid.v4();
    this.payloads = this.buildPayloads(topics);
    this.topicPayloads = this.buildTopicPayloads(topics);
    this.connect();
};
util.inherits(HighLevelConsumer, events.EventEmitter);

HighLevelConsumer.prototype.buildPayloads = function (payloads) {
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

// Initially this will always be empty - until after a re-balance
HighLevelConsumer.prototype.buildTopicPayloads = function (topics) {
    var self = this;
    return topics.map(function (j) {
        var k = { topic: j.topic };
        return k;
    });
};

// Provide the topic payloads if requested
HighLevelConsumer.prototype.getTopicPayloads = function () {
    if (!this.rebalancing) return this.topicPayloads;
    return null;
};

HighLevelConsumer.prototype.connect = function () {
    var self = this;
    //Client alreadyexists
    if (this.client.ready)
        this.init();

    this.client.on('ready', function () {
        if (!self.initialised) self.init();

        // Check the topics exist and create a watcher on them
        var topics = self.payloads.map(function (p) {
            return p.topic;
        })

        self.client.topicExists(topics, function (err) {
            if (err) {
                return self.emit('error', err);
            }
            self.initialised = true;
        });
    });

    // Wait for the consumer to be ready
    this.on('rebalanced', function () {
        self.fetchOffset(self.topicPayloads, function (err, topics) {
            if (err) {
                return self.emit('error', err);
            }

            self.ready = true;
            self.updateOffsets(topics, true);
            self.fetch();
        });
    });

    // Check partition ownership
    this.checkPartitionOwnershipInterval = setInterval(function () {
        if (!self.rebalancing) {
            async.each(self.topicPayloads, function (tp, cbb) {
                if (tp['partition'] !== undefined) {
                    self.client.zk.checkPartitionOwnership(self.id, self.options.groupId, tp['topic'], tp['partition'], function (err) {
                        if (err) {
                            cbb(err);
                        }
                        else cbb();
                    });
                }
                else {
                    cbb();
                }
            }, function (err) {
                if (err)
                    self.emit('error', new errors.FailedToRegisterConsumerError(err.toString()));
            });
        }
    }, 20000);

    function rebalance() {

        if (!self.rebalancing) {
            deregister();

            self.emit('rebalancing');

            self.rebalancing = true;
            // Nasty hack to retry 3 times to re-balance - TBD fix this
            var oldTopicPayloads = self.topicPayloads;
            var operation = retry.operation({
                retries: 10,
                factor: 2,
                minTimeout: 1 * 100,
                maxTimeout: 1 * 1000,
                randomize: true
            });

            operation.attempt(function (currentAttempt) {
                self.rebalanceAttempt(oldTopicPayloads, function (err) {
                    if (operation.retry(err)) {
                        return;
                    }
                    if (err) {
                        self.rebalancing = false;
                        return self.emit('error', new errors.FailedToRebalanceConsumerError(operation.mainError().toString()));
                    }
                    else {
                        var topicNames = self.topicPayloads.map(function (p) {
                            return p.topic;
                        });
                        self.client.refreshMetadata(topicNames, function (err) {
                            register();
                            self.rebalancing = false;
                            if (err) {
                              self.emit('error', err);
                            } else {
                              self.emit('rebalanced');
                            }
                        });
                    }
                });
            });
        }
    }

    // Wait for the consumer to be ready
    this.on('registered', function () {
        rebalance();
    });

    function register() {
        debug("Registered listeners");
        // Register for re-balances (broker or consumer changes)
        self.client.zk.on('consumersChanged', rebalance);
        self.client.on('brokersChanged', rebalance);
    }

    function deregister() {
        debug("Deregistered listeners");
        // Register for re-balances (broker or consumer changes)
        self.client.zk.removeListener('consumersChanged', rebalance);
        self.client.removeListener('brokersChanged', rebalance);
    }

    function attachZookeeperErrorListener() {
        self.client.zk.on('error', function (err) {
        self.emit('error', err);
    });
    }

    attachZookeeperErrorListener();

    this.client.on('zkReconnect', function () {
        attachZookeeperErrorListener();

        self.registerConsumer(function () {
            rebalance();
        });
    });

    this.client.on('error', function (err) {
        self.emit('error', err);
    });

    this.client.on('reconnect', function(lastError){
        self.fetch();
    })

    this.client.on('close', function () {
        debug('close');
    });

    this.on('offsetOutOfRange', function (topic) {
        self.pause();
        topic.maxNum = self.options.maxNumSegments;
        topic.metadata = 'm';
        topic.time = Date.now();
        self.offsetRequest([topic], function (err, offsets) {
            if (err) self.emit('error', new errors.InvalidConsumerOffsetError(self));
            else {
                var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
                // set minimal offset
                self.setOffset(topic.topic, topic.partition, min);
                self.resume();
            }
        });
    });


    // 'done' will be emit when a message fetch request complete
    this.on('done', function (topics) {
        self.updateOffsets(topics);
        if (!self.paused) {
            setImmediate(function () {
                self.fetch();
            });
        }
    });
};

HighLevelConsumer.prototype.rebalanceAttempt = function (oldTopicPayloads, cb) {
    var self = this;
    // Do the rebalance.....
    var consumerPerTopicMap;
    var newTopicPayloads = [];
    debug("HighLevelConsumer %s is attempting to rebalance", self.id);
    async.series([

            // Stop fetching data and commit offsets
            function (callback) {
                debug("HighLevelConsumer %s stopping data read during rebalance", self.id);
                self.stop(function () {
                    callback();
                })
            },

            // Assemble the data
            function (callback) {
                debug("HighLevelConsumer %s assembling data for rebalance", self.id);
                self.client.zk.getConsumersPerTopic(self.options.groupId, function (err, obj) {
                    if (err) {
                        callback(err);
                    }
                    else {
                        consumerPerTopicMap = obj;
                        callback();
                    }
                });
            },

            // Release current partitions
            function (callback) {
                debug("HighLevelConsumer %s releasing current partitions during rebalance", self.id);
                async.eachSeries(oldTopicPayloads, function (tp, cbb) {
                    if (tp['partition'] !== undefined) {
                        async.series([
                                function (delcbb) {
                                    self.client.zk.checkPartitionOwnership(self.id, self.options.groupId, tp['topic'], tp['partition'], function (err) {
                                        if (err) {
                                            // Partition doesn't exist simply carry on
                                            cbb();
                                        }
                                        else delcbb();
                                    });
                                },
                                function (delcbb) {
                                    self.client.zk.deletePartitionOwnership(self.options.groupId, tp['topic'], tp['partition'], function (err) {
                                        if (err) {
                                            delcbb(err);
                                        }
                                        else delcbb();
                                    });
                                },
                                function (delcbb) {
                                    self.client.zk.checkPartitionOwnership(self.id, self.options.groupId, tp['topic'], tp['partition'], function (err) {
                                        if (err) {
                                            delcbb();
                                        }
                                        else{
                                            delcbb("Partition should not exist");
                                        };
                                    });
                                }],
                            function (err) {
                                if (err) cbb(err);
                                else cbb();
                            });
                    }
                    else {
                        cbb();
                    }
                }, function (err) {
                    if (err) {
                        callback(err);
                    }
                    else callback();
                });
            },

            // Reblannce
            function (callback) {
                debug("HighLevelConsumer %s determining the partitions to own during rebalance", self.id);
                for (var topic in consumerPerTopicMap.consumerTopicMap[self.id]) {
                    var topicToAdd = consumerPerTopicMap.consumerTopicMap[self.id][topic];
                    var numberOfConsumers = consumerPerTopicMap.topicConsumerMap[topicToAdd].length;
                    var numberOfPartition = consumerPerTopicMap.topicPartitionMap[topicToAdd].length;
                    var partitionsPerConsumer = Math.floor(numberOfPartition / numberOfConsumers);
                    var extraPartitions = numberOfPartition % numberOfConsumers;
                    var currentConsumerIndex;
                    for (var index in consumerPerTopicMap.topicConsumerMap[topicToAdd]) {
                        if (consumerPerTopicMap.topicConsumerMap[topicToAdd][index] === self.id) {
                            currentConsumerIndex = parseInt(index);
                            break;
                        }
                    }
                    var extraBit = currentConsumerIndex;
                    if (currentConsumerIndex > extraPartitions) extraBit = extraPartitions
                    var startPart = partitionsPerConsumer * currentConsumerIndex + extraBit;
                    var extraNParts = 1;
                    if (currentConsumerIndex + 1 > extraPartitions) extraNParts = 0;
                    var nParts = partitionsPerConsumer + extraNParts;

                    for (var i = startPart; i < startPart + nParts; i++) {
                        newTopicPayloads.push({ topic: topicToAdd, partition: consumerPerTopicMap.topicPartitionMap[topicToAdd][i], offset: 0, maxBytes: self.options.fetchMaxBytes, metadata: 'm'});
                    }
                }
                callback();
            },

            // Update ZK with new ownership
            function (callback) {
                if (newTopicPayloads.length) {
                    debug("HighLevelConsumer %s gaining ownership of partitions during rebalance", self.id);
                    async.eachSeries(newTopicPayloads, function (tp, cbb) {
                        if (tp['partition'] !== undefined) {
                            async.series([
                                    function (addcbb) {
                                        self.client.zk.checkPartitionOwnership(self.id, self.options.groupId, tp['topic'], tp['partition'], function (err) {
                                            if (err) {
                                                // Partition doesn't exist simply carry on
                                                addcbb();
                                            }
                                            // Partition exists simply carry on
                                            else cbb();
                                        });
                                    },
                                    function (addcbb) {
                                        self.client.zk.addPartitionOwnership(self.id, self.options.groupId, tp['topic'], tp['partition'], function (err) {
                                            if (err) {
                                                addcbb(err);
                                            }
                                            else addcbb();
                                        });
                                    }],
                                function (err) {
                                    if (err) {
                                        cbb(err);
                                    }
                                    else cbb();
                                });
                        }
                        else {
                            cbb();
                        }
                    }, function (err) {
                        if (err) {
                            callback(err);
                        }
                        else
                            callback();
                    });
                }
                else {
                    debug("HighLevelConsumer %s has been assigned no partitions during rebalance", self.id);
                    callback();
                }
            },

            // Update the new topic offsets
            function (callback) {
                self.topicPayloads = newTopicPayloads;
                callback();
            }],
        function (err) {
            if (err) {
                debug("HighLevelConsumer %s rebalance attempt failed", self.id);
                cb(err);
            }
            else {
                debug("HighLevelConsumer %s rebalance attempt was successful", self.id);
                cb();
            }
        });
};

HighLevelConsumer.prototype.init = function () {

    var self = this;

    if (!self.topicPayloads.length) {
        return;
    }

    self.registerConsumer(function (err) {
        if (err) {
            return self.emit('error', new errors.FailedToRegisterConsumerError(err.toString()));
        }

        // Close the
        return self.emit('registered');
    });
};

/*
 * Update offset info in current payloads
 * @param {Object} Topic-partition-offset
 * @param {Boolean} Don't commit when initing consumer
 */
HighLevelConsumer.prototype.updateOffsets = function (topics, initing) {
    this.topicPayloads.forEach(function (p) {
        if (!_.isEmpty(topics[p.topic]) && topics[p.topic][p.partition] !== undefined) {
            var offset = topics[p.topic][p.partition];
            if (offset === -1) offset = 0;
            if (!initing) p.offset = offset + 1;
            else p.offset = offset;
        }
    });

    if (this.options.autoCommit && !initing) {
        this.autoCommit(false, function (err) {
            err && debug('auto commit offset', err);
        });
    }
};

function autoCommit(force, cb) {
    if (arguments.length === 1) {
        cb = force;
        force = false;
    }

    if (this.committing && !force) return cb(null, 'Offset committing');

    this.committing = true;
    setTimeout(function () {
        this.committing = false;
    }.bind(this), this.options.autoCommitIntervalMs);

    var commits = this.topicPayloads.filter(function (p) { return p.offset !== 0 });

    if (commits.length) {
        this.client.sendOffsetCommitRequest(this.options.groupId, commits, cb);
    } else {
        cb(null, 'Nothing to be committed');
    }
}
HighLevelConsumer.prototype.commit = HighLevelConsumer.prototype.autoCommit = autoCommit;

HighLevelConsumer.prototype.fetch = function () {
    if (!this.ready || this.rebalancing || this.paused) {
        return;
    }

    this.client.sendFetchRequest(this, this.topicPayloads, this.options.fetchMaxWaitMs, this.options.fetchMinBytes, this.options.maxTickMessages);
};

HighLevelConsumer.prototype.fetchOffset = function (payloads, cb) {
    this.client.sendOffsetFetchRequest(this.options.groupId, payloads, cb);
};

HighLevelConsumer.prototype.offsetRequest = function (payloads, cb) {
    this.client.sendOffsetRequest(payloads, cb);
};


/**
 * Register a consumer against a group
 *
 * @param consumer to register
 *
 * @param {Client~failedToRegisterConsumerCallback} cb A function to call the consumer has been registered
 */
HighLevelConsumer.prototype.registerConsumer = function (cb) {
    this.client.zk.registerConsumer(this.options.groupId, this.id, this.payloads, function (err) {
        if (err) return cb(err);
        cb();
    })
    this.client.zk.listConsumers(this.options.groupId);
};

HighLevelConsumer.prototype.addTopics = function (topics, cb) {
    var self = this;
    if (!this.ready) {
        setTimeout(function () {
                self.addTopics(topics, cb)
            }
            , 100);
        return;
    }
    this.client.addTopics(
        topics,
        function (err, added) {
            if (err) return cb && cb(err, added);

            var payloads = self.buildPayloads(topics);
            // update offset of topics that will be added
            self.fetchOffset(payloads, function (err, offsets) {
                if (err) return cb(err);
                payloads.forEach(function (p) {
                    var offset = offsets[p.topic][p.partition];
                    if (offset === -1) offset = 0;
                    p.offset = offset;
                    self.topicPayloads.push(p);
                });
                // TODO: rebalance consumer
                cb && cb(null, added);
            });
        }
    );
};

HighLevelConsumer.prototype.removeTopics = function (topics, cb) {
    topics = typeof topics === 'string' ? [topics] : topics;
    this.payloads = this.payloads.filter(function (p) {
        return !~topics.indexOf(p.topic);
    });

    this.client.removeTopicMetadata(topics, cb);
};

HighLevelConsumer.prototype.close = function (force, cb) {
    this.ready = false;
    clearInterval(this.checkPartitionOwnershipInterval);

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

HighLevelConsumer.prototype.stop = function (cb) {
    if (!this.options.autoCommit) return cb && cb();
    this.commit(true, function (err) {
        cb && cb();
    }.bind(this));
};

HighLevelConsumer.prototype.setOffset = function (topic, partition, offset) {
    this.topicPayloads.every(function (p) {
        if (p.topic === topic && p.partition == partition) {
            p.offset = offset;
            return false;
        }
        return true;
    });
};

HighLevelConsumer.prototype.pause = function () {
    this.paused = true;
};

HighLevelConsumer.prototype.resume = function () {
    this.paused = false;
    this.fetch();
};

module.exports = HighLevelConsumer;
