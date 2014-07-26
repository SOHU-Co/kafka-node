'use strict';

var zookeeper = require('node-zookeeper-client')
    , util = require('util')
    , async = require("async")
    , EventEmiter = require('events').EventEmitter;

/**
 * Provides kafka specific helpers for talking with zookeeper
 *
 * @param {String} [connectionString='localhost:2181/kafka0.8'] A list of host:port for each zookeeper node and
 *      optionally a chroot path
 *
 * @constructor
 */
var Zookeeper = function (connectionString, options) {
    this.client = zookeeper.createClient(connectionString, options);

    var that = this;
    this.client.on('connected', function () {
        that.listBrokers();
    });
    this.client.connect();
};

util.inherits(Zookeeper, EventEmiter);

Zookeeper.prototype.registerConsumer = function (groupId, consumerId, payloads, cb) {
    var path = '/consumers/' + groupId;
    var that = this;

    async.series([
            function (callback) {
                that.client.create(
                    path,
                    function (error, path) {
                        // simply carry on
                        callback();
                    });
            },
            function (callback) {
                that.client.create(
                        path + '/ids',
                    function (error, path) {
                        // simply carry on
                        callback();
                    });
            },
            function (callback) {
                that.client.create(
                        path + '/ids/' + consumerId,
                    null,
                    null,
                    zookeeper.CreateMode.EPHEMERAL,
                    function (error, path) {
                        if (error) {
                            callback(error);
                        }
                        else {
                            callback();
                        }
                    });
            },
            function (callback) {
                var metadata = '{"version":1,"subscription":'
                metadata += '{'
                var sep = '';
                payloads.map(function (p) {
                    metadata += sep + '"' + p.topic + '": 1'
                    sep = ', ';
                });
                metadata += '}'
                var milliseconds = (new Date).getTime();
                metadata += ',"pattern":"white_list","timestamp":"' + milliseconds + '"}';
                that.client.setData(path + '/ids/' + consumerId, new Buffer(metadata), function (error, stat) {
                    if (error) {
                        callback(error);
                    }
                    else {
                        console.log('Node: %s was created.', path + '/ids/' + consumerId);
                        cb();
                    }
                });
            }],
        function (err) {
            if (err) cb(err);
            else cb();
        });
};

Zookeeper.prototype.getConsumersPerTopic = function (groupId, cb) {
    var consumersPath = '/consumers/' + groupId + '/ids';
    var brokerTopicsPath = '/brokers/topics';
    var consumers = [];
    var path = '/consumers/' + groupId;
    var that = this;
    var consumerPerTopicMap = new ZookeeperConsumerMappings();

    async.series([
            function (callback) {
                that.client.getChildren(consumersPath, function (error, children, stats) {
                    if (error) {
                        callback(error);
                        return;
                    }
                    else {
                        console.log('Children are: %j.', children);
                        consumers = children;
                        async.each(children, function (consumer, cbb) {
                            var path = consumersPath + '/' + consumer;
                            that.client.getData(
                                path,
                                function (error, data) {
                                    if (error) {
                                        cbb(error);
                                    }
                                    else {
                                        try {
                                            var obj = JSON.parse(data.toString());
                                            // For each topic
                                            for (var topic in obj.subscription) {
                                                if (consumerPerTopicMap.topicConsumerMap[topic] == undefined) {
                                                    consumerPerTopicMap.topicConsumerMap[topic] = [];
                                                }
                                                consumerPerTopicMap.topicConsumerMap[topic].push(consumer);

                                                if (consumerPerTopicMap.consumerTopicMap[consumer] == undefined) {
                                                    consumerPerTopicMap.consumerTopicMap[consumer] = [];
                                                }
                                                consumerPerTopicMap.consumerTopicMap[consumer].push(topic);
                                            }

                                            cbb();
                                        } catch (e) {
                                            console.log(e);
                                            callback(new Error("Unable to assemble data"));
                                        }
                                    }
                                }
                            );
                        }, function (err) {
                            if (err)
                                callback(err);
                            else
                                callback();
                        });
                    }
                });
            },
            function (callback) {
                Object.keys(consumerPerTopicMap.topicConsumerMap).forEach(function (key) {
                    consumerPerTopicMap.topicConsumerMap[key] = consumerPerTopicMap.topicConsumerMap[key].sort();
                });
                callback();
            },
            function (callback) {
                async.each(Object.keys(consumerPerTopicMap.topicConsumerMap), function (topic, cbb) {
                    var path = brokerTopicsPath + '/' + topic;
                    that.client.getData(
                        path,
                        function (error, data) {
                            if (error) {
                                cbb(error);
                            }
                            else {
                                var obj = JSON.parse(data.toString());
                                // Get the topic partitions
                                var partitions = Object.keys(obj.partitions).map(function (partition) {
                                    return partition;
                                });
                                consumerPerTopicMap.topicPartitionMap[topic] = partitions.sort(compareNumbers);
                                cbb();
                            }
                        }
                    );
                }, function (err) {
                    if (err)
                        callback(err);
                    else
                        callback();
                });
            }],
        function (err) {
            if (err) {
                console.log(err);
                cb(err);
            }
            else cb(null, consumerPerTopicMap);
        });
}

function compareNumbers(a, b) {
    return a - b;
}

Zookeeper.prototype.getBrokerDetail = function (brokerId, cb) {
    var path = '/brokers/ids/' + brokerId;
    this.client.getData(
        path,
        function (error, data) {
            if (error) {
                console.log('Error occurred when getting data: %s.', error);
            }

            cb && cb(data);
        }
    );
};

Zookeeper.prototype.listBrokers = function (cb) {
    var that = this;
    var path = '/brokers/ids';
    this.client.getChildren(
        path,
        function () {
            that.listBrokers();
        },
        function (error, children) {
            if (error)
                return console.log('Failed to list children of node: %s due to: %s.', path, error);

            if (children.length) {
                var brokers = {};
                if (!that.inited) {
                    var brokerId = children.shift();
                    that.getBrokerDetail(brokerId, function (data) {
                        brokers[brokerId] = JSON.parse(data.toString());
                        that.emit('init', brokers);
                        that.inited = true;
                        cb && cb(brokers); //For test
                    })
                } else {
                    var count = 0;
                    children.forEach(function (brokerId) {
                        that.getBrokerDetail(brokerId, function (data) {
                            brokers[brokerId] = JSON.parse(data.toString());
                            if (++count == children.length) {
                                that.emit('brokersChanged', brokers)
                                cb && cb(brokers); //For test
                            }
                        })
                    })
                }

            } else {
                if (that.inited)
                    return that.emit('brokersChanged', {})
                that.inited = true;
                that.emit('init', {});
            }
        }
    );
};


Zookeeper.prototype.listConsumers = function (groupId) {
    var that = this;
    var path = '/consumers/' + groupId + '/ids';
    this.client.getChildren(
        path,
        function () {
            that.listConsumers(groupId);
        },
        function (error, children) {
            if (error)
                that.emit('error', error)
            else
                that.emit('consumersChanged')
        }
    );
};

Zookeeper.prototype.topicExists = function (topic, cb, watch) {
    var path = '/brokers/topics/' + topic,
        self = this;
    this.client.exists(
        path,
        function (event) {
            console.log('Got event: %s.', event);
            if (watch)
                self.topicExists(topic, cb);
        },
        function (error, stat) {
            if (error) return;
            cb(!!stat, topic);
        }
    );
}

Zookeeper.prototype.deletePartitionOwnership = function (groupId, topic, partition, cb) {
    var path = '/consumers/' + groupId + '/owners/' + topic + '/' + partition,
        self = this;
    this.client.remove(
        path,
        function (error) {
            if (error)
                cb(error);
            else {
                console.log("Removed partition ownership %s", path);
                cb();
            }
        }
    );
}

Zookeeper.prototype.addPartitionOwnership = function (consumerId, groupId, topic, partition, cb) {
    var path = '/consumers/' + groupId + '/owners/' + topic + '/' + partition,
        self = this;

    async.series([
            function (callback) {
                self.client.create(
                '/consumers/' + groupId,
                    function (error, path) {
                        // simply carry on
                        callback();
                    });
            },
            function (callback) {
                self.client.create(
                        '/consumers/' + groupId + '/owners',
                    function (error, path) {
                        // simply carry on
                        callback();
                    });
            },
            function (callback) {
                self.client.create(
                        '/consumers/' + groupId + '/owners/' + topic,
                    function (error, path) {
                        // simply carry on
                        callback();
                    });
            },
            function (callback) {
                self.client.create(
                    path,
                    null,
                    null,
                    zookeeper.CreateMode.EPHEMERAL,
                    function (error, path) {
                        if (error)
                            callback(error);
                        else callback();
                    });
            },
            function (callback) {
                self.client.setData(path, new Buffer(consumerId), function (error, stat) {
                    if (error) {
                        callback(error);
                    }
                    else {
                        callback();
                    }
                });
            },
            function (callback) {
                self.client.exists(path, null, function (error, stat) {
                    if (error) {
                        callback(error);
                    }
                    else if (stat){
                        console.log('Gained ownership of %s by %s.', path, consumerId);
                        callback();
                    }
                    else{
                        callback("Path wasn't created");
                    }
                });
            }],
        function (err) {
            if (err) cb(err);
            else cb();
        });
}

var ZookeeperConsumerMappings = function () {
    this.consumerTopicMap = {};
    this.topicConsumerMap = {};
    this.topicPartitionMap = {};
};


exports.Zookeeper = Zookeeper;
exports.ZookeeperConsumerMappings = ZookeeperConsumerMappings;
