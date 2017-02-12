'use strict';

var zookeeper = require('node-zookeeper-client');
var util = require('util');
var async = require('async');
var EventEmiter = require('events').EventEmitter;
var logger = require('./logging')('kafka-node:zookeeper');

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
  this.client.on('disconnected', function () {
    that.emit('disconnected');
  });
  this.client.connect();
};

util.inherits(Zookeeper, EventEmiter);

Zookeeper.prototype.unregisterConsumer = function (groupId, consumerId, cb) {
  var path = '/consumers/' + groupId + '/ids/' + consumerId;
  var self = this;
  logger.debug('unregister consumer node: %s', path);
  async.waterfall([
    function (callback) {
      self.client.exists(path, callback);
    },
    function (stat, callback) {
      if (stat) {
        self.client.remove(path, function (error) {
          if (error) {
            logger.error(error);
            return callback(error);
          }
          callback(null, true);
        });
      } else {
        callback(null, false);
      }
    }
  ], cb);
};

Zookeeper.prototype.registerConsumer = function (groupId, consumerId, payloads, cb) {
  var path = '/consumers/' + groupId;
  var that = this;

  async.series([
    /* eslint-disable handle-callback-err */
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
    /* eslint-enable handle-callback-err */
    function (callback) {
      that.client.create(
        path + '/ids/' + consumerId,
        null,
        null,
        zookeeper.CreateMode.EPHEMERAL,
        function (error, path) {
          if (error) {
            callback(error);
          } else {
            callback();
          }
        });
    },
    function (callback) {
      var metadata = '{"version":1,"subscription":';
      metadata += '{';
      var sep = '';
      payloads.map(function (p) {
        metadata += sep + '"' + p.topic + '": 1';
        sep = ', ';
      });
      metadata += '}';
      var milliseconds = (new Date()).getTime();
      metadata += ',"pattern":"white_list","timestamp":"' + milliseconds + '"}';
      that.client.setData(path + '/ids/' + consumerId, new Buffer(metadata), function (error, stat) {
        if (error) {
          callback(error);
        } else {
          logger.debug('Node: %s was created.', path + '/ids/' + consumerId);
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
  var that = this;
  var consumerPerTopicMap = new ZookeeperConsumerMappings();

  async.series([
    function (callback) {
      that.client.getChildren(consumersPath, function (error, children, stats) {
        if (error) {
          callback(error);
          return;
        } else {
          logger.debug('Children are: %j.', children);
          async.each(children, function (consumer, cbb) {
            var path = consumersPath + '/' + consumer;
            that.client.getData(
              path,
              function (error, data) {
                if (error) {
                  cbb(error);
                } else {
                  try {
                    var obj = JSON.parse(data.toString());
                    // For each topic
                    for (var topic in obj.subscription) {
                      if (!obj.subscription.hasOwnProperty(topic)) {
                        continue;
                      }
                      if (consumerPerTopicMap.topicConsumerMap[topic] == null) {
                        consumerPerTopicMap.topicConsumerMap[topic] = [];
                      }
                      consumerPerTopicMap.topicConsumerMap[topic].push(consumer);

                      if (consumerPerTopicMap.consumerTopicMap[consumer] == null) {
                        consumerPerTopicMap.consumerTopicMap[consumer] = [];
                      }
                      consumerPerTopicMap.consumerTopicMap[consumer].push(topic);
                    }

                    cbb();
                  } catch (e) {
                    logger.error(e);
                    cbb(new Error('Unable to assemble data'));
                  }
                }
              }
            );
          }, function (err) {
            if (err) {
              callback(err);
            } else {
              callback();
            }
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
            } else {
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
        if (err) {
          callback(err);
        } else {
          callback();
        }
      });
    }],
    function (err) {
      if (err) {
        logger.debug(err);
        cb(err);
      } else cb(null, consumerPerTopicMap);
    });
};

function compareNumbers (a, b) {
  return a - b;
}

Zookeeper.prototype.listPartitions = function (topic) {
  var self = this;
  var path = '/brokers/topics/' + topic + '/partitions';
  this.client.getChildren(
    path,
    function () {
      if (!self.closed) {
        self.listPartitions(topic);
      }
    },
    function (error, children) {
      if (error) {
        logger.error(error);
        // Ignore NO_NODE error here #157
        if (error.name !== 'NO_NODE') {
          self.emit('error', error);
        }
      } else {
        if (self.initPartitions) {
          return self.emit('partitionsChanged');
        }
        self.initPartitions = true;
      }
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
      if (error) {
        logger.debug('Failed to list children of node: %s due to: %s.', path, error);
        that.emit('error', error);
        return;
      }

      if (children.length) {
        var brokers = {};
        async.each(children, getBrokerDetail, function (err) {
          if (err) {
            that.emit('error', err);
            return;
          }
          if (!that.inited) {
            that.emit('init', brokers);
            that.inited = true;
          } else {
            that.emit('brokersChanged', brokers);
          }
          cb && cb(brokers); // For test
        });
      } else {
        if (that.inited) {
          return that.emit('brokersChanged', {});
        }
        that.inited = true;
        that.emit('init', {});
      }

      function getBrokerDetail (id, cb) {
        var path = '/brokers/ids/' + id;
        that.client.getData(path, function (err, data) {
          if (err) return cb(err);
          brokers[id] = JSON.parse(data.toString());
          cb();
        });
      }
    }
  );
};

Zookeeper.prototype.isConsumerRegistered = function (groupId, consumerId, callback) {
  var path = '/consumers/' + groupId + '/ids/' + consumerId;
  this.client.exists(path, function (error, stat) {
    if (error) {
      return callback(error);
    }
    callback(null, !!stat);
  });
};

Zookeeper.prototype.listConsumers = function (groupId) {
  var that = this;
  var path = '/consumers/' + groupId + '/ids';
  this.client.getChildren(
    path,
    function () {
      if (!that.closed) {
        that.listConsumers(groupId);
      }
    },
    function (error, children) {
      if (error) {
        logger.error(error);
        // Ignore NO_NODE error here #157
        if (error.name !== 'NO_NODE') {
          that.emit('error', error);
        }
      } else {
        if (that.listConsumersInitialized) {
          that.emit('consumersChanged');
        }
      }
      that.listConsumersInitialized = true;
    }
  );
};

Zookeeper.prototype.topicExists = function (topic, cb, watch) {
  var path = '/brokers/topics/' + topic;
  var self = this;
  this.client.exists(
    path,
    function (event) {
      logger.debug('Got event: %s.', event);
      if (watch) {
        self.topicExists(topic, cb);
      }
    },
    function (error, stat) {
      if (error) return cb(error);
      cb(null, !!stat, topic);
    }
  );
};

Zookeeper.prototype.deletePartitionOwnership = function (groupId, topic, partition, cb) {
  var path = '/consumers/' + groupId + '/owners/' + topic + '/' + partition;
  this.client.remove(
    path,
    function (error) {
      if (error) {
        cb(error);
      } else {
        logger.debug('Removed partition ownership %s', path);
        cb();
      }
    }
  );
};

Zookeeper.prototype.addPartitionOwnership = function (consumerId, groupId, topic, partition, cb) {
  var path = '/consumers/' + groupId + '/owners/' + topic + '/' + partition;
  var self = this;

  async.series([
    /* eslint-disable handle-callback-err */
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
    /* eslint-enable handle-callback-err */
    function (callback) {
      self.client.create(
        path,
        new Buffer(consumerId),
        null,
        zookeeper.CreateMode.EPHEMERAL,
        function (error, path) {
          if (error) {
            callback(error);
          } else callback();
        });
    },
    function (callback) {
      self.client.exists(path, null, function (error, stat) {
        if (error) {
          callback(error);
        } else if (stat) {
          logger.debug('Gained ownership of %s by %s.', path, consumerId);
          callback();
        } else {
          callback("Path wasn't created");
        }
      });
    }],
    function (err) {
      if (err) cb(err);
      else cb();
    });
};

Zookeeper.prototype.checkPartitionOwnership = function (consumerId, groupId, topic, partition, cb) {
  var path = '/consumers/' + groupId + '/owners/' + topic + '/' + partition;
  var self = this;

  async.series([
    function (callback) {
      self.client.exists(path, null, function (error, stat) {
        if (error) {
          callback(error);
        } else if (stat) {
          callback();
        } else {
          callback("Path wasn't created");
        }
      });
    },
    function (callback) {
      self.client.getData(
        path,
        function (error, data) {
          if (error) {
            callback(error);
          } else {
            if (consumerId !== data.toString()) {
              callback('Consumer not registered ' + consumerId);
            } else callback();
          }
        }
      );
    }],
    function (err) {
      if (err) cb(err);
      else cb();
    });
};

Zookeeper.prototype.close = function () {
  this.closed = true;
  this.client.close();
};

var ZookeeperConsumerMappings = function () {
  this.consumerTopicMap = {};
  this.topicConsumerMap = {};
  this.topicPartitionMap = {};
};

exports.Zookeeper = Zookeeper;
exports.ZookeeperConsumerMappings = ZookeeperConsumerMappings;
