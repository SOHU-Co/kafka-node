'use strict';

var util = require('util');
var _ = require('lodash');
var EventEmitter = require('events');
var uuid = require('uuid');
var async = require('async');
var errors = require('./errors');
var retry = require('retry');
var logger = require('./logging')('kafka-node:HighLevelConsumer');
var validateConfig = require('./utils').validateConfig;
const KafkaClient = require('./kafkaClient');

var DEFAULTS = {
  groupId: 'kafka-node-group',
  // Auto commit config
  autoCommit: true,
  autoCommitIntervalMs: 5000,
  // Fetch message config
  fetchMaxWaitMs: 100,
  paused: false,
  maxNumSegments: 1000,
  fetchMinBytes: 1,
  fetchMaxBytes: 1024 * 1024,
  maxTickMessages: 1000,
  fromOffset: false,
  rebalanceRetry: {
    retries: 10,
    factor: 2,
    minTimeout: 1 * 100,
    maxTimeout: 1 * 1000,
    randomize: true
  }
};

var HighLevelConsumer = function (client, topics, options) {
  EventEmitter.call(this);
  if (!topics) {
    throw new Error('Must have payloads');
  }

  if (client instanceof KafkaClient) {
    throw new Error('Client for HighLevelConsumer cannot be an instance of KafkaClient');
  }

  this.fetchCount = 0;
  this.client = client;
  this.options = _.defaultsDeep((options || {}), DEFAULTS);
  this.initialised = false;
  this.ready = false;
  this.closing = false;
  this.paused = this.options.paused;
  this.rebalancing = false;
  this.pendingRebalances = 0;
  this.committing = false;
  this.needToCommit = false;
  this.id = this.options.id || this.options.groupId + '_' + uuid.v4();
  this.payloads = this.buildPayloads(topics);
  this.topicPayloads = this.buildTopicPayloads(topics);
  this.connect();

  if (this.options.groupId) {
    validateConfig('options.groupId', this.options.groupId);
  }
};
util.inherits(HighLevelConsumer, EventEmitter);

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
  // Client alreadyexists
  if (this.client.ready) {
    this.init();
  }

  this.client.on('ready', function () {
    if (!self.initialised) self.init();

    // Check the topics exist and create a watcher on them
    var topics = self.payloads.map(function (p) {
      return p.topic;
    });

    self.client.topicExists(topics, function (err) {
      if (err) {
        return self.emit('error', err);
      }
      self.initialised = true;
    });
  });

  function checkPartitionOwnership (callback) {
    async.each(self.topicPayloads, function (tp, cbb) {
      if (tp.partition !== undefined) {
        self.client.zk.checkPartitionOwnership(self.id, self.options.groupId, tp.topic, tp.partition, function (err) {
          if (err) {
            cbb(err);
          } else {
            cbb();
          }
        });
      } else {
        cbb();
      }
    }, callback);
  }

  // Check partition ownership and registration
  this.checkPartitionOwnershipInterval = setInterval(function () {
    if (!self.rebalancing) {
      async.parallel([
        checkPartitionOwnership,
        function (callback) {
          self.client.zk.isConsumerRegistered(self.options.groupId, self.id, function (error, registered) {
            if (error) {
              return callback(error);
            }
            if (registered) {
              callback();
            } else {
              callback(new Error(util.format('Consumer %s is not registered in group %s', self.id, self.options.groupId)));
            }
          });
        }
      ], function (error) {
        if (error) {
          self.emit('error', new errors.FailedToRegisterConsumerError(error.toString(), error));
        }
      });
    }
  }, 20000);

  function fetchAndUpdateOffsets (cb) {
    self.fetchOffset(self.topicPayloads, function (err, topics) {
      if (err) {
        return cb(err);
      }

      self.ready = true;
      self.updateOffsets(topics, true);

      return cb();
    });
  }

  function rebalance () {
    logger.debug('rebalance() %s is rebalancing: %s ready: %s', self.id, self.rebalancing, self.ready);
    if (!self.rebalancing && !self.closing) {
      deregister();

      self.emit('rebalancing');

      self.rebalancing = true;
      logger.debug('HighLevelConsumer rebalance retry config: %s', JSON.stringify(self.options.rebalanceRetry));
      var oldTopicPayloads = self.topicPayloads;
      var operation = retry.operation(self.options.rebalanceRetry);

      operation.attempt(function (currentAttempt) {
        self.rebalanceAttempt(oldTopicPayloads, function (err) {
          if (operation.retry(err)) {
            return;
          }
          if (err) {
            self.rebalancing = false;
            return self.emit('error', new errors.FailedToRebalanceConsumerError(operation.mainError().toString()));
          } else {
            var topicNames = self.topicPayloads.map(function (p) {
              return p.topic;
            });
            self.client.refreshMetadata(topicNames, function (err) {
              register();
              if (err) {
                self.rebalancing = false;
                self.emit('error', err);
                return;
              }

              if (self.topicPayloads.length) {
                fetchAndUpdateOffsets(function (err) {
                  self.rebalancing = false;
                  if (err) {
                    self.emit('error', new errors.FailedToRebalanceConsumerError(err.message));
                    return;
                  }
                  self.fetch();
                  self.emit('rebalanced');
                });
              } else { // was not assigned any partitions during rebalance
                self.rebalancing = false;
                self.emit('rebalanced');
              }
            });
          }
        });
      });
    }
  }

  // Wait for the consumer to be ready
  this.on('registered', rebalance);

  function register (fn) {
    logger.debug('Registered listeners %s', self.id);
    self.client.zk.on('consumersChanged', fn || rebalance);
    self.client.zk.on('partitionsChanged', fn || rebalance);
    self.client.on('brokersChanged', fn || rebalance);
  }

  function deregister (fn) {
    logger.debug('Deregistered listeners %s', self.id);
    self.client.zk.removeListener('consumersChanged', fn || rebalance);
    self.client.zk.removeListener('partitionsChanged', fn || rebalance);
    self.client.removeListener('brokersChanged', fn || rebalance);
  }

  function pendingRebalance () {
    if (self.rebalancing) {
      self.pendingRebalances++;
      logger.debug('%s added a pendingRebalances %d', self.id, self.pendingRebalances);
    }
  }

  function attachZookeeperErrorListener () {
    self.client.zk.on('error', function (err) {
      self.emit('error', err);
    });
  }

  attachZookeeperErrorListener();

  this.client.on('zkReconnect', function () {
    logger.debug('zookeeper reconnect for %s', self.id);
    attachZookeeperErrorListener();

    // clean up what's leftover
    self.leaveGroup(function () {
      // rejoin the group
      self.registerConsumer(function (error) {
        if (error) {
          return self.emit('error', new errors.FailedToRegisterConsumerError('Failed to register consumer on zkReconnect', error));
        }
        self.emit('registered');
      });
    });
  });

  this.on('rebalanced', function () {
    deregister(pendingRebalance);
    if (self.pendingRebalances && !self.closing) {
      logger.debug('%s pendingRebalances is %d scheduling a rebalance...', self.id, self.pendingRebalances);
      setTimeout(function () {
        logger.debug('%s running scheduled rebalance', self.id);
        rebalance();
      }, 500);
    }
  });

  this.on('rebalancing', function () {
    register(pendingRebalance);
    self.pendingRebalances = 0;
  });

  this.client.on('error', function (err) {
    self.emit('error', err);
  });

  this.client.on('reconnect', function (lastError) {
    self.fetch();
  });

  this.client.on('close', function () {
    logger.debug('close');
  });

  this.on('offsetOutOfRange', function (topic) {
    self.pause();
    topic.maxNum = self.options.maxNumSegments;
    topic.metadata = 'm';
    topic.time = Date.now();
    self.offsetRequest([topic], function (err, offsets) {
      if (err) {
        self.emit('error', new errors.InvalidConsumerOffsetError(self));
      } else {
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

HighLevelConsumer.prototype._releasePartitions = function (topicPayloads, callback) {
  var self = this;
  async.each(topicPayloads, function (tp, cbb) {
    if (tp.partition !== undefined) {
      async.series([
        function (delcbb) {
          self.client.zk.checkPartitionOwnership(self.id, self.options.groupId, tp.topic, tp.partition, function (err) {
            if (err) {
              // Partition doesn't exist simply carry on
              cbb();
            } else delcbb();
          });
        },
        function (delcbb) {
          self.client.zk.deletePartitionOwnership(self.options.groupId, tp.topic, tp.partition, delcbb);
        },
        function (delcbb) {
          self.client.zk.checkPartitionOwnership(self.id, self.options.groupId, tp.topic, tp.partition, function (err) {
            if (err) {
              delcbb();
            } else {
              delcbb('Partition should not exist');
            }
          });
        }],
      cbb);
    } else {
      cbb();
    }
  }, callback);
};

HighLevelConsumer.prototype.rebalanceAttempt = function (oldTopicPayloads, cb) {
  var self = this;
  // Do the rebalance.....
  var consumerPerTopicMap;
  var newTopicPayloads = [];
  logger.debug('HighLevelConsumer %s is attempting to rebalance', self.id);
  async.series([

    // Stop fetching data and commit offsets
    function (callback) {
      logger.debug('HighLevelConsumer %s stopping data read during rebalance', self.id);
      self.stop(function () {
        callback();
      });
    },

    // Assemble the data
    function (callback) {
      logger.debug('HighLevelConsumer %s assembling data for rebalance', self.id);
      self.client.zk.getConsumersPerTopic(self.options.groupId, function (err, obj) {
        if (err) {
          callback(err);
        } else {
          consumerPerTopicMap = obj;
          callback();
        }
      });
    },

    // Release current partitions
    function (callback) {
      logger.debug('HighLevelConsumer %s releasing current partitions during rebalance', self.id);
      self._releasePartitions(oldTopicPayloads, callback);
    },

    // Rebalance
    function (callback) {
      logger.debug('HighLevelConsumer %s determining the partitions to own during rebalance', self.id);
      logger.debug('consumerPerTopicMap.consumerTopicMap %j', consumerPerTopicMap.consumerTopicMap);
      for (var topic in consumerPerTopicMap.consumerTopicMap[self.id]) {
        if (!consumerPerTopicMap.consumerTopicMap[self.id].hasOwnProperty(topic)) {
          continue;
        }
        var topicToAdd = consumerPerTopicMap.consumerTopicMap[self.id][topic];
        var numberOfConsumers = consumerPerTopicMap.topicConsumerMap[topicToAdd].length;
        var numberOfPartition = consumerPerTopicMap.topicPartitionMap[topicToAdd].length;
        var partitionsPerConsumer = Math.floor(numberOfPartition / numberOfConsumers);
        var extraPartitions = numberOfPartition % numberOfConsumers;
        var currentConsumerIndex;
        for (var index in consumerPerTopicMap.topicConsumerMap[topicToAdd]) {
          if (!consumerPerTopicMap.topicConsumerMap[topicToAdd].hasOwnProperty(index)) {
            continue;
          }
          if (consumerPerTopicMap.topicConsumerMap[topicToAdd][index] === self.id) {
            currentConsumerIndex = parseInt(index);
            break;
          }
        }
        var extraBit = currentConsumerIndex;
        if (currentConsumerIndex > extraPartitions) extraBit = extraPartitions;
        var startPart = partitionsPerConsumer * currentConsumerIndex + extraBit;
        var extraNParts = 1;
        if (currentConsumerIndex + 1 > extraPartitions) extraNParts = 0;
        var nParts = partitionsPerConsumer + extraNParts;

        for (var i = startPart; i < startPart + nParts; i++) {
          newTopicPayloads.push({
            topic: topicToAdd,
            partition: consumerPerTopicMap.topicPartitionMap[topicToAdd][i],
            offset: 0,
            maxBytes: self.options.fetchMaxBytes,
            metadata: 'm'
          });
        }
      }
      logger.debug('newTopicPayloads %j', newTopicPayloads);
      callback();
    },

    // Update ZK with new ownership
    function (callback) {
      if (newTopicPayloads.length) {
        logger.debug('HighLevelConsumer %s gaining ownership of partitions during rebalance', self.id);
        async.eachSeries(newTopicPayloads, function (tp, cbb) {
          if (tp.partition !== undefined) {
            async.series([
              function (addcbb) {
                self.client.zk.checkPartitionOwnership(self.id, self.options.groupId, tp.topic, tp.partition, function (err) {
                  if (err) {
                    // Partition doesn't exist simply carry on
                    addcbb();
                  } else cbb(); // Partition exists simply carry on
                });
              },
              function (addcbb) {
                self.client.zk.addPartitionOwnership(self.id, self.options.groupId, tp.topic, tp.partition, function (err) {
                  if (err) {
                    addcbb(err);
                  } else addcbb();
                });
              }],
              function (err) {
                if (err) {
                  cbb(err);
                } else cbb();
              });
          } else {
            cbb();
          }
        }, function (err) {
          if (err) {
            callback(err);
          } else {
            callback();
          }
        });
      } else {
        logger.debug('HighLevelConsumer %s has been assigned no partitions during rebalance', self.id);
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
        logger.debug('HighLevelConsumer %s rebalance attempt failed', self.id);
        cb(err);
      } else {
        logger.debug('HighLevelConsumer %s rebalance attempt was successful', self.id);
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
  this.topicPayloads.forEach(p => {
    if (!_.isEmpty(topics[p.topic]) && topics[p.topic][p.partition] !== undefined) {
      var offset = topics[p.topic][p.partition];
      if (offset === -1) offset = 0;
      if (!initing) p.offset = offset + 1;
      else p.offset = offset;
      this.needToCommit = true;
    }
  });

  if (this.options.autoCommit && !initing) {
    this.autoCommit(false, function (err) {
      err && logger.debug('auto commit offset', err);
    });
  }
};

HighLevelConsumer.prototype.sendOffsetCommitRequest = function (commits, cb) {
  this.client.sendOffsetCommitRequest(this.options.groupId, commits, cb);
};

function autoCommit (force, cb) {
  if (arguments.length === 1) {
    cb = force;
    force = false;
  }

  if (!force) {
    if (this.committing) return cb(null, 'Offset committing');
    if (!this.needToCommit) return cb(null, 'Commit not needed');
  }

  this.needToCommit = false;
  this.committing = true;
  setTimeout(function () {
    this.committing = false;
  }.bind(this), this.options.autoCommitIntervalMs);

  var commits = this.topicPayloads.filter(function (p) { return p.offset !== -1; });

  if (commits.length) {
    this.sendOffsetCommitRequest(commits, cb);
  } else {
    cb(null, 'Nothing to be committed');
  }
}
HighLevelConsumer.prototype.commit = HighLevelConsumer.prototype.autoCommit = autoCommit;

HighLevelConsumer.prototype.fetch = function () {
  if (!this.ready || this.rebalancing || this.paused || this.closing) {
    return;
  }

  this.client.sendFetchRequest(
    this,
    this.topicPayloads,
    this.options.fetchMaxWaitMs,
    this.options.fetchMinBytes,
    this.options.maxTickMessages
  );
};

HighLevelConsumer.prototype.fetchOffset = function (payloads, cb) {
  logger.debug('in fetchOffset %s payloads: %j', this.id, payloads);
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
  var self = this;
  var groupId = this.options.groupId;
  this.client.zk.registerConsumer(groupId, this.id, this.payloads, function (err) {
    if (err) return cb(err);
    self.client.zk.listConsumers(self.options.groupId);
    var topics = self.topicPayloads.reduce(function (ret, topicPayload) {
      if (ret.indexOf(topicPayload.topic) === -1) {
        ret.push(topicPayload.topic);
      }
      return ret;
    }, []);
    topics.forEach(function (topic) {
      self.client.zk.listPartitions(topic);
    });
    cb();
  });
};

HighLevelConsumer.prototype.addTopics = function (topics, cb) {
  var self = this;
  if (!this.ready) {
    setTimeout(function () {
      self.addTopics(topics, cb);
    }, 100);
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

HighLevelConsumer.prototype.leaveGroup = function (cb) {
  var self = this;
  async.parallel([
    function (callback) {
      if (self.topicPayloads.length) {
        self._releasePartitions(self.topicPayloads, callback);
      } else {
        callback(null);
      }
    },
    function (callback) {
      self.client.zk.unregisterConsumer(self.options.groupId, self.id, callback);
    }
  ], cb);
};

HighLevelConsumer.prototype.close = function (force, cb) {
  var self = this;
  this.ready = false;
  this.closing = true;
  clearInterval(this.checkPartitionOwnershipInterval);

  if (typeof force === 'function') {
    cb = force;
    force = false;
  }

  async.series([
    function (callback) {
      self.leaveGroup(callback);
    },
    function (callback) {
      if (force) {
        async.series([
          function (callback) {
            self.commit(true, callback);
          },
          function (callback) {
            self.client.close(callback);
          }
        ], callback);
        return;
      }
      self.client.close(callback);
    }
  ], cb);
};

HighLevelConsumer.prototype.stop = function (cb) {
  if (!this.options.autoCommit) return cb && cb();
  this.commit(true, function (err) {
    cb && cb(err);
  });
};

HighLevelConsumer.prototype.setOffset = function (topic, partition, offset) {
  this.topicPayloads.every(function (p) {
    if (p.topic === topic && p.partition == partition) { // eslint-disable-line eqeqeq
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
