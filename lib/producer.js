'use strict';

var util = require('util'),
    events = require('events'),
    _ = require('lodash'),
    async = require('async'),
    Client = require('./client'),
    protocol = require('./protocol'),
    Message = protocol.Message,
    ProduceRequest = protocol.ProduceRequest,
    partitioner = require('./partitioner');

var PARTITIONER_TYPES = {
    default: 0,
    random: 1,
    keyed: 2
};

/*
 *
    requireAcks: Indicates when a produce request is considered completed:
      0: Never wait for an acknowledgement from the broker.
      1: Get an acknowledgement after the leader replica has received the messages.
     -1: Get an acknowledgement after all in-sync replicas have received the messages.
      x: Get an acknowledgement after an x number of in-sync replicas have received the messages.
    ackTimeoutMs: The amount of time the broker will wait trying to meet the requireAcks requirement before sending back an error.
    partitionerType: Defines how messages will be written to a topic's partitions.
    metadataRefreshIntervalMs: The interval to refresh the topics' metadata. A negative value means metadata will only get refreshed on failure, 0 means the topics will be refreshed on every message push (not recommended).
 *
 */
var DEFAULTS =
{
    requireAcks: 1,
    ackTimeoutMs: 100,
    partitionerType: PARTITIONER_TYPES.default,
    metadataRefreshIntervalMs: 600 * 1000 // 10 min
};

var Producer = function (client, options) {
    this.ready = false;
    this.client = client;

    this.buildOptions(options);
    this.connect();
    this.lastMetadataRefreshTime = new Date().getTime();
}
util.inherits(Producer, events.EventEmitter);

Producer.prototype.buildOptions = function (options) {
    this.options = _.defaults((options||{}), DEFAULTS);

    if (this.options.partitionerType == PARTITIONER_TYPES.random) {
        this.options.partitioner = new partitioner.RandomPartitioner();
    } else if (this.options.partitionerType == PARTITIONER_TYPES.keyed) {
        this.options.partitioner = new partitioner.KeyedPartitioner();
    } else {
        this.options.partitioner = new partitioner.DefaultPartitioner();
    }
}

Producer.prototype.connect = function () {
    // emiter...
    var self = this;
    this.ready = this.client.ready;
    if (this.ready) self.emit('ready');
    this.client.on('ready', function () {
        if (!self.ready) self.emit('ready'); 
        self.ready = true;
    });
    this.client.on('error', function (err) {
    });
    this.client.on('close', function () {
    });
}

Producer.prototype.send = function (payloads, cb) {
    var self = this;

    this.buildPayloads(payloads, function (err, produceRequests) {
        if (err) {
            cb(err, null);
        } else {
            // Regroup ProduceRequests by topic and partition since there a bug when we try to send 2 ProduceRequests with the same topic and partition
            var groupedProduceRequests = []

            _.forEach(produceRequests, function (produceRequest) {
                var exists = _.find(groupedProduceRequests, function (pr) {
                    return ((pr.topic == produceRequest.topic) && (pr.partition == produceRequest.partition));
                });

                if (_.isEmpty(exists)) {
                    var messages = [];

                    _.chain(produceRequests)
                     .filter(function (p) {
                         return ((p.topic == produceRequest.topic) && (p.partition == produceRequest.partition));
                     })
                     .forEach(function (p) {
                         messages = messages.concat(p.messages)
                     })
                     .value();

                    groupedProduceRequests.push(new ProduceRequest(produceRequest.topic, produceRequest.partition, messages));
                }
            });

            self.client.sendProduceRequest(groupedProduceRequests, self.options.requireAcks, self.options.ackTimeoutMs, cb);
        }
    });
}

Producer.prototype.buildPayloads = function (payloads, buildPayloadsCallback) {
    var self = this;

    // Create array of ProduceRequest instances, grouping messages by topic and partition
    async.map(payloads, function (p, payloadsMapCallback) {

        var messages = _.isArray(p.messages) ? p.messages : [p.messages];

        // Create array of Messages from the payloads
        messages = messages.map(function (message) {
            return new Message(0, 0, '', message);
        });

        async.waterfall([
          // Refresh the topics metadata after a regular interval
          function (callback) {
              if (self.options.metadataRefreshIntervalMs >= 0 && new Date().getTime() - self.lastMetadataRefreshTime > self.options.metadataRefreshIntervalMs) {
                  self.lastMetadataRefreshTime = new Date().getTime();

                  self.client.refreshMetadata(null, function (err) {
                      callback(err);
                  });
              } else {
                  callback();
              }
          },
          function (callback) {
              // If a partition is passed in the payload, use it (for backward compatibility)
              if (!_.isUndefined(p.partition)) {
                  callback(null, [p.partition]);
              } else {
                  // If topicPartitions contains the topic's partitions, use them, else get them
                  if (_.isUndefined(self.client.topicPartitions[p.topic])) {
                      self.client.refreshMetadata([p.topic], function (err) {
                          if (err) {
                              callback(err);
                          } else {
                              callback(null, self.client.topicPartitions[p.topic]);
                          }
                      });
                  } else {
                      callback(null, self.client.topicPartitions[p.topic]);
                  }
              }
          }
        ],
          function (err, partitions) {
              if (err) {
                  payloadsMapCallback(err)
              } else {
                  payloadsMapCallback(null, new ProduceRequest(p.topic, self.options.partitioner.getPartition(partitions, p.key), messages))
              }
          }
        );
    },
    function (err, results) {
        buildPayloadsCallback(err, results);
    });
};

Producer.prototype.createTopics = function (topics, async, cb) {
    var self = this;
    if (!this.ready) {
        setTimeout(function () {
            self.createTopics(topics, async, cb); 
        }, 100);
        return;
    }
    topics = typeof topic === 'string' ? [topics] : topics;
    if (typeof async === 'function' && typeof cb === 'undefined') {
        cb = async;
        async = true;
    }

    // first, load metadata to create topics
    this.client.loadMetadataForTopics(topics, function (err, resp) {
        if (async) return cb && cb(null, 'All requests sent');
        var topicMetadata = resp[1].metadata;
        // ommit existed topics
        var topicsNotExists = 
            _.pairs(topicMetadata)
            .filter(function (pairs) { return _.isEmpty(pairs[1]) })
            .map(function (pairs) { return pairs[0] });

        if (!topicsNotExists.length) return  cb && cb(null, 'All created');
        // check from zookeeper to make sure topic created
        self.client.createTopics(topicsNotExists, function (err, created) {
            cb && cb(null, 'All created');
        });
    });
}

function noAcks() {
    return 'Not require ACK';
}
module.exports = Producer;