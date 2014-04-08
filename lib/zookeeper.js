'use strict';

var zookeeper = require('node-zookeeper-client')
    , util = require('util')
    , EventEmiter = require('events').EventEmitter;

/**
 * Create a zookeeper client and get/watch brokers.
 * @param connect
 * @constructor
 */
var Zookeeper = function (connectionString, options) {
    this.client = zookeeper.createClient(connectionString,options);

    var that = this;
    this.client.on('connected', function () {
        that.listBrokers();
    });
    this.client.connect();
};
util.inherits(Zookeeper, EventEmiter);

Zookeeper.prototype.getBrokerDetail = function (brokerId, cb) {
    var path = '/brokers/ids/' + brokerId;
    this.client.getData(
        path,
        function (error, data) {
            if (error) {
                console.log('Zookeeper: Error occurred when getting data: %s.', error);
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
            if (error) {
                return that.emit('error',  util.format('Zookeeper: Failed to list children of node: %s due to: %s.', path, error));
                }

            if (children.length) {
                var brokers = {};
                var count = 0;
                children.forEach(function (brokerId) {
                    that.getBrokerDetail(brokerId, function (data) {
                        brokers[brokerId] = JSON.parse(data.toString());
                        if (++count == children.length) {
                            if (!that.inited) {
                                that.emit('init', brokers);
                                that.inited = true;
                            } else {
                                that.emit('brokersChanged', brokers)
                            }
                            cb && cb(brokers); //For test
                        }
                    });
                })
            } else {
                if (that.inited)
                    return that.emit('brokersChanged', {})
                that.inited = true;
                that.emit('init', {});
            }
        }
    );
};

Zookeeper.prototype.topicExists = function (topic, cb, watch) {
    var path = '/brokers/topics/' + topic,
        self = this;
    this.client.exists(
        path,
        function (event) {
            console.log('Zookeeper: Got event: %s.', event);
            if (watch) self.topicExists(topic, cb);
        },
        function (error, stat) {
            if (error) return;
            cb(!!stat, topic);
        }
    );
}

module.exports = Zookeeper;
