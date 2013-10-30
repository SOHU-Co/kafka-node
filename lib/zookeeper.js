'use strict';

var zookeeper = require('node-zookeeper-client')
    , util = require('util')
    , EventEmiter = require('events').EventEmitter;

/**
 * Create a zookeeper client and get/watch brokers.
 * @param connect
 * @constructor
 */
var Zookeeper = function (connectionString) {
    this.client = zookeeper.createClient(
        connectionString || 'localhost:2181/kafka0.8'
    );

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

Zookeeper.prototype.topicExists = function (topic, cb, watch) {
    var path = '/brokers/topics/' + topic,
        self = this;
    this.client.exists(
        path,
        function (event) {
            console.log('Got event: %s.', event);
            if (watch) self.topicExists(topic, cb);
        },
        function (error, stat) {
            if (error) return;
            cb(!!stat, topic);
        }
    );
}

module.exports = Zookeeper;
