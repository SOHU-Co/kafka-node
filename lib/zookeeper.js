var zookeeper = require('node-zookeeper-client')
    , util = require('util')
    , EventEmiter = require('events').EventEmitter;

var Zookeeper = function (connect) {
    this.client = zookeeper.createClient(
        connect || 'localhost:2181'
    );

    var that = this;
    this.client.on('connected', function () {
        console.log('Connected to ZooKeeper.');
        that.listChildren();
    });
    this.client.connect();
};

util.inherits(Zookeeper, EventEmiter);

Zookeeper.prototype.getData = function (path, cb) {
    this.client.getData(
        path,
        function (error, data) {
            if (error) {
                console.log('Error occurred when getting data: %s.', error);
            }

            cb && cb(data)
        }
    );
};
Zookeeper.prototype.listChildren = function () {
    var that = this;
    var path = '/brokers/ids';
    this.client.getChildren(
        path,
        function () {
            that.listChildren();
        },
        function (error, children) {
            if (error) {
                console.log(
                    'Failed to list children of node: %s due to: %s.',
                    path,
                    error
                );
                return;
            }

            if (children.length) {
                var count = 0;
                var brokers = {};
                children.forEach(function (brokerId) {
                    that.getData('/brokers/ids/' + brokerId, function (data) {
                        brokers[brokerId] = JSON.parse(data.toString());
                        if (++count == children.length) {
                            that.emit('brokersChanged', brokers)
                        } else if (!that.inited) {
                            that.emit('init', brokers);
                            that.inited = true;
                        }
                    })
                })
            } else {
                that.emit('brokersChanged', {})
            }
        }
    );
};

module.exports = Zookeeper;

