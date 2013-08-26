var ZooKeeper = require('zookeeper')
    , util = require('util')
    , EventEmiter = require('events').EventEmitter;


var Zookeeper = function (connect) {
    this.zk = new ZooKeeper({
        connect: connect || 'localhost:2181'
    });

    var that = this;
    this.zk.connect(function (err) {
        if (err) throw err;

        that.zk.aw_get_children('/brokers/ids'
            , watchCb.bind(that)
            , childCb.bind(that)
        )
    });
};

util.inherits(Zookeeper, EventEmiter);


var watchCb = function () {
    this.zk.aw_get_children('/brokers/ids', watchCb.bind(this), childCb.bind(this))
};
var childCb = function (rc, error, children) {
    if (children.length) {
        var count = 0;
        var brokers = {};
        var that = this;
        children.forEach(function (brokerId) {
            that.zk.a_get('/brokers/ids/' + brokerId, null, function (rc, error, stat, data) {
                if (rc) throw error;

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
        this.emit('brokersChanged', {})
    }
};

module.exports = Zookeeper;

