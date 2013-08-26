var Zookeeper = require('../lib/zookeeper');
var z = new Zookeeper();
z.once('init', function () {
    console.log('init', arguments)
});
z.on('brokersChanged', function () {
    console.log('brokerChanged', arguments)
});
