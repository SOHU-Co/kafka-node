'use strict';

var kafka = require('../kafka');

var client = new kafka.Client('localhost:2181/');

var consumer = new kafka.MessageConsumer(client, ['test'], { fromBeginning: true, fromOffset: true });

var count = {}, messageCount = 0;
consumer.on('message', function(message) {
  ++messageCount;
  count[message.value] = (count[message.value] || 0) + 1;
});
setInterval(function() {
  var hist = {};
  for (var i = 1; i <= 1000000; ++i) {
    var k = i < 1e6 ? i + '' : '1e+06';
    hist[count[k]] = (hist[count[k]] || 0) + 1;
  }
  console.log(hist, messageCount);
}, 1000);
