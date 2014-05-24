'use strict';

var kafka = require('../kafka'),
    async = require('async');

var client = new kafka.Client('localhost:2181/');
var payloads = [];
for (var i = 0; i < 1000; ++i) {
  payloads.push({
    topic: 'test',
    partition: i,
  });
}

var consumer = new kafka.Consumer(client, payloads, { fromBeginning: false, fromOffset: false });
var count = {};
consumer.on('message', function(message) {
  count[message.value] = (count[message.value] || 0) + 1;
});
setInterval(function() {
  var hist = {};
  for (var i = 1; i <= 1000000; ++i) {
    var k = i < 1e6 ? i + '' : '1e+06';
    hist[count[k]] = (hist[count[k]] || 0) + 1;
  }
  console.log(hist);
}, 1000);
