'use strict';

var kafka = require('..');
var Client = kafka.Client;
var Offset = kafka.Offset;
var offset = new Offset(new Client());
var topic = 'topic1';

// Fetch available offsets
offset.fetch([
  { topic: topic, partition: 0, maxNum: 2 },
  { topic: topic, partition: 1 }
], function (err, offsets) {
  console.log(err || offsets);
});

// Fetch commited offset
offset.commit('kafka-node-group', [
  { topic: topic, partition: 0 }
], function (err, result) {
  console.log(err || result);
});
