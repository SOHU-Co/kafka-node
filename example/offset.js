'use strict';

var kafka = require('../kafka');
var Client = kafka.Client;
var Offset = kafka.Offset;
var offset = new Offset(new Client());

offset.fetch([{topic: 'topic3', partition: 0, time: Date.now() + 8*60*60*1000, maxNum: 1}], function (err, data) {
    if (err) console.log(err)
    console.log('offset', data);
});
