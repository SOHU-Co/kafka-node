'use strict';

var kafka = require('../kafka');
var Client = kafka.Client;
var Offset = kafka.Offset;
var offset = new Offset(new Client());

offset.fetch([{topic: 'new_1', partition: 0, maxNum: 2}, {topic: 'topic2', partition: 0, maxNum: 2}], function (err, data) {
    if (err) console.log(err)
    console.log('offset', data);
});
