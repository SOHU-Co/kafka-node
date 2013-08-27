'use strict';

var Consumer = require('../lib/consumer');

var consumer = new Consumer([{topic: 'topic'}]);

consumer.fetchOffset(function (err, data) {
    console.log('offset', data);
});
