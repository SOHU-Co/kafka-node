var util = require('./util')
    , config = require('./config.json')
    , fs = require('fs');

var total = config.topicNum * config.msgNum * config.repeat;
var buffer = '';

function appendData(i) {
    fs.appendFileSync(config.dataSource, buffer);
    console.log('writed:', i);
    buffer = ''
}
for (var i = 0; i < total; i++) {
    var topic = util.md5(Math.floor(Math.random() * config.topicNum).toString());
    var str = util.randomString(32);
    buffer += topic + ' ' + str + '\n';
    !(i % 10000) && appendData(i)
}
buffer.length && appendData(i);
