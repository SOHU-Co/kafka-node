var util = require('./util')
    , config = require('./config.json')
    , fs = require('fs');

var total = config.topicNum * config.msgNum;
var buffer = '';
for (var i = 0; i < total; i++) {
    var topic = util.md5(Math.floor(Math.random() * config.topicNum).toString());
    var str = util.randomString(2048 + Math.random() * 2048);
    buffer += topic + ' ' + str + '\n';
    if (i % 10000 == 0) {
        fs.appendFileSync(config.dataSource, buffer);
        console.log('writed:', i);
        buffer = ''
    }
}

fs.appendFileSync(config.dataSource, buffer);
console.log('writed:', i);
