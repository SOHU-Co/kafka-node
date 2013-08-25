var util = require('./util')
    , fs = require('fs')

var total = util.topicNum * util.msgNum
for (var i = 0; i < total; i++) {
    var topic = util.md5(Math.floor(Math.random() * util.topicNum).toString())
    var str = util.randomString(2048 + Math.random() * 2048)
    str = topic + ' ' + str + '\n'
    fs.appendFileSync('data.txt', str)
}
