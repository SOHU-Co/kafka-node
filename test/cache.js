var Cache = require('../lib/cache');

var c = new Cache();

var i = 0;
setInterval(function () {
    c.set('topic' + i++, {partition: ++i})
}, 1000);
setInterval(function () {
    console.log(c.get('topic20'));
}, 1000);
