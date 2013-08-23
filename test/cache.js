var Cache = require('../lib/cache');

var c = new Cache();

var i = 0;
setInterval(function () {
    c.set('topic' + i++, {partition: ++i})
}, 1000);
setInterval(function () {
    console.log(c._store);
}, 1000);
