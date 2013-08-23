var Producer = require('../lib/producer');

var producer = new Producer();

producer.send([ 
    {topic: 'sdsh-topic', message: 'hello world3'},
    {topic: 't-topic', message: 'hello world3'} 
],function (data) {
    console.log(data);
});
