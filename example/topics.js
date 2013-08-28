var Producer = require('../lib/producer');

var producer = new Producer('localhost:2181');

producer.createTopics(['t1', 't2', 't3', 't5', 't12', 't10'],false, function (err, data) {
    console.log(data);
});
