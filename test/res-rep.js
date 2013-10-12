var kafka = require('../');

var client = new kafka.Client('localhost:2181/kafka0.8')
    , groupId = 'test-group'
    , waitMs = 10000;
client.on('error', function () {
    console.log('client error:', arguments)
});
['1-news', '1-news', 'test-topic'].forEach(function (topic, index) {
    var c = new kafka.Consumer(client,
        [
            {topic: topic}
        ],
        {
            groupId: groupId,
            fetchMaxWaitMs: waitMs,
            autoCommit: false
        }
    );
    ['error', 'message', 'fetch'].forEach(function (event) {
        c.on(event, function () {
            console.log('consumer:', index, topic, ':', event, arguments);
        })
    });
});

setInterval(function () {
    console.log('------------------');
}, waitMs);

//client.once('ready', function () {
//    var o = new kafka.Offset(client);
//    o.commit(groupId, [
//        {topic: '1-news', offset: 2222}
//    ], function () {
//        console.log(arguments);
//    })
//})
