var Producer = require('../lib/producer'),
    lineByLineReader = require('line-by-line');

var respTimes = 0;

var lr = new lineByLineReader('test/data.txt');
/*
*/
//192.168.105.223
var producer = new Producer();

//producer.client.loadMetadataForTopics([], function (resp) {
    //console.log(resp);
    //producer.client.updateMetadatas(resp);
    lr.on('line', function (line) {
            sendLine(line)
    });

//});
var count=0,req=0;
function sendLine(line) {
    var topic = line.slice(0, line.indexOf(' '));
    var msg = line.slice(line.indexOf(' ') + 1);
    //if (++req >= 10000) process.exit();
    producer.send([{topic: topic, message: msg}], function (data) {
//console.log(count)
        if (++count === 25600) process.exit();
    });    
}
/*
Client.prototype.test = function (chunk) {
    if (typeof chunk != "undefined") {
            // new data
            this.buffer.data = Buffer.concat([
                this.buffer.data,
                chunk
            ]);
        }

        if (!this.buffer.length) {
            // new response
            var vars = Binary.parse(this.buffer.data)
                .word32bu('length')
                .word32bu('correlationId')
                .vars;
            console.log('correlationId:', vars.correlationId);
           //console.log('buffer.data.length:', socket.buffer.data.length);
            this.buffer.length = vars.length + 4;
            this.buffer.correlationId = vars.correlationId;
            if (!this.buffer.length || // empty resp?
                this.buffer.data.length < this.buffer.length)
                return
        }
        global.d++;
console.log('s:',global.s,'r:', global.r, 'm:', global.m, 'd', global.d)
        // handle resp
        var resp = this.buffer.data.slice(0, this.buffer.length);

            // callback
            var handlers = this.cbqueue[this.buffer.correlationId],
            decoder = handlers[0],
            cb = handlers[1];
            // Delete callback functions after finish a request
            delete this.cbqueue[this.buffer.correlationId];
            var data = '';
            if (this.buffer.correlationId === 0) data = decoder(resp);
            if (typeof cb === 'function') cb.call(this, data);
        // clean buffer
        this.buffer.data = this.buffer.data.slice(this.buffer.length);
//        console.log('left data length:', this.buffer.data.length)
        this.buffer.length = 0;
        if (this.buffer.data.length) this.test();
}
*/
