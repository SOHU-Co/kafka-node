// Run this test with:
// mocha test/manual.gracefulexit.js --no-exit

var Client = require('../lib/client');
var config = require('./config');

describe('Client', function () {
    describe('#close', function () {
        it('should close gracefully', function (done) {
            var client = new Client(config.zoo);
            client.on('ready', function () {
                client.close(done);
            });
        });
    });
});
