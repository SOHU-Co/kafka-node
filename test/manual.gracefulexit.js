// Run this test with:
// mocha test/manual.gracefulexit.js --no-exit

var Client = require('../lib/client');

describe('Client', function () {
  describe('#close', function () {
    it('should close gracefully', function (done) {
      var client = new Client();
      client.on('ready', function () {
        client.close(done);
      });
    });
  });
});
