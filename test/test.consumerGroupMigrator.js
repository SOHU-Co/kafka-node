'use strict';

const sinon = require('sinon');
const ConsumerGroupMigrator = require('../lib/consumerGroupMigrator');
const EventEmitter = require('events').EventEmitter;

describe('ConsumerGroupMigrator', function () {
  describe('#saveHighLevelConsumerOffsets and #getOffset', function () {
    it('saves HLC offsets and maps offsets with -1 to 0', function (done) {
      const fakeClient = new EventEmitter();
      fakeClient.ready = false;
      fakeClient.sendOffsetFetchRequest = sinon.stub().yields(null, {
        TestTopic: {
          0: -1,
          1: 0,
          2: 10
        },
        TestEvent: {
          '0': -1
        }
      });

      const consumerGroup = {
        client: fakeClient,
        options: {
          migrateRolling: false
        }
      };

      const migrator = new ConsumerGroupMigrator(consumerGroup);
      migrator.saveHighLevelConsumerOffsets(['TestTopic', 'TestEvent'], function (error) {
        migrator.getOffset({topic: 'TestTopic', partition: 0}, 0).should.be.eql(0);
        migrator.getOffset({topic: 'TestTopic', partition: 1}, 23).should.be.eql(0);
        migrator.getOffset({topic: 'TestTopic', partition: 2}, 128).should.be.eql(10);
        migrator.getOffset({topic: 'TestEvent', partition: 0}, 4).should.be.eql(4);
        migrator.getOffset({topic: 'TestEvent', partition: 1}, 222).should.be.eql(222);
        done(error);
      });
    });
  });
});
