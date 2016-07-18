'use strict';

var sinon = require('sinon');
var HighLevelConsumer = require('../lib/highLevelConsumer');
var FakeClient = require('./mocks/mockClient');
var should = require('should');
var InvalidConfigError = require('../lib/errors/InvalidConfigError');

describe('HighLevelConsumer', function () {
  describe('validate groupId', function () {
    function validateThrowsInvalidConfigError (groupId) {
      should.throws(function () {
        var client = new FakeClient();
        // eslint-disable-next-line no-new
        new HighLevelConsumer(client, [ { topic: 'some_topic' } ], {groupId: groupId});
      }, InvalidConfigError);
    }

    function validateDoesNotThrowInvalidConfigError (groupId) {
      should.doesNotThrow(function () {
        var client = new FakeClient();
        // eslint-disable-next-line no-new
        new HighLevelConsumer(client, [ { topic: 'some_topic' } ], {groupId: groupId});
      });
    }

    it('should throws an error on invalid group IDs', function () {
      validateThrowsInvalidConfigError('myGroupId:12345');
      validateThrowsInvalidConfigError('myGroupId,12345');
      validateThrowsInvalidConfigError('myGroupId"12345"');
      validateThrowsInvalidConfigError('myGroupId?12345');
    });

    it('should not throw on valid group IDs', function () {
      validateDoesNotThrowInvalidConfigError('myGroupId.12345');
      validateDoesNotThrowInvalidConfigError('something_12345');
      validateDoesNotThrowInvalidConfigError('myGroupId-12345');
    });
  });

  describe('rebalance', function () {
    var client,
      highLevelConsumer,
      sandbox;

    beforeEach(function () {
      client = new FakeClient();

      highLevelConsumer = new HighLevelConsumer(
        client,
        [ {topic: 'fake-topic'} ]
      );

      clearTimeout(highLevelConsumer.checkPartitionOwnershipInterval);

      sandbox = sinon.sandbox.create();
    });

    afterEach(function () {
      highLevelConsumer.close(function () {});
      sandbox.restore();
      client = null;
      highLevelConsumer = null;
    });

    it('should emit rebalanced event and clear rebalancing flag only after offsets are updated', function (done) {
      client.emit('ready');

      highLevelConsumer.on('registered', function () {
        // verify rebalancing is false until rebalance finishes
        var refreshMetadataStub = sandbox.stub(client, 'refreshMetadata', function (topicNames, cb) {
          highLevelConsumer.rebalancing.should.be.true;
          setImmediate(cb);
        });

        var sendOffsetFetchRequestStub = sandbox.stub(client, 'sendOffsetFetchRequest', function (groupId, payloads, cb) {
          highLevelConsumer.rebalancing.should.be.true;
          // wait for the results
          setImmediate(function () {
            // verify again before the callback
            highLevelConsumer.rebalancing.should.be.true;
            cb();
          });
        });

        highLevelConsumer.on('rebalanced', function () {
          refreshMetadataStub.calledOnce.should.be.true;
          sendOffsetFetchRequestStub.calledOnce.should.be.true;
          sinon.assert.callOrder(refreshMetadataStub, sendOffsetFetchRequestStub);
          highLevelConsumer.rebalancing.should.be.false;
          done();
        });
      });

      highLevelConsumer.on('error', function (err) {
        done(err);
      });
    });

    it('should emit error and clear rebalancing flag if fetchOffset failed', function (done) {
      client.emit('ready');

      highLevelConsumer.on('registered', function () {
        sandbox.stub(client, 'sendOffsetFetchRequest', function (groupId, payloads, cb) {
          setImmediate(cb, new Error('Fetching offset failed'));
        });

        highLevelConsumer.on('rebalanced', function () {
          done(new Error('rebalance is not expected to succeed'));
        });
      });

      highLevelConsumer.on('error', function (err) {
        if (err.name === 'FailedToRebalanceConsumerError' && err.message === 'Fetching offset failed') {
          done();
        } else {
          done(err);
        }
      });
    });

    it('should ignore fetch calls from "done" event handler during rebalance', function (done) {
      client.emit('ready');

      var sendFetchRequestSpy = sandbox.spy(client, 'sendFetchRequest');
      var fetchSpy = sandbox.spy(highLevelConsumer, 'fetch');

      highLevelConsumer.on('registered', function () {
        client.sendOffsetFetchRequest = function (groupId, payloads, cb) {
          // simulate a done event before offset fetch returns
          highLevelConsumer.ready = true;
          highLevelConsumer.paused = false;
          highLevelConsumer.emit('done', {});

          setTimeout(function () {
            cb();
          }, 100);
        };

        highLevelConsumer.on('rebalanced', function () {
          if (fetchSpy.callCount !== 2) {
            done(fetchSpy.callCount.should.equal(2));
            return;
          }

          if (!sendFetchRequestSpy.calledOnce) {
            done(new Error('client.sendFetchRequest expected to be called only once'));
            return;
          }

          done();
        });
      });

      highLevelConsumer.on('error', function (err) {
        done(err);
      });
    });
  });
});
