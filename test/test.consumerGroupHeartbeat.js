'use strict';

const sinon = require('sinon');
const should = require('should');
const Heartbeat = require('../lib/consumerGroupHeartbeat');
const HeartbeatTimeout = require('../lib/errors/HeartbeatTimeoutError');

describe('Consumer Group Heartbeat', function () {
  let sandbox;

  beforeEach(function () {
    sandbox = sinon.sandbox.create();
  });

  afterEach(function () {
    sandbox.restore();
  });

  it('should call heartbeat handler if heartbeat yields error', function (done) {
    const mockClient = {
      sendHeartbeatRequest: sandbox.stub().yieldsAsync(new Error('busted'))
    };

    const heartbeat = new Heartbeat(mockClient, function (error) {
      error.message.should.eql('busted');
      sinon.assert.calledWithExactly(mockClient.sendHeartbeatRequest, 'groupId', 1, 'fake-member-id', sinon.match.func);
      heartbeat.pending.should.be.false;
      setImmediate(done);
    });

    heartbeat.pending.should.be.true;
    heartbeat.send('groupId', 1, 'fake-member-id');
    setImmediate(() => heartbeat.verifyResolved().should.be.true);
  });

  it('should call heartbeat handler if heartbeat yields null', function (done) {
    const mockClient = {
      sendHeartbeatRequest: sandbox.stub().yieldsAsync(null)
    };

    const heartbeat = new Heartbeat(mockClient, function (error) {
      should(error).be.null;
      sinon.assert.calledWithExactly(mockClient.sendHeartbeatRequest, 'groupId', 1, 'fake-member-id', sinon.match.func);
      heartbeat.pending.should.be.false;
      setImmediate(done);
    });

    heartbeat.pending.should.be.true;
    heartbeat.send('groupId', 1, 'fake-member-id');
    setImmediate(() => heartbeat.verifyResolved().should.be.true);
  });

  it('should call heartbeat handler with instance of TimeoutError if heartbeat timed out', function (done) {
    const mockClient = {
      sendHeartbeatRequest: sandbox.stub()
    };

    const heartbeat = new Heartbeat(mockClient, function (error) {
      error.should.be.an.instanceOf(HeartbeatTimeout);
      sinon.assert.calledWithExactly(mockClient.sendHeartbeatRequest, 'groupId', 1, 'fake-member-id', sinon.match.func);
      heartbeat.pending.should.be.false;
      heartbeat.canceled.should.be.true;
      setImmediate(done);
    });

    heartbeat.pending.should.be.true;
    heartbeat.send('groupId', 1, 'fake-member-id');
    setImmediate(function () {
      heartbeat.verifyResolved().should.be.false;
    });
  });
});
