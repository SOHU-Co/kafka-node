'use strict';

const HeartbeatTimeoutError = require('./errors/HeartbeatTimeout');
const debug = require('debug')('kafka-node:ConsumerGroupHeartbeat');

function getHrTimeInMs (time) {
  return (time[0] * 1e9 + time[1]) / 1e6;
}

module.exports = class Heartbeat {
  constructor (client, timeout, handler) {
    this.client = client;
    this.timeoutMs = timeout;
    this.handler = handler;

    this.pending = true;
  }

  send (groupId, generationId, memberId) {
    this.startTime = process.hrtime();
    this.client.sendHeartbeatRequest(groupId, generationId, memberId, (error) => {
      if (this.canceled) {
        debug('heartbeat yielded after being canceled', error);
        return;
      }
      this.pending = false;
      this.handler(error);
    });
  }

  verifyResolved () {
    const timeTook = getHrTimeInMs(process.hrtime(this.startTime));
    if (this.pending && timeTook > this.timeoutMs) {
      this.canceled = true;
      this.pending = false;
      this.handler(new HeartbeatTimeoutError(`Heartbeat timed out. Took ${timeTook}ms`));
      return false;
    }
    return true;
  }
};
