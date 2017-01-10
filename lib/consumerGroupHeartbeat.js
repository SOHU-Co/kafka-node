'use strict';

const HeartbeatTimeoutError = require('./errors/HeartbeatTimeoutError');
const debug = require('debug')('kafka-node:ConsumerGroupHeartbeat');

module.exports = class Heartbeat {
  constructor (client, handler) {
    this.client = client;
    this.handler = handler;
    this.pending = true;
  }

  send (groupId, generationId, memberId) {
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
    if (this.pending) {
      this.canceled = true;
      this.pending = false;
      this.handler(new HeartbeatTimeoutError('Heartbeat timed out'));
      return false;
    }
    return true;
  }
};
