'use strict';

var HeartbeatTimeoutError = require('./errors/HeartbeatTimeoutError');
var logger = require('./logging')('kafka-node:ConsumerGroupHeartbeat');

//module.exports = class Heartbeat {
//  constructor (client, handler) {
  
function  Heartbeat (client, handler) {
    this.client = client;
    this.handler = handler;
    this.pending = true;
  }

Heartbeat.prototype.send = function (groupId, generationId, memberId) {
    this.client.sendHeartbeatRequest(groupId, generationId, memberId, function (error) {
      if (this.canceled) {
        logger.debug('heartbeat yielded after being canceled', error);
        return;
      }
      this.pending = false;
      this.handler(error);
    });
  }

Heartbeat.prototype.verifyResolved = function () {
    if (this.pending) {
      this.canceled = true;
      this.pending = false;
      this.handler(new HeartbeatTimeoutError('Heartbeat timed out'));
      return false;
    }
    return true;
  }
// };
module.exports = Heartbeat;
