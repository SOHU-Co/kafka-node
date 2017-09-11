'use strict';

const retry = require('retry');
const logger = require('./logging')('kafka-node:ConsumerGroupRecovery');
const assert = require('assert');
const _ = require('lodash');

const GroupCoordinatorNotAvailable = require('./errors/GroupCoordinatorNotAvailableError');
const NotCoordinatorForGroup = require('./errors/NotCoordinatorForGroupError');
const IllegalGeneration = require('./errors/IllegalGenerationError');
const GroupLoadInProgress = require('./errors/GroupLoadInProgressError');
const UnknownMemberId = require('./errors/UnknownMemberIdError');
const RebalanceInProgress = require('./errors/RebalanceInProgressError');
const HeartbeatTimeout = require('./errors/HeartbeatTimeoutError');
const TimeoutError = require('./errors/TimeoutError');
const BrokerNotAvailableError = require('./errors').BrokerNotAvailableError;

const NETWORK_ERROR_CODES = [
  'ETIMEDOUT',
  'ECONNRESET',
  'ESOCKETTIMEDOUT',
  'ECONNREFUSED',
  'EHOSTUNREACH',
  'EADDRNOTAVAIL'
];

const recoverableErrors = [
  {
    errors: [
      GroupCoordinatorNotAvailable,
      IllegalGeneration,
      GroupLoadInProgress,
      RebalanceInProgress,
      HeartbeatTimeout,
      TimeoutError
    ]
  },
  {
    errors: [NotCoordinatorForGroup, BrokerNotAvailableError],
    handler: function () {
      delete this.client.coordinatorId;
    }
  },
  {
    errors: [UnknownMemberId],
    handler: function () {
      this.memberId = null;
    }
  }
];

function isErrorInstanceOf (error, errors) {
  return errors.some(function (errorClass) {
    return error instanceof errorClass;
  });
}

function ConsumerGroupRecovery (consumerGroup) {
  this.consumerGroup = consumerGroup;
  this.options = consumerGroup.options;
}

function isNetworkError (error) {
  if (error && error.code && error.errno) {
    return _.includes(NETWORK_ERROR_CODES, error.code);
  }
  return false;
}

ConsumerGroupRecovery.prototype.tryToRecoverFrom = function (error, source) {
  logger.debug('tryToRecoverFrom', source, error);
  this.consumerGroup.ready = false;
  this.consumerGroup.stopHeartbeats();

  var retryTimeout = false;
  var retry =
    isNetworkError(error) ||
    recoverableErrors.some(function (recoverableItem) {
      if (isErrorInstanceOf(error, recoverableItem.errors)) {
        recoverableItem.handler && recoverableItem.handler.call(this.consumerGroup, error);
        return true;
      }
      return false;
    }, this);

  if (retry) {
    retryTimeout = this.getRetryTimeout(error);
  }

  if (retry && retryTimeout) {
    logger.debug(
      'RECOVERY from %s: %s retrying in %s ms',
      source,
      this.consumerGroup.client.clientId,
      retryTimeout,
      error
    );
    this.consumerGroup.scheduleReconnect(retryTimeout);
  } else {
    this.consumerGroup.emit('error', error);
  }
  this.lastError = error;
};

ConsumerGroupRecovery.prototype.clearError = function () {
  this.lastError = null;
};

ConsumerGroupRecovery.prototype.getRetryTimeout = function (error) {
  assert(error);
  if (!this._timeouts) {
    this._timeouts = retry.timeouts({
      retries: this.options.retries,
      factor: this.options.retryFactor,
      minTimeout: this.options.retryMinTimeout
    });
  }

  if (this._retryIndex == null || this.lastError == null || error.errorCode !== this.lastError.errorCode) {
    this._retryIndex = 0;
  }

  var index = this._retryIndex++;
  if (index >= this._timeouts.length) {
    return false;
  }
  return this._timeouts[index];
};

module.exports = ConsumerGroupRecovery;
