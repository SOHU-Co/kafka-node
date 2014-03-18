var util = require('util')

/**
 * One or more topics' have encountered an error while running the requested operation.
 *
 * @example Format:
 * {
 *   'topics': {
 *     'topic0': {
 *       'errorCode': 5,
 *       'errorMessage': 'LeaderNotAvailable'
 *      },
 *     'topic1': {
 *       'errorCode': 2,
 *       'partitions': [0,1]
 *       'errorMessage': 'InvalidMessage'
 *     }
 *   },
 *   'message': 'One or more topic\'s have encountered an  error while running the requested operation.'
 * }
 *
 * @constructor
 */
var TopicsError = function () {
    this.topics = {};
    this.message = 'One or more topic\'s have encountered an error while running the requested operation.'

    /** @protected */
    TopicsError.prototype._addTopic = function (payload, errorCode, errorMessage) {
        this.topics[payload.topic] = this.topics[payload.topic] || {};
        if (typeof payload.partition !== 'undefined') {
            this.topics[payload.topic].partitions = this.topics[payload.topic].partitions || [];
            this.topics[payload.topic].partitions.push(payload.partition);
        }
        this.topics[payload.topic].errorCode = errorCode;
        this.topics[payload.topic].errorMessage = errorMessage;
    }
}

util.inherits(TopicsError, Error)
TopicsError.prototype.name = 'TopicsError'

module.exports = TopicsError