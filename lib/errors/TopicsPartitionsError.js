var util = require('util')

/**
 * One or more of the topics\' partitions have encountered an error while running the requested operation.
 *
 * @example Format:
 * {
 *   'topics': {
 *     'topic1': { 'partitions': { '0': { 'offset': 4, 'errorCode': 7, "errorMessage': 'RequestTimedOut' } } },
 *     'topic0': { 'partitions': { '0': { 'offset': 4, 'errorCode': 7, 'errorMessage': 'RequestTimedOut' }, '1': { 'offset': 4, 'errorCode': 7, 'errorMessage': 'RequestTimedOut' } } } },
 *   'message': 'One or more of the topic\'s partitions have encountered an error while running the requested operation.'
 * }
 *
 * @constructor
 */
var TopicsPartitionsError = function () {
    this.topics = {};
    this.message = 'One or more of the topic\'s partitions have encountered an error while running the requested operation.'

    /** @protected */
    TopicsPartitionsError.prototype._addTopic = function (payload, errorCode, errorMessage) {
        this.topics[payload.topic] = this.topics[payload.topic] || { partitions: {} };
        this.topics[payload.topic].partitions[payload.partition] = {};
        if ((typeof payload.offset !== 'undefined')) {
            this.topics[payload.topic].partitions[payload.partition].offset = payload.offset;
        }
        this.topics[payload.topic].partitions[payload.partition].errorCode = errorCode;
        this.topics[payload.topic].partitions[payload.partition].errorMessage = errorMessage;
    }
}

util.inherits(TopicsPartitionsError, Error)
TopicsPartitionsError.prototype.name = 'TopicsPartitionsError'

module.exports = TopicsPartitionsError