var util = require('util')

/**
 * One or more topics' broker is not available.
 *
 * @param {String|String[]} topics Either an array or single topic name
 *
 * @constructor
 */
var TopicsBrokerNotAvailableError = function (topics) {
    this.topics = topics;
    this.message = 'The topic(s) ' + topics.toString() + ' broker(s) is not available.'
}

util.inherits(TopicsBrokerNotAvailableError, Error)
TopicsBrokerNotAvailableError.prototype.name = 'TopicsBrokerNotAvailableError'

module.exports = TopicsBrokerNotAvailableError