var util = require('util');

/**
 * One or more topics did not exist for the requested action
 *
 * @param {String|String[]} topics Either an array or single topic name
 *
 * @constructor
 */
var TopicsNotExistError = function (topics) {
    Error.captureStackTrace(this, this);
    this.topics = topics;
    this.message = 'The topic(s) ' + topics.toString() + ' do not exist';
};

util.inherits(TopicsNotExistError, Error);
TopicsNotExistError.prototype.name = 'TopicsNotExistError';
;
module.exports = TopicsNotExistError;
