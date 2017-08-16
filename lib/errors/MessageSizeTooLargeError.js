var util = require('util');

var MessageSizeTooLarge = function (vars) {
  Error.captureStackTrace(this, this);
  if (typeof vars === 'object') {
    this.message = `Found a message larger than the maximum fetch size of this consumer on topic ${vars.topic} partition ${vars.partition} at fetch offset ${vars.offset}. Increase the fetch size, or decrease the maximum message size the broker will allow.`;
  } else {
    this.message = vars;
  }
};

util.inherits(MessageSizeTooLarge, Error);
MessageSizeTooLarge.prototype.name = 'MessageSizeTooLarge';

module.exports = MessageSizeTooLarge;
