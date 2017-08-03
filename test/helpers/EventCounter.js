'use strict';

class EventCounter {
  constructor () {
    this.events = {};
  }

  /**
   * Creates an event counter with optional expected number and callback.
   *
   * @param {String} eventName - The name of the event counter.
   * @param {Number} limit - The number of events after which the callback
   *   should be called.
   * @param {Function} callback - The callback to envoke when the expected
   *   number is reached.
   * @returns {Function} - A function that can be called to increment the
   *   coutner and collect the result.
   */
  createEventCounter (eventName, limit, callback) {
    if (!limit) {
      limit = Number.POSITIVE_INFINITY;
    }
    this.events[eventName] = {
      count: 0,
      events: []
    };
    return function () {
      this.events[eventName].count++;
      this.events[eventName].events.push(arguments);
      if (this.events[eventName].count === limit) {
        if (callback) {
          callback(null, this.events[eventName]);
        }
      }
    }.bind(this);
  }
}

module.exports = EventCounter;
