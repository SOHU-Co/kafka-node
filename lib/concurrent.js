'use strict';

module.exports = {
    lock: lock,
    retry: retry,
    retryWithDelay: retryWithDelay,
    parallelLocked: parallelLocked,
};

var async = require('async'),
    errors = require('./errors');

/*
 * Returns a lock function, which:
 * - interrupts the lock owner
 * - acquires the lock
 * - calls txn(release)
 * - releases the lock
 * - calls cb
 * lock.cancels inserts a cancellation point before a thunk, which now fails when there are waiters
 */
function lock () {
    var waiters = {}, start = 0, end = 0, locked = false;
    function runInLock(txn /* (cb (err, ...)) */, cb /* (err, ...) */) {
        if (locked) {
            waiters[end++] = acquired;
        } else {
            acquired();
        }
        function acquired () {
            locked = true;
            try {
                txn(function (err) {
                    locked = false;
                    cb && cb.apply(this, arguments);
                    if (start < end) {
                        setImmediate(waiters[start++]); // same guarantee as java
                    }
                });
            } catch (err) {
                if (locked) {
                    locked = false;
                    cb && cb.apply(this, arguments);
                }
                if (start < end) {
                    setImmediate(waiters[start++]); // same guarantee as java
                }
                throw err;
            }
        }
    }
    runInLock.cancels = function (thunk /* (..., cb(err, ...)) */) {
        var debug = new Error().stack; // XXX
        return function () {
            if (runInLock.cancellable) { // make this a runtime switch
                console.assert(locked, debug);
                if (start < end) {
                    var cb = null;
                    for (var i = arguments.length - 1; i >= 0; --i) {
                        cb = arguments[i];
                        if (cb != null) { // undefined, null, or a function
                            break;
                        }
                    }
                    console.log('interrupted!');
                    cb(new errors.InterruptedError('interrupted')); // scala returns the stack of the interrupted thread
                    return;
                }
            }
            return thunk.apply(this, arguments);
        };
    };
    runInLock.cancellable = true;
    return runInLock;
}

function retry() {
    var funcs = [];
    for (var i = 0; i < arguments.length; ++i) {
        if (arguments[i] instanceof Function) {
            funcs.push(i);
        }
    }
    if (funcs.length) { // should always be true
        var interruptedError = null;
        var task = arguments[funcs[0]];
        arguments[funcs[0]] = function () {
            for (var i = arguments.length - 1; i >= 0; --i) {
                var cb = arguments[i]; // we need to trick async's spy
                if (cb != null) {
                    arguments[i] = function (err) {
                        if (err instanceof errors.InterruptedError) {
                            console.assert(interruptedError === null);
                            interruptedError = err;
                            return cb(); // "succeeded"
                        }
                        return cb.apply(this, arguments);
                    };
                    break;
                }
            }
            return task.apply(this, arguments);
        };
        if (funcs.length > 1) {
            var callback = arguments[funcs[1]];
            arguments[funcs[1]] = function (err) {
                if (interruptedError) {
                    console.assert(!err);
                    arguments[0] = interruptedError;
                    interruptedError = null;
                }
                return callback.apply(this, arguments);
            };
        }
    }
    return async.retry.apply(this, arguments);
}

function retryWithDelay (times, delay, task, cb, failure) {
    failure = failure || function (cb) { cb(); };
    retry(times, function (cb) {
        task(function (err) {
            var thiz = this, argumentz = arguments;
            if (err) {
                failure(function () {
                    setTimeout(function () {
                        cb.apply(thiz, argumentz); // failure is deferred
                    }, delay);
                });
            } else {
                cb.apply(thiz, argumentz); // success is immediate
            }
        });
    }, cb);
}

function parallelLocked (tasks, callback) {
    var errors = [];
    async.map(tasks, function (task, cb) {
        task(function (err, res) {
            if (err) {
                errors.push(err);
            }
            cb(null, res);
        });
    }, function (err, results) {
        console.assert(!err);
        if (errors.length) {
            errors[0].errors = errors;
            callback(errors[0]);
            return;
        }
        callback(null, results);
    });
}
