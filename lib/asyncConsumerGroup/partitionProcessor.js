'use strict';

const EventEmitter = require('events');
const async = require('async');
const retry = require('retry');
const debug = require('debug')('kafka-node:asyncConsumerGroup:PartitionProcessor');

class PartitionProcessor extends EventEmitter {
  constructor (topic, partition, fn, commitManager, options) {
    super();
    this.topic = topic;
    this.partition = partition;
    this.options = options;
    this.fn = fn;
    this.acquiescing = false;
    this.commitManager = commitManager;

    //
    // TODO may be useful to opt-in to more parallelism within a single partition if deliver order doesn't matter.
    // Ideally users would simply just scale up the number of partitions to increase parallelism.
    //
    this.queue = async.queue((task, done) => {
      //
      // FIXME hard to tell async.queue "let me know when you're done with the current task, but process no more"
      // hack around it in O(n), but a custom implementation could acquiesce in O(1) once the last task completes.
      //
      if (this.acquiescing) {
        return done();
      }

      //
      // TODO set a ceiling on processed-but-not--yet-committed messages by playing around with the call to done()
      // or the call to resolve() or impl of ack() in dispatchMessage. This is super duper interesting in ways that
      // may not be obvious at first glance.
      //
      // e.g. pipeline up to N messages before requiring an interleaving commit per partition; or e.g. require a commit
      // per message for certain critical low volume topics where it might hurt to do unnecessary reprocessing. VERY
      // USEFUL to have this sort of control. Folks will probably want to configure this either per topic, globally or
      // both.
      //
      // The implementation won't by *hard* but it will be a little fiddly. Punting until we have some consensus on the
      // general shape of this thing.
      //
      this._dispatchMessage(task)
        .catch(err => {
          this.emit('error', err);
          done();
        })
        .then(() => done());
    });
    this.queue.drain = () => this.emit('internal:idle');
    this.queue.error = err => this.emit('error', err);
  }

  push (msg) {
    if (this.acquiescing) {
      return false;
    }
    this.queue.push({message: msg});
    return true;
  }

  acquiesce () {
    return new Promise(resolve => {
      this.acquiescing = true;
      if (this.queue.idle()) {
        return resolve();
      }
      this.once('internal:idle', () => resolve());
    });
  }

  _dispatchMessage (task) {
    return new Promise(resolve => {
      let {message} = task;

      let acked = false;
      const ack = (commitCallback) => {
        debug('acking message at offset %d in %s/%d', message.offset, message.topic, message.partition);

        //
        // Attempts to ack the same message twice should be treated as a bug -- probably in user code -- since it means
        // there's still something out there trying to process this message in spite of the fact we've moved on to
        // processing other messages. All internal attempts to implicitly ack for one reason or another should check
        // `acked` before calling ack().
        //
        if (acked) {
          this.emit('error', new Error('detected multiple attempts to ack the same message'));
          return;
        }

        this.commitManager.ack(message, commitCallback);
        acked = true;

        //
        // resolve on ack, not on commit: that's what the commitCallback (optional parameter to ack()) is for
        //
        // TODO set a ceiling on processed-but-not--yet-committed (see long comment up where _dispatchMessage is called)
        //
        resolve();
      };
      message = Object.assign({}, message, {ack});

      //
      // Make multiple attempts to retry in the event processing fails.
      //
      // TODO does this belong in kafka-node? Could be lifted into user code, but the retry semantics are often useful.
      //
      const r = retry.operation(this.options.processRetryOptions);

      const maybeRetry = err => {
        if (r.retry(err)) {
          return;
        }

        //
        // Be extra paranoid about errors here in the unlikely scenario ack() is the thing throwing.
        //
        // TODO instead of ack() (which changes offsets & triggers commit), maybe just skip without bumping offsets?
        //
        if (!acked) {
          try {
            ack();
          } catch (err) {
            this.emit('error', err);
          }
        }

        this.emit('error', r.mainError());
      };

      r.attempt(() => {
        debug('attempting to process message at offset %d in %s/%d', message.offset, message.topic, message.partition);
        let promise;
        try {
          promise = this.fn(message);
        } catch (err) {
          return maybeRetry(err);
        }

        //
        // If the value returned from fn() looks & smells like a promise & smells & it hasn't already been acked, ack()
        // when the promise resolves; retry processing on catch.
        //
        if (promise && promise.then) {
          promise
            .then(() => {
              if (!acked) {
                ack();
              }
            })
            .catch(err => maybeRetry(err));
        }
      });
    });
  }
}

module.exports = PartitionProcessor;
