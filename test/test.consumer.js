'use strict';

var libPath = process.env['KAFKA_COV'] ? '../lib-cov/' : '../lib/';
var Consumer = require(libPath + 'consumer');
var Producer = require(libPath + 'producer');
var Offset = require(libPath + 'offset');
var Client = require(libPath + 'client');
var should = require('should');
var uuid = require('node-uuid');
var TopicsNotExistError = require(libPath + 'errors').TopicsNotExistError;
var FakeClient = require('./mocks/mockClient');
var InvalidConfigError = require('../lib/errors/InvalidConfigError');

var client, producer, offset;

var TOPIC_POSTFIX = '_test_' + Date.now();
var EXISTS_TOPIC_1 = '_exists_1' + TOPIC_POSTFIX;
var EXISTS_TOPIC_2 = '_exists_2' + TOPIC_POSTFIX;
var EXISTS_GZIP = '_exists_gzip'; // + TOPIC_POSTFIX
var EXISTS_SNAPPY = '_exists_snappy'; // + TOPIC_POSTFIX

// Compression-friendly to be interesting
var SNAPPY_MESSAGE = new Array(20).join('snappy');

var host = process.env['KAFKA_TEST_HOST'] || '';
function noop () { console.log(arguments); }

describe('Consumer', function () {
  describe('#setOffset', function () {
    var client, consumer;

    beforeEach(function () {
      client = new FakeClient();

      consumer = new Consumer(
        client,
        [ {topic: 'fake-topic'} ]
      );

      clearTimeout(consumer.checkPartitionOwnershipInterval);
      consumer.payloads = [
        {topic: 'fake-topic', partition: '0', offset: 0, maxBytes: 1048576, metadata: 'm'},
        {topic: 'fake-topic', partition: '1', offset: 0, maxBytes: 1048576, metadata: 'm'}
      ];
    });

    it('should setOffset correctly given partition is a string', function () {
      consumer.setOffset('fake-topic', '0', 86);
      consumer.payloads[0].offset.should.be.eql(86);

      consumer.setOffset('fake-topic', '1', 23);
      consumer.payloads[1].offset.should.be.eql(23);
    });

    it('should setOffset correctly if given partition is a number', function () {
      consumer.setOffset('fake-topic', 0, 67);
      consumer.payloads[0].offset.should.be.eql(67);

      consumer.setOffset('fake-topic', 1, 98);
      consumer.payloads[1].offset.should.be.eql(98);
    });
  });

  describe('validate topics', function () {
    function validateThrowsInvalidConfigError (topics) {
      should.throws(function () {
        var client = new FakeClient(host);
        // eslint-disable-next-line no-new
        new Consumer(client, topics);
      }, InvalidConfigError);
    }

    function validateDoesNotThrowInvalidConfigError (topics) {
      should.doesNotThrow(function () {
        var client = new FakeClient(host);
        // eslint-disable-next-line no-new
        new Consumer(client, topics);
      }, InvalidConfigError);
    }

    it('should throw an error on invalid topic partitions', function () {
      validateThrowsInvalidConfigError([
        {
          topic: 'another-topic'
        },
        {
          topic: 'my-fake-topic',
          partition: '7'
        }
      ]);

      validateThrowsInvalidConfigError([
        {
          topic: 'another-topic',
          partition: 3
        },

        {
          topic: 'my-fake-topic',
          partition: {}
        }
      ]);

      validateThrowsInvalidConfigError([
        {
          topic: 'my-fake-topic',
          partition: false
        }
      ]);
    });

    it('should not throw an error for valid topic partition', function () {
      validateDoesNotThrowInvalidConfigError([
        {
          topic: 'fake-topic',
          partition: 7
        },
        {
          topic: 'another-topic'
        }
      ]);
      validateDoesNotThrowInvalidConfigError([
        {
          topic: 'another-topic'
        },
        {
          topic: 'fake-topic',
          partition: 0
        }
      ]);
    });
  });

  describe('validate groupId', function () {
    function validateThrowsInvalidConfigError (groupId) {
      should.throws(function () {
        var client = new FakeClient(host);
        // eslint-disable-next-line no-new
        new Consumer(client, [ { topic: EXISTS_TOPIC_2 } ], {groupId: groupId});
      }, InvalidConfigError);
    }

    function validateDoesNotThrowInvalidConfigError (groupId) {
      should.doesNotThrow(function () {
        var client = new FakeClient(host);
        // eslint-disable-next-line no-new
        new Consumer(client, [ { topic: EXISTS_TOPIC_2 } ], {groupId: groupId});
      });
    }

    it('should throws an error on invalid group IDs', function () {
      validateThrowsInvalidConfigError('myGroupId:12345');
      validateThrowsInvalidConfigError('myGroupId,12345');
      validateThrowsInvalidConfigError('myGroupId"12345"');
      validateThrowsInvalidConfigError('myGroupId?12345');
    });

    it('should not throw on valid group IDs', function () {
      validateDoesNotThrowInvalidConfigError('myGroupId.12345');
      validateDoesNotThrowInvalidConfigError('something_12345');
      validateDoesNotThrowInvalidConfigError('myGroupId-12345');
    });
  });

  function offsetOutOfRange (topic, consumer) {
    topic.maxNum = 2;
    offset.fetch([topic], function (err, offsets) {
      if (err) consumer.emit('error', err);
      var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
      consumer.setOffset(topic.topic, topic.partition, min);
    });
  }

  [
    {
      name: 'PLAINTEXT Consumer'
    },
    {
      name: 'SSL Consumer',
      sslOptions: {
        rejectUnauthorized: false
      },
      suiteTimeout: 30000
    }
  ].forEach(function (testParameters) {
    var sslOptions = testParameters.sslOptions || undefined;
    var suiteTimeout = testParameters.suiteTimeout || null;
    var suiteName = testParameters.name;

    function createClient () {
      var clientId = 'kafka-node-client-' + uuid.v4();
      return new Client(host, clientId, undefined, undefined, sslOptions);
    }

    describe(suiteName, function () {
      before(function (done) {
        if (suiteTimeout) { this.timeout(suiteTimeout); }
        client = createClient();
        producer = new Producer(client);
        offset = new Offset(client);
        producer.on('ready', function () {
          producer.createTopics([
            EXISTS_TOPIC_1,
            EXISTS_TOPIC_2,
            EXISTS_GZIP,
            EXISTS_SNAPPY
          ], false, function (err, created) {
            if (err) return done(err);

            function useNewTopics () {
              producer.send([
                { topic: EXISTS_TOPIC_2, messages: 'hello kafka' },
                { topic: EXISTS_GZIP, messages: 'hello gzip', attributes: 1 },
                { topic: EXISTS_SNAPPY, messages: SNAPPY_MESSAGE, attributes: 2 }
              ], done);
            }
            // Ensure leader selection happened
            setTimeout(useNewTopics, 1000);
          });
        });
      });

      after(function (done) {
        client.close(done);
      });

      describe('events', function () {
        it('should emit message when get new message', function (done) {
          if (suiteTimeout) { this.timeout(suiteTimeout); }
          var topics = [ { topic: EXISTS_TOPIC_2 } ];
          var options = { autoCommit: false, groupId: '_groupId_1_test' };
          var consumer = new Consumer(client, topics, options);
          var count = 0;
          consumer.on('error', noop);
          consumer.on('offsetOutOfRange', function (topic) {
            offsetOutOfRange(topic, this);
          });
          consumer.on('message', function (message) {
            message.topic.should.equal(EXISTS_TOPIC_2);
            message.value.should.equal('hello kafka');
            message.partition.should.equal(0);
            offset.commit('_groupId_1_test', [message], function (err) {
              if (count++ === 0) done(err);
            });
          });
        });

        it('should decode gzip messages', function (done) {
          if (suiteTimeout) { this.timeout(suiteTimeout); }
          var topics = [ { topic: EXISTS_GZIP } ];
          var options = { autoCommit: false, groupId: '_groupId_gzip_test' };
          var client = createClient();
          var consumer = new Consumer(client, topics, options);
          var count = 0;
          consumer.on('error', noop);
          consumer.on('offsetOutOfRange', function (topic) {
            offsetOutOfRange(topic, this);
          });
          consumer.on('message', function (message) {
            message.topic.should.equal(EXISTS_GZIP);
            message.value.should.equal('hello gzip');
            offset.commit('_groupId_gzip_test', [message], function (err) {
              if (count++ === 0) {
                consumer.close(function () {
                  done(err);
                });
              }
            });
          });
        });

        it('should decode snappy messages', function (done) {
          if (suiteTimeout) { this.timeout(suiteTimeout); }
          var topics = [ { topic: EXISTS_SNAPPY } ];
          var options = { autoCommit: false, groupId: '_groupId_snappy_test' };
          var client = createClient();
          var consumer = new Consumer(client, topics, options);
          var count = 0;
          consumer.on('error', noop);
          consumer.on('offsetOutOfRange', function (topic) {
            offsetOutOfRange(topic, this);
          });
          consumer.once('message', function (message) {
            message.topic.should.equal(EXISTS_SNAPPY);
            message.value.should.equal(SNAPPY_MESSAGE);
            offset.commit('_groupId_snappy_test', [message], function (err) {
              if (count++ === 0) {
                consumer.close(function () {
                  done(err);
                });
              }
            });
          });
        });

        it('should emit error when topic not exists', function (done) {
          var topics = [ { topic: '_not_exist_topic_1_test' } ];
          var consumer = new Consumer(client, topics);
          consumer.on('error', function (error) {
            error.should.be.an.instanceOf(TopicsNotExistError)
              .and.have.property('message', 'The topic(s) _not_exist_topic_1_test do not exist');
            done();
          });
        });

        it('should emit offsetOutOfRange when offset out of range', function (done) {
          var topics = [ { topic: EXISTS_TOPIC_1, offset: 100 } ];
          var options = { fromOffset: true, autoCommit: false };
          var count = 0;
          var client = createClient();
          var consumer = new Consumer(client, topics, options);
          consumer.on('offsetOutOfRange', function (topic) {
            topic.topic.should.equal(EXISTS_TOPIC_1);
            topic.partition.should.equal(0);
            topic.message.should.equal('OffsetOutOfRange');
            if (count++ === 0) {
              consumer.close(function () {
                done();
              });
            }
          });
          consumer.on('error', noop);
        });
      });

      describe('#addTopics', function () {
        describe('when topic need to added not exist', function () {
          it('should return error', function (done) {
            var options = { autoCommit: false, groupId: '_groupId_1_test' };
            var topics = ['_not_exist_topic_1_test'];
            var consumer = new Consumer(client, [], options);
            consumer.on('error', noop);
            consumer.addTopics(topics, function (err) {
              err.topics.length.should.equal(1);
              err.topics.should.eql(topics);
              done();
            });
          });

          it('should return error when using payload as well', function (done) {
            var options = { autoCommit: false, groupId: '_groupId_1_test' };
            var topics = [{topic: '_not_exist_topic_1_test', offset: 42}];
            var consumer = new Consumer(client, [], options);
            consumer.on('error', noop);
            consumer.addTopics(topics, function (err) {
              err.topics.length.should.equal(1);
              err.topics.should.eql(topics.map(function (p) { return p.topic; }));
              done();
            }, true);
          });
        });

        describe('when topic need to added exist', function () {
          it('should added successfully', function (done) {
            var options = { autoCommit: false, groupId: '_groupId_addTopics_test' };
            var topics = [EXISTS_TOPIC_2];
            var consumer = new Consumer(client, [], options);
            consumer.on('error', noop);
            consumer.addTopics(topics, function (err, data) {
              data.should.eql(topics);
              consumer.payloads.some(function (p) { return p.topic === topics[0]; }).should.be.ok;
              done(err);
            });
          });

          it('should add with given offset', function (done) {
            var options = { autoCommit: false, groupId: '_groupId_addTopics_test' };
            var topics = [{topic: EXISTS_TOPIC_2, offset: 42}];
            var consumer = new Consumer(client, [], options);
            consumer.on('error', noop);
            consumer.addTopics(topics, function (err, data) {
              data.should.eql(topics.map(function (p) { return p.topic; }));
              consumer.payloads.some(function (p) { return p.topic === topics[0].topic && p.offset === topics[0].offset; }).should.be.ok;
              done(err);
            }, true);
          });
        });

        describe('#removeTopics', function () {
          it('should remove topics successfully', function (done) {
            var options = { autoCommit: false, groupId: '_groupId_addTopics_test' };
            var topics = [{ topic: EXISTS_TOPIC_2 }, { topic: EXISTS_TOPIC_1 }];
            var consumer = new Consumer(client, topics, options);
            consumer.on('error', noop);

            consumer.payloads.length.should.equal(2);
            consumer.removeTopics(EXISTS_TOPIC_2, function (err, removed) {
              removed.should.equal(1);
              consumer.payloads.length.should.equal(1);
              done(err);
            });
          });
        });

        describe('#setOffset', function () {
          it('should update the offset in consumer', function () {
            var options = { autoCommit: false, groupId: '_groupId_addTopics_test' };
            var topics = [{ topic: EXISTS_TOPIC_2 }];
            var consumer = new Consumer(client, topics, options);
            consumer.on('error', noop);

            consumer.setOffset(EXISTS_TOPIC_2, 0, 100);
            consumer.payloads.filter(function (p) {
              return p.topic === EXISTS_TOPIC_2;
            })[0].offset.should.equal(100);
          });
        });

        describe('#commit', function () {
          it('should commit offset of current topics', function (done) {
            var topics = [ { topic: EXISTS_TOPIC_2 } ];
            var options = { autoCommit: false, groupId: '_groupId_commit_test' };

            var client = new Client(host);
            var consumer = new Consumer(client, topics, options);
            var count = 0;
            consumer.on('error', noop);
            consumer.on('offsetOutOfRange', function (topic) {
              offsetOutOfRange(topic, this);
            });
            consumer.on('message', function (message) {
              consumer.commit(true, function (err) {
                if (!err && count++ === 0) done(err);
              });
            });
          });
        });

        describe('#buildPayloads', function () {
          it('should set default config on the topic', function () {
            var defaults = {
              groupId: 'kafka-node-group',
              autoCommit: true,
              autoCommitIntervalMs: 5000,
              encoding: 'utf8',
              fetchMaxWaitMs: 100,
              fetchMinBytes: 1,
              fetchMaxBytes: 1024 * 1024,
              fromOffset: false
            };

            var consumer = new Consumer(client, []);
            consumer.options.should.eql(defaults);

            var topic = consumer.buildPayloads([{ topic: 'topic' }])[0];
            topic.partition.should.equal(0);
            topic.offset.should.equal(0);
            topic.metadata.should.equal('m');
            topic.maxBytes.should.equal(1024 * 1024);
          });

          it('should support custom options', function () {
            var options = {
              groupId: 'custom-group',
              autoCommit: false,
              autoCommitIntervalMs: 1000,
              fetchMaxWaitMs: 200,
              fetchMinBytes: 1,
              fetchMaxBytes: 1024,
              fromOffset: false
            };
            var consumer = new Consumer(client, [], options);
            consumer.options.should.equal(options);
            var topic = consumer.buildPayloads([{ topic: 'topic' }])[0];
            topic.partition.should.equal(0);
            topic.offset.should.equal(0);
            topic.metadata.should.equal('m');
            topic.maxBytes.should.equal(1024);
          });

          it('should return right payloads when arguments is string', function () {
            var consumer = new Consumer(client, []);
            var topics = ['topic'];
            var topic = consumer.buildPayloads(topics)[0];
            topic.should.be.an.instanceof(Object);
            topic.partition.should.equal(0);
            topic.offset.should.equal(0);
            topic.metadata.should.equal('m');
            topic.maxBytes.should.equal(1024 * 1024);
          });
        });

        describe('#close', function () {
          it('should close the consumer', function (done) {
            var client = new Client(host);
            var topics = [ { topic: EXISTS_TOPIC_2 } ];
            var options = { autoCommit: false, groupId: '_groupId_close_test' };

            var consumer = new Consumer(client, topics, options);
            consumer.once('message', function (message) {
              consumer.close(function (err) {
                done(err);
              });
            });
          });

          it('should commit the offset if force', function (done) {
            var client = new Client(host);
            var topics = [ { topic: EXISTS_TOPIC_2 } ];
            var force = true;
            var options = { autoCommit: false, groupId: '_groupId_close_test' };

            var consumer = new Consumer(client, topics, options);
            consumer.once('message', function (message) {
              consumer.close(force, function (err) {
                done(err);
              });
            });
          });
        });

        xdescribe('#pauseTopics|resumeTopics', function () {
          it('should pause or resume the topics', function (done) {
            var client = new Client(host);
            var topics = [
              {topic: EXISTS_TOPIC_1, partition: 0},
              {topic: EXISTS_TOPIC_1, partition: 1},
              {topic: EXISTS_TOPIC_2, partition: 0},
              {topic: EXISTS_TOPIC_2, partition: 1}
            ];
            var consumer = new Consumer(client, topics, {});
            consumer.on('error', function () {});

            function normalize (p) {
              return { topic: p.topic, partition: p.partition };
            }
            function compare (p1, p2) {
              return p1.topic === p2.topic
                ? p1.partition - p2.partition
                : p1.topic > p2.topic;
            }

            consumer.payloads.should.eql(topics);
            consumer.pauseTopics([EXISTS_TOPIC_1, { topic: EXISTS_TOPIC_2, partition: 0 }]);
            consumer.payloads.map(normalize).should.eql([{ topic: EXISTS_TOPIC_2, partition: 1 }]);

            consumer.resumeTopics([{topic: EXISTS_TOPIC_1, partition: 0}]);
            consumer.payloads.map(normalize).sort(compare).should.eql([
              {topic: EXISTS_TOPIC_1, partition: 0},
              {topic: EXISTS_TOPIC_2, partition: 1}
            ]);
            consumer.pausedPayloads.map(normalize).sort(compare).should.eql([
              {topic: EXISTS_TOPIC_1, partition: 1},
              {topic: EXISTS_TOPIC_2, partition: 0}
            ]);

            consumer.resumeTopics([EXISTS_TOPIC_1, EXISTS_TOPIC_2]);
            consumer.payloads.sort(compare).should.eql(topics);
            consumer.pausedPayloads.should.eql([]);
            consumer.once('message', function () {
              consumer.close(true, done);
            });
          });
        });

        describe('Consumer', function () {
          before(function (done) {
            if (suiteTimeout) { this.timeout(suiteTimeout); }
            client = createClient();
            producer = new Producer(client);
            offset = new Offset(client);
            producer.on('ready', function () {
              producer.createTopics([
                EXISTS_TOPIC_1,
                EXISTS_TOPIC_2,
                EXISTS_GZIP,
                EXISTS_SNAPPY
              ], false, function (err, created) {
                if (err) return done(err);

                function useNewTopics () {
                  producer.send([
                    { topic: EXISTS_TOPIC_2, messages: 'hello kafka' },
                    { topic: EXISTS_GZIP, messages: 'hello gzip', attributes: 1 },
                    { topic: EXISTS_SNAPPY, messages: SNAPPY_MESSAGE, attributes: 2 }
                  ], done);
                }
                // Ensure leader selection happened
                setTimeout(useNewTopics, 1000);
              });
            });
          });
        });

        after(function (done) {
          client.close(done);
        });
      });
    });
  });
});
