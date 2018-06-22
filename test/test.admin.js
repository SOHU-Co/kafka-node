'use strict';

const Admin = require('../lib/admin');
const ConsumerGroup = require('../lib/consumerGroup');
const uuid = require('uuid');

describe('Admin', function () {
  describe('#listGroups', function () {
    const createTopic = require('../docker/createTopic');
    let admin, consumer;
    const topic = uuid.v4();
    const groupId = 'test-group-id';

    before(function (done) {
      if (process.env.KAFKA_VERSION === '0.8') {
        this.skip();
      }

      createTopic(topic, 1, 1).then(function () {
        consumer = new ConsumerGroup({
          kafkaHost: 'localhost:9092',
          groupId: groupId
        }, topic);
        consumer.once('connect', function () {
          done();
        });
      });
    });

    after(function (done) {
      consumer.close(done);
    });

    it('should return a list of consumer groups', function (done) {
      admin = new Admin(consumer.client);
      admin.listGroups(function (error, res) {
        res.should.have.keys(groupId);
        res[groupId].should.eql('consumer');
        done(error);
      });
    });
  });

  describe('#describeGroups', function () {
    const createTopic = require('../docker/createTopic');
    let admin, consumer;
    const topic = uuid.v4();
    const groupId = 'test-group-id';

    before(function (done) {
      if (process.env.KAFKA_VERSION === '0.8') {
        this.skip();
      }

      createTopic(topic, 1, 1).then(function () {
        consumer = new ConsumerGroup({
          kafkaHost: 'localhost:9092',
          groupId: groupId
        }, topic);
        consumer.once('connect', function () {
          done();
        });
      });
    });

    after(function (done) {
      consumer.close(done);
    });

    it('should describe a list of consumer groups', function (done) {
      admin = new Admin(consumer.client);
      admin.describeGroups([groupId], function (error, res) {
        res.should.have.keys(groupId);
        res[groupId].should.have.property('members').with.lengthOf(1);
        res[groupId].should.have.property('state', 'Stable');
        done(error);
      });
    });

    it('should return empty members if consumer group doesnt exist', function (done) {
      admin = new Admin(consumer.client);
      const nonExistentGroup = 'non-existent-group';
      admin.describeGroups([nonExistentGroup], function (error, res) {
        res.should.have.keys(nonExistentGroup);
        res[nonExistentGroup].should.have.property('members').with.lengthOf(0);
        res[nonExistentGroup].should.have.property('state', 'Dead');
        done(error);
      });
    });
  });
});
