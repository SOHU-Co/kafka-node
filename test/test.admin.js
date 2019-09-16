'use strict';

const Admin = require('../lib/admin');
const ConsumerGroup = require('../lib/consumerGroup');
const KafkaClient = require('../lib/kafkaClient');
const uuid = require('uuid');
const should = require('should');

describe('Admin', function () {
  describe('#listGroups', function () {
    const createTopic = require('../docker/createTopic');
    let admin, consumer;
    const topic = uuid.v4();
    const groupId = 'test-group-id';

    before(function (done) {
      createTopic(topic, 1, 1).then(function () {
        consumer = new ConsumerGroup(
          {
            kafkaHost: 'localhost:9092',
            groupId: groupId
          },
          topic
        );
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
      createTopic(topic, 1, 1).then(function () {
        consumer = new ConsumerGroup(
          {
            kafkaHost: 'localhost:9092',
            groupId: groupId
          },
          topic
        );
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

  describe('#describeConfigs', function () {
    const createTopic = require('../docker/createTopic');
    let admin, client;
    const topicName = uuid.v4();

    before(function (done) {
      if (['0.9', '0.10'].includes(process.env.KAFKA_VERSION)) {
        this.skip();
      }

      createTopic(topicName, 1, 1).then(function () {
        client = new KafkaClient({ kafkaHost: 'localhost:9092' });
        admin = new Admin(client);
        admin.once('ready', done);
      });
    });

    after(function (done) {
      if (client) {
        client.close(done);
      } else {
        done();
      }
    });

    it('should describe a list of topic configs', function (done) {
      const request = {
        resourceType: admin.RESOURCE_TYPES.topic,
        resourceName: topicName,
        configNames: []
      };
      const payload = {
        includeSynonyms: false,
        resources: [request]
      };
      admin.describeConfigs(payload, function (error, res) {
        res.should.be.instanceof(Array);
        res.length.should.be.exactly(1);
        const entries = res[0];
        entries.should.have.property('resourceType').and.exactly('2');
        entries.should.have.property('resourceName').and.exactly(topicName);
        entries.should.have.property('configEntries');
        entries.configEntries.length.should.be.greaterThan(1);
        done(error);
      });
    });

    it('should describe a list of broker configs for a specific broker id', function (done) {
      const brokerName = '1001';
      const request = {
        resourceType: admin.RESOURCE_TYPES.broker,
        resourceName: brokerName,
        configNames: []
      };
      const payload = {
        includeSynonyms: false,
        resources: [request]
      };
      admin.describeConfigs(payload, function (error, res) {
        res.should.be.instanceof(Array);
        res.length.should.be.exactly(1);
        const entries = res[0];
        entries.should.have.property('resourceType').and.exactly('4');
        entries.should.have.property('resourceName').and.exactly(brokerName);
        entries.should.have.property('configEntries');
        entries.configEntries.length.should.be.greaterThan(1);
        done(error);
      });
    });

    it('should return an error if the resource (topic) doesnt exist', function (done) {
      const request = {
        resourceType: admin.RESOURCE_TYPES.topic,
        resourceName: '',
        configNames: []
      };
      const payload = {
        includeSynonyms: false,
        resources: [request]
      };
      admin.describeConfigs(payload, function (error, res) {
        error.should.have.property('message').and.containEql('InvalidTopic');
        done();
      });
    });

    it('should return an error if the resource (broker) doesnt exist', function (done) {
      const brokerId = '9999';
      const request = {
        resourceType: admin.RESOURCE_TYPES.broker,
        resourceName: brokerId,
        configNames: []
      };
      const payload = {
        includeSynonyms: false,
        resources: [request]
      };
      admin.describeConfigs(payload, function (error, res) {
        should.not.exist(res);
        error.should.have.property('message').and.containEql('No broker with id ' + brokerId);
        done();
      });
    });

    it('should return error for invalid resource type', function (done) {
      const request = {
        resourceType: 25,
        resourceName: topicName,
        configNames: []
      };
      const payload = {
        includeSynonyms: false,
        resources: [request]
      };
      admin.describeConfigs(payload, function (error, res) {
        should.not.exist(res);
        error.should.have.property('message').and.equal(`Unexpected resource type 25 for resource ${topicName}`);
        done();
      });
    });
  });
});
