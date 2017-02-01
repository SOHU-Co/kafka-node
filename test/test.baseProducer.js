'use strict';

const BaseProducer = require('../lib/baseProducer');
const Client = require('./mocks/mockClient');
const sinon = require('sinon');

describe('BaseProducer', function () {
  describe('On Brokers Changed', function () {
    it('should emit error when refreshMetadata fails', function (done) {
      const fakeClient = new Client();
      fakeClient.topicMetadata = {};

      const producer = new BaseProducer(fakeClient, {}, BaseProducer.PARTITIONER_TYPES.default);

      producer.once('error', function (error) {
        error.should.be.an.instanceOf(Error);
        error.message.should.be.exactly('boo');
        done();
      });

      const myError = new Error('boo');
      const refreshMetadataStub = sinon.stub(fakeClient, 'refreshMetadata').yields(myError);

      fakeClient.emit('brokersChanged');

      sinon.assert.calledWith(refreshMetadataStub, []);
    });

    it('should call client.refreshMetadata when brokerChanges', function (done) {
      const fakeClient = new Client();

      fakeClient.topicMetadata = {
        MyTopic: [0],
        YourTopic: [0, 1, 2]
      };

      const producer = new BaseProducer(fakeClient, {}, BaseProducer.PARTITIONER_TYPES.default);

      producer.once('error', done);

      const refreshMetadataStub = sinon.stub(fakeClient, 'refreshMetadata').yields(null);

      fakeClient.emit('brokersChanged');

      fakeClient.topicMetadata.should.have.property('MyTopic');
      fakeClient.topicMetadata.should.have.property('YourTopic');
      sinon.assert.calledWith(refreshMetadataStub, ['MyTopic', 'YourTopic']);
      done();
    });
  });
});
