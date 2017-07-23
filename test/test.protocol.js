'use strict';

const versionSupport = require('../lib/protocol/versionSupportMapping');
const protocolStruct = require('../lib/protocol/protocol_struct');
const _ = require('lodash');

describe('Protocol', function () {
  it('exports two properties', function () {
    versionSupport.should.have.property('apiMap');
    versionSupport.should.have.property('maxSupported');
  });

  describe('verify API map', function () {
    it('should contain the same keys as request type', function () {
      Object.keys(protocolStruct.REQUEST_TYPE).should.be.eql(Object.keys(versionSupport.apiMap));
    });

    it('should contain different versions of encode/decode functions', function () {
      _.forOwn(versionSupport.apiMap, function (value) {
        if (value === null) {
          return;
        }
        value.should.be.an.instanceOf(Array);
        value.length.should.be.above(0);
        for (let requestResponse of value) {
          requestResponse.should.have.a.lengthOf(2);
          let encoder = requestResponse[0];
          encoder.name.should.startWith('encode');
          encoder.name.should.not.startWith('decode');

          encoder.name.should.endWith('Request');
          encoder.name.should.not.endWith('Response');
        }
      });
    });
  });
});
