'use strict';

var Sonicd = require('../nodejs/lib.js');
var client = new Client('ws://0.0.0.0:9111');

describe('Sonicd ws', function() {
  beforeEach(function(done) {
    done();
  });

  beforeEach(function(done) {
    done();
  });

  afterEach(function(done) {
    done();
  });

  beforeEach(function(done) {
    done();
  });

  describe('run api', function() {
    it('should be able to run a simple query and stream the data back from the server', function(done) {
      var query = {
        query: '5',
        config: {
          "class" : "SyntheticSource",
          "seed" : 1000,
          "progress-delay" : 10
        }
      };
      done();
    });
  });
});
