/* eslint-env node, mocha */

var Client = require('../lib/nodejs/lib.js').Client;
var assert = require('chai').assert;
var process = require('process');
var sonicdHost = process.env.SONICD_HOST || 'wss://0.0.0.0:443';
var util = require('./util');

function runSpecTests(client, id) {
  it(id + ' - should be able to run a simple query and stream the data back from the server', function(done) {
    var query = {
      query: '5',
      config: {
        class: 'SyntheticSource',
        seed: 1000,
        'progress-delay': 10
      }
    };

    util.testHappyPath(client, query, 5, done);
  });

  it(id + ' - should return an error if source class is unknown', function(done) {
    var query = {
      query: '1',
      config: {
        class: 'UnknownClass'
      }
    };

    util.expectError(client, query, done);
  });

  it(id + ' - should return an error if query or config is null', function(done) {
    var query = {
      query: null,
      config: {
        class: 'SyntheticSource'
      }
    };

    util.expectError(client, query, done);
  });

  it(id + ' - should return an error if config is null', function(done) {
    var query = {
      query: '1',
      config: null
    };

    util.expectError(client, query, done);
  });

  it(id + ' - should return an error if source publisher completes stream with exception', function(done) {
    var query = {
      // signals source to throw expected exception
      query: '28',
      config: {
        class: 'SyntheticSource'
      }
    };

    util.expectError(client, query, done);
  });

  it(id + ' - should return an error if source throws an exception and terminates', function(done) {
    var query = {
      // signals source to throw unexpected exception
      query: '-1',
      config: {
        class: 'SyntheticSource'
      }
    };

    util.expectError(client, query, done);
  });
}

describe('Sonicd ws', function() {

  var client = new Client(sonicdHost);

  runSpecTests(client, 'unauthenticated');

  it('should return an error if source requires authentication and user is unauthenticated', function(done) {
    var query = {
      query: '1',
      // tipically set server side, but also
      // valid to be passed client side
      config: {
        class: 'SyntheticSource',
        security: 2
      }
    };

    util.expectError(client, query, done);
  });


  describe('Sonicd ws auth', function() {

    it('should throw an error if api key is invalid', function(done) {
      client.authenticate('spec_tests', 'mariano', function(err) {
        if(err) {
          done();
        } else {
          done(new Error('did not return error on invalid api key'));
        }
      });
    });

    it('should authenticate user', function(done) {
      util.doAuthenticate(client, done);
    });
  });


  describe('Sonicd ws with authentication', function() {

    var authenticated = new Client(sonicdHost);

    beforeEach(function(done) {
      util.doAuthenticate(authenticated, done);
    });

    // client is authenticated
    runSpecTests(authenticated, 'authenticated');

    it('should allow an authenticated and authorized user to run a query on a secure source', function(done) {
      var query = {
        query: '5',
        config: {
          class: 'SyntheticSource',
          security: 1
        }
      };

      util.testHappyPath(authenticated, query, 5, done);
    });

    it('should return error if an authenticated user but unauthorized user tries to run a query on a secured source', function(done) {
      var query = {
        query: '5',
        config: {
          class: 'SyntheticSource',
          security: 2000
        }
      };

      util.expectError(authenticated, query, done);
    });

    it('should return error if an authenticated and authorized user from not a whitelisted IP tries to run a query on a secured source', function(done) {
      var query = {
        query: '5',
        config: {
          class: 'SyntheticSource',
          security: 1,
        }
      };

      util.doAuthenticate(authenticated, done, 'only_from_ip'); // check server's reference.conf

      util.expectError(authenticated, query, done);
      authenticated.close();
    });
  });
});
