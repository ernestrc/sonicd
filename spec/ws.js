/* eslint-env node, mocha */

var Client = require('../lib/nodejs/lib.js').Client;
var assert = require('chai').assert;
var process = require('process');
var sonicdHost = process.env.SONICD_HOST || 'wss://0.0.0.0:443';

function testHappyPath(client, query, n, done) {
  var stream;
  var _done = 0;

  client.run(query, function(err, data) {
    if (err) {
      done(err);
      return;
    }
    assert(data.length === n, 'data was not' + n);
    if (_done === 1) {
      done();
    } else {
      _done += 1;
    }
  });

  stream = client.stream(query);

  stream.on('error', function(err) {
    if (err) {
      done(err);
    } else {
      done(new Error('error emitted but no error returned!'));
    }
  });

  stream.on('done', function(err) {
    if (err) {
      done(err);
    } else if (_done === 1) {
      done();
    } else {
      _done += 1;
    }
  });
}

function expectError(client, query, done) {
  var _done = 0;
  var stream;

  client.run(query, function(err) {
    if (err) {
      if (done) {
        if (_done === 1) {
          done();
        } else {
          _done += 1;
        }
      }
    } else {
      done(new Error('expected error but no error returned'));
    }
  });

  stream = client.stream(query);

  stream.on('error', function(err) {
    if (err) {
      if (done) {
        done();
      }
    } else {
      done(new Error('expected error but no error returned'));
    }
  });

  stream.on('done', function(err) {
    if (err) {
      if (done) {
        if (_done === 1) {
          done();
        } else {
          _done += 1;
        }
      }
    } else {
      done(new Error('expected error but no error returned'));
    }
  });
}

function doAuthenticate(client, done, apiKeyMaybe) {
  var apiKey = apiKeyMaybe || '1234';
  client.authenticate('spec_tests', apiKey, function(err) {
    if (err) {
      done(new Error('failed to authenticate'));
      return;
    }

    if (client.token) {
      done();
    } else {
      done(new Error('protocol error: no token received from server'));
    }
  });
}

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

    testHappyPath(client, query, 5, done);
  });

  it(id + ' - should return an error if source class is unknown', function(done) {
    var query = {
      query: '1',
      config: {
        class: 'UnknownClass'
      }
    };

    expectError(client, query, done);
  });

  it(id + ' - should return an error if query or config is null', function(done) {
    var query = {
      query: null,
      config: {
        class: 'SyntheticSource'
      }
    };

    expectError(client, query, done);
  });

  it(id + ' - should return an error if config is null', function(done) {
    var query = {
      query: '1',
      config: null
    };

    expectError(client, query, done);
  });

  it(id + ' - should return an error if source publisher completes stream with exception', function(done) {
    var query = {
      // signals source to throw expected exception
      query: '28',
      config: {
        class: 'SyntheticSource'
      }
    };

    expectError(client, query, done);
  });

  it(id + ' - should return an error if source throws an exception and terminates', function(done) {
    var query = {
      // signals source to throw unexpected exception
      query: '-1',
      config: {
        class: 'SyntheticSource'
      }
    };

    expectError(client, query, done);
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

    expectError(client, query, done);
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
      doAuthenticate(client, done);
    });
  });


  describe('Sonicd ws with authentication', function() {

    var authenticated = new Client(sonicdHost);

    beforeEach(function(done) {
      doAuthenticate(authenticated, done);
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

      testHappyPath(authenticated, query, 5, done);
    });

    it('should return error if an authenticated user but unauthorized user tries to run a query on a secured source', function(done) {
      var query = {
        query: '5',
        config: {
          class: 'SyntheticSource',
          security: 2000
        }
      };

      expectError(client, query, done);
    });

    it('should return error if an authenticated and authorized user from not a whitelisted IP tries to run a query on a secured source', function(done) {
      var query = {
        query: '5',
        config: {
          class: 'SyntheticSource',
          security: 1,
        }
      };

      doAuthenticate(authenticated, done, 'only_from_ip'); // check server's reference.conf

      expectError(client, query, done);
    });
  });
});
