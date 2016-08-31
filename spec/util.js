var assert = require('chai').assert;

module.exports.testHappyPathSingle = function (client, query, n, done) {
  client.run(query, function(err, data, traceId) {
    assert(typeof traceId !== 'undefined');
    if (err) {
      done(err);
      return;
    }
    if (data.length !== n) {
      done(new Error('data was not ' + n));
      return;
    }
    done();
  });
}

module.exports.testHappyPath = function (client, query, n, done) {
  var stream;
  var _done = 0;
  var traceId;

  client.run(query, function(err, data, traceId) {
    assert(typeof traceId !== 'undefined');
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

  stream.on('trace', function(id) {
    traceId = id;
  });

  stream.on('error', function(err) {
    if (err) {
      done(err);
    } else {
      done(new Error('error emitted but no error returned!'));
    }
  });

  stream.on('done', function(err) {
    assert(typeof traceId !== 'undefined');
    if (err) {
      done(err);
    } else if (_done === 1) {
      done();
    } else {
      _done += 1;
    }
  });
}

module.exports.expectError = function(client, query, done) {
  var _done = 0;
  var stream;
  var traceId;

  client.run(query, function(err, traceId) {
    assert(typeof traceId !== 'undefined');
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

  stream.on('trace', function(id) {
    traceId = id;
  });

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
    assert(typeof traceId !== 'undefined');
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

module.exports.doAuthenticate = function (client, done, apiKeyMaybe) {
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

