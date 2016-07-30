/* eslint-env node, mocha */

var Client = require('../lib/nodejs/lib.js').Client;
var assert = require('chai').assert;
var process = require('process');
var sonicdHost = process.env.SONICD_HOST || 'wss://0.0.0.0:443';
var util = require('./util');


describe('ws sonicd', function() {

  var client = new Client(sonicdHost);

  it('should be able to handle multiple concurrent queries', function(done) {

    var connections = 10000;
    var checkWait = 1000; //1 second
    var messages = 1;
    var errs = [];
    var finished = 0;

    var query = {
      query: messages.toString(),
      config: {
        'progress-delay': 0,
        size: messages,
        indexed: true,
        class: 'SyntheticSource'
      }
    };

    function count(err) {
      if (err) {
        errs.push(err);
      }
      finished += 1;
    }

    function checkCondition() {
      console.log('checking if done..' + finished + '/' + connections);
      if (connections > finished) {
        setTimeout(checkCondition, checkWait);
      } else {
        console.log('done with load test: connections ' + connections + '; errors: ', errs.length);
        if (errs.length > 0) {
          done(new Error(JSON.stringify(errs)));
        } else {
          done();
        }
      }
    }

    for (var i = 0, len = connections; i < len; i++) {
      try {
        util.testHappyPathSingle(client, query, messages, count);
      } catch (e) {
        errs.push(e);
      }
    }

    checkCondition();
  }).timeout(100000);
});
