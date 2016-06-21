#!/usr/bin/env node

//var Client = require('sonicd').Client;
var Client = require('../../lib/nodejs/lib.js').Client;
var assert = require('assert');

var client = new Client('wss://0.0.0.0:443');


var query = {
  query: '5',
  config: {
    "class" : "SyntheticSource",
    "seed" : 1000,
    "progress-delay" : 10
  }
};

/* UNAUTHENTICATED Client.prototype.run */

client.run(query, function(err, res) {
  if (err) {
    console.log(err);
    return;
  }

  res.forEach(function(e) {
    console.log(e);
  });

  console.log('exec is done!');

});

/* UNAUTHENTICATED Client.prototype.stream */

var stream = client.stream(query);

var done = 0;

stream.on('data', function(data) {
  console.log(data);
});

stream.on('progress', function(p) {
  done += p;
  console.log('running.. ' + done + '% done');
});

stream.on('output', function(out) {
  console.log(out);
});

stream.on('metadata', function(meta) {
  console.log('metadata: ' + JSON.stringify(meta));
});

stream.on('done', function() {
  console.log('stream is done!');
});

stream.on('error', function(err) {
  console.log('stream error: ' + err);
});



var query2 = {
  query: '5',
  config: 'secured_test',
};

/* AUTHENTICATED Client.prototype.run */

//`secured_test` source can be accessed without
//an auth token that grants
//authorization equal or higher than 3.
client.run(query2, function(err, res) {
  assert.throws(function () {
    if (err) {
      throw err;
    }
  });
})


var API_KEY = '1234';
var USER = 'serrallonga';

//first we need to authenticate
client.authenticate(USER, API_KEY, function(err, token) {
  if (err) {
    throw err;
  }

  query2.authToken = token;

  client.run(query2, function(err, res) {
    if (err) {
      console.log(err);
      return;
    }

    res.forEach(function(e) {
      console.log(e);
    });

    console.log('secured exec is done!');

  });
})
