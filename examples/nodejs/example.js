#!/usr/bin/env node

//var Client = require('sonicd').Client;
var Client = require('../../lib/nodejs/lib.js').Client;

var query = {
  query: '20000',
  config: {
    "class" : "SyntheticSource",
    "seed" : 1000,
    "progress-delay" : 10
  }
};

var client = new Client('ws://localhost:9111');

//client.exec(query, function(err, res) {
//  if (err) {
//    console.log(err);
//    return;
//  }
//
//  res.forEach(function(e) {
//    console.log(e);
//  });
//
//  console.log('exec is done!');
//
//});

var done = 0;
var stream = client.stream(query);

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
