'use strict';

var WebSocket = require('ws');
var EventEmitter = require('events');
var util = require('util');

function Client(sonicdAddress) {
  this.address = sonicdAddress;
}

function SonicdEmitter() {
  EventEmitter.call(this);
}

util.inherits(SonicdEmitter, EventEmitter);

function run(address, query, done, outputCb, progressCb, metadataCb) {
  var output = outputCb || (function() {});
  var progress = progressCb || (function() {});
  var metadata = metadataCb || (function() {});
  var isDone = false;
  var uri = address + '/v1/query';
  var ws = new WebSocket(uri);

  ws.on('close', function() {
    if (!isDone) {
      done(new Error('connection closed unexpectedly'));
    }
  });

  ws.on('message', function(message) {
    var msg = JSON.parse(message.toString('utf-8'));

    if (msg.event_type === 'P') {
      progress(msg.payload);
    } else if (msg.event_type === 'D') {
      isDone = true;
      if (msg.variation === 'success') {
        done();
      } else {
        done(new Error(JSON.stringify(msg.payload)));
      }
    } else if (msg.event_type === 'T') {
      metadata(msg.payload.map(function(elem) {
        return [elem[0], typeof elem[1]];
      }));
    } else if (msg.event_type === 'O') {
      output(msg.payload);
    }
  });

  ws.on('open', function() {
    ws.send(JSON.stringify(query));
  });
}

Client.prototype.stream = function(query) {
  var emitter = new SonicdEmitter();

  function done(err) {
    if (err) {
      emitter.emit('error', err);
      return;
    }

    emitter.emit('done');
  }

  function output(elems) {
    emitter.emit('data', elems);
  }

  function metadata(meta) {
    emitter.emit('metadata', meta);
  }

  function progress(prog) {
    if (typeof prog.progress !== 'undefined') {
      emitter.emit('progress', prog.progress);
    }

    if (typeof prog.output !== 'undefined') {
      emitter.emit('output', prog.progress);
    }
  }

  run(this.address, query, done, output, progress, metadata);

  return emitter;
};

Client.prototype.exec = function(query, doneCb) {

  var buffer = [];

  function done(err) {
    if (err) {
      doneCb(err);
    } else {
      doneCb(null, buffer);
    }
  }

  function output(elems) {
    buffer.push(elems);
  }

  run(this.address, query, done, output);
};

module.exports.Client = Client;
