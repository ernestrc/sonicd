'use strict';

var WebSocket = require('ws');
var EventEmitter = require('events');
var util = require('util');

function Client(sonicdAddress, token) {
  this.address = sonicdAddress;
  this.token = token;
}

function SonicdEmitter() {
  EventEmitter.call(this);
}

util.inherits(SonicdEmitter, EventEmitter);

function toMsg(query, token, traceId) {
  return {
    e: 'Q',
    v: query.query,
    p: {
      auth: token,
      trace_id: traceId,
      config: query.config
    }
  };
}

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

    if (msg.e === 'P') {
      progress(msg.p);
    } else if (msg.e === 'D') {
      isDone = true;
      // send ack
      ws.send(JSON.stringify({ e: 'A' }));
      if (msg.v === 'success') {
        done();
      } else {
        done(new Error(JSON.stringify(msg.p)));
      }
    } else if (msg.e === 'T') {
      metadata(msg.p.map(function(elem) {
        return [elem[0], typeof elem[1]];
      }));
    } else if (msg.e === 'O') {
      output(msg.p);
    }
  });

  ws.on('open', function() {
    ws.send(JSON.stringify(query));
  });
}

Client.prototype.stream = function(query, traceId) {
  var emitter = new SonicdEmitter();
  var queryMsg = toMsg(query, this.token, traceId);

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

  run(this.address, queryMsg, done, output, progress, metadata);

  return emitter;
};

Client.prototype.exec = function(query, doneCb, traceId) {

  var buffer = [];
  var queryMsg = toMsg(query, this.token, traceId);

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

  run(this.address, queryMsg, done, output);
};

Client.prototype.getToken = function(apiKey, user, doneCb) {
  var token;
  var authMsg = {
    e: 'H',
    p: user,
    v: apiKey
  };

  function done(err) {
    if (err) {
      doneCb(err);
    } else {
      doneCb(null, token);
    }
  }

  function output(elems) {
    token = elems[0];
  }

  run(this.address, authMsg, done, output);
};

module.exports.Client = Client;
