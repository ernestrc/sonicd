'use strict';

var WebSocket = require('ws');
var EventEmitter = require('events');
var util = require('util');
var utils = require('./util');

function Client(sonicdAddress, token) {
  this.address = sonicdAddress;
  this.token = token;
}

function SonicdEmitter() {
  EventEmitter.call(this);
}

util.inherits(SonicdEmitter, EventEmitter);

function exec(address, command, done, outputCb, progressCb, metadataCb) {
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
      progress(utils.toProgress(msg.p));
    } else if (msg.e === 'D') {
      isDone = true;
      // send ack
      ws.send(JSON.stringify({ e: 'A' }));
      if (msg.v) {
        done(new Error(msg.v));
      } else {
        done();
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
    ws.send(JSON.stringify(command));
  });

  return ws;
}

Client.prototype.stream = function(query) {
  var emitter = new SonicdEmitter();
  var queryMsg = utils.toMsg(query, query.traceId, this.token);

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
    emitter.emit('progress', prog);
  }

  this.client = exec(this.address, queryMsg, done, output, progress, metadata);

  return emitter;
};

Client.prototype.run = function(query, doneCb) {

  var buffer = [];
  var queryMsg = utils.toMsg(query, query.traceId, this.token);

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

  this.client = exec(this.address, queryMsg, done, output);
};

Client.prototype.authenticate = function(user, apiKey, doneCb, traceId) {
  var self = this;
  var authMsg = {
    e: 'H',
    p: {
      user: user,
      trace_id: traceId
    },
    v: apiKey
  };

  function done(err) {
    if (err) {
      doneCb(err);
    } else {
      doneCb(null, self.token);
    }
  }

  function output(elems) {
    self.token = elems[0];
  }

  this.client = exec(this.address, authMsg, done, output);
};

Client.prototype.close = function() {

  if (!this.client) {
    return;
  }

  this.client.close(1000, 'user closed');

};

module.exports.Client = Client;
