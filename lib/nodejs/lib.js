'use strict';

var BrowserWebSocket = global.MozWebSocket || global.WebSocket;
var WebSocket =  BrowserWebSocket || require('ws');
var EventEmitter = require('events');
var util = require('util');
var utils = require('./util');

// this is an ugly hack to prevent browseryfied `ws` module to throw errors at runtime
// because the EventEmitter API used in Node.js is not available with the WebSocket browser API
if (BrowserWebSocket) {
  WebSocket.prototype.on = function(event, callback) {
    this['on' + event] = callback;
  };
}

function Client(sonicdAddress, token) {
  this.address = sonicdAddress;
  this.token = token;
  this.connections = [];
}

function SonicdEmitter() {
  EventEmitter.call(this);
}

util.inherits(SonicdEmitter, EventEmitter);

Client.prototype.exec = function(address, command, done, outputCb, progressCb, metadataCb) {
  var output = outputCb || (function() {});
  var progress = progressCb || (function() {});
  var metadata = metadataCb || (function() {});
  var isDone = false;
  var isError = false;
  var uri = address + '/v1/query';
  var ws = new WebSocket(uri);
  var self = this;

  function closedUnexp() {
    done(new Error('connection closed unexpectedly'));
  }

  ws.on('close', function(ev) {
    var idx = self.connections.indexOf(ws);

    // browser
    if (BrowserWebSocket) {
      if (isError) {
        done(new Error('WebSocket close code: ' + ev.code + '; reason: ' + ev.reason));
      } else if (ev.code !== 1000 && !isDone) {
        closedUnexp();
      }

      // ws
    } else if (!isDone && ev !== 1000) {
      closedUnexp();
    }

    if (idx > 0) {
      self.connections.splice(idx, 1);
    }
  });

  ws.on('error', function(ev) {
    // ev is defined with `ws`, but not with the
    // browser's WebSocket API
    if (BrowserWebSocket) {
      isError = true;
    } else {
      isDone = true;
      done(ev);
    }
  });

  ws.on('message', function(message) {
    var msg = BrowserWebSocket ? JSON.parse(message.data) : JSON.parse(message.toString('utf-8'));

    switch (msg.e) {
      case 'P':
        progress(utils.toProgress(msg.p));
        break;

      case 'D':
        isDone = true;
        // send ack
        ws.send(JSON.stringify({ e: 'A' }));
        if (msg.v) {
          done(new Error(msg.v));
        } else {
          done();
        }
        break;

      case 'T':
        metadata(msg.p.map(function(elem) {
          return [elem[0], typeof elem[1]];
        }));
        break;

      case 'O':
        output(msg.p);
        break;

      default:
        done(new Error('protocol error: unexpected event_type: ' + msg.e));
        break;
    }
  });

  ws.on('error', function(err) {
    done(err);
  });

  ws.on('open', function() {
    ws.send(JSON.stringify(command));
  });

  return ws;
};

Client.prototype.stream = function(query) {
  var emitter = new SonicdEmitter();
  var queryMsg = utils.toMsg(query, this.token);

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

  this.connections.push(this.exec(this.address, queryMsg, done, output, progress, metadata));

  return emitter;
};

Client.prototype.run = function(query, doneCb) {

  var buffer = [];
  var queryMsg = utils.toMsg(query, this.token);

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

  this.connections.push(this.exec(this.address, queryMsg, done, output));
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

  this.connections.push(this.exec(this.address, authMsg, done, output));
};

Client.prototype.close = function() {

  if (!this.client) {
    return;
  }

  this.connections.forEach(function(conn) {
    conn.close(1000, 'user closed');
  });

};

module.exports.Client = Client;
