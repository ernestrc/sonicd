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

function Client(sonicAddress, config) {
  this.url = sonicAddress + '/v1/query';
  this.pipeline = config && config.pipeline || false;
  this.ws = null;
  this.busy = false;
  this.open = false;
  this.commands = [];
  this.queue = [];
}

function SonicEmitter() {
  EventEmitter.call(this);
}

util.inherits(SonicEmitter, EventEmitter);

Client.prototype.send = function(doneCb, outputCb, progressCb, metadataCb, startedCb) {
  var output = outputCb || (function() {});
  var progress = progressCb || (function() {});
  var metadata = metadataCb || (function() {});
  var isDone = false;
  var isError = false;
  var self = this;
  var traceId;

  return function (ws) {
    self.busy = true;

    function done(err, id) {
      var task, command, doExec;

      if (!self.pipeline) {
        ws.close();
      } else if (self.queue.length > 0) {
        task = self.queue.splice(0, 1)[0];
        command = task[0];
        doExec = task[1];

        // register new listeners
        self.ws.removeAllListeners();
        doExec(self.ws);
      } else {
        self.busy = false;
      }

      doneCb(err, id);
    }

    function closedUnexp() {
      done(new Error('connection closed unexpectedly'), traceId);
    }

    ws.on('close', function(ev) {
      // browser
      if (BrowserWebSocket) {
        if (isError) {
          done(new Error('WebSocket close code: ' + ev.code + '; reason: ' + ev.reason), traceId);
        } else if (ev.code !== 1000 && !isDone) {
          closedUnexp();
        }

        // ws
      } else if (!isDone && ev !== 1000) {
        closedUnexp();
      }
    });

    ws.on('error', function(ev) {
      // ev is defined with `ws`, but not with the
      // browser's WebSocket API
      if (BrowserWebSocket) {
        isError = true;
      } else {
        isDone = true;
        done(ev, traceId);
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
          if (msg.v) {
            done(new Error('Query with trace_id `' + msg.p.trace_id + '` failed: ' + msg.v), traceId);
          } else {
            done(null, traceId);
          }
          // send ack
          ws.send(JSON.stringify({ e: 'A' }));
          break;

        case 'T':
          metadata(msg.p.map(function(elem) {
            return [elem[0], typeof elem[1]];
          }));
          break;

        case 'S':
          traceId = msg.v;
          if (typeof startedCb !== 'undefined') {
            startedCb(traceId);
          }
          break;

        case 'O':
          output(msg.p);
          break;

        default:
          // ignore to improve forwards compatibility
          break;
      }
    });
  };
};

Client.prototype.exec = function(command, doneCb, outputCb, progressCb, metadataCb, startedCb) {
  var self = this;
  var doExec = self.send(doneCb, outputCb, progressCb, metadataCb, startedCb);

  if (self.pipeline && self.ws) {
    if (self.busy) {
      self.queue.push([command, doExec]);
    } else {
      self.ws.removeAllListeners();
      doExec(self.ws);
    }
    if (self.open) {
      self.ws.send(JSON.stringify(command));
    } else {
      self.commands.push(command);
    }
  } else {
    var ws = new WebSocket(self.url);
    self.ws = ws;
    doExec(ws);
    self.commands.push(command);
    ws.on('open', function() {
      self.open = true;
      self.commands.forEach(function(cmd) {
        ws.send(JSON.stringify(cmd));
      });
    });
  }
};

Client.prototype.stream = function(query) {
  var emitter = new SonicEmitter();
  var queryMsg = utils.toMsg(query);

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

  function started(traceId) {
    emitter.emit('trace', traceId);
  }

  this.exec(queryMsg, done, output, progress, metadata, started);

  return emitter;
};

Client.prototype.run = function(query, doneCb) {

  var data = [];
  var queryMsg = utils.toMsg(query);

  function done(err, traceId) {
    if (err) {
      doneCb(err, null, traceId);
    } else {
      doneCb(null, data, traceId);
    }
  }

  function output(elems) {
    data.push(elems);
  }

  this.exec(queryMsg, done, output);
};

Client.prototype.authenticate = function(user, apiKey, doneCb, traceId) {
  var token;
  var authMsg = {
    e: 'H',
    p: {
      user: user,
      trace_id: traceId
    },
    v: apiKey
  };

  function done(err, traceId) {
    if (err) {
      doneCb(err, null, traceId);
    } else {
      doneCb(null, token, traceId);
    }
  }

  function output(elems) {
    token = elems[0];
  }

  this.exec(authMsg, done, output);
};

Client.prototype.close = function() {
  if (this.ws) {
    this.ws.close();
  }
};

module.exports.Client = Client;
