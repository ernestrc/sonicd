'use strict';

function toMsg(query, tokenMaybe) {
  var traceId = query.trace_id || query.traceId;
  var token = query.token || tokenMaybe;
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

function toProgress(payload) {
  return {
    status: payload.s,
    progress: payload.p,
    total: payload.t,
    units: payload.u
  };
}

module.exports = {
  toMsg: toMsg,
  toProgress: toProgress
};
