'use strict';

function toMsg(query, traceId, token) {
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
