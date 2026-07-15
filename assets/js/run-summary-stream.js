/* assets/js/run-summary-stream.js */
/* CrossWatch - Run-summary SSE stream lifecycle controller */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(function (root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) {
    module.exports = api;
  } else {
    (root.CW = root.CW || {}).createSummaryStreamController = api.createSummaryStreamController;
  }
})(typeof self !== "undefined" ? self : this, function () {
  function createSummaryStreamController(deps) {
    const isHidden = deps.isHidden;
    const openStream = deps.openStream;
    const closeStream = deps.closeStream;
    const pullSummary = deps.pullSummary;
    const setTimer = deps.setTimer || ((fn, ms) => setTimeout(fn, ms));
    const clearTimer = deps.clearTimer || ((t) => clearTimeout(t));
    const baseDelay = deps.baseDelay ?? 2000;
    const maxDelay = deps.maxDelay ?? 30000;

    let reconnectTimer = null;
    let attempt = 0;

    function clearReconnect() {
      if (reconnectTimer !== null) {
        clearTimer(reconnectTimer);
        reconnectTimer = null;
      }
    }

    function scheduleReconnect() {
      if (isHidden()) return;
      if (reconnectTimer !== null) return;
      const delay = Math.min(baseDelay * 2 ** attempt, maxDelay);
      attempt += 1;
      reconnectTimer = setTimer(() => {
        reconnectTimer = null;
        if (isHidden()) return;
        openStream();
      }, delay);
    }

    return {
      onOpen() {
        attempt = 0;
        clearReconnect();
      },
      onError() {
        closeStream();
        if (isHidden()) return;
        scheduleReconnect();
      },
      onVisibility() {
        if (isHidden()) {
          closeStream();
          clearReconnect();
          return Promise.resolve();
        }
        clearReconnect();
        attempt = 0;
        let pulled;
        try { pulled = pullSummary(); } catch (e) { pulled = Promise.reject(e); }
        return Promise.resolve(pulled)
          .catch(() => {})
          .then(() => {
            if (isHidden()) return;
            openStream();
          });
      },
      clearReconnect,
      _state() {
        return { hasTimer: reconnectTimer !== null, attempt };
      },
    };
  }

  return { createSummaryStreamController };
});
