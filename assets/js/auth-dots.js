/* assets/js/auth-dots.js */
/* CrossWatch - Auth dot refresh policy controller */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(function (root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) {
    module.exports = api;
  } else {
    (root.CW = root.CW || {}).createAuthDotsController = api.createAuthDotsController;
  }
})(typeof self !== "undefined" ? self : this, function () {
  function createAuthDotsController(deps) {
    const loadConfig = deps.loadConfig;
    const getCachedConfig = deps.getCachedConfig || (() => ({}));
    const applyDots = deps.applyDots || (() => false);
    const activeTab = deps.activeTab || (() => "main");
    const connectObserver = deps.connectObserver || (() => {});
    const disconnectObserver = deps.disconnectObserver || (() => {});

    let observing = false;
    let knownTab = null;
    let forced = null;

    function currentTab() {
      const tab = knownTab !== null ? knownTab : activeTab();
      return String(tab || "").toLowerCase();
    }

    const onSettings = () => currentTab() === "settings";

    function syncObserver() {
      const want = onSettings();
      if (want === observing) return observing;
      observing = want;
      if (want) connectObserver();
      else disconnectObserver();
      return observing;
    }

    function applyCached() {
      return applyDots(getCachedConfig() || {});
    }

    function refresh(force = false) {
      if (force && forced) return forced;
      const run = Promise.resolve()
        .then(() => loadConfig(!!force))
        .then((cfg) => applyDots(cfg && typeof cfg === "object" ? cfg : getCachedConfig() || {}))
        .catch(() => applyCached());
      if (!force) return run;
      const done = run.then((ok) => {
        if (forced === done) forced = null;
        return ok;
      });
      forced = done;
      return done;
    }

    return {
      onMutation() {
        syncObserver();
        if (!onSettings()) return false;
        return applyCached();
      },
      onTabChanged(tab) {
        const next = String(tab || "").toLowerCase();
        if (next) knownTab = next;
        syncObserver();
        return refresh(onSettings());
      },
      onConfigChanged() {
        return refresh(true);
      },
      refresh,
      applyCached,
      syncObserver,
      _state() {
        return { observing, tab: currentTab(), forcing: forced !== null };
      },
    };
  }

  return { createAuthDotsController };
});
