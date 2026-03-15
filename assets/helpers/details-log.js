/* assets/helpers/details-log.js */
/* Details log UI and logic for live sync logs and watcher logs. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

// Details Log (live stream)
(function(){
if (typeof window.esDet === "undefined") window.esDet = null;
if (typeof window.esDetSummary === "undefined") window.esDetSummary = null;
if (typeof window._detStaleIV === "undefined") window._detStaleIV = null;
if (typeof window._detRetryTO === "undefined") window._detRetryTO = null;
if (typeof window._detVisibilityHandler === "undefined") window._detVisibilityHandler = null;
if (typeof window.detStickBottom === "undefined") window.detStickBottom = true;
if (typeof window.esWatch === "undefined") window.esWatch = null;
if (typeof window._watchRetryTO === "undefined") window._watchRetryTO = null;
if (typeof window._watchStaleIV === "undefined") window._watchStaleIV = null;
if (typeof window._watchVisibilityHandler === "undefined") window._watchVisibilityHandler = null;
if (typeof window._watchFlushIV === "undefined") window._watchFlushIV = null;
if (typeof window.watchStickBottom === "undefined") window.watchStickBottom = true;
if (typeof window.watchBuf === "undefined") window.watchBuf = [];
if (typeof window._detailsTabsWired === "undefined") window._detailsTabsWired = false;
if (typeof window._detailsTab === "undefined") window._detailsTab = "sync";
if (typeof window.DETAILS_MAX_LINES === "undefined") window.DETAILS_MAX_LINES = 2500;

function _activeDetailsLogEl() {
  return window._detailsTab === "watcher"
    ? document.getElementById("det-watch-log")
    : document.getElementById("det-log");
}

function _pruneDetailsLog(el) {
  const max = Number(window.DETAILS_MAX_LINES || 0) || 2500;
  while (el && el.childNodes && el.childNodes.length > max) el.removeChild(el.firstChild);
}

function _isAppDebugMode(cfg) {
  return !!(cfg?.runtime?.debug || cfg?.runtime?.debug_mods);
}

function _decodeLogLine(line) {
  const host = document.createElement("textarea");
  host.innerHTML = String(line ?? "").replace(/<br\s*\/?>/gi, "\n");
  return host.value;
}

async function _copyDetailsLog(btn) {
  const el = _activeDetailsLogEl();
  const text = (el?.innerText || el?.textContent || "").trim();
  if (!text) return;

  try {
    await navigator.clipboard.writeText(text);
  } catch (_) {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    document.execCommand("copy");
    document.body.removeChild(ta);
  }

  if (!btn) return;
  const prev = btn.textContent;
  btn.textContent = "Copied";
  clearTimeout(btn._copyFlashTO);
  btn._copyFlashTO = setTimeout(() => {
    btn.textContent = prev;
  }, 1200);
}

function setDetailsTab(tab) {
  const t = (tab === "watcher") ? "watcher" : "sync";
  window._detailsTab = t;

  const syncPanel  = document.getElementById("det-panel-sync");
  const watchPanel = document.getElementById("det-panel-watcher");
  const tabSync    = document.getElementById("det-tab-sync");
  const tabWatch   = document.getElementById("det-tab-watcher");
  if (!syncPanel || !watchPanel || !tabSync || !tabWatch) return;

  const isWatch = t === "watcher";
  syncPanel.classList.toggle("hidden", isWatch);
  watchPanel.classList.toggle("hidden", !isWatch);

  tabSync.classList.toggle("active", !isWatch);
  tabSync.setAttribute("aria-selected", String(!isWatch));
  tabWatch.classList.toggle("active", isWatch);
  tabWatch.setAttribute("aria-selected", String(isWatch));

  if (isWatch) { try { openWatcherLog(); } catch {} }
}

function initDetailsTabs() {
  if (window._detailsTabsWired) return;
  const tabSync  = document.getElementById("det-tab-sync");
  const tabWatch = document.getElementById("det-tab-watcher");
  if (!tabSync || !tabWatch) return;
  window._detailsTabsWired = true;

  tabSync.addEventListener("click", () => setDetailsTab("sync"));
  tabWatch.addEventListener("click", () => setDetailsTab("watcher"));

  const btnCopy = document.getElementById("det-copy");
  if (btnCopy) {
    btnCopy.addEventListener("click", () => {
      _copyDetailsLog(btnCopy).catch(() => {});
    });
  }

  const btnClear = document.getElementById("det-clear");
  if (btnClear) {
    btnClear.addEventListener("click", () => {
      const el = _activeDetailsLogEl();
      if (el) el.innerHTML = "";
      if (window._detailsTab === "watcher") window.watchBuf.length = 0;
    });
  }

  const btnFollow = document.getElementById("det-follow");
  if (btnFollow) {
    btnFollow.addEventListener("click", () => {
      if (window._detailsTab === "watcher") {
        window.watchStickBottom = !window.watchStickBottom;
        const el = document.getElementById("det-watch-log");
        if (window.watchStickBottom && el) el.scrollTop = el.scrollHeight;
      } else {
        window.detStickBottom = !window.detStickBottom;
        const el = document.getElementById("det-log");
        if (window.detStickBottom && el) el.scrollTop = el.scrollHeight;
      }
    });
  }
}

function closeWatcherLog() {
  try { window.esWatch?.close?.(); } catch {}
  window.esWatch = null;
  if (window._watchRetryTO) { clearTimeout(window._watchRetryTO); window._watchRetryTO = null; }
  if (window._watchStaleIV) { clearInterval(window._watchStaleIV); window._watchStaleIV = null; }
  if (window._watchFlushIV) { clearInterval(window._watchFlushIV); window._watchFlushIV = null; }
  if (window._watchVisibilityHandler) {
    document.removeEventListener("visibilitychange", window._watchVisibilityHandler);
    window._watchVisibilityHandler = null;
  }
  const tabWatch = document.getElementById("det-tab-watcher");
  tabWatch?.classList.remove("connected", "stale");
}

async function openWatcherLog() {
  const el = document.getElementById("det-watch-log");
  const details = document.getElementById("details");
  const tabWatch = document.getElementById("det-tab-watcher");
  if (!el || !details || details.classList.contains("hidden")) return;
  if (window.esWatch || window._watchOpening) return;
  window._watchOpening = true;

  try {
    let cfg = window._cfgCache;
    if (!cfg) {
      try {
        cfg = await fetch("/api/config", { cache: "no-store" }).then(r => r.json());
        window._cfgCache = cfg;
      } catch {}
    }

    const sc = (cfg && typeof cfg === "object") ? (cfg.scrobble || {}) : {};
    let watchCfg = (sc && typeof sc === "object" && sc.watch && typeof sc.watch === "object") ? sc.watch : null;
    if (!watchCfg && cfg && typeof cfg.watch === "object") watchCfg = cfg.watch;
    watchCfg = watchCfg || {};

    const norm = (t) => {
      const s = String(t || "").trim().toUpperCase();
      if (!s) return "";
      if (s === "JFIN" || s === "JELLY") return "JELLYFIN";
      return s;
    };

    const provider = norm(watchCfg.provider || "plex");
    const sinksRaw = String(watchCfg.sink || "trakt");
    const sinks = sinksRaw.split(/[,&+]/g).map(norm).filter(Boolean);
    const tags = [provider, ...sinks].filter(Boolean);
    const uniq = [];
    for (const t of tags) if (!uniq.includes(t)) uniq.push(t);

    const url = new URL("/api/logs/watcher", document.baseURI);
    url.searchParams.set("tail", "200");
    if (uniq.length) url.searchParams.set("tags", uniq.join(","));

    if (!el.__cwScrollWired) {
      el.addEventListener("scroll", () => {
        const pad = 12;
        window.watchStickBottom = el.scrollTop >= el.scrollHeight - el.clientHeight - pad;
      }, { passive: true });
      el.__cwScrollWired = true;
    }

    window.watchBuf.length = 0;
    el.innerHTML = "";
    window.watchStickBottom = true;

    const MAX_LINES = Number(window.DETAILS_MAX_LINES || 0) || 2500;
    let lastMsgAt = Date.now();

    const enqueue = (tag, html) => {
      if (!html) return;
      window.watchBuf.push({ tag, html });
      lastMsgAt = Date.now();
    };

    const flush = () => {
      if (!window.watchBuf.length) return;
      const frag = document.createDocumentFragment();
      const items = window.watchBuf.splice(0, window.watchBuf.length);
      for (const it of items) {
        const row = document.createElement("div");
        row.className = "wlog-line";

        const badge = document.createElement("span");
        badge.className = "wlog-tag";
        badge.textContent = it.tag;

        const msg = document.createElement("span");
        msg.className = "wlog-msg";
        msg.innerHTML = it.html;

        row.appendChild(badge);
        row.appendChild(msg);
        frag.appendChild(row);
      }
      el.appendChild(frag);

      while (el.childNodes.length > MAX_LINES) el.removeChild(el.firstChild);
      if (window.watchStickBottom) el.scrollTop = el.scrollHeight;
    };

    if (window._watchFlushIV) clearInterval(window._watchFlushIV);
    window._watchFlushIV = setInterval(flush, 120);

    url.searchParams.set("_ts", String(Date.now()));
    const es = new EventSource(url.toString());
    window.esWatch = es;
    tabWatch?.classList.add("connected");
    tabWatch?.classList.remove("stale");

    for (const t of (uniq.length ? uniq : ["PLEX","JELLYFIN","EMBY","TRAKT","SIMKL","TMDB","MDBLIST","TRBL"])) {
      es.addEventListener(t, (ev) => enqueue(t, ev?.data));
    }

    es.addEventListener("ping", () => { lastMsgAt = Date.now(); });

    es.onerror = () => {
      tabWatch?.classList.remove("connected");
      try { window.esWatch?.close?.(); } catch {}
      window.esWatch = null;

      if (window._watchRetryTO) clearTimeout(window._watchRetryTO);
      window._watchRetryTO = setTimeout(() => {
        if (window._detailsTab === "watcher") { try { openWatcherLog(); } catch {} }
      }, 1200);
    };

    if (window._watchStaleIV) clearInterval(window._watchStaleIV);
    window._watchStaleIV = setInterval(() => {
      const stale = (Date.now() - lastMsgAt) > 20000;
      tabWatch?.classList.toggle("stale", stale);
    }, 1000);

    if (window._watchVisibilityHandler) {
      document.removeEventListener("visibilitychange", window._watchVisibilityHandler);
    }
    window._watchVisibilityHandler = () => {
      if (document.visibilityState !== "visible") return;
      if (window._detailsTab === "watcher") { try { openWatcherLog(); } catch {} }
    };
    document.addEventListener("visibilitychange", window._watchVisibilityHandler);
  } finally {
    window._watchOpening = false;
  }
}

async function openDetailsLog() {
  const el = document.getElementById("det-log");
  const slider = document.getElementById("det-scrub");
  if (!el) return;
  const authSetupPending = () => window.cwIsAuthSetupPending?.() === true;
  const tabSync = document.getElementById("det-tab-sync");
  try { initDetailsTabs(); } catch {}

  try {
    const cfg = window._cfgCache || await fetch("/api/config", { cache: "no-store" }).then(r => r.json());
    window._cfgCache = cfg;
    window.appDebug = _isAppDebugMode(cfg);
  } catch (_) {}

  const CF = window.ClientFormatter;
  const useFormatter = !window.appDebug && CF && CF.processChunk && CF.renderInto;

  el.innerHTML = "";
  el.classList?.toggle("cf-log", !!useFormatter);
  el.classList?.remove("cf-log-plain");
  if (!useFormatter) el.classList?.add("cf-log-plain");
  window.detStickBottom = true;
  try { CF?.reset?.(); } catch {}

  try { window.esDet?.close(); } catch {}
  try { window.esDetSummary?.close(); } catch {}
  window.esDet = null;
  window.esDetSummary = null;
  if (window._detStaleIV) { clearInterval(window._detStaleIV); window._detStaleIV = null; }
  if (window._detRetryTO) { clearTimeout(window._detRetryTO); window._detRetryTO = null; }
  if (window._detVisibilityHandler) { document.removeEventListener("visibilitychange", window._detVisibilityHandler); window._detVisibilityHandler = null; }

  const updateSlider = () => {
    if (!slider) return;
    const max = el.scrollHeight - el.clientHeight;
    slider.value = max <= 0 ? 100 : Math.round((el.scrollTop / max) * 100);
  };

  const updateStick = () => {
    const pad = 6;
    window.detStickBottom = el.scrollTop >= el.scrollHeight - el.clientHeight - pad;
  };

  el.addEventListener("scroll", () => { updateSlider(); updateStick(); }, { passive: true });

  if (slider) {
    slider.addEventListener("input", () => {
      const max = el.scrollHeight - el.clientHeight;
      el.scrollTop = Math.round((slider.value / 100) * max);
      window.detStickBottom = slider.value >= 99;
    });
  }

  const appendRaw = (s) => {
    const lines = String(s).replace(/\r\n/g, "\n").split("\n");
    for (const line of lines) {
      if (!line) continue;
      const row = document.createElement("div");
      row.className = "wlog-line det-plain-line";

      const msg = document.createElement("span");
      msg.className = "wlog-msg";
      msg.textContent = _decodeLogLine(line);

      row.appendChild(msg);
      el.appendChild(row);
    }
  };

  let detBuf = "";
  let lastMsgAt = Date.now();
  let retryMs = 1000;
  const STALE_MS = 20000;

  const connect = () => {
    if (authSetupPending()) return;
    try { window.esDet?.close(); } catch (_) {}
    const url = new URL("/api/logs/stream", document.baseURI);
    url.searchParams.set("tag", "SYNC");
    url.searchParams.set("_ts", String(Date.now()));
    window.esDet = new EventSource(url.toString());
    window.esDet.onopen = () => { tabSync?.classList.add("connected"); tabSync?.classList.remove("stale"); };

    window.esDet.onmessage = (ev) => {
      lastMsgAt = Date.now();
      tabSync?.classList.add("connected");
      tabSync?.classList.remove("stale");
      if (!ev?.data) return;

      if (ev.data === "::CLEAR::") {
        el.textContent = "";
        detBuf = "";
        try { CF?.reset?.(); } catch {}
        updateSlider();
        return;
      }

      if (!useFormatter) {
        appendRaw(ev.data);
      } else {
        const { tokens, buf } = CF.processChunk(detBuf, ev.data);
        detBuf = buf;
        for (const tok of tokens) CF.renderInto(el, tok, false);
      }
      _pruneDetailsLog(el);
      if (window.detStickBottom) el.scrollTop = el.scrollHeight;
      updateSlider();
      retryMs = 1000;
    };

    window.esDet.onerror = () => {
        tabSync?.classList.remove("connected");
        tabSync?.classList.add("stale");
      try { window.esDet?.close(); } catch (_) {}
      window.esDet = null;

      if (useFormatter && detBuf && detBuf.trim()) {
        const { tokens } = CF.processChunk("", detBuf);
        detBuf = "";
        for (const tok of tokens) CF.renderInto(el, tok, false);
        if (window.detStickBottom) el.scrollTop = el.scrollHeight;
        updateSlider();
      }

      if (!window._detRetryTO) {
        if (authSetupPending()) return;
        window._detRetryTO = setTimeout(() => {
          window._detRetryTO = null;
          connect();
        }, retryMs);
        retryMs = Math.min(retryMs * 2, 15000);
      }
    };
  };

  connect();

  window._detStaleIV = setInterval(() => {
    tabSync?.classList.toggle("stale", (Date.now() - lastMsgAt) > STALE_MS);
    if (!window.esDet) return;
    if (document.visibilityState !== "visible") return;
    if (Date.now() - lastMsgAt > STALE_MS) {
      try { window.esDet.close(); } catch (_) {}
      window.esDet = null;
      connect();
    }
  }, STALE_MS);

  window._detVisibilityHandler = () => {
    if (document.visibilityState !== "visible") return;
    if (!window.esDet || (Date.now() - lastMsgAt > STALE_MS)) connect();
  };
  document.addEventListener("visibilitychange", window._detVisibilityHandler);

  try { window.esDetSummary?.close(); } catch (_) {}
  window.esDetSummary = null;

  requestAnimationFrame(() => {
    el.scrollTop = el.scrollHeight;
    updateSlider();
  });
}

function closeDetailsLog() {
  try { window.esDet?.close(); } catch (_) {}
  try { window.esDetSummary?.close(); } catch (_) {}
  window.esDet = null;
  window.esDetSummary = null;
  if (window._detStaleIV) { clearInterval(window._detStaleIV); window._detStaleIV = null; }
  if (window._detRetryTO) { clearTimeout(window._detRetryTO); window._detRetryTO = null; }
  if (window._detVisibilityHandler) { document.removeEventListener("visibilitychange", window._detVisibilityHandler); window._detVisibilityHandler = null; }
  const tabSync = document.getElementById("det-tab-sync");
  tabSync?.classList.remove("connected", "stale");
  const tabWatch = document.getElementById("det-tab-watcher");
  tabWatch?.classList.remove("connected", "stale");
  try { closeWatcherLog(); } catch {}
}

function toggleDetails() {
  const d = document.getElementById("details");
  d.classList.toggle("hidden");
  if (!d.classList.contains("hidden")) {
    try { initDetailsTabs(); } catch {}
    try { setDetailsTab(window._detailsTab || "sync"); } catch {}
    openDetailsLog();
    if (window._detailsTab === "watcher") { try { openWatcherLog(); } catch {} }
  } else {
    closeDetailsLog();
    closeWatcherLog();
  }
}

window.addEventListener("beforeunload", () => {
  try { closeDetailsLog(); } catch {}
  try { closeWatcherLog(); } catch {}
});


  const DetailsLog = {
    setDetailsTab,
    initDetailsTabs,
    closeWatcherLog,
    openWatcherLog,
    openDetailsLog,
    closeDetailsLog,
    toggleDetails,
  };

  (window.CW ||= {}).DetailsLog = DetailsLog;
  Object.assign(window, DetailsLog);
})();
