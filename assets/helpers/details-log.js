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
if (typeof window._watchFlushRAF === "undefined") window._watchFlushRAF = null;
if (typeof window.watchStickBottom === "undefined") window.watchStickBottom = true;
if (typeof window.watchBuf === "undefined") window.watchBuf = [];
if (typeof window.esDebug === "undefined") window.esDebug = null;
if (typeof window._debugRetryTO === "undefined") window._debugRetryTO = null;
if (typeof window._debugStaleIV === "undefined") window._debugStaleIV = null;
if (typeof window._debugVisibilityHandler === "undefined") window._debugVisibilityHandler = null;
if (typeof window.debugStickBottom === "undefined") window.debugStickBottom = true;
if (typeof window.debugBuf === "undefined") window.debugBuf = [];
if (typeof window._debugFlushRAF === "undefined") window._debugFlushRAF = null;
if (typeof window._detailsTabsWired === "undefined") window._detailsTabsWired = false;
if (typeof window._detailsTab === "undefined") window._detailsTab = "sync";
if (typeof window.DETAILS_MAX_LINES === "undefined") window.DETAILS_MAX_LINES = 600;
if (typeof window.DETAILS_STREAM_TAIL === "undefined") window.DETAILS_STREAM_TAIL = 400;
if (typeof window.DETAILS_QUEUE_MAX === "undefined") window.DETAILS_QUEUE_MAX = 1200;
if (typeof window.DETAILS_BATCH_ROWS === "undefined") window.DETAILS_BATCH_ROWS = 80;
if (typeof window.DETAILS_FRAME_BUDGET_MS === "undefined") window.DETAILS_FRAME_BUDGET_MS = 8;
if (typeof window._detOpenSeq === "undefined") window._detOpenSeq = 0;
if (typeof window.syncBuf === "undefined") window.syncBuf = [];
if (typeof window._syncFlushRAF === "undefined") window._syncFlushRAF = null;
if (typeof window._detSeenLines === "undefined") window._detSeenLines = [];
if (typeof window._detReplayActive === "undefined") window._detReplayActive = false;
if (typeof window._detReplayCursor === "undefined") window._detReplayCursor = 0;
if (typeof window._detDidConnectOnce === "undefined") window._detDidConnectOnce = false;
if (typeof window._detLastSeq === "undefined") window._detLastSeq = 0;
if (typeof window._debugLastSeq === "undefined") window._debugLastSeq = 0;
if (typeof window._detailsDropped === "undefined") window._detailsDropped = { sync: 0, watcher: 0, debug: 0 };
if (typeof window._detailsStatusRAF === "undefined") window._detailsStatusRAF = null;

function _activeDetailsLogEl() {
  if (window._detailsTab === "watcher") return document.getElementById("det-watch-log");
  if (window._detailsTab === "debug") return document.getElementById("det-debug-log");
  return document.getElementById("det-log");
}

function _pruneDetailsLog(el) {
  const max = Number(window.DETAILS_MAX_LINES || 0) || 600;
  while (el && el.childNodes && el.childNodes.length > max) el.removeChild(el.firstChild);
}

function _pruneSeenDetailLines() {
  const max = Number(window.DETAILS_MAX_LINES || 0) || 600;
  const lines = Array.isArray(window._detSeenLines) ? window._detSeenLines : [];
  if (lines.length > max) window._detSeenLines = lines.slice(lines.length - max);
}

function _detailsLimit(name, fallback) {
  const value = Number(window[name] || 0);
  return Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
}

function _resetDetailsDropped(tab) {
  if (!window._detailsDropped || typeof window._detailsDropped !== "object") {
    window._detailsDropped = { sync: 0, watcher: 0, debug: 0 };
  }
  window._detailsDropped[tab] = 0;
}

function _enqueueDetailsItem(queue, item, tab) {
  queue.push(item);
  const max = _detailsLimit("DETAILS_QUEUE_MAX", 1200);
  if (queue.length <= max) return;
  const dropped = queue.length - max;
  queue.splice(0, dropped);
  window._detailsDropped[tab] = Number(window._detailsDropped[tab] || 0) + dropped;
  _scheduleDetailsConsoleStatus();
}

function _runDetailsBatch(queue, renderItem, afterBatch, scheduleNext) {
  const maxRows = _detailsLimit("DETAILS_BATCH_ROWS", 80);
  const budget = _detailsLimit("DETAILS_FRAME_BUDGET_MS", 8);
  const started = performance.now();
  let rendered = 0;
  while (rendered < queue.length && rendered < maxRows && performance.now() - started < budget) {
    renderItem(queue[rendered]);
    rendered += 1;
  }
  if (rendered) queue.splice(0, rendered);
  if (rendered) afterBatch?.(rendered);
  if (queue.length) scheduleNext();
}

function _rememberDetailLine(line) {
  if (!Array.isArray(window._detSeenLines)) window._detSeenLines = [];
  window._detSeenLines.push(String(line ?? ""));
  _pruneSeenDetailLines();
}

function _beginDetailReplayFilter() {
  const lines = Array.isArray(window._detSeenLines) ? window._detSeenLines : [];
  window._detReplayActive = lines.length > 0;
  window._detReplayCursor = 0;
}

function _shouldSkipReplayedDetailLine(line) {
  if (!window._detReplayActive) return false;
  const lines = Array.isArray(window._detSeenLines) ? window._detSeenLines : [];
  if (window._detReplayCursor >= lines.length) {
    window._detReplayActive = false;
    return false;
  }
  if (String(lines[window._detReplayCursor] ?? "") === String(line ?? "")) {
    window._detReplayCursor += 1;
    return true;
  }
  window._detReplayActive = false;
  return false;
}

function _detailsVisible() {
  const details = document.getElementById("details");
  return !!(details && !details.classList.contains("hidden"));
}

function _cwModalOpen() {
  return !!(document.body && document.body.dataset && document.body.dataset.cxModalOpen);
}

function _streamsAllowed() {
  return _detailsVisible() && !_cwModalOpen();
}

function _watchLogKnownTags() {
  return [
    "WATCH",
    "WATCHM",
    "SCROBBLE",
    "WEBHOOK",
    "PLEX-WATCH",
    "JELLYFIN-WATCH",
    "EMBY-WATCH",
    "TRAKT-SCROBBLE",
    "SIMKL-SCROBBLE",
    "MDBLIST-SCROBBLE",
  ];
}

function _watchLogTagsFromConfig(cfg) {
  return _watchLogKnownTags();
}

function _isAppDebugMode(cfg) {
  return !!(cfg?.runtime?.debug || cfg?.runtime?.debug_mods);
}

function _decodeLogLine(line) {
  return String(line ?? "");
}

function _plainLogText(value) {
  return String(value ?? "").replace(/\u00a0/g, " ").trim();
}

function _logTimeNow() {
  return new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
}

function _parseLogParts(raw, fallbackProvider = "") {
  let text = _plainLogText(raw);
  const fallback = String(fallbackProvider || "").trim().toUpperCase();
  let provider = "";
  let level = "";
  let time = "";
  const known = new Set(_watchLogKnownTags());
  const levelName = (value) => {
    const key = String(value || "").trim().toUpperCase();
    if (key === "WARNING") return "WARN";
    if (key === "ERR" || key === "CRITICAL" || key === "FATAL") return "ERROR";
    if (key === "I") return "INFO";
    return ["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "SUCCESS"].includes(key) ? key : "";
  };

  for (let i = 0; i < 5; i++) {
    const match = text.match(/^\s*\[([^\]]+)]\s*/);
    if (!match) break;
    const token = String(match[1] || "").trim();
    const tokenLevel = levelName(token);
    const tokenTime = token.match(/\b(\d{2}:\d{2}:\d{2})(?:[.,]\d+)?\b/);
    if (tokenTime) time ||= tokenTime[1];
    else if (tokenLevel) level ||= tokenLevel;
    else if (known.has(token.toUpperCase()) || /^[A-Z][A-Z0-9_-]{1,20}$/.test(token)) provider ||= token.toUpperCase();
    else break;
    text = text.slice(match[0].length);
  }

  const timeMatch = text.match(/(?:^|\s)(\d{2}:\d{2}:\d{2})(?:[.,]\d+)?(?=\s|$)/);
  if (timeMatch) {
    time ||= timeMatch[1];
    text = `${text.slice(0, timeMatch.index)} ${text.slice((timeMatch.index || 0) + timeMatch[0].length)}`.trim();
  }

  if (provider) {
    const providerPrefix = new RegExp(`^\\s*(?:\\[${provider.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\]\\s*)+`, "i");
    text = text.replace(providerPrefix, "");
  } else {
    const providerMatch = text.match(/^\s*([A-Z][A-Z0-9_-]{1,20})\s+(?=(?:TRACE|DEBUG|INFO|WARN(?:ING)?|ERROR|CRITICAL|SUCCESS)\b)/i);
    if (providerMatch) {
      provider = providerMatch[1].toUpperCase();
      text = text.slice(providerMatch[0].length);
    }
  }

  const levelMatch = text.match(/^\s*(TRACE|DEBUG|INFO|WARN(?:ING)?|ERROR|ERR|CRITICAL|FATAL|SUCCESS)\b[:\s-]*/i);
  if (levelMatch) {
    level ||= levelName(levelMatch[1]);
    text = text.slice(levelMatch[0].length);
  }

  return {
    provider: provider || fallback || "SYSTEM",
    level: level || "INFO",
    message: text.trim() || _plainLogText(raw) || "-",
    time: time || _logTimeNow(),
  };
}

function _cwUnresEsc(s) {
  return String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

function _renderUnresolvedList(items) {
  if (!items.length) return `<div class="cw-unres-empty">No unresolved items.</div>`;
  const counts = new Map();
  for (const it of items) {
    const r = it.reason || "unresolved";
    counts.set(r, (counts.get(r) || 0) + 1);
  }
  let head = `<div class="cw-unres-summary">`;
  for (const [r, n] of counts) head += `<span class="cw-unres-chip">${_cwUnresEsc(r)} <b>${n}</b></span>`;
  head += `</div>`;

  let html = `<ul class="cw-unres-list">`;
  for (const it of items) {
    const label = it.code ? `${_cwUnresEsc(it.title)} · ${_cwUnresEsc(it.code)}` : _cwUnresEsc(it.title);
    html += `<li class="cw-unres-item"><span class="cw-unres-item-t">${label}</span>` +
            `<span class="cw-unres-item-r">${_cwUnresEsc(it.reason || "unresolved")}</span></li>`;
  }
  html += `</ul>`;
  return head + html;
}

function _ensureUnresolvedStyles() {
  if (document.getElementById("cw-unres-css")) return;
  const css = `
  .cw-unres-modal{position:fixed;inset:0;z-index:9999;display:flex;align-items:center;justify-content:center}
  .cw-unres-modal.hidden{display:none}
  .cw-unres-backdrop{position:absolute;inset:0;background:rgba(0,0,0,.55)}
  .cw-unres-card{position:relative;max-width:720px;width:calc(100% - 32px);max-height:80vh;display:flex;flex-direction:column;
    background:var(--card,#1b1d22);color:var(--text,#e8eaed);border:1px solid rgba(255,255,255,.1);border-radius:12px;box-shadow:0 12px 40px rgba(0,0,0,.5)}
  .cw-unres-h{display:flex;align-items:center;justify-content:space-between;padding:14px 16px;border-bottom:1px solid rgba(255,255,255,.08)}
  .cw-unres-title{font-weight:600}
  .cw-unres-close{background:transparent;border:0;color:inherit;font-size:16px;cursor:pointer;opacity:.7}
  .cw-unres-close:hover{opacity:1}
  .cw-unres-body{padding:0 16px 16px;overflow:auto}
  .cw-unres-summary{position:sticky;top:0;z-index:1;background:var(--card,#1b1d22);display:flex;flex-wrap:wrap;gap:6px;
    padding:10px 0;border-bottom:1px solid rgba(255,255,255,.08)}
  .cw-unres-chip{font-size:11px;background:rgba(255,255,255,.08);border-radius:10px;padding:2px 9px}
  .cw-unres-chip b{font-weight:600}
  .cw-unres-list{list-style:none;margin:0;padding:0}
  .cw-unres-item{display:flex;align-items:center;gap:12px;padding:5px 0;font-size:13px;border-bottom:1px solid rgba(255,255,255,.05)}
  .cw-unres-item-t{flex:1 1 auto;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .cw-unres-item-r{flex:0 0 auto;font-size:11px;opacity:.6;white-space:nowrap}
  .cw-unres-loading,.cw-unres-empty,.cw-unres-error{padding:24px 0;text-align:center;opacity:.7}
  .cw-unres-link{color:#8ea2ff;cursor:pointer;text-decoration:underline;text-underline-offset:2px}
  .cw-unres-link:hover{color:#b8c6ff}`;
  const style = document.createElement("style");
  style.id = "cw-unres-css";
  style.textContent = css;
  document.head.appendChild(style);
}

async function _showUnresolvedModal() {
  _ensureUnresolvedStyles();
  let modal = document.getElementById("cw-unresolved-modal");
  if (!modal) {
    modal = document.createElement("div");
    modal.id = "cw-unresolved-modal";
    modal.className = "cw-unres-modal hidden";
    modal.innerHTML =
      `<div class="cw-unres-backdrop"></div>` +
      `<div class="cw-unres-card">` +
      `<div class="cw-unres-h"><div class="cw-unres-title">Unresolved items</div>` +
      `<button class="cw-unres-close" type="button" aria-label="Close">✕</button></div>` +
      `<div class="cw-unres-body cw-scrollbars"><div class="cw-unres-loading">Loading…</div></div>` +
      `</div>`;
    document.body.appendChild(modal);
    const close = () => modal.classList.add("hidden");
    modal.querySelector(".cw-unres-close").addEventListener("click", close);
    modal.querySelector(".cw-unres-backdrop").addEventListener("click", close);
    document.addEventListener("keydown", (e) => { if (e.key === "Escape") close(); });
  }
  const body = modal.querySelector(".cw-unres-body");
  const titleEl = modal.querySelector(".cw-unres-title");
  body.innerHTML = `<div class="cw-unres-loading">Loading…</div>`;
  modal.classList.remove("hidden");
  try {
    const r = await fetch("/api/run/unresolved", { headers: { Accept: "application/json" } });
    const data = await r.json();
    const items = Array.isArray(data.items) ? data.items : [];
    titleEl.textContent = `Unresolved items (${data.total ?? items.length})`;
    body.innerHTML = _renderUnresolvedList(items);
  } catch (_) {
    body.innerHTML = `<div class="cw-unres-error">Failed to load unresolved items.</div>`;
  }
}

function _unresolvedMoreRow() {
  // Render like a normal structured log line (SYNC / INFO / message) so it sits
  // flush with the rest of the log; only the message is a clickable link.
  const row = document.createElement("div");
  row.className = "wlog-line det-structured-line";
  row.dataset.provider = "SYNC";
  row.dataset.level = "info";

  const badge = document.createElement("span");
  badge.className = "wlog-tag";
  badge.textContent = "SYNC";

  const level = document.createElement("span");
  level.className = "wlog-level level-info";
  level.textContent = "INFO";

  const msg = document.createElement("span");
  msg.className = "wlog-msg";
  const link = document.createElement("a");
  link.className = "cw-unres-link";
  link.href = "#";
  link.textContent = "Show the full unresolved list";
  link.addEventListener("click", (e) => { e.preventDefault(); _showUnresolvedModal(); });
  msg.appendChild(link);

  const time = document.createElement("time");
  time.className = "wlog-time";
  time.textContent = "";

  row.append(badge, level, msg, time);
  return row;
}

function _structuredLogRow(raw, fallbackProvider = "") {
  if (String(raw ?? "").includes("::CW_UNRESOLVED_MORE::")) return _unresolvedMoreRow();
  const parts = _parseLogParts(raw, fallbackProvider);
  const row = document.createElement("div");
  row.className = "wlog-line det-structured-line";
  row.dataset.provider = parts.provider;
  row.dataset.level = parts.level.toLowerCase();

  const badge = document.createElement("span");
  badge.className = "wlog-tag";
  badge.textContent = parts.provider;

  const level = document.createElement("span");
  level.className = `wlog-level level-${parts.level.toLowerCase()}`;
  level.textContent = parts.level;

  const msg = document.createElement("span");
  msg.className = "wlog-msg";
  msg.textContent = parts.message;

  const time = document.createElement("time");
  time.className = "wlog-time";
  time.textContent = parts.time;

  row.append(badge, level, msg, time);
  return row;
}

function _detailsStickBottom() {
  if (window._detailsTab === "watcher") return !!window.watchStickBottom;
  if (window._detailsTab === "debug") return !!window.debugStickBottom;
  return !!window.detStickBottom;
}

function _updateDetailsConsoleStatus() {
  const el = _activeDetailsLogEl();
  const tab = document.getElementById(`det-tab-${window._detailsTab || "sync"}`);
  const live = document.getElementById("det-live-state");
  const liveLabel = live?.querySelector(".det-live-label");
  const followState = document.getElementById("det-follow-state");
  const count = document.getElementById("det-line-count");
  const follow = document.getElementById("det-follow");
  const following = _detailsStickBottom();
  const stale = !!tab?.classList.contains("stale");
  const connected = !!tab?.classList.contains("connected");
  const rows = el?.childElementCount || 0;
  const dropped = Number(window._detailsDropped?.[window._detailsTab || "sync"] || 0);

  live?.classList.toggle("is-live", connected && !stale);
  live?.classList.toggle("is-stale", stale);
  if (liveLabel) liveLabel.textContent = stale ? "Reconnecting" : (connected ? "Live" : "Idle");
  if (followState) followState.textContent = `Auto-scroll ${following ? "on" : "off"}`;
  if (count) count.textContent = `${rows} ${rows === 1 ? "line" : "lines"}${dropped ? ` · ${dropped} omitted` : ""}`;
  follow?.classList.toggle("is-on", following);
  follow?.setAttribute("aria-pressed", String(following));
}

function _scheduleDetailsConsoleStatus() {
  if (window._detailsStatusRAF) return;
  window._detailsStatusRAF = requestAnimationFrame(() => {
    window._detailsStatusRAF = null;
    _updateDetailsConsoleStatus();
  });
}

function _wireDetailsConsoleStatus() {
  for (const id of ["det-log", "det-watch-log", "det-debug-log"]) {
    const el = document.getElementById(id);
    if (!el || el.__cwStatusObserver) continue;
    el.__cwStatusObserver = new MutationObserver(_scheduleDetailsConsoleStatus);
    el.__cwStatusObserver.observe(el, { childList: true });
  }
  _updateDetailsConsoleStatus();
}

function _copyableDetailsRow(row) {
  if (!row) return "";
  if (row.classList?.contains("det-structured-line")) {
    const time = row.querySelector(".wlog-time")?.textContent?.trim() || "";
    const provider = row.querySelector(".wlog-tag")?.textContent?.trim() || "SYSTEM";
    const level = row.querySelector(".wlog-level")?.textContent?.trim() || "INFO";
    const message = row.querySelector(".wlog-msg")?.textContent?.replace(/\s+/g, " ").trim() || "";
    return [time, `[${provider}]`, level, message].filter(Boolean).join(" ");
  }
  return String(row.innerText || row.textContent || "")
    .replace(/\s*\n\s*/g, " ")
    .replace(/[\t ]+/g, " ")
    .trim();
}

function _copyableDetailsLog(el) {
  if (!el) return "";
  const lines = Array.from(el.children, _copyableDetailsRow).filter(Boolean);
  const dropped = Number(window._detailsDropped?.[window._detailsTab || "sync"] || 0);
  if (dropped) lines.push(`--- ${dropped} incoming log ${dropped === 1 ? "entry was" : "entries were"} omitted from this view ---`);
  return lines.join("\n");
}

async function _copyDetailsLog(btn) {
  const el = _activeDetailsLogEl();
  const text = _copyableDetailsLog(el);
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
  const icon = btn.querySelector(".material-symbols-rounded");
  const prev = icon?.textContent || "content_copy";
  if (icon) icon.textContent = "check";
  btn.classList.add("is-copied");
  clearTimeout(btn._copyFlashTO);
  btn._copyFlashTO = setTimeout(() => {
    if (icon) icon.textContent = prev;
    btn.classList.remove("is-copied");
  }, 1200);
}

function setDetailsTab(tab) {
  const t = (tab === "watcher" || tab === "debug") ? tab : "sync";
  window._detailsTab = t;

  const syncPanel  = document.getElementById("det-panel-sync");
  const watchPanel = document.getElementById("det-panel-watcher");
  const debugPanel = document.getElementById("det-panel-debug");
  const tabSync    = document.getElementById("det-tab-sync");
  const tabWatch   = document.getElementById("det-tab-watcher");
  const tabDebug   = document.getElementById("det-tab-debug");
  if (!syncPanel || !watchPanel || !debugPanel || !tabSync || !tabWatch || !tabDebug) return;

  const isWatch = t === "watcher";
  const isDebug = t === "debug";
  syncPanel.classList.toggle("hidden", isWatch || isDebug);
  watchPanel.classList.toggle("hidden", !isWatch);
  debugPanel.classList.toggle("hidden", !isDebug);

  tabSync.classList.toggle("active", t === "sync");
  tabSync.setAttribute("aria-selected", String(t === "sync"));
  tabWatch.classList.toggle("active", isWatch);
  tabWatch.setAttribute("aria-selected", String(isWatch));
  tabDebug.classList.toggle("active", isDebug);
  tabDebug.setAttribute("aria-selected", String(isDebug));

  if (t === "sync") {
    try { closeWatcherLog(); } catch {}
    try { closeDebugLog(); } catch {}
    if (_detailsVisible()) { try { openDetailsLog(); } catch {} }
  } else if (isWatch) {
    try { closeSyncLog(); } catch {}
    try { closeDebugLog(); } catch {}
    try { openWatcherLog(); } catch {}
  } else if (isDebug) {
    try { closeSyncLog(); } catch {}
    try { closeWatcherLog(); } catch {}
    try { openDebugLog(); } catch {}
  }
  _updateDetailsConsoleStatus();
}

function initDetailsTabs() {
  if (window._detailsTabsWired) return;
  const tabSync  = document.getElementById("det-tab-sync");
  const tabWatch = document.getElementById("det-tab-watcher");
  const tabDebug = document.getElementById("det-tab-debug");
  if (!tabSync || !tabWatch || !tabDebug) return;
  window._detailsTabsWired = true;
  _wireDetailsConsoleStatus();

  tabSync.addEventListener("click", () => setDetailsTab("sync"));
  tabWatch.addEventListener("click", () => setDetailsTab("watcher"));
  tabDebug.addEventListener("click", () => setDetailsTab("debug"));

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
      if (window._detailsTab === "debug") window.debugBuf.length = 0;
      if (window._detailsTab === "sync") window.syncBuf.length = 0;
      _resetDetailsDropped(window._detailsTab || "sync");
      _updateDetailsConsoleStatus();
    });
  }

  const btnFollow = document.getElementById("det-follow");
  if (btnFollow) {
    btnFollow.addEventListener("click", () => {
      if (window._detailsTab === "watcher") {
        window.watchStickBottom = !window.watchStickBottom;
        const el = document.getElementById("det-watch-log");
        if (window.watchStickBottom && el) el.scrollTop = el.scrollHeight;
      } else if (window._detailsTab === "debug") {
        window.debugStickBottom = !window.debugStickBottom;
        const el = document.getElementById("det-debug-log");
        if (window.debugStickBottom && el) el.scrollTop = el.scrollHeight;
      } else {
        window.detStickBottom = !window.detStickBottom;
        const el = document.getElementById("det-log");
        if (window.detStickBottom && el) el.scrollTop = el.scrollHeight;
      }
      _updateDetailsConsoleStatus();
    });
  }
}

function closeSyncLog() {
  window._detOpenSeq += 1;
  try { window.esDet?.close?.(); } catch {}
  try { window.esDetSummary?.close?.(); } catch {}
  window.esDet = null;
  window.esDetSummary = null;
  window._detLastSeq = 0;
  window.syncBuf.length = 0;
  if (window._syncFlushRAF) { cancelAnimationFrame(window._syncFlushRAF); window._syncFlushRAF = null; }
  if (window._detStaleIV) { clearInterval(window._detStaleIV); window._detStaleIV = null; }
  if (window._detRetryTO) { clearTimeout(window._detRetryTO); window._detRetryTO = null; }
  if (window._detVisibilityHandler) {
    document.removeEventListener("visibilitychange", window._detVisibilityHandler);
    window._detVisibilityHandler = null;
  }
  const tabSync = document.getElementById("det-tab-sync");
  tabSync?.classList.remove("connected", "stale");
  _scheduleDetailsConsoleStatus();
}

function closeWatcherLog() {
  try { window.esWatch?.close?.(); } catch {}
  window.esWatch = null;
  window.watchBuf.length = 0;
  if (window._watchRetryTO) { clearTimeout(window._watchRetryTO); window._watchRetryTO = null; }
  if (window._watchStaleIV) { clearInterval(window._watchStaleIV); window._watchStaleIV = null; }
  if (window._watchFlushRAF) { cancelAnimationFrame(window._watchFlushRAF); window._watchFlushRAF = null; }
  if (window._watchVisibilityHandler) {
    document.removeEventListener("visibilitychange", window._watchVisibilityHandler);
    window._watchVisibilityHandler = null;
  }
  const tabWatch = document.getElementById("det-tab-watcher");
  tabWatch?.classList.remove("connected", "stale");
  _updateDetailsConsoleStatus();
}

function closeDebugLog() {
  try { window.esDebug?.close?.(); } catch {}
  window.esDebug = null;
  window._debugLastSeq = 0;
  window.debugBuf.length = 0;
  if (window._debugFlushRAF) { cancelAnimationFrame(window._debugFlushRAF); window._debugFlushRAF = null; }
  if (window._debugRetryTO) { clearTimeout(window._debugRetryTO); window._debugRetryTO = null; }
  if (window._debugStaleIV) { clearInterval(window._debugStaleIV); window._debugStaleIV = null; }
  if (window._debugVisibilityHandler) {
    document.removeEventListener("visibilitychange", window._debugVisibilityHandler);
    window._debugVisibilityHandler = null;
  }
  const tabDebug = document.getElementById("det-tab-debug");
  tabDebug?.classList.remove("connected", "stale");
  _updateDetailsConsoleStatus();
}

function openDebugLog() {
  const el = document.getElementById("det-debug-log");
  const details = document.getElementById("details");
  const tabDebug = document.getElementById("det-tab-debug");
  if (!el || !details || details.classList.contains("hidden") || _cwModalOpen() || window._detailsTab !== "debug") return;
  if (window.esDebug || window._debugOpening) return;
  window._debugOpening = true;

  try {
    if (!el.__cwScrollWired) {
      el.addEventListener("scroll", () => {
        const pad = 12;
        window.debugStickBottom = el.scrollTop >= el.scrollHeight - el.clientHeight - pad;
        _updateDetailsConsoleStatus();
      }, { passive: true });
      el.__cwScrollWired = true;
    }

    const debugSince = Math.max(0, Number(window._debugLastSeq || 0) || 0);
    const debugReconnect = debugSince > 0 && el.childElementCount > 0;
    if (!debugReconnect) {
      el.innerHTML = "";
      window.debugStickBottom = true;
      window.debugBuf.length = 0;
      _resetDetailsDropped("debug");
    }
    let lastMsgAt = Date.now();

    const scheduleFlush = () => {
      if (window._debugFlushRAF || window._detailsTab !== "debug") return;
      window._debugFlushRAF = requestAnimationFrame(() => {
        window._debugFlushRAF = null;
        const frag = document.createDocumentFragment();
        _runDetailsBatch(window.debugBuf, (html) => {
          const row = _structuredLogRow(html);
          row.classList.add("det-debug-line");
          frag.appendChild(row);
        }, () => {
          el.appendChild(frag);
          _pruneDetailsLog(el);
          if (window.debugStickBottom) el.scrollTop = el.scrollHeight;
          _scheduleDetailsConsoleStatus();
        }, scheduleFlush);
      });
    };

    const url = new URL("/api/logs/stream", document.baseURI);
    url.searchParams.set("tag", "DEBUG");
    if (debugReconnect) url.searchParams.set("since", String(debugSince));
    else url.searchParams.set("tail", String(_detailsLimit("DETAILS_STREAM_TAIL", 400)));
    url.searchParams.set("plain", "1");
    url.searchParams.set("_ts", String(Date.now()));

    const es = new EventSource(url.toString());
    window.esDebug = es;
    es.onopen = () => {
      tabDebug?.classList.add("connected");
      tabDebug?.classList.remove("stale");
      _updateDetailsConsoleStatus();
    };
    es.onmessage = (ev) => {
      tabDebug?.classList.add("connected");
      tabDebug?.classList.remove("stale");
      if (!ev?.data) return;
      lastMsgAt = Date.now();
      const seq = Number(ev?.lastEventId || 0) || 0;
      if (seq > 0) window._debugLastSeq = seq;
      _enqueueDetailsItem(window.debugBuf, ev.data, "debug");
      scheduleFlush();
    };
    es.addEventListener("ping", () => {
      lastMsgAt = Date.now();
      tabDebug?.classList.add("connected");
      tabDebug?.classList.remove("stale");
      _updateDetailsConsoleStatus();
    });
    es.onerror = () => {
      tabDebug?.classList.remove("connected");
      tabDebug?.classList.add("stale");
      _updateDetailsConsoleStatus();
      try { window.esDebug?.close?.(); } catch {}
      window.esDebug = null;

      if (window._debugRetryTO) clearTimeout(window._debugRetryTO);
      window._debugRetryTO = setTimeout(() => {
        if (_detailsVisible() && window._detailsTab === "debug") { try { openDebugLog(); } catch {} }
      }, 1200);
    };

    if (window._debugStaleIV) clearInterval(window._debugStaleIV);
    window._debugStaleIV = setInterval(() => {
      const stale = (Date.now() - lastMsgAt) > 20000;
      tabDebug?.classList.toggle("stale", stale);
      _updateDetailsConsoleStatus();
    }, 1000);

    if (window._debugVisibilityHandler) {
      document.removeEventListener("visibilitychange", window._debugVisibilityHandler);
    }
    window._debugVisibilityHandler = () => {
      if (document.visibilityState !== "visible") return;
      if (_detailsVisible() && window._detailsTab === "debug") { try { openDebugLog(); } catch {} }
    };
    document.addEventListener("visibilitychange", window._debugVisibilityHandler);
  } finally {
    window._debugOpening = false;
  }
}

async function openWatcherLog() {
  const el = document.getElementById("det-watch-log");
  const details = document.getElementById("details");
  const tabWatch = document.getElementById("det-tab-watcher");
  if (!el || !details || details.classList.contains("hidden") || _cwModalOpen() || window._detailsTab !== "watcher") return;
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

    if (!_detailsVisible() || window._detailsTab !== "watcher") return;
    const uniq = _watchLogTagsFromConfig(cfg);

    const url = new URL("/api/logs/watcher", document.baseURI);
    url.searchParams.set("tail", "200");
    url.searchParams.set("plain", "1");
    if (uniq.length) url.searchParams.set("tags", uniq.join(","));

    if (!el.__cwScrollWired) {
      el.addEventListener("scroll", () => {
        const pad = 12;
        window.watchStickBottom = el.scrollTop >= el.scrollHeight - el.clientHeight - pad;
        _updateDetailsConsoleStatus();
      }, { passive: true });
      el.__cwScrollWired = true;
    }

    window.watchBuf.length = 0;
    el.innerHTML = "";
    window.watchStickBottom = true;
    _resetDetailsDropped("watcher");
    let lastMsgAt = Date.now();

    const enqueue = (tag, html) => {
      if (!html) return;
      _enqueueDetailsItem(window.watchBuf, { tag, html }, "watcher");
      lastMsgAt = Date.now();
      scheduleFlush();
    };

    const scheduleFlush = () => {
      if (window._watchFlushRAF || window._detailsTab !== "watcher") return;
      window._watchFlushRAF = requestAnimationFrame(() => {
        window._watchFlushRAF = null;
        const frag = document.createDocumentFragment();
        _runDetailsBatch(window.watchBuf, (it) => {
          frag.appendChild(_structuredLogRow(it.html, it.tag));
        }, () => {
          el.appendChild(frag);
          _pruneDetailsLog(el);
          if (window.watchStickBottom) el.scrollTop = el.scrollHeight;
          _scheduleDetailsConsoleStatus();
        }, scheduleFlush);
      });
    };

    url.searchParams.set("_ts", String(Date.now()));
    const es = new EventSource(url.toString());
    window.esWatch = es;
    tabWatch?.classList.add("connected");
    tabWatch?.classList.remove("stale");
    _updateDetailsConsoleStatus();

    for (const t of (uniq.length ? uniq : _watchLogKnownTags())) {
      es.addEventListener(t, (ev) => enqueue(t, ev?.data));
    }

    es.addEventListener("ping", () => { lastMsgAt = Date.now(); });

    es.onerror = () => {
      tabWatch?.classList.remove("connected");
      tabWatch?.classList.add("stale");
      _updateDetailsConsoleStatus();
      try { window.esWatch?.close?.(); } catch {}
      window.esWatch = null;

      if (window._watchRetryTO) clearTimeout(window._watchRetryTO);
      window._watchRetryTO = setTimeout(() => {
        if (_detailsVisible() && window._detailsTab === "watcher") { try { openWatcherLog(); } catch {} }
      }, 1200);
    };

    if (window._watchStaleIV) clearInterval(window._watchStaleIV);
    window._watchStaleIV = setInterval(() => {
      const stale = (Date.now() - lastMsgAt) > 20000;
      tabWatch?.classList.toggle("stale", stale);
      _updateDetailsConsoleStatus();
    }, 1000);

    if (window._watchVisibilityHandler) {
      document.removeEventListener("visibilitychange", window._watchVisibilityHandler);
    }
    window._watchVisibilityHandler = () => {
      if (document.visibilityState !== "visible") return;
      if (_detailsVisible() && window._detailsTab === "watcher") { try { openWatcherLog(); } catch {} }
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
  if (!_detailsVisible() || _cwModalOpen() || window._detailsTab !== "sync" || window.esDet) return;
  const authSetupPending = () => window.cwIsAuthSetupPending?.() === true;
  const tabSync = document.getElementById("det-tab-sync");
  const openSeq = ++window._detOpenSeq;
  try { initDetailsTabs(); } catch {}

  try {
    const cfg = window._cfgCache || await fetch("/api/config", { cache: "no-store" }).then(r => r.json());
    if (openSeq !== window._detOpenSeq) return;
    window._cfgCache = cfg;
    window.appDebug = _isAppDebugMode(cfg);
  } catch (_) {}
  if (openSeq !== window._detOpenSeq || !_detailsVisible() || window._detailsTab !== "sync") return;

  el.innerHTML = "";
  el.classList?.remove("cf-log");
  el.classList?.add("cf-log-plain");
  window.detStickBottom = true;
  window.syncBuf.length = 0;
  _resetDetailsDropped("sync");
  window._detSeenLines = [];
  window._detReplayActive = false;
  window._detReplayCursor = 0;
  window._detDidConnectOnce = false;
  window._detLastSeq = 0;

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

  if (!el.__cwSyncScrollWired) {
    el.addEventListener("scroll", () => { updateSlider(); updateStick(); _scheduleDetailsConsoleStatus(); }, { passive: true });
    el.__cwSyncScrollWired = true;
  }

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
      const row = _structuredLogRow(_decodeLogLine(line), "SYNC");
      row.classList.add("det-plain-line");
      el.appendChild(row);
    }
  };

  let lastMsgAt = Date.now();
  let retryMs = 1000;
  const STALE_MS = 20000;

  const scheduleFlush = () => {
    if (window._syncFlushRAF || window._detailsTab !== "sync") return;
    window._syncFlushRAF = requestAnimationFrame(() => {
      window._syncFlushRAF = null;
      _runDetailsBatch(window.syncBuf, (line) => {
        appendRaw(line);
      }, () => {
        _pruneDetailsLog(el);
        if (window.detStickBottom) el.scrollTop = el.scrollHeight;
        updateSlider();
        _scheduleDetailsConsoleStatus();
      }, scheduleFlush);
    });
  };

  const connect = () => {
    if (authSetupPending() || !_streamsAllowed() || window._detailsTab !== "sync") return;
    try { window.esDet?.close(); } catch (_) {}
    const initialConnect = !window._detDidConnectOnce;
    if (!initialConnect) _beginDetailReplayFilter();
    const since = Math.max(0, Number(window._detLastSeq || 0) || 0);
    const url = new URL("/api/logs/stream", document.baseURI);
    url.searchParams.set("tag", "SYNC");
    if (!initialConnect && since > 0) url.searchParams.set("since", String(since));
    else url.searchParams.set("tail", String(_detailsLimit("DETAILS_STREAM_TAIL", 400)));
    url.searchParams.set("plain", "1");
    url.searchParams.set("_ts", String(Date.now()));
    window.esDet = new EventSource(url.toString());
    window.esDet.onopen = () => {
      window._detDidConnectOnce = true;
      tabSync?.classList.add("connected");
      tabSync?.classList.remove("stale");
      _updateDetailsConsoleStatus();
    };

    window.esDet.onmessage = (ev) => {
      lastMsgAt = Date.now();
      tabSync?.classList.add("connected");
      tabSync?.classList.remove("stale");
      if (!ev?.data) return;
      const seq = Number(ev?.lastEventId || 0) || 0;
      if (seq > 0) window._detLastSeq = seq;

      if (ev.data === "::CLEAR::") {
        el.textContent = "";
        window.syncBuf.length = 0;
        window._detSeenLines = [];
        window._detReplayActive = false;
        window._detReplayCursor = 0;
        _resetDetailsDropped("sync");
        updateSlider();
        _scheduleDetailsConsoleStatus();
        return;
      }

      if (_shouldSkipReplayedDetailLine(ev.data)) {
        retryMs = 1000;
        return;
      }

      _enqueueDetailsItem(window.syncBuf, ev.data, "sync");
      _rememberDetailLine(ev.data);
      scheduleFlush();
      retryMs = 1000;
    };

    window.esDet.addEventListener("ping", () => {
      lastMsgAt = Date.now();
      tabSync?.classList.add("connected");
      tabSync?.classList.remove("stale");
      _updateDetailsConsoleStatus();
    });

    window.esDet.onerror = () => {
        tabSync?.classList.remove("connected");
        tabSync?.classList.add("stale");
      _updateDetailsConsoleStatus();
      try { window.esDet?.close(); } catch (_) {}
      window.esDet = null;

      if (!window._detRetryTO) {
        if (authSetupPending() || window._detailsTab !== "sync" || !_detailsVisible()) return;
        window._detRetryTO = setTimeout(() => {
          window._detRetryTO = null;
          if (window._detailsTab === "sync" && _detailsVisible()) connect();
        }, retryMs);
        retryMs = Math.min(retryMs * 2, 15000);
      }
    };
  };

  connect();

  window._detStaleIV = setInterval(() => {
    tabSync?.classList.toggle("stale", (Date.now() - lastMsgAt) > STALE_MS);
    _updateDetailsConsoleStatus();
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
    if (window._detailsTab === "sync" && (!window.esDet || (Date.now() - lastMsgAt > STALE_MS))) connect();
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
  window._detReplayActive = false;
  window._detReplayCursor = 0;
  try { closeSyncLog(); } catch {}
  try { closeWatcherLog(); } catch {}
  try { closeDebugLog(); } catch {}
}

function toggleDetails() {
  const d = document.getElementById("details");
  if (!d) return;
  const isOpen = d.classList.contains("hidden");
  const layout = document.getElementById("layout");

  d.classList.toggle("hidden", !isOpen);
  layout?.classList.toggle("details-open", isOpen);
  if (isOpen) {
    try { initDetailsTabs(); } catch {}
    try { setDetailsTab(window._detailsTab || "sync"); } catch {}
  } else {
    closeDetailsLog();
  }
}

function resetDetailsSyncLog() {
  const el = document.getElementById("det-log");
  if (el) el.textContent = "";
  window.syncBuf.length = 0;
  if (window._syncFlushRAF) { cancelAnimationFrame(window._syncFlushRAF); window._syncFlushRAF = null; }
  window._detSeenLines = [];
  window._detReplayActive = false;
  window._detReplayCursor = 0;
  window._detDidConnectOnce = false;
  window._detLastSeq = 0;
  window.detStickBottom = true;
  _resetDetailsDropped("sync");
}

function _wireModalStreamPause() {
  if (window._cwModalStreamPauseWired || !document.body) return;
  window._cwModalStreamPauseWired = true;
  let wasOpen = _cwModalOpen();
  const obs = new MutationObserver(() => {
    const open = _cwModalOpen();
    if (open === wasOpen) return;
    wasOpen = open;
    if (open) {
      try { closeDetailsLog(); } catch {}
    } else if (_detailsVisible()) {
      try { setDetailsTab(window._detailsTab || "sync"); } catch {}
    }
  });
  obs.observe(document.body, { attributes: true, attributeFilter: ["data-cx-modal-open"] });
}

if (document.body) _wireModalStreamPause();
else document.addEventListener("DOMContentLoaded", _wireModalStreamPause, { once: true });

window.addEventListener("beforeunload", () => {
  try { closeDetailsLog(); } catch {}
});


  const DetailsLog = {
    setDetailsTab,
    initDetailsTabs,
    closeSyncLog,
    closeWatcherLog,
    openWatcherLog,
    closeDebugLog,
    openDebugLog,
    openDetailsLog,
    closeDetailsLog,
    resetDetailsSyncLog,
    toggleDetails,
  };

  (window.CW ||= {}).DetailsLog = DetailsLog;
  Object.assign(window, DetailsLog);
})();
