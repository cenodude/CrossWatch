/* captures.js - Provider captures (watchlist/ratings/history/progress/collection) */
/* CrossWatch - Captures/Snapshots page UI logic */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const authSetupPending = () => window.cwIsAuthSetupPending?.() === true;
  const isManagedUser = () => document.documentElement?.dataset?.cwRole === "user";
  const schedulerAvailable = () => !isManagedUser();
  let authRetryWired = false;

  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

function escapeHtml(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

  function _uiCaptureLabel(label) {
    const t = String(label || "").trim();
    if (!t) return "";
    const low = t.toLowerCase();
    if (low === "snapshot" || low === "snapshots" || low === "capture" || low === "captures") return "CAPTURE";
    return t;
  }

  const API = () => (window.CW && window.CW.API && window.CW.API.j) ? window.CW.API.j : async (u, opt) => {
    const r = await fetch(u, { cache: "no-store", ...(opt || {}) });
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  };

    function apiJson(url, opt = {}, timeoutMs = 180000) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    return fetch(url, { cache: "no-store", signal: ctrl.signal, ...(opt || {}) })
      .then(async (r) => {
        clearTimeout(t);
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
        return r.json();
      })
      .catch((e) => {
        clearTimeout(t);
        if (e && e.name === "AbortError") throw new Error("timeout");
        throw e;
      });
  }

const toast = (msg, ok = true) => {
    try { window.CW?.DOM?.showToast?.(msg, ok); } catch {}
    if (!window.CW?.DOM?.showToast) console.log(msg);
  };
  const POST_JSON = (url, body, timeoutMs) => apiJson(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) }, timeoutMs);
  function diffSelection() {
    return compareSelectionState();
  }

  const state = {
    providers: [], snapshots: [], selectedPath: "", selectedSnap: null, diffPick: [], diffResult: null,
    busy: false, captureBusy: false, lastRefresh: 0,
    listLimit: 8, showAll: false, expandedBundles: {}, scheduleQueue: [], _spinUntil: 0,
    captureProgressId: "", captureProgressTimer: null, restoreConfirmUntil: 0, restoreConfirmTimer: null, restoreConfirmRaf: 0,
    refreshBusy: false, lastSyncAt: null, lastRefreshFailed: false, syncClock: null,
  };

  const configuredProviderIds = () => new Set(
    (Array.isArray(state.providers) ? state.providers : [])
      .filter((p) => !!p?.configured)
      .map((p) => String(p.id || "").trim().toLowerCase())
      .filter(Boolean)
  );

  function _providerMetaById(pid) {
    const id = String(pid || "").trim().toLowerCase();
    const meta = window.CW?.ProviderMeta || {};
    const label = meta.label?.(id) || String(pid || "").trim() || "-";
    const icon = meta.logLogoPath?.(id) || meta.logoPath?.(id) || "";
    return { id, label, icon };
  }

  function _enhanceProviderIconSelect(sel) {
    const helper = window.CW?.ProfileSelect?.enhanceProvider;
    if (!sel || typeof helper !== "function") return;
    helper(sel, {
      className: "ss-icon-select",
      getOptionData: (value, option) => {
        const meta = _providerMetaById(value);
        return {
          label: String(option?.textContent || meta.label || "-").trim() || "-",
          icons: meta.icon && value ? [{ src: meta.icon, alt: meta.label || value }] : [],
          disabled: !!option?.disabled,
        };
      },
    });
  }

  function _enhanceProfileIconSelect(sel) {
    const helper = window.CW?.ProfileSelect?.enhanceProfile;
    if (!sel || typeof helper !== "function") return;
    helper(sel, { className: "ss-icon-select" });
  }

  function fmtTsFromStamp(stamp) {
    // stamp: 20260127T135959Z
    const m = String(stamp || "").match(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/);
    if (!m) return "";
    const d = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]));
    return d.toLocaleString();
  }

  function _findSnapByPath(path) {
  const rows = Array.isArray(state.snapshots) ? state.snapshots : [];
  const p = String(path || "");
  if (!p) return null;
  for (const s of rows) {
    if (s && String(s.path || "") === p) return s;
  }
  return null;
}

function _stampEpoch(stamp) {
  const m = String(stamp || "").match(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/);
  if (!m) return 0;
  return Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]);
}

function _diffScope() {
  const picks = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];
  if (picks.length !== 1) return null;
  if (!picks.length) return null;
  const s0 = _findSnapByPath(String(picks[0] || ""));
  if (!s0) return null;
  return {
    provider: String(s0.provider || "").toLowerCase(),
    instance: String(s0.instance || s0.instance_id || s0.profile || "default").toLowerCase(),
    feature: String(s0.feature || "").toLowerCase(),
  };
}

function _snapMatchesScope(s, scope) {
  if (!s || !scope) return true;
  const p = String(s.provider || "").toLowerCase();
  const i = String(s.instance || s.instance_id || s.profile || "default").toLowerCase();
  const f = String(s.feature || "").toLowerCase();
  return p === scope.provider && i === scope.instance && f === scope.feature;
}

function _diffPickAB() {
  const picks = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];
  if (picks.length !== 2) return { a: "", b: "", sa: null, sb: null };
  const p0 = String(picks[0] || "");
  const p1 = String(picks[1] || "");
  const s0 = _findSnapByPath(p0);
  const s1 = _findSnapByPath(p1);

  // Keep explicit UI selection order stable: first checked/dragged card is A, second is B.
  return { a: p0, b: p1, sa: s0, sb: s1 };
}

function clearDiffPicks() {
  state.diffPick = [];
  state.diffResult = null;
  try { renderList(); renderDiffPicked(); renderDiff(); updateDiffAvailability(); } catch {}
}

function toggleDiffPick(path, checked) {
  const p = String(path || "");
  if (!p) return;

  const snap = _findSnapByPath(p);
  const picks = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];

  if (checked) {
    if (!snap) return;
    if (!picks.includes(p)) {
      picks.push(p);
    }
  } else {
    const ix = picks.indexOf(p);
    if (ix !== -1) picks.splice(ix, 1);
  }

  state.diffPick = picks;

  if (picks.length !== 2) state.diffResult = null;

  renderList();
  renderDiffPicked();
  renderDiff();
  updateDiffAvailability();
}

function bundleKey(s) {
    const stamp = String((s && s.stamp) || "");
    const prov = String((s && s.provider) || "").toLowerCase();
    const inst = String((s && (s.instance || s.instance_id || s.profile)) || "default").toLowerCase();
    const label = String((s && s.label) || "").toLowerCase();
    return stamp + "|" + prov + "|" + inst + "|" + label;
  }

  function buildBundleIndex(allRows) {
    const bundlesByKey = {};
    const childrenByKey = {};
    (allRows || []).forEach((s) => {
      const feat = String((s && s.feature) || "").toLowerCase();
      if (feat !== "all") return;
      const k = bundleKey(s);
      if (k) bundlesByKey[k] = s;
    });
    (allRows || []).forEach((s) => {
      const feat = String((s && s.feature) || "").toLowerCase();
      if (feat === "all") return;
      const k = bundleKey(s);
      if (!k || !bundlesByKey[k]) return;
      if (!childrenByKey[k]) childrenByKey[k] = [];
      childrenByKey[k].push(s);
    });
    return { bundlesByKey, childrenByKey };
  }

  function humanBytes(n) {
    const v = Number(n || 0);
    if (!isFinite(v) || v <= 0) return "0 B";
    const u = ["B", "KB", "MB", "GB"];
    let i = 0, x = v;
    while (x >= 1024 && i < u.length - 1) { x /= 1024; i++; }
    return `${x.toFixed(i === 0 ? 0 : 1)} ${u[i]}`;
  }

  function snapFile(path) {
    return String(path || "").split(/[\\/]/).pop() || "";
  }

  function snapshotOverview() {
    const snaps = Array.isArray(state.snapshots) ? state.snapshots.filter(Boolean) : [];
    const idx = buildBundleIndex(snaps);
    const hiddenChildPaths = new Set();

    Object.values(idx.childrenByKey || {}).forEach((kids) => {
      (kids || []).forEach((s) => {
        if (s && s.path) hiddenChildPaths.add(String(s.path));
      });
    });

    const configuredProviders = (Array.isArray(state.providers) ? state.providers : []).filter((p) => p && p.configured !== false);
    const providerIds = new Set(
      (configuredProviders.length ? configuredProviders : (Array.isArray(state.providers) ? state.providers : []))
        .map((p) => String(p?.id || p?.label || "").trim().toUpperCase())
        .filter(Boolean)
    );

    if (!providerIds.size) {
      snaps.forEach((s) => {
        const prov = String(s?.provider || "").trim().toUpperCase();
        if (prov) providerIds.add(prov);
      });
    }

    const total = snaps.filter((s) => s && !hiddenChildPaths.has(String(s.path || ""))).length;
    const fullSets = Object.keys(idx.bundlesByKey || {}).length;
    const latest = snaps.slice().sort((a, b) => {
      const am = Number(a?.mtime || 0) || (_stampEpoch(a?.stamp) / 1000);
      const bm = Number(b?.mtime || 0) || (_stampEpoch(b?.stamp) / 1000);
      return bm - am;
    })[0] || null;
    const scheduledJobs = countScheduledCaptureJobs();
    return { total, providers: providerIds.size, fullSets, latest, scheduledJobs };
  }

  function countScheduledCaptureJobs() {
    const queued = Array.isArray(state.scheduleQueue) ? state.scheduleQueue.length : 0;
    const draft = Array.isArray(window.__cwCaptureSchedulerDraftJobs) ? window.__cwCaptureSchedulerDraftJobs : [];
    const pending = Array.isArray(window.__cwCaptureSchedulerPrefillQueue) ? window.__cwCaptureSchedulerPrefillQueue : [];
    return queued + draft.filter(Boolean).length + pending.filter(Boolean).length;
  }

  function snapInstance(snap) {
    return String(snap?.instance || snap?.instance_id || snap?.profile || "default");
  }

  function snapCreatedText(snap) {
    if (!snap) return "-";
    if (snap.stamp) return fmtTsFromStamp(snap.stamp) || "-";
    if (snap.created_at) return new Date(String(snap.created_at)).toLocaleString();
    if (snap.mtime) return new Date(Number(snap.mtime || 0) * 1000).toLocaleString();
    return "-";
  }

  function snapCreatedMinuteText(snap) {
    let d = null;
    if (snap?.stamp) {
      const ms = _stampEpoch(snap.stamp);
      d = ms ? new Date(ms) : null;
    } else if (snap?.created_at) {
      d = new Date(String(snap.created_at));
    } else if (snap?.mtime) {
      d = new Date(Number(snap.mtime || 0) * 1000);
    }
    if (!d || Number.isNaN(d.getTime())) return snapCreatedText(snap);
    return d.toLocaleString(undefined, {
      year: "numeric",
      month: "numeric",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  }

  function snapItemCount(snap) {
    const stats = snap?.stats || {};
    const count = stats.count ?? stats.total ?? snap?.count;
    return Number(count || 0);
  }

  function snapTypeLabel(snap) {
    const feature = String(snap?.feature || "").toLowerCase();
    return feature === "all" ? "Full set" : "Feature";
  }

  function compareSelectionState() {
    const pick = _diffPickAB();
    const selected = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];
    const result = { ...pick, selected, ok: false, reason: "" };
    if (selected.length !== 2) {
      result.reason = selected.length
        ? `Comparison needs exactly two captures. ${selected.length} selected.`
        : "Select exactly two captures to enable comparison.";
      return result;
    }
    if (!pick.sa || !pick.sb) {
      result.reason = "One selected capture is no longer available.";
      return result;
    }
    const pa = String(pick.sa.provider || "").toLowerCase();
    const pb = String(pick.sb.provider || "").toLowerCase();
    const ia = snapInstance(pick.sa).toLowerCase();
    const ib = snapInstance(pick.sb).toLowerCase();
    const fa = String(pick.sa.feature || "").toLowerCase();
    const fb = String(pick.sb.feature || "").toLowerCase();
    if (pa !== pb) {
      result.reason = "Selected captures use different providers.";
      return result;
    }
    if (ia !== ib) {
      result.reason = "Selected captures use different profiles.";
      return result;
    }
    if (fa !== fb) {
      result.reason = "Selected captures use different features.";
      return result;
    }
    result.ok = !!pick.a && !!pick.b && pick.a !== pick.b;
    result.reason = result.ok
      ? `Comparison enabled for ${(pick.sa.provider || "-").toUpperCase()} / ${fa || "-"} / ${snapInstance(pick.sa)}.`
      : "Select two different captures.";
    return result;
  }

  function updateTopStats() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const overview = snapshotOverview();
    const setStat = (name, value) => {
      const el = page.querySelector(`[data-stat="${name}"] strong`);
      if (el) el.textContent = String(value);
    };
    const setSub = (name, value) => {
      const el = page.querySelector(`[data-stat="${name}"] .ss-summary-sub`);
      if (el) el.textContent = String(value);
    };
    setStat("captures", overview.total);
    setStat("providers", overview.providers);
    setStat("full-sets", overview.fullSets);
    setStat("latest", overview.latest ? snapCreatedMinuteText(overview.latest) : "-");
    setSub("latest", overview.latest ? `${String(overview.latest.provider || "-").toUpperCase()} / ${String(overview.latest.feature || "-").toLowerCase()}` : "No captures yet");
    setStat("scheduled", overview.scheduledJobs);
    setSub("scheduled", overview.scheduledJobs ? "Queued or drafted" : "No queued jobs");
  }

  function render() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    delete page.dataset.diffControlsWired;

    const overview = snapshotOverview();
    const latest = overview.latest;
    const latestMain = latest ? snapCreatedMinuteText(latest) : "-";
    const latestSub = latest ? `${String(latest.provider || "-").toUpperCase()} / ${String(latest.feature || "-").toLowerCase()}` : "No captures yet";
    page.innerHTML = `
      <div class="ss-top cw-page-hero cw-page-hero-captures" data-hero-icon="database">
        <div class="ss-top-copy cw-page-hero-copy">
          <div class="cw-page-hero-kicker">CAPTURES</div>
          <div class="ss-title cw-page-hero-title">Captures</div>
          <div class="ss-sub cw-page-hero-sub">Save snapshots of your provider data, compare changes, and restore whenever needed.</div>
        </div>
        <div class="ss-actions cw-page-hero-actions ss-hero-summary" id="ss-hero-summary" aria-label="Capture sync summary">
          <div class="ss-hero-seg ss-hero-sync"><span>Synced</span><strong id="ss-sync-time">not yet</strong></div>
          <button id="ss-refresh" class="ss-hero-refresh" title="Refresh captures" aria-label="Refresh captures"><span id="ss-refresh-icon" class="material-symbol ss-refresh-icon">refresh</span></button>
        </div>
      </div>
      <div class="ss-topstats">
        ${summaryCard("database", "Total captures", overview.total, "Across all providers", "captures")}
        ${summaryCard("groups", "Providers", overview.providers, "Active providers", "providers")}
        ${summaryCard("layers", "Full sets", overview.fullSets, "Complete snapshots", "full-sets")}
        ${summaryCard("calendar_month", "Latest capture", latestMain, latestSub, "latest")}
        ${summaryCard("schedule", "Scheduled jobs", overview.scheduledJobs, overview.scheduledJobs ? "Queued or drafted" : "No queued jobs", "scheduled")}
      </div>
      <div class="ss-wrap">
        <div class="ss-col">
          <div class="ss-card ss-accent">
            <div class="ss-card-head">
              <div class="ss-headcopy">
                <h3>Create capture</h3>
                <div class="ss-headsub">Choose the source, set capture options, then create now or queue for scheduler.</div>
              </div>
            </div>
            <div class="ss-create-flow">
              <div class="ss-stepper">
                <div class="ss-step on"><span>1</span><b>Source</b></div>
                <div class="ss-step"><span>2</span><b>Options</b></div>
                <div class="ss-step"><span>3</span><b>Review</b></div>
              </div>
              <div class="ss-create-fields">
                <label>Provider<div class="ss-field"><select id="ss-prov"></select></div></label>
                <label>Profile<div class="ss-field"><select id="ss-prov-inst" class="input grow"></select></div></label>
                <label>Capture type<div class="ss-field"><select id="ss-feature"></select></div></label>
                <label>Optional label<div class="ss-field"><input id="ss-label" maxlength="12" placeholder="e.g. Maint" /></div><small>Max 12 characters</small></label>
              </div>
              <div id="ss-create-review" class="ss-selected-card ss-small"></div>
              <div class="ss-create-actions">
                <button id="ss-create" class="btn primary" type="button"><span class="material-symbols-rounded" aria-hidden="true">bolt</span><span class="ss-btn-label">Create now</span></button>
                <button id="ss-add-schedule" class="btn" type="button"><span class="material-symbols-rounded" aria-hidden="true">schedule</span><span class="ss-btn-label">Queue for scheduler</span></button>
              </div>
            </div>
            <div id="ss-schedule-queue-wrap" class="ss-queue hidden">
              <div class="ss-queue-head">
                <div>
                  <div class="ss-queue-title">Scheduler queue</div>
                  <div class="ss-queue-note">Queue capture schedules here, then send them to Advanced Scheduling.</div>
                </div>
                <div class="ss-queue-actions">
                  <button id="ss-send-schedule-queue" class="btn" type="button">Send queue</button>
                  <button id="ss-clear-schedule-queue" class="btn" type="button">Clear queue</button>
                </div>
              </div>
              <div id="ss-queue-feedback" class="ss-small ss-muted" style="display:none;padding:9px 10px;border-radius:12px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.03)"></div>
              <div id="ss-schedule-queue" class="ss-queue-list"></div>
            </div>
          </div>
        </div>
        <div class="ss-center-col">
          <div class="ss-card ss-browser-card ss-lockable">
            <div class="ss-card-head ss-list-head">
              <div class="ss-headcopy">
                <h3>Capture browser</h3>
                <div class="ss-headsub">Tick a row to load it for restore and select it for compare and tools. Clicking a row loads it too.</div>
              </div>
            </div>
            <div class="ss-toolbar">
              <div class="ss-filterbar">
                <div id="ss-filter-provider-wrap" class="ss-filtermini"><select id="ss-filter-provider" class="input"></select></div>
                <div id="ss-filter-feature-wrap" class="ss-filtermini"><select id="ss-filter-feature" class="input"></select></div>
                <div id="ss-filter-kind-wrap" class="ss-filtermini"><select id="ss-filter-kind" class="input"></select></div>
                <button id="ss-filter-clear" class="btn ss-filterclear" type="button"><span class="material-symbols-rounded">filter_list</span>Filters</button>
              </div>
              <div id="ss-capture-lock" class="ss-lockmsg hidden"><span class="material-symbol">hourglass_top</span><div>Creating capture... browser, restore, compare, and tools unlock after refresh.</div></div>
            </div>
            <div id="ss-list" class="ss-list"></div>
            <div id="ss-list-footer" class="ss-row" style="justify-content:space-between;margin:10px 0 14px"></div>
          </div>
        </div>
        <div class="ss-col">
          <div class="ss-card ss-lockable" data-coll="restore">
            <div class="ss-card-head">
              <div class="ss-headcopy">
                <h3>Restore capture</h3>
                <div class="ss-headsub">Restore stays visible so the target and method are always clear.</div>
              </div>
            </div>
            <div id="ss-selected" class="ss-selected-card ss-selected-empty">Pick a capture from the browser.</div>
            <div class="ss-target-label">Target profile</div>
            <div class="ss-row"><select id="ss-restore-inst" class="input grow"></select></div>
            <div class="ss-restore-modebar">
              <div class="ss-restore-modes">
                <button class="ss-modebtn active" type="button" data-restore-mode="merge"><strong>Merge missing</strong><span>Only add items</span></button>
                <button class="ss-modebtn" type="button" data-restore-mode="clear_restore"><strong>Replace</strong><span>Match capture</span></button>
              </div>
              <select id="ss-restore-mode" class="input hidden" aria-hidden="true"><option value="merge">Merge missing only</option><option value="clear_restore">Replace exactly</option></select>
              <div id="ss-restore-warning" class="ss-restore-warning hidden">Replace exactly clears the target feature first, then restores this capture.</div>
            </div>
            <div class="ss-row" style="margin-top:12px"><button id="ss-restore" class="btn danger cw-danger-confirm" style="width:100%" type="button"><span class="material-symbols-rounded" aria-hidden="true">restore</span><span>Restore capture</span></button></div>
            <div id="ss-restore-progress" class="ss-progress hidden"><div class="ss-pbar"></div><div class="ss-plabel">Working...</div></div>
            <div id="ss-restore-out" class="ss-small ss-muted" style="margin-top:10px"></div>
          </div>
          <div class="ss-card ss-lockable" data-coll="compare">
            <div class="ss-card-head">
              <div class="ss-headcopy">
                <h3>Compare captures</h3>
                <div class="ss-headsub">Comparison requires exactly two compatible captures.</div>
              </div>
            </div>
            <div class="ss-compare-panel">
              <div id="ss-compare-state" class="ss-compare-state"></div>
              <div id="ss-diff-picked" class="ss-picked ss-compare-picks"></div>
              <div class="ss-compare-actions">
                <button id="ss-diff-run" class="btn" type="button">Compare Captures</button>
              </div>
              <div id="ss-diff-out" class="ss-muted ss-small"></div>
            </div>
          </div>
          <div class="ss-card ss-lockable" data-coll="tools">
            <div class="ss-card-head">
              <div class="ss-headcopy">
                <h3>Tools</h3>
                <div class="ss-headsub">Secondary actions for selected captures.</div>
              </div>
            </div>
            <div class="ss-tools-list">
              <button id="ss-export-selected" class="ss-tool-btn" type="button"><span class="material-symbols-rounded">download</span><span class="ss-tool-main"><span class="ss-tool-title">Export selected</span><span class="ss-tool-sub">Download selected capture data</span></span><span class="material-symbols-rounded">chevron_right</span></button>
              <button id="ss-delete" class="ss-tool-btn danger" type="button"><span class="material-symbols-rounded">delete</span><span class="ss-tool-main"><span class="ss-tool-title">Delete selected</span><span class="ss-tool-sub">Remove selected captures</span></span><span class="material-symbols-rounded">chevron_right</span></button>
              <button id="ss-cleanup-old" class="ss-tool-btn danger" type="button"><span class="material-symbols-rounded">auto_delete</span><span class="ss-tool-main"><span class="ss-tool-title">Cleanup Captures</span><span class="ss-tool-sub">Remove all stored captures</span></span><span class="material-symbols-rounded">chevron_right</span></button>
              <button id="ss-view-details" class="ss-tool-btn" type="button"><span class="material-symbols-rounded">info</span><span class="ss-tool-main"><span class="ss-tool-title">View details</span><span class="ss-tool-sub">Show technical details</span></span><span class="material-symbols-rounded">chevron_right</span></button>
            </div>
            <div id="ss-tools-progress" class="ss-progress hidden"><div class="ss-pbar"></div><div class="ss-plabel">Working...</div></div>
            <div id="ss-tools-out" class="ss-small ss-muted hidden" style="margin-top:10px"></div>
            <div id="ss-detail-out" class="ss-code ss-detail-pre hidden"></div>
          </div>
        </div>
      </div>
      <div id="ss-capture-modal" class="ss-capture-modal hidden" role="dialog" aria-modal="true" aria-labelledby="ss-capture-modal-title">
        <div class="ss-capture-dialog">
          <div class="ss-capture-modal-head">
            <div class="ss-capture-icon"><span class="material-symbols-rounded">database_upload</span></div>
            <div>
              <div id="ss-capture-modal-title" class="ss-capture-title">Creating capture</div>
              <div id="ss-capture-modal-sub" class="ss-capture-sub">Preparing provider capture...</div>
            </div>
          </div>
          <div class="ss-capture-meter" aria-hidden="true"><div id="ss-capture-meter-fill" class="ss-capture-meter-fill"></div></div>
          <div class="ss-capture-progress-line"><span id="ss-capture-percent">0%</span><b id="ss-capture-stage">Starting</b></div>
          <div class="ss-capture-grid">
            <div><span>Provider</span><b id="ss-capture-provider">-</b></div>
            <div><span>Profile</span><b id="ss-capture-instance">default</b></div>
            <div><span>Feature</span><b id="ss-capture-feature">-</b></div>
            <div><span>Items</span><b id="ss-capture-items">-</b></div>
          </div>
          <div id="ss-capture-message" class="ss-capture-message">Starting capture...</div>
          <div id="ss-capture-actions" class="ss-capture-actions">
            <button id="ss-capture-ack" class="btn" type="button">OK</button>
          </div>
        </div>
      </div>`;

    updateCaptureBusyUI();

    $("#ss-refresh", page)?.addEventListener("click", () => {
      if (state.refreshBusy) return;
      state._spinUntil = Date.now() + 550;
      setRefreshSpinning(true);
      refresh(true, true);
      setTimeout(() => { if (!state.busy) setRefreshSpinning(false); }, 600);
    });
    $("#ss-capture-ack", page)?.addEventListener("click", () => {
      hideCaptureProgressModal(0);
      window.setTimeout(() => refresh(true, false), 80);
    });
    $("#ss-create", page)?.addEventListener("click", () => onCreate());
    const addScheduleBtn = $("#ss-add-schedule", page);
    if (schedulerAvailable()) addScheduleBtn?.addEventListener("click", () => onAddToScheduler());
    else addScheduleBtn?.remove();
    $("#ss-send-schedule-queue", page)?.addEventListener("click", () => onSendScheduleQueue());
    $("#ss-clear-schedule-queue", page)?.addEventListener("click", () => {
      state.scheduleQueue = [];
      renderScheduleQueue();
      setScheduleQueueFeedback("Queue cleared", true);
    });
    $("#ss-prov", page)?.addEventListener("change", () => { repopFeatures(); repopCreateInstances(); updateCreateReview(); });
    $("#ss-prov-inst", page)?.addEventListener("change", () => updateCreateReview());
    $("#ss-feature", page)?.addEventListener("change", () => updateCreateReview());
    $("#ss-label", page)?.addEventListener("input", (ev) => {
      const el = ev.currentTarget;
      const next = String(el?.value || "").slice(0, 12);
      if (el && el.value !== next) el.value = next;
      updateCreateReview();
    });
    $("#ss-filter-provider", page)?.addEventListener("change", () => { state.showAll = false; updateBrowserFilterBar(); renderList(); });
    $("#ss-filter-feature", page)?.addEventListener("change", () => { state.showAll = false; updateBrowserFilterBar(); renderList(); });
    $("#ss-filter-kind", page)?.addEventListener("change", () => { state.showAll = false; updateBrowserFilterBar(); renderList(); });
    $("#ss-filter-clear", page)?.addEventListener("click", () => {
      const provSel = $("#ss-filter-provider", page);
      const featSel = $("#ss-filter-feature", page);
      const kindSel = $("#ss-filter-kind", page);
      const resetSelect = (sel) => {
        if (!sel) return;
        sel.selectedIndex = 0;
        sel.dispatchEvent(new Event("change", { bubbles: true }));
      };
      resetSelect(provSel);
      resetSelect(featSel);
      resetSelect(kindSel);
      state.showAll = false;
      updateBrowserFilterBar();
      renderList();
    });

    $("#ss-restore", page)?.addEventListener("click", () => onRestore());
    $("#ss-delete", page)?.addEventListener("click", () => onDeleteSelected());
    $("#ss-export-selected", page)?.addEventListener("click", () => onExportSelected());
    $("#ss-cleanup-old", page)?.addEventListener("click", () => onCleanupOldCaptures());
    $("#ss-view-details", page)?.addEventListener("click", () => onViewDetails());
    $("#ss-restore-inst", page)?.addEventListener("change", () => { resetRestoreConfirm(); updateRestoreAvailability(); });
    $("#ss-restore-mode", page)?.addEventListener("change", () => updateRestoreModeUI());
    $$("[data-restore-mode]", page).forEach((btn) => btn.addEventListener("click", () => {
      resetRestoreConfirm();
      const sel = $("#ss-restore-mode", page);
      const mode = String(btn.getAttribute("data-restore-mode") || "merge");
      if (sel) sel.value = mode;
      updateRestoreModeUI();
    }));
    updateRestoreAvailability();
    updateRestoreModeUI();

    wireDiffControls();
    renderScheduleQueue();
    updateCreateReview();
  }

  function summaryCard(icon, label, value, sub, stat) {
    return `<div class="ss-summary-card" data-stat="${escapeHtml(stat)}"><div class="ss-summary-icon"><span class="material-symbols-rounded">${escapeHtml(icon)}</span></div><div class="ss-summary-copy"><div class="ss-summary-label">${escapeHtml(label)}</div><div class="ss-summary-value"><strong>${escapeHtml(value)}</strong></div><div class="ss-summary-sub">${escapeHtml(sub)}</div></div></div>`;
  }

  function setProgress(sel, on, label, tone) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const el = $(sel, page);
    if (!el) return;
    const lab = $(".ss-plabel", el);
    if (lab) lab.textContent = label || "Working...";
    el.style.setProperty("--pcol", tone === "danger" ? "var(--danger)" : "var(--accent)");
    el.classList.toggle("hidden", !on);
  }

  function newCaptureProgressId() {
    try {
      if (window.crypto?.randomUUID) return window.crypto.randomUUID();
    } catch {}
    return `capture-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  }

  function stopCaptureProgressPoll() {
    if (state.captureProgressTimer) {
      clearTimeout(state.captureProgressTimer);
      state.captureProgressTimer = null;
    }
  }

  function resetRestoreConfirm() {
    const page = document.getElementById("page-snapshots");
    state.restoreConfirmUntil = 0;
    if (state.restoreConfirmTimer) {
      clearTimeout(state.restoreConfirmTimer);
      state.restoreConfirmTimer = null;
    }
    if (state.restoreConfirmRaf) {
      cancelAnimationFrame(state.restoreConfirmRaf);
      state.restoreConfirmRaf = 0;
    }
    const btn = page ? $("#ss-restore", page) : null;
    if (!btn) return;
    btn.classList.remove("is-confirming");
    btn.style.removeProperty("--ss-confirm-progress");
    btn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">restore</span><span>Restore capture</span>`;
  }

  function armRestoreConfirm() {
    const page = document.getElementById("page-snapshots");
    const btn = page ? $("#ss-restore", page) : null;
    state.restoreConfirmUntil = Date.now() + 4200;
    if (!btn) return;
    btn.classList.add("is-confirming");
    btn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">warning</span><span>Confirm restore</span>`;
    if (state.restoreConfirmTimer) clearTimeout(state.restoreConfirmTimer);
    if (state.restoreConfirmRaf) cancelAnimationFrame(state.restoreConfirmRaf);
    const tick = () => {
      const remaining = Math.max(0, state.restoreConfirmUntil - Date.now());
      const pct = Math.max(0, Math.min(100, (remaining / 4200) * 100));
      btn.style.setProperty("--ss-confirm-progress", `${pct.toFixed(2)}%`);
      if (remaining > 0 && btn.classList.contains("is-confirming")) {
        state.restoreConfirmRaf = requestAnimationFrame(tick);
      } else {
        state.restoreConfirmRaf = 0;
      }
    };
    tick();
    state.restoreConfirmTimer = setTimeout(resetRestoreConfirm, 4200);
  }

  function captureStageLabel(stage) {
    const key = String(stage || "").toLowerCase();
    if (key === "queued") return "Queued";
    if (key === "waiting") return "Waiting";
    if (key === "reading") return "Reading provider";
    if (key === "planning") return "Planning changes";
    if (key === "clearing") return "Clearing target";
    if (key === "restoring") return "Restoring items";
    if (key === "normalizing") return "Normalizing items";
    if (key === "writing") return "Writing capture";
    if (key === "feature_done") return "Feature complete";
    if (key === "bundling") return "Bundling full set";
    if (key === "done") return "Done";
    if (key === "error") return "Failed";
    return "Starting";
  }

  function captureFeatureLabel(feature) {
    const raw = String(feature || "").trim().toLowerCase();
    if (!raw) return "-";
    return raw === "all" ? "All features" : raw.charAt(0).toUpperCase() + raw.slice(1);
  }

  function captureItemText(progress) {
    if (String(progress?.operation || "").toLowerCase() === "restore") {
      const added = Number(progress?.added);
      const removed = Number(progress?.removed);
      const hasAdded = Number.isFinite(added) && added > 0;
      const hasRemoved = Number.isFinite(removed) && removed > 0;
      if (hasAdded || hasRemoved) return `${hasAdded ? added.toLocaleString() : "0"} added, ${hasRemoved ? removed.toLocaleString() : "0"} removed`;
    }
    const done = Number(progress?.items_done ?? progress?.total_items);
    const featureItems = Number(progress?.feature_items);
    const hasDone = Number.isFinite(done) && done > 0;
    const hasFeature = Number.isFinite(featureItems) && featureItems > 0;
    if (hasDone && hasFeature && done !== featureItems) return `${done.toLocaleString()} total, ${featureItems.toLocaleString()} current`;
    if (hasDone) return `${done.toLocaleString()} items`;
    if (hasFeature) return `${featureItems.toLocaleString()} current`;
    const seen = Number(progress?.activity_seen);
    if (Number.isFinite(seen) && seen > 0) return `${seen.toLocaleString()} seen`;
    if (String(progress?.stage || "").toLowerCase() === "reading") return "Counting...";
    return "-";
  }

  function updateCaptureProgressModal(progress) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const modal = $("#ss-capture-modal", page);
    if (!modal) return;
    const data = progress || {};
    const percent = Math.max(0, Math.min(100, Math.round(Number(data.percent || 0))));
    const currentFeature = String(data.current_feature || data.feature || "").trim();
    const featureTotal = Number(data.feature_total || 1);
    const featureIndex = Number(data.feature_index || 0);
    const sub = featureTotal > 1 && featureIndex > 0
      ? `Feature ${featureIndex} of ${featureTotal}`
      : "Single capture";

    $("#ss-capture-meter-fill", page)?.style.setProperty("width", `${percent}%`);
    const pct = $("#ss-capture-percent", page);
    if (pct) pct.textContent = `${percent}%`;
    const stageKey = String(data.stage || "").toLowerCase();
    const operation = String(data.operation || "capture").toLowerCase();
    const restore = operation === "restore";
    const failed = (!!data.done && data.ok === false) || stageKey === "error";
    const stage = $("#ss-capture-stage", page);
    if (stage) stage.textContent = captureStageLabel(data.stage);
    const title = $("#ss-capture-modal-title", page);
    if (title) title.textContent = failed ? (restore ? "Restore failed" : "Capture failed") : data.done ? (restore ? "Restore complete" : "Capture complete") : (restore ? "Restoring capture" : "Creating capture");
    const subtitle = $("#ss-capture-modal-sub", page);
    if (subtitle) subtitle.textContent = sub;
    const provider = $("#ss-capture-provider", page);
    if (provider) provider.textContent = String(data.provider || "-").toUpperCase();
    const instance = $("#ss-capture-instance", page);
    if (instance) instance.textContent = String(data.instance || "default");
    const feature = $("#ss-capture-feature", page);
    if (feature) feature.textContent = captureFeatureLabel(currentFeature);
    const items = $("#ss-capture-items", page);
    if (items) items.textContent = captureItemText(data);
    const msg = $("#ss-capture-message", page);
    if (msg) msg.textContent = String(data.message || "Working...");
    const actions = $("#ss-capture-actions", page);
    if (actions) actions.classList.toggle("show", !!data.done || failed);
    modal.classList.toggle("is-error", failed);
    modal.classList.toggle("is-done", !!data.done && data.ok !== false);
    modal.classList.toggle("is-active", !data.done && !failed);
  }

  function showCaptureProgressModal(seed) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const modal = $("#ss-capture-modal", page);
    if (!modal) return;
    modal.classList.remove("hidden", "is-error", "is-done");
    $("#ss-capture-actions", page)?.classList.remove("show");
    updateCaptureProgressModal({
      ok: true,
      done: false,
      operation: seed?.operation || "capture",
      stage: "starting",
      message: seed?.operation === "restore" ? "Preparing restore..." : "Preparing capture...",
      percent: 3,
      provider: seed?.provider || "",
      instance: seed?.instance || "default",
      feature: seed?.feature || "",
      current_feature: seed?.feature === "all" ? "" : seed?.feature,
      feature_index: 0,
      feature_total: seed?.feature === "all" ? 4 : 1,
      items_done: 0,
    });
  }

  function hideCaptureProgressModal(delay = 900) {
    const page = document.getElementById("page-snapshots");
    stopCaptureProgressPoll();
    if (!page) return;
    window.setTimeout(() => {
      const modal = $("#ss-capture-modal", page);
      if (modal) modal.classList.add("hidden");
      $("#ss-capture-actions", page)?.classList.remove("show");
      state.captureProgressId = "";
    }, delay);
  }

  function sleep(ms) {
    return new Promise((resolve) => window.setTimeout(resolve, ms));
  }

  async function waitForCaptureProgress(progressId) {
    stopCaptureProgressPoll();
    const id = String(progressId || "");
    if (!id) return null;
    let misses = 0;
    while (state.captureProgressId === id) {
      try {
        const r = await API()(`/api/snapshots/capture-progress/${encodeURIComponent(id)}?_=${Date.now()}`);
        const progress = r && r.progress ? r.progress : null;
        misses = 0;
        if (progress) {
          updateCaptureProgressModal(progress);
          if (progress.done) return progress;
        }
      } catch (e) {
        misses += 1;
        updateCaptureProgressModal({ stage: "waiting", message: "Waiting for progress update...", percent: 8 });
        if (misses >= 8) throw e;
      }
      await sleep(700);
    }
    return null;
  }

  function pollCaptureProgress(progressId) {
    stopCaptureProgressPoll();
    const id = String(progressId || "");
    if (!id) return;
    const tick = async () => {
      if (!state.captureProgressId || state.captureProgressId !== id) return;
      try {
        const r = await API()(`/api/snapshots/capture-progress/${encodeURIComponent(id)}?_=${Date.now()}`);
        const progress = r && r.progress ? r.progress : null;
        if (progress) {
          updateCaptureProgressModal(progress);
          if (progress.done) return;
        }
      } catch (e) {
        updateCaptureProgressModal({ stage: "waiting", message: "Waiting for progress update...", percent: 8 });
      }
      state.captureProgressTimer = window.setTimeout(tick, 650);
    };
    tick();
  }

  function selectedCapturePaths() {
    const picks = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];
    if (picks.length) return picks.slice();
    return state.selectedPath ? [String(state.selectedPath)] : [];
  }

  function updateCreateReview() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const host = $("#ss-create-review", page);
    if (!host) return;
    const provider = String($("#ss-prov", page)?.value || "").toUpperCase() || "-";
    const instance = String($("#ss-prov-inst", page)?.value || "default") || "default";
    const feature = String($("#ss-feature", page)?.value || "").toLowerCase() || "-";
    const label = String($("#ss-label", page)?.value || "").trim().slice(0, 12) || "No label";
    host.innerHTML = `
      <div class="ss-review-line"><span>Source</span><b>${escapeHtml(provider)} / ${escapeHtml(instance)}</b></div>
      <div class="ss-review-line"><span>Feature</span><b>${escapeHtml(feature === "all" ? "All features" : feature)}</b></div>
      <div class="ss-review-line"><span>Label</span><b>${escapeHtml(label)}</b></div>
    `;
    updateCreateActionsAvailability();
  }

  function updateCreateActionsAvailability() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const provider = String($("#ss-prov", page)?.value || "").trim();
    const feature = String($("#ss-feature", page)?.value || "").trim();
    const enabled = !!provider && !!feature && !state.busy && !state.captureBusy;
    ["#ss-create", "#ss-add-schedule"].forEach((id) => {
      const btn = $(id, page);
      if (!btn) return;
      btn.disabled = !enabled;
      btn.title = enabled ? "" : "Pick a provider first";
    });
  }

  function updateSelectionToolsAvailability() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const selected = selectedCapturePaths();
    const set = (id, enabled, title) => {
      const btn = $(id, page);
      if (!btn) return;
      btn.disabled = !!state.busy || !enabled;
      btn.title = enabled ? "" : title;
    };
    set("#ss-export-selected", selected.length > 0, "Select at least one capture");
    set("#ss-delete", selected.length > 0, "Select or load a capture");
    set("#ss-view-details", !!state.selectedSnap || selected.length > 0, "Select or load a capture");
  }

  function wireDiffControls() {
    const page = document.getElementById("page-snapshots");
    if (!page || page.dataset.diffControlsWired === "1") return;
    page.dataset.diffControlsWired = "1";
    const diffRun = $("#ss-diff-run", page);
    if (diffRun) diffRun.addEventListener("click", (e) => { e.preventDefault(); onDiffRun(); });
  }
  function setCollapsed(id, collapsed) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const card = page.querySelector(`.ss-card[data-coll="${id}"]`);
    const head = page.querySelector(`[data-coll-head="${id}"]`);
    const body = page.querySelector(`[data-coll-body="${id}"]`);
    if (!card || !head || !body) return;
    card.classList.toggle("is-collapsed", !!collapsed);
    body.classList.toggle("hidden", !!collapsed);
    head.setAttribute("aria-expanded", collapsed ? "false" : "true");
  }

  function wireCollapsible(id) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const card = page.querySelector(`.ss-card[data-coll="${id}"]`);
    const head = page.querySelector(`[data-coll-head="${id}"]`);
    const body = page.querySelector(`[data-coll-body="${id}"]`);
    if (!card || !head || !body) return;

    const toggle = () => {
      const collapsed = card.classList.toggle("is-collapsed");
      body.classList.toggle("hidden", collapsed);
      head.setAttribute("aria-expanded", collapsed ? "false" : "true");
    };

    head.addEventListener("click", (e) => { e.preventDefault(); toggle(); });
    head.addEventListener("keydown", (e) => {
      const k = e.key;
      if (k === "Enter" || k === " ") { e.preventDefault(); toggle(); }
    });
  }

  function updateRestoreAvailability() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const b = $("#ss-restore", page);
    const d = $("#ss-delete", page);
    const instSel = $("#ss-restore-inst", page);
    if (!b) return;
    const pid = String(state.selectedSnap?.provider || "").toUpperCase();
    const targetInst = String($("#ss-restore-inst", page)?.value || "default");
    const p = _providerById(pid);
    const instMeta = Array.isArray(p?.instances) ? p.instances.find((x) => String(x?.id || "") === targetInst) : null;
    const instOk = instMeta ? !!instMeta.configured : true;

    b.disabled = state.busy || !state.selectedPath || !instOk;
    b.title = !state.selectedPath ? "Select a snapshot first" : (!instOk ? "Target profile not configured" : "");
    if (d) {
      d.disabled = state.busy || !state.selectedPath;
      d.title = state.selectedPath ? "" : "Select a snapshot first";
    }

    if (instSel) {
      instSel.disabled = state.busy || !state.selectedPath || instSel.options.length <= 1;
    }
    $$("[data-restore-mode]", page).forEach((btn) => {
      btn.disabled = state.busy || !state.selectedPath;
    });
  }

  function updateRestoreModeUI() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const mode = String($("#ss-restore-mode", page)?.value || "merge").toLowerCase();
    $$("[data-restore-mode]", page).forEach((btn) => {
      btn.classList.toggle("active", String(btn.getAttribute("data-restore-mode") || "") === mode);
    });
    const warning = $("#ss-restore-warning", page);
    if (warning) warning.classList.toggle("hidden", mode !== "clear_restore");
  }

function repopDiffSelects() {
  renderDiffPicked();
  updateDiffAvailability();
  renderDiff();
}

function updateDiffAvailability() {
  const page = document.getElementById("page-snapshots");
  if (!page) return;
  const stateInfo = diffSelection();
  const { ok } = stateInfo;
  const hint = stateInfo.reason || "Pick two compatible captures";
  const count = (Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : []).length;
  const status = $("#ss-compare-state", page);
  if (status) {
    status.classList.toggle("ok", !!ok);
    status.innerHTML = `
      <span class="material-symbols-rounded" aria-hidden="true">${ok ? "compare_arrows" : "info"}</span>
      <div class="ss-compare-state-copy"><b>${ok ? "Ready to compare" : "Comparison disabled"}</b><span>${escapeHtml(hint)}</span></div>
      <div class="ss-compare-count"><strong>${count}</strong><span>selected</span></div>
    `;
  }
  [["#ss-diff-run", ok ? "Compare Captures" : hint]].forEach(([id, title]) => {
    const el = $(id, page);
    if (!el) return;
    el.disabled = state.busy || !ok;
    el.title = title;
  });
  updateSelectionToolsAvailability();
}

function renderDiffPicked() {
  const page = document.getElementById("page-snapshots");
  if (!page) return;
  const host = $("#ss-diff-picked", page);
  if (!host) return;

  const picks = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];
  if (picks.length !== 2) {
    const scope = _diffScope();
    const scopeText = scope ? `${scope.provider} / ${scope.feature}` : "No compare scope yet";
    host.innerHTML = `
      <div class="ss-compare-empty">
        <div><b>${picks.length}/2 captures selected</b><span>${escapeHtml(scopeText)}</span></div>
      </div>
    `;
    return;
  }

  const mkCard = (snap, tag, path, idx) => {
    const d = document.createElement("div");
    d.className = "ss-pick-card";
    d.setAttribute("draggable", "true");
    d.dataset.diffIndex = String(idx);
    d.dataset.diffPath = String(path || "");
    d.title = "Drag to swap A/B";
    if (!snap) {
      d.innerHTML = `<div class="ss-pick-date">${tag}</div><div class="ss-muted ss-small">Capture not found</div>`;
      return d;
    }
    const feat = String(snap.feature || "-").toLowerCase();
    const inst = snapInstance(snap);
    const meta = `${(snap.provider || "-").toUpperCase()} / ${inst} / ${feat}`;
    const sub = snap.label ? String(snap.label).slice(0, 60) : String(snap.path || "").slice(0, 80);
    d.innerHTML = `
      <div class="ss-pick-tag">${tag}</div>
      <div class="ss-pick-main">
        <div class="ss-pick-date">${escapeHtml(snapCreatedText(snap))}</div>
        <div class="ss-pick-meta">${escapeHtml(meta)}</div>
        <div class="ss-muted ss-small">${escapeHtml(sub)}</div>
      </div>
    `;
    return d;
  };

  host.innerHTML = "";
  const cards = [
    mkCard(_findSnapByPath(picks[0]), "A", picks[0], 0),
    mkCard(_findSnapByPath(picks[1]), "B", picks[1], 1),
  ];
  cards.forEach((card) => host.appendChild(card));
  cards.forEach((el) => {
    el.addEventListener("dragstart", (e) => {
      el.classList.add("dragging");
      e.dataTransfer.effectAllowed = "move";
      e.dataTransfer.setData("text/plain", String(el.dataset.diffIndex || ""));
    });
    el.addEventListener("dragend", () => { el.classList.remove("dragging"); });
    el.addEventListener("dragover", (e) => { e.preventDefault(); e.dataTransfer.dropEffect = "move"; });
    el.addEventListener("drop", (e) => {
      e.preventDefault();
      const from = Number(e.dataTransfer.getData("text/plain"));
      const to = Number(el.dataset.diffIndex || "0");
      if (!Number.isFinite(from) || !Number.isFinite(to) || from === to) return;
      const arr = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];
      if (arr.length !== 2) return;
      const tmp = arr[from];
      arr[from] = arr[to];
      arr[to] = tmp;
      state.diffPick = arr;
      renderList();
      renderDiffPicked();
      updateDiffAvailability();
    });
  });
}

function renderDiff() {
  const page = document.getElementById("page-snapshots");
  if (!page) return;

  const out = $("#ss-diff-out", page);
  if (!out) return;

  const r = state.diffResult;
  if (!r) {
    out.innerHTML = "";
    return;
  }

  const sum = r.summary || {};
  const trunc = r.truncated || {};
  const extra = (trunc.added || trunc.removed || trunc.updated) ? ` (showing up to ${r.limit} per section)` : "";
  out.innerHTML = `
<div class="ss-diff-summary">
  <span class="ss-pill" title="${sum.added ?? 0} added" aria-label="${sum.added ?? 0} added"><span class="material-symbols-rounded" aria-hidden="true">add_circle</span><strong>${sum.added ?? 0}</strong></span>
  <span class="ss-pill" title="${sum.removed ?? 0} deleted" aria-label="${sum.removed ?? 0} deleted"><span class="material-symbols-rounded" aria-hidden="true">delete</span><strong>${sum.removed ?? 0}</strong></span>
  <span class="ss-pill" title="${sum.updated ?? 0} updated" aria-label="${sum.updated ?? 0} updated"><span class="material-symbols-rounded" aria-hidden="true">edit</span><strong>${sum.updated ?? 0}</strong></span>
  <span class="ss-pill" title="${sum.unchanged ?? 0} unchanged" aria-label="${sum.unchanged ?? 0} unchanged"><span class="material-symbols-rounded" aria-hidden="true">check_circle</span><strong>${sum.unchanged ?? 0}</strong></span>
</div>
${extra ? `<div class="ss-small ss-muted" style="margin-top:8px;text-align:center">${extra}</div>` : ""}`;
}

async function onDiffRun() {
  const page = document.getElementById("page-snapshots");
  if (!page) return;
  const { a, b, ok } = diffSelection();
  if (!ok) return toast("Pick two captures from the same provider and instance", false);
  if (!window.openCaptureCompare) return toast("Capture Compare modal not available", false);
  window.openCaptureCompare({ aPath: a, bPath: b });
}

  function setBusy(on) {
    state.busy = !!on;
    if (!on) {
      setProgress("#ss-restore-progress", false, "", "danger");
      setProgress("#ss-tools-progress", false, "", "danger");
    }
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    $$("#page-snapshots button, #page-snapshots input, #page-snapshots select").forEach((el) => {
      if (!el) return;
      el.disabled = !!on;
    });
    if (!on) {
      // Restore feature-based disabling after busy state.
      try { updateCreateActionsAvailability(); } catch {}
      try { updateRestoreAvailability(); } catch {}
      try { updateSelectionToolsAvailability(); } catch {}
      try { updateDiffAvailability(); } catch {}
    }
  }

  function updateCaptureBusyUI() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const on = !!state.captureBusy;
    page.classList.toggle("ss-capture-running", on);
    $$(".ss-lockable", page).forEach((card) => card.classList.toggle("ss-locked", on));
    const lockMsg = $("#ss-capture-lock", page);
    if (lockMsg) lockMsg.classList.toggle("hidden", !on);
    updateCreateActionsAvailability();
  }

  function repopProviders() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const provSel = $("#ss-prov", page);
    const fProv = $("#ss-filter-provider", page);

    const configured = (state.providers || []).filter((p) => !!p.configured);
    const opts = [{ id: "", label: "- provider -", configured: true }].concat(configured);
    const fill = (sel, addAll = false) => {
      if (!sel) return;
      const cur = String(sel.value || "");
      sel.innerHTML = "";
      const rows = addAll ? [{ id: "", label: "All providers", configured: true }].concat(configured) : opts;
      rows.forEach((p) => {
        const o = document.createElement("option");
        o.value = p.id || "";
        o.textContent = (p.label || p.id || "-");
        sel.appendChild(o);
      });
      const has = Array.from(sel.options).some((o) => String(o.value) === cur);
      sel.value = has ? cur : "";
    };

    fill(provSel, false);
    fill(fProv, true);

    // Provider dropdowns with brand icons
    _enhanceProviderIconSelect(provSel);
    _enhanceProviderIconSelect(fProv);

    repopFeatures();
    repopCreateInstances();
    repopRestoreInstances(state.selectedSnap);
    updateBrowserFilterBar();
    updateCreateReview();

wireDiffControls();
repopDiffSelects();
}

  function _providerById(pid) {
    const id = String(pid || "").toUpperCase();
    return (state.providers || []).find((x) => String(x.id || "").toUpperCase() === id) || null;
  }

  function _fillInstanceSelect(sel, pid, prefer) {
    if (!sel) return;
    const p = _providerById(pid);
    const insts = Array.isArray(p?.instances) ? p.instances : [{ id: "default", label: "Default", configured: true }];
    const cur = String(prefer ?? sel.value ?? "");
    sel.innerHTML = "";

    const options = insts.length ? insts : [{ id: "default", label: "Default", configured: true }];
    options.forEach((it) => {
      const id = String(it?.id || "default");
      const label = String(it?.label || id || "default");
      const configured = (typeof it?.configured === "boolean") ? !!it.configured : true;

      const o = document.createElement("option");
      o.value = id;
      o.textContent = configured ? label : `${label} (not configured)`;
      o.disabled = !configured;
      sel.appendChild(o);
    });

    const has = Array.from(sel.options).some((o) => String(o.value) === cur && !o.disabled);
    if (has) {
      sel.value = cur;
    } else {
      const firstOk = Array.from(sel.options).find((o) => !o.disabled);
      sel.value = firstOk ? String(firstOk.value) : "default";
    }

    sel.disabled = sel.options.length <= 1;
    _enhanceProfileIconSelect(sel);
  }

  function repopInstances(fromSel, toSel) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    _fillInstanceSelect($(toSel, page), String($(fromSel, page)?.value || "").toUpperCase(), null);
  }
  const repopCreateInstances = () => repopInstances("#ss-prov", "#ss-prov-inst");

  function repopRestoreInstances(snap) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const s = snap || state.selectedSnap || {};
    const pid = String(s.provider || "").toUpperCase();
    const inst = String(s.instance || s.instance_id || s.profile || "default");
    const sel = $("#ss-restore-inst", page);
    _fillInstanceSelect(sel, pid, inst);

    if (sel && pid && inst && !Array.from(sel.options).some((o) => String(o.value) === inst)) {
      const o = document.createElement("option");
      o.value = inst;
      o.textContent = `${inst} (missing)`;
      o.disabled = true;
      sel.appendChild(o);
      sel.value = inst;
      _enhanceProfileIconSelect(sel);
    }
  }

  function repopFeatures() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const provId = String($("#ss-prov", page)?.value || "").toUpperCase();
    const p = (state.providers || []).find((x) => String(x.id || "").toUpperCase() === provId);
    const feats = (p && p.features) ? p.features : {};
    const fSel = $("#ss-feature", page);

    const featureOptions = ["all", "watchlist", "ratings", "history", "progress", "collection"];

    if (fSel) {
      const cur = String(fSel.value || "");
      fSel.innerHTML = "";
      featureOptions.forEach((k) => {
        const o = document.createElement("option");
        o.value = k;
        o.textContent = (k === "all") ? "All features" : k;
        if (k === "all") o.disabled = !featureOptions.slice(1).some((name) => !!feats[name]);
        else o.disabled = !feats[k];
        fSel.appendChild(o);
      });
      if (cur) fSel.value = cur;
    }

    const fFeat = $("#ss-filter-feature", page);
    if (fFeat && fFeat.options.length === 0) {
      ["", ...featureOptions.slice(1)].forEach((k) => {
        const o = document.createElement("option");
        o.value = k;
        o.textContent = k || "All features";
        fFeat.appendChild(o);
      });
    }
    const fKind = $("#ss-filter-kind", page);
    if (fKind && fKind.options.length === 0) {
      [
        ["", "All types"],
        ["manual", "Manual"],
        ["auto", "Auto"],
      ].forEach(([value, label]) => {
        const o = document.createElement("option");
        o.value = value;
        o.textContent = label;
        fKind.appendChild(o);
      });
    }
    updateBrowserFilterBar();
  }

  function isAutoCapture(snap) {
    const label = String(snap?.label || "").trim().toLowerCase();
    return label.startsWith("auto-");
  }

  function updateBrowserFilterBar() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const provSel = $("#ss-filter-provider", page);
    const featSel = $("#ss-filter-feature", page);
    const kindSel = $("#ss-filter-kind", page);
    const clearBtn = $("#ss-filter-clear", page);
    const provWrap = $("#ss-filter-provider-wrap", page);
    const featWrap = $("#ss-filter-feature-wrap", page);
    const kindWrap = $("#ss-filter-kind-wrap", page);

    const sync = (sel, wrap) => {
      if (!sel || !wrap) return false;
      const hasValue = !!String(sel.value || "").trim();
      wrap.classList.toggle("active", hasValue);
      return hasValue;
    };

    const hasProvider = sync(provSel, provWrap);
    const hasFeature = sync(featSel, featWrap);
    const hasKind = sync(kindSel, kindWrap);

    if (clearBtn) {
      clearBtn.disabled = !hasProvider && !hasFeature && !hasKind;
      clearBtn.classList.toggle("active", hasProvider || hasFeature || hasKind);
    }
  }

  function renderList() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    updateCaptureBusyUI();

    const list = $("#ss-list", page);
    if (!list) return;

    const fp = String($("#ss-filter-provider", page)?.value || "").trim().toLowerCase();
    const ff = String($("#ss-filter-feature", page)?.value || "").trim().toLowerCase();
    const fk = String($("#ss-filter-kind", page)?.value || "").trim().toLowerCase();
    const all = state.snapshots || [];
    const idx = buildBundleIndex(all);
    const hiddenChildPaths = new Set();
    const childFeaturesByKey = {};

    Object.keys(idx.childrenByKey || {}).forEach((k) => {
      const kids = idx.childrenByKey[k] || [];
      childFeaturesByKey[k] = new Set(kids.map((x) => String(x.feature || "").toLowerCase()));
      kids.forEach((x) => { if (x && x.path) hiddenChildPaths.add(String(x.path)); });
    });

    const matches = (s) => {
      const prov = String(s.provider || "").toLowerCase();
      const feat = String(s.feature || "").toLowerCase();
      const kind = isAutoCapture(s) ? "auto" : "manual";
      if (fp && prov !== fp) return false;
      if (fk && kind !== fk) return false;
      if (ff) {
        if (feat === ff) {
          // direct feature match
        } else if (feat === "all") {
          const set = childFeaturesByKey[bundleKey(s)];
          if (!set || !set.has(ff)) return false;
        } else {
          return false;
        }
      }
      return true;
    };

    const allowChildren = !!ff;
    const top = [];
    all.forEach((s) => {
      if (!s) return;
      const isChild = hiddenChildPaths.has(String(s.path || ""));
      if (!allowChildren && isChild) return;
      if (matches(s)) top.push(s);
    });
    const topOnly = allowChildren ? top : top.filter((s) => !hiddenChildPaths.has(String(s.path || "")));
    const limit = state.showAll ? topOnly.length : (state.listLimit || 8);
    const rows = topOnly.slice(0, limit);

    const footer = $("#ss-list-footer", page);
    if (footer) {
      footer.innerHTML = "";
      if (topOnly.length > limit) {
        footer.innerHTML = `<div class="ss-small ss-muted">Showing ${limit} of ${topOnly.length} captures</div><button id="ss-more" class="btn">Show all (${topOnly.length})</button>`;
      } else if (state.showAll && topOnly.length > (state.listLimit || 8)) {
        footer.innerHTML = `<div class="ss-small ss-muted">Showing ${topOnly.length} of ${topOnly.length} captures</div><button id="ss-less" class="btn">Show less</button>`;
      } else {
        footer.innerHTML = topOnly.length ? `<div class="ss-small ss-muted">Showing ${topOnly.length} capture${topOnly.length === 1 ? "" : "s"}</div>` : "";
      }
      $("#ss-more", footer)?.addEventListener("click", () => { state.showAll = true; renderList(); });
      $("#ss-less", footer)?.addEventListener("click", () => { state.showAll = false; renderList(); });
    }

    if (rows.length === 0) {
      list.innerHTML = `<div class="ss-empty ss-empty-captures" role="status"><span class="material-symbols-rounded ss-empty-icon" aria-hidden="true">inventory_2</span><div class="ss-empty-text">No captures found.</div></div>`;
      updateSelectionToolsAvailability();
      return;
    }

    list.innerHTML = `
      <div class="ss-table-wrap">
        <table class="ss-table">
          <thead>
            <tr>
              <th class="ss-col-check"></th>
              <th class="ss-col-provider">Provider</th>
              <th class="ss-col-feature">Feature</th>
              <th class="ss-col-type">Type</th>
              <th class="ss-col-label">Label</th>
              <th class="ss-col-created">Created</th>
              <th class="ss-col-profile">Profile</th>
              <th class="ss-col-menu"></th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>`;
    const body = $("tbody", list);
    const pathToSnap = new Map();
    (all || []).forEach((s) => { if (s && s.path) pathToSnap.set(String(s.path), s); });

    const clearRestorePick = () => {
      state.selectedPath = "";
      state.selectedSnap = null;
      renderList();
      renderSelected();
      updateRestoreAvailability();
    };

    const syncRestorePick = (path, on) => {
      if (on) {
        if (state.selectedPath !== path) selectSnapshot(path);
        return;
      }
      if (state.selectedPath !== path) return;
      const rest = (Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : []).filter((p) => p !== path);
      if (rest.length) selectSnapshot(rest[rest.length - 1]);
      else clearRestorePick();
    };

    const renderRow = (s, opts = {}) => {
      const child = !!opts.child;
      const childCount = Number(opts.childCount || 0);
      const path = String(s.path || "");
      const picks = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];
      const checked = path && picks.includes(path);
      const feat = String(s.feature || "-").toLowerCase();
      const featureLabel = feat === "all" ? "All features" : feat;
      const isBundle = feat === "all";
      const exp = !!(state.expandedBundles && state.expandedBundles[path]);
      const ixPick = path ? picks.indexOf(path) : -1;
      const abTag = ixPick === 0 ? "A" : (ixPick === 1 ? "B" : "");
      const extra = isBundle && childCount ? `<button class="ss-mini" data-act="toggle">${exp ? "Hide" : "Show"} ${childCount}</button>` : "";
      const label = s.label ? _uiCaptureLabel(s.label) : "-";

      const item = document.createElement("tr");
      item.className = `${child ? "child " : ""}${state.selectedPath === path ? "active " : ""}${checked ? "checked" : ""}`;
      item.dataset.path = path;
      item.innerHTML = `
        <td class="ss-col-check"><span class="ss-pickcell">${abTag ? `<span class="ss-ab ${abTag === "A" ? "a" : "b"}" data-act="diffremove" title="Unselect capture">${abTag}</span>` : ""}<input class="ss-chk" type="checkbox" name="ss-diffpick" title="Select capture" data-act="diffpick" ${checked ? "checked" : ""} /></span></td>
        <td class="ss-col-provider"><span class="ss-badge ok" data-provider="${escapeHtml(String(s.provider || "").toLowerCase())}">${escapeHtml((s.provider || "-").toUpperCase())}</span></td>
        <td class="ss-col-feature" title="${escapeHtml(featureLabel)}"><span class="ss-feature-cell"><span class="ss-feature-label">${escapeHtml(featureLabel)}</span>${extra}</span></td>
        <td class="ss-col-type">${escapeHtml(snapTypeLabel(s))}</td>
        <td class="ss-col-label ${s.label ? "" : "ss-row-muted"}">${escapeHtml(String(label).slice(0, 60))}</td>
        <td class="ss-col-created">${escapeHtml(snapCreatedText(s))}</td>
        <td class="ss-col-profile">${escapeHtml(snapInstance(s))}</td>
        <td class="ss-col-menu"><span class="material-symbols-rounded">chevron_right</span></td>
      `;

      const pick = item.querySelector('input[data-act="diffpick"]');
      pick?.addEventListener("click", (ev) => { ev.stopPropagation(); });
      pick?.addEventListener("change", (ev) => {
        const on = !!ev.currentTarget.checked;
        toggleDiffPick(path, on);
        syncRestorePick(path, on);
      });
      item.querySelector('[data-act="diffremove"]')?.addEventListener("click", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        toggleDiffPick(path, false);
        syncRestorePick(path, false);
      });
      item.querySelector('[data-act="toggle"]')?.addEventListener("click", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        state.expandedBundles = state.expandedBundles || {};
        state.expandedBundles[path] = !state.expandedBundles[path];
        renderList();
        try { repopDiffSelects(); } catch {}
      });
      item.addEventListener("click", () => {
        if (state.captureBusy) return;
        if (path && state.selectedPath === path) {
          state.selectedPath = "";
          state.selectedSnap = null;
          renderList();
          renderSelected();
          updateRestoreAvailability();
          return;
        }
        selectSnapshot(path);
      });
      body?.appendChild(item);
    };

    rows.forEach((s) => {
      const feat = String(s.feature || "").toLowerCase();
      if (feat === "all") {
        const kids = idx.childrenByKey[bundleKey(s)] || [];
        renderRow(s, { childCount: kids.length });
        if (state.expandedBundles && state.expandedBundles[String(s.path || "")]) {
          kids.forEach((c) => renderRow(pathToSnap.get(String(c.path || "")) || c, { child: true }));
        }
      } else {
        renderRow(s);
      }
    });

    updateSelectionToolsAvailability();
  }

function renderSelected() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const host = $("#ss-selected", page);
    if (!host) return;

    const s = state.selectedSnap;
    if (!s) {
      host.classList.add("ss-selected-empty");
      host.classList.remove("ss-muted");
      host.innerHTML = `<div class="ss-selected-title">No capture selected</div><div class="ss-small ss-muted">Pick one from the browser to inspect or restore it.</div>`;
      return;
    }

    const stats = s.stats || {};
    const by = stats.by_type || {};
    const featStats = stats.features || null;
    const inst = String(s.instance || s.instance_id || s.profile || "default");
    const showInst = inst && String(inst).toLowerCase() !== "default";
    const created = s.created_at ? new Date(String(s.created_at)).toLocaleString() : "-";
    const selectedStats = (featStats ? Object.entries(featStats) : Object.entries(by))
      .slice(0, 4)
      .map(([k, v]) => `<div class="ss-selected-stat"><strong>${Number(v || 0)}</strong><span>${escapeHtml(String(k || ""))}</span></div>`)
      .join("");

    host.classList.remove("ss-selected-empty","ss-muted");
    host.innerHTML = `
      <div class="ss-selected-summary">
        <div class="ss-item-meta">
          <span class="ss-badge ok" data-provider="${escapeHtml(String(s.provider || "").toLowerCase())}">${String(s.provider || "").toUpperCase()}</span>
          ${showInst ? `<span class="ss-badge">${escapeHtml(inst)}</span>` : ``}
          <span class="ss-badge">${String(s.feature || "").toLowerCase()}</span>
          ${s.label ? `<span class="ss-badge warn">${escapeHtml(_uiCaptureLabel(s.label)).slice(0, 40)}</span>` : ``}
        </div>
        <div class="ss-selected-count"><strong>${Number(stats.count || 0)}</strong><span>items</span></div>
      </div>
      <div class="ss-selected-meta">
        <div class="ss-selected-kv"><b>Captured</b> ${escapeHtml(created)}</div>
      </div>
      ${selectedStats ? `<div class="ss-selected-stats">${selectedStats}</div>` : ``}
    `;
  }

  function setRefreshSpinning(on) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const icon = $("#ss-refresh-icon", page);
    if (!icon) return;
    if (on) { icon.classList.add("ss-spin"); return; }
    if (Date.now() < (state._spinUntil || 0)) return;
    icon.classList.remove("ss-spin");
  }

  function fmtSyncTime(value) {
    if (!value) return "not yet";
    const diff = Math.max(0, Date.now() - Number(value));
    const mins = Math.floor(diff / 60000);
    if (mins < 1) return "just now";
    if (mins < 60) return `${mins} min ago`;
    const hours = Math.floor(mins / 60);
    if (hours < 24) return `${hours}h ago`;
    const days = Math.floor(hours / 24);
    return `${days} day${days === 1 ? "" : "s"} ago`;
  }

  function updateSyncStatus() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const wrap = $("#ss-hero-summary", page);
    const time = $("#ss-sync-time", page);
    const btn = $("#ss-refresh", page);
    if (!wrap || !time) return;
    const status = state.refreshBusy ? "refreshing" : state.lastRefreshFailed ? "failed" : "ready";
    wrap.dataset.state = status;
    time.textContent = fmtSyncTime(state.lastSyncAt);
    time.title = state.lastSyncAt ? new Date(Number(state.lastSyncAt)).toLocaleString() : "";
    wrap.title = status === "failed" ? "Latest refresh failed" : "";
    if (btn) btn.disabled = state.refreshBusy;
  }

  function startSyncClock() {
    if (state.syncClock) return;
    state.syncClock = setInterval(updateSyncStatus, 30000);
  }

  async function refresh(force = false, announce = true) {
    if (authSetupPending()) return;
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    if (state.refreshBusy) return;

    const now = Date.now();
    if (!force && now - state.lastRefresh < 2500) return;
    state.lastRefresh = now;
    const bust = `_=${now}`;
    const manifestUrl = `/api/snapshots/manifest?${bust}`;
    const listUrl = `/api/snapshots/list?${bust}`;

    const wasBusy = !!state.busy;
    state.refreshBusy = true;
    updateSyncStatus();
    if (!wasBusy) setBusy(true);
    setRefreshSpinning(true);
    try {
      const [m, l] = await Promise.all([
        API()(manifestUrl),
        API()(listUrl),
      ]);

      state.providers = (m && m.providers) ? m.providers : [];
      {
        const visibleProviders = configuredProviderIds();
        const rows = (l && l.snapshots) ? l.snapshots : [];
        state.snapshots = Array.isArray(rows)
          ? rows.filter((snap) => visibleProviders.has(String(snap?.provider || "").trim().toLowerCase()))
          : [];
      }

      updateTopStats();
      repopProviders();
      renderList();
      try { repopDiffSelects(); } catch {}
      state.lastRefreshFailed = false;
      state.lastSyncAt = Date.now();
      updateSyncStatus();

      // keep selection
      if (state.selectedPath) {
        const still = state.snapshots.find((x) => x.path === state.selectedPath);
        if (!still) {
          state.selectedPath = "";
          state.selectedSnap = null;
          renderSelected();
        } else {
          try { repopRestoreInstances(state.selectedSnap); } catch {}
        }
      }
    } catch (e) {
      state.lastRefreshFailed = true;
      updateSyncStatus();
      console.warn("[snapshots] refresh failed", e);
      console.warn("[snapshots]", `Refresh failed: ${e.message || e}`);
      toast(`Snapshots refresh failed: ${e.message || e}`, false);
    } finally {
      state.refreshBusy = false;
      updateSyncStatus();
      setRefreshSpinning(false);
      if (!wasBusy) setBusy(false);
    }
  }

  async function selectSnapshot(path) {
    if (!path) return;
    setBusy(true);
    try {
      const r = await API()(`/api/snapshots/read?path=${encodeURIComponent(path)}`);
      state.selectedPath = path;
      state.selectedSnap = r && r.snapshot ? r.snapshot : null;
      resetRestoreConfirm();
      repopRestoreInstances(state.selectedSnap);
      renderList();
      try { repopDiffSelects(); } catch {}
      renderSelected();
      updateRestoreAvailability();
      $("#ss-restore-out") && ($("#ss-restore-out").textContent = "");
      toast("Snapshot loaded", true);
    } catch (e) {
      console.warn("[snapshots] read failed", e);
      toast(`Snapshot read failed: ${e.message || e}`, false);
    } finally {
      setProgress("#ss-restore-progress", false, "", "danger");
      setRefreshSpinning(false);
      setBusy(false);
    }
  }

  async function onCreate() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const provider = String($("#ss-prov", page)?.value || "").toUpperCase();
    const instance = String($("#ss-prov-inst", page)?.value || "default");
    const feature = String($("#ss-feature", page)?.value || "").toLowerCase();
    const label = String($("#ss-label", page)?.value || "").trim().slice(0, 12);

    if (!provider) return toast("Pick a provider first", false);
    if (!feature) return toast("Pick a feature", false);
    const progressId = newCaptureProgressId();
    state.captureProgressId = progressId;
    state.captureBusy = true;
    updateCaptureBusyUI();

    showCaptureProgressModal({ provider, instance, feature });
    setBusy(true);
    try {
      const r = await POST_JSON("/api/snapshots/create", { provider, instance, feature, label, progress_id: progressId, background: true }, 30000);
      const snap = r && r.snapshot ? r.snapshot : null;
      const progress = snap ? {
        ok: true,
        done: true,
        stage: "done",
        message: "Capture complete.",
        percent: 100,
        provider,
        instance,
        feature: snap.feature || feature,
        current_feature: snap.feature || feature,
        items_done: Number(snap?.stats?.count || 0),
        total_items: Number(snap?.stats?.count || 0),
        result_path: snap.path || "",
      } : await waitForCaptureProgress(progressId);
      if (!progress) throw new Error("Capture progress was interrupted.");
      if (progress.ok === false) throw new Error(progress.error || progress.message || "Capture failed");
      updateCaptureProgressModal(progress);
      $("#ss-label", page).value = "";
      await refresh(true, false);

      const resultPath = String(progress.result_path || snap?.path || "");
      if (resultPath) {
        await selectSnapshot(resultPath);
      }
      setTimeout(() => refresh(true, false), 450);
      setTimeout(() => refresh(true, false), 1600);
      toast("Capture created", true);
    } catch (e) {
      console.warn("[snapshots] create failed", e);
      const msg = String(e && e.message ? e.message : e);
      updateCaptureProgressModal({
        ok: false,
        done: true,
        stage: "error",
        message: msg.toLowerCase().includes("timeout") ? "Capture is still running longer than expected." : msg,
        percent: 100,
        provider,
        instance,
        feature,
        current_feature: feature,
      });
      if (msg.toLowerCase().includes("timeout")) {
        toast("Create is taking longer than expected. Refreshing...", true);
        setTimeout(() => refresh(true, false), 1200);
        setTimeout(() => refresh(true, false), 5000);
      } else {
        toast(`Snapshot create failed: ${msg}`, false);
      }
    } finally {
      state.captureBusy = false;
      updateCaptureBusyUI();
      stopCaptureProgressPoll();
      setBusy(false);
    }
  }

  function readScheduleDraft() {
    const page = document.getElementById("page-snapshots");
    if (!page) return null;
    const provider = String($("#ss-prov", page)?.value || "").toUpperCase();
    const instance = String($("#ss-prov-inst", page)?.value || "default");
    const feature = String($("#ss-feature", page)?.value || "").toLowerCase();
    const label = String($("#ss-label", page)?.value || "").trim().slice(0, 12) || "auto-{provider}-{feature}-{date}";
    if (!provider) {
      toast("Pick a provider first", false);
      return null;
    }
    if (!feature) {
      toast("Pick a feature first", false);
      return null;
    }
    return { provider, instance, feature, label_template: label };
  }

  function scheduleQueueKey(item) {
    return [
      String(item?.provider || "").trim().toUpperCase(),
      String(item?.instance || "default").trim() || "default",
      String(item?.feature || "").trim().toLowerCase(),
      String(item?.label_template || "").trim(),
    ].join("|");
  }

  function renderScheduleQueue() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const host = $("#ss-schedule-queue", page);
    const wrap = $("#ss-schedule-queue-wrap", page);
    const sendBtn = $("#ss-send-schedule-queue", page);
    const clearBtn = $("#ss-clear-schedule-queue", page);
    if (!host) return;
    const items = Array.isArray(state.scheduleQueue) ? state.scheduleQueue : [];
    if (wrap) wrap.classList.toggle("hidden", !schedulerAvailable() || !items.length);
    if (sendBtn) sendBtn.disabled = !items.length;
    if (clearBtn) clearBtn.disabled = !items.length;
    if (!items.length) {
      host.innerHTML = "";
      return;
    }
    host.innerHTML = items.map((item, index) => {
      const provider = String(item.provider || "").toUpperCase();
      const instance = String(item.instance || "default");
      const feature = String(item.feature || "").toLowerCase();
      const label = String(item.label_template || "");
      const showInst = instance && instance.toLowerCase() !== "default";
      const main = [provider, ...(showInst ? [instance] : []), feature === "all" ? "all features" : feature].filter(Boolean).join(" / ");
      return `<div class="ss-queue-item">
        <div class="ss-queue-copy">
          <div class="ss-queue-main">${escapeHtml(main)}</div>
          <div class="ss-queue-sub">${escapeHtml(label)}</div>
        </div>
        <button class="btn" type="button" data-queue-remove="${index}">Remove</button>
      </div>`;
    }).join("");
    $$("[data-queue-remove]", host).forEach((btn) => {
      btn.addEventListener("click", () => {
        const ix = parseInt(String(btn.getAttribute("data-queue-remove") || "-1"), 10);
        if (!Number.isFinite(ix) || ix < 0) return;
        state.scheduleQueue.splice(ix, 1);
        renderScheduleQueue();
        setScheduleQueueFeedback("Removed from queue", true);
      });
    });
  }

  function setScheduleQueueFeedback(message, ok = true, ms = 1800) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const feedback = $("#ss-queue-feedback", page);
    if (feedback) {
      feedback.textContent = String(message || "");
      feedback.style.display = message ? "block" : "none";
      feedback.style.color = ok ? "rgba(224,255,236,.94)" : "rgba(255,232,232,.94)";
      feedback.style.borderColor = ok ? "rgba(98,238,181,.22)" : "rgba(255,122,122,.22)";
      feedback.style.background = ok ? "rgba(25,195,125,.08)" : "rgba(255,77,79,.08)";
    }

    const btn = $("#ss-add-schedule", page);
    if (!btn || !message) return;
    const label = btn.querySelector(".ss-btn-label");
    btn.dataset.defaultText ||= label?.textContent || "Queue for scheduler";
    if (btn.__ssQueueFlashTimer) clearTimeout(btn.__ssQueueFlashTimer);
    if (label) label.textContent = message;
    else btn.textContent = message;
    btn.style.borderColor = ok ? "rgba(98,238,181,.30)" : "rgba(255,122,122,.26)";
    btn.style.boxShadow = ok ? "0 0 12px rgba(25,195,125,.18)" : "0 0 12px rgba(255,77,79,.18)";
    btn.__ssQueueFlashTimer = setTimeout(() => {
      if (label) label.textContent = btn.dataset.defaultText || "Queue for scheduler";
      else btn.textContent = btn.dataset.defaultText || "Queue for scheduler";
      btn.style.borderColor = "";
      btn.style.boxShadow = "";
    }, ms);
  }

  function setToolsOutput(message, ok = true, ms = 3200) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const out = $("#ss-tools-out", page);
    if (!out) return;
    const text = String(message || "").trim();
    out.textContent = text;
    out.classList.toggle("hidden", !text);
    out.style.color = ok ? "rgba(224,255,236,.94)" : "rgba(255,232,232,.94)";
    out.style.borderColor = ok ? "rgba(98,238,181,.22)" : "rgba(255,122,122,.22)";
    out.style.background = ok ? "rgba(25,195,125,.08)" : "rgba(255,77,79,.08)";
    out.style.padding = text ? "9px 10px" : "";
    out.style.borderRadius = text ? "7px" : "";
    out.style.borderStyle = text ? "solid" : "";
    out.style.borderWidth = text ? "1px" : "";
    if (out.__ssToolsTimer) clearTimeout(out.__ssToolsTimer);
    if (text && ms > 0) {
      out.__ssToolsTimer = setTimeout(() => {
        out.textContent = "";
        out.classList.add("hidden");
      }, ms);
    }
  }

  function onQueueScheduleDraft() {
    const payload = readScheduleDraft();
    if (!payload) {
      setScheduleQueueFeedback("Check provider and feature first", false);
      return;
    }
    const key = scheduleQueueKey(payload);
    if (state.scheduleQueue.some((item) => scheduleQueueKey(item) === key)) {
      setScheduleQueueFeedback("Already in queue", false);
      toast("That capture schedule is already queued", false);
      return;
    }
    state.scheduleQueue.push(payload);
    renderScheduleQueue();
    setScheduleQueueFeedback("Added to queue", true);
    toast(`Queued ${state.scheduleQueue.length} capture schedule${state.scheduleQueue.length === 1 ? "" : "s"}`, true);
  }

  async function onSendScheduleQueue() {
    const items = Array.isArray(state.scheduleQueue) ? state.scheduleQueue.slice() : [];
    if (!items.length) return toast("Queue at least one capture schedule first", false);

    try { window.showTab?.("settings"); } catch {}
    setTimeout(async () => {
      try {
        window.cwSettingsSelect?.("scheduling");
        try { await window.loadScheduling?.(); } catch {}
        const sec = document.getElementById("sec-scheduling");
        if (sec && !sec.classList.contains("open")) window.toggleSection?.("sec-scheduling");
        window.cwSchedSettingsSelect?.("advanced");
        let applied = false;
        for (let attempt = 0; attempt < 6 && !applied; attempt += 1) {
          applied = !!window.prefillCaptureSchedules?.(items.slice());
          if (!applied) await new Promise((resolve) => setTimeout(resolve, 80));
        }
        if (!applied) throw new Error("Unable to add queued capture schedules.");
        state.scheduleQueue = [];
        renderScheduleQueue();
        sec?.scrollIntoView({ behavior: "smooth", block: "start" });
        toast(`Added ${items.length} capture schedule${items.length === 1 ? "" : "s"} to Scheduling. Pick times, then save settings.`, true);
      } catch (e) {
        console.warn("[snapshots] queued scheduler prefill failed", e);
        const msg = String(e && e.message ? e.message : e) || "Unable to add queued capture schedules";
        toast(`Queue send failed: ${msg}`, false);
      }
    }, 80);
  }

  async function onAddToScheduler() {
    onQueueScheduleDraft();
  }

  async function onDeleteSelected() {
    const paths = selectedCapturePaths();
    if (!paths.length) return toast("Select a capture first", false);
    const msg = paths.length === 1
      ? "Delete the selected capture? Bundle captures also remove their child captures."
      : `Delete ${paths.length} selected captures? Bundle captures also remove their child captures.`;
    if (!confirm(msg)) return;

    setBusy(true);
    setRefreshSpinning(true);
    try {
      for (const path of paths) {
        const r = await API()("/api/snapshots/delete", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ path, delete_children: true }),
        });
        const res = r && r.result ? r.result : null;
        const ok = res ? !!res.ok : !!(r && r.ok);
        if (!ok) {
          const err = (res && res.errors && res.errors.length) ? res.errors.join(" | ") : (r && r.error) ? r.error : "Delete failed";
          console.warn("[snapshots]", err);
          toast(err, false);
          return;
        }
      }

      state.selectedPath = "";
      state.selectedSnap = null;
      state.diffPick = [];
      state.diffResult = null;
      renderSelected();
      updateRestoreAvailability();

      await refresh(true, false);
      toast(paths.length === 1 ? "Capture deleted" : "Captures deleted", true);
    } catch (e) {
      console.warn("[snapshots]", "Delete failed: " + (e.message || e));
      toast("Delete failed: " + (e.message || e), false);
    } finally {
      setRefreshSpinning(false);
      setBusy(false);
    }
  }

  async function onExportSelected() {
    const paths = selectedCapturePaths();
    if (!paths.length) return toast("Select at least one capture first", false);
    setBusy(true);
    try {
      const captures = [];
      for (const path of paths) {
        const r = await API()(`/api/snapshots/read?path=${encodeURIComponent(path)}`);
        captures.push({ path, snapshot: r && r.snapshot ? r.snapshot : null });
      }
      const payload = {
        kind: "capture_export",
        exported_at: new Date().toISOString(),
        count: captures.length,
        captures,
      };
      const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = captures.length === 1 ? "crosswatch-capture.json" : "crosswatch-captures.json";
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
      toast("Capture export ready", true);
    } catch (e) {
      console.warn("[snapshots] export failed", e);
      toast(`Export failed: ${e.message || e}`, false);
    } finally {
      setBusy(false);
    }
  }

  async function onCleanupOldCaptures() {
    if (!confirm("Cleanup Captures removes all stored captures. Continue?")) return;
    setProgress("#ss-tools-progress", true, "Cleaning up captures...", "danger");
    setBusy(true);
    try {
      const r = await POST_JSON("/api/snapshots/clear", {});
      const res = r && r.result ? r.result : {};
      const summary = res.summary || {};
      const removedFiles = Number(summary.removed_files ?? res.deleted_count ?? 0);
      const freed = Number(summary.freed_bytes || 0);
      setToolsOutput(`Cleanup complete: removed ${removedFiles.toLocaleString()} capture file${removedFiles === 1 ? "" : "s"}${freed ? `, freed ${humanBytes(freed)}` : ""}.`, true);
      state.selectedPath = "";
      state.selectedSnap = null;
      state.diffPick = [];
      state.diffResult = null;
      renderSelected();
      await refresh(true, false);
      toast("Capture cleanup complete", true);
    } catch (e) {
      console.warn("[snapshots] cleanup failed", e);
      toast(`Cleanup failed: ${e.message || e}`, false);
    } finally {
      setProgress("#ss-tools-progress", false, "", "danger");
      setBusy(false);
    }
  }

  async function onViewDetails() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const out = $("#ss-detail-out", page);
    if (!out) return;
    if (!out.classList.contains("hidden")) {
      out.classList.add("hidden");
      out.textContent = "";
      return;
    }
    const paths = selectedCapturePaths();
    const path = state.selectedPath || paths[0] || "";
    if (!path) return toast("Select or load a capture first", false);
    setBusy(true);
    try {
      const snap = state.selectedPath === path && state.selectedSnap
        ? state.selectedSnap
        : (await API()(`/api/snapshots/read?path=${encodeURIComponent(path)}`))?.snapshot;
      out.classList.remove("hidden");
      out.textContent = JSON.stringify({ path, snapshot: snap }, null, 2);
    } catch (e) {
      console.warn("[snapshots] details failed", e);
      toast(`Details failed: ${e.message || e}`, false);
    } finally {
      setBusy(false);
    }
  }

  async function onRestore() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    if (!state.selectedPath) return toast("Select a snapshot first", false);
    const mode = String($("#ss-restore-mode", page)?.value || "merge").toLowerCase();
    const instance = String($("#ss-restore-inst", page)?.value || "default");

    if (Date.now() > Number(state.restoreConfirmUntil || 0)) {
      armRestoreConfirm();
      return;
    }
    resetRestoreConfirm();

    const provider = String(state.selectedSnap?.provider || "").toUpperCase();
    const feature = String(state.selectedSnap?.feature || "").toLowerCase();
    const progressId = newCaptureProgressId();
    state.captureProgressId = progressId;
    state.captureBusy = true;
    updateCaptureBusyUI();
    showCaptureProgressModal({ operation: "restore", provider, instance, feature });
    setBusy(true);
    try {
      const r = await POST_JSON("/api/snapshots/restore", { path: state.selectedPath, mode, instance, progress_id: progressId, background: true }, 30000);
      const direct = r && r.result ? r.result : null;
      const progress = direct ? {
        ok: !!direct.ok,
        done: true,
        operation: "restore",
        stage: direct.ok ? "done" : "error",
        message: direct.message || "Restore complete.",
        percent: 100,
        provider: direct.provider || provider,
        instance: direct.instance || instance,
        feature: direct.feature || feature,
        current_feature: direct.feature || feature,
        added: Number(direct.added || 0),
        removed: Number(direct.removed || 0),
        restore_result: direct,
      } : await waitForCaptureProgress(progressId);

      if (!progress) throw new Error("Restore progress was interrupted.");
      if (progress.ok === false) throw new Error(progress.error || progress.message || "Restore failed");
      updateCaptureProgressModal(progress);
      const res = progress.restore_result || progress.result || {};
      const out = $("#ss-restore-out", page);
      if (out) {
        if (res.ok) out.textContent = `Done. Added ${res.added || 0}, removed ${res.removed || 0}.`;
        else out.textContent = `Restore finished with errors: ${(res.errors || []).join("; ") || "unknown error"}`;
      }

      toast(res.ok ? "Restore complete" : "Restore finished with errors", !!res.ok);
      await refresh(true, false);
    } catch (e) {
      console.warn("[snapshots] restore failed", e);
      toast(`Restore failed: ${e.message || e}`, false);
      const out = $("#ss-restore-out", page);
      if (out) out.textContent = `Restore failed: ${e.message || e}`;
      updateCaptureProgressModal({
        ok: false,
        done: true,
        operation: "restore",
        stage: "error",
        message: String(e?.message || e || "Restore failed"),
        percent: 100,
        provider,
        instance,
        feature,
        current_feature: feature,
      });
    } finally {
      state.captureBusy = false;
      updateCaptureBusyUI();
      stopCaptureProgressPoll();
      setBusy(false);
    }
  }
  async function init() {
    if (authSetupPending()) return retryInitAfterAuth();
    render();
    updateSyncStatus();
    startSyncClock();
    await refresh(true, false);
  }

  function retryInitAfterAuth() {
    if (authRetryWired) return;
    authRetryWired = true;
    const retry = () => {
      if (authSetupPending()) return;
      authRetryWired = false;
      window.removeEventListener("cw-auth-setup-pending", onAuth);
      init();
    };
    const onAuth = (e) => { if (e?.detail?.pending === false) retry(); };
    window.addEventListener("cw-auth-setup-pending", onAuth);
    Promise.resolve(window.__cwAuthBootstrapPromise).catch(() => null).finally(retry);
  }

  function refreshOrInit(force = false) {
    const page = document.getElementById("page-snapshots");
    return page && !page.children.length ? init() : refresh(!!force);
  }

  // public hook for core.js
  window.Snapshots = {
    refresh: refreshOrInit,
    init,
  };

  if (document.getElementById("page-snapshots")) {
    init();
  } else {
    document.addEventListener("tab-changed", (e) => {
      if (authSetupPending()) return;
      if (e?.detail?.id === "snapshots") {
        try { init(); } catch {}
      }
    });
  }

})();
