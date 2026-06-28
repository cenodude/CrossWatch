/* assets/js/activity.js */
/* CrossWatch Recent Scrobble UI */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
(function () {
  const $ = (sel, root = document) => root.querySelector(sel);
  const DEFAULT_RECENT_LIMIT = 3;
  const PAGE_SIZE = 50;
  const state = { modal: null, offset: 0, total: 0, busy: false };
  let initialized = false;
  let refreshTimer = null;
  let retryTimer = null;
  let viewAllBound = false;
  let configCheck = null;

  function esc(value) {
    return String(value == null ? "" : value).replace(/[&<>"']/g, (ch) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    }[ch]));
  }

  async function fetchJSON(url, opts) {
    const r = await fetch(url, { cache: "no-store", credentials: "same-origin", ...(opts || {}) });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  }

  function authSetupPending() {
    try { return window.cwIsAuthSetupPending?.() === true; } catch { return false; }
  }

  function scheduleRetry(delay = 1200) {
    if (retryTimer) return;
    retryTimer = setTimeout(() => {
      retryTimer = null;
      refreshRecentActivity();
    }, delay);
  }

  function rel(epoch) {
    const n = Number(epoch || 0);
    if (!n) return "";
    if (typeof window.relTimeFromEpoch === "function") return window.relTimeFromEpoch(n);
    const diff = Math.max(0, Math.floor(Date.now() / 1000) - n);
    if (diff < 60) return "just now";
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
    return `${Math.floor(diff / 86400)}d ago`;
  }

  function titleOf(item) {
    const title = String(item?.title || "Untitled");
    if (String(item?.media_type || "").toLowerCase() !== "episode") {
      return item?.year ? `${title} (${item.year})` : title;
    }
    const s = Number(item?.season || 0);
    const e = Number(item?.episode || 0);
    const code = s && e ? `S${String(s).padStart(2, "0")}E${String(e).padStart(2, "0")}` : "";
    return code ? `${title} - ${code}` : title;
  }

  function providerMeta() {
    return window.CW?.ProviderMeta || {};
  }

  function providerLabel(value) {
    const meta = providerMeta();
    return meta.label?.(value) || String(value || "").trim().toUpperCase() || "Provider";
  }

  function providerLogo(value) {
    const meta = providerMeta();
    return meta.logLogoPath?.(value) || meta.logoPath?.(value) || "";
  }

  function profileLabel(value) {
    const text = String(value || "default").trim() || "default";
    return text.toLowerCase() === "default" ? "Default" : text;
  }

  function endpointHTML(item, side) {
    const provider = side === "source" ? item?.source : item?.target;
    const instance = side === "source" ? item?.source_instance : item?.target_instance;
    const label = providerLabel(provider);
    const profile = profileLabel(instance);
    const logo = providerLogo(provider);
    const img = logo ? `<img src="${esc(logo)}" alt="" aria-hidden="true">` : `<span>${esc(label.slice(0, 2).toUpperCase())}</span>`;
    return `
      <span class="activity-endpoint" title="${esc(`${label} profile: ${profile}`)}" aria-label="${esc(`${label} profile: ${profile}`)}">
        <span class="activity-provider-icon">${img}</span>
      </span>
    `;
  }

  function targetEndpointHTML(target) {
    return endpointHTML({ target: target?.target, target_instance: target?.target_instance }, "target");
  }

  function routeHTML(item) {
    const rawTargets = Array.isArray(item?.targets) ? item.targets : [];
    const targets = rawTargets.length ? rawTargets : [{ target: item?.target, target_instance: item?.target_instance }];
    return `
      <div class="activity-route">
        ${endpointHTML(item, "source")}
        <span class="activity-route-arrow" aria-hidden="true"></span>
        <span class="activity-targets">${targets.map(targetEndpointHTML).join("")}</span>
      </div>
    `;
  }

  function methodOf(item) {
    const method = String(item?.method || "").trim().toLowerCase();
    return method === "webhook" ? "webhook" : "watcher";
  }

  function badgeText(item, status) {
    if (status !== "ok") return "failed";
    const method = methodOf(item);
    if (method === "webhook") return "webhook";
    return "watcher";
  }

  function metricHTML(media, progress, time) {
    const parts = [];
    if (media) parts.push(`<span class="activity-metric media">${esc(media)}</span>`);
    if (progress) {
      const pct = Math.max(0, Math.min(100, Number(progress || 0)));
      parts.push(`<span class="activity-metric progress"><span class="activity-progress-bar"><span style="width:${pct}%"></span></span>${pct}%</span>`);
    }
    if (time) parts.push(`<span class="activity-metric time">${esc(time)}</span>`);
    return parts.join("");
  }

  function rowHTML(item) {
    const status = String(item?.status || "ok").toLowerCase();
    const media = String(item?.media_type || "").toLowerCase();
    const cls = status === "ok" ? "ok" : "err";
    const time = rel(item?.captured_at || item?.watched_at);
    const progress = Number(item?.progress || 0);
    return `
      <div class="activity-item ${cls}">
        <div class="activity-main">
          <div class="activity-title">${esc(titleOf(item))}</div>
          <div class="activity-flow-line">
            ${routeHTML(item)}
            <div class="activity-meta-line">${metricHTML(media, progress, time)}</div>
          </div>
        </div>
        <div class="activity-badges">
          <span class="activity-badge ${cls}">${badgeText(item, status)}</span>
        </div>
      </div>
    `;
  }

  function cachedWidgetEnabled() {
    const cfg = window._cfgCache;
    const ui = cfg && typeof cfg.ui === "object" ? cfg.ui : {};
    return typeof ui.show_recent_activity === "boolean" ? !!ui.show_recent_activity : true;
  }

  function recentDisplay() {
    const cfg = window._cfgCache;
    const ui = cfg && typeof cfg.ui === "object" ? cfg.ui : {};
    const raw = String(ui.recent_activity_display || "").trim().toLowerCase();
    const countMatch = /^count:(3|4|5)$/.exec(raw);
    if (countMatch) return { mode: "count", limit: Number(countMatch[1]), hours: 0, since: 0 };
    const hoursMatch = /^hours:(24|48|72)$/.exec(raw);
    if (hoursMatch) {
      const hours = Number(hoursMatch[1]);
      return {
        mode: "hours",
        limit: 5,
        hours,
        since: Math.max(0, Math.floor(Date.now() / 1000) - (hours * 3600)),
      };
    }
    const rawLimit = Number(ui.recent_activity_limit);
    return { mode: "count", limit: Math.max(3, Math.min(5, Number.isFinite(rawLimit) ? rawLimit : DEFAULT_RECENT_LIMIT)), hours: 0, since: 0 };
  }

  async function syncWidgetEnabledSetting() {
    if (configCheck) return configCheck;
    configCheck = (async () => {
      try {
        const cfg = await fetchJSON("/api/config");
        if (cfg && typeof cfg === "object") window._cfgCache = cfg;
        const ui = cfg && typeof cfg.ui === "object" ? cfg.ui : {};
        const enabled = typeof ui.show_recent_activity === "boolean" ? !!ui.show_recent_activity : true;
        const block = $("#recent-activity-block");
        block?.classList.toggle("hidden", !enabled);
      } catch {
        // Keep the default-on behavior if config is slow or temporarily unavailable.
      } finally {
        configCheck = null;
      }
    })();
    return configCheck;
  }

  async function refreshRecentActivity() {
    injectCSS();
    bindViewAllButton();

    const block = $("#recent-activity-block");
    const host = $("#recent-activity");
    if (!block || !host) return;

    if (authSetupPending()) {
      if (!host.children.length) host.innerHTML = `<div class="activity-empty">Loading scrobble...</div>`;
      scheduleRetry();
      return;
    }

    await syncWidgetEnabledSetting();
    const enabled = cachedWidgetEnabled();
    block.classList.toggle("hidden", !enabled);
    if (!enabled) return;

    try {
      const display = recentDisplay();
      const params = new URLSearchParams({ limit: String(display.limit) });
      if (display.mode === "hours" && display.since) params.set("since", String(display.since));
      const data = await fetchJSON(`/api/activity/recent?${params.toString()}`);
      if (!data || data.ok !== true || !Array.isArray(data.items)) {
        throw new Error("invalid_activity_response");
      }
      const items = data.items;
      host.innerHTML = items.length
        ? items.map((item) => rowHTML(item)).join("")
        : `<div class="activity-empty">${display.mode === "hours" ? `No recent scrobble in the last ${display.hours} hours.` : "No recent scrobble yet."}</div>`;
    } catch {
      if (!host.children.length || host.textContent.trim() === "Loading scrobble...") {
        host.innerHTML = `<div class="activity-empty">Recent scrobble could not be loaded.</div>`;
      }
      scheduleRetry(2500);
    }
  }

  function injectCSS() {
    if ($("#activity-css")) return;
    const el = document.createElement("style");
    el.id = "activity-css";
    el.textContent = `
      #recent-activity-block.hidden{display:none!important}
      #recent-activity{display:grid;gap:8px}
      .activity-item{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:11px 12px;border-radius:14px;background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.018));border:1px solid rgba(255,255,255,.06);box-shadow:inset 0 1px 0 rgba(255,255,255,.04)}
      .activity-item.ok{border-color:rgba(34,197,94,.14)}
      .activity-item.err{border-color:rgba(239,68,68,.18)}
      .activity-main{min-width:0;display:grid;gap:4px}
      .activity-title{min-width:0;color:rgba(238,243,255,.92);font-weight:800;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
      .activity-flow-line{display:flex;align-items:center;gap:8px;min-width:0;flex-wrap:wrap}
      .activity-route{display:flex;align-items:center;gap:7px;min-width:0;flex-wrap:wrap}
      .activity-route-arrow{position:relative;display:inline-flex;align-items:center;justify-content:center;width:18px;height:14px;flex:0 0 auto;opacity:.72}
      .activity-route-arrow::before{content:"";width:14px;height:1px;border-radius:999px;background:linear-gradient(90deg,rgba(130,149,210,.18),rgba(130,149,210,.72))}
      .activity-route-arrow::after{content:"";position:absolute;right:1px;width:6px;height:6px;border-right:2px solid rgba(174,194,232,.70);border-top:2px solid rgba(174,194,232,.70);transform:rotate(45deg)}
      .activity-targets{display:inline-flex;align-items:center;gap:5px;min-width:0;flex-wrap:wrap}
      .activity-endpoint{display:inline-flex;align-items:center;justify-content:center;flex:0 0 auto;width:28px;height:24px;border-radius:999px;border:1px solid rgba(130,149,210,.14);background:rgba(255,255,255,.035);color:rgba(229,235,248,.86)}
      .activity-provider-icon{display:inline-flex;align-items:center;justify-content:center;flex:0 0 auto;width:16px;height:16px;border-radius:999px;overflow:hidden;background:rgba(255,255,255,.06);font-size:8px;font-weight:900;color:rgba(238,243,255,.82)}
      .activity-provider-icon img{display:block;width:13px;height:13px;object-fit:contain}
      .activity-meta-line{min-width:0;display:flex;align-items:center;gap:7px;flex-wrap:wrap}
      .activity-metric{display:inline-flex;align-items:center;justify-content:center;gap:5px;min-height:22px;padding:0 8px;border-radius:999px;border:1px solid rgba(255,255,255,.075);background:rgba(255,255,255,.032);color:rgba(188,198,215,.76);font-size:10.5px;font-weight:800;line-height:1;text-transform:uppercase;white-space:nowrap}
      .activity-metric.progress{color:rgba(218,227,255,.86);text-transform:none}
      .activity-progress-bar{position:relative;width:24px;height:5px;border-radius:999px;overflow:hidden;background:rgba(255,255,255,.08)}
      .activity-progress-bar span{display:block;height:100%;border-radius:inherit;background:linear-gradient(90deg,#6d76ff,#50a2ff)}
      .activity-metric.time{text-transform:none;color:rgba(174,182,194,.70)}
      .activity-badges{display:flex;align-items:center;justify-content:flex-end;gap:6px;flex-wrap:wrap;flex:0 0 auto}
      .activity-badge{display:inline-flex;align-items:center;justify-content:center;padding:4px 8px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:rgba(8,10,18,.46);font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.04em;color:rgba(224,230,246,.74)}
      .activity-badge.ok{background:rgba(25,195,125,.075);border-color:rgba(25,195,125,.22);color:rgba(205,255,230,.90);box-shadow:0 0 14px rgba(25,195,125,.10)}
      .activity-badge.err{background:rgba(255,77,79,.12);border-color:rgba(255,77,79,.30);color:#ffd2d2}
      .activity-empty{padding:11px 12px;border-radius:14px;background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.018));border:1px solid rgba(255,255,255,.06);box-shadow:inset 0 1px 0 rgba(255,255,255,.04);font:inherit;font-size:12px;font-weight:400;line-height:1.5;color:var(--muted);opacity:.6}
      .activity-modal{position:fixed;inset:0;z-index:1300;display:flex;align-items:center;justify-content:center;padding:18px;background:rgba(4,8,16,.54);backdrop-filter:blur(10px) saturate(116%);-webkit-backdrop-filter:blur(10px) saturate(116%)}
      .activity-modal.hidden{display:none!important}
      .activity-dialog{width:min(860px,calc(100vw - 28px));max-height:min(82vh,760px);display:grid;grid-template-rows:auto auto minmax(0,1fr) auto;gap:12px;padding:16px;border-radius:22px;background:linear-gradient(180deg,rgba(10,13,22,.96),rgba(6,9,18,.96));border:1px solid rgba(255,255,255,.09);box-shadow:0 28px 64px rgba(0,0,0,.42),inset 0 1px 0 rgba(255,255,255,.04)}
      .activity-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px}
      .activity-head-title{font-size:16px;font-weight:900;color:#f4f7ff}
      .activity-head-sub{margin-top:3px;font-size:12px;color:rgba(197,205,220,.72)}
      .activity-close{border:1px solid rgba(255,255,255,.09);background:rgba(255,255,255,.04);color:#eef3ff;border-radius:999px;padding:7px 12px;cursor:pointer;font-weight:800}
      .activity-filters{display:grid;grid-template-columns:1fr 150px 150px;gap:8px}
      .activity-filters input,.activity-filters select{min-width:0;background:rgba(4,6,10,.94);border:1px solid rgba(255,255,255,.08);border-radius:12px;color:#eef3ff;padding:9px 10px}
      .activity-list{display:grid;gap:8px;overflow:auto;min-height:160px;max-height:min(62vh,650px);padding-right:2px}
      .activity-foot{display:flex;align-items:center;justify-content:space-between;gap:10px}
      .activity-foot .activity-count{font-size:12px;color:rgba(197,205,220,.72)}
      .activity-load{border:1px solid rgba(255,255,255,.09);background:rgba(255,255,255,.05);color:#fff;border-radius:999px;padding:8px 14px;cursor:pointer;font-weight:800}
      .activity-load[disabled]{opacity:.5;cursor:default}
      #sync-history .history-meta.muted{font-size:12px;font-weight:400;color:var(--muted);opacity:.6}
      @media(max-width:620px){.activity-item{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:8px;padding:10px 12px}.activity-title{font-size:13px}.activity-flow-line{gap:6px}.activity-route{gap:5px}.activity-targets{gap:4px}.activity-endpoint{width:25px;height:22px}.activity-provider-icon{width:14px;height:14px}.activity-provider-icon img{width:11px;height:11px}.activity-metric{min-height:20px;padding:0 7px;font-size:10px}.activity-progress-bar{width:20px}.activity-badges{justify-content:flex-end;align-self:center}.activity-badge{padding:3px 7px;font-size:10px;letter-spacing:.02em}.activity-filters{grid-template-columns:1fr}.activity-dialog{max-height:86vh}}
      @media(max-width:380px){.activity-item{grid-template-columns:minmax(0,1fr)}.activity-badges{justify-content:flex-start}}
    `;
    document.head.appendChild(el);
  }

  function ensureModal() {
    if (state.modal) return state.modal;
    const modal = document.createElement("div");
    modal.className = "activity-modal hidden";
    modal.innerHTML = `
      <div class="activity-dialog" role="dialog" aria-modal="true" aria-label="Activity Log">
        <div class="activity-head">
          <div>
            <div class="activity-head-title">Scrobble Activity</div>
            <div class="activity-head-sub">Local CrossWatch records for scrobbled movies and episodes.</div>
          </div>
          <button type="button" class="activity-close">Close</button>
        </div>
        <div class="activity-filters">
          <input id="activity-q" type="search" placeholder="Search title, provider or profile">
          <select id="activity-media"><option value="all">All media</option><option value="movie">Movies</option><option value="episode">Episodes</option></select>
          <select id="activity-status"><option value="all">All statuses</option><option value="ok">Scrobbled</option><option value="failed">Failed</option></select>
        </div>
        <div id="activity-list" class="activity-list"></div>
        <div class="activity-foot">
          <div id="activity-count" class="activity-count"></div>
          <button type="button" id="activity-load" class="activity-load">Load more</button>
        </div>
      </div>
    `;
    document.body.appendChild(modal);
    state.modal = modal;

    $(".activity-close", modal)?.addEventListener("click", closeModal);
    modal.addEventListener("click", (e) => {
      if (e.target === modal) closeModal();
    });
    ["#activity-q", "#activity-media", "#activity-status"].forEach((sel) => {
      $(sel, modal)?.addEventListener("input", () => loadActivityPage(true));
      $(sel, modal)?.addEventListener("change", () => loadActivityPage(true));
    });
    $("#activity-load", modal)?.addEventListener("click", () => loadActivityPage(false));
    return modal;
  }

  function closeModal() {
    state.modal?.classList.add("hidden");
  }

  function modalParams() {
    const modal = ensureModal();
    const q = encodeURIComponent(String($("#activity-q", modal)?.value || ""));
    const media = encodeURIComponent(String($("#activity-media", modal)?.value || "all"));
    const status = encodeURIComponent(String($("#activity-status", modal)?.value || "all"));
    return `limit=${PAGE_SIZE}&offset=${state.offset}&media_type=${media}&status=${status}&q=${q}`;
  }

  async function loadActivityPage(reset) {
    if (state.busy) return;
    const modal = ensureModal();
    if (reset) state.offset = 0;
    state.busy = true;
    const list = $("#activity-list", modal);
    const count = $("#activity-count", modal);
    const load = $("#activity-load", modal);
    if (load) load.disabled = true;
    if (reset && list) list.innerHTML = `<div class="activity-empty">Loading...</div>`;

    try {
      const data = await fetchJSON(`/api/activity/history?${modalParams()}`);
      if (!data || data.ok !== true || !Array.isArray(data.items)) {
        throw new Error("invalid_activity_response");
      }
      const items = data.items;
      state.total = Number(data?.total || 0);
      const html = items.map((item) => rowHTML(item)).join("");
      if (list) {
        if (reset) list.innerHTML = html || `<div class="activity-empty">No activity matches this view.</div>`;
        else list.insertAdjacentHTML("beforeend", html);
      }
      state.offset += items.length;
      if (count) count.textContent = `${Math.min(state.offset, state.total)} of ${state.total}`;
      if (load) {
        load.disabled = !data?.has_more;
        load.textContent = data?.has_more ? "Load more" : "All loaded";
      }
    } catch {
      if (list) list.innerHTML = `<div class="activity-empty">Recent scrobble could not be loaded.</div>`;
      if (count) count.textContent = "";
    } finally {
      state.busy = false;
    }
  }

  function openModal() {
    const modal = ensureModal();
    modal.classList.remove("hidden");
    loadActivityPage(true);
  }

  function bindViewAllButton() {
    const btn = $("#activity-view-all");
    if (!btn || viewAllBound) return;
    btn.addEventListener("click", openModal);
    viewAllBound = true;
  }

  function init() {
    if (initialized) return;
    initialized = true;
    injectCSS();
    bindViewAllButton();
    refreshRecentActivity();
    if (!refreshTimer) refreshTimer = setInterval(refreshRecentActivity, 60000);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init, { once: true });
  } else {
    init();
  }
  window.addEventListener("load", () => setTimeout(refreshRecentActivity, 0), { once: true });
  window.addEventListener("cw-auth-setup-pending", (e) => {
    if (e?.detail?.pending === false) setTimeout(refreshRecentActivity, 0);
  });
  document.addEventListener("tab-changed", (e) => {
    const tab = String(e?.detail?.id || e?.detail?.tab || "").toLowerCase();
    if (tab === "main" || !tab) refreshRecentActivity();
  });
  window.addEventListener("settings-changed", () => setTimeout(refreshRecentActivity, 300));
  window.addEventListener("activity-log-cleared", () => {
    refreshRecentActivity();
    if (state.modal && !state.modal.classList.contains("hidden")) loadActivityPage(true);
  });
  window.refreshRecentActivity = refreshRecentActivity;
})();
