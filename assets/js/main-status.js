/* assets/js/main-status.js */
/* CrossWatch - Main status and probe badges */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(() => {
  const $ = (id) => document.getElementById(id);
  const txt = (v) => String(v ?? "").trim();
  const meta = window.CW?.ProviderMeta || {};
  const up = (v) => (typeof meta.keyOf === "function" ? meta.keyOf(v) : txt(v).toUpperCase());
  const providerLabel = (v) => (typeof meta.label === "function" ? meta.label(v) : (txt(v) || up(v)));
  const mask = (v) => v === "*****" || /^[•]+$/.test(v);
  const pretty = (v) => (txt(v).toLowerCase() === "default" ? "Default" : txt(v));
  const AUTH_MAP = typeof meta.authProviders === "function"
    ? meta.authProviders().map((info) => [info.sectionId, info.key])
    : [];
  const CROWN =
    '<svg viewBox="0 0 64 64" fill="currentColor" aria-hidden="true"><path d="M8 20l10 8 10-14 10 14 10-8 4 26H4l4-26zM10 52h44v4H10z"/></svg>';

  let providersCache = null;
  let authMo = null;
  let metaMo = null;
  let busy = false;

  function getCachedConfig() {
    return window.CW?.Cache?.getCfg?.() || window._cfgCache || {};
  }

  async function loadConfig(force = false) {
    try {
      if (typeof window.CW?.API?.Config?.load === "function") {
        const cfg = await window.CW.API.Config.load(!!force);
        if (cfg && typeof cfg === "object") return cfg;
      }
    } catch {}
    return getCachedConfig();
  }

  async function loadStatus(force = false) {
    try {
      if (!force && typeof window.CW?.API?.Status?.get === "function") {
        return await window.CW.API.Status.get(!!force);
      }
      if (typeof window.CW?.API?.j === "function") {
        return await window.CW.API.j(`/api/status${force ? "?fresh=1" : ""}`, {}, 15000);
      }
    } catch {}

    try {
      const res = await fetch(`/api/status${force ? "?fresh=1" : ""}`, { cache: "no-store" });
      return res?.ok ? await res.json() : null;
    } catch {}
    return null;
  }

  window.invalidateConfigCache = () => {
    try { window.CW?.Cache?.invalidate?.("config"); } catch {}
    try { delete window._cfgCache; } catch {}
  };

  function isProviderConfigured(key, cfg = getCachedConfig()) {
    try {
      if (typeof window.getConfiguredProviders === "function") {
        return window.getConfiguredProviders(cfg).has(up(key));
      }
    } catch {}
    return false;
  }

  function setDot(id, on) {
    const sec = $(id);
    const head = sec?.querySelector(".head") || sec?.firstElementChild;
    if (!head) return false;
    if (getComputedStyle(head).display !== "flex") {
      Object.assign(head.style, { display: "flex", alignItems: "center" });
    }
    const dot =
      head.querySelector(".auth-dot") ||
      head.appendChild(Object.assign(document.createElement("span"), { className: "auth-dot" }));
    dot.classList.toggle("on", !!on);
    dot.title = on ? "Configured" : "Not configured";
    dot.setAttribute("aria-label", dot.title);
    return true;
  }

  async function refreshAuthDots(force = false) {
    const cfg = await loadConfig(force);
    const host = $("auth-providers-icons");
    host?.querySelectorAll("img[data-prov]").forEach((img) => {
      img.style.display = isProviderConfigured(img.dataset.prov, cfg) ? "inline-block" : "none";
    });
    return AUTH_MAP.reduce((any, [id, key]) => setDot(id, isProviderConfigured(key, cfg)) || any, false);
  }

  window.refreshAuthDots = refreshAuthDots;

  function syncMetadataProviderDot() {
    const chip = $("hub_tmdb_key");
    const dot = $("meta-tmdb-dot");
    const tile = dot?.closest?.(".cw-hub-tile.tmdb");
    if (!chip || !dot || !tile) return false;

    const cfg = getCachedConfig();
    const cfgKey = txt(cfg?.tmdb?.api_key);
    const cfgHas = cfgKey.length > 0 || mask(cfgKey);
    const keyEl = $("tmdb_api_key");
    let uiHas = false;
    let touched = false;
    if (keyEl) {
      const v = txt(keyEl.value);
      touched = keyEl.dataset?.touched === "1";
      uiHas = v.length > 0 || mask(v) || keyEl.dataset?.masked === "1";
      if (touched) uiHas = v.length > 0 || mask(v);
    }

    const raw = txt(chip.textContent).toLowerCase();
    const chipHas = /\bset\b/.test(raw) && !/\bmissing\b|\bnot set\b|\bunset\b|\bempty\b|—/.test(raw);
    const on = uiHas || (!touched && (cfgHas || chipHas));
    dot.classList.toggle("on", on);
    dot.title = on ? "Configured" : "Not configured";
    dot.setAttribute("aria-label", dot.title);
    tile.classList.toggle("is-configured", on);
    return true;
  }

  window.syncMetadataProviderDot = syncMetadataProviderDot;

  function observe(hostId, slot, delay, fn, opts) {
    const host = $(hostId);
    if (!host) {
      setTimeout(() => observe(hostId, slot, delay, fn, opts), 150);
      return;
    }
    fn();
    if ((slot === "auth" && authMo) || (slot === "meta" && metaMo)) return;
    const mo = new MutationObserver(() => {
      clearTimeout(mo._t);
      mo._t = setTimeout(fn, delay);
    });
    mo.observe(host, opts);
    if (slot === "auth") authMo = mo;
    else metaMo = mo;
  }

  function titleCase(v) {
    v = txt(v);
    return v ? v[0] + v.slice(1).toLowerCase() : v;
  }

  function instancesDetail(data) {
    const inst = data && typeof data === "object" ? data.instances : null;
    const sum = data && typeof data === "object" ? data.instances_summary : null;
    if (!inst || typeof inst !== "object") return "";
    const total = Number(sum?.total);
    if (!Number.isFinite(total) || total <= 1) return "";
    const lines = [
      Number.isFinite(Number(sum?.ok))
        ? `Profiles: ${Number(sum.ok)}/${total} connected`
        : `Profiles: ${total}`,
    ];
    const used = Array.isArray(sum?.used) ? sum.used : [];
    if (used.length) {
      const labels = used.slice(0, 4).map(pretty);
      lines.push(`Used: ${labels.join(", ")}${used.length > 4 ? "..." : ""}`);
    }
    return lines.join("\n");
  }

  function usageDetail(data) {
    const hint = txt(data?.usage_hint).replace(/\s*\+\s*/g, " and ");
    if (hint) return hint;
    const usedBy = Array.isArray(data?.used_by) ? data.used_by : [];
    return usedBy.length
      ? `Used by: ${usedBy.map((x) => (txt(x).toLowerCase() === "pair" ? "Sync" : "Watcher")).join(" and ")}`
      : "";
  }

  function providerMeta(key, data) {
    switch (up(key)) {
      case "PLEX":
        return {
          vip: !!(data?.plexpass || data?.subscription?.plan),
          detail: data?.subscription?.plan ? `Plex Pass - ${data.subscription.plan}` : "",
        };
      case "TRAKT":
        return { vip: !!data?.vip, detail: data?.vip ? "VIP status" : "Free account" };
      case "EMBY":
        return { vip: !!data?.premiere, detail: data?.premiere ? "Premiere active" : "" };
      case "MDBLIST":
        return { vip: !!data?.vip, detail: "" };
      default:
        return { vip: false, detail: "" };
    }
  }

  function makeConn({ name, connected, vip, detail, key }) {
    const wrap = document.createElement("div");
    const pill = document.createElement("div");
    wrap.className = "conn-item";
    pill.className = `conn-pill ${connected ? "ok" : "no"}${vip ? " has-vip" : ""}`;
    pill.dataset.prov = up(key || name);
    pill.role = "status";
    pill.ariaLabel = `${name} ${connected ? "connected" : "disconnected"}`;
    if (detail) pill.title = detail;
    pill.innerHTML = `<div class="conn-brand"><span class="conn-logo"></span>${
      vip ? `<span class="conn-slot">${CROWN}</span>` : ""
    }</div><span class="conn-text"></span><span class="dot ${
      connected ? "ok" : "no"
    }" aria-hidden="true"></span>`;
    pill.querySelector(".conn-text").textContent = name;
    wrap.appendChild(pill);
    return wrap;
  }

  function renderProviders() {
    const host = $("conn-badges");
    const btn = $("btn-status-refresh");
    const cfg = getCachedConfig();
    const providers = providersCache || {};
    if (!host) return;

    host.classList.add("vip-badges");
    if (btn && host.contains(btn)) host.removeChild(btn);
    host.querySelectorAll(".conn-item").forEach((n) => n.remove());

    const keys = Object.keys(providers).filter((k) => isProviderConfigured(k, cfg)).sort();
    const none = !keys.length;
    host.classList.toggle("hidden", none);
    if (none) {
      const hdr = document.querySelector(".cw-main-card-head-actions") || document.querySelector(".ops-header");
      if (btn && hdr) hdr.appendChild(btn);
      return;
    }

    keys
      .map((key) => {
        const data = providers[key] || {};
        const name = providerLabel(key) || titleCase(key);
        const meta = providerMeta(key, data);
        const detail =
          [instancesDetail(data), meta.detail, usageDetail(data)].filter(Boolean).join("\n") ||
          `${name} ${data?.connected ? "connected" : "not connected"}`;
        return makeConn({ name, connected: !!data.connected, vip: meta.vip, detail, key });
      })
      .forEach((el) => host.appendChild(el));
  }

  async function refreshMainStatus(force = false) {
    if (busy || window.cwIsAuthSetupPending?.() === true) return;
    busy = true;

    const btn = $("btn-status-refresh");
    if (btn) {
      btn.disabled = true;
      btn.dataset.busy = "1";
    }

    try {
      const [cfgJson, statusJson] = await Promise.all([
        loadConfig(force).catch(() => null),
        loadStatus(force).catch(() => null),
      ]);
      if (cfgJson && typeof cfgJson === "object") {
        try { window.CW?.Cache?.setCfg?.(cfgJson); } catch {}
        try { window._cfgCache = cfgJson; } catch {}
      }
      if (statusJson?.providers && typeof statusJson.providers === "object") providersCache = statusJson.providers;
      Promise.resolve(refreshAuthDots(force)).catch(() => {});
      renderProviders();
    } catch (err) {
      console.error("Status refresh failed:", err);
      renderProviders();
    } finally {
      busy = false;
      if (btn) {
        btn.disabled = false;
        delete btn.dataset.busy;
      }
    }
  }

  window.manualRefreshStatus = (e) => {
    e?.preventDefault?.();
    return refreshMainStatus(true);
  };

  function bindStatusButton() {
    const btn = $("btn-status-refresh");
    if (!btn || btn.dataset.boundClick === "1") return;
    btn.dataset.boundClick = "1";
    btn.addEventListener("click", window.manualRefreshStatus);
  }

  function init() {
    bindStatusButton();
    observe("auth-providers", "auth", 200, () => refreshAuthDots(true).catch(() => {}), { childList: true, subtree: true });
    observe("hub_tmdb_key", "meta", 0, syncMetadataProviderDot, { childList: true, characterData: true, subtree: true });
    let tries = 0;
    (function retry() {
      refreshAuthDots(false)
        .then((ok) => ok || ++tries >= 50 || setTimeout(retry, 200))
        .catch(() => ++tries < 50 && setTimeout(retry, 200));
    })();
    if (String(document.documentElement?.dataset?.tab || document.body?.dataset?.tab || "main").toLowerCase() === "main") {
      void refreshMainStatus(false);
    }
  }

  document.addEventListener(
    "settings-collect",
    () => {
      refreshAuthDots(true).catch(() => {});
      syncMetadataProviderDot();
    },
    true
  );

  document.addEventListener(
    "tab-changed",
    (event) => {
      const tab = String(event?.detail?.id || event?.detail?.tab || "").toLowerCase();
      refreshAuthDots(tab === "settings").catch(() => {});
      syncMetadataProviderDot();
      if (tab === "main") setTimeout(() => { void refreshMainStatus(false); }, 0);
    },
    true
  );

  window.addEventListener("auth-changed", () => {
    refreshAuthDots(true).catch(() => {});
  });

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init, { once: true });
  else init();
})();
