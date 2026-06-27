/* assets/js/main-status.js */
/* CrossWatch - Main status and probe badges */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(() => {
  const $ = (id) => document.getElementById(id);
  const txt = (v) => String(v ?? "").trim();
  const meta = window.CW?.ProviderMeta || {};
  const up = (v) => (typeof meta.keyOf === "function" ? meta.keyOf(v) : txt(v).toUpperCase());
  const providerLabel = (v) => (typeof meta.label === "function" ? meta.label(v) : (txt(v) || up(v)));
  const providerLogo = (v) => (typeof meta.logoPath === "function" ? meta.logoPath(v) : `/assets/img/${up(v)}.svg`);
  const providerTone = (v) => (typeof meta.tone === "function" ? meta.tone(v)?.rgb : "255,255,255") || "255,255,255";
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
    if (cfg && typeof cfg === "object") {
      try { window.CW?.Cache?.setCfg?.(cfg); } catch {}
      try { window._cfgCache = cfg; } catch {}
    }
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
    const panel = dot?.closest?.('.cw-meta-provider-panel[data-provider="tmdb"]') || dot?.closest?.(".cw-hub-tile.tmdb");
    if (!dot) return false;

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

    const raw = txt(chip?.textContent).toLowerCase();
    const chipHas = /\bset\b/.test(raw) && !/\bmissing\b|\bnot set\b|\bunset\b|\bempty\b|—/.test(raw);
    const on = uiHas || (!touched && (cfgHas || chipHas));
    dot.classList.toggle("on", on);
    dot.title = on ? "Configured" : "Not configured";
    dot.setAttribute("aria-label", dot.title);
    panel?.classList?.toggle("is-configured", on);
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
    const profileIds = Object.keys(inst).sort((a, b) => (a !== "default") - (b !== "default") || a.localeCompare(b));
    if (!profileIds.length) return "";
    const ok = Number(sum?.ok);
    const probed = Number(sum?.probed);
    const total = Number(sum?.total);
    const lines = [`Profiles: ${profileIds.map(pretty).join(", ")}`];
    if (Number.isFinite(total) && total > 1) {
      lines.push(Number.isFinite(probed) && probed > 0 && probed < total
        ? `Checked profiles: ${ok}/${probed}, ${total} total`
        : Number.isFinite(ok)
        ? `Connected profiles: ${ok}/${total}`
        : `Profile count: ${total}`);
    }
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

  function updateConn(wrap, { name, connected, vip, detail, key }) {
    const pill = wrap?.querySelector?.(".conn-pill");
    if (!pill) return;
    const provKey = up(key || name);
    const dot = pill.querySelector(".dot");
    const brand = pill.querySelector(".conn-brand");
    const hasSlot = !!pill.querySelector(".conn-slot");
    let visual = pill.querySelector(".conn-provider-visual");
    if (!visual) {
      visual = document.createElement("span");
      visual.className = "conn-provider-visual";
      visual.setAttribute("aria-hidden", "true");
      const legacy = pill.querySelector(".conn-provider-logo,.conn-text");
      if (legacy) legacy.replaceWith(visual);
      else pill.insertBefore(visual, dot || null);
    }
    if (dot && dot.parentElement !== visual) visual.appendChild(dot);

    wrap.dataset.prov = provKey;
    pill.dataset.prov = provKey;
    pill.classList.toggle("ok", !!connected);
    pill.classList.toggle("no", !connected);
    pill.classList.toggle("has-vip", !!vip);
    pill.ariaLabel = `${name} ${connected ? "connected" : "disconnected"}`;
    if (detail) pill.title = detail;
    else pill.removeAttribute("title");
    const logoSrc = providerLogo(provKey);
    visual.style.setProperty("--conn-provider-logo", `url("${logoSrc}")`);
    visual.style.setProperty("--conn-provider-rgb", providerTone(provKey));
    if (dot) {
      dot.classList.toggle("ok", !!connected);
      dot.classList.toggle("no", !connected);
    }
    if (brand && vip && !hasSlot) {
      brand.insertAdjacentHTML("beforeend", `<span class="conn-slot">${CROWN}</span>`);
    } else if (brand && !vip && hasSlot) {
      brand.querySelector(".conn-slot")?.remove();
    }
  }

  function makeConn({ name, connected, vip, detail, key }) {
    const wrap = document.createElement("div");
    const pill = document.createElement("div");
    wrap.className = "conn-item";
    wrap.dataset.prov = up(key || name);
    pill.className = `conn-pill ${connected ? "ok" : "no"}${vip ? " has-vip" : ""}`;
    pill.dataset.prov = up(key || name);
    pill.role = "status";
    pill.ariaLabel = `${name} ${connected ? "connected" : "disconnected"}`;
    if (detail) pill.title = detail;
    pill.innerHTML = `<div class="conn-brand">${
      vip ? `<span class="conn-slot">${CROWN}</span>` : ""
    }</div><span class="conn-provider-visual" aria-hidden="true"><span class="dot ${
      connected ? "ok" : "no"
    }" aria-hidden="true"></span></span>`;
    wrap.appendChild(pill);
    updateConn(wrap, { name, connected, vip, detail, key });
    return wrap;
  }

  function placeConnItems(host, items) {
    let anchor = null;
    for (const item of items) {
      const next = anchor ? anchor.nextSibling : host.firstChild;
      if (next !== item) host.insertBefore(item, next);
      anchor = item;
    }
  }

  function renderProviders() {
    const host = $("conn-badges");
    const btn = $("btn-status-refresh");
    const cfg = getCachedConfig();
    const providers = providersCache || {};
    if (!host) return;

    host.classList.add("vip-badges");
    if (btn && host.contains(btn)) host.removeChild(btn);

    const keys = Object.keys(providers).filter((k) => isProviderConfigured(k, cfg)).sort();
    const none = !keys.length;
    host.classList.toggle("hidden", none);
    if (none) {
      const hdr = document.querySelector(".cw-main-card-head-actions") || document.querySelector(".ops-header");
      if (btn && hdr) hdr.appendChild(btn);
      return;
    }

    const wanted = new Set(keys.map(up));
    const existingByKey = new Map();
    host.querySelectorAll(".conn-item").forEach((node) => {
      const provKey = up(node.dataset.prov);
      if (!wanted.has(provKey)) node.remove();
      else existingByKey.set(provKey, node);
    });

    const items = keys
      .map((key) => {
        const data = providers[key] || {};
        const name = providerLabel(key) || titleCase(key);
        const meta = providerMeta(key, data);
        const detail = [
          `Provider: ${name}`,
          `Status: ${data?.connected ? "Connected" : "Not connected"}`,
          instancesDetail(data) || "Profiles: Not reported",
          meta.detail,
          usageDetail(data),
        ].filter(Boolean).join("\n");
        const provKey = up(key);
        const existing = existingByKey.get(provKey);
        const item = existing || makeConn({ name, connected: !!data.connected, vip: meta.vip, detail, key });
        updateConn(item, { name, connected: !!data.connected, vip: meta.vip, detail, key });
        return item;
      });
    placeConnItems(host, items);
  }

  function applyStatusProviders(providers) {
    if (providers && typeof providers === "object") providersCache = providers;
    renderProviders();
  }

  function renderCachedProviders() {
    const cached = typeof window.loadStatusCache === "function" ? window.loadStatusCache() : null;
    if (cached?.providers && typeof cached.providers === "object") {
      providersCache = cached.providers;
    }
    renderProviders();
  }

  function bindStatusButton() {
    const btn = $("btn-status-refresh");
    if (!btn || btn.dataset.boundClick === "1") return;
    btn.dataset.boundClick = "1";
    btn.addEventListener("click", (e) => window.manualRefreshStatus?.(e));
  }

  function init() {
    bindStatusButton();
    observe("auth-providers", "auth", 200, () => refreshAuthDots(true).catch(() => {}).finally(renderProviders), { childList: true, subtree: true });
    observe("meta-tmdb-dot", "meta", 0, syncMetadataProviderDot, { childList: true, characterData: true, subtree: true });
    renderCachedProviders();
    let tries = 0;
    (function retry() {
      refreshAuthDots(false)
        .then((ok) => {
          renderProviders();
          return ok || ++tries >= 50 || setTimeout(retry, 200);
        })
        .catch(() => ++tries < 50 && setTimeout(retry, 200));
    })();
  }

  document.addEventListener(
    "settings-collect",
    () => {
      refreshAuthDots(true).catch(() => {}).finally(renderProviders);
      syncMetadataProviderDot();
    },
    true
  );

  document.addEventListener(
    "tab-changed",
    (event) => {
      const tab = String(event?.detail?.id || event?.detail?.tab || "").toLowerCase();
      refreshAuthDots(tab === "settings").catch(() => {}).finally(renderProviders);
      syncMetadataProviderDot();
      if (tab === "main") setTimeout(renderCachedProviders, 0);
    },
    true
  );

  document.addEventListener(
    "cw-status-updated",
    (event) => {
      const providers = event?.detail?.providers || null;
      refreshAuthDots(false).catch(() => {}).finally(() => applyStatusProviders(providers));
    },
    true
  );

  window.addEventListener("auth-changed", () => {
    refreshAuthDots(true).catch(() => {}).finally(renderProviders);
  });

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init, { once: true });
  else init();
})();
