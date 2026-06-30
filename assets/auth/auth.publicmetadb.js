// assets/auth/auth.publicmetadb.js
(function () {
  if (window._publicMetaDBPatched) return;
  window._publicMetaDBPatched = true;

  const Shared = window.CW.AuthShared;
  const el = Shared.el;
  const txt = Shared.txt;
  const note = Shared.notify;
  const readSecretField = Shared.readSecretField;
  const SECTION = "#sec-publicmetadb";
  const profile = Shared.createProfileAdapter({
    provider: "publicmetadb",
    configKey: "publicmetadb",
    label: "PublicMetaDB",
    sectionId: "sec-publicmetadb",
    selectId: "publicmetadb_instance",
    storageKey: "cw.ui.publicmetadb.auth.instance.v1",
  });
  let publicMetaDBConnected = false;
  let tmdbRecommendationDismissed = false;
  let tmdbRecommendationCfg = null;
  let tmdbRecommendationRefreshTimer = null;

  function getInstance() {
    return profile ? profile.getInstance() : "default";
  }

  function setInstance(v) {
    if (profile) profile.setInstance(v);
  }

  function api(path) {
    return profile ? profile.api(path) : String(path || "");
  }

  async function fetchJSON(url, opts) {
    return Shared.fetchJSON(url, opts);
  }

  async function getCfg() {
    return Shared.getConfig();
  }

  function cfgBlock(cfg) {
    return profile ? profile.cfgBlock(cfg, true) : {};
  }

  function hasTmdbMetadata(cfg) {
    return !!txt(cfg?.tmdb?.api_key || cfg?.metadata?.tmdb_api_key);
  }

  function ensureTmdbRecommendation() {
    const sub = document.querySelector(SECTION + ' .cw-subpanel[data-sub="auth"]');
    if (!sub) return null;

    let box = el("publicmetadb_tmdb_recommendation");
    if (box) return box;

    box = document.createElement("div");
    box.id = "publicmetadb_tmdb_recommendation";
    box.className = "publicmetadb-tmdb-rec hidden";
    box.innerHTML =
      '<div class="publicmetadb-tmdb-rec-copy">' +
        '<div class="publicmetadb-tmdb-rec-kicker">Recommended for PublicMetaDB</div>' +
        '<strong>Configure TMDb Metadata</strong>' +
        '<div class="muted">Adds titles and release years to PublicMetaDB history, ratings, watchlist, and progress.</div>' +
      '</div>' +
      '<div class="publicmetadb-tmdb-rec-actions">' +
        '<button class="btn primary" type="button" id="btn-publicmetadb-configure-tmdb">Configure TMDb Metadata</button>' +
        '<button class="btn" type="button" id="btn-publicmetadb-dismiss-tmdb">Not now</button>' +
      '</div>';

    const grid = sub.querySelector(".grid2");
    if (grid) grid.insertAdjacentElement("afterend", box);
    else sub.appendChild(box);

    const configure = el("btn-publicmetadb-configure-tmdb");
    if (configure && !configure.__wired) {
      configure.addEventListener("click", openTmdbMetadata);
      configure.__wired = true;
    }

    const dismiss = el("btn-publicmetadb-dismiss-tmdb");
    if (dismiss && !dismiss.__wired) {
      dismiss.addEventListener("click", () => {
        tmdbRecommendationDismissed = true;
        renderTmdbRecommendation();
      });
      dismiss.__wired = true;
    }

    return box;
  }

  function renderTmdbRecommendation(cfg = tmdbRecommendationCfg || window._cfgCache || {}) {
    const box = ensureTmdbRecommendation();
    if (!box) return;
    const show = publicMetaDBConnected && !hasTmdbMetadata(cfg) && !tmdbRecommendationDismissed;
    box.classList.toggle("hidden", !show);
  }

  async function refreshTmdbRecommendation(forceFresh = false) {
    ensureTmdbRecommendation();
    try {
      if (forceFresh) {
        const r = await fetchJSON("/api/config?ts=" + Date.now(), { cache: "no-store" });
        if (r.ok && r.data) tmdbRecommendationCfg = r.data;
      } else {
        tmdbRecommendationCfg = tmdbRecommendationCfg || window._cfgCache || await getCfg();
      }
    } catch {}
    renderTmdbRecommendation();
  }

  function queueTmdbRecommendationRefresh(forceFresh = false, delay = 0) {
    clearTimeout(tmdbRecommendationRefreshTimer);
    tmdbRecommendationRefreshTimer = setTimeout(() => {
      refreshTmdbRecommendation(forceFresh).catch(() => {});
    }, delay);
  }

  async function openTmdbMetadata() {
    tmdbRecommendationDismissed = true;
    renderTmdbRecommendation();
    try { window.cwSettingsSelect?.("providers"); } catch {}
    try { window.openSection?.("sec-meta"); } catch {}
    try { await window.mountMetadataProviders?.(); } catch {}
    try { window.cwMetaProviderEnsure?.(); } catch {}
    try { window.cwMetaProviderSelect?.("tmdb"); } catch {}
    try { window.cwMetaProviderSubSelect?.("tmdb", "api", { persist: false }); } catch {}
    try { window.openSection?.("sec-meta-tmdb"); } catch {}
    const target = el("sec-meta-tmdb") || el("sec-meta");
    try { target?.scrollIntoView({ behavior: "smooth", block: "start" }); } catch {}
    setTimeout(() => { try { el("tmdb_api_key")?.focus({ preventScroll: true }); } catch {} }, 250);
  }

  async function refreshInstanceOptions(preserve) {
    if (profile) await profile.refreshOptions(preserve);
  }

  function ensureInstanceUI() {
    profile?.ensureUI(() => {
      tmdbRecommendationDismissed = false;
      void hydrate();
    });
  }

  async function saveKey(key) {
    const r = await fetchJSON(api("/api/publicmetadb/save"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ api_key: key })
    });
    if (!r.ok || (r.data && r.data.ok === false)) throw new Error(friendlyError((r.data && r.data.error) || "save_failed"));
  }

  function friendlyError(code) {
    switch (String(code || "")) {
      case "api_key_required": return "Enter your PublicMetaDB API key";
      case "invalid_api_key": return "Invalid PublicMetaDB API key";
      case "validation_timeout": return "PublicMetaDB validation timed out";
      case "validation_failed": return "Could not validate PublicMetaDB API key";
      case "validation_bad_response": return "PublicMetaDB validation returned an unexpected response";
      default:
        if (String(code || "").startsWith("validation_http_")) {
          return "PublicMetaDB validation failed";
        }
        return "Saving PublicMetaDB key failed";
    }
  }

  function setConn(ok, msg) {
    return Shared.setStatus("publicmetadb_msg", ok, msg);
  }

  async function refresh() {
    try {
      const r = await fetchJSON(api("/api/publicmetadb/status"), { cache: "no-store" });
      const ok = !!(r.ok && r.data && r.data.connected);
      const reason = String((r.data && r.data.reason) || "");
      publicMetaDBConnected = ok;
      setConn(ok, ok || reason === "api_key_required" ? "" : friendlyError(reason));
      renderTmdbRecommendation();
      note(ok ? "PublicMetaDB connected" : "PublicMetaDB not connected");
    } catch {
      publicMetaDBConnected = false;
      setConn(false, "PublicMetaDB verify failed");
      renderTmdbRecommendation();
      note("PublicMetaDB verify failed");
    }
  }

  function maskInput(i, has) {
    return Shared.maskSecret(i, has);
  }

  async function hydrate() {
    ensureInstanceUI();
    const cfg = window._cfgCache || await getCfg();
    tmdbRecommendationCfg = cfg;
    const blk = cfgBlock(cfg);
    const has = !!txt(blk && blk.api_key);
    maskInput(el("publicmetadb_key"), has);
    el("publicmetadb_hint")?.classList.toggle("hidden", has);
    await refresh();
    renderTmdbRecommendation(cfg);
  }

  async function onSave() {
    const i = el("publicmetadb_key");
    const keyState = readSecretField(i);
    if (!keyState.value) {
      if (keyState.masked || (i && i.dataset.hasKey === "1")) { await refresh(); note("Key unchanged"); return; }
      note("Enter your PublicMetaDB API key"); return;
    }
    try {
      await saveKey(keyState.value);
      maskInput(i, true);
      el("publicmetadb_hint")?.classList.add("hidden");
      note("PublicMetaDB key saved");
      await refresh();
    } catch (e) {
      const msg = e && e.message ? e.message : "Saving PublicMetaDB key failed";
      setConn(false, msg);
      note(msg);
    }
  }

  async function onDisc() {
    try {
      const r = await fetchJSON(api("/api/publicmetadb/disconnect"), { method: "POST" });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error("disconnect_failed");
      maskInput(el("publicmetadb_key"), false);
      el("publicmetadb_hint")?.classList.remove("hidden");
      publicMetaDBConnected = false;
      setConn(false);
      renderTmdbRecommendation();
      note("PublicMetaDB disconnected");
    } catch {
      note("PublicMetaDB disconnect failed");
    }
  }

  function wire() {
    const s = el("publicmetadb_save");
    if (s && !s.__wired) { s.addEventListener("click", onSave); s.__wired = true; }
    const d = el("publicmetadb_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisc); d.__wired = true; }
    const k = el("publicmetadb_key");
    if (k && !k.__wiredSecret) {
      Shared.wireSecretInput(k);
      k.__wiredSecret = true;
    }
  }

  function watch() {
    const host = document.getElementById("auth-providers");
    if (!host || watch._obs) return;
    watch._sectionOpen = !!el("sec-publicmetadb")?.classList.contains("open");
    watch._obs = new MutationObserver(() => {
      ensureInstanceUI();
      wire();
      const open = !!el("sec-publicmetadb")?.classList.contains("open");
      if (open && !watch._sectionOpen) {
        tmdbRecommendationDismissed = false;
        queueTmdbRecommendationRefresh(true);
      }
      watch._sectionOpen = open;
    });
    watch._obs.observe(host, { childList: true, subtree: true, attributes: true, attributeFilter: ["class"] });
  }

  function boot() {
    ensureInstanceUI();
    ensureTmdbRecommendation();
    wire();
    watch();
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", hydrate, { once: true });
    } else {
      hydrate();
    }
  }

  document.addEventListener("settings-collect", (ev) => {
    const cfg = ev?.detail?.cfg;
    if (!cfg) return;
    const keyState = readSecretField(el("publicmetadb_key"));
    if (!keyState.value) return;
    const inst = getInstance();
    cfg.publicmetadb = cfg.publicmetadb || {};
    if (inst === "default") {
      cfg.publicmetadb.api_key = keyState.value;
      return;
    }
    cfg.publicmetadb.instances = cfg.publicmetadb.instances || {};
    cfg.publicmetadb.instances[inst] = cfg.publicmetadb.instances[inst] || {};
    cfg.publicmetadb.instances[inst].api_key = keyState.value;
  });

  document.addEventListener("cw-settings-pane-changed", (ev) => {
    const pane = String(ev?.detail?.pane || "").toLowerCase();
    if (pane === "providers" && boot._lastSettingsPane !== "providers") {
      tmdbRecommendationDismissed = false;
      queueTmdbRecommendationRefresh(true);
    }
    boot._lastSettingsPane = pane;
  });

  document.addEventListener("tab-changed", (ev) => {
    const tab = String(ev?.detail?.id || ev?.detail?.tab || "").toLowerCase();
    if (tab !== "settings") return;
    const pane = String(window.__cwSettingsPane || "").toLowerCase();
    if (pane !== "providers") return;
    tmdbRecommendationDismissed = false;
    queueTmdbRecommendationRefresh(true);
  });

  document.addEventListener("click", (ev) => {
    if (!ev?.target?.closest?.('[data-target="sec-auth"], [data-toggle-section="sec-auth"]')) return;
    tmdbRecommendationDismissed = false;
    queueTmdbRecommendationRefresh(true);
  }, true);

  window.addEventListener("auth-changed", () => queueTmdbRecommendationRefresh(true, 40));
  window.addEventListener("settings-changed", () => queueTmdbRecommendationRefresh(true, 40));

  window.initPublicMetaDBAuthUI = boot;
  window.refreshPublicMetaDBTmdbRecommendation = refreshTmdbRecommendation;
  boot();
})();
