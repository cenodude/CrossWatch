// assets/auth/auth.tmdb.js
(function () {
  if (window._tmdbPatched) return;
  window._tmdbPatched = true;

  const API = {
    start: "/api/tmdb_sync/connect/start",
    verify: "/api/tmdb_sync/verify",
    disconnect: "/api/tmdb_sync/disconnect",
  };

  const Shared = window.CW.AuthShared;
  const el = Shared.el;
  const txt = Shared.txt;
  const note = Shared.notify;
  const profile = Shared.createProfileAdapter({
    provider: "tmdb_sync",
    configKey: "tmdb_sync",
    label: "TMDb Sync",
    sectionId: "sec-tmdb-sync",
    selectId: "tmdb_sync_instance",
    storageKey: "cw.ui.tmdb_sync.auth.instance.v1",
    title: "Select which TMDb Sync account this config applies to.",
  });

  function getTMDbSyncInstance() {
    return profile ? profile.getInstance() : "default";
  }

  function setTMDbSyncInstance(v) {
    if (profile) profile.setInstance(v);
  }

  function tmdbApi(path) {
    return profile ? profile.api(path) : String(path || "");
  }

  async function fetchJSON(url, opts) {
    return Shared.fetchJSON(url, opts);
  }

  async function getCfg(forceFresh) {
    try {
      if (typeof window.getConfig === "function") {
        const cfg = await window.getConfig(!!forceFresh);
        if (cfg) return cfg;
      }
    } catch {}
    const r = await fetchJSON("/api/config?ts=" + Date.now(), { cache: "no-store" });
    return r.ok ? (r.data || {}) : {};
  }

  function getTMDbSyncCfgBlock(cfg) {
    return profile ? profile.cfgBlock(cfg, true) : {};
  }

  async function refreshTMDbSyncInstanceOptions(preserve) {
    if (profile) await profile.refreshOptions(preserve);
  }

  function ensureTMDbSyncInstanceUI() {
    profile?.ensureUI(() => { void hydrate(true, true); });
  }

  function setConn(ok, msg) {
    return Shared.setStatus("tmdb_sync_msg", ok, msg);
  }

  function maskInput(i, has) {
    if (!i || i.dataset.touched === "1") return;
    return Shared.maskSecret(i, has);
  }

  async function hydrate(forceFresh, verifyAfter) {
    ensureTMDbSyncInstanceUI();
    await refreshTMDbSyncInstanceOptions(true);

    const cfg = await getCfg(!!forceFresh);
    const tm = getTMDbSyncCfgBlock(cfg);

    const hasAcc  = !!txt(tm?.account_id);
    const hasKey  = !!txt(tm?.api_key) || hasAcc;
    const hasSess = !!txt(tm?.session_id) || hasAcc;

    const keyEl = el("tmdb_sync_api_key");
    const sessEl = el("tmdb_sync_session_id");
    maskInput(keyEl, hasKey);
    maskInput(sessEl, hasSess);

    el("tmdb_sync_hint")?.classList.toggle("hidden", hasKey);

    if (verifyAfter) await refresh(true);
  }

  let pollTimer = null;
  let pollUntil = 0;

  function stopPoll() {
    if (pollTimer) clearTimeout(pollTimer);
    pollTimer = null;
    pollUntil = 0;
  }

  async function refresh(silent) {
    try {
      await fetch("/api/debug/clear_probe_cache", { method: "POST" }).catch(() => {});
      const r = await fetchJSON(tmdbApi(API.verify), { cache: "no-store" });
      const j = r.data || {};
      const ok = !!j.connected;
      if (ok) {
        await hydrate(true, false);
        const u = j.account?.username ? ` (${j.account.username})` : "";
        setConn(true, `Connected${u}`);
        if (!silent) note("TMDb verified ");
        return;
      }
      if (j.pending) {
        setConn(false, "Pending approval...");
        if (!silent) note("TMDb pending approval");
        return;
      }
      setConn(false, j.error || "Not connected");
      if (!silent) note("TMDb not connected");
    } catch {
      setConn(false, "TMDb verify failed");
      if (!silent) note("TMDb verify failed");
    }
  }

  async function tickPoll() {
    const r = await fetchJSON(tmdbApi(API.verify), { cache: "no-store" });
    const j = r.data || {};

    if (j.connected) {
      stopPoll();
      await hydrate(true, false);
      const u = j.account?.username ? ` (${j.account.username})` : "";
      setConn(true, `Connected${u}`);
      note("TMDb connected ");
      return;
    }

    if (Date.now() >= pollUntil) {
      stopPoll();
      setConn(false, j.pending ? "Still pending. Approve on TMDb, then click Connect." : (j.error || "Not connected"));
      return;
    }

    if (!j.pending) {
      stopPoll();
      setConn(false, j.error || "Not connected");
      return;
    }

    setConn(false, "Waiting...");
    pollTimer = setTimeout(tickPoll, 2000);
  }

  function startPoll(ms) {
    if (pollTimer) return;
    pollUntil = Date.now() + (ms || 120000);
    tickPoll();
  }

  async function onConnect() {
    const keyEl = el("tmdb_sync_api_key");
    const sessEl = el("tmdb_sync_session_id");
    const apiKey = txt(keyEl?.value);
    const hasSess = !!txt(sessEl?.value) || sessEl?.dataset.masked === "1";
    if (hasSess) { await refresh(false); return; }

    if (!apiKey || apiKey.includes("***")) {
      setConn(false, "Enter your API key first.");
      el("tmdb_sync_hint")?.classList.remove("hidden");
      return;
    }

    try {
      const r = await fetchJSON(tmdbApi(API.start), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ api_key: apiKey }),
      });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(r.data?.error || "connect_failed");
      const j = r.data || {};
      if (j.auth_url) window.open(j.auth_url, "_blank", "noopener,noreferrer");
      setConn(false, "Approve in TMDb...");
      startPoll(120000);
    } catch {
      setConn(false, "TMDb connect failed");
    }
  }

  async function checkPendingApproval() {
    stopPoll();
    await refresh(false);
    const m = el("tmdb_sync_msg");
    if (m && m.textContent && m.textContent.toLowerCase().includes("pending")) startPoll(60000);
  }

  async function onDisconnect() {
    stopPoll();
    try {
      const r = await fetchJSON(tmdbApi(API.disconnect), { method: "POST" });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error("disconnect_failed");
      const keyEl = el("tmdb_sync_api_key");
      const sessEl = el("tmdb_sync_session_id");
      if (keyEl) { keyEl.dataset.touched = "0"; keyEl.value = ""; keyEl.dataset.masked = "0"; }
      if (sessEl) { sessEl.dataset.touched = "0"; sessEl.value = ""; sessEl.dataset.masked = "0"; }
      el("tmdb_sync_hint")?.classList.remove("hidden");
      setConn(false, "Disconnected");
      note("TMDb disconnected");
    } catch {
      setConn(false, "TMDb disconnect failed");
      note("TMDb disconnect failed");
    }
  }

  function wire() {
    ensureTMDbSyncInstanceUI();

    const root = el("sec-tmdb-sync");
    if (!root) return;

    const keyEl = el("tmdb_sync_api_key");
    const sessEl = el("tmdb_sync_session_id");

    if (keyEl && !keyEl.__wired) {
      keyEl.addEventListener("input", () => {
        keyEl.dataset.touched = "1";
        const has = (keyEl.value || "").trim().length > 0;
        el("tmdb_sync_hint")?.classList.toggle("hidden", has);
      });
      keyEl.addEventListener("change", () => {
        const has = (keyEl.value || "").trim().length > 0;
        el("tmdb_sync_hint")?.classList.toggle("hidden", has);
      });
      keyEl.__wired = true;
    }

    if (sessEl && !sessEl.__wired) {
      sessEl.addEventListener("input", () => { sessEl.dataset.touched = "1"; });
      sessEl.__wired = true;
    }

    const c = el("tmdb_sync_connect");
    if (c && !c.__wired) { c.addEventListener("click", onConnect); c.__wired = true; }

    const d = el("tmdb_sync_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisconnect); d.__wired = true; }

    if (!wire._focusWired) {
      window.addEventListener("focus", () => { checkPendingApproval(); }, { passive: true });
      wire._focusWired = true;
    }

    if (!root.__hydrated) {
      root.__hydrated = true;
      hydrate(false, true);
    }
  }

  function watch() {
    if (watch._obs) return;

    const attach = () => {
      const host = document.getElementById("auth-providers");
      if (!host) return false;
      watch._obs = new MutationObserver(() => { ensureTMDbSyncInstanceUI(); wire(); });
      watch._obs.observe(host, { childList: true, subtree: true });
      wire();
      return true;
    };

    if (attach()) return;

    watch._obs = new MutationObserver(() => {
      if (attach()) {
        try { watch._obs.disconnect(); } catch {}
      }
    });
    watch._obs.observe(document.documentElement || document.body, { childList: true, subtree: true });
  }

  document.addEventListener("settings-collect", (ev) => {
    const cfg = ev?.detail?.cfg;
    if (!cfg) return;

    const keyEl = el("tmdb_sync_api_key");
    const sessEl = el("tmdb_sync_session_id");

    const key = txt(keyEl?.value || "");
    const sess = txt(sessEl?.value || "");

    const inst = getTMDbSyncInstance();
    cfg.tmdb_sync = cfg.tmdb_sync || {};

    const dst = (inst === "default")
      ? cfg.tmdb_sync
      : ((cfg.tmdb_sync.instances = cfg.tmdb_sync.instances || {}), (cfg.tmdb_sync.instances[inst] = cfg.tmdb_sync.instances[inst] || {}), cfg.tmdb_sync.instances[inst]);

    if (key && !key.includes("***") && keyEl?.dataset.masked !== "1") dst.api_key = key;
    if (sess && !sess.includes("***") && sessEl?.dataset.masked !== "1") dst.session_id = sess;
  });

  function boot() {
    wire();
    watch();
    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", wire, { once: true });
  }

  window.initTMDbAuthUI = boot;
  boot();
})();
