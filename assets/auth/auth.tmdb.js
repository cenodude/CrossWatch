// assets/auth/auth.tmdb.js
(function () {
  if (window._tmdbPatched) return;
  window._tmdbPatched = true;

  const API = {
    start: "/api/tmdb_sync/connect/start",
    verify: "/api/tmdb_sync/verify",
    disconnect: "/api/tmdb_sync/disconnect",
  };

  const el = (id) => document.getElementById(id);
  const txt = (v) => (typeof v === "string" ? v : "").trim();
  const note = (m) => (typeof window.notify === "function" ? window.notify(m) : void 0);

  const TMDB_SYNC_INSTANCE_KEY = "cw.ui.tmdb_sync.auth.instance.v1";

  function getTMDbSyncInstance() {
    var s = el("tmdb_sync_instance");
    var v = s ? txt(s.value) : "";
    if (!v) {
      try { v = localStorage.getItem(TMDB_SYNC_INSTANCE_KEY) || ""; } catch (_) {}
    }
    v = txt(v) || "default";
    return v.toLowerCase() === "default" ? "default" : v;
  }

  function setTMDbSyncInstance(v) {
    var id = txt(String(v || "")) || "default";
    try { localStorage.setItem(TMDB_SYNC_INSTANCE_KEY, id); } catch (_) {}
    var s = el("tmdb_sync_instance");
    if (s) s.value = id;
  }

  function tmdbApi(path) {
    var p = String(path || "");
    var sep = p.indexOf("?") >= 0 ? "&" : "?";
    return p + sep + "instance=" + encodeURIComponent(getTMDbSyncInstance()) + "&ts=" + Date.now();
  }

  async function fetchJSON(url, opts) {
    const r = await fetch(url, opts || {});
    let j = null;
    try { j = await r.json(); } catch {}
    return { ok: r.ok, data: j };
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
    cfg = cfg || {};
    var base = (cfg.tmdb_sync && typeof cfg.tmdb_sync === "object") ? cfg.tmdb_sync : (cfg.tmdb_sync = {});
    var inst = getTMDbSyncInstance();
    if (inst === "default") return base;
    if (!base.instances || typeof base.instances !== "object") base.instances = {};
    if (!base.instances[inst] || typeof base.instances[inst] !== "object") base.instances[inst] = {};
    return base.instances[inst];
  }

  async function refreshTMDbSyncInstanceOptions(preserve) {
    var sel = el("tmdb_sync_instance");
    if (!sel) return;
    var want = preserve === false ? "default" : getTMDbSyncInstance();
    try {
      var r = await fetch("/api/provider-instances/tmdb_sync?ts=" + Date.now(), { cache: "no-store" });
      var arr = await r.json().catch(function(){ return []; });
      var opts = Array.isArray(arr) ? arr : [];
      sel.innerHTML = "";
      function addOpt(id, label) {
        var o = document.createElement("option");
        o.value = String(id);
        o.textContent = String(label || id);
        sel.appendChild(o);
      }
      addOpt("default", "Default");
      opts.forEach(function(o){ if (o && o.id && o.id !== "default") addOpt(o.id, o.label || o.id); });
      if (!Array.from(sel.options).some(function(o){ return o.value === want; })) want = "default";
      sel.value = want;
      setTMDbSyncInstance(want);
    } catch (_) {}
  }

  function ensureTMDbSyncInstanceUI() {
    var panel = document.querySelector('#sec-tmdb-sync .cw-meta-provider-panel[data-provider="tmdb_sync"]') || document.querySelector('#sec-tmdb-sync .cw-meta-provider-panel') || document.querySelector('#sec-tmdb-sync');
    var head = panel ? panel.querySelector('.cw-panel-head') : null;
    if (!head || head.__tmdbSyncInstanceUI) return;
    head.__tmdbSyncInstanceUI = true;

    var wrap = document.createElement('div');
    wrap.className = 'inline';
    wrap.style.display = 'flex';
    wrap.style.gap = '8px';
    wrap.style.alignItems = 'center';
    wrap.style.marginLeft = 'auto';
    wrap.style.flexWrap = 'nowrap';
    wrap.title = 'Select which TMDb Sync account this config applies to.';

    var lab = document.createElement('span');
    lab.className = 'muted';
    lab.textContent = 'Profile';

    var selEl = document.createElement('select');
    selEl.id = 'tmdb_sync_instance';
    selEl.className = 'input';
    selEl.style.minWidth = '160px';

    // Match Trakt: keep it compact and let content drive the width.
    selEl.style.width = 'auto';
    selEl.style.maxWidth = '220px';
    selEl.style.flex = '0 0 auto';
    var btnNewEl = document.createElement('button');
    btnNewEl.type = 'button';
    btnNewEl.className = 'btn secondary';
    btnNewEl.id = 'tmdb_sync_instance_new';
    btnNewEl.textContent = 'New';

    var btnDelEl = document.createElement('button');
    btnDelEl.type = 'button';
    btnDelEl.className = 'btn secondary';
    btnDelEl.id = 'tmdb_sync_instance_del';
    btnDelEl.textContent = 'Delete';

    wrap.appendChild(lab);
    wrap.appendChild(selEl);
    wrap.appendChild(btnNewEl);
    wrap.appendChild(btnDelEl);
    head.appendChild(wrap);

    refreshTMDbSyncInstanceOptions(true);

    var sel = el("tmdb_sync_instance");
    if (sel && !sel._wired) {
      sel._wired = true;
      sel.addEventListener("change", function () {
        setTMDbSyncInstance(sel.value);
        void hydrate(true, true);
      });
    }

    var btnNew = el("tmdb_sync_instance_new");
    if (btnNew && !btnNew._wired) {
      btnNew._wired = true;
      btnNew.addEventListener("click", async function () {
        try {
          var r = await fetch("/api/provider-instances/tmdb_sync/next?ts=" + Date.now(), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: "{}",
            cache: "no-store"
          });
          var j = await r.json().catch(function(){ return {}; });
          var id = txt((j && j.id) || "");
          if (!r.ok || (j && j.ok === false) || !id) throw new Error(String((j && j.error) || "create_failed"));
          setTMDbSyncInstance(id);
          await refreshTMDbSyncInstanceOptions(true);
          await hydrate(true, true);
          note("TMDb Sync profile created");
        } catch {
          note("TMDb Sync profile create failed");
        }
      });
    }

    var btnDel = el("tmdb_sync_instance_del");
    if (btnDel && !btnDel._wired) {
      btnDel._wired = true;
      btnDel.addEventListener("click", async function () {
        var inst = getTMDbSyncInstance();
        if (inst === "default") { note("Cannot delete Default"); return; }
        if (!confirm("Delete TMDb Sync profile '" + inst + "'?")) return;
        try {
          var r = await fetch("/api/provider-instances/tmdb_sync/" + encodeURIComponent(inst), { method: "DELETE", cache: "no-store" });
          var j = await r.json().catch(function(){ return {}; });
          if (!r.ok || (j && j.ok === false)) throw new Error(String((j && j.error) || "delete_failed"));
          setTMDbSyncInstance("default");
          await refreshTMDbSyncInstanceOptions(false);
          await hydrate(true, true);
          note("TMDb Sync profile deleted");
        } catch {
          note("TMDb Sync profile delete failed");
        }
      });
    }
  }

  function setConn(ok, msg) {
    const m = el("tmdb_sync_msg");
    if (!m) return;
    const text = ok ? (msg || "Connected.") : (msg || "Not connected");
    m.textContent = text;
    m.classList.remove("hidden", "ok", "warn");
    m.classList.add(ok ? "ok" : "warn");
  }

  function maskInput(i, has) {
    if (!i) return;
    if (i.dataset.touched === "1") return;
    if (has) { i.value = "••••••••"; i.dataset.masked = "1"; }
    else { i.value = ""; i.dataset.masked = "0"; }
    i.dataset.loaded = "1";
    i.dataset.hasValue = has ? "1" : "";
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
        if (!silent) note("TMDb verified ✓");
        return;
      }
      if (j.pending) {
        setConn(false, "Pending approval…");
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
      note("TMDb connected ✓");
      return;
    }

    if (Date.now() >= pollUntil) {
      stopPoll();
      setConn(false, j.pending ? "Still pending. Approve on TMDb, then click Verify." : (j.error || "Not connected"));
      return;
    }

    if (!j.pending) {
      stopPoll();
      setConn(false, j.error || "Not connected");
      return;
    }

    setConn(false, "Waiting…");
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

    if (!apiKey || apiKey.includes("•••")) {
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
      setConn(false, "Approve in TMDb…");
      startPoll(120000);
    } catch {
      setConn(false, "TMDb connect failed");
    }
  }

  async function onVerify() {
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

    const v = el("tmdb_sync_verify");
    if (v && !v.__wired) { v.addEventListener("click", onVerify); v.__wired = true; }

    const d = el("tmdb_sync_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisconnect); d.__wired = true; }

    if (!wire._focusWired) {
      window.addEventListener("focus", () => { onVerify(); }, { passive: true });
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

    if (key && !key.includes("•••") && keyEl?.dataset.masked !== "1") dst.api_key = key;
    if (sess && !sess.includes("•••") && sessEl?.dataset.masked !== "1") dst.session_id = sess;
  });

  function boot() {
    wire();
    watch();
    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", wire, { once: true });
  }

  window.initTMDbAuthUI = boot;
  boot();
})();
