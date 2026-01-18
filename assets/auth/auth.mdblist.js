// assets/auth/auth.mdblist.js
(function () {
  if (window._mdblPatched) return;
  window._mdblPatched = true;

  const el = (id) => document.getElementById(id);
  const txt = (v) => (typeof v === "string" ? v : "").trim();
  const note = (m) => (typeof window.notify === "function" ? window.notify(m) : void 0);

  async function fetchJSON(url, opts) {
    const r = await fetch(url, opts || {});
    let j = null; try { j = await r.json(); } catch {}
    return { ok: r.ok, data: j };
  }

  async function getCfg() {
    const r = await fetchJSON("/api/config", { cache: "no-store" });
    return r.ok ? (r.data || {}) : {};
  }

  async function saveKeyNarrow(key) {
    const r = await fetchJSON("/api/mdblist/save", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ api_key: key })
    });
    if (!r.ok || (r.data && r.data.ok === false)) throw new Error("save_failed");
  }
  function setConn(ok, msg) {
    const m = el("mdblist_msg");
    if (!m) return;
    const text = ok ? (msg || "Connected.") : (msg || "Not connected");
    m.textContent = text;
    m.classList.remove("hidden", "ok", "warn");
    m.classList.add(ok ? "ok" : "warn");
  }

  async function refresh() {
    try {
      await fetch("/api/debug/clear_probe_cache", { method: "POST" }).catch(() => {});
      const r = await fetchJSON("/api/status?fresh=1", { cache: "no-store" });
      const ok = !!(r.ok && r.data && r.data.providers && r.data.providers.MDBLIST && r.data.providers.MDBLIST.connected);
      setConn(ok);
      note(ok ? "MDBList verified ✓" : "MDBList not connected");
    } catch {
      setConn(false, "MDBList verify failed");
      note("MDBList verify failed");
    }
  }

  function maskInput(i, has) {
    if (!i) return;
    if (has) { i.value = "••••••••"; i.dataset.masked = "1"; }
    else { i.value = ""; i.dataset.masked = "0"; }
    i.dataset.loaded = "1";
    i.dataset.touched = "";
    i.dataset.clear = "";
    i.dataset.hasKey = has ? "1" : "";
  }

  async function hydrate() {
    const cfg = window._cfgCache || await getCfg();
    const has = !!txt(cfg?.mdblist?.api_key);
    const i = el("mdblist_key");
    maskInput(i, has);
    el("mdblist_hint")?.classList.toggle("hidden", has);
    await refresh();
  }

  async function onSave() {
    const i = el("mdblist_key");
    const key = txt(i && i.value);
    if (!key) {
      if (i && i.dataset.hasKey === "1") { await refresh(); note("Key unchanged"); return; }
      note("Enter your MDBList API key"); return;
    }
    try {
      await saveKeyNarrow(key);
      if (i) maskInput(i, true);
      el("mdblist_hint")?.classList.add("hidden");
      note("MDBList key saved");
      await refresh();
    } catch {
      note("Saving MDBList key failed");
    }
  }

  async function onDisc() {
    try {
      const r = await fetchJSON("/api/mdblist/disconnect", { method: "POST" });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error("disconnect_failed");
      const i = el("mdblist_key");
      maskInput(i, false);
      el("mdblist_hint")?.classList.remove("hidden");
      setConn(false);
      note("MDBList disconnected");
    } catch {
      note("MDBList disconnect failed");
    }
  }

  function wire() {
    const s = el("mdblist_save");
    if (s && !s.__wired) { s.addEventListener("click", onSave); s.__wired = true; }
    const v = el("mdblist_verify");
    if (v && !v.__wired) { v.addEventListener("click", refresh); v.__wired = true; }
    const d = el("mdblist_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisc); d.__wired = true; }
  }

  function watch() {
    const host = document.getElementById("auth-providers");
    if (!host) return;
    if (watch._obs) return;
    watch._obs = new MutationObserver(() => { wire(); });
    watch._obs.observe(host, { childList: true, subtree: true });
  }

  function boot() {
    wire();
    watch();
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", hydrate, { once: true });
    } else {
      hydrate();
    }
  }

  document.addEventListener("settings-collect", (ev) => {
    const cfg = ev?.detail?.cfg; const v = txt(el("mdblist_key")?.value || "");
    if (!cfg || !v) return;
    cfg.mdblist = cfg.mdblist || {};
    cfg.mdblist.api_key = v;
  });

  window.initMDBListAuthUI = boot;

  boot();
})();
