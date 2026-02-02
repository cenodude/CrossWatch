// assets/auth/auth.mdblist.js
(function () {
  if (window._mdblPatched) return;
  window._mdblPatched = true;

  const el = (id) => document.getElementById(id);
  const txt = (v) => (typeof v === "string" ? v : "").trim();
  const note = (m) => (typeof window.notify === "function" ? window.notify(m) : void 0);

  const MDBLIST_INSTANCE_KEY = "cw.ui.mdblist.auth.instance.v1";

  function getMDBListInstance() {
    var s = el("mdblist_instance");
    var v = s ? txt(s.value) : "";
    if (!v) { try { v = localStorage.getItem(MDBLIST_INSTANCE_KEY) || ""; } catch (_) {} }
    v = txt(v) || "default";
    return v.toLowerCase() === "default" ? "default" : v;
  }

  function setMDBListInstance(v) {
    var id = txt(String(v || "")) || "default";
    try { localStorage.setItem(MDBLIST_INSTANCE_KEY, id); } catch (_) {}
    var s = el("mdblist_instance");
    if (s) s.value = id;
  }

  function mdblApi(path) {
    var p = String(path || "");
    var sep = p.indexOf("?") >= 0 ? "&" : "?";
    return p + sep + "instance=" + encodeURIComponent(getMDBListInstance()) + "&ts=" + Date.now();
  }

  async function fetchJSON(url, opts) {
    const r = await fetch(url, opts || {});
    let j = null; try { j = await r.json(); } catch {}
    return { ok: r.ok, data: j };
  }

  async function getCfg() {
    const r = await fetchJSON("/api/config?ts=" + Date.now(), { cache: "no-store" });
    return r.ok ? (r.data || {}) : {};
  }

  function getMDBListCfgBlock(cfg) {
    cfg = cfg || {};
    var base = (cfg.mdblist && typeof cfg.mdblist === "object") ? cfg.mdblist : (cfg.mdblist = {});
    var inst = getMDBListInstance();
    if (inst === "default") return base;
    if (!base.instances || typeof base.instances !== "object") base.instances = {};
    if (!base.instances[inst] || typeof base.instances[inst] !== "object") base.instances[inst] = {};
    return base.instances[inst];
  }

  async function refreshMDBListInstanceOptions(preserve) {
    var sel = el("mdblist_instance");
    if (!sel) return;
    var want = preserve === false ? "default" : getMDBListInstance();
    try {
      var r = await fetch("/api/provider-instances/mdblist?ts=" + Date.now(), { cache: "no-store" });
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
      setMDBListInstance(want);
    } catch (_) {}
  }

  function ensureMDBListInstanceUI() {
    var panel = document.querySelector('#sec-mdblist .cw-meta-provider-panel[data-provider="mdblist"]') || document.querySelector('#sec-mdblist .cw-meta-provider-panel') || document.querySelector('#sec-mdblist');
    var head = panel ? panel.querySelector('.cw-panel-head') : null;
    if (!head || head.__mdblistInstanceUI) return;
    head.__mdblistInstanceUI = true;

    var wrap = document.createElement('div');
    wrap.className = 'inline';
    wrap.style.display = 'flex';
    wrap.style.gap = '8px';
    wrap.style.alignItems = 'center';
    wrap.style.marginLeft = 'auto';
    wrap.style.flexWrap = 'nowrap';
    wrap.title = 'Select which MDBList profile this config applies to.';

    var lab = document.createElement('span');
    lab.className = 'muted';
    lab.textContent = 'Profile';

    var sel = document.createElement('select');
    sel.id = 'mdblist_instance';
    sel.className = 'input';
    sel.style.minWidth = '160px';

    // Match Trakt: keep it compact and let content drive the width.
    sel.style.width = 'auto';
    sel.style.maxWidth = '220px';
    sel.style.flex = '0 0 auto';
    var btnNew = document.createElement('button');
    btnNew.type = 'button';
    btnNew.className = 'btn secondary';
    btnNew.id = 'mdblist_instance_new';
    btnNew.textContent = 'New';

    var btnDel = document.createElement('button');
    btnDel.type = 'button';
    btnDel.className = 'btn secondary';
    btnDel.id = 'mdblist_instance_del';
    btnDel.textContent = 'Delete';

    wrap.appendChild(lab);
    wrap.appendChild(sel);
    wrap.appendChild(btnNew);
    wrap.appendChild(btnDel);
    head.appendChild(wrap);

    refreshMDBListInstanceOptions(true);

    if (sel && !sel._wired) {
      sel._wired = true;
      sel.addEventListener("change", function () {
        setMDBListInstance(sel.value);
        void hydrate();
      });
    }

    var btnNew = el("mdblist_instance_new");
    if (btnNew && !btnNew._wired) {
      btnNew._wired = true;
      btnNew.addEventListener("click", async function () {
        try {
          var r = await fetch("/api/provider-instances/mdblist/next?ts=" + Date.now(), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: "{}",
            cache: "no-store"
          });
          var j = await r.json().catch(function(){ return {}; });
          var id = txt((j && j.id) || "");
          if (!r.ok || (j && j.ok === false) || !id) throw new Error(String((j && j.error) || "create_failed"));
          setMDBListInstance(id);
          await refreshMDBListInstanceOptions(true);
          void hydrate();
        } catch (e) {
          note("Could not create profile: " + (e && e.message ? e.message : e));
        }
      });
    }

    var btnDel = el("mdblist_instance_del");
    if (btnDel && !btnDel._wired) {
      btnDel._wired = true;
      btnDel.addEventListener("click", async function () {
        var id = getMDBListInstance();
        if (id === "default") return note("Default profile cannot be deleted.");
        if (!confirm('Delete MDBList profile "' + id + '"?')) return;
        try {
          var r = await fetch("/api/provider-instances/mdblist/" + encodeURIComponent(id), { method: "DELETE", cache: "no-store" });
          var j = await r.json().catch(function(){ return {}; });
          if (!r.ok || (j && j.ok === false)) throw new Error(String((j && j.error) || "delete_failed"));
          setMDBListInstance("default");
          await refreshMDBListInstanceOptions(false);
          void hydrate();
        } catch (e) {
          note("Could not delete profile: " + (e && e.message ? e.message : e));
        }
      });
    }
  }

  async function saveKeyNarrow(key) {
    const r = await fetchJSON(mdblApi("/api/mdblist/save"), {
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
      const r = await fetchJSON(mdblApi("/api/mdblist/status"), { cache: "no-store" });
      const ok = !!(r.ok && r.data && r.data.connected);
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
    ensureMDBListInstanceUI();
    const cfg = window._cfgCache || await getCfg();
    const blk = getMDBListCfgBlock(cfg);
    const has = !!txt(blk?.api_key);
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
      const r = await fetchJSON(mdblApi("/api/mdblist/disconnect"), { method: "POST" });
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
    watch._obs = new MutationObserver(() => { ensureMDBListInstanceUI(); wire(); });
    watch._obs.observe(host, { childList: true, subtree: true });
  }

  function boot() {
    ensureMDBListInstanceUI();
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
    const v = txt(el("mdblist_key")?.value || "");
    if (!cfg || !v) return;
    const inst = getMDBListInstance();
    cfg.mdblist = cfg.mdblist || {};
    if (inst === "default") {
      cfg.mdblist.api_key = v;
      return;
    }
    cfg.mdblist.instances = cfg.mdblist.instances || {};
    cfg.mdblist.instances[inst] = cfg.mdblist.instances[inst] || {};
    cfg.mdblist.instances[inst].api_key = v;
  });

  window.initMDBListAuthUI = boot;

  boot();
})();
