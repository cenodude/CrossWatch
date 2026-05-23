// assets/auth/auth.publicmetadb.js
(function () {
  if (window._publicMetaDBPatched) return;
  window._publicMetaDBPatched = true;

  const el = (id) => document.getElementById(id);
  const txt = (v) => (typeof v === "string" ? v : "").trim();
  const note = (m) => (typeof window.notify === "function" ? window.notify(m) : void 0);

  function isMaskedSecret(v) {
    const value = txt(v);
    if (!value) return false;
    if (value === "********" || value === "**********") return true;
    return /^[*]{3,}$/.test(value);
  }

  function readSecretField(i) {
    const raw = txt(i && i.value);
    const masked = !!(i && (i.dataset.masked === "1" || isMaskedSecret(raw)));
    if (!raw && !masked) return { hasValue: false, masked: false, value: "" };
    if (masked) return { hasValue: true, masked: true, value: "" };
    return { hasValue: true, masked: false, value: raw };
  }

  const INSTANCE_KEY = "cw.ui.publicmetadb.auth.instance.v1";

  function getInstance() {
    var s = el("publicmetadb_instance");
    var v = s ? txt(s.value) : "";
    if (!v) { try { v = localStorage.getItem(INSTANCE_KEY) || ""; } catch (_) {} }
    v = txt(v) || "default";
    return v.toLowerCase() === "default" ? "default" : v;
  }

  function setInstance(v) {
    var id = txt(String(v || "")) || "default";
    try { localStorage.setItem(INSTANCE_KEY, id); } catch (_) {}
    var s = el("publicmetadb_instance");
    if (s) s.value = id;
  }

  function api(path) {
    var p = String(path || "");
    var sep = p.indexOf("?") >= 0 ? "&" : "?";
    return p + sep + "instance=" + encodeURIComponent(getInstance()) + "&ts=" + Date.now();
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

  function cfgBlock(cfg) {
    cfg = cfg || {};
    var base = (cfg.publicmetadb && typeof cfg.publicmetadb === "object") ? cfg.publicmetadb : (cfg.publicmetadb = {});
    var inst = getInstance();
    if (inst === "default") return base;
    if (!base.instances || typeof base.instances !== "object") base.instances = {};
    if (!base.instances[inst] || typeof base.instances[inst] !== "object") base.instances[inst] = {};
    return base.instances[inst];
  }

  async function refreshInstanceOptions(preserve) {
    var sel = el("publicmetadb_instance");
    if (!sel) return;
    var want = preserve === false ? "default" : getInstance();
    try {
      var r = await fetch("/api/provider-instances/publicmetadb?ts=" + Date.now(), { cache: "no-store" });
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
      setInstance(want);
    } catch (_) {}
  }

  function ensureInstanceUI() {
    var panel = document.querySelector('#sec-publicmetadb .cw-meta-provider-panel[data-provider="publicmetadb"]') || document.querySelector('#sec-publicmetadb');
    var head = panel ? panel.querySelector('.cw-panel-head') : null;
    if (!head || head.__publicmetadbInstanceUI) return;
    head.__publicmetadbInstanceUI = true;

    var wrap = document.createElement('div');
    wrap.className = 'inline';
    wrap.style.display = 'flex';
    wrap.style.gap = '8px';
    wrap.style.alignItems = 'center';
    wrap.style.marginLeft = 'auto';
    wrap.style.flexWrap = 'nowrap';

    var lab = document.createElement('span');
    lab.className = 'muted';
    lab.textContent = 'Profile';

    var sel = document.createElement('select');
    sel.id = 'publicmetadb_instance';
    sel.name = 'publicmetadb_instance';
    sel.className = 'input';
    sel.style.minWidth = '160px';
    sel.style.width = 'auto';
    sel.style.maxWidth = '220px';
    sel.style.flex = '0 0 auto';

    var btnNew = document.createElement('button');
    btnNew.type = 'button';
    btnNew.className = 'btn secondary';
    btnNew.id = 'publicmetadb_instance_new';
    btnNew.textContent = 'New';

    var btnDel = document.createElement('button');
    btnDel.type = 'button';
    btnDel.className = 'btn secondary';
    btnDel.id = 'publicmetadb_instance_del';
    btnDel.textContent = 'Delete';

    wrap.appendChild(lab);
    wrap.appendChild(sel);
    wrap.appendChild(btnNew);
    wrap.appendChild(btnDel);
    head.appendChild(wrap);

    refreshInstanceOptions(true);
    sel.addEventListener("change", function () {
      setInstance(sel.value);
      void hydrate();
    });
    btnNew.addEventListener("click", async function () {
      try {
        var r = await fetch("/api/provider-instances/publicmetadb/next?ts=" + Date.now(), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: "{}",
          cache: "no-store"
        });
        var j = await r.json().catch(function(){ return {}; });
        var id = txt((j && j.id) || "");
        if (!r.ok || (j && j.ok === false) || !id) throw new Error(String((j && j.error) || "create_failed"));
        setInstance(id);
        await refreshInstanceOptions(true);
        void hydrate();
      } catch (e) {
        note("Could not create profile: " + (e && e.message ? e.message : e));
      }
    });
    btnDel.addEventListener("click", async function () {
      var id = getInstance();
      if (id === "default") return note("Default profile cannot be deleted.");
      if (!confirm('Delete PublicMetaDB profile "' + id + '"?')) return;
      try {
        var r = await fetch("/api/provider-instances/publicmetadb/" + encodeURIComponent(id), { method: "DELETE", cache: "no-store" });
        var j = await r.json().catch(function(){ return {}; });
        if (!r.ok || (j && j.ok === false)) throw new Error(String((j && j.error) || "delete_failed"));
        setInstance("default");
        await refreshInstanceOptions(false);
        void hydrate();
      } catch (e) {
        note("Could not delete profile: " + (e && e.message ? e.message : e));
      }
    });
  }

  async function saveKey(key) {
    const r = await fetchJSON(api("/api/publicmetadb/save"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ api_key: key })
    });
    if (!r.ok || (r.data && r.data.ok === false)) throw new Error("save_failed");
  }

  function setConn(ok, msg) {
    const m = el("publicmetadb_msg");
    if (!m) return;
    m.textContent = ok ? (msg || "Connected.") : (msg || "Not connected");
    m.classList.remove("hidden", "ok", "warn");
    m.classList.add(ok ? "ok" : "warn");
  }

  async function refresh() {
    try {
      const r = await fetchJSON(api("/api/publicmetadb/status"), { cache: "no-store" });
      const ok = !!(r.ok && r.data && r.data.connected);
      setConn(ok);
      note(ok ? "PublicMetaDB verified" : "PublicMetaDB not connected");
    } catch {
      setConn(false, "PublicMetaDB verify failed");
      note("PublicMetaDB verify failed");
    }
  }

  function maskInput(i, has) {
    if (!i) return;
    if (has) { i.value = "********"; i.dataset.masked = "1"; }
    else { i.value = ""; i.dataset.masked = "0"; }
    i.dataset.loaded = "1";
    i.dataset.touched = "";
    i.dataset.clear = "";
    i.dataset.hasKey = has ? "1" : "";
  }

  async function hydrate() {
    ensureInstanceUI();
    const cfg = window._cfgCache || await getCfg();
    const blk = cfgBlock(cfg);
    const has = !!txt(blk && blk.api_key);
    maskInput(el("publicmetadb_key"), has);
    el("publicmetadb_hint")?.classList.toggle("hidden", has);
    await refresh();
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
    } catch {
      note("Saving PublicMetaDB key failed");
    }
  }

  async function onDisc() {
    try {
      const r = await fetchJSON(api("/api/publicmetadb/disconnect"), { method: "POST" });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error("disconnect_failed");
      maskInput(el("publicmetadb_key"), false);
      el("publicmetadb_hint")?.classList.remove("hidden");
      setConn(false);
      note("PublicMetaDB disconnected");
    } catch {
      note("PublicMetaDB disconnect failed");
    }
  }

  function wire() {
    const s = el("publicmetadb_save");
    if (s && !s.__wired) { s.addEventListener("click", onSave); s.__wired = true; }
    const v = el("publicmetadb_verify");
    if (v && !v.__wired) { v.addEventListener("click", refresh); v.__wired = true; }
    const d = el("publicmetadb_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisc); d.__wired = true; }
    const k = el("publicmetadb_key");
    if (k && !k.__wiredSecret) {
      const clearMask = () => {
        if (k.dataset.masked === "1") {
          k.value = "";
          k.dataset.masked = "0";
          k.dataset.touched = "1";
          k.dataset.hasKey = "";
        }
      };
      k.addEventListener("focus", clearMask);
      k.addEventListener("beforeinput", clearMask);
      k.addEventListener("input", () => {
        k.dataset.masked = isMaskedSecret(k.value) ? "1" : "0";
        k.dataset.touched = "1";
        if (k.dataset.masked !== "1") k.dataset.hasKey = "";
      });
      k.__wiredSecret = true;
    }
  }

  function watch() {
    const host = document.getElementById("auth-providers");
    if (!host || watch._obs) return;
    watch._obs = new MutationObserver(() => { ensureInstanceUI(); wire(); });
    watch._obs.observe(host, { childList: true, subtree: true });
  }

  function boot() {
    ensureInstanceUI();
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

  window.initPublicMetaDBAuthUI = boot;
  boot();
})();
