// assets/auth/auth.mdblist.js
(function () {
  if (window._mdblPatched) return;
  window._mdblPatched = true;

  const el = (id) => document.getElementById(id);
  const txt = (v) => (typeof v === "string" ? v : "").trim();
  const note = (m) => (typeof window.notify === "function" ? window.notify(m) : void 0);
  const MASK = "********";
  const MDBLIST_INSTANCE_KEY = "cw.ui.mdblist.auth.instance.v1";
  let pollTimer = null;
  let methodOverride = "";

  function isMaskedSecret(v) {
    const value = txt(v);
    return !!value && (value === MASK || /^([*]|[•]){3,}$/.test(value));
  }

  function readSecretField(i) {
    const raw = txt(i && i.value);
    const masked = !!(i && (i.dataset.masked === "1" || isMaskedSecret(raw)));
    if (!raw && !masked) return { hasValue: false, masked: false, value: "" };
    if (masked) return { hasValue: true, masked: true, value: "" };
    return { hasValue: true, masked: false, value: raw };
  }

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

  function friendlyError(code) {
    const key = String(code || "").trim();
    const map = {
      api_key_required: "Enter your MDBList API key",
      invalid_api_key: "Invalid MDBList API key",
      validation_timeout: "MDBList validation timed out",
      validation_failed: "Could not validate MDBList API key",
      validation_bad_response: "MDBList returned an invalid validation response",
      save_failed: "Saving MDBList key failed",
    };
    return map[key] || key.replace(/_/g, " ") || "Saving MDBList key failed";
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

  function activeMethodFromBlock(blk) {
    if (blk?._pending_device && (txt(blk._pending_device.user_code) || txt(blk._pending_device.device_code))) return "device_code";
    if (txt(blk?.api_key) && !txt(blk?.access_token) && !txt(blk?.refresh_token)) return "api_key";
    if (txt(blk?.access_token) || txt(blk?.refresh_token)) return "device_code";
    const raw = txt(String(blk?.auth_method || "")).toLowerCase().replace("-", "_");
    if (raw === "api" || raw === "apikey" || raw === "api_key") return "api_key";
    if (raw === "device" || raw === "device_code" || raw === "oauth") return "device_code";
    return "device_code";
  }

  function activeMethodFromStatus(data) {
    if (data && data.pending) return "device_code";
    if (data && data.api_key_configured && !data.device_configured) return "api_key";
    if (data && data.device_configured) return "device_code";
    return data && data.auth_method === "api_key" ? "api_key" : "device_code";
  }

  function setConn(ok, msg) {
    const m = el("mdblist_msg");
    if (!m) return;
    m.textContent = ok ? (msg || "Connected.") : (msg || "Not connected");
    m.classList.remove("hidden", "ok", "warn");
    m.classList.add(ok ? "ok" : "warn");
  }

  function maskInput(i, has) {
    if (!i) return;
    if (has) { i.value = MASK; i.dataset.masked = "1"; }
    else { i.value = ""; i.dataset.masked = "0"; }
    i.dataset.loaded = "1";
    i.dataset.touched = "";
    i.dataset.clear = "";
    i.dataset.hasKey = has ? "1" : "";
  }

  function setReadonlySecret(id, has) {
    const i = el(id);
    if (!i) return;
    i.value = has ? MASK : "";
    i.dataset.masked = has ? "1" : "0";
  }

  async function copyField(id, btn) {
    const i = el(id);
    const value = txt(i?.value);
    if (!value || isMaskedSecret(value)) return note("Nothing to copy");
    try {
      await navigator.clipboard.writeText(value);
      if (btn) {
        const old = btn.textContent;
        btn.textContent = "Copied";
        setTimeout(() => { btn.textContent = old || "Copy"; }, 900);
      }
    } catch (_) {
      note("Copy failed");
    }
  }

  function setMethodUI(method) {
    const m = method === "api_key" ? "api_key" : "device_code";
    const sel = el("mdblist_auth_method");
    if (sel) {
      sel.value = m;
      try {
        window.CW?.IconSelect?.enhance(sel, sel.__cwIconSelectCfg || { className: "cw-plain-select" });
      } catch (_) {}
    }
    const dev = el("mdblist_device_panel");
    const api = el("mdblist_api_panel");
    if (dev) dev.style.display = m === "device_code" ? "" : "none";
    if (api) api.style.display = m === "api_key" ? "" : "none";
  }

  function setApiHintVisible(visible) {
    const h = el("mdblist_hint");
    if (!h) return;
    h.classList.toggle("hidden", !visible);
    h.style.display = visible ? "" : "none";
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

    var lab = document.createElement('span');
    lab.className = 'muted';
    lab.textContent = 'Profile';

    var sel = document.createElement('select');
    sel.id = 'mdblist_instance';
    sel.name = 'mdblist_instance';
    sel.className = 'input';
    sel.style.minWidth = '160px';
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
  }

  async function saveAuth(payload) {
    const r = await fetchJSON(mdblApi("/api/mdblist/save"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload || {})
    });
    if (!r.ok || (r.data && r.data.ok === false)) throw new Error(friendlyError((r.data && r.data.error) || "save_failed"));
    return r.data || {};
  }

  async function refresh(showToast) {
    try {
      const r = await fetchJSON(mdblApi("/api/mdblist/status"), { cache: "no-store" });
      const data = r.data || {};
      const ok = !!(r.ok && data.connected);
      const statusMethod = activeMethodFromStatus(data);
      const method = methodOverride || statusMethod;
      setMethodUI(method);
      setReadonlySecret("mdblist_access_token", !!data.device_configured);
      if (data.pending) {
        const code = el("mdblist_device_code");
        if (code && data.pending.user_code) code.value = txt(data.pending.user_code);
        if (data.pending.user_code && !pollTimer) startPoll(Math.max(2, Number(data.pending.interval || 5)));
      }
      let msg = ok ? (statusMethod === "api_key" ? "Connected with API key." : "Connected with Device Code.") : "Not connected";
      if (data.pending && !data.device_configured) msg = "Waiting for Device Code approval";
      if (ok && data.expires_at && statusMethod === "device_code") msg = "Connected with Device Code.";
      setConn(data.pending && !data.device_configured ? false : ok, msg);
      if (showToast) note(ok ? "MDBList verified" : "MDBList not connected");
    } catch {
      setConn(false, "MDBList verify failed");
      if (showToast) note("MDBList verify failed");
    }
  }

  async function hydrate() {
    ensureMDBListInstanceUI();
    const cfg = window._cfgCache || await getCfg();
    const blk = getMDBListCfgBlock(cfg);
    const method = methodOverride || activeMethodFromBlock(blk);
    setMethodUI(method);
    setReadonlySecret("mdblist_access_token", !!txt(blk?.access_token));
    const hasApiKey = !!txt(blk?.api_key);
    maskInput(el("mdblist_key"), hasApiKey);
    setApiHintVisible(!hasApiKey);

    const pend = blk?._pending_device || {};
    if (pend.user_code) {
      const code = el("mdblist_device_code");
      if (code) code.value = txt(pend.user_code);
      startPoll(Math.max(2, Number(pend.interval || 5)));
    }
    await refresh(false);
  }

  async function onMethodChange() {
    const method = el("mdblist_auth_method")?.value === "api_key" ? "api_key" : "device_code";
    methodOverride = method === "device_code" ? "device_code" : "";
    setMethodUI(method);
    try {
      await saveAuth({ auth_method: method });
      await refresh(false);
    } catch {
      note("MDBList method switch failed");
    }
  }

  async function onSaveApiKey() {
    const i = el("mdblist_key");
    const keyState = readSecretField(i);
    if (!keyState.value) {
      if (keyState.masked || (i && i.dataset.hasKey === "1")) { await refresh(true); note("Key unchanged"); return; }
      note("Enter your MDBList API key"); return;
    }
    try {
      methodOverride = "";
      await saveAuth({ auth_method: "api_key", api_key: keyState.value });
      maskInput(i, true);
      setApiHintVisible(false);
      note("MDBList API key saved");
      await refresh(true);
    } catch (e) {
      const msg = e && e.message ? e.message : "Saving MDBList key failed";
      setConn(false, msg);
      note(msg);
    }
  }

  function stopPoll() {
    if (pollTimer) clearTimeout(pollTimer);
    pollTimer = null;
  }

  function startPoll(intervalSec) {
    stopPoll();
    const run = async () => {
      try {
        const r = await fetchJSON(mdblApi("/api/mdblist/device/poll"), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: "{}",
          cache: "no-store"
        });
        const data = r.data || {};
        if (r.ok && data.ok) {
          stopPoll();
          methodOverride = "";
          note("MDBList connected");
          await hydrate();
          return;
        }
        const status = String(data.status || data.error || "");
        if (status && !["authorization_pending", "slow_down"].includes(status)) {
          setConn(false, status.replace(/_/g, " "));
          stopPoll();
          return;
        }
      } catch (_) {}
      pollTimer = setTimeout(run, Math.max(2, Number(intervalSec || 5)) * 1000);
    };
    pollTimer = setTimeout(run, Math.max(2, Number(intervalSec || 5)) * 1000);
  }

  async function onDeviceStart() {
    let win = null;
    try { win = window.open("about:blank", "_blank"); } catch (_) {}
    try {
      methodOverride = "device_code";
      const r = await fetchJSON(mdblApi("/api/mdblist/device/start"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
        cache: "no-store"
      });
      const data = r.data || {};
      if (!r.ok || !data.ok) throw new Error(String(data.error || "device_start_failed"));
      const code = el("mdblist_device_code");
      if (code) code.value = txt(data.user_code);
      setConn(false, "Waiting for approval");
      startPoll(Math.max(2, Number(data.interval || 5)));
      if (win && !win.closed) {
        try { win.location.href = txt(data.verification_uri || "https://mdblist.com/oauth/device/"); win.focus(); } catch (_) {}
      } else {
        note("Popup blocked - allow popups and try again");
      }
    } catch (e) {
      try { if (win && !win.closed) win.close(); } catch (_) {}
      note("MDBList Device Code start failed: " + (e && e.message ? e.message : e));
    }
  }

  async function onDisc() {
    stopPoll();
    try {
      methodOverride = "";
      const r = await fetchJSON(mdblApi("/api/mdblist/disconnect"), { method: "POST" });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error("disconnect_failed");
      maskInput(el("mdblist_key"), false);
      const code = el("mdblist_device_code"); if (code) code.value = "";
      setReadonlySecret("mdblist_access_token", false);
      setConn(false);
      note("MDBList disconnected");
      await hydrate();
    } catch {
      note("MDBList disconnect failed");
    }
  }

  function wire() {
    const method = el("mdblist_auth_method");
    if (method && !method.__wired) { method.addEventListener("change", onMethodChange); method.__wired = true; }
    const s = el("mdblist_save");
    if (s && !s.__wired) { s.addEventListener("click", onSaveApiKey); s.__wired = true; }
    const start = el("mdblist_device_start");
    if (start && !start.__wired) { start.addEventListener("click", onDeviceStart); start.__wired = true; }
    const copyTok = el("mdblist_copy_token");
    if (copyTok && !copyTok.__wired) { copyTok.addEventListener("click", () => copyField("mdblist_access_token", copyTok)); copyTok.__wired = true; }
    const copyCode = el("mdblist_copy_code");
    if (copyCode && !copyCode.__wired) { copyCode.addEventListener("click", () => copyField("mdblist_device_code", copyCode)); copyCode.__wired = true; }
    const d = el("mdblist_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisc); d.__wired = true; }
    const dd = el("mdblist_disconnect_device");
    if (dd && !dd.__wired) { dd.addEventListener("click", onDisc); dd.__wired = true; }
    const da = el("mdblist_disconnect_api");
    if (da && !da.__wired) { da.addEventListener("click", onDisc); da.__wired = true; }
    const k = el("mdblist_key");
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
    const sel = el("mdblist_instance");
    if (sel && !sel.__wiredMdbl) {
      sel.addEventListener("change", function () {
        setMDBListInstance(sel.value);
        methodOverride = "";
        stopPoll();
        void hydrate();
      });
      sel.__wiredMdbl = true;
    }
    const btnNew = el("mdblist_instance_new");
    if (btnNew && !btnNew.__wiredMdbl) {
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
      btnNew.__wiredMdbl = true;
    }
    const btnDel = el("mdblist_instance_del");
    if (btnDel && !btnDel.__wiredMdbl) {
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
      btnDel.__wiredMdbl = true;
    }
  }

  function watch() {
    const host = document.getElementById("auth-providers");
    if (!host || watch._obs) return;
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
    if (!cfg) return;

    const inst = getMDBListInstance();
    cfg.mdblist = cfg.mdblist || {};
    const target = inst === "default"
      ? cfg.mdblist
      : ((cfg.mdblist.instances = cfg.mdblist.instances || {}), (cfg.mdblist.instances[inst] = cfg.mdblist.instances[inst] || {}));

    const method = el("mdblist_auth_method")?.value === "api_key" ? "api_key" : "device_code";
    target.auth_method = method;
    const keyState = readSecretField(el("mdblist_key"));
    if (method === "api_key" && keyState.value) target.api_key = keyState.value;
  });

  window.initMDBListAuthUI = boot;
  boot();
})();
