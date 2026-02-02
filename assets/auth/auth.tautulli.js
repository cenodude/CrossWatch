// assets/auth/auth.tautulli.js
(function () {
  if (window._tautPatched) return;
  window._tautPatched = true;

  const el = (id) => document.getElementById(id);
  const txt = (v) => (typeof v === "string" ? v : "").trim();
  const note = (m) => (typeof window.notify === "function" ? window.notify(m) : void 0);

  const TAUTULLI_INSTANCE_KEY = "cw.ui.tautulli.auth.instance.v1";

  function getTautulliInstance() {
    var s = el("tautulli_instance");
    var v = s ? txt(s.value) : "";
    if (!v) { try { v = localStorage.getItem(TAUTULLI_INSTANCE_KEY) || ""; } catch (_) {} }
    v = txt(v) || "default";
    return v.toLowerCase() === "default" ? "default" : v;
  }

  function setTautulliInstance(v) {
    var id = txt(String(v || "")) || "default";
    try { localStorage.setItem(TAUTULLI_INSTANCE_KEY, id); } catch (_) {}
    var s = el("tautulli_instance");
    if (s) s.value = id;
  }

  function tautApi(path) {
    var p = String(path || "");
    var sep = p.indexOf("?") >= 0 ? "&" : "?";
    return p + sep + "instance=" + encodeURIComponent(getTautulliInstance()) + "&ts=" + Date.now();
  }

  async function fetchJSON(url, opts) {
    const r = await fetch(url, opts || {});
    let j = null;
    try { j = await r.json(); } catch {}
    return { ok: r.ok, data: j, status: r.status };
  }

  async function getCfg() {
    const r = await fetchJSON("/api/config?ts=" + Date.now(), { cache: "no-store" });
    return r.ok ? (r.data || {}) : {};
  }

  function getTautulliCfgBlock(cfg) {
    cfg = cfg || {};
    var base = (cfg.tautulli && typeof cfg.tautulli === "object") ? cfg.tautulli : (cfg.tautulli = {});
    var inst = getTautulliInstance();
    if (inst === "default") return base;
    if (!base.instances || typeof base.instances !== "object") base.instances = {};
    if (!base.instances[inst] || typeof base.instances[inst] !== "object") base.instances[inst] = {};
    return base.instances[inst];
  }

  async function refreshTautulliInstanceOptions(preserve) {
    var sel = el("tautulli_instance");
    if (!sel) return;
    var want = preserve === false ? "default" : getTautulliInstance();
    try {
      var r = await fetch("/api/provider-instances/tautulli?ts=" + Date.now(), { cache: "no-store" });
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
      setTautulliInstance(want);
    } catch (_) {}
  }

  function ensureTautulliInstanceUI() {
    var panel = document.querySelector('#sec-tautulli .cw-meta-provider-panel[data-provider="tautulli"]') || document.querySelector('#sec-tautulli .cw-meta-provider-panel') || document.querySelector('#sec-tautulli');
    var head = panel ? panel.querySelector('.cw-panel-head') : null;
    if (!head || head.__tautulliInstanceUI) return;
    head.__tautulliInstanceUI = true;

    var wrap = document.createElement('div');
    wrap.className = 'inline';
    wrap.style.display = 'flex';
    wrap.style.gap = '8px';
    wrap.style.alignItems = 'center';
    wrap.style.marginLeft = 'auto';
    wrap.style.flexWrap = 'nowrap';
    wrap.title = 'Select which Tautulli server this config applies to.';

    var lab = document.createElement('span');
    lab.className = 'muted';
    lab.textContent = 'Profile';

    var sel = document.createElement('select');
    sel.id = 'tautulli_instance';
    sel.className = 'input';
    sel.style.minWidth = '160px';

    // Match Trakt: keep it compact and let content drive the width.
    sel.style.width = 'auto';
    sel.style.maxWidth = '220px';
    sel.style.flex = '0 0 auto';
    var btnNewEl = document.createElement('button');
    btnNewEl.type = 'button';
    btnNewEl.className = 'btn secondary';
    btnNewEl.id = 'tautulli_instance_new';
    btnNewEl.textContent = 'New';

    var btnDelEl = document.createElement('button');
    btnDelEl.type = 'button';
    btnDelEl.className = 'btn secondary';
    btnDelEl.id = 'tautulli_instance_del';
    btnDelEl.textContent = 'Delete';

    wrap.appendChild(lab);
    wrap.appendChild(sel);
    wrap.appendChild(btnNewEl);
    wrap.appendChild(btnDelEl);
    head.appendChild(wrap);

    refreshTautulliInstanceOptions(true);

    if (sel && !sel._wired) {
      sel._wired = true;
      sel.addEventListener('change', function () {
        setTautulliInstance(sel.value);
        void hydrate();
      });
    }

    var btnNew = el("tautulli_instance_new");
    if (btnNew && !btnNew._wired) {
      btnNew._wired = true;
      btnNew.addEventListener("click", async function () {
        try {
          var r = await fetch("/api/provider-instances/tautulli/next?ts=" + Date.now(), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: "{}",
            cache: "no-store",
          });
          var j = await r.json().catch(function(){ return {}; });
          var id = txt((j && j.id) || "");
          if (!r.ok || (j && j.ok === false) || !id) throw new Error(String((j && j.error) || "create_failed"));
          setTautulliInstance(id);
          await refreshTautulliInstanceOptions(true);
          void hydrate();
        } catch (e) {
          note("Could not create profile: " + (e && e.message ? e.message : e));
        }
      });
    }

    var btnDel = el("tautulli_instance_del");
    if (btnDel && !btnDel._wired) {
      btnDel._wired = true;
      btnDel.addEventListener("click", async function () {
        var id = getTautulliInstance();
        if (id === "default") return note("Default profile cannot be deleted.");
        if (!confirm('Delete Tautulli profile "' + id + '"?')) return;
        try {
          var r = await fetch("/api/provider-instances/tautulli/" + encodeURIComponent(id), {
            method: "DELETE",
            cache: "no-store",
          });
          var j = await r.json().catch(function(){ return {}; });
          if (!r.ok || (j && j.ok === false)) throw new Error(String((j && j.error) || "delete_failed"));
          setTautulliInstance("default");
          await refreshTautulliInstanceOptions(false);
          void hydrate();
        } catch (e) {
          note("Could not delete profile: " + (e && e.message ? e.message : e));
        }
      });
    }
  }

  function setConn(ok, msg) {
    const m = el("tautulli_msg");
    if (!m) return;
    const text = ok ? (msg || "Connected.") : (msg || "Not connected");
    m.textContent = text;
    m.classList.remove("hidden", "ok", "warn");
    m.classList.add(ok ? "ok" : "warn");
  }

  function pickFieldBox(input) {
    if (!input) return null;
    let cur = input;
    for (let i = 0; i < 6; i++) {
      const p = cur && cur.parentElement;
      if (!p) break;
      const fields = p.querySelectorAll("input,select,textarea");
      if (fields.length === 1 && p.contains(input)) return p;
      cur = p;
    }
    return input.parentElement;
  }

  function ensureFieldsLayout() {
    const root = document.getElementById("sec-tautulli") || document.querySelector("#sec-tautulli");
    if (!root || root.__cwTautulliFieldsV2) return;

    const serverEl = el("tautulli_server");
    const keyEl = el("tautulli_key");
    const userEl = el("tautulli_user_id");
    const hintEl = el("tautulli_hint");
    if (!serverEl || !keyEl || !userEl) return;
    if (!root.contains(serverEl) || !root.contains(keyEl) || !root.contains(userEl)) return;

    const outerGrid = root.querySelector('.body .grid2');
    if (outerGrid) {
      outerGrid.style.gridTemplateColumns = "1fr";
      const rightCol = outerGrid.querySelector('div:nth-child(2)');
      if (rightCol) rightCol.style.display = "none";
    }

    const leftCol = root.querySelector('.body .grid2 > div:first-child') || root.querySelector('.body');
    if (!leftCol) return;

    const actions = document.getElementById("tautulli_actions_row");
    const saveBtn = el("tautulli_save");
    const verifyBtn = el("tautulli_verify");
    const discBtn = el("tautulli_disconnect");
    const msgEl = el("tautulli_msg");
    const saved = {
      server: serverEl,
      key: keyEl,
      user: userEl,
      hint: hintEl,
      actions,
      saveBtn,
      verifyBtn,
      discBtn,
      msgEl,
    };

    function mkField(labelText, inputEl) {
      const w = document.createElement("div");
      const lab = document.createElement("label");
      lab.textContent = labelText;
      w.appendChild(lab);
      w.appendChild(inputEl);
      return w;
    }

    try {
      // Rebuild the left column to avoid earlier DOM surgery leaving broken wrappers.
      leftCol.innerHTML = "";

      const row1 = document.createElement("div");
      row1.id = "tautulli_row1";
      row1.style.display = "grid";
      row1.style.gridTemplateColumns = "1fr 1fr";
      row1.style.gap = "12px";

      saved.server.style.width = "100%";
      saved.key.style.width = "100%";

      row1.appendChild(mkField("Server URL", saved.server));
      row1.appendChild(mkField("API Key", saved.key));
      leftCol.appendChild(row1);

      const userWrap = mkField("User ID (optional)", saved.user);
      saved.user.style.maxWidth = "240px";
      saved.user.style.width = "240px";
      userWrap.style.maxWidth = "240px";
      userWrap.style.marginTop = "10px";
      leftCol.appendChild(userWrap);

      if (saved.hint) {
        saved.hint.style.marginTop = "10px";
        leftCol.appendChild(saved.hint);
      }

      if (saved.actions) {
        saved.actions.style.marginTop = "12px";
        leftCol.appendChild(saved.actions);
      } else if (saved.saveBtn && saved.verifyBtn && saved.discBtn && saved.msgEl) {
        const row = document.createElement("div");
        row.id = "tautulli_actions_row";
        row.className = "inline";
        row.style.display = "flex";
        row.style.gap = "10px";
        row.style.alignItems = "center";
        row.style.marginTop = "12px";
        saved.msgEl.style.marginLeft = "auto";
        row.appendChild(saved.saveBtn);
        row.appendChild(saved.verifyBtn);
        row.appendChild(saved.discBtn);
        row.appendChild(saved.msgEl);
        leftCol.appendChild(row);

        const oldInline = saved.verifyBtn.closest(".inline") || saved.verifyBtn.parentElement;
        const oldCol = oldInline ? oldInline.parentElement : null;
        if (oldCol && oldCol !== row && root.contains(oldCol)) oldCol.style.display = "none";
      }

      root.__cwTautulliFieldsV2 = true;
    } catch (_) {}
  }

  function compactLayout() {
    const root = document.getElementById("sec-tautulli") || document.querySelector("#sec-tautulli");
    if (!root || root.__cwTautulliCompact) return;

    const save = el("tautulli_save");
    const verify = el("tautulli_verify");
    const disc = el("tautulli_disconnect");
    const msg = el("tautulli_msg");
    if (!save || !verify || !disc || !msg) return;
    if (!root.contains(save) || !root.contains(verify) || !root.contains(disc) || !root.contains(msg)) return;

    // Capture the original status column so we can hide it after moving the controls.
    const oldInline = verify.closest(".inline") || verify.parentElement;
    const oldCol = oldInline ? oldInline.parentElement : null;

    const user = el("tautulli_user_id");
    let anchor = user ? (user.closest("div") || user.parentElement) : null;
    if (!anchor || !root.contains(anchor)) anchor = save.closest("div") || save.parentElement;

    const row = document.createElement("div");
    row.id = "tautulli_actions_row";
    row.className = "inline";
    row.style.display = "flex";
    row.style.gap = "10px";
    row.style.alignItems = "center";
    row.style.marginTop = "12px";

    msg.style.marginLeft = "auto";

    // Move buttons into a single, Trakt-style action row.
    row.appendChild(save);
    row.appendChild(verify);
    row.appendChild(disc);
    row.appendChild(msg);
    anchor.insertAdjacentElement("afterend", row);

    if (oldCol && oldCol !== row && root.contains(oldCol)) oldCol.style.display = "none";

    root.__cwTautulliCompact = true;
  }

  function maskKey(i, has) {
    if (!i) return;
    if (has) { i.value = "••••••••"; i.dataset.masked = "1"; }
    else { i.value = ""; i.dataset.masked = "0"; }
    i.dataset.hasKey = has ? "1" : "";
  }

  async function refresh() {
    try {
      const r = await fetchJSON(tautApi("/api/tautulli/status?verify=1"), { cache: "no-store" });
      const ok = !!(r.ok && r.data && r.data.connected);
      setConn(ok, ok ? "Connected" : (r.data && r.data.reason ? String(r.data.reason) : "Not connected"));
      note(ok ? "Tautulli verified ✓" : "Tautulli not connected");
    } catch {
      setConn(false, "Verify failed");
      note("Tautulli verify failed");
    }
  }

  async function hydrate() {
    ensureTautulliInstanceUI();
    compactLayout();
    ensureFieldsLayout();
    const cfg = window._cfgCache || await getCfg();
    const t = getTautulliCfgBlock(cfg);
    const h = t && typeof t.history === "object" ? t.history : {};

    const server = txt((t && t.server_url) || "");
    const hasKey = !!txt((t && t.api_key) || "");
    const userId = txt((h && h.user_id) || "");

    if (el("tautulli_server")) el("tautulli_server").value = server;
    if (el("tautulli_user_id")) el("tautulli_user_id").value = userId;

    maskKey(el("tautulli_key"), hasKey);
    el("tautulli_hint")?.classList.toggle("hidden", hasKey);

    await refresh();
  }

  async function onSave() {
    const server = txt(el("tautulli_server")?.value || "");
    const keyInput = el("tautulli_key");
    const key = txt(keyInput?.value || "");
    const user_id = txt(el("tautulli_user_id")?.value || "");

    if (!server) { note("Enter Tautulli server URL"); return; }

    if (!key && !(keyInput && keyInput.dataset.hasKey === "1")) {
      note("Enter your Tautulli API key");
      return;
    }

    try {
      const r = await fetchJSON(tautApi("/api/tautulli/save"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ server_url: server, api_key: key, user_id }),
      });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(r.data?.error || "save_failed");

      if (key) maskKey(keyInput, true);
      el("tautulli_hint")?.classList.add("hidden");
      note("Tautulli saved");
      await refresh();
    } catch (e) {
      note("Saving Tautulli failed" + (e && e.message ? ": " + e.message : ""));
    }
  }

  async function onDisc() {
    try {
      const r = await fetchJSON(tautApi("/api/tautulli/disconnect"), { method: "POST" });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(r.data?.error || "disconnect_failed");

      maskKey(el("tautulli_key"), false);
      el("tautulli_hint")?.classList.remove("hidden");
      setConn(false);
      note("Tautulli disconnected");
    } catch (e) {
      note("Tautulli disconnect failed" + (e && e.message ? ": " + e.message : ""));
    }
  }

  function wire() {
    compactLayout();
    ensureFieldsLayout();
    const s = el("tautulli_save");
    if (s && !s.__wired) { s.addEventListener("click", onSave); s.__wired = true; }

    const v = el("tautulli_verify");
    if (v && !v.__wired) { v.addEventListener("click", refresh); v.__wired = true; }

    const d = el("tautulli_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisc); d.__wired = true; }

    const k = el("tautulli_key");
    if (k && !k.__wiredSecret) {
      k.addEventListener("focus", () => {
        if (k.dataset.masked === "1") {
          k.value = "";
          k.dataset.masked = "0";
          k.dataset.touched = "1";
        }
      });
      k.addEventListener("input", () => {
        k.dataset.masked = "0";
        k.dataset.touched = "1";
        k.dataset.hasKey = "";
      });
      k.__wiredSecret = true;
    }

    const u = el("tautulli_user_id");
    if (u && !u.__wiredUser) {
      u.addEventListener("input", () => { u.dataset.touched = "1"; });
      u.__wiredUser = true;
    }

    // Keep layout consistent if the section is re-rendered.
    compactLayout();
  }

  function watch() {
    const host = document.getElementById("auth-providers");
    if (!host || watch._obs) return;
    watch._obs = new MutationObserver(() => wire());
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
    const cfg = ev?.detail?.cfg;
    if (!cfg) return;

    const inst = getTautulliInstance();

    const server = txt(el("tautulli_server")?.value || "");
    const keyEl = el("tautulli_key");
    let key = txt(keyEl?.value || "");

    const userIdEl = el("tautulli_user_id");
    const user_id = txt(userIdEl?.value || "");
    const uidTouched = !!userIdEl?.dataset.touched;

    if (keyEl && (keyEl.dataset.masked === "1" || key === "••••••••" || key === "********" || key === "**********")) {
      key = "";
    }

    if (!server && !key && !user_id && !uidTouched) return;

    cfg.tautulli = cfg.tautulli || {};
    let t = cfg.tautulli;
    if (inst !== "default") {
      t.instances = t.instances || {};
      t.instances[inst] = t.instances[inst] || {};
      t = t.instances[inst];
    }

    if (server) t.server_url = server;
    if (key) t.api_key = key;

    if (user_id || uidTouched) {
      t.history = t.history || {};
      t.history.user_id = user_id;
    }
  });

  window.initTautulliAuthUI = boot;
  boot();
})();
