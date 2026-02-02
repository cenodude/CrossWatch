// auth.simkl.js
(function (w, d) {
  const $ = (s) => d.getElementById(s);
  const q = (sel, root = d) => root.querySelector(sel);
  const notify = w.notify || ((m) => console.log("[notify]", m));
  const bust = () => `?ts=${Date.now()}`;

  // Profiles
  const SIMKL_INSTANCE_KEY = "cw.ui.simkl.auth.instance.v1";
  const _str = (v) => (typeof v === "string" ? v.trim() : (v == null ? "" : String(v).trim()));
  const _setVal = (id, v) => { const e = $(id); if (e && String(e.value || "") !== String(v || "")) e.value = String(v || ""); };

  function getSimklInstance() {
    try { return (_str(localStorage.getItem(SIMKL_INSTANCE_KEY)) || "default"); } catch (_) { return "default"; }
  }
  function setSimklInstance(id) {
    try { localStorage.setItem(SIMKL_INSTANCE_KEY, _str(id) || "default"); } catch (_) {}
  }
  function simklApi(url) {
    try {
      const u = new URL(url, d.baseURI);
      u.searchParams.set("instance", getSimklInstance());
      u.searchParams.set("ts", Date.now());
      return u.toString();
    } catch (_) {
      const sep = String(url).includes("?") ? "&" : "?";
      return String(url) + sep + "instance=" + encodeURIComponent(getSimklInstance()) + "&ts=" + Date.now();
    }
  }
  async function refreshSimklInstanceOptions(preserve) {
    const sel = $("simkl_instance");
    if (!sel) return;
    let want = preserve === false ? "default" : getSimklInstance();
    try {
      const r = await fetch("/api/provider-instances/simkl?ts=" + Date.now(), { cache: "no-store" });
      const arr = await r.json().catch(() => []);
      const opts = Array.isArray(arr) ? arr : [];
      sel.innerHTML = "";
      const addOpt = (id, label) => {
        const o = d.createElement("option");
        o.value = String(id);
        o.textContent = String(label || id);
        sel.appendChild(o);
      };
      addOpt("default", "Default");
      opts.forEach((o) => { if (o && o.id && o.id !== "default") addOpt(o.id, o.label || o.id); });
      if (!Array.from(sel.options).some((o) => o.value === want)) want = "default";
      sel.value = want;
      setSimklInstance(want);
    } catch (_) {}
  }
  function ensureSimklInstanceUI() {
    const panel = d.querySelector('#sec-simkl .cw-meta-provider-panel[data-provider="simkl"]') || d.querySelector('.cw-meta-provider-panel[data-provider="simkl"]') || d.querySelector('#sec-simkl .cw-meta-provider-panel') || d.querySelector('#sec-simkl') || d.querySelector('[data-provider="simkl"]');
    if (!panel) return;
    if (panel.querySelector('#simkl_instance')) return;
    const head = panel.querySelector('.cw-panel-head') || panel.querySelector('.panel-head') || panel.querySelector('header') || panel;
    if (head.__simklInstanceUI) return;
    head.__simklInstanceUI = true;

    const wrap = d.createElement('div');
    wrap.className = 'inline';
    wrap.style.display = 'flex';
    wrap.style.gap = '8px';
    wrap.style.alignItems = 'center';
    try { head.style.flexWrap = 'wrap'; } catch (_) {}
    wrap.style.marginLeft = 'auto';
    wrap.style.flexWrap = 'nowrap';
    wrap.title = 'Select which SIMKL account this config applies to.';

    const label = d.createElement('span');
    label.className = 'muted';
    label.textContent = 'Profile';

    const sel = d.createElement('select');
    sel.id = 'simkl_instance';
    sel.className = 'input';
    sel.style.minWidth = '160px';
    // Match Trakt: keep it compact and let content drive the width.
    sel.style.width = 'auto';
    sel.style.maxWidth = '220px';
    sel.style.flex = '0 0 auto';
    sel.style.display = 'inline-block';

    const btnNew = d.createElement('button');
    btnNew.type = 'button';
    btnNew.className = 'btn secondary';
    btnNew.textContent = 'New';

    const btnDel = d.createElement('button');
    btnDel.type = 'button';
    btnDel.className = 'btn secondary';
    btnDel.textContent = 'Delete';

    wrap.appendChild(label);
    wrap.appendChild(sel);
    wrap.appendChild(btnNew);
    wrap.appendChild(btnDel);
    if (head === panel) panel.insertBefore(wrap, panel.firstChild);
    else head.appendChild(wrap);

    sel.addEventListener("change", async () => {
      setSimklInstance(sel.value);
      await hydrateSimklFromConfig();
    });

    btnNew.addEventListener("click", async () => {
      try {
        const r = await fetch("/api/provider-instances/simkl/next?ts=" + Date.now(), { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}", cache: "no-store" });
        const j = await r.json().catch(() => ({}));
        const inst = _str(j && j.id);
        if (!r.ok || (j && j.ok === false) || !inst) throw new Error(String((j && j.error) || "create_failed"));
        setSimklInstance(inst);
        await refreshSimklInstanceOptions(true);
        await hydrateSimklFromConfig();
      } catch (e) {
        notify("Could not create profile: " + (e && e.message ? e.message : e));
      }
    });

    btnDel.addEventListener("click", async () => {
      const id = getSimklInstance();
      if (id === "default") return notify("Default profile cannot be deleted.");
      if (!confirm('Delete SIMKL profile "' + id + '"?')) return;
      try {
        const r = await fetch("/api/provider-instances/simkl/" + encodeURIComponent(id), { method: "DELETE", cache: "no-store" });
        const j = await r.json().catch(() => ({}));
        if (!r.ok || (j && j.ok === false)) throw new Error(String((j && j.error) || "delete_failed"));
        setSimklInstance("default");
        await refreshSimklInstanceOptions(false);
        await hydrateSimklFromConfig();
      } catch (e) {
        notify("Could not delete profile: " + (e && e.message ? e.message : e));
      }
    });
  }

  let _simklPersistT = null;
  async function persistSimklClientFields() {
    try {
      const cid = _str($("simkl_client_id")?.value);
      const sec = _str($("simkl_client_secret")?.value);
      const cfg = await fetch("/api/config", { cache: "no-store" }).then((r) => (r.ok ? r.json() : null)).catch(() => null);
      if (!cfg) return;
      const base = (cfg.simkl && typeof cfg.simkl === "object") ? cfg.simkl : (cfg.simkl = {});
      const inst = getSimklInstance();
      if (inst === "default") {
        base.client_id = cid;
        base.client_secret = sec;
      } else {
        if (!base.instances || typeof base.instances !== "object") base.instances = {};
        if (!base.instances[inst] || typeof base.instances[inst] !== "object") base.instances[inst] = {};
        base.instances[inst].client_id = cid;
        base.instances[inst].client_secret = sec;
      }
      await fetch("/api/config", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(cfg) }).catch(() => {});
    } catch (_) {}
  }
  function schedulePersistSimkl() {
    if (_simklPersistT) clearTimeout(_simklPersistT);
    _simklPersistT = setTimeout(persistSimklClientFields, 350);
  }

  async function hydrateSimklFromConfig() {
    try {
      const cfg = await fetch("/api/config" + bust(), { cache: "no-store", credentials: "same-origin" }).then((r) => (r.ok ? r.json() : null)).catch(() => null);
      if (!cfg) return;
      const inst = getSimklInstance();
      const base = (cfg.simkl && typeof cfg.simkl === "object") ? cfg.simkl : {};
      const blk = (inst === "default") ? base : (base.instances && base.instances[inst]) || {};
      const isDefault = (inst === "default");
      _setVal("simkl_client_id", _str(blk.client_id || (isDefault ? base.client_id : "")));
      _setVal("simkl_client_secret", _str(blk.client_secret || (isDefault ? base.client_secret : "")));
      const tok = _str(blk.access_token || (isDefault ? (cfg?.auth?.simkl?.access_token || "") : ""));
      _setVal("simkl_access_token", tok);
      try { setSimklSuccess(!!tok); } catch {}
      try { updateSimklButtonState(); } catch {}
    } catch (e) {
      console.warn("[simkl] hydrate failed", e);
    }
  }

  const computeRedirect = () =>
    (typeof w.computeRedirectURI === "function"
      ? w.computeRedirectURI()
      : (location.origin + "/callback"));

  function setSimklBanner(kind, text) {
    const el = $("simkl_msg");
    if (!el) return;
    el.classList.remove("hidden", "ok", "warn");
    if (!kind) { el.classList.add("hidden"); el.textContent = ""; return; }
    el.classList.add(kind);
    el.textContent = text || "";
  }

  function setSimklSuccess(on, text) {
    if (on) setSimklBanner("ok", text || "Connected.");
    else setSimklBanner(null, "");
  }

  // Keep SIMKL hint banner intact: only toggle visibility, do NOT replace its HTML/text
  function updateSimklButtonState() {
    try {
      const cid = ($("simkl_client_id")?.value || "").trim();
      const sec = ($("simkl_client_secret")?.value || "").trim();
      const btn = $("btn-connect-simkl") || $("simkl_start_btn") || $("btn_simkl_connect");
      const hint = $("simkl_hint");
      const rid = $("redirect_uri_preview");

      if (rid) rid.textContent = computeRedirect();

      const ok = cid.length > 0 && sec.length > 0;
      if (btn) btn.disabled = !ok;
      if (hint) hint.classList.toggle("hidden", ok);
    } catch (e) {
      console.warn("updateSimklButtonState failed", e);
    }
  }
  const updateSimklHint = updateSimklButtonState;

  async function copyRedirect() {
    try {
      await navigator.clipboard.writeText(computeRedirect());
      notify("Redirect URI copied ✓");
    } catch {
      notify("Copy failed");
    }
  }

  async function simklDeleteToken() {
    const btn = q("#sec-simkl .btn.danger");
    const msg = $("simkl_msg");
    if (btn) { btn.disabled = true; btn.classList.add("busy"); }
    if (msg) { msg.classList.remove("hidden"); msg.classList.remove("warn"); msg.textContent = ""; }
    try {
      const r = await fetch(simklApi("/api/simkl/token/delete"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
        cache: "no-store",
      });
      const j = await r.json().catch(() => ({}));
      if (r.ok && (j.ok !== false)) {
        try { const el = $("simkl_access_token"); if (el) el.value = ""; } catch {}
        try { setSimklSuccess(false); } catch {}
        notify("SIMKL access token removed.");
      } else {
        if (msg) { msg.classList.add("warn"); msg.textContent = "Could not remove token."; }
      }
    } catch {
      if (msg) { msg.classList.add("warn"); msg.textContent = "Error removing token."; }
    } finally {
      if (btn) { btn.disabled = false; btn.classList.remove("busy"); }
    }
  }

  let simklPoll = null;
  let simklVisHandler = null;

  async function startSimkl() {
    try { setSimklSuccess(false); } catch {}

    const cid = ($("simkl_client_id")?.value || "").trim();
    const sec = ($("simkl_client_secret")?.value || "").trim();
    if (!cid || !sec) {
      notify("Fill in SIMKL Client ID + Client Secret first");
      updateSimklButtonState();
      return;
    }

    let win = null;
    try { win = w.open("https://simkl.com/", "_blank"); } catch {}

    try { await w.saveSettings?.(); } catch {}

    const origin = location.origin;
    const j = await fetch(simklApi("/api/simkl/authorize"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({ origin }),
      cache: "no-store",
    }).then((r) => r.json()).catch(() => null);

    if (!j?.ok || !j.authorize_url) {
      const err = (j && (j.error || j.message)) ? String(j.error || j.message) : "SIMKL authorize failed";
      notify(err);
      try { if (win && !win.closed) win.close(); } catch {}
      return;
    }

    if (win && !win.closed) {
      try { win.location.href = j.authorize_url; win.focus(); } catch {}
    } else {
      notify("Popup blocked - allow popups and try again");
    }

    const cleanup = () => {
      if (simklPoll) { clearTimeout(simklPoll); simklPoll = null; }
      if (simklVisHandler) { d.removeEventListener("visibilitychange", simklVisHandler); simklVisHandler = null; }
    };
    cleanup();

    const startTs = Date.now();
    const deadline = startTs + 120000;
    const fastUntil = startTs + 30000; // 2s polling for ~30s
    const back = [5000, 7500, 10000, 15000, 20000, 20000];
    let i = 0;
    let inFlight = false;

    const poll = async () => {
      if (Date.now() >= deadline) { cleanup(); return; }

      const settingsVisible = !!($("page-settings") && !$("page-settings").classList.contains("hidden"));
      if (d.hidden || !settingsVisible) { simklPoll = setTimeout(poll, 5000); return; }
      if (inFlight) return;

      inFlight = true;
      let cfg = null;
      try {
        const r = await fetch("/api/config" + bust(), { cache: "no-store", credentials: "same-origin" });
        if (r.status === 401) { notify("Session expired - please log in again"); cleanup(); return; }
        cfg = await r.json();
      } catch {} finally { inFlight = false; }

      const inst = getSimklInstance();
      const base = (cfg?.simkl && typeof cfg.simkl === "object") ? cfg.simkl : {};
      const blk = (inst === "default") ? base : (base.instances && base.instances[inst]) || {};
      const tok = _str(blk.access_token || (inst === "default" ? (cfg?.auth?.simkl?.access_token || "") : ""));
      if (tok) {
        try { const el = $("simkl_access_token"); if (el) el.value = tok; } catch {}
        try { setSimklSuccess(true); } catch {}
        cleanup();
        return;
      }

      const delay = (Date.now() < fastUntil) ? 2000 : back[Math.min(i++, back.length - 1)];
      simklPoll = setTimeout(poll, delay);
    };

    simklVisHandler = () => {
      if (d.hidden) return;
      const settingsVisible = !!($("page-settings") && !$("page-settings").classList.contains("hidden"));
      if (!settingsVisible) return;
      if (!simklPoll) return;
      clearTimeout(simklPoll);
      simklPoll = null;
      void poll();
    };
    d.addEventListener("visibilitychange", simklVisHandler);

    simklPoll = setTimeout(poll, 2000);
  }

  let __simklInitDone = false;
  function initSimklAuthUI() {
    if (__simklInitDone) return;
    __simklInitDone = true;
    try { ensureSimklInstanceUI(); } catch (_) {}
    try { refreshSimklInstanceOptions(true); } catch (_) {}
    try { hydrateSimklFromConfig(); } catch (_) {}

    $("simkl_client_id")?.addEventListener("input", updateSimklButtonState);
    $("simkl_client_secret")?.addEventListener("input", updateSimklButtonState);

    const rid = $("redirect_uri_preview");
    if (rid) rid.textContent = computeRedirect();

    updateSimklButtonState();
  }

  if (d.readyState === "loading") d.addEventListener("DOMContentLoaded", initSimklAuthUI, { once: true });
  else initSimklAuthUI();

  w.cwAuth = w.cwAuth || {};
  w.cwAuth.simkl = w.cwAuth.simkl || {};
  w.cwAuth.simkl.init = initSimklAuthUI;

  Object.assign(w, {
    setSimklSuccess,
    updateSimklButtonState,
    updateSimklHint,
    startSimkl,
    copyRedirect,
    simklDeleteToken,
  });
})(window, document);
