// auth.simkl.js
(function (w, d) {
  const Shared = w.CW && w.CW.AuthShared;
  const $ = (s) => d.getElementById(s);
  const q = (sel, root = d) => root.querySelector(sel);
  const notify = Shared.notify;
  const bust = () => `?ts=${Date.now()}`;

  // Profiles
  const profile = Shared.createProfileAdapter({
    provider: "simkl",
    configKey: "simkl",
    label: "SIMKL",
    sectionId: "sec-simkl",
    selectId: "simkl_instance",
    storageKey: "cw.ui.simkl.auth.instance.v1",
    panelSelector: '#sec-simkl .cw-meta-provider-panel[data-provider="simkl"], .cw-meta-provider-panel[data-provider="simkl"]',
    title: "Select which SIMKL account this config applies to.",
  });
  const _str = Shared.txt;
  const _setVal = (id, v) => { const e = $(id); if (e && String(e.value || "") !== String(v || "")) e.value = String(v || ""); };

  function getSimklInstance() {
    return profile ? profile.getInstance() : "default";
  }
  function setSimklInstance(id) {
    if (profile) profile.setInstance(id);
  }
  function simklApi(url) {
    return profile ? profile.api(url) : String(url || "");
  }
  async function refreshSimklInstanceOptions(preserve) {
    if (profile) await profile.refreshOptions(preserve);
  }
  function ensureSimklInstanceUI() {
    profile?.ensureUI(() => { void hydrateSimklFromConfig(); });
  }

  let _simklPersistT = null;
  async function persistSimklClientFields() {
    try {
      const cid = _str($("simkl_client_id")?.value);
      const sec = _str($("simkl_client_secret")?.value);
      const cfg = await Shared.getConfig();
      if (!cfg) return;
      const block = profile ? profile.cfgBlock(cfg, true) : (cfg.simkl = cfg.simkl || {});
      block.client_id = cid;
      block.client_secret = sec;
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
      const blk = profile ? profile.cfgBlock(cfg, false) : ((inst === "default") ? base : (base.instances && base.instances[inst]) || {});
      const isDefault = (inst === "default");
      _setVal("simkl_client_id", _str(blk.client_id || (isDefault ? base.client_id : "")));
      _setVal("simkl_client_secret", _str(blk.client_secret || (isDefault ? base.client_secret : "")));
      const tok = _str(blk.access_token || (isDefault ? (cfg?.auth?.simkl?.access_token || "") : ""));
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
    return Shared.setStatusPill("simkl_msg", kind, text);
  }

  function setSimklSuccess(on, text) {
    if (on) setSimklBanner("ok", text || "Connected");
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
    return Shared.copyText(computeRedirect(), $("btn-copy-simkl-redirect"), { successMessage: "Redirect URI copied" });
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
        try { setSimklSuccess(false); } catch {}
        notify("SIMKL disconnected.");
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
    try { ensureSimklInstanceUI(); } catch (_) {}
    try { refreshSimklInstanceOptions(true); } catch (_) {}
    try { hydrateSimklFromConfig(); } catch (_) {}

    const cid = $("simkl_client_id");
    if (cid && !cid.__simklBound) { cid.addEventListener("input", updateSimklButtonState); cid.__simklBound = true; }
    const sec = $("simkl_client_secret");
    if (sec && !sec.__simklBound) { sec.addEventListener("input", updateSimklButtonState); sec.__simklBound = true; }
    const copy = $("btn-copy-simkl-redirect");
    if (copy && !copy.__simklBound) { copy.addEventListener("click", copyRedirect); copy.__simklBound = true; }
    const connect = $("btn-connect-simkl");
    if (connect && !connect.__simklBound) { connect.addEventListener("click", startSimkl); connect.__simklBound = true; }
    const del = $("btn-delete-simkl");
    if (del && !del.__simklBound) { del.addEventListener("click", simklDeleteToken); del.__simklBound = true; }

    const rid = $("redirect_uri_preview");
    if (rid) rid.textContent = computeRedirect();

    updateSimklButtonState();
    __simklInitDone = true;
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
