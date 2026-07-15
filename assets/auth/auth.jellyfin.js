// assets/auth/auth.jellyfin.js
(function () {
  "use strict";

  const Shared = window.CW && window.CW.AuthShared;
  const Q = (s, r = document) => r.querySelector(s);
  const Qa = (s, r = document) => Array.from(r.querySelectorAll(s) || []);
  const ESC = (s) => String(s || "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const SECTION = "#sec-jellyfin";
  const LIB_URL = "/api/jellyfin/libraries";

  let H = new Set();
  let R = new Set();
  let P = new Set();
  let S = new Set();
  let lastLibraries = [];
  let hydrated = false;
  let connected = false;

  const JFY_SUBTAB_KEY = "cw.ui.jellyfin.auth.subtab.v1";
  let jfyAutoTabInst = "";
  let jfyNewProfileInst = "";
  let jfyQuickSave = null;

  const _notify = Shared.notify;
  const jfyProfile = Shared.createProfileAdapter({
    provider: "jellyfin",
    configKey: "jellyfin",
    label: "Jellyfin",
    sectionId: "sec-jellyfin",
    selectId: "jellyfin_instance",
    storageKey: "cw.ui.jellyfin.auth.instance.v1",
    title: "Select which Jellyfin server/account this config applies to.",
  });

  function getJfyInstance() {
    return jfyProfile ? jfyProfile.getInstance() : "default";
  }

  function setJfyInstance(v) {
    if (jfyProfile) jfyProfile.setInstance(v);
  }

  function jfyApi(path) {
    return jfyProfile ? jfyProfile.api(path) : String(path || "");
  }

  function getJfyCfgBlock(cfg) {
    return jfyProfile ? jfyProfile.cfgBlock(cfg, true) : {};
  }

  async function refreshJfyInstanceOptions(preserve = true) {
    if (jfyProfile) await jfyProfile.refreshOptions(preserve);
  }

  function ensureJfyInstanceUI() {
    jfyProfile?.ensureUI(() => {
      try { jfyQcStop({ cancel: true }); } catch {}
      hydrated = false;
      hydrateFromConfig(true);
      jfyLoadLibraries?.();
    });
  }


  function jfyAuthSubSelect(tab, opts = {}) {
    const root = Q('#sec-jellyfin .cw-meta-provider-panel[data-provider="jellyfin"]') || Q("#sec-jellyfin .cw-panel");
    if (!root) return;

    const want = String(tab || "auth").toLowerCase();
    let sub = ["auth", "settings", "whitelist"].includes(want) ? want : "auth";
    const state = getJfySetupState();
    if (sub === "settings" && !state.settingsEnabled) sub = "auth";
    if (sub === "whitelist" && !state.whitelistEnabled) sub = state.settingsEnabled ? "settings" : "auth";
    try { Shared.applyMediaTabState(root, state); } catch {}

    root.querySelectorAll(".cw-subtile[data-sub]").forEach((btn) => {
      btn.classList.toggle("active", String(btn.dataset.sub || "").toLowerCase() === sub);
    });
    root.querySelectorAll(".cw-subpanel[data-sub]").forEach((sp) => {
      sp.classList.toggle("active", String(sp.dataset.sub || "").toLowerCase() === sub);
    });

    if (opts.persist !== false) {
      try { localStorage.setItem(JFY_SUBTAB_KEY, sub); } catch {}
    }

    if (sub === "whitelist") {
      try { jfyLoadLibraries(); } catch {}
    }
    if (sub !== "auth") { try { jfyQcStop({ cancel: true }); } catch {} }
    try { Shared.setMediaAuthStep(root, sub); } catch {}
  }

  function mountJfyAuthTabs() {
    const root = Q('#sec-jellyfin .cw-meta-provider-panel[data-provider="jellyfin"]');
    if (!root) return;
    try {
      Shared.mediaAuthGuide(root, {
        kind: "jellyfin",
        label: "Jellyfin",
        title: "Connect to Jellyfin",
        copy: "Enter your Jellyfin server URL, then connect with Quick Connect (recommended) or your username and password. Next, validate the settings, pick the user, and optionally whitelist libraries."
      });
      syncJfySetupTabs();
    } catch {}

    root.querySelectorAll(".cw-subtile[data-sub]").forEach((btn) => {
      if (btn.__jfyTabWired) return;
      btn.__jfyTabWired = true;
      btn.addEventListener("click", () => jfyAuthSubSelect(btn.dataset.sub));
    });

    if (root.__jfyTabsInit) return;
    root.__jfyTabsInit = true;

    let last = "auth";
    try { last = localStorage.getItem(JFY_SUBTAB_KEY) || "auth"; } catch {}
    jfyAuthSubSelect(last, { persist: false });
  }

  function getJfySetupState() {
    const server = (Q("#jfy_server_url")?.value || Q("#jfy_server")?.value || "").trim();
    const user = (Q("#jfy_username")?.value || Q("#jfy_user")?.value || "").trim();
    const userId = (Q("#jfy_user_id")?.value || "").trim();
    const configured = connected || !!(server || user || userId);
    return {
      configured,
      connected,
      settingsEnabled: connected,
      whitelistEnabled: connected,
    };
  }

  function syncJfySetupTabs(opts = {}) {
    const root = Q('#sec-jellyfin .cw-meta-provider-panel[data-provider="jellyfin"]') || Q("#sec-jellyfin .cw-panel");
    if (!root) return;
    const state = getJfySetupState();
    try { Shared.applyMediaTabState(root, state); } catch {}
    if (opts.auto) {
      const inst = getJfyInstance();
      if (jfyAutoTabInst !== inst) {
        jfyAutoTabInst = inst;
        const cur = root.querySelector(".cw-subtile.active[data-sub]")?.dataset?.sub || "auth";
        if (cur === "auth") jfyAuthSubSelect("auth", { persist: false });
        return;
      }
    }
    if (opts.preferSettings && state.settingsEnabled) {
      const cur = root.querySelector(".cw-subtile.active[data-sub]")?.dataset?.sub || "auth";
      if (cur === "auth") {
        jfyAuthSubSelect("settings", { persist: opts.persist !== false });
        return;
      }
    }
    const active = root.querySelector(".cw-subtile.active[data-sub]");
    jfyAuthSubSelect(active?.dataset?.sub || "auth", { persist: false });
  }

  async function persistJfyRuntimeSettings() {
    if (!getJfySetupState().configured) return null;
    jfyQuickSave ||= Shared.saveMergedConfig((cfg) => {
      mergeJellyfinIntoCfg(cfg);
    }).finally(() => { jfyQuickSave = null; });
    return jfyQuickSave;
  }


  const put = (sel, val) => { const el = Q(sel); if (el != null) el.value = (val ?? "") + ""; };
  const visible = (el) => !!el && getComputedStyle(el).display !== "none" && !el.hidden;

  function setMsgBanner(msg, kind, text) {
    return Shared.setStatusPill(msg, kind, text);
  }

  function connectedLabel(method) {
    const m = String(method || "").toLowerCase();
    if (m === "quick_connect") return "Connected using Quick Connect";
    if (m === "password") return "Connected using password";
    return "Connected";
  }

  function setConnected(has, method) {
    connected = !!has;
    const msg = Q("#jfy_msg");
    if (connected) {
      const m = method != null ? method : (getJfyCfgBlock(window.__cfg || {})?.auth_method);
      setMsgBanner(msg, "ok", connectedLabel(m));
      jfyQcStop({ cancel: true });
    } else {
      setMsgBanner(msg, null, "");
    }
    try { Shared.setConnectLocked(["btn-jfy-qc-start", "btn-jfy-qc-restart", "btn-jfy-login"], connected); } catch {}
    try { syncJfySetupTabs(); } catch {}
  }

  function syncHidden() {
    const selH = Q("#jfy_lib_history");
    const selR = Q("#jfy_lib_ratings");
    const selP = Q("#jfy_lib_progress");
    const selS = Q("#jfy_lib_scrobble");
    if (selH) selH.innerHTML = [...H].map(id => `<option selected value="${id}">${id}</option>`).join("");
    if (selR) selR.innerHTML = [...R].map(id => `<option selected value="${id}">${id}</option>`).join("");
    if (selP) selP.innerHTML = [...P].map(id => `<option selected value="${id}">${id}</option>`).join("");
    if (selS) selS.innerHTML = [...S].map(id => `<option selected value="${id}">${id}</option>`).join("");
  }

  function syncSelectAll() {
    const rows = Qa("#jfy_lib_matrix .lm-row:not(.hide)");
    const allHist = rows.length && rows.every(r => r.querySelector(".lm-dot.hist")?.classList.contains("on"));
    const allRate = rows.length && rows.every(r => r.querySelector(".lm-dot.rate")?.classList.contains("on"));
    const allProg = rows.length && rows.every(r => r.querySelector(".lm-dot.prog")?.classList.contains("on"));
    const allScr = rows.length && rows.every(r => r.querySelector(".lm-dot.scr")?.classList.contains("on"));
    const h = Q("#jfy_hist_all"), r = Q("#jfy_rate_all"), p = Q("#jfy_prog_all"), s = Q("#jfy_scr_all");
    if (h) { h.classList.toggle("on", !!allHist); h.setAttribute("aria-pressed", allHist ? "true" : "false"); }
    if (r) { r.classList.toggle("on", !!allRate); r.setAttribute("aria-pressed", allRate ? "true" : "false"); }
    if (p) { p.classList.toggle("on", !!allProg); p.setAttribute("aria-pressed", allProg ? "true" : "false"); }
    if (s) { s.classList.toggle("on", !!allScr); s.setAttribute("aria-pressed", allScr ? "true" : "false"); }
  }

  function renderLibraries(libs) {
    lastLibraries = Array.isArray(libs) ? libs : [];
    const box = Q("#jfy_lib_matrix"); if (!box) return;
    box.innerHTML = "";
    const f = document.createDocumentFragment();
    (Array.isArray(libs) ? libs : []).forEach((it) => {
      const id = String(it.key);
      const row = document.createElement("div");
      row.className = "lm-row"; row.dataset.id = id;
      row.innerHTML = `
        <div class="lm-name">${ESC(it.title)}</div>
        <button type="button" class="lm-dot hist${H.has(id) ? " on" : ""}" data-kind="history" aria-pressed="${H.has(id)}" title="Toggle History"></button>
        <button type="button" class="lm-dot rate${R.has(id) ? " on" : ""}" data-kind="ratings" aria-pressed="${R.has(id)}" title="Toggle Ratings"></button>
        <button type="button" class="lm-dot prog${P.has(id) ? " on" : ""}" data-kind="progress" aria-pressed="${P.has(id)}" title="Toggle Progress"></button>
        <button type="button" class="lm-dot scr${S.has(id) ? " on" : ""}" data-kind="scrobble" aria-pressed="${S.has(id)}" title="Toggle Scrobble"></button>`;
      f.appendChild(row);
    });
    box.appendChild(f);
    syncHidden();
    syncSelectAll();
  }

  function repaint() {
    const libs = Qa("#jfy_lib_matrix .lm-row").map(r => ({
      key: r.dataset.id,
      title: r.querySelector(".lm-name")?.textContent || ""
    }));
    renderLibraries(libs);
  }

  async function jfyLoadLibraries(force = false) {
    if (!force && lastLibraries.length) {
      renderLibraries(lastLibraries);
      return;
    }
    try {
      await persistJfyRuntimeSettings();
      const r = await fetch(jfyApi(LIB_URL), { cache: "no-store" });
      const d = r.ok ? await r.json().catch(() => ({})) : {};
      const libs = Array.isArray(d?.libraries) ? d.libraries : (Array.isArray(d) ? d : []);
      renderLibraries(libs);
    } catch { renderLibraries([]); }
  }

  function jfySectionLooksEmpty() {
    const s1 = Q("#jfy_server") || Q("#jfy_server_url");
    const u1 = Q("#jfy_user") || Q("#jfy_username");
    const vals = [s1, u1].map(el => el ? String(el.value || "").trim() : "");
    return vals.every(v => !v);
  }

  async function hydrateFromConfig(force = false) {
    if (hydrated && !force) return;
    try {
      let cfg = !force && window.__cfg && Object.keys(window.__cfg).length ? window.__cfg : null;
      if (!cfg) {
        const r = await fetch("/api/config", { cache: "no-store" });
        if (!r.ok) return;
        cfg = await r.json();
      }
      window.__cfg = cfg;
      const jf = getJfyCfgBlock(cfg);

      put("#jfy_server", jf.server); put("#jfy_server_url", jf.server);
      put("#jfy_user", jf.user || jf.username); put("#jfy_username", jf.user || jf.username);
      put("#jfy_user_id", jf.user_id);
      const v1 = Q("#jfy_verify_ssl"), v2 = Q("#jfy_verify_ssl_dup");
      if (v1) v1.checked = !!jf.verify_ssl;
      if (v2) v2.checked = !!jf.verify_ssl;
      setConnected(!!(jf.access_token || "").trim(), jf.auth_method);
      try { initMethod(); } catch {}

      H = new Set((jf.history?.libraries || []).map(String));
      R = new Set((jf.ratings?.libraries || []).map(String));
      P = new Set((jf.progress?.libraries || []).map(String));
      S = new Set((jf.scrobble?.libraries || []).map(String));

      hydrated = true;
      if (lastLibraries.length) renderLibraries(lastLibraries);
      else await jfyLoadLibraries();
      syncJfySetupTabs({ auto: true });
    } catch { }
  }

  function ensureHydrate() {
    try { mountJfyAuthTabs(); } catch {}
    try { ensureJfyInstanceUI(); } catch {}
    const sec = Q(SECTION);
    const body = sec?.querySelector(".body");
    if (!sec || (body && !visible(body))) return;
    const force = jfySectionLooksEmpty();
    hydrateFromConfig(force);
  }

  if (!Q(SECTION)) {
    const mo = new MutationObserver(() => {
      if (Q(SECTION)) { mo.disconnect(); ensureHydrate(); }
    });
    mo.observe(document.documentElement, { childList: true, subtree: true });
  }

  document.addEventListener("click", (e) => {
    const head = Q("#sec-jellyfin .head");
    if (head && head.contains(e.target)) setTimeout(ensureHydrate, 0);
  }, true);

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => setTimeout(ensureHydrate, 30));
  } else {
    setTimeout(ensureHydrate, 30);
  }

  async function jfyAuto() {
    try {
      const r = await fetch(jfyApi("/api/jellyfin/inspect"), { cache: "no-store" });
      if (!r.ok) return;
      const d = await r.json();
      if (d.server_url) { put("#jfy_server", d.server_url); put("#jfy_server_url", d.server_url); }
      if (d.username)   { put("#jfy_user", d.username);     put("#jfy_username", d.username); }
      if (d.user_id)    { put("#jfy_user_id", d.user_id); }
    } catch {}
  }

  async function jfyLogin() {
    const server = (Q("#jfy_server")?.value || "").trim();
    const username = (Q("#jfy_user")?.value || "").trim();
    const password = Q("#jfy_pass")?.value || "";
    const verify_ssl = !!(Q("#jfy_verify_ssl")?.checked || Q("#jfy_verify_ssl_dup")?.checked);
    const btn = Q("#btn-jfy-login"), msg = Q("#jfy_msg");
    if (btn) { btn.disabled = true; btn.classList.add("busy"); }
    setMsgBanner(msg, null, '');
    try {
      const r = await fetch(jfyApi("/api/jellyfin/login"), {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ server, username, password, verify_ssl }), cache: "no-store"
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok || j?.ok === false) { setMsgBanner(msg, 'warn', 'Login failed'); return; }
      put("#jfy_server_url", server); put("#jfy_username", username);
      if (j?.user_id) put("#jfy_user_id", j.user_id);
      setConnected(true, "password"); if (Q("#jfy_pass")) Q("#jfy_pass").value = "";
      await jfyLoadLibraries();
    } finally {
      if (btn) {
        btn.classList.remove("busy");
        if (connected) Shared.setConnectLocked(["btn-jfy-qc-start", "btn-jfy-qc-restart", "btn-jfy-login"], true);
        else btn.disabled = false;
      }
    }
  }

  async function jfyDeleteToken(delBtn) {
    delBtn = delBtn || document.querySelector('#sec-jellyfin .cw-jfy-delete');
    const msg = document.querySelector('#jfy_msg');
    if (delBtn) { delBtn.disabled = true; delBtn.classList.add('busy'); }
    if (msg) { msg.className = 'msg hidden'; msg.textContent = ''; }
    try {
      const r = await fetch(jfyApi("/api/jellyfin/token/delete"), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
        cache: 'no-store'
      });
      const j = await r.json().catch(() => ({}));
      if (r.ok && (j.ok !== false)) {
        const pass = document.querySelector('#jfy_pass'); if (pass) pass.value = '';
        setConnected(false);
        if (msg) { msg.className = 'msg'; msg.textContent = 'Disconnected'; }
        try { syncJfySetupTabs(); } catch {}
      } else {
        if (msg) { msg.className = 'msg warn'; msg.textContent = 'Could not remove token.'; }
      }
    } catch {
      if (msg) { msg.className = 'msg warn'; msg.textContent = 'Error removing token.'; }
    } finally {
      if (delBtn) { delBtn.disabled = false; delBtn.classList.remove('busy'); }
    }
  }

  function mergeJellyfinIntoCfg(cfg) {
    cfg = cfg || (window.__cfg ||= {});
    const v = (sel) => (Q(sel)?.value || "").trim();
    const jf = getJfyCfgBlock(cfg);
    const server = v("#jfy_server_url") || v("#jfy_server");
    const user = v("#jfy_username") || v("#jfy_user");
    if (server) jf.server = server;
    if (user) { jf.user = user; jf.username = user || jf.username || ""; }
    const uid = v("#jfy_user_id"); if (uid) jf.user_id = uid;
    if (hydrated) {
      const vs = Q("#jfy_verify_ssl"), vs2 = Q("#jfy_verify_ssl_dup");
      jf.verify_ssl = !!((vs && vs.checked) || (vs2 && vs2.checked));
      jf.history = Object.assign({}, jf.history || {}, { libraries: Array.from(H) });
      jf.ratings = Object.assign({}, jf.ratings || {}, { libraries: Array.from(R) });
      jf.progress = Object.assign({}, jf.progress || {}, { libraries: Array.from(P) });
      jf.scrobble = Object.assign({}, jf.scrobble || {}, { libraries: Array.from(S) });
    }
    return cfg;
  }

  document.addEventListener("click", (ev) => {
    const t = ev.target; if (!(t instanceof Element)) return;
    const btn = t.closest(".lm-dot") || t.closest(".lm-col")?.querySelector(".lm-dot"); if (!btn) return;

    if (btn.id === "jfy_hist_all") {
      ev.preventDefault(); ev.stopPropagation();
      const on = !btn.classList.contains("on"); btn.classList.toggle("on", on); btn.setAttribute("aria-pressed", on ? "true" : "false");
      H = new Set();
      Qa("#jfy_lib_matrix .lm-dot.hist").forEach((b) => {
        b.classList.toggle("on", on); b.setAttribute("aria-pressed", on ? "true" : "false");
        if (on) { const r = b.closest(".lm-row"); if (r) H.add(String(r.dataset.id || "")); }
      });
      syncHidden();
      repaint();
      syncSelectAll();
      return;
    }

    if (btn.id === "jfy_rate_all") {
      ev.preventDefault(); ev.stopPropagation();
      const on = !btn.classList.contains("on"); btn.classList.toggle("on", on); btn.setAttribute("aria-pressed", on ? "true" : "false");
      R = new Set();
      Qa("#jfy_lib_matrix .lm-dot.rate").forEach((b) => {
        b.classList.toggle("on", on); b.setAttribute("aria-pressed", on ? "true" : "false");
        if (on) { const r = b.closest(".lm-row"); if (r) R.add(String(r.dataset.id || "")); }
      });
      syncHidden();
      repaint();
      syncSelectAll();
      return;
    }

    if (btn.id === "jfy_scr_all") {
      ev.preventDefault(); ev.stopPropagation();
      const on = !btn.classList.contains("on"); btn.classList.toggle("on", on); btn.setAttribute("aria-pressed", on ? "true" : "false");
      S = new Set();
      Qa("#jfy_lib_matrix .lm-dot.scr").forEach((b) => {
        b.classList.toggle("on", on); b.setAttribute("aria-pressed", on ? "true" : "false");
        if (on) { const r = b.closest(".lm-row"); if (r) S.add(String(r.dataset.id || "")); }
      });
      syncHidden();
      repaint();
      syncSelectAll();
      return;
    }

    if (btn.id === "jfy_prog_all") {
      ev.preventDefault(); ev.stopPropagation();
      const on = !btn.classList.contains("on"); btn.classList.toggle("on", on); btn.setAttribute("aria-pressed", on ? "true" : "false");
      P = new Set();
      Qa("#jfy_lib_matrix .lm-dot.prog").forEach((b) => {
        b.classList.toggle("on", on); b.setAttribute("aria-pressed", on ? "true" : "false");
        if (on) { const r = b.closest(".lm-row"); if (r) P.add(String(r.dataset.id || "")); }
      });
      syncHidden();
      repaint();
      syncSelectAll();
      return;
    }

    if (btn.closest("#jfy_lib_matrix")) {
      ev.preventDefault(); ev.stopPropagation();
      const row = btn.closest(".lm-row"); if (!row) return;
      const id = String(row.dataset.id || ""), kind = btn.dataset.kind;
      const on = !btn.classList.contains("on");
      btn.classList.toggle("on", on); btn.setAttribute("aria-pressed", on ? "true" : "false");
      if (kind === "history") { (on ? H.add(id) : H.delete(id)); }
      else if (kind === "ratings") { (on ? R.add(id) : R.delete(id)); }
      else if (kind === "progress") { (on ? P.add(id) : P.delete(id)); }
      else if (kind === "scrobble") { (on ? S.add(id) : S.delete(id)); }
      syncHidden();
      repaint();
      syncSelectAll();
      return;
    }
  }, true);

  async function jfyPickUser(ev) {
    try { ev?.preventDefault?.(); } catch {}
    try { await persistJfyRuntimeSettings(); } catch {}
    if (!window.cwMediaUserPicker || typeof window.cwMediaUserPicker.open !== "function") {
      window.notify?.("User picker not available", "warn");
      return;
    }
    const inst = getJfyInstance();
    window.cwMediaUserPicker.open({
      provider: "jellyfin",
      instance: inst,
      anchorEl: Q("#jfy_pick_user") || null,
      title: "Pick Jellyfin user",
      onPick: (u) => {
        const id = String(u?.id || "").trim();
        const name = String(u?.name || "").trim();
        const idEl = Q("#jfy_user_id");
        if (idEl) idEl.value = id;
        const nameEl = Q("#jfy_username");
        if (nameEl && name) nameEl.value = name;
        const authNameEl = Q("#jfy_user");
        if (authNameEl && name) authNameEl.value = name;
        window.notify?.(name ? `Selected: ${name}` : "User selected", "ok");
      },
    });
  }

  document.addEventListener("click", (ev) => {
    const t = ev?.target;
    if (t && t.id === "jfy_pick_user") jfyPickUser(ev);
    if (t && t.id === "btn-jfy-login") jfyLogin();
    const delBtn = t instanceof Element ? t.closest(".cw-jfy-delete") : null;
    if (delBtn && Q("#sec-jellyfin")?.contains(delBtn)) jfyDeleteToken(delBtn);
    if (t && t.id === "btn-jfy-auto") jfyAuto();
    if (t && t.id === "btn-jfy-load-libraries") jfyLoadLibraries(true);
  }, true);

  document.addEventListener("change", (ev) => {
    const t = ev?.target;
    if (t && t.id === "jfy_verify_ssl_dup") {
      const primary = Q("#jfy_verify_ssl");
      if (primary) primary.checked = !!t.checked;
    }
  }, true);

  document.addEventListener("input", (ev) => {
    const id = ev?.target?.id || "";
    if (["jfy_server", "jfy_server_url", "jfy_user", "jfy_username", "jfy_user_id"].includes(id)) syncJfySetupTabs();
  }, true);

  document.addEventListener("cw-auth-profile-created", (ev) => {
    const provider = String(ev?.detail?.provider || "").toLowerCase();
    if (provider !== "jellyfin") return;
    jfyNewProfileInst = getJfyInstance();
    jfyAutoTabInst = jfyNewProfileInst;
    try { syncJfySetupTabs(); } catch {}
    try { jfyAuthSubSelect("auth", { persist: false }); } catch {}
  }, true);

  const JFY_METHOD_KEY = "cw.ui.jellyfin.auth.method.v1";
  const QC_POLL_MS = 1500;
  const QC_MAX_MS = 300000;
  let qcMethod = "quick";
  let qcTimer = null;
  let qcDeadline = 0;
  let qcDetectToken = 0;
  let jfyQcPoller = null;

  function paneFor(method) {
    return Q(`#sec-jellyfin .jfy-pane[data-method="${method}"]`);
  }

  let qcLastNote = { text: "", kind: null };

  function renderMethodNote() {
    const el = Q("#jfy_method_note"); if (!el) return;
    const show = qcMethod === "quick";
    el.textContent = show ? (qcLastNote.text || "") : "";
    el.classList.toggle("warn", show && qcLastNote.kind === "warn");
  }

  function selectMethod(method, opts = {}) {
    const want = method === "password" ? "password" : "quick";
    if (want !== "quick") jfyQcStop({ cancel: true });
    qcMethod = want;
    Qa("#sec-jellyfin .jfy-method").forEach((b) => {
      const on = String(b.dataset.method || "") === want;
      b.classList.toggle("active", on);
      b.setAttribute("aria-selected", on ? "true" : "false");
    });
    Qa("#sec-jellyfin [data-method-actions]").forEach((node) => {
      node.classList.toggle("hidden", String(node.dataset.methodActions || "") !== want);
    });
    const quick = paneFor("quick"), pass = paneFor("password");
    if (quick) quick.classList.toggle("hidden", want !== "quick");
    if (pass) pass.classList.toggle("hidden", want !== "password");
    if (opts.persist !== false) { try { localStorage.setItem(JFY_METHOD_KEY, want); } catch {} }
    renderMethodNote();
  }

  function setMethodNote(text, kind) {
    qcLastNote = { text: text || "", kind: kind || null };
    renderMethodNote();
  }

  function initMethod() {
    let stored = "quick"; try { stored = localStorage.getItem(JFY_METHOD_KEY) || "quick"; } catch {}
    selectMethod(stored, { persist: false });
    detectMethod();
  }

  async function detectMethod() {
    if (connected) { setMethodNote(""); return; }
    const server = (Q("#jfy_server")?.value || Q("#jfy_server_url")?.value || "").trim();
    if (!server) { setMethodNote(""); return; }
    const token = ++qcDetectToken;
    try {
      const r = await fetch(jfyApi("/api/jellyfin/quickconnect/available"), { cache: "no-store" });
      if (token !== qcDetectToken) return;
      const d = r.ok ? await r.json().catch(() => ({})) : {};
      if (connected || qcTimer) return;
      let stored = null; try { stored = localStorage.getItem(JFY_METHOD_KEY); } catch {}
      if (d.supported && d.enabled) {
        setMethodNote("");
        if (stored !== "password") selectMethod("quick", { persist: false });
      } else if (d.supported && !d.enabled) {
        setMethodNote("Quick Connect is disabled on this server. Use username & password.", "warn");
        selectMethod("password", { persist: false });
      } else if (d.reason && d.reason !== "missing_server") {
        setMethodNote("Could not verify Quick Connect support: " + (d.error || d.reason), "warn");
      } else {
        setMethodNote("");
      }
    } catch {
      if (token === qcDetectToken) setMethodNote("");
    }
  }

  function qcSetState(show) {
    const box = Q("#jfy_qc_state"); if (box) box.classList.toggle("hidden", !show);
    const start = Q("#btn-jfy-qc-start"), cancel = Q("#btn-jfy-qc-cancel"), restart = Q("#btn-jfy-qc-restart");
    if (start) start.classList.toggle("hidden", show);
    if (cancel) cancel.classList.toggle("hidden", !show);
    if (restart) restart.classList.add("hidden");
  }

  function qcShowRestart() {
    const restart = Q("#btn-jfy-qc-restart"), start = Q("#btn-jfy-qc-start"), cancel = Q("#btn-jfy-qc-cancel");
    if (restart) restart.classList.remove("hidden");
    if (start) start.classList.add("hidden");
    if (cancel) cancel.classList.add("hidden");
  }

  function qcUpdateTimer() {
    const el = Q("#jfy_qc_timer"); if (!el) return;
    const left = Math.max(0, Math.round((qcDeadline - Date.now()) / 1000));
    const mm = Math.floor(left / 60);
    const ss = String(left % 60).padStart(2, "0");
    el.textContent = left > 0 ? `Expires in ${mm}:${ss}` : "";
  }

  function jfyQcStop(opts = {}) {
    if (qcTimer) { clearInterval(qcTimer); qcTimer = null; }
    if (jfyQcPoller) jfyQcPoller.stop();
    qcDeadline = 0;
    qcSetState(false);
    if (opts.cancel) {
      try {
        fetch(jfyApi("/api/jellyfin/quickconnect/cancel"), {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: "{}", cache: "no-store", keepalive: true,
        });
      } catch {}
    }
  }

  function ensureJfyQcPoller() {
    if (jfyQcPoller) return jfyQcPoller;
    jfyQcPoller = Shared.createDevicePoll({
      url: () => jfyApi("/api/jellyfin/quickconnect/poll"),
      method: "GET",
      minIntervalMs: QC_POLL_MS,
      maxTotalMs: QC_MAX_MS,
      classify: (status, data) => {
        const state = data && data.state;
        if (state === "authorized") return { state: "authorized" };
        if (state === "waiting") return { state: "pending" };
        return { state: "terminal", message: (data && data.error) || "Quick Connect request expired. Restart to try again." };
      },
      onPending: () => { const s = Q("#jfy_qc_status"); if (s) s.textContent = "Waiting for authorization…"; },
      onAuthorized: async (data) => {
        jfyQcStop();
        if (data.server) put("#jfy_server_url", data.server);
        if (data.username) { put("#jfy_username", data.username); put("#jfy_user", data.username); }
        if (data.user_id) put("#jfy_user_id", data.user_id);
        window.__cfg = null;
        setConnected(true, "quick_connect");
        await jfyLoadLibraries(true);
      },
      onTerminal: (verdict) => {
        jfyQcStop();
        setMethodNote((verdict && verdict.message) || "Quick Connect request expired. Restart to try again.", "warn");
        qcShowRestart();
      },
      onTimeout: () => {
        jfyQcStop({ cancel: true });
        setMethodNote("Quick Connect timed out. Restart to try again.", "warn");
        qcShowRestart();
      },
    });
    return jfyQcPoller;
  }

  async function jfyQcStart() {
    const server = (Q("#jfy_server")?.value || "").trim();
    if (!server) { setMethodNote("Enter a server URL first.", "warn"); return; }
    const verify_ssl = !!(Q("#jfy_verify_ssl")?.checked || Q("#jfy_verify_ssl_dup")?.checked);
    jfyQcStop({ cancel: true });
    setMethodNote("");
    const start = Q("#btn-jfy-qc-start"); if (start) { start.disabled = true; start.classList.add("busy"); }
    try {
      const r = await fetch(jfyApi("/api/jellyfin/quickconnect/start"), {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ server, verify_ssl }), cache: "no-store",
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok || d?.ok === false) {
        setMethodNote(d?.error || "Could not start Quick Connect.", "warn");
        if (d?.reason === "disabled") selectMethod("password");
        return;
      }
      const code = Q("#jfy_qc_code"); if (code) code.textContent = d.code || "------";
      const st = Q("#jfy_qc_status"); if (st) st.textContent = "Waiting for authorization…";
      qcDeadline = Date.now() + Math.min(QC_MAX_MS, (Number(d.expires_in) || 600) * 1000);
      qcSetState(true);
      qcUpdateTimer();
      qcTimer = setInterval(qcUpdateTimer, 1000);
      ensureJfyQcPoller().start({ intervalMs: QC_POLL_MS, deadlineMs: qcDeadline });
    } finally {
      if (start) { start.disabled = false; start.classList.remove("busy"); }
    }
  }

  const QC_ICON_COPY = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>';
  const QC_ICON_CHECK = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>';
  let qcCopyRevert = null;

  async function jfyQcCopy(btn) {
    const code = (Q("#jfy_qc_code")?.textContent || "").replace(/\s+/g, "").trim();
    if (!code || code === "------") return;
    let ok = false;
    try { if (navigator.clipboard?.writeText) { await navigator.clipboard.writeText(code); ok = true; } } catch {}
    if (!ok) {
      try {
        const ta = document.createElement("textarea");
        ta.value = code; ta.style.position = "fixed"; ta.style.opacity = "0";
        document.body.appendChild(ta); ta.focus(); ta.select();
        ok = document.execCommand("copy");
        document.body.removeChild(ta);
      } catch {}
    }
    if (!ok) { window.notify?.("Copy failed", "warn"); return; }
    btn.classList.add("copied");
    btn.innerHTML = QC_ICON_CHECK;
    btn.title = "Copied!";
    if (qcCopyRevert) clearTimeout(qcCopyRevert);
    qcCopyRevert = setTimeout(() => {
      btn.classList.remove("copied");
      btn.innerHTML = QC_ICON_COPY;
      btn.title = "Copy code";
    }, 1400);
  }

  document.addEventListener("click", (ev) => {
    const t = ev?.target;
    if (!(t instanceof Element)) return;
    const copyBtn = t.closest("#jfy_qc_copy");
    if (copyBtn) { ev.preventDefault(); jfyQcCopy(copyBtn); return; }
    const methodBtn = t.closest(".jfy-method");
    if (methodBtn && Q("#sec-jellyfin")?.contains(methodBtn)) { ev.preventDefault(); selectMethod(methodBtn.dataset.method); return; }
    if (t.id === "btn-jfy-qc-start" || t.id === "btn-jfy-qc-restart") { jfyQcStart(); return; }
    if (t.id === "btn-jfy-qc-cancel") { jfyQcStop({ cancel: true }); setMethodNote(""); return; }
  }, true);

  document.addEventListener("change", (ev) => {
    const id = ev?.target?.id || "";
    if (id === "jfy_server" || id === "jfy_server_url") detectMethod();
  }, true);

  window.addEventListener("beforeunload", () => {
    if (qcTimer) { clearInterval(qcTimer); qcTimer = null; }
    if (jfyQcPoller) jfyQcPoller.stop();
  });

  window.cwAuth = window.cwAuth || {};
  window.cwAuth.jellyfin = window.cwAuth.jellyfin || {};
  window.cwAuth.jellyfin.init = ensureHydrate;

  window.jfyAuto = jfyAuto;
  window.jfyLoadLibraries = jfyLoadLibraries;
  window.mergeJellyfinIntoCfg = mergeJellyfinIntoCfg;
  window.jfyLogin = jfyLogin;
  window.jfyDeleteToken = jfyDeleteToken;
  window.persistJfyRuntimeSettings = persistJfyRuntimeSettings;

  window.registerSettingsCollector?.(mergeJellyfinIntoCfg);
  document.addEventListener("settings-collect", (e) => { try { mergeJellyfinIntoCfg(e?.detail?.cfg || (window.__cfg ||= {})); } catch {} }, true);
})();
