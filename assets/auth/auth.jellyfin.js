// assets/auth/auth.jellyfin.js
(function () {
  "use strict";

  const Q = (s, r = document) => r.querySelector(s);
  const Qa = (s, r = document) => Array.from(r.querySelectorAll(s) || []);
  const ESC = (s) => String(s || "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const SECTION = "#sec-jellyfin";
  const LIB_URL = "/api/jellyfin/libraries";

  let H = new Set();
  let R = new Set();
  let S = new Set();
  let hydrated = false;

  const JFY_SUBTAB_KEY = "cw.ui.jellyfin.auth.subtab.v1";

  const JFY_INSTANCE_KEY = "cw.ui.jellyfin.auth.instance.v1";
  const _notify = (m) => { try { if (typeof window.notify === "function") window.notify(m); } catch {} };

  function getJfyInstance() {
    const el = Q("#jellyfin_instance");
    let v = el ? String(el.value || "").trim() : "";
    if (!v) { try { v = localStorage.getItem(JFY_INSTANCE_KEY) || ""; } catch {} }
    v = (v || "").trim() || "default";
    return v.toLowerCase() === "default" ? "default" : v;
  }

  function setJfyInstance(v) {
    const id = (String(v || "").trim() || "default");
    try { localStorage.setItem(JFY_INSTANCE_KEY, id); } catch {}
    const el = Q("#jellyfin_instance");
    if (el) el.value = id;
  }

  function jfyApi(path) {
    const p = String(path || "");
    const sep = p.includes("?") ? "&" : "?";
    return p + sep + "instance=" + encodeURIComponent(getJfyInstance()) + "&ts=" + Date.now();
  }

  function getJfyCfgBlock(cfg) {
    cfg = cfg || {};
    const base = (cfg.jellyfin && typeof cfg.jellyfin === "object") ? cfg.jellyfin : (cfg.jellyfin = {});
    const inst = getJfyInstance();
    if (inst === "default") return base;
    if (!base.instances || typeof base.instances !== "object") base.instances = {};
    if (!base.instances[inst] || typeof base.instances[inst] !== "object") base.instances[inst] = {};
    return base.instances[inst];
  }

  async function refreshJfyInstanceOptions(preserve = true) {
    const sel = Q("#jellyfin_instance");
    if (!sel) return;
    let want = preserve ? getJfyInstance() : "default";
    try {
      const r = await fetch("/api/provider-instances/jellyfin?ts=" + Date.now(), { cache: "no-store" });
      const arr = await r.json().catch(() => []);
      const opts = Array.isArray(arr) ? arr : [];

      sel.innerHTML = "";
      const addOpt = (id, label) => {
        const o = document.createElement("option");
        o.value = String(id);
        o.textContent = String(label || id);
        sel.appendChild(o);
      };

      addOpt("default", "Default");
      opts.forEach((o) => { if (o && o.id && o.id !== "default") addOpt(o.id, o.label || o.id); });

      if (!Array.from(sel.options).some(o => o.value === want)) want = "default";
      sel.value = want;
      setJfyInstance(want);
    } catch {}
  }

  function ensureJfyInstanceUI() {
    const panel = Q('#sec-jellyfin .cw-meta-provider-panel[data-provider="jellyfin"]');
    const head = panel ? Q(".cw-panel-head", panel) : null;
    if (!head || head.__jfyInstanceUI) return;
    head.__jfyInstanceUI = true;

    const wrap = document.createElement("div");
    wrap.className = "inline";
    wrap.style.display = "flex";
    wrap.style.gap = "8px";
    wrap.style.alignItems = "center";

    const lab = document.createElement("span");
    lab.className = "muted";
    lab.textContent = "Profile";

    const sel = document.createElement("select");
    sel.id = "jellyfin_instance";
    sel.className = "input";
    sel.style.minWidth = "160px";

    const btnNew = document.createElement("button");
    btnNew.type = "button";
    btnNew.className = "btn secondary";
    btnNew.textContent = "New";

    const btnDel = document.createElement("button");
    btnDel.type = "button";
    btnDel.className = "btn secondary";
    btnDel.textContent = "Delete";

    wrap.appendChild(lab);
    wrap.appendChild(sel);
    wrap.appendChild(btnNew);
    wrap.appendChild(btnDel);

    head.appendChild(wrap);

    sel.addEventListener("change", () => {
      setJfyInstance(sel.value);
      hydrated = false;
      hydrateFromConfig(true);
      jfyLoadLibraries?.();
    });

    btnNew.addEventListener("click", async () => {
      try {
        const r = await fetch(`/api/provider-instances/jellyfin/next?ts=${Date.now()}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}", cache: "no-store" });
        const j = await r.json().catch(() => ({}));
        const id = String((j && j.id) || "").trim();
        if (!r.ok || (j && j.ok === false) || !id) { _notify("Could not create profile"); return; }
        setJfyInstance(id);
        await refreshJfyInstanceOptions(true);
        sel.value = id;
        hydrated = false;
        hydrateFromConfig(true);
      } catch { _notify("Could not create profile"); }
    });

    btnDel.addEventListener("click", async () => {
      const id = getJfyInstance();
      if (id === "default") { _notify("Default profile cannot be deleted"); return; }
      if (!confirm(`Delete Jellyfin profile '${id}'?`)) return;
      try {
        const r = await fetch(`/api/provider-instances/jellyfin/${encodeURIComponent(id)}`, { method: "DELETE", cache: "no-store" });
        const j = await r.json().catch(() => ({}));
        if (!(j && j.ok)) { _notify("Could not delete profile"); return; }
        await refreshJfyInstanceOptions(false);
        setJfyInstance("default");
        sel.value = "default";
        hydrated = false;
        hydrateFromConfig(true);
      } catch { _notify("Could not delete profile"); }
    });

    refreshJfyInstanceOptions(true);
    setTimeout(() => { try { sel.value = getJfyInstance(); } catch {} }, 0);
  }


  function jfyAuthSubSelect(tab, opts = {}) {
    const root = Q('#sec-jellyfin .cw-meta-provider-panel[data-provider="jellyfin"]') || Q("#sec-jellyfin .cw-panel");
    if (!root) return;

    const want = String(tab || "auth").toLowerCase();
    const sub = ["auth", "settings", "whitelist"].includes(want) ? want : "auth";

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
  }

  function mountJfyAuthTabs() {
    const root = Q('#sec-jellyfin .cw-meta-provider-panel[data-provider="jellyfin"]');
    if (!root) return;

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


  const put = (sel, val) => { const el = Q(sel); if (el != null) el.value = (val ?? "") + ""; };
  const maskToken = (has) => { const el = Q("#jfy_tok"); if (el) { el.value = has ? "••••••••" : ""; el.dataset.masked = has ? "1" : "0"; } };
  const visible = (el) => !!el && getComputedStyle(el).display !== "none" && !el.hidden;

  function setMsgBanner(msg, kind, text) {
    if (!msg) return;
    msg.classList.remove('hidden', 'ok', 'warn');
    if (!kind) { msg.classList.add('hidden'); msg.textContent = ''; return; }
    msg.classList.add(kind);
    msg.textContent = text || '';
  }

  function applyFilter() {
    const qv = (Q("#jfy_lib_filter")?.value || "").toLowerCase().trim();
    Qa("#jfy_lib_matrix .lm-row").forEach((r) => {
      const name = (r.querySelector(".lm-name")?.textContent || "").toLowerCase();
      r.classList.toggle("hide", !!qv && !name.includes(qv));
    });
  }

  function syncHidden() {
    const selH = Q("#jfy_lib_history");
    const selR = Q("#jfy_lib_ratings");
    const selS = Q("#jfy_lib_scrobble");
    if (selH) selH.innerHTML = [...H].map(id => `<option selected value="${id}">${id}</option>`).join("");
    if (selR) selR.innerHTML = [...R].map(id => `<option selected value="${id}">${id}</option>`).join("");
    if (selS) selS.innerHTML = [...S].map(id => `<option selected value="${id}">${id}</option>`).join("");
  }

  function syncSelectAll() {
    const rows = Qa("#jfy_lib_matrix .lm-row:not(.hide)");
    const allHist = rows.length && rows.every(r => r.querySelector(".lm-dot.hist")?.classList.contains("on"));
    const allRate = rows.length && rows.every(r => r.querySelector(".lm-dot.rate")?.classList.contains("on"));
    const allScr = rows.length && rows.every(r => r.querySelector(".lm-dot.scr")?.classList.contains("on"));
    const h = Q("#jfy_hist_all"), r = Q("#jfy_rate_all"), s = Q("#jfy_scr_all");
    if (h) { h.classList.toggle("on", !!allHist); h.setAttribute("aria-pressed", allHist ? "true" : "false"); }
    if (r) { r.classList.toggle("on", !!allRate); r.setAttribute("aria-pressed", allRate ? "true" : "false"); }
    if (s) { s.classList.toggle("on", !!allScr); s.setAttribute("aria-pressed", allScr ? "true" : "false"); }
  }

  function renderLibraries(libs) {
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
        <button type="button" class="lm-dot scr${S.has(id) ? " on" : ""}" data-kind="scrobble" aria-pressed="${S.has(id)}" title="Toggle Scrobble"></button>`;
      f.appendChild(row);
    });
    box.appendChild(f);
    applyFilter();
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

  async function jfyLoadLibraries() {
    try {
      const r = await fetch(jfyApi(LIB_URL), { cache: "no-store" });
      const d = r.ok ? await r.json().catch(() => ({})) : {};
      const libs = Array.isArray(d?.libraries) ? d.libraries : (Array.isArray(d) ? d : []);
      renderLibraries(libs);
    } catch { renderLibraries([]); }
  }

  function jfySectionLooksEmpty() {
    const s1 = Q("#jfy_server") || Q("#jfy_server_url");
    const u1 = Q("#jfy_user") || Q("#jfy_username");
    const tok = Q("#jfy_tok");
    const vals = [s1, u1, tok].map(el => el ? String(el.value || "").trim() : "");
    return vals.every(v => !v);
  }

  async function hydrateFromConfig(force = false) {
    if (hydrated && !force) return;
    try {
      const r = await fetch("/api/config", { cache: "no-store" });
      if (!r.ok) return;
      const cfg = await r.json();
      window.__cfg = cfg;
      const jf = getJfyCfgBlock(cfg);

      put("#jfy_server", jf.server); put("#jfy_server_url", jf.server);
      put("#jfy_user", jf.user || jf.username); put("#jfy_username", jf.user || jf.username);
      put("#jfy_user_id", jf.user_id);
      const v1 = Q("#jfy_verify_ssl"), v2 = Q("#jfy_verify_ssl_dup");
      if (v1) v1.checked = !!jf.verify_ssl;
      if (v2) v2.checked = !!jf.verify_ssl;
      maskToken(!!(jf.access_token || "").trim());

      H = new Set((jf.history?.libraries || []).map(String));
      R = new Set((jf.ratings?.libraries || []).map(String));
      S = new Set((jf.scrobble?.libraries || []).map(String));

      hydrated = true;
      await jfyLoadLibraries();
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
    const btn = Q("button.btn.jellyfin"), msg = Q("#jfy_msg");
    if (btn) { btn.disabled = true; btn.classList.add("busy"); }
    setMsgBanner(msg, null, '');
    try {
      const r = await fetch(jfyApi("/api/jellyfin/login"), {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ server, username, password }), cache: "no-store"
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok || j?.ok === false) { setMsgBanner(msg, 'warn', 'Login failed'); return; }
      put("#jfy_server_url", server); put("#jfy_username", username);
      if (j?.user_id) put("#jfy_user_id", j.user_id);
      maskToken(true); if (Q("#jfy_pass")) Q("#jfy_pass").value = "";
      setMsgBanner(msg, 'ok', 'Connected.');
      await jfyLoadLibraries();
    } finally { if (btn) { btn.disabled = false; btn.classList.remove("busy"); } }
  }

  async function jfyDeleteToken() {
    const delBtn = document.querySelector('#sec-jellyfin .btn.danger');
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
        const tok = document.querySelector('#jfy_tok'); if (tok) { tok.value = ''; tok.dataset.masked = '0'; }
        const pass = document.querySelector('#jfy_pass'); if (pass) pass.value = '';
        if (msg) { msg.className = 'msg'; msg.textContent = 'Access token removed.'; }
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

    if (btn.closest("#jfy_lib_matrix")) {
      ev.preventDefault(); ev.stopPropagation();
      const row = btn.closest(".lm-row"); if (!row) return;
      const id = String(row.dataset.id || ""), kind = btn.dataset.kind;
      const on = !btn.classList.contains("on");
      btn.classList.toggle("on", on); btn.setAttribute("aria-pressed", on ? "true" : "false");
      if (kind === "history") { (on ? H.add(id) : H.delete(id)); }
      else if (kind === "ratings") { (on ? R.add(id) : R.delete(id)); }
      else if (kind === "scrobble") { (on ? S.add(id) : S.delete(id)); }
      syncHidden();
      repaint();
      syncSelectAll();
      return;
    }
  }, true);

  document.addEventListener("input", (ev) => { if (ev.target?.id === "jfy_lib_filter") applyFilter(); }, true);

  window.jfyAuto = jfyAuto;
  window.jfyLoadLibraries = jfyLoadLibraries;
  window.mergeJellyfinIntoCfg = mergeJellyfinIntoCfg;
  window.jfyLogin = jfyLogin;
  window.jfyDeleteToken = jfyDeleteToken;

  window.registerSettingsCollector?.(mergeJellyfinIntoCfg);
  document.addEventListener("settings-collect", (e) => { try { mergeJellyfinIntoCfg(e?.detail?.cfg || (window.__cfg ||= {})); } catch {} }, true);
})();

