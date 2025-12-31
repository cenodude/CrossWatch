// assets/auth/auth.emby.js
(function () {
  "use strict";

  // --- utils
  const Q = (s, r = document) => r.querySelector(s);
  const Qa = (s, r = document) => Array.from(r.querySelectorAll(s) || []);
  const ESC = (s) => String(s || "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const SECTION = "#sec-emby";
  const LIB_URL = "/api/emby/libraries";

  let H = new Set(); // history lib ids
  let R = new Set(); // ratings lib ids
  let S = new Set(); // scrobble lib ids
  let hydrated = false;

  // --- tiny helpers
  const put = (sel, val) => { const el = Q(sel); if (el != null) el.value = (val ?? "") + ""; };
  const maskToken = (has) => { const el = Q("#emby_tok"); if (el) { el.value = has ? "••••••••" : ""; el.dataset.masked = has ? "1" : "0"; } };
  const visible = (el) => !!el && getComputedStyle(el).display !== "none" && !el.hidden;

  // --- libraries UI
  function applyFilter() {
    const qv = (Q("#emby_lib_filter")?.value || "").toLowerCase().trim();
    Qa("#emby_lib_matrix .lm-row").forEach((r) => {
      const name = (r.querySelector(".lm-name")?.textContent || "").toLowerCase();
      r.classList.toggle("hide", !!qv && !name.includes(qv));
    });
  }

  function syncHidden() {
    const selH = Q("#emby_lib_history");
    const selR = Q("#emby_lib_ratings");
    const selS = Q("#emby_lib_scrobble");
    if (selH) selH.innerHTML = [...H].map(id => `<option selected value="${id}">${id}</option>`).join("");
    if (selR) selR.innerHTML = [...R].map(id => `<option selected value="${id}">${id}</option>`).join("");
    if (selS) selS.innerHTML = [...S].map(id => `<option selected value="${id}">${id}</option>`).join("");
  }

  function syncSelectAll() {
    const rows = Qa("#emby_lib_matrix .lm-row:not(.hide)");
    const allHist = rows.length && rows.every(r => r.querySelector(".lm-dot.hist")?.classList.contains("on"));
    const allRate = rows.length && rows.every(r => r.querySelector(".lm-dot.rate")?.classList.contains("on"));
    const allScr = rows.length && rows.every(r => r.querySelector(".lm-dot.scr")?.classList.contains("on"));
    const h = Q("#emby_hist_all"), r = Q("#emby_rate_all"), s = Q("#emby_scr_all");
    if (h) { h.classList.toggle("on", !!allHist); h.setAttribute("aria-pressed", allHist ? "true" : "false"); }
    if (r) { r.classList.toggle("on", !!allRate); r.setAttribute("aria-pressed", allRate ? "true" : "false"); }
    if (s) { s.classList.toggle("on", !!allScr); s.setAttribute("aria-pressed", allScr ? "true" : "false"); }
  }

  function renderLibraries(libs) {
    const box = Q("#emby_lib_matrix"); if (!box) return;
    box.innerHTML = "";
    const f = document.createDocumentFragment();
    (Array.isArray(libs) ? libs : []).forEach((it) => {
      const id = String(it.key);
      const row = document.createElement("div");
      row.className = "lm-row"; row.dataset.id = id;
      row.innerHTML = `
        <div class="lm-name">${ESC(it.title)}</div>
        <button class="lm-dot hist${H.has(id) ? " on" : ""}" data-kind="history" aria-pressed="${H.has(id)}" title="Toggle History"></button>
        <button class="lm-dot rate${R.has(id) ? " on" : ""}" data-kind="ratings" aria-pressed="${R.has(id)}" title="Toggle Ratings"></button>
        <button class="lm-dot scr${S.has(id) ? " on" : ""}" data-kind="scrobble" aria-pressed="${S.has(id)}" title="Toggle Scrobble"></button>`;
      f.appendChild(row);
    });
    box.appendChild(f);
    applyFilter();
    syncHidden();
    syncSelectAll();
  }

  async function embyLoadLibraries() {
    try {
      const r = await fetch(LIB_URL + "?ts=" + Date.now(), { cache: "no-store" });
      const d = r.ok ? await r.json().catch(() => ({})) : {};
      const libs = Array.isArray(d?.libraries) ? d.libraries : (Array.isArray(d) ? d : []);
      renderLibraries(libs);
    } catch { renderLibraries([]); }
  }

  // --- hydrate from /api/config (auto when section becomes visible)
  function embySectionLooksEmpty() {
    const s1 = Q("#emby_server") || Q("#emby_server_url");
    const u1 = Q("#emby_user") || Q("#emby_username");
    const tok = Q("#emby_tok");
    const vals = [s1, u1, tok].map(el => el ? String(el.value || "").trim() : "");
    return vals.every(v => !v);
  }

  // --- hydrate from /api/config (auto when section becomes visible)
  async function hydrateFromConfig(force = false) {
    if (hydrated && !force) return;
    try {
      const r = await fetch("/api/config", { cache: "no-store" });
      if (!r.ok) return;
      const cfg = await r.json();
      window.__cfg = cfg;
      const em = cfg.emby || {};

      put("#emby_server", em.server); put("#emby_server_url", em.server);
      put("#emby_user", em.user || em.username); put("#emby_username", em.user || em.username);
      put("#emby_user_id", em.user_id);
      const v1 = Q("#emby_verify_ssl"), v2 = Q("#emby_verify_ssl_dup");
      if (v1) v1.checked = !!em.verify_ssl;
      if (v2) v2.checked = !!em.verify_ssl;
      maskToken(!!(em.access_token || "").trim());

      H = new Set((em.history?.libraries || []).map(String));
      R = new Set((em.ratings?.libraries || []).map(String));
      S = new Set((em.scrobble?.libraries || []).map(String));

      hydrated = true;
      await embyLoadLibraries();
    } catch { /* ignore */ }
  }

  // ensure hydrate when section is present and visible
  function ensureHydrate() {
    const sec = Q(SECTION);
    const body = sec?.querySelector(".body");
    if (!sec || (body && !visible(body))) return;
    const force = embySectionLooksEmpty();
    hydrateFromConfig(force);
  }


  // observe section insertion (SPAs/late render)
  if (!Q(SECTION)) {
    const mo = new MutationObserver(() => {
      if (Q(SECTION)) { mo.disconnect(); ensureHydrate(); }
    });
    mo.observe(document.documentElement, { childList: true, subtree: true });
  }

  // click on the section header → open → hydrate
  document.addEventListener("click", (e) => {
    const head = Q("#sec-emby .head");
    if (head && head.contains(e.target)) setTimeout(ensureHydrate, 0);
  }, true);

  // run once on ready
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => setTimeout(ensureHydrate, 30));
  } else {
    setTimeout(ensureHydrate, 30);
  }

  // --- auto-fill from inspect
  async function embyAuto() {
    try {
      const r = await fetch("/api/emby/inspect?ts=" + Date.now(), { cache: "no-store" });
      if (!r.ok) return;
      const d = await r.json();
      if (d.server_url) { put("#emby_server", d.server_url); put("#emby_server_url", d.server_url); }
      if (d.username)   { put("#emby_user", d.username);     put("#emby_username", d.username); }
      if (d.user_id)    { put("#emby_user_id", d.user_id); }
    } catch {}
  }

  // --- login
  async function embyLogin() {
    const server = (Q("#emby_server")?.value || "").trim();
    const username = (Q("#emby_user")?.value || "").trim();
    const password = Q("#emby_pass")?.value || "";
    const verify_ssl = !!(Q("#emby_verify_ssl")?.checked || Q("#emby_verify_ssl_dup")?.checked);
    const btn = Q("button.btn.emby"), msg = Q("#emby_msg");
    if (btn) { btn.disabled = true; btn.classList.add("busy"); }
    if (msg) { msg.className = "msg hidden"; msg.textContent = ""; }
    try {
      const r = await fetch("/api/emby/login", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ server, username, password, verify_ssl }), cache: "no-store"
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok || j?.ok === false) { if (msg) { msg.className = "msg warn"; msg.textContent = "Login failed"; } return; }
      put("#emby_server_url", server); put("#emby_username", username);
      if (j?.user_id) put("#emby_user_id", j.user_id);
      maskToken(true); if (Q("#emby_pass")) Q("#emby_pass").value = "";
      if (msg) { msg.className = "msg"; msg.textContent = "Emby connected."; }
      await embyLoadLibraries();
    } finally { if (btn) { btn.disabled = false; btn.classList.remove("busy"); } }
  }

  // --- delete token
  async function embyDeleteToken() {
    const delBtn = Q('#sec-emby .btn.danger');
    const msg = Q('#emby_msg');
    if (delBtn) { delBtn.disabled = true; delBtn.classList.add('busy'); }
    if (msg) { msg.className = 'msg hidden'; msg.textContent = ''; }
    try {
      const r = await fetch('/api/emby/token/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
        cache: 'no-store'
      });
      const j = await r.json().catch(() => ({}));
      if (r.ok && (j.ok !== false)) {
        maskToken(false);
        if (Q('#emby_pass')) Q('#emby_pass').value = '';
        if (msg) { msg.className = 'msg'; msg.textContent = 'Access token removed.'; }
      } else {
        if (msg) { msg.className = 'msg warn'; msg.textContent = 'Could not remove token.'; }
      }
    } catch (_) {
      if (msg) { msg.className = 'msg warn'; msg.textContent = 'Error removing token.'; }
    } finally {
      if (delBtn) { delBtn.disabled = false; delBtn.classList.remove('busy'); }
    }
  }

  // --- merge back to cfg
  function mergeEmbyIntoCfg(cfg) {
    const v = (sel) => (Q(sel)?.value || "").trim();
    const em = (cfg.emby = cfg.emby || {});
    const server = v("#emby_server_url") || v("#emby_server");
    const user = v("#emby_username") || v("#emby_user");
    if (server) em.server = server;
    if (user) { em.user = user; em.username = user || em.username || ""; }
    const uid = v("#emby_user_id"); if (uid) em.user_id = uid;
    const vs = Q("#emby_verify_ssl"), vs2 = Q("#emby_verify_ssl_dup");
    em.verify_ssl = !!((vs && vs.checked) || (vs2 && vs2.checked));
    em.history = Object.assign({}, em.history || {}, { libraries: Array.from(H) });
    em.ratings = Object.assign({}, em.ratings || {}, { libraries: Array.from(R) });
    em.scrobble = Object.assign({}, em.scrobble || {}, { libraries: Array.from(S) });
    return cfg;
  }


  // --- toggles + master toggles
  document.addEventListener("click", (ev) => {
    const t = ev.target; if (!(t instanceof Element)) return;
    const btn = t.closest(".lm-dot") || t.closest(".lm-col")?.querySelector(".lm-dot");
    if (!btn) return;

    if (btn.closest("#emby_lib_matrix")) {
      const row = btn.closest(".lm-row"); if (!row) return;
      const id = String(row.dataset.id || ""), kind = btn.dataset.kind;
      const on = !btn.classList.contains("on");
      btn.classList.toggle("on", on); btn.setAttribute("aria-pressed", on ? "true" : "false");
      if (kind === "history") { (on ? H.add(id) : H.delete(id)); }
      else if (kind === "ratings") { (on ? R.add(id) : R.delete(id)); }
      else if (kind === "scrobble") { (on ? S.add(id) : S.delete(id)); }
      syncHidden();
      syncSelectAll();
      return;
    }

    if (btn.id === "emby_hist_all") {
      const on = !btn.classList.contains("on");
      btn.classList.toggle("on", on); btn.setAttribute("aria-pressed", on ? "true" : "false");
      H = new Set();
      Qa("#emby_lib_matrix .lm-dot.hist").forEach((b) => {
        b.classList.toggle("on", on); b.setAttribute("aria-pressed", on ? "true" : "false");
        if (on) { const r = b.closest(".lm-row"); if (r) H.add(String(r.dataset.id || "")); }
      });
      syncHidden();
      syncSelectAll();
      return;
    }

    if (btn.id === "emby_rate_all") {
      const on = !btn.classList.contains("on");
      btn.classList.toggle("on", on); btn.setAttribute("aria-pressed", on ? "true" : "false");
      R = new Set();
      Qa("#emby_lib_matrix .lm-dot.rate").forEach((b) => {
        b.classList.toggle("on", on); b.setAttribute("aria-pressed", on ? "true" : "false");
        if (on) { const r = b.closest(".lm-row"); if (r) R.add(String(r.dataset.id || "")); }
      });
      syncHidden();
      syncSelectAll();
      return;
    }

    if (btn.id === "emby_scr_all") {
      const on = !btn.classList.contains("on");
      btn.classList.toggle("on", on); btn.setAttribute("aria-pressed", on ? "true" : "false");
      S = new Set();
      Qa("#emby_lib_matrix .lm-dot.scr").forEach((b) => {
        b.classList.toggle("on", on); b.setAttribute("aria-pressed", on ? "true" : "false");
        if (on) { const r = b.closest(".lm-row"); if (r) S.add(String(r.dataset.id || "")); }
      });
      syncHidden();
      syncSelectAll();
      return;
    }
  }, true);

  document.addEventListener("input", (ev) => { if (ev.target?.id === "emby_lib_filter") applyFilter(); }, true);

  // expose
  window.embyAuto = embyAuto;
  window.embyLoadLibraries = embyLoadLibraries;
  window.mergeEmbyIntoCfg = mergeEmbyIntoCfg;
  window.embyLogin = embyLogin;
  window.embyDeleteToken = embyDeleteToken;

  // optional integration
  window.registerSettingsCollector?.(mergeEmbyIntoCfg);
  document.addEventListener("settings-collect", (e) => { try { mergeEmbyIntoCfg(e?.detail?.cfg || (window.__cfg ||= {})); } catch {} }, true);
})();

// Force Emby settings collapsed by default
(function(){
  const SEL = '#sec-emby details.settings';
  const collapse = (root=document) => root.querySelectorAll(SEL).forEach(d=>{ d.open = false; d.removeAttribute('open'); });

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => collapse());
  } else {
    collapse();
  }
  new MutationObserver(muts=>{
    for (const m of muts) {
      for (const n of (m.addedNodes || [])) {
        if (n.nodeType === 1) collapse(n);
      }
    }
  }).observe(document.documentElement, { childList: true, subtree: true });
})();
