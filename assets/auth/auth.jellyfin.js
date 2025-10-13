// assets/auth/auth.jellyfin.js
(function () {
  "use strict";

  // --- utils
  const Q = (s, r = document) => r.querySelector(s);
  const Qa = (s, r = document) => Array.from(r.querySelectorAll(s) || []);
  const ESC = (s) => String(s || "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const SECTION = "#sec-jellyfin";
  const LIB_URL = "/api/jellyfin/libraries";

  let H = new Set(); // history lib ids
  let R = new Set(); // ratings lib ids
  let hydrated = false;

  // --- tiny helpers
  const put = (sel, val) => { const el = Q(sel); if (el != null) el.value = (val ?? "") + ""; };
  const maskToken = (has) => { const el = Q("#jfy_tok"); if (el) { el.value = has ? "••••••••" : ""; el.dataset.masked = has ? "1" : "0"; } };
  const visible = (el) => !!el && getComputedStyle(el).display !== "none" && !el.hidden;

  // --- libraries UI
  function applyFilter() {
    const qv = (Q("#jfy_lib_filter")?.value || "").toLowerCase().trim();
    Qa("#jfy_lib_matrix .lm-row").forEach((r) => {
      const name = (r.querySelector(".lm-name")?.textContent || "").toLowerCase();
      r.classList.toggle("hide", !!qv && !name.includes(qv));
    });
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
        <button class="lm-dot hist${H.has(id) ? " on" : ""}" data-kind="history" aria-pressed="${H.has(id)}" title="Toggle History"></button>
        <button class="lm-dot rate${R.has(id) ? " on" : ""}" data-kind="ratings" aria-pressed="${R.has(id)}" title="Toggle Ratings"></button>`;
      f.appendChild(row);
    });
    box.appendChild(f);
    applyFilter();
  }

  async function jfyLoadLibraries() {
    try {
      const r = await fetch(LIB_URL + "?ts=" + Date.now(), { cache: "no-store" });
      const d = r.ok ? await r.json().catch(() => ({})) : {};
      const libs = Array.isArray(d?.libraries) ? d.libraries : (Array.isArray(d) ? d : []);
      renderLibraries(libs);
    } catch { renderLibraries([]); }
  }

  // --- hydrate from /api/config (auto when section becomes visible)
  async function hydrateFromConfig() {
    if (hydrated) return;
    try {
      const r = await fetch("/api/config", { cache: "no-store" });
      if (!r.ok) return;
      const cfg = await r.json();
      window.__cfg = cfg;
      const jf = cfg.jellyfin || {};

      put("#jfy_server", jf.server); put("#jfy_server_url", jf.server);
      put("#jfy_user", jf.user || jf.username); put("#jfy_username", jf.user || jf.username);
      put("#jfy_user_id", jf.user_id);
      const v1 = Q("#jfy_verify_ssl"), v2 = Q("#jfy_verify_ssl_dup");
      if (v1) v1.checked = !!jf.verify_ssl;
      if (v2) v2.checked = !!jf.verify_ssl;
      maskToken(!!(jf.access_token || "").trim());

      H = new Set((jf.history?.libraries || []).map(String));
      R = new Set((jf.ratings?.libraries || []).map(String));

      hydrated = true;
      await jfyLoadLibraries();
    } catch { /* ignore */ }
  }

  // ensure hydrate when section is present and visible
  function ensureHydrate() {
    const sec = Q(SECTION);
    const body = sec?.querySelector(".body");
    if (sec && (!body || visible(body))) hydrateFromConfig();
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
    const head = Q("#sec-jellyfin .head");
    if (head && head.contains(e.target)) setTimeout(ensureHydrate, 0);
  }, true);

  // run once on ready
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => setTimeout(ensureHydrate, 30));
  } else {
    setTimeout(ensureHydrate, 30);
  }

  // --- auto-fill from inspect
  async function jfyAuto() {
    try {
      const r = await fetch("/api/jellyfin/inspect?ts=" + Date.now(), { cache: "no-store" });
      if (!r.ok) return;
      const d = await r.json();
      if (d.server_url) { put("#jfy_server", d.server_url); put("#jfy_server_url", d.server_url); }
      if (d.username)   { put("#jfy_user", d.username);     put("#jfy_username", d.username); }
      if (d.user_id)    { put("#jfy_user_id", d.user_id); }
    } catch {}
  }

  // --- login
  async function jfyLogin() {
    const server = (Q("#jfy_server")?.value || "").trim();
    const username = (Q("#jfy_user")?.value || "").trim();
    const password = Q("#jfy_pass")?.value || "";
    const btn = Q("button.btn.jellyfin"), msg = Q("#jfy_msg");
    if (btn) { btn.disabled = true; btn.classList.add("busy"); }
    if (msg) { msg.className = "msg hidden"; msg.textContent = ""; }
    try {
      const r = await fetch("/api/jellyfin/login", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ server, username, password }), cache: "no-store"
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok || j?.ok === false) { if (msg) { msg.className = "msg warn"; msg.textContent = "Login failed"; } return; }
      put("#jfy_server_url", server); put("#jfy_username", username);
      maskToken(true); if (Q("#jfy_pass")) Q("#jfy_pass").value = "";
      if (msg) { msg.className = "msg"; msg.textContent = "Jellyfin connected."; }
      await jfyLoadLibraries();
    } finally { if (btn) { btn.disabled = false; btn.classList.remove("busy"); } }
  }

  // --- merge back to cfg
  function mergeJellyfinIntoCfg(cfg) {
    const v = (sel) => (Q(sel)?.value || "").trim();
    const jf = (cfg.jellyfin = cfg.jellyfin || {});
    const server = v("#jfy_server_url") || v("#jfy_server");
    const user = v("#jfy_username") || v("#jfy_user");
    if (server) jf.server = server;
    if (user) { jf.user = user; jf.username = user || jf.username || ""; }
    const uid = v("#jfy_user_id"); if (uid) jf.user_id = uid;
    const vs = Q("#jfy_verify_ssl"), vs2 = Q("#jfy_verify_ssl_dup");
    jf.verify_ssl = !!((vs && vs.checked) || (vs2 && vs2.checked));
    jf.history = Object.assign({}, jf.history || {}, { libraries: Array.from(H) });
    jf.ratings = Object.assign({}, jf.ratings || {}, { libraries: Array.from(R) });
    return cfg;
  }

  // --- toggles + master toggles
  document.addEventListener("click", (ev) => {
    const t = ev.target; if (!(t instanceof HTMLElement)) return;

    if (t.classList.contains("lm-dot")) {
      const row = t.closest(".lm-row"); if (!row) return;
      const id = String(row.dataset.id || ""), kind = t.dataset.kind;
      const on = !t.classList.contains("on");
      t.classList.toggle("on", on); t.setAttribute("aria-pressed", on ? "true" : "false");
      if (kind === "history") (on ? H.add(id) : H.delete(id)); else (on ? R.add(id) : R.delete(id));
      return;
    }

    if (t.id === "jfy_hist_all" && t.classList.contains("lm-dot")) {
      const on = !t.classList.contains("on"); t.classList.toggle("on", on); t.setAttribute("aria-pressed", on ? "true" : "false");
      H = new Set();
      Qa("#jfy_lib_matrix .lm-dot.hist").forEach((b) => {
        b.classList.toggle("on", on); b.setAttribute("aria-pressed", on ? "true" : "false");
        if (on) { const r = b.closest(".lm-row"); if (r) H.add(String(r.dataset.id || "")); }
      });
      return;
    }

    if (t.id === "jfy_rate_all" && t.classList.contains("lm-dot")) {
      const on = !t.classList.contains("on"); t.classList.toggle("on", on); t.setAttribute("aria-pressed", on ? "true" : "false");
      R = new Set();
      Qa("#jfy_lib_matrix .lm-dot.rate").forEach((b) => {
        b.classList.toggle("on", on); b.setAttribute("aria-pressed", on ? "true" : "false");
        if (on) { const r = b.closest(".lm-row"); if (r) R.add(String(r.dataset.id || "")); }
      });
      return;
    }
  }, true);

  document.addEventListener("input", (ev) => { if (ev.target?.id === "jfy_lib_filter") applyFilter(); }, true);

  // expose
  window.jfyAuto = jfyAuto;
  window.jfyLoadLibraries = jfyLoadLibraries;
  window.mergeJellyfinIntoCfg = mergeJellyfinIntoCfg;
  window.jfyLogin = jfyLogin;

  // optional integration
  window.registerSettingsCollector?.(mergeJellyfinIntoCfg);
  document.addEventListener("settings-collect", (e) => { try { mergeJellyfinIntoCfg(e?.detail?.cfg || (window.__cfg ||= {})); } catch {} }, true);
})();

// Force Jellyfin settings collapsed by default
(function(){
  const SEL = '#sec-jellyfin details.settings';
  const collapse = (root=document) => root.querySelectorAll(SEL).forEach(d=>{ d.open = false; d.removeAttribute('open'); });

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => collapse());
  } else {
    collapse();
  }
  new MutationObserver(muts=>{
    for (const m of muts) {
      for (const n of m.addedNodes || []) {
        if (n.nodeType === 1) collapse(n);
      }
    }
  }).observe(document.documentElement, { childList: true, subtree: true });
})();
