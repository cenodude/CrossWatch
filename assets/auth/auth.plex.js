// auth.plex.js - Plex auth
(function (w, d) {

  const Shared = w.CW && w.CW.AuthShared;
  const $ = (s) => d.getElementById(s);
  const q = (sel, root = d) => root.querySelector(sel);
  const notify = Shared.notify;
  const bust = () => `?ts=${Date.now()}`;
  const exists = (sel) => !!q(sel);
  function waitFor(sel, timeout = 12000) {
    return new Promise((res) => {
      const end = Date.now() + timeout;
      (function loop() {
        if (exists(sel)) return res(q(sel));
        if (Date.now() > end) return res(null);
        requestAnimationFrame(loop);
      })();
    });
  }


  const PLEX_SUBTAB_KEY = "cw.ui.plex.auth.subtab.v1";
  let __plexAutoTabInst = "";
  let __plexNewProfileInst = "";

const plexProfile = Shared.createProfileAdapter({
  provider: "plex",
  configKey: "plex",
  label: "Plex",
  sectionId: "sec-plex",
  selectId: "plex_instance",
  storageKey: "cw.ui.plex.auth.instance.v1",
  title: "Select which Plex account this config applies to.",
});

function getPlexInstance() {
  return plexProfile ? plexProfile.getInstance() : "default";
}

function setPlexInstance(v) {
  if (plexProfile) plexProfile.setInstance(v);
}

function plexApi(path) {
  return plexProfile ? plexProfile.api(path) : String(path || "");
}

function getPlexCfgBlock(cfg) {
  return plexProfile ? plexProfile.cfgBlock(cfg, false) : {};
}

function ensurePlexCfgBlock(cfg) {
  return plexProfile ? plexProfile.cfgBlock(cfg, true) : {};
}

async function refreshPlexInstanceOptions(preserve = true) {
  if (plexProfile) await plexProfile.refreshOptions(preserve);
}

function ensurePlexInstanceUI() {
  plexProfile?.ensureUI(async () => {
    try { await hydratePlexFromConfigRaw(); } catch {}
    try { refreshPlexLibraries(); } catch {}
    try { mountPlexUserPicker(); } catch {}
    try { schedulePlexPmsProbe(200); } catch {}
  });
}


  function plexAuthSubSelect(tab, opts = {}) {
    const root = q('#sec-plex .cw-meta-provider-panel[data-provider="plex"]') || q("#sec-plex .cw-panel");
    if (!root) return;

    const want = String(tab || "auth").toLowerCase();
    let sub = ["auth", "settings", "whitelist"].includes(want) ? want : "auth";
    const state = getPlexSetupState();
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
      try { localStorage.setItem(PLEX_SUBTAB_KEY, sub); } catch {}
    }

    if (sub === "whitelist") {
      const fresh = !w.__plexWlHandle;
      try { mountPlexLibraryMatrix(); } catch {}
      if (!fresh) { try { w.__plexWlHandle?.load(true); } catch {} }
    }

    if (sub === "settings") {
      setTimeout(() => { try { plexRefreshPmsSuggestions({ force: true }); } catch {} }, 0);
    }

    try { Shared.setMediaAuthStep(root, sub); } catch {}
  }

  function mountPlexAuthTabs() {
    const root = q('#sec-plex .cw-meta-provider-panel[data-provider="plex"]');
    if (!root) return;
    try {
      Shared.mediaAuthGuide(root, {
        kind: "plex",
        label: "Plex",
        title: "Link Plex, then tune the server",
        copy: "Click Connect Plex to get a link code. CrossWatch shows the code, then opens plex.tv/link so you can enter it and approve access. Next, validate the server URL and optionally whitelist libraries."
      });
      syncPlexSetupTabs();
    } catch {}

    root.querySelectorAll(".cw-subtile[data-sub]").forEach((btn) => {
      if (btn.__plexTabWired) return;
      btn.__plexTabWired = true;
      btn.addEventListener("click", () => plexAuthSubSelect(btn.dataset.sub));
    });

    if (root.__plexTabsInit) return;
    root.__plexTabsInit = true;

    let last = "auth";
    try { last = localStorage.getItem(PLEX_SUBTAB_KEY) || "auth"; } catch {}
    plexAuthSubSelect(last, { persist: false });
  }

  function getPlexSetupState() {
    let connected = false;
    try { connected = !!getPlexState().connected; } catch {}
    const url = String($("plex_server_url")?.value || "").trim();
    const user = String($("plex_username")?.value || "").trim();
    const aid = String($("plex_account_id")?.value || "").trim();
    const configured = connected || !!(url || user || aid);
    return {
      configured,
      connected,
      settingsEnabled: connected,
      whitelistEnabled: connected,
    };
  }

  function syncPlexSetupTabs(opts = {}) {
    const root = q('#sec-plex .cw-meta-provider-panel[data-provider="plex"]') || q("#sec-plex .cw-panel");
    if (!root) return;
    const state = getPlexSetupState();
    try { Shared.applyMediaTabState(root, state); } catch {}
    if (opts.auto) {
      const inst = getPlexInstance();
      if (__plexAutoTabInst !== inst) {
        __plexAutoTabInst = inst;
        const cur = root.querySelector(".cw-subtile.active[data-sub]")?.dataset?.sub || "auth";
        if (cur === "auth") plexAuthSubSelect("auth", { persist: false });
        return;
      }
    }
    if (opts.preferSettings && state.settingsEnabled) {
      const cur = root.querySelector(".cw-subtile.active[data-sub]")?.dataset?.sub || "auth";
      if (cur === "auth") {
        plexAuthSubSelect("settings", { persist: opts.persist !== false });
        return;
      }
    }
    const active = root.querySelector(".cw-subtile.active[data-sub]");
    plexAuthSubSelect(active?.dataset?.sub || "auth", { persist: false });
  }

  let __plexHydrateWatch = null;
  let __plexHydrateTimer = null;
  let __plexHydrateBusy = false;
  function schedulePlexHydrate(delayMs = 0) {
    try { if (__plexHydrateTimer) clearTimeout(__plexHydrateTimer); } catch {}
    __plexHydrateTimer = setTimeout(async () => {
      try { ensurePlexInstanceUI(); } catch {}
      try { mountPlexAuthTabs(); } catch {}
      try { wirePlexCopyButton(); } catch {}
      try { wirePlexActionButtons(); } catch {}
      if (!$("plex_server_url") || !$("plex_username")) return;
      if (__plexHydrateBusy) return;
      __plexHydrateBusy = true;
      try { await hydratePlexFromConfigRaw(); } catch {}
      try { mountPlexLibraryMatrix(); } catch {}
      try { mountPlexUserPicker(); } catch {}
      try { schedulePlexPmsProbe(250); } catch {}
      __plexHydrateBusy = false;
      try {
        if (__plexHydrateWatch && getPlexState().hydrated) {
          __plexHydrateWatch.disconnect();
          __plexHydrateWatch = null;
        }
      } catch {}
    }, Math.max(0, delayMs | 0));
  }

  function watchForPlexDom() {
    if (__plexHydrateWatch) return;
    schedulePlexHydrate(0);
    try {
      __plexHydrateWatch = new MutationObserver(() => schedulePlexHydrate(60));
      __plexHydrateWatch.observe(d.documentElement || d.body, { childList: true, subtree: true });
    } catch {}
  }

  // status banner
  function setPlexBanner(kind, text) {
    return Shared.setStatusPill("plex_msg", kind, text);
  }

  function ensurePlexPanelNotice() {
    const panel = q('#sec-plex .cw-meta-provider-panel[data-provider="plex"]');
    if (!panel) return null;
    let el = q('#plex_panel_notice', panel);
    if (el) return el;

    el = d.createElement('div');
    el.id = 'plex_panel_notice';
    el.className = 'hidden';
    el.style.margin = '8px 0 2px';
    el.style.padding = '8px 10px';
    el.style.borderRadius = '8px';
    el.style.border = '1px solid rgba(247,185,85,.35)';
    el.style.background = 'rgba(247,185,85,.08)';
    el.style.fontSize = '12px';
    el.style.lineHeight = '1.35';

    const head = q('.cw-panel-head', panel);
    if (head && head.parentNode) {
      head.insertAdjacentElement('afterend', el);
    } else {
      panel.insertBefore(el, panel.firstChild || null);
    }
    return el;
  }

  function setPlexPanelNotice(kind, text) {
    const el = ensurePlexPanelNotice();
    if (!el) return;
    if (!kind || !text) {
      el.classList.add('hidden');
      el.textContent = '';
      return;
    }
    el.classList.remove('hidden');
    el.textContent = text || '';
    const warn = String(kind || '').toLowerCase() === 'warn';
    el.style.color = warn ? '#f7b955' : '';
    el.style.borderColor = warn ? 'rgba(247,185,85,.35)' : 'rgba(120,120,120,.25)';
    el.style.background = warn ? 'rgba(247,185,85,.08)' : 'rgba(120,120,120,.08)';
  }

  function setPlexBannerDetail(kind, text) {
    const el = $("plex_msg_detail");
    if (el) {
      el.classList.remove("hidden", "warn");
      if (!kind || !text) {
        el.classList.add("hidden");
        el.textContent = "";
      } else {
        if (kind) el.classList.add(kind);
        el.textContent = text || "";
      }
    }
    setPlexPanelNotice(kind, text);
  }

  function ensurePlexUserScopeNotice() {
    const userPick = $("plex_user_pick_btn")?.closest(".userpick") || $("plex_username")?.parentElement;
    if (!userPick) return null;
    let el = $("plex_user_scope_notice");
    if (el) return el;
    el = d.createElement("div");
    el.id = "plex_user_scope_notice";
    el.className = "sub hidden";
    el.style.marginTop = "8px";
    el.style.padding = "8px 10px";
    el.style.borderRadius = "8px";
    el.style.border = "1px solid rgba(247,185,85,.35)";
    el.style.background = "rgba(247,185,85,.08)";
    el.style.color = "#f7b955";
    el.style.lineHeight = "1.35";
    userPick.insertAdjacentElement("afterend", el);
    return el;
  }

  function setPlexUserScopeNotice(user) {
    const el = ensurePlexUserScopeNotice();
    if (!el) return;
    const type = String(user?.type || "").trim().toLowerCase();
    if (type !== "friend") {
      el.classList.add("hidden");
      el.textContent = "";
      return;
    }
    const name = String(user?.username || "This friend").trim() || "This friend";
    el.classList.remove("hidden");
    el.textContent = `${name} is a Plex friend/shared account, not a Plex Home user. CrossWatch can only use Plex's server-scoped shared token for History and Progress. Ratings and Watchlist still need that friend's own Plex profile/authentication, and Plex may remove this token path later.`;
  }

  function setPlexSuccess(on, text) {
    try { getPlexState().connected = !!on; } catch {}
    if (on) setPlexBanner("ok", text || "Connected");
    else { setPlexBanner(null, ""); setPlexBannerDetail(null, ""); }
    try { Shared.setConnectLocked(["btn-connect-plex", "btn-plex-restart"], !!on); } catch {}
    try { syncPlexSetupTabs(); } catch {}
  }

  function setPlexConnected() {
    setPlexSuccess(true, "Connected");
    setPlexBannerDetail(null, "");
    schedulePlexPmsProbe(200);
  }

  const PLEX_QC_MAX_MS = 300000;
  const PLEX_ICON_COPY = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>';
  const PLEX_ICON_CHECK = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>';
  let plexQcDeadline = 0;
  let plexQcTimer = null;
  let plexQcCopyRevert = null;

  function plexQcSetState(show) {
    const box = $("plex_qc_state"); if (box) box.classList.toggle("hidden", !show);
    const connect = $("btn-connect-plex"), cancel = $("btn-plex-cancel"), restart = $("btn-plex-restart");
    if (connect) connect.classList.toggle("hidden", show);
    if (cancel) cancel.classList.toggle("hidden", !show);
    if (restart) restart.classList.add("hidden");
  }

  function plexQcShowRestart() {
    const restart = $("btn-plex-restart"), connect = $("btn-connect-plex"), cancel = $("btn-plex-cancel");
    if (restart) restart.classList.remove("hidden");
    if (connect) connect.classList.add("hidden");
    if (cancel) cancel.classList.add("hidden");
  }

  function plexQcUpdateTimer() {
    if (plexQcDeadline && Date.now() > plexQcDeadline) { plexQcTimeout(); return; }
    const el = $("plex_qc_timer"); if (!el) return;
    const left = Math.max(0, Math.round((plexQcDeadline - Date.now()) / 1000));
    const mm = Math.floor(left / 60), ss = String(left % 60).padStart(2, "0");
    el.textContent = left > 0 ? `Expires in ${mm}:${ss}` : "";
  }

  function plexQcStop() {
    if (plexQcTimer) { clearInterval(plexQcTimer); plexQcTimer = null; }
    try { if (plexTokenPoller) plexTokenPoller.stop(); } catch {}
    plexQcDeadline = 0;
    plexQcSetState(false);
  }

  function plexQcTimeout() {
    if (plexQcTimer) { clearInterval(plexQcTimer); plexQcTimer = null; }
    try { if (plexTokenPoller) plexTokenPoller.stop(); } catch {}
    plexQcDeadline = 0;
    const st = $("plex_qc_status"); if (st) st.textContent = "Link code expired. Restart to try again.";
    const el = $("plex_qc_timer"); if (el) el.textContent = "";
    plexQcShowRestart();
  }

  async function plexQcCopy(btn) {
    const code = ($("plex_qc_code")?.textContent || $("plex_pin")?.value || "").replace(/\s+/g, "").trim();
    if (!code || code === "----") return;
    let ok = false;
    try { if (navigator.clipboard?.writeText) { await navigator.clipboard.writeText(code); ok = true; } } catch {}
    if (!ok) {
      try {
        const ta = d.createElement("textarea");
        ta.value = code; ta.style.position = "fixed"; ta.style.opacity = "0";
        d.body.appendChild(ta); ta.focus(); ta.select();
        ok = d.execCommand("copy");
        d.body.removeChild(ta);
      } catch {}
    }
    if (!ok) { notify("Copy failed"); return; }
    btn.classList.add("copied");
    btn.innerHTML = PLEX_ICON_CHECK;
    btn.title = "Copied!";
    if (plexQcCopyRevert) clearTimeout(plexQcCopyRevert);
    plexQcCopyRevert = setTimeout(() => {
      btn.classList.remove("copied");
      btn.innerHTML = PLEX_ICON_COPY;
      btn.title = "Copy code";
    }, 1400);
  }

  function wirePlexCopyButton() {
    const copy = $("plex_qc_copy");
    if (copy && !copy.__wired) { copy.__wired = true; copy.addEventListener("click", (e) => { e.preventDefault(); plexQcCopy(copy); }); }
    const cancel = $("btn-plex-cancel");
    if (cancel && !cancel.__wired) { cancel.__wired = true; cancel.addEventListener("click", () => plexQcStop()); }
    const restart = $("btn-plex-restart");
    if (restart && !restart.__wired) { restart.__wired = true; restart.addEventListener("click", () => requestPlexPin()); }
  }

  function wirePlexActionButtons() {
    const connect = $("btn-connect-plex");
    if (connect && !connect.__wired) { connect.addEventListener("click", requestPlexPin); connect.__wired = true; }
    const del = $("btn-delete-plex");
    if (del && !del.__wired) { del.addEventListener("click", plexDeleteToken); del.__wired = true; }
    const auto = $("btn-plex-auto");
    if (auto && !auto.__wired) { auto.addEventListener("click", plexAuto); auto.__wired = true; }
    const libs = $("btn-plex-load-libraries");
    if (libs && !libs.__wired) { libs.addEventListener("click", refreshPlexLibraries); libs.__wired = true; }
  }

  let __plexProbeT = null;
  function schedulePlexPmsProbe(delayMs = 400) {
    try { if (__plexProbeT) clearTimeout(__plexProbeT); } catch {}
    __plexProbeT = setTimeout(() => { plexProbePmsReachability(); }, Math.max(0, delayMs | 0));
  }

  async function plexProbePmsReachability() {
    const cfg = await fetch("/api/config" + bust(), { cache: "no-store" }).then(r => r.json()).catch(() => ({}));
    const tok = String(getPlexCfgBlock(cfg || {}).account_token || "").trim();
    if (!tok) { try { window.CW.AuthShared.clearConnectionWarnings(); } catch {} return; }
    try {
      const r = await fetch(plexApi("/api/plex/pms/probe") + plexLiveQS(), { cache: "no-store" });
      const j = await r.json().catch(() => ({}));
      if (r.ok && j?.reachable) {
        try { window.CW.AuthShared.clearConnectionWarnings(); } catch {}
        return;
      }
      const base = String(j?.server_url || "").trim();
      const sc = Number(j?.status);
      let msg = "Connected, but PMS is not reachable - validate settings.";
      if (!base) msg = "Connected, but no PMS URL is set - validate settings.";
      else if (sc === 401 || sc === 403) msg = "Connected, but PMS rejected the token - validate settings.";
      try { window.CW.AuthShared.showConnectionWarning(msg); } catch {}
    } catch {
      try { window.CW.AuthShared.showConnectionWarning("Connected, but PMS is not reachable - validate settings."); } catch {}
    }
  }

  async function requestPlexPin() {
    try { plexQcStop(); } catch {}
    try { setPlexSuccess(false); } catch {}
    try { setPlexBannerDetail(null, ""); } catch {}

    const connectBtn = $("btn-connect-plex");
    if (connectBtn) { connectBtn.disabled = true; connectBtn.classList.add("busy"); }

    let win = null; try { win = w.open("about:blank", "_blank"); } catch {}

    let data = null;
    try {
      const r = await fetch(plexApi("/api/plex/pin/new"), { method: "POST", cache: "no-store" });
      data = await r.json();
      if (!r.ok || data?.ok === false) throw new Error(data?.error || "PIN request failed");
    } catch (e) {
      console.warn("plex pin fetch failed", e);
      try { if (win && !win.closed) win.close(); } catch {}
      setPlexBanner("warn", "Could not request a Plex link code.");
      if (connectBtn) { connectBtn.disabled = false; connectBtn.classList.remove("busy"); }
      return;
    }

    const pin = String(data.code || data.pin || data.id || "").trim();
    const pinEl = $("plex_pin"); if (pinEl) pinEl.value = pin;
    const codeEl = $("plex_qc_code"); if (codeEl) codeEl.textContent = pin || "----";
    const statusEl = $("plex_qc_status"); if (statusEl) statusEl.textContent = "Waiting for authorization…";
    const helpEl = $("plex_qc_help");
    if (helpEl) helpEl.textContent = win
      ? "Opening plex.tv/link — enter this code there and approve CrossWatch."
      : "Open plex.tv/link and enter this code to approve CrossWatch.";

    plexQcDeadline = Date.now() + PLEX_QC_MAX_MS;
    plexQcSetState(true);
    plexQcUpdateTimer();
    plexQcTimer = setInterval(plexQcUpdateTimer, 1000);

    if (win && !win.closed) {
      try {
        win.document.write(
          '<!doctype html><meta charset="utf-8"><title>CrossWatch → Plex</title>' +
          '<body style="margin:0;height:100vh;display:flex;align-items:center;justify-content:center;background:#0b0d12;color:#e9eefb;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;text-align:center">' +
          '<div><div style="font-size:14px;opacity:.7;margin-bottom:12px">Opening plex.tv/link…</div>' +
          '<div style="font-size:36px;font-weight:700;letter-spacing:.22em;color:#8ff0c2">' + pin + '</div>' +
          '<div style="font-size:12px;opacity:.6;margin-top:12px">Redirecting in a moment…</div></div></body>'
        );
      } catch {}
      setTimeout(() => { try { if (win && !win.closed) win.location.href = "https://plex.tv/link"; } catch {} }, 3000);
    } else {
      notify("Popup blocked - open plex.tv/link and enter the code.");
    }

    if (connectBtn) { connectBtn.disabled = false; connectBtn.classList.remove("busy"); }
    try { startPlexTokenPoll(); } catch {}
  }

  // token poll
  let plexTokenPoller = null;
  let __plexLastTok = "";

  function ensurePlexTokenPoller() {
    if (plexTokenPoller) return plexTokenPoller;
    plexTokenPoller = Shared.createDevicePoll({
      url: () => "/api/config" + bust(),
      method: "GET",
      minIntervalMs: 1500,
      maxTotalMs: PLEX_QC_MAX_MS,
      shouldPause: () => {
        if (d.hidden) return true;
        return !($("page-settings") && !$("page-settings").classList.contains("hidden"));
      },
      classify: (status, cfg) => {
        const p = getPlexCfgBlock(cfg || {});
        return String(p.account_token || "").trim() ? { state: "authorized" } : { state: "pending" };
      },
      onAuthorized: async (cfg) => {
        try { plexQcStop(); } catch {}
        try { setPlexSuccess(true, "Connected"); } catch {}
        try { d.dispatchEvent(new CustomEvent("cw-provider-connected", { bubbles: true, detail: { provider: "plex", key: "PLEX" } })); } catch {}
        try {
          const tok = String(getPlexCfgBlock(cfg || {}).account_token || "").trim();
          if (tok !== __plexLastTok) {
            __plexLastTok = tok;
            try { __plexUsersByInst.delete(getPlexInstance()); } catch {}
          }
          try { await fetch(plexApi("/api/plex/inspect"), { cache: "no-store" }); } catch {}
          try { await hydratePlexFromConfigRaw(); } catch {}
          const missing = !($("plex_server_url")?.value?.trim()) || !($("plex_username")?.value?.trim()) || !($("plex_account_id")?.value?.trim());
          if (missing && typeof plexAuto === "function") { try { await plexAuto(); } catch {} }
          try { await plexProbePmsReachability(); } catch {}
        } catch (e) {
          console.warn("plex post-auth sync failed", e);
        }
      },
    });
    return plexTokenPoller;
  }

  function startPlexTokenPoll() {
    ensurePlexTokenPoller().start({ intervalMs: 1500, deadlineMs: plexQcDeadline || 0 });
  }

  // delete Plex account token
async function plexDeleteToken() {
  const btn = $("btn-delete-plex") || d.querySelector('#sec-plex [data-action="plex-delete"], #sec-plex button[id*="delete"]');
  try { plexQcStop(); } catch {}
  try { if (btn) { btn.disabled = true; btn.classList.add("busy"); } } catch {}
  try {
    const r = await fetch(plexApi("/api/plex/token/delete"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
      cache: "no-store"
    });
    const j = await r.json().catch(() => ({}));
    if (Shared.reportProviderUsage({ status: r.status, data: j })) return;
    if (r.ok && (j.ok !== false)) {
      ["plex_pin", "plex_username", "plex_account_id"].forEach((id) => { const el = $(id); if (el) el.value = ""; });
      try { const codeEl = $("plex_qc_code"); if (codeEl) codeEl.textContent = "----"; } catch {}
      try { const st = getPlexState(); st.libs = []; st.connected = false; } catch {}
      try { __plexUsersByInst.delete(getPlexInstance()); } catch {}
      try { setPlexBanner("warn", "Disconnected"); } catch {}
      try { setPlexBannerDetail("warn", "Token deleted and saved."); } catch {}
      try { notify("Plex disconnected (saved)."); } catch {}
      try { refreshPlexLibraries(); } catch {}
      try { syncPlexSetupTabs(); } catch {}
    } else {
      const msg = String(j?.error || j?.message || "").trim() || "Could not remove Plex token.";
      try { setPlexBannerDetail("warn", msg); } catch {}
    }
  } catch (e) {
    console.warn("plex delete token failed", e);
    try { setPlexBannerDetail("warn", "Error removing Plex token."); } catch {}
  } finally {
    try { if (btn) { btn.disabled = false; btn.classList.remove("busy"); } } catch {}
  }
}


  function getPlexState() {
    const st = (w.__plexState ||= { hist: new Set(), rate: new Set(), prog: new Set(), scr: new Set(), libs: [], hydrated: false, connected: false });
    st.prog ||= new Set();
    return st;
  }

  // Config
  async function hydratePlexFromConfigRaw() {
    try {
      const r = await fetch("/api/config", { cache: "no-store" }); if (!r.ok) return;
      const cfg = await r.json(); const p = getPlexCfgBlock(cfg);
      w.__cfg = cfg;
      const st = getPlexState();
      st.hist = new Set((p.history?.libraries || []).map(x => String(x)));
      st.rate = new Set((p.ratings?.libraries || []).map(x => String(x)));
      st.prog = new Set((p.progress?.libraries || []).map(x => String(x)));
      st.scr  = new Set((p.scrobble?.libraries || []).map(x => String(x)));
      st.hydrated = true;
      w.__plexHydrated = true;
      if (st.libs.length) mountPlexLibraryMatrix();
      await waitFor("#plex_server_url"); await waitFor("#plex_username");
      const set = (id, val) => { const el = $(id); if (el != null && val != null) el.value = String(val); };
      const tok = String(p.account_token || '').trim();
      if (tok) setPlexConnected();
      else setPlexSuccess(false);
      set("plex_pin", p._pending_pin?.code || "");
      set("plex_server_url", p.server_url || "");
      set("plex_username", p.username || "");
      const aid = (p.account_id != null ? String(p.account_id).trim() : "");
      set("plex_account_id", aid || "");
      // If account_id is missing, resolve it once from /api/plex/pickusers.
      await resolvePlexAccountIdFromUsers();
      await refreshPlexSelectedUserScopeNotice();
      try { const cb = $("plex_verify_ssl"); if (cb) cb.checked = !!p.verify_ssl; } catch {}

      ["plex_lib_history", "plex_lib_ratings", "plex_lib_progress", "plex_lib_scrobble"].forEach(id => {
        const el = $(id); if (!el) return;
        Array.from(el.options || []).forEach(o => {
          if (id === "plex_lib_history") o.selected = st.hist.has(o.value);
          if (id === "plex_lib_ratings") o.selected = st.rate.has(o.value);
          if (id === "plex_lib_progress") o.selected = st.prog.has(o.value);
          if (id === "plex_lib_scrobble") o.selected = st.scr.has(o.value);
        });
      });
      try { plexRefreshPmsSuggestions(); } catch {}
      try { syncPlexSetupTabs({ auto: true }); } catch {}
    } catch (e) { console.warn("[plex] hydrate failed", e); }
  }

  // build server suggestions (from /api/plex/pms)
  function fillPlexServerSuggestions(servers) {
    const dl = document.getElementById("plex_server_suggestions");
    if (!dl) return "";

    const items = [];
    const seen = new Set();

    const isPrivateHost = (host) => {
      if (!host) return false;
      const h = host.toLowerCase();

      const isPrivateDotted = (ip) => {
        const p = ip.split(".").map(n => parseInt(n, 10));
        if (p.length !== 4 || p.some(n => !Number.isFinite(n) || n < 0 || n > 255)) return false;
        if (p[0] === 10) return true;
        if (p[0] === 192 && p[1] === 168) return true;
        if (p[0] === 172 && p[1] >= 16 && p[1] <= 31) return true;
        if (p[0] === 127) return true;
        if (p[0] === 169 && p[1] === 254) return true;
        return false;
      };

      if (/^\d{1,3}(\.\d{1,3}){3}$/.test(h)) return isPrivateDotted(h);

      const m = h.match(/^(\d{1,3}(?:-\d{1,3}){3})\.plex\.direct$/);
      if (m) return isPrivateDotted(m[1].replace(/-/g, "."));

      return false;
    };

    const add = (key, meta = {}) => {
      const url = (key || "").trim().replace(/\/+$/, "");
      if (!url || seen.has(url)) return;
      seen.add(url);

      const local    = !!meta.local;
      const relay    = !!meta.relay;
      const proto    = (meta.proto || "").toLowerCase();
      const hostKind = meta.hostKind || "domain";

      let host = "";
      try { host = new URL(url).hostname || ""; } catch {}
      const privateHost = isPrivateHost(host);
      const effProto = proto || (url.startsWith("https://") ? "https" : "http");

      const remote = !local;
      const direct = !relay;

      const score =
        (remote ? 16 : 0) +
        (effProto === "https" ? 8 : 0) +
        (direct ? 4 : 0) +
        (hostKind === "domain" ? 2 : 0) +
        (!privateHost ? 1 : 0);
const tags = [
        local ? "local" : "remote",
        relay ? "relay" : "direct",
        effProto,
        privateHost ? "private" : hostKind
      ].join(", ");

      items.push({ url, score, label: `${url} — ${tags}` });
    };

    (servers || []).forEach((s) => {
      (s.connections || []).forEach((c) => {
        const address = (c.address || "").trim();
        const port = c.port ? `:${c.port}` : "";
        const local = !!c.local;
        const relay = !!c.relay;

        if (address) {
          add(`http://${address}${port}`,  { local, relay, proto: "http",  hostKind: "ip" });
          add(`https://${address}${port}`, { local, relay, proto: "https", hostKind: "ip" });
        }

        if (c.uri) {
          try {
            const u = new URL(c.uri);
            add(c.uri, {
              local,
              relay,
              proto: u.protocol.replace(":", ""),
              hostKind: "domain"
            });
          } catch {}
        }
      });
    });

    items.sort((a, b) => b.score - a.score || a.url.length - b.url.length);
    dl.innerHTML = items
      .map((it) => `<option value="${it.url}" label="${it.label}"></option>`)
      .join("");

    return items[0]?.url || "";
  }


  // Fetch PMS connections to populate the discovered Server URL picker.
  // Only shows the picker when Server URL is empty.
  const __plexPmsSugCache = { inst: "", at: 0, servers: null, inFlight: false };

  async function plexRefreshPmsSuggestions(opts = {}) {
    const force = !!opts.force;
    const cfg = await fetch("/api/config" + bust(), { cache: "no-store" }).then(r => r.json()).catch(() => ({}));
    const tok = String(getPlexCfgBlock(cfg || {}).account_token || "").trim();
    if (!tok) return;

    const inst = getPlexInstance();
    const now = Date.now();
    const fresh = __plexPmsSugCache.inst === inst && (now - __plexPmsSugCache.at) < 15000 && Array.isArray(__plexPmsSugCache.servers);

    if (!force && fresh) {
      try { fillPlexServerSuggestions(__plexPmsSugCache.servers); } catch {}
      return;
    }

    if (__plexPmsSugCache.inFlight) return;
    __plexPmsSugCache.inFlight = true;
    try {
      const r = await fetch(plexApi("/api/plex/pms"), { cache: "no-store" });
      if (!r.ok) return;
      const j = await r.json().catch(() => ({}));
      const servers = Array.isArray(j?.servers) ? j.servers : [];
      __plexPmsSugCache.inst = inst;
      __plexPmsSugCache.at = now;
      __plexPmsSugCache.servers = servers;
      try { fillPlexServerSuggestions(servers); } catch {}
    } catch {
      // ignore
    } finally {
      __plexPmsSugCache.inFlight = false;
    }
  }

  // Auto-Fetch: prefer /api/plex/pms; then hydrate user/id via /api/plex/inspect
  async function plexAuto() {
    const urlEl = document.getElementById("plex_server_url");
    const setIfEmpty = (el, val) => { if (el && !el.value && val) el.value = String(val); };

    try {
      let cfgUrl = "";
      try {
        const rCfg = await fetch("/api/config?ts=" + Date.now(), { cache: "no-store" });
        if (rCfg.ok) {
          const cfg = await rCfg.json();
          const blk = getPlexCfgBlock(cfg || {});
          cfgUrl = (blk?.server_url || "").trim();
          setIfEmpty(urlEl, cfgUrl);
        }
      } catch {}

      // Fetch /api/plex/pms for server suggestions
      let bestSuggestion = "";
      try {
        const r = await fetch(plexApi("/api/plex/pms"), { cache: "no-store" });
        if (r.ok) {
          const j = await r.json();
          const servers = Array.isArray(j?.servers) ? j.servers : [];
          bestSuggestion = fillPlexServerSuggestions(servers) || "";
        }
      } catch {}

      if (urlEl && bestSuggestion) {
        const curr = (urlEl.value || "").trim();

        const currCloudish = (() => {
          if (!curr) return false;
          try {
            const h = (new URL(curr)).hostname.toLowerCase();
            return h.endsWith(".plex.direct") || h.endsWith(".plex.tv") || h.endsWith(".plexapp.com");
          } catch {
            return /plex\.direct|plex\.tv|plexapp\.com/i.test(curr);
          }
        })();

        const bestPrivateHttp =
          /^http:\/\//i.test(bestSuggestion) &&
          /^(http:\/\/)?(10\.|192\.168\.|172\.(1[6-9]|2\d|3[0-1])\.|127\.|169\.254\.)/i.test(bestSuggestion);

        if (!curr || (currCloudish && bestPrivateHttp)) {
          urlEl.value = bestSuggestion;
          urlEl.dispatchEvent(new Event("input",  { bubbles: true }));
          urlEl.dispatchEvent(new Event("change", { bubbles: true }));
        }
      }

      // Hydrate username/account_id via /api/plex/inspect
      try {
        const rr = await fetch(plexApi("/api/plex/inspect"), { cache: "no-store" });
        if (rr.ok) {
          const dta = await rr.json();
          const set = (id, val) => {
            const el = document.getElementById(id);
            if (el && val != null) el.value = String(val);
          };
          setIfEmpty(urlEl, dta.server_url);
          if (dta.username) set("plex_username", dta.username);
          if (dta.account_id != null) {
            const v = String(dta.account_id).trim();
            if (v) set("plex_account_id", v);
            else set("plex_account_id", "");
          }
        }
      } catch {}

      // Resolve account_id via /api/plex/pickusers if missing.
      await resolvePlexAccountIdFromUsers({ bustCache: true });
} catch (e) {
      console.warn("[plex] Auto-Fetch failed", e);
    }
    try { schedulePlexPmsProbe(200); } catch {}
  }
  


  // Resolve account_id via /api/plex/pickusers (shared by hydrate + auto-fetch)
  async function resolvePlexAccountIdFromUsers(opts = {}) {
    try {
      const idEl = $("plex_account_id");
      if (!idEl) return;
      const userEl = $("plex_username");
      const wantUser = String(opts.username ?? (userEl?.value || "")).trim().toLowerCase();
      const currId = String(idEl.value || "").trim();
      const currNum = parseInt(currId, 10);
      const needsResolve = !(Number.isFinite(currNum) && currNum > 0);
      if (!needsResolve) return;

      if (opts.bustCache) {
        try { __plexUsersByInst.delete(getPlexInstance()); } catch {}
      }

      const users = await fetchPlexUsers();
      const norm = (u) => String(u?.username || u?.title || "").trim().toLowerCase();

      const match = wantUser ? (users.find((u) => norm(u) === wantUser) || null) : null;
      let pick = match;

      if (!pick && needsResolve) {
        pick = users.find((u) => String(u?.type || "").toLowerCase() === "self") || null;
        if (!pick) pick = users.find((u) => String(u?.type || "").toLowerCase() === "owner") || null;
        if (!pick) pick = users.find((u) => String(u?.type || "").toLowerCase() === "managed") || null;
        if (!pick) pick = users[0] || null;
      }

      const uid = pick ? (pick.id ?? pick.account_id) : null;
      if (uid != null) {
        const next = String(uid).trim();
        idEl.value = next || "";
      }

      if (userEl && !userEl.value && pick) {
        const u = String(pick.username || pick.title || "").trim();
        if (u) userEl.value = u;
      }
    } catch {}
  }

  const __plexUsersByInst = new Map();

  function plexLiveQS() {
    let qs = "";
    const url = $("plex_server_url")?.value?.trim() || "";
    if (url) qs += `&server=${encodeURIComponent(url)}`;
    const cb = $("plex_verify_ssl");
    if (cb) qs += `&verify_ssl=${cb.checked ? 1 : 0}`;
    return qs;
  }

  async function fetchPlexUsers() {
    const inst = getPlexInstance();
    const currUrl = $("plex_server_url")?.value?.trim() || "";
    if (currUrl && currUrl !== (w.__lastPlexUrl || "")) {
      try { __plexUsersByInst.delete(inst); } catch {}
      w.__lastPlexUrl = currUrl;
    }
    if (__plexUsersByInst.has(inst)) return __plexUsersByInst.get(inst) || [];
    let out = [];
    try {
      const r = await fetch(plexApi("/api/plex/pickusers") + plexLiveQS(), { cache: "no-store" });
      const j = await r.json();
      out = Array.isArray(j?.users) ? j.users : [];
    } catch { out = []; }
    __plexUsersByInst.set(inst, out);
    return out;
  }

  async function refreshPlexSelectedUserScopeNotice() {
    try {
      const userEl = $("plex_username");
      const idEl = $("plex_account_id");
      const wantUser = String(userEl?.value || "").trim().toLowerCase();
      const wantId = String(idEl?.value || "").trim();
      if (!wantUser && !wantId) {
        setPlexUserScopeNotice(null);
        return;
      }

      const users = await fetchPlexUsers();
      const sameId = (u) => {
        if (!wantId) return false;
        return [u?.id, u?.account_id, u?.cloud_account_id, u?.pms_account_id]
          .some((v) => v != null && String(v).trim() === wantId);
      };
      const sameUser = (u) => {
        if (!wantUser) return false;
        return String(u?.username || u?.title || "").trim().toLowerCase() === wantUser;
      };
      const match = users.find(sameId) || users.find(sameUser) || null;
      setPlexUserScopeNotice(match);
    } catch {
      setPlexUserScopeNotice(null);
    }
  }

  async function openPlexUserPicker() {
    const btn = $("plex_user_pick_btn");
    if (!window.cwMediaUserPicker || typeof window.cwMediaUserPicker.open !== "function") return;
    window.cwMediaUserPicker.open({
      provider: "plex",
      instance: getPlexInstance(),
      server: $("plex_server_url")?.value?.trim() || "",
      verifySsl: !!$("plex_verify_ssl")?.checked,
      anchorEl: btn,
      title: "Pick Plex user",
      onPick: (u) => {
        const uname = String(u?.name || "").trim();
        const uid = String(u?.id || "").trim();
        const raw = u?.raw || {};
        const uEl = $("plex_username"); if (uEl) uEl.value = uname;
        const aEl = $("plex_account_id"); if (aEl) aEl.value = uid;
        setPlexUserScopeNotice({ username: uname, type: raw.type, label: raw.label, source: raw.source });
        try { document.dispatchEvent(new CustomEvent("settings-collect", { detail: { section: "plex-users" } })); } catch {}
      },
    });
  }

  function closePlexUserPicker() { try { window.cwMediaUserPicker?.close?.(); } catch {} }

  function mountPlexUserPicker() {
    const pickBtn = $("plex_user_pick_btn");
    if (pickBtn && !pickBtn.__wired){
      pickBtn.__wired = true;
      pickBtn.addEventListener("click", (e)=>{ e.preventDefault(); openPlexUserPicker(); });
    }
    ["plex_username", "plex_account_id"].forEach((id) => {
      const input = $(id);
      if (!input || input.__plexUserScopeNoticeWired) return;
      input.__plexUserScopeNoticeWired = true;
      input.addEventListener("input", () => setPlexUserScopeNotice(null));
    });
  }

  // Libraries
  async function plexLoadLibraries() {
    let libs = [];
    try {
      const r = await fetch(plexApi("/api/plex/libraries") + plexLiveQS(), { cache: "no-store" });
      if (r.ok) {
        const j = await r.json();
        libs = Array.isArray(j?.libraries) ? j.libraries : [];
      }
    } catch (e) {
      console.warn("[plex] libraries fetch failed", e);
    }

    try {
      const fill = (id) => {
        const el = $(id); if (!el) return;
        const keep = new Set(Array.from(el.selectedOptions || []).map(o => o.value));
        el.innerHTML = "";
        libs.forEach(it => {
          const o = d.createElement("option");
          o.value = String(it.key);
          o.textContent = `${it.title} (${it.type || "lib"}) — #${it.key}`;
          if (keep.has(o.value)) o.selected = true;
          el.appendChild(o);
        });
      };
      fill("plex_lib_history");
      fill("plex_lib_ratings");
      fill("plex_lib_progress");
      fill("plex_lib_scrobble");
    } catch (e) {
      console.warn("[plex] library select fill failed", e);
    }

    try {
      getPlexState().libs = libs.map(it => ({
        id: String(it.key),
        title: String(it.title),
        type: String(it.type || "lib")
      }));
    } catch (e) {
      console.warn("[plex] state update failed", e);
    }
    try {
      const hasServer =
        (document.getElementById("plex_server_url")?.value?.trim() || "") &&
        getPlexState().connected;
      if (!libs.length && hasServer) {
        notify("No libraries could be loaded from Plex. Check the Server URL and make sure this is a Plex server your account can access.");
      }
    } catch {}

    return libs;
  }


  async function refreshPlexLibraries() {
    try {
      const host = document.getElementById("plex_lib_matrix");
      if (host) host.innerHTML = '<div class="sub">Loading libraries…</div>';
    } catch {}
    try { getPlexState().libs = []; } catch {}
    const hydrate = hydratePlexFromConfigRaw().catch(() => {});
    try { await plexLoadLibraries(); } catch {}
    try { mountPlexLibraryMatrix(); } catch {}
    await hydrate;
    try { mountPlexLibraryMatrix(); } catch {}
  }

  // Matrix UI
  function mountPlexLibraryMatrix() {
    const host = $("plex_lib_matrix");
    if (!host || !w.cwWhitelistTable) return;
    const st = getPlexState();
    const setKey = { hist: "hist", rate: "rate", prog: "prog", scr: "scr" };
    const selId  = { hist: "plex_lib_history", rate: "plex_lib_ratings", prog: "plex_lib_progress", scr: "plex_lib_scrobble" };
    const syncSelects = () => {
      Object.keys(selId).forEach((k) => {
        const sel = $(selId[k]); if (!sel) return;
        const set = st[setKey[k]];
        Array.from(sel.options).forEach((o) => { o.selected = set.has(String(o.value)); });
      });
    };
    w.__plexWlHandle = w.cwWhitelistTable.mount({
      host,
      features: [
        { key: "hist", label: "History" },
        { key: "rate", label: "Ratings" },
        { key: "prog", label: "Progress" },
        { key: "scr",  label: "Scrobble" },
      ],
      getLibs: () => getPlexState().libs || [],
      isOn: (fk, id) => st[setKey[fk]].has(String(id)),
      setOn: (fk, id, on) => { const s = st[setKey[fk]]; if (on) s.add(String(id)); else s.delete(String(id)); },
      commit: syncSelects,
      load: async () => { await plexLoadLibraries(); syncSelects(); },
    });
    return w.__plexWlHandle;
  }

  function mergePlexIntoCfg(cfg) {
    const v = (sel) => {
      const el = q(sel);
      return el ? String(el.value || "").trim() : null;
    };

    cfg = cfg || (w.__cfg ||= {});
    const plex = ensurePlexCfgBlock(cfg);

    const url  = v("#plex_server_url");
    const user = v("#plex_username");
    const aid  = v("#plex_account_id");
    const pin  = v("#plex_home_pin");

    if (url)  plex.server_url = url;
    if (user) plex.username   = user;

    if (pin !== null) plex.home_pin = pin;

    // account_id is optional. Keep it empty unless the user picked/entered a valid id.
    if (aid !== null) {
      const raw = String(aid || "").trim();
      if (!raw) plex.account_id = "";
      else {
        const n = parseInt(raw, 10);
        plex.account_id = (Number.isFinite(n) && n > 0) ? n : "";
      }
    }

    try { const cb = $("plex_verify_ssl"); if (cb) plex.verify_ssl = !!cb.checked; } catch {}

    const st = getPlexState();
    const uiReady = !!st.hydrated ||
      !!document.querySelector("#plex_lib_matrix .cw-wl-row") ||
      !!document.querySelector("#plex_lib_history option, #plex_lib_ratings option, #plex_lib_progress option, #plex_lib_scrobble option");
    if (uiReady) {
      const toInts = (set) => Array.from(set || []).map(x => parseInt(String(x), 10)).filter(Number.isFinite);
      const hist = toInts(st.hist);
      const rate = toInts(st.rate);
      const prog = toInts(st.prog);
      const scr  = toInts(st.scr);
      plex.scrobble = Object.assign({}, plex.scrobble || {}, { libraries: scr });
      plex.history  = Object.assign({}, plex.history  || {}, { libraries: hist });
      plex.ratings  = Object.assign({}, plex.ratings  || {}, { libraries: rate });
      plex.progress = Object.assign({}, plex.progress || {}, { libraries: prog });
    }
    return cfg;
  }


  let __plexUrlDirty = false;


  function hookPlexSave() {
    try {
      const api = w.CW?.API?.Config;
      if (api && typeof api.save === "function" && !api._wrappedByPlex) {
        const orig = api.save.bind(api);
        api.save = async (cfg) => {
          try { mergePlexIntoCfg(cfg); } catch {}
          const prevUrl = (w.__lastPlexUrl || "");
          const currUrl = $("#plex_server_url")?.value?.trim() || "";
          __plexUrlDirty = (currUrl !== prevUrl);
          const res = await orig(cfg);
          try {
            if (__plexUrlDirty) {
              await refreshPlexLibraries();
              w.__lastPlexUrl = currUrl;
              __plexUrlDirty = false;
            }
          } catch {}
          return res;
        };
        api._wrappedByPlex = true;
      }
    } catch {}

    d.addEventListener("click", (e) => {
      const t = e.target;
      if (!t) return;
      if (t.id === "save-fab-btn" || t.matches('[data-action="save"], .btn.save, button#save, button[id*="save"]')) {
        try { mergePlexIntoCfg(w.__cfg ||= {}); } catch {}
        setTimeout(() => {
          const prevUrl = (w.__lastPlexUrl || "");
          const currUrl = $("#plex_server_url")?.value?.trim() || "";
          if (currUrl !== prevUrl) {
            refreshPlexLibraries()?.then(()=>{ w.__lastPlexUrl = currUrl; }).catch(()=>{});
          }
        }, 0);
      }
    }, true);

    d.addEventListener("settings-collect", (ev) => {
      try { mergePlexIntoCfg(ev?.detail?.cfg || (w.__cfg ||= {})); } catch {}
    }, true);

    d.addEventListener("cw-auth-profile-created", (ev) => {
      const provider = String(ev?.detail?.provider || "").toLowerCase();
      if (provider !== "plex") return;
      __plexNewProfileInst = getPlexInstance();
      __plexAutoTabInst = __plexNewProfileInst;
      try { syncPlexSetupTabs(); } catch {}
      try { plexAuthSubSelect("auth", { persist: false }); } catch {}
    }, true);

    w.registerSettingsCollector?.((cfg) => { try { mergePlexIntoCfg(cfg); } catch {} });
  }


  let __plexInitDone = false;
  function initPlexAuthUI() {
    try { ensurePlexInstanceUI(); } catch {}
    try { mountPlexAuthTabs(); } catch {}
    try { wirePlexCopyButton(); } catch {}
    try { wirePlexActionButtons(); } catch {}

    if (__plexInitDone) return;
    __plexInitDone = true;
    try { watchForPlexDom(); } catch {}
    try { hookPlexSave(); } catch {}
    setTimeout(() => { try { hydratePlexFromConfigRaw(); } catch {} }, 100);
    setTimeout(() => { try { schedulePlexPmsProbe(300); } catch {} }, 450);
    try { mountPlexLibraryMatrix(); } catch {}
    try { mountPlexUserPicker(); } catch {}
    try { w.__lastPlexUrl = $("plex_server_url")?.value?.trim() || ""; } catch {}

    try {
      const urlEl = $("plex_server_url");
      if (urlEl && !urlEl.__pmsProbeWired) {
        urlEl.__pmsProbeWired = true;
        urlEl.addEventListener("change", () => schedulePlexPmsProbe(300));
        urlEl.addEventListener("blur", () => schedulePlexPmsProbe(300));
        urlEl.addEventListener("input", () => {
          if (!(urlEl.value || "").trim()) { try { plexRefreshPmsSuggestions(); } catch {} }
        });
      }
      const sslEl = $("plex_verify_ssl");
      if (sslEl && !sslEl.__pmsProbeWired) {
        sslEl.__pmsProbeWired = true;
        sslEl.addEventListener("change", () => schedulePlexPmsProbe(300));
      }
    } catch {}
  }

  if (d.readyState === "loading") d.addEventListener("DOMContentLoaded", initPlexAuthUI, { once: true });
  else initPlexAuthUI();

  w.cwAuth = w.cwAuth || {};
  w.cwAuth.plex = w.cwAuth.plex || {};
  w.cwAuth.plex.init = initPlexAuthUI;
  w.cwAuth.plex.rehydrate = () => { try { hydratePlexFromConfigRaw(); } catch {} };

  d.addEventListener("tab-changed", async (ev) => {
    const onSettings = ev?.detail?.id ? /settings/i.test(ev.detail.id) : !!q("#sec-plex");
    if (onSettings) {
      try { watchForPlexDom(); } catch {}
      try { mountPlexAuthTabs(); } catch {}
      try { wirePlexCopyButton(); } catch {}
      try { wirePlexActionButtons(); } catch {}
      await waitFor("#plex_server_url");
      try { hydratePlexFromConfigRaw(); } catch {}
      try { await plexLoadLibraries(); } catch {}
      try { mountPlexLibraryMatrix(); } catch {}
      try { mountPlexUserPicker(); } catch {}
      try { syncPlexSetupTabs({ auto: true }); } catch {}
    } else {
      try { setPlexSuccess(false); } catch {}
    }
  });

  //  exports
  Object.assign(w, {
    setPlexSuccess, requestPlexPin, startPlexTokenPoll, plexDeleteToken,
    mergePlexIntoCfg, plexAuto, plexLoadLibraries,
    hydratePlexFromConfigRaw, mountPlexLibraryMatrix,
    openPlexUserPicker, closePlexUserPicker, mountPlexUserPicker,
    refreshPlexLibraries,
    plexProbePmsReachability,
  });

})(window, document);
