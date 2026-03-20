/* assets/helpers/settings-ui.js */
/* Extracted settings hubs/config hydration from core.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function(){
function formatCwSnapshotLabel(name) {
  if (!name || typeof name !== "string") return name || "";
  const stem = name.replace(/\.json$/,"").split("-", 1)[0];
  if (!/^\d{8}T\d{6}Z$/.test(stem)) return name;

  const year  = stem.slice(0, 4);
  const month = stem.slice(4, 6);
  const day   = stem.slice(6, 8);
  const hour  = stem.slice(9, 11);
  const min   = stem.slice(11, 13);

  return `${year}-${month}-${day} - ${hour}:${min}`;
}

async function loadCrossWatchSnapshots(cfg) {
  const cw = (cfg && cfg.crosswatch) || {};
  const desired = {
    watchlist: (cw.restore_watchlist || "latest").trim() || "latest",
    history:   (cw.restore_history   || "latest").trim() || "latest",
    ratings:   (cw.restore_ratings   || "latest").trim() || "latest",
  };

  try {
    const res = await fetch("/api/files?path=/config/.cw_provider/snapshots");
    if (!res.ok) {
      console.warn("CrossWatch snapshot list HTTP", res.status);
      return;
    }

    const files = await res.json();
    const list = (Array.isArray(files) ? files : []).filter(
      (f) => f && typeof f.name === "string" && f.name.endsWith(".json")
    );

    
    const groups = {
      watchlist: [],
      history:   [],
      ratings:   [],
    };

    for (const f of list) {
      const name = f.name;
      if (name.endsWith("-watchlist.json")) groups.watchlist.push(name);
      else if (name.endsWith("-history.json")) groups.history.push(name);
      else if (name.endsWith("-ratings.json")) groups.ratings.push(name);
    }
    Object.keys(groups).forEach((k) => groups[k].sort());

    const idMap = {
      watchlist: "cw_restore_watchlist",
      history:   "cw_restore_history",
      ratings:   "cw_restore_ratings",
    };

    for (const key of ["watchlist", "history", "ratings"]) {
      const sel = document.getElementById(idMap[key]);
      if (!sel) continue;

      const names = groups[key];
      sel.innerHTML = "";

      
      const baseOpt = document.createElement("option");
      baseOpt.value = "latest";
      baseOpt.textContent = "Latest (default)";
      sel.appendChild(baseOpt);

      for (const name of names) {
        const o = document.createElement("option");
        o.value = name;
        o.textContent = formatCwSnapshotLabel(name);
        sel.appendChild(o);
      }

      const wanted = desired[key] || "latest";
      const hasWanted = names.includes(wanted);
      sel.value = hasWanted ? wanted : "latest";
    }
  } catch (e) {
    console.warn("CrossWatch snapshot list failed", e);
  }
}

/*! Settings */


/* Settings Hub: UI / Security / CW Tracker */

const UI_SETTINGS_TAB_KEY = "cw.ui.settings.tab.v1";

function _uiDaysLeftFromEpochSeconds(epochSeconds) {
  if (!epochSeconds || !Number.isFinite(epochSeconds)) return null;
  const ms = epochSeconds * 1000;
  const diffMs = ms - Date.now();
  if (diffMs <= 0) return 0;
  return Math.ceil(diffMs / (24 * 60 * 60 * 1000));
}

function cwUiSettingsSelect(tab, opts = {}) {
  const t = String(tab || "ui").toLowerCase();
  const persist = opts.persist !== false;

  const hub = document.getElementById("ui_settings_hub");
  const panels = document.getElementById("ui_settings_panels");
  if (!hub || !panels) return;

  const tiles = hub.querySelectorAll(".cw-hub-tile");
  tiles.forEach((btn) => {
    const k = String(btn.dataset.tab || "").toLowerCase();
    btn.classList.toggle("active", k === t);
    btn.setAttribute("aria-selected", k === t ? "true" : "false");
  });

  const ps = panels.querySelectorAll(".cw-settings-panel");
  ps.forEach((p) => {
    const k = String(p.dataset.tab || "").toLowerCase();
    p.classList.toggle("active", k === t);
  });

  if (persist) {
    try { localStorage.setItem(UI_SETTINGS_TAB_KEY, t); } catch {}
  }

  try { cwUiSettingsHubUpdate(); } catch {}
}

function cwUiSettingsHubUpdate() {
  const set = (id, text) => {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
  };

  const wl = document.getElementById("ui_show_watchlist_preview");
  if (wl) set("hub_ui_watchlist", `Watchlist: ${wl.value === "false" ? "Hide" : "Show"}`);

  const pc = document.getElementById("ui_show_playingcard");
  if (pc) set("hub_ui_playing", `Playing: ${pc.value === "false" ? "Hide" : "Show"}`);

  const ai = document.getElementById("ui_show_AI");
  if (ai) set("hub_ui_askai", `ASK AI: ${ai.value === "false" ? "Hide" : "Show"}`);

  const proto = document.getElementById("ui_protocol");
  if (proto) set("hub_ui_proto", `Proto: ${String(proto.value || "http").toUpperCase()}`);

  const aaEnabled = true;
  const aaRememberEnabled = (document.getElementById("app_auth_remember_enabled")?.value || "").toString() === "true";
  const st = window._appAuthStatus || null;

  if (st && st.configured && st.authenticated) {
    set("hub_sec_auth", "Auth: On");
    if (!aaRememberEnabled) {
      set("hub_sec_session", "Session: browser");
    } else {
      const days = _uiDaysLeftFromEpochSeconds(st.session_expires_at);
      set("hub_sec_session", days == null ? "Session: active" : `Session: ${days}d`);
    }
  } else if (st && st.enabled && !st.configured) {
    set("hub_sec_auth", "Auth: On");
    set("hub_sec_session", "Set password");
  } else {
    set("hub_sec_auth", "Auth: On");
    set("hub_sec_session", "Locked");
  }

  // Trusted reverse proxies indicator
  try {
    const raw = (document.getElementById("trusted_proxies")?.value || "").toString().trim();
    let on = false;
    if (raw) {
      on = raw.split(/[;\n,]+/).map(s => s.trim()).filter(Boolean).length > 0;
    } else {
      const tp = window._cfgCache?.security?.trusted_proxies;
      on = Array.isArray(tp) && tp.length > 0;
    }
    set("hub_sec_proxy", `Proxy: ${on ? "On" : "Off"}`);
  } catch {
    set("hub_sec_proxy", "Proxy: —");
  }

  const cwEnabled = (document.getElementById("cw_enabled")?.value || "").toString() !== "false";
  set("hub_cw_enabled", `Tracker: ${cwEnabled ? "On" : "Off"}`);

  const retRaw = (document.getElementById("cw_retention_days")?.value || "").toString().trim();
  const ret = retRaw === "" ? null : parseInt(retRaw, 10);
  if (ret == null || Number.isNaN(ret)) set("hub_cw_retention", "Retention: —");
  else if (ret === 0) set("hub_cw_retention", "Retention: ∞");
  else set("hub_cw_retention", `Retention: ${ret}d`);

  const authFields = document.getElementById("app_auth_fields");
  if (authFields) authFields.classList.remove("cw-disabled");
  const authSessionFields = document.getElementById("app_auth_session_fields");
  if (authSessionFields) authSessionFields.classList.remove("cw-disabled");
  const rememberDaysWrap = document.getElementById("app_auth_remember_days_wrap");
  if (rememberDaysWrap) rememberDaysWrap.classList.toggle("cw-disabled", !aaRememberEnabled);
  const rememberDays = document.getElementById("app_auth_remember_days");
  if (rememberDays) rememberDays.disabled = !aaRememberEnabled;
  try { cwValidateAppAuthRememberDays(); } catch {}

  const trackerFields = document.getElementById("cw_restore_fields");
  if (trackerFields) trackerFields.classList.toggle("cw-disabled", !cwEnabled);
}

function _cwTrustedProxiesEl() {
  return (
    document.getElementById("trusted_proxies") ||
    document.getElementById("trusted_reverse_proxies") ||
    document.getElementById("security_trusted_proxies")
  );
}

function _cwSanitizeAppAuthRememberDays(value) {
  return String(value || "").replace(/\D+/g, "").slice(0, 3);
}

function _cwAppAuthRememberDaysErrorEl() {
  return document.getElementById("app_auth_remember_days_error");
}

function _cwSetAppAuthRememberDaysError(message) {
  const el = _cwAppAuthRememberDaysErrorEl();
  if (!el) return;
  const text = String(message || "").trim();
  el.textContent = text;
  el.classList.toggle("hidden", !text);
}

function cwValidateAppAuthRememberDays(opts = {}) {
  const el = document.getElementById("app_auth_remember_days");
  if (!el) return true;

  if (el.disabled) {
    el.classList.remove("cw-invalid");
    el.removeAttribute("aria-invalid");
    _cwSetAppAuthRememberDaysError("");
    return true;
  }

  const sanitized = _cwSanitizeAppAuthRememberDays(el.value);
  if (el.value !== sanitized) el.value = sanitized;

  const blank = sanitized === "";
  const days = blank ? NaN : parseInt(sanitized, 10);
  const valid = blank || (Number.isFinite(days) && days >= 1 && days <= 365);
  const message = valid ? "" : "Session cache days must be between 1 and 365";

  el.classList.toggle("cw-invalid", !valid);
  if (valid) el.removeAttribute("aria-invalid");
  else el.setAttribute("aria-invalid", "true");
  _cwSetAppAuthRememberDaysError(message);

  return valid;
}

function cwUiSettingsHubInit() {
  if (window.__cwUiSettingsHubInit) return;
  window.__cwUiSettingsHubInit = true;

  const ids = [
    "ui_show_watchlist_preview",
    "ui_show_playingcard",
    "ui_show_AI",
    "ui_protocol",
    "app_auth_username",
    "app_auth_password",
    "app_auth_password2",
    "app_auth_remember_enabled",
    "app_auth_remember_days",
    "trusted_proxies",
    "cw_enabled",
    "cw_retention_days",
    "cw_auto_snapshot",
    "cw_max_snapshots",
    "cw_restore_watchlist",
    "cw_restore_history",
    "cw_restore_ratings"
  ];

  ids.forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    if (el.__hubWired) return;
    el.addEventListener("change", () => { try { cwUiSettingsHubUpdate(); } catch {} });
    el.addEventListener("input",  () => { try { cwUiSettingsHubUpdate(); } catch {} });
    el.__hubWired = true;
  });

  const rememberDays = document.getElementById("app_auth_remember_days");
  if (rememberDays && !rememberDays.__rememberDaysWired) {
    rememberDays.addEventListener("input", () => { try { cwValidateAppAuthRememberDays(); } catch {} });
    rememberDays.addEventListener("blur", () => { try { cwValidateAppAuthRememberDays({ report: true }); } catch {} });
    rememberDays.__rememberDaysWired = true;
  }

  let tab = "ui";
  try {
    const saved = (localStorage.getItem(UI_SETTINGS_TAB_KEY) || "").toLowerCase();
    if (["ui","security","tracker"].includes(saved)) tab = saved;
  } catch {}

  cwUiSettingsSelect(tab, { persist: false });
  try { cwValidateAppAuthRememberDays(); } catch {}
  try { cwUiSettingsHubUpdate(); } catch {}
}

try {
  window.cwUiSettingsSelect = cwUiSettingsSelect;
  window.cwUiSettingsHubInit = cwUiSettingsHubInit;
  window.cwUiSettingsHubUpdate = cwUiSettingsHubUpdate;
  window.cwValidateAppAuthRememberDays = cwValidateAppAuthRememberDays;
} catch {}

async function cwAppAuthPlexRefreshStatus() {
  try {
    const r = await fetch("/api/app-auth/plex/status", { cache: "no-store", credentials: "same-origin" });
    const st = r.ok ? await r.json() : null;
    const label = document.getElementById("app_auth_plex_state");
    if (label) {
      if (!st || !st.linked) label.textContent = "Not linked";
      else {
        const who = [st.linked_username, st.linked_email].filter(Boolean).join(" · ");
        label.textContent = who || "Linked";
      }
    }
    const unlinkBtn = document.getElementById("btn-app-auth-plex-unlink");
    if (unlinkBtn) unlinkBtn.disabled = !(st && st.linked);
    return st;
  } catch {
    const label = document.getElementById("app_auth_plex_state");
    if (label) label.textContent = "Unavailable";
    return null;
  }
}

window.cwAppAuthPlexLink = async function cwAppAuthPlexLink() {
  const btn = document.getElementById("btn-app-auth-plex-link");
  const original = btn?.textContent || "Link Plex account";
  const popup = window.open("about:blank", "cw_plex_link", "width=620,height=760,popup=yes");
  try {
    if (btn) {
      btn.disabled = true;
      btn.textContent = "Waiting for Plex...";
    }
    const r = await fetch("/api/app-auth/plex/link/start", {
      method: "POST",
      cache: "no-store",
      credentials: "same-origin",
    });
    const data = await r.json().catch(() => null);
    if (!r.ok || !data?.ok || !data?.state || !data?.auth_url) {
      if (popup && !popup.closed) popup.close();
      throw new Error(data?.error || `Plex link failed (${r.status})`);
    }
    if (popup && !popup.closed) popup.location.href = data.auth_url;
    else window.open(data.auth_url, "_blank", "noopener,noreferrer");

    for (;;) {
      await new Promise((resolve) => setTimeout(resolve, 2000));
      const pr = await fetch("/api/app-auth/plex/link/check", {
        method: "POST",
        cache: "no-store",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ state: data.state }),
      });
      const pd = await pr.json().catch(() => null);
      if (pr.ok && pd?.ok && pd.pending === true) continue;
      if (!pr.ok || !pd?.ok) throw new Error(pd?.error || `Plex link failed (${pr.status})`);
      if (popup && !popup.closed) popup.close();
      await cwAppAuthPlexRefreshStatus();
      try { window._appAuthStatus && (window._appAuthStatus.plex_sso_enabled = true); } catch {}
      try { _cwShowToast?.("Plex sign-in linked", true); } catch {}
      return;
    }
  } catch (e) {
    if (popup && !popup.closed) popup.close();
    try { _cwShowToast?.(String(e?.message || e || "Plex link failed"), false); } catch {}
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = original;
    }
  }
};

window.cwAppAuthPlexUnlink = async function cwAppAuthPlexUnlink() {
  const ok = window.confirm("Unlink Plex sign-in from this CrossWatch admin?");
  if (!ok) return;
  try {
    const r = await fetch("/api/app-auth/plex/unlink", {
      method: "POST",
      cache: "no-store",
      credentials: "same-origin",
    });
    const data = await r.json().catch(() => null);
    if (!r.ok || !data?.ok) throw new Error(data?.error || `Plex unlink failed (${r.status})`);
    await cwAppAuthPlexRefreshStatus();
    try { _cwShowToast?.("Plex sign-in unlinked", true); } catch {}
  } catch (e) {
    try { _cwShowToast?.(String(e?.message || e || "Plex unlink failed"), false); } catch {}
  }
};


/* Settings Hub: Scheduling */
const SCHED_SETTINGS_TAB_KEY = "cw.ui.scheduling.tab.v1";
const SCHED_PROVIDER_OPEN_KEY = "cw.ui.scheduling.open.v1";

let __cwSchedOpen = false;

function cwSchedProviderSelect(open, opts = {}) {
  const tilesHost = document.getElementById("sched_provider_tiles");
  const panelHost = document.getElementById("sched-provider-panel");
  if (!panelHost) return;

  const wantOpen = (open == null) ? !__cwSchedOpen : !!open;
  __cwSchedOpen = wantOpen;

  if (tilesHost) {
    const tile = tilesHost.querySelector('[data-provider="scheduler"]');
    if (tile) {
      tile.classList.toggle("active", wantOpen);
      tile.setAttribute("aria-selected", wantOpen ? "true" : "false");
    }
  }

  panelHost.classList.toggle("hidden", !wantOpen);

  if (opts.persist !== false) {
    try { localStorage.setItem(SCHED_PROVIDER_OPEN_KEY, wantOpen ? "1" : "0"); } catch {}
  }
}

function cwSchedSettingsSelect(tab, opts = {}) {
  const panelHost = document.getElementById("sched-provider-panel");
  const panel = panelHost?.querySelector('.cw-meta-provider-panel[data-provider="scheduler"]');
  const paneTabs = document.getElementById("sched-pane-tabs");
  if (!panelHost || !panel) return;

  const t = (tab || "basic").toLowerCase();
  const want = ["basic", "advanced"].includes(t) ? t : "basic";

  panel.querySelectorAll(".cw-subtile[data-sub]").forEach((btn) => {
    btn.classList.toggle("active", (btn.dataset.sub || "").toLowerCase() === want);
  });
  paneTabs?.querySelectorAll("[data-sub]").forEach((btn) => {
    btn.classList.toggle("active", (btn.dataset.sub || "").toLowerCase() === want);
  });
  panel.querySelectorAll(".cw-subpanel[data-sub]").forEach((sp) => {
    sp.classList.toggle("active", (sp.dataset.sub || "").toLowerCase() === want);
  });

  if (opts.persist !== false) {
    try { localStorage.setItem(SCHED_SETTINGS_TAB_KEY, want); } catch {}
  }
  try { cwSchedSettingsHubUpdate(); } catch {}
}

function cwBuildSchedulerPanel() {
  const panelHost = document.getElementById("sched-provider-panel");
  if (!panelHost) return;
  if (panelHost.querySelector('.cw-meta-provider-panel[data-provider="scheduler"]')) return;

  const wrap = document.createElement("div");
  wrap.className = "cw-meta-provider-panel active";
  wrap.dataset.provider = "scheduler";

  const subPanels = document.createElement("div");
  subPanels.className = "cw-subpanels";

  const pBasic = document.createElement("div");
  pBasic.className = "cw-subpanel active";
  pBasic.dataset.sub = "basic";

  const pAdv = document.createElement("div");
  pAdv.className = "cw-subpanel";
  pAdv.dataset.sub = "advanced";

  const detach = (id) => {
    const el = document.getElementById(id);
    if (!el) return null;
    try { el.parentNode?.removeChild(el); } catch {}
    return el;
  };

  const mkField = (labelText, ctrl, noteText) => {
    if (!ctrl) return null;
    if (ctrl.classList?.contains("cw-icon-select-native")) {
      ctrl.classList.remove("cw-icon-select-native");
    }
    const f = document.createElement("div");
    f.className = "field";
    f.innerHTML = `<div class="muted" style="margin-bottom:6px;">${labelText}</div>`;
    f.appendChild(ctrl);
    if (noteText) {
      const n = document.createElement("div");
      n.className = "auth-card-notes";
      n.textContent = noteText;
      f.appendChild(n);
    }
    return f;
  };

  const enabledEl = detach("schEnabled");
  const modeEl = detach("schMode");
  const nEl = detach("schN");
  const timeEl = detach("schTime");
  const customValueEl = detach("schCustomValue");
  const customUnitEl = detach("schCustomUnit");

  const basicCard = document.createElement("div");
  basicCard.className = "auth-card";
  const basicFields = document.createElement("div");
  basicFields.className = "auth-card-fields";

  const f1 = mkField("Enable", enabledEl);
  const f2 = mkField("Frequency", modeEl, "Choose the timer mode.");
  const f3 = mkField("Every N hours", nEl, "Only used when Frequency = Every N hours.");
  const f4 = mkField("Time", timeEl, "Only used when Frequency = Daily at…");
  const customWrap = document.createElement("div");
  customWrap.className = "cw-inline-row";
  if (customValueEl) customWrap.appendChild(customValueEl);
  if (customUnitEl) customWrap.appendChild(customUnitEl);
  const f5 = mkField("Custom interval", customWrap.childNodes.length ? customWrap : null, "Only used when Frequency = Custom.");

  [f1, f2, f3, f4, f5].forEach((x) => x && basicFields.appendChild(x));
  if (basicFields.childNodes.length) basicCard.appendChild(basicFields);
  pBasic.appendChild(basicCard);

  const advMount = detach("sched_advanced_mount") || (() => {
    const d = document.createElement("div");
    d.id = "sched_advanced_mount";
    return d;
  })();

  pAdv.appendChild(advMount);

  subPanels.appendChild(pBasic);
  subPanels.appendChild(pAdv);

  wrap.appendChild(subPanels);

  panelHost.appendChild(wrap);

  try {
    const raw = document.getElementById("sched-provider-raw");
    if (raw) raw.classList.add("hidden");
  } catch {}

  document.querySelectorAll("#sched-pane-tabs [data-sub]").forEach((btn) => {
    btn.addEventListener("click", () => cwSchedSettingsSelect(btn.dataset.sub));
  });

  let lastSub = "basic";
  try { lastSub = (localStorage.getItem(SCHED_SETTINGS_TAB_KEY) || "basic").toLowerCase(); } catch {}
  cwSchedSettingsSelect((lastSub === "advanced") ? "advanced" : "basic", { persist: false });
}

function cwSchedProviderEnsure() {
  const tilesHost = document.getElementById("sched_provider_tiles");
  const panelHost = document.getElementById("sched-provider-panel");
  if (!panelHost) return;

  if (!panelHost.dataset.__cwSchedBuilt) {
    try { cwBuildSchedulerPanel(); } catch {}
    panelHost.dataset.__cwSchedBuilt = "1";
  }

  if (tilesHost) {
    tilesHost.querySelectorAll("[data-provider]").forEach((btn) => {
      if (btn.__cwSchedWired) return;
      btn.addEventListener("click", () => {
        const isOpen = !document.getElementById("sched-provider-panel")?.classList.contains("hidden");
        cwSchedProviderSelect(!isOpen);
      });
      btn.__cwSchedWired = true;
    });

    let open = "0";
    try { open = localStorage.getItem(SCHED_PROVIDER_OPEN_KEY) || "0"; } catch {}
    cwSchedProviderSelect(open === "1", { persist: false });
  } else {
    cwSchedProviderSelect(true, { persist: false });
  }

  try { cwSchedSettingsHubUpdate(); } catch {}
}

function cwSchedSettingsHubUpdate() {
  const set = (id, text) => {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
  };

  let patch = null;
  try {
    patch = (typeof window.getSchedulingPatch === "function") ? window.getSchedulingPatch() : null;
  } catch {}

  if (!patch) {
    const enabled = (document.getElementById("schEnabled")?.value || "").toString().trim() === "true";
    const mode = document.getElementById("schMode")?.value || "hourly";
    const every_n_hours = parseInt(document.getElementById("schN")?.value || "12", 10);
    const daily_time = document.getElementById("schTime")?.value || "03:30";
    const customValue = parseInt(document.getElementById("schCustomValue")?.value || "60", 10) || 60;
    const customUnit = document.getElementById("schCustomUnit")?.value || "minutes";
    const custom_interval_minutes = Math.max(15, customUnit === "hours" ? customValue * 60 : customValue);
    const advOn = !!document.getElementById("schAdvEnabled")?.checked;
    patch = { enabled, mode, every_n_hours, daily_time, custom_interval_minutes, advanced: { enabled: advOn, jobs: [] } };
  }

  set("hub_sch_enabled", `Status: ${patch.enabled ? "Enabled" : "Disabled"}`);

  let modeText = patch.mode || "hourly";
  if (patch.mode === "hourly") modeText = "Every hour";
  else if (patch.mode === "every_n_hours") modeText = `Every ${patch.every_n_hours || 2}h`;
  else if (patch.mode === "daily_time") modeText = `Daily ${patch.daily_time || "—"}`;
  else if (patch.mode === "custom_interval") {
    const minutes = Math.max(15, parseInt(patch.custom_interval_minutes || 60, 10) || 60);
    modeText = minutes % 60 === 0 ? `Custom ${minutes / 60}h` : `Custom ${minutes} min`;
  }
  set("hub_sch_mode", `Mode: ${modeText}`);

  const adv = patch.advanced || {};
  const jobs = Array.isArray(adv.jobs) ? adv.jobs : [];
  const active = jobs.filter(j => j && j.active !== false).length;
  const total = jobs.length;

  set("hub_sch_adv", `Plan: ${adv.enabled ? "On" : "Off"}`);
  set("hub_sch_steps", total ? `Steps: ${active}/${total}` : "Steps: —");
}

function cwSchedSettingsHubInit() {
  const first = !window.__cwSchedSettingsHubInit;
  if (first) window.__cwSchedSettingsHubInit = true;

  try { cwSchedProviderEnsure(); } catch {}

  const wire = (id) => {
    const el = document.getElementById(id);
    if (!el || el.__hubWired) return;
    el.addEventListener("change", () => { try { cwSchedSettingsHubUpdate(); } catch {} });
    el.addEventListener("input",  () => { try { cwSchedSettingsHubUpdate(); } catch {} });
    el.__hubWired = true;
  };

  ["schEnabled", "schMode", "schN", "schTime", "schCustomValue", "schCustomUnit", "schAdvEnabled"].forEach(wire);

  const adv = document.getElementById("schAdv");
  if (adv && !adv.__hubWired) {
    adv.addEventListener("change", () => { try { cwSchedSettingsHubUpdate(); } catch {} }, true);
    adv.addEventListener("input",  () => { try { cwSchedSettingsHubUpdate(); } catch {} }, true);
    adv.__hubWired = true;
  }

  if (first) {
    let tab = "basic";
    try {
      const saved = (localStorage.getItem(SCHED_SETTINGS_TAB_KEY) || "").toLowerCase();
      if (["basic", "advanced"].includes(saved)) tab = saved;
    } catch {}
    cwSchedSettingsSelect(tab, { persist: false });
  }

  try { cwSchedSettingsHubUpdate(); } catch {}
}

try {
  window.cwSchedProviderSelect = cwSchedProviderSelect;
  window.cwSchedProviderEnsure = cwSchedProviderEnsure;
  window.cwSchedSettingsSelect = cwSchedSettingsSelect;
  window.cwSchedSettingsHubInit = cwSchedSettingsHubInit;
  window.cwSchedSettingsHubUpdate = cwSchedSettingsHubUpdate;
} catch {}


/* Settings Hub: Metadata Providers */
const META_SETTINGS_TAB_KEY = "cw.ui.metadata.tab.v1";
const META_PROVIDER_STATE_KEY = "cw.ui.meta.provider.v1";
const TMDB_META_SUBTAB_KEY = "cw.ui.meta.tmdb.sub.v1";

let activeMetaProvider = null;

function cwMetaProviderUpdateChips() {
  try { cwMetaSettingsHubUpdate?.(); } catch {}
}

function cwMetaProviderSelect(provider, opts = {}) {
  const want = provider ? String(provider).toLowerCase() : null;

  const tilesHost = document.getElementById("meta_provider_tiles");
  const panelHost = document.getElementById("meta-provider-panel");
  if (!tilesHost || !panelHost) return;

  const tiles = tilesHost.querySelectorAll("[data-provider]");
  tiles.forEach((btn) => {
    const k = String(btn.dataset.provider || "").toLowerCase();
    const on = !!(want && k === want);
    btn.classList.toggle("active", on);
    btn.setAttribute("aria-selected", on ? "true" : "false");
  });

  activeMetaProvider = want;
  panelHost.classList.toggle("hidden", !want);

  const panels = panelHost.querySelectorAll(".cw-meta-provider-panel");
  panels.forEach((p) => {
    const k = String(p.dataset.provider || "").toLowerCase();
    p.classList.toggle("active", !!(want && k === want));
  });

  if (opts.persist !== false) {
    try { localStorage.setItem(META_PROVIDER_STATE_KEY, want || ""); } catch {}
  }

  try { cwMetaProviderUpdateChips(); } catch {}
}

function cwMetaProviderSubSelect(provider, sub, opts = {}) {
  const p = (provider || "").toLowerCase();
  const s = (sub || "").toLowerCase();
  if (!p || !s) return;

  const panelHost = document.getElementById("meta-provider-panel");
  const panel = panelHost?.querySelector(`.cw-meta-provider-panel[data-provider="${p}"]`);
  if (!panel) return;

  const tiles = panel.querySelectorAll(".cw-subtile[data-sub]");
  tiles.forEach((b) => b.classList.toggle("active", (b.dataset.sub || "").toLowerCase() === s));

  const subs = panel.querySelectorAll(".cw-subpanel[data-sub]");
  subs.forEach((sp) => sp.classList.toggle("active", (sp.dataset.sub || "").toLowerCase() === s));

  if (opts.persist !== false) {
    try { localStorage.setItem(TMDB_META_SUBTAB_KEY, s); } catch {}
  }
}

function cwMetaProviderInit() {
  cwMetaProviderSelect(null, { persist: false });
}

function cwMetaProviderEnsure() {
  const tilesHost = document.getElementById("meta_provider_tiles");
  const panelHost = document.getElementById("meta-provider-panel");
  if (!tilesHost || !panelHost) return;

  tilesHost.querySelectorAll("[data-provider]").forEach((btn) => {
    if (btn.__cwMetaWired) return;
    btn.addEventListener("click", () => {
      const want = String(btn.dataset.provider || "").toLowerCase();
      if (want && activeMetaProvider === want) cwMetaProviderSelect(null);
      else cwMetaProviderSelect(btn.dataset.provider || null);
    });
    btn.__cwMetaWired = true;
  });

  if (!panelHost.dataset.__cwMetaBuilt) {
    try { cwBuildTmdbPanel(); } catch {}
    panelHost.dataset.__cwMetaBuilt = "1";
  }

  try {
    const keyEl = document.getElementById("tmdb_api_key");
    if (keyEl && !keyEl.__tmdbChipWired) {
      keyEl.addEventListener("input", () => { try { cwMetaProviderUpdateChips(); } catch {} });
      keyEl.__tmdbChipWired = true;
    }
  } catch {}

  try { cwMetaProviderInit(); } catch {}
  try { cwMetaProviderUpdateChips(); } catch {}
}

function cwBuildTmdbPanel() {
  const panelHost = document.getElementById("meta-provider-panel");
  if (!panelHost) return;

  if (panelHost.querySelector('.cw-meta-provider-panel[data-provider="tmdb"]')) return;

  const wrap = document.createElement("div");
  wrap.className = "cw-meta-provider-panel";
  wrap.dataset.provider = "tmdb";

  const head = document.createElement("div");
  head.className = "cw-panel-head";
  head.innerHTML = `
    <div>
      <div class="cw-panel-title">TMDb (The Movie Database)</div>
      <div class="muted">Metadata and images fetched from TMDb.</div>
    </div>
  `;

  const subTiles = document.createElement("div");
  subTiles.className = "cw-subtiles";
  subTiles.innerHTML = `
    <button type="button" class="cw-subtile active" data-sub="api">API key</button>
    <button type="button" class="cw-subtile" data-sub="advanced">Advanced</button>
  `;

  const subPanels = document.createElement("div");
  subPanels.className = "cw-subpanels";

  const pApi = document.createElement("div");
  pApi.className = "cw-subpanel active";
  pApi.dataset.sub = "api";

  const pAdv = document.createElement("div");
  pAdv.className = "cw-subpanel";
  pAdv.dataset.sub = "advanced";

  const detach = (id) => {
    const el = document.getElementById(id);
    if (!el) return null;
    try { el.parentNode?.removeChild(el); } catch {}
    return el;
  };

  const keyInput = detach("tmdb_api_key") || (() => {
    const i = document.createElement("input");
    i.id = "tmdb_api_key";
    i.type = "text";
    i.placeholder = "TMDb API key";
    return i;
  })();

  const hint = detach("tmdb_hint") || (() => {
    const d = document.createElement("div");
    d.id = "tmdb_hint";
    d.className = "auth-card-notes";
    d.textContent = "Add a TMDb API key to enable metadata lookups.";
    return d;
  })();

  if (!hint.classList.contains("auth-card-notes")) hint.classList.add("auth-card-notes");

  const apiCard = document.createElement("div");
  apiCard.className = "auth-card";

  const apiFields = document.createElement("div");
  apiFields.className = "auth-card-fields";

  const apiField = document.createElement("div");
  apiField.className = "field";
  apiField.innerHTML = `<div class="muted" style="margin-bottom:6px;">API key</div>`;
  apiField.appendChild(keyInput);

  apiFields.appendChild(apiField);
  apiCard.appendChild(hint);
  apiCard.appendChild(apiFields);

  pApi.appendChild(apiCard);

  const localeEl = detach("metadata_locale");
  const ttlEl = detach("metadata_ttl_hours");

  const advCard = document.createElement("div");
  advCard.className = "auth-card";

  const advFields = document.createElement("div");
  advFields.className = "auth-card-fields";

  if (localeEl) {
    const f = document.createElement("div");
    f.className = "field";
    f.innerHTML = `<div class="muted" style="margin-bottom:6px;">Language / locale</div>`;
    advFields.appendChild(f);
    f.appendChild(localeEl);
    const note = document.createElement("div");
    note.className = "auth-card-notes";
    note.textContent = "Optional. Example: en-US, nl-NL.";
    advFields.appendChild(note);
  }

  if (ttlEl) {
    const f = document.createElement("div");
    f.className = "field";
    f.innerHTML = `<div class="muted" style="margin-bottom:6px;">Cache TTL (hours)</div>`;
    f.appendChild(ttlEl);
    advFields.appendChild(f);
    const note = document.createElement("div");
    note.className = "auth-card-notes";
    note.textContent = "How long metadata stays cached before re-fetching.";
    advFields.appendChild(note);
  }

  if (!localeEl && !ttlEl) {
    const note = document.createElement("div");
    note.className = "auth-card-notes";
    note.textContent = "No advanced options available yet.";
    advCard.appendChild(note);
  }

  if (advFields.childNodes.length) advCard.appendChild(advFields);
  pAdv.appendChild(advCard);

  subPanels.appendChild(pApi);
  subPanels.appendChild(pAdv);

  wrap.appendChild(head);
  wrap.appendChild(subTiles);
  wrap.appendChild(subPanels);

  panelHost.appendChild(wrap);

  subTiles.querySelectorAll(".cw-subtile[data-sub]").forEach((btn) => {
    btn.addEventListener("click", () => cwMetaProviderSubSelect("tmdb", btn.dataset.sub));
  });

  let lastSub = "api";
  try { lastSub = (localStorage.getItem(TMDB_META_SUBTAB_KEY) || "api").toLowerCase(); } catch {}
  cwMetaProviderSubSelect("tmdb", (lastSub === "advanced") ? "advanced" : "api", { persist: false });
}

try {
  window.cwMetaProviderSelect = cwMetaProviderSelect;
  window.cwMetaProviderEnsure = cwMetaProviderEnsure;
  window.cwMetaProviderSubSelect = cwMetaProviderSubSelect;
} catch {}


function cwMetaSettingsSelect(tab, opts = {}) {
  const hub = document.getElementById("meta_settings_hub");
  const panels = document.getElementById("meta_settings_panels");
  if (!hub || !panels) return;

  const t = (tab || "tmdb").toLowerCase();
  const want = ["tmdb"].includes(t) ? t : "tmdb";

  hub.querySelectorAll(".cw-hub-tile").forEach((btn) => {
    btn.classList.toggle("active", (btn.dataset.tab || "") === want);
  });
  panels.querySelectorAll(".cw-settings-panel").forEach((p) => {
    p.classList.toggle("active", (p.dataset.tab || "") === want);
  });

  if (opts.persist !== false) {
    try { localStorage.setItem(META_SETTINGS_TAB_KEY, want); } catch {}
  }
  try { cwMetaSettingsHubUpdate(); } catch {}
}

function cwMetaSettingsHubUpdate() {
  const chip = document.getElementById("hub_tmdb_key");
  if (!chip) return;

  const cfg = window._cfgCache || {};
  const cfgKey = String(cfg?.tmdb?.api_key || "").trim();
  const cfgMasked = cfgKey === "*****" || /^[•]+$/.test(cfgKey);
  const cfgHasKey = cfgKey.length > 0 || cfgMasked;

  const keyEl = document.getElementById("tmdb_api_key");
  let uiHasKey = false;
  let uiTouched = false;

  if (keyEl) {
    const v = String(keyEl.value || "").trim();
    uiTouched = keyEl.dataset?.touched === "1";
    const vMasked = v === "*****" || /^[•]+$/.test(v);
    const dsMasked = keyEl.dataset?.masked === "1";
    uiHasKey = v.length > 0 || vMasked || dsMasked;
    if (uiTouched) uiHasKey = v.length > 0 || vMasked;
  }

  const hasKeyNow = uiHasKey || (!uiTouched && cfgHasKey);
  chip.textContent = `API key: ${hasKeyNow ? "set" : "missing"}`;
}

function cwMetaSettingsHubInit() {
  let last = null;
  try { last = localStorage.getItem(META_SETTINGS_TAB_KEY); } catch {}
  cwMetaSettingsSelect(last || "tmdb", { persist: false });
  try { cwMetaSettingsHubUpdate(); } catch {}
}

function cwMetaSettingsHubEnsure() {
  const host = document.getElementById("metadata-providers");
  if (!host || host.dataset.metaHubified === "1") return;

  // New provider presents
  if (document.getElementById("meta_provider_tiles") || document.getElementById("meta-provider-panel")) {
    host.dataset.metaHubified = "1";
    return;
  }

  if (host.querySelector("#meta_settings_hub")) {
    host.dataset.metaHubified = "1";
    return;
  }

  const hub = document.createElement("div");
  hub.className = "cw-settings-hub cw-settings-hub--single";
  hub.id = "meta_settings_hub";

  const tile = document.createElement("button");
  tile.type = "button";
  tile.className = "cw-hub-tile tmdb active";
  tile.dataset.tab = "tmdb";
  const tmdbLogo = window.CW?.ProviderMeta?.logoPath?.("tmdb") || "/assets/img/TMDB.svg";
  tile.innerHTML = `
    <div class="cw-hub-dots" aria-hidden="true">
      <span class="cw-hub-dot dot-a"></span>
      <span class="cw-hub-dot dot-b"></span>
      <span class="cw-hub-dot dot-c"></span>
    </div>
    <div class="cw-hub-title-row">
      <img class="cw-hub-logo" src="${tmdbLogo}" alt="" loading="lazy">
      <div>
        <div class="cw-hub-title">TMDb</div>
        <div class="cw-hub-desc">The Movie Database</div>
      </div>
    </div>
    <div class="chips">
      <span class="chip" id="hub_tmdb_key">API key: —</span>
    </div>
  `;
  tile.addEventListener("click", () => cwMetaSettingsSelect("tmdb"));

  hub.appendChild(tile);

  const panels = document.createElement("div");
  panels.className = "cw-settings-panels";
  panels.id = "meta_settings_panels";

  const panel = document.createElement("div");
  panel.className = "cw-settings-panel active";
  panel.dataset.tab = "tmdb";

  while (host.firstChild) panel.appendChild(host.firstChild);

  panels.appendChild(panel);

  host.appendChild(hub);
  host.appendChild(panels);

  host.dataset.metaHubified = "1";

  const keyEl = document.getElementById("tmdb_api_key");
  if (keyEl && !keyEl.__tmdbChipWired) {
    keyEl.addEventListener("input", () => {
      try { cwMetaSettingsHubUpdate(); } catch {}
    });
    keyEl.__tmdbChipWired = true;
  }

  setTimeout(() => {
    try { cwMetaSettingsHubInit(); } catch {}
  }, 0);
}

try {
  window.cwMetaSettingsSelect = cwMetaSettingsSelect;
  window.cwMetaSettingsHubInit = cwMetaSettingsHubInit;
  window.cwMetaSettingsHubUpdate = cwMetaSettingsHubUpdate;
  window.cwMetaSettingsHubEnsure = cwMetaSettingsHubEnsure;
} catch {}

async function loadConfig() {
  const r = await fetch("/api/config", { cache: "no-store", credentials: "same-origin" });
  if (r.status === 401) {
    location.href = "/login";
    return;
  }
  if (!r.ok) throw new Error(`GET /api/config ${r.status}`);
  const cfg = await r.json();
  window._cfgCache = cfg;

  const _refreshSelectUi = (el) => {
    if (!el) return;
    try { el.dispatchEvent(new Event("change")); } catch {}
    try { window.CW?.IconSelect?.enhance?.(el, el.__cwIconSelectCfg || { className: "cw-plain-select" }); } catch {}
  };
  const _setSelectValue = (id, value) => {
    const el = document.getElementById(id);
    if (!el) return null;
    el.value = value;
    _refreshSelectUi(el);
    return el;
  };

  try { bindSyncVisibilityObservers?.(); } catch {}
  try {
    if (typeof scheduleApplySyncVisibility === "function") scheduleApplySyncVisibility();
    else applySyncVisibility?.();
  } catch {}

  _setSelectValue("mode", cfg.sync?.bidirectional?.mode || "two-way");
  _setSelectValue("source", cfg.sync?.bidirectional?.source_of_truth || "plex");
  (function(){
    const rt = cfg.runtime || {};
    let mode = 'off';
    if (rt.debug) mode = (rt.debug_mods && rt.debug_http) ? 'full' : (rt.debug_mods ? 'mods' : 'on');
    _setSelectValue("debug", mode);
  })();
  _setVal("metadata_locale", cfg.metadata?.locale || "");
  _setVal("metadata_ttl_hours", String(Number.isFinite(cfg.metadata?.ttl_hours) ? cfg.metadata.ttl_hours : 72));

  
  (function () {
    const ui = cfg.ui || cfg.user_interface || {};
    const cw = cfg.crosswatch || {};
    const aa = cfg.app_auth || {};

    
    {
      const on = (typeof ui.show_watchlist_preview === "boolean")
        ? !!ui.show_watchlist_preview
        : true;
      _setSelectValue("ui_show_watchlist_preview", on ? "true" : "false");
    }

    {
      const on = (typeof ui.show_playingcard === "boolean")
        ? !!ui.show_playingcard
        : true;
      _setSelectValue("ui_show_playingcard", on ? "true" : "false");
    }

    {
      const on = (typeof ui.show_AI === "boolean")
        ? !!ui.show_AI
        : true;
      _setSelectValue("ui_show_AI", on ? "true" : "false");
    }

    {
      const p = String(ui.protocol || "http").trim().toLowerCase();
      _setSelectValue("ui_protocol", (p === "https") ? "https" : "http");
    }

    const aaUserEl = document.getElementById("app_auth_username");
    if (aaUserEl) aaUserEl.value = (typeof aa.username === "string") ? aa.username : "";
    _setSelectValue("app_auth_remember_enabled", (aa.remember_session_enabled === true) ? "true" : "false");
    const aaRememberDaysEl = document.getElementById("app_auth_remember_days");
    if (aaRememberDaysEl) {
      const days = Number.isFinite(aa.remember_session_days) ? aa.remember_session_days : 30;
      aaRememberDaysEl.value = String(Math.max(1, Math.min(365, Number(days) || 30)));
      try { cwValidateAppAuthRememberDays(); } catch {}
    }
    const aaP1 = document.getElementById("app_auth_password");
    if (aaP1) aaP1.value = "";
    const aaP2 = document.getElementById("app_auth_password2");
    if (aaP2) aaP2.value = "";

    // Trusted reverse proxies (optional)
    const tpEl = _cwTrustedProxiesEl();
    if (tpEl) {
      const tp = (cfg.security && Array.isArray(cfg.security.trusted_proxies)) ? cfg.security.trusted_proxies : [];
      tpEl.value = tp.filter((x) => typeof x === "string" && x.trim()).join(";");
    }


    
    {
      const enabled = (cw.enabled === false) ? "false" : "true";
      _setSelectValue("cw_enabled", enabled);
    }
    const cwRetEl = document.getElementById("cw_retention_days");
    if (cwRetEl) {
      const v = Number.isFinite(cw.retention_days) ? cw.retention_days : 30;
      cwRetEl.value = String(v);
    }
    {
      const on = (cw.auto_snapshot === false) ? "false" : "true";
      _setSelectValue("cw_auto_snapshot", on);
    }
    const cwMaxEl = document.getElementById("cw_max_snapshots");
    if (cwMaxEl) {
      const v = Number.isFinite(cw.max_snapshots) ? cw.max_snapshots : 64;
      cwMaxEl.value = String(v);
    }
    const setVal = (id, val) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.value = val || "latest";
      _refreshSelectUi(el);
    };
    setVal("cw_restore_watchlist", cw.restore_watchlist || "latest");
    setVal("cw_restore_history", cw.restore_history || "latest");
    setVal("cw_restore_ratings", cw.restore_ratings || "latest");
  })();

  try { cwUiSettingsHubInit?.(); } catch {}

  await loadCrossWatchSnapshots(cfg);
  window.appDebug = !!(cfg?.runtime?.debug || cfg?.runtime?.debug_mods);


(function hydrateSecretsRaw(cfg){
  const val = (x) => (typeof x === "string" ? x.trim() : "");
  const setRaw = (id, v) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.value = v || "";
    el.dataset.masked  = "0";
    el.dataset.loaded  = "1";
    el.dataset.touched = "";
    el.dataset.clear   = "";
    try { wireSecretTouch(id); } catch {}
  };

  
  setRaw("plex_token",    val(cfg.plex?.account_token));
  setRaw("plex_home_pin", val(cfg.plex?.home_pin));

  
  setRaw("simkl_client_id",     val(cfg.simkl?.client_id));
  setRaw("simkl_client_secret", val(cfg.simkl?.client_secret));
  setRaw("simkl_access_token",  val(cfg.simkl?.access_token) || val(cfg.auth?.simkl?.access_token));

  
  setRaw("anilist_client_id",     val(cfg.anilist?.client_id));
  setRaw("anilist_client_secret", val(cfg.anilist?.client_secret));
  setRaw("anilist_access_token",  val(cfg.anilist?.access_token) || val(cfg.auth?.anilist?.access_token));

  
  setRaw("tmdb_api_key",        val(cfg.tmdb?.api_key));

  
  setRaw("mdblist_key",         val(cfg.mdblist?.api_key));

  
  setRaw("trakt_client_id",     val(cfg.trakt?.client_id));
  setRaw("trakt_client_secret", val(cfg.trakt?.client_secret));
  setRaw("trakt_token",         val(cfg.trakt?.access_token) || val(cfg.auth?.trakt?.access_token));
})(cfg);

  try { cwMetaSettingsHubUpdate(); } catch {}

  const s = cfg.scheduling || {};
  _setSelectValue("schEnabled", String(!!s.enabled));
  _setSelectValue("schMode", typeof s.mode === "string" && s.mode ? s.mode : "hourly");
  _setVal("schN",       Number.isFinite(s.every_n_hours) ? String(s.every_n_hours) : "12");
  _setVal("schTime",    typeof s.daily_time === "string" && s.daily_time ? s.daily_time : "03:30");
  const customMinutes = Math.max(15, parseInt(s.custom_interval_minutes ?? 60, 10) || 60);
  if (customMinutes % 60 === 0) {
    _setVal("schCustomValue", String(Math.max(1, customMinutes / 60)));
    _setSelectValue("schCustomUnit", "hours");
  } else {
    _setVal("schCustomValue", String(customMinutes));
    _setSelectValue("schCustomUnit", "minutes");
  }
  if (document.getElementById("schTz")) _setSelectValue("schTz", s.timezone || "");

  try {
    const r = await fetch("/api/app-auth/status", { cache: "no-store", credentials: "same-origin" });
    const st = r.ok ? await r.json() : null;
    window._appAuthStatus = st;
    const rememberEnabled = st
      ? !!st.remember_session_enabled
      : ((document.getElementById("app_auth_remember_enabled")?.value || "").toString() === "true");

    try {
      const aaUserEl = document.getElementById("app_auth_username");
      if (aaUserEl && st && st.configured) aaUserEl.value = (st.username || "").toString();
    } catch {}

    const el = document.getElementById("app_auth_state");
    if (el) {
      if (!st) el.textContent = "—";
      else if (!st.configured || st.reset_required) el.textContent = "Auth: set password";
      else if (st.authenticated) {
        const exp = (st.session_expires_at && st.session_expires_at > 0) ? new Date(st.session_expires_at * 1000) : null;
        el.textContent = !rememberEnabled
          ? "Auth: signed in (browser session)"
          : (exp ? `Auth: signed in (until ${exp.toISOString().replace('T',' ').slice(0,16)}Z)` : "Auth: signed in");
      } else el.textContent = "Auth: locked";
    }
    const btn = document.getElementById("btn-auth-logout");
    if (btn) btn.disabled = !(st && st.authenticated);
  } catch {}

  try { await cwAppAuthPlexRefreshStatus(); } catch {}
  try { cwUiSettingsHubUpdate?.(); } catch {}

  try { window.updateSimklButtonState?.(); } catch {}
  try { window.updateSimklHint?.();      } catch {}
  try { window.updateTmdbHint?.();       } catch {}
  try {
    if (typeof scheduleApplySyncVisibility === "function") scheduleApplySyncVisibility();
    else applySyncVisibility?.();
  } catch {}
}

window.cwAppLogout = async function cwAppLogout() {
  try {
    await fetch("/api/app-auth/logout", { method: "POST", cache: "no-store", credentials: "same-origin" });
  } catch {}
  location.href = "/login";
};

async function updateTmdbHint() {
  const hint = document.getElementById("tmdb_hint");
  const input = document.getElementById("tmdb_api_key");

  if (!hint || !input) return;

  const settingsVisible = !document
    .getElementById("page-settings")
    ?.classList.contains("hidden");

  if (!settingsVisible) return;

  const v = (input.value || "").trim();

  if (document.activeElement === input) input.dataset.dirty = "1";

  if (input.dataset.dirty === "1") {
    hint.classList.toggle("hidden", !!v);
    return;
  }

  if (v) {
    hint.classList.add("hidden");
    return;
  }

  try {
    const cfg = await fetch("/api/config", { cache: "no-store" }).then((r) =>
      r.json()
    );

    const has = !!(cfg.tmdb?.api_key || "").trim();

    hint.classList.toggle("hidden", has);
  } catch {
    hint.classList.remove("hidden");
  }
}

function setTraktSuccess(show) {
  const el = document.getElementById("trakt_msg");
  if (el) el.classList.toggle("hidden", !show);
}

  const SettingsUI = {
    formatCwSnapshotLabel,
    loadCrossWatchSnapshots,
    cwUiSettingsSelect,
    cwUiSettingsHubUpdate,
    cwUiSettingsHubInit,
    cwSchedProviderSelect,
    cwSchedSettingsSelect,
    cwBuildSchedulerPanel,
    cwSchedProviderEnsure,
    cwSchedSettingsHubUpdate,
    cwSchedSettingsHubInit,
    cwMetaProviderUpdateChips,
    cwMetaProviderSelect,
    cwMetaProviderSubSelect,
    cwMetaProviderInit,
    cwMetaProviderEnsure,
    cwBuildTmdbPanel,
    cwMetaSettingsSelect,
    cwMetaSettingsHubUpdate,
    cwMetaSettingsHubInit,
    cwMetaSettingsHubEnsure,
    loadConfig,
    updateTmdbHint,
    setTraktSuccess,
  };

  (window.CW ||= {}).SettingsUI = SettingsUI;
  Object.assign(window, SettingsUI);
})();
