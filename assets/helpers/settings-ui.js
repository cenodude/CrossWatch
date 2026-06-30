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

function cwUiSettingsSelect(tab, opts = {}) {
  const t = String(tab || "ui").toLowerCase();
  const persist = opts.persist !== false;

  const panels = document.getElementById("ui_settings_panels");
  if (!panels) return;

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
  const aaRememberEnabled = (document.getElementById("app_auth_remember_enabled")?.value || "").toString() === "true";
  const cwEnabled = (document.getElementById("cw_enabled")?.value || "").toString() !== "false";

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
    "ui_show_recent_activity",
    "ui_show_recent_history_widget",
    "ui_show_latest_ratings_widget",
    "ui_show_recent_scrobble_widget",
    "ui_recent_activity_display",
    "ui_recent_syncs_display",
    "ui_show_AI",
    "ui_show_quick_add_desktop",
    "ui_show_quick_add_mobile",
    "ui_theme",
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
const ANIME_MAPPING_PROVIDER = "anime-mapping";

let activeMetaProvider = null;
let animeMappingBusy = false;

function cwMetaProviderUpdateChips() {
  try { cwMetaSettingsHubUpdate?.(); } catch {}
}

function cwMetaProviderSelect(provider, opts = {}) {
  const want = provider ? String(provider).toLowerCase() : null;

  const panelHost = document.getElementById("meta-provider-panel");
  if (!panelHost) return;

  activeMetaProvider = want;
  panelHost.classList.remove("hidden");

  const panels = panelHost.querySelectorAll(".cw-meta-provider-panel");
  panels.forEach((p) => {
    p.classList.add("active");
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
  const panelHost = document.getElementById("meta-provider-panel");
  if (!panelHost) return;

  if (!panelHost.dataset.__cwMetaBuilt) {
    try { cwBuildTmdbPanel(); } catch {}
    try { cwBuildAnimeMappingPanel(); } catch {}
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
  try { cwAnimeMappingRefreshStatus(); } catch {}
}

function _cwSetText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = String(value ?? "");
}

function _cwSetChecked(id, value) {
  const el = document.getElementById(id);
  if (el) el.checked = !!value;
}

function _cwFormatUtc(value) {
  const raw = String(value || "").trim();
  if (!raw) return "-";
  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) return raw;
  return d.toISOString().replace("T", " ").slice(0, 16) + " UTC";
}

function _cwAnimeMappingUseForLabel() {
  const cfg = window._cfgCache || {};
  const block = cfg.anime_mapping || {};
  const raw = Array.isArray(block.use_for_pairs) ? block.use_for_pairs : ["anilist"];
  const vals = raw.map((x) => String(x || "").trim().toLowerCase()).filter(Boolean);
  if (!vals.length || vals.includes("anilist")) return "AniList pairs";
  return vals.map((x) => x.toUpperCase()).join(", ");
}

function _cwAnimeMappingSetBusy(on, label = "") {
  animeMappingBusy = !!on;
  ["anime_mapping_enabled", "anime_mapping_auto_update", "btn-anime-mapping-update", "btn-anime-mapping-rebuild"].forEach((id) => {
    const el = document.getElementById(id);
    if (el) el.disabled = !!on;
  });
  if (on) {
    _cwSetText("anime_mapping_dataset", label || "Updating");
  }
}

function cwAnimeMappingRenderStatus(st = {}) {
  const cfg = window._cfgCache || {};
  const block = cfg.anime_mapping || {};
  const err = String(st.error || st.message || "").trim();
  const installed = !!st.installed;
  const ready = !!st.index_ready;
  const enabled = st.enabled !== undefined ? !!st.enabled : !!block.enabled;
  const autoUpdate = st.auto_update !== undefined ? !!st.auto_update : block.auto_update !== false;
  const dataset = err ? "Error" : (animeMappingBusy ? "Updating" : (installed ? "Installed" : "Missing"));
  const index = ready ? "Ready" : "Missing";

  _cwSetChecked("anime_mapping_enabled", enabled);
  _cwSetChecked("anime_mapping_auto_update", autoUpdate);
  _cwSetText("anime_mapping_used_for", _cwAnimeMappingUseForLabel());
  _cwSetText("anime_mapping_auto_update_state", autoUpdate ? "Daily" : "Manual");
  _cwSetText("anime_mapping_dataset", dataset);
  _cwSetText("anime_mapping_generated", _cwFormatUtc(st.dataset_generated_on));
  _cwSetText("anime_mapping_index", index);
  _cwSetText("anime_mapping_counts", installed ? `${Number(st.source_count || 0).toLocaleString()} sources | ${Number(st.edge_count || 0).toLocaleString()} edges` : "-");
  _cwSetText("anime_mapping_error", err);

  const dot = document.getElementById("anime-mapping-dot");
  if (dot) {
    dot.classList.toggle("on", !!(enabled && installed && ready && !err));
    dot.classList.toggle("off", !!(!enabled || !installed || !ready || err));
  }
}

async function cwAnimeMappingRefreshStatus() {
  try {
    const r = await fetch("/api/anime-mapping/status", { cache: "no-store", credentials: "same-origin" });
    if (!r.ok) throw new Error(`GET /api/anime-mapping/status ${r.status}`);
    const st = await r.json();
    window.__animeMappingStatus = st || {};
    cwAnimeMappingRenderStatus(st || {});
    return st;
  } catch (e) {
    cwAnimeMappingRenderStatus({ error: e?.message || "Status failed" });
    return null;
  }
}

async function cwAnimeMappingSaveSettings() {
  const enabled = !!document.getElementById("anime_mapping_enabled")?.checked;
  const autoUpdate = !!document.getElementById("anime_mapping_auto_update")?.checked;
  const st0 = window.__animeMappingStatus || {};
  const needsBootstrap = enabled && !(st0.installed && st0.index_ready);
  _cwAnimeMappingSetBusy(true, needsBootstrap ? "Downloading" : "Saving");
  try {
    const r = await fetch("/api/anime-mapping/settings", {
      method: "POST",
      cache: "no-store",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        enabled,
        auto_update: autoUpdate,
        provider: "anibridge",
        use_for_pairs: ["anilist"],
      }),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok || data.ok === false) throw new Error(data.message || data.error || `Settings failed (${r.status})`);
    window._cfgCache ||= {};
    window._cfgCache.anime_mapping = data.anime_mapping || {
      ...(window._cfgCache.anime_mapping || {}),
      enabled,
      auto_update: autoUpdate,
      provider: "anibridge",
      use_for_pairs: ["anilist"],
    };
    if (data.status) window.__animeMappingStatus = data.status;
    cwAnimeMappingRenderStatus(data.status || window.__animeMappingStatus || {});
    if (data.bootstrap_error) throw new Error(data.bootstrap_error);
    try { window.CW?.DOM?.showToast?.(needsBootstrap ? "Anime mapping enabled and downloaded" : "Anime ID Mapping saved", true); } catch {}
  } catch (e) {
    cwAnimeMappingRenderStatus({ ...(window.__animeMappingStatus || {}), error: e?.message || "Save failed" });
    try { window.CW?.DOM?.showToast?.(e?.message || "Anime ID Mapping save failed", false); } catch {}
  } finally {
    _cwAnimeMappingSetBusy(false);
    try { await cwAnimeMappingRefreshStatus(); } catch {}
  }
}

async function cwAnimeMappingRun(action) {
  const update = action === "update";
  _cwAnimeMappingSetBusy(true, update ? "Updating" : "Rebuilding");
  try {
    const r = await fetch(update ? "/api/anime-mapping/update" : "/api/anime-mapping/rebuild-index", {
      method: "POST",
      cache: "no-store",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(update ? { force: false } : {}),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok || data.ok === false) throw new Error(data.message || data.error || `${update ? "Update" : "Rebuild"} failed (${r.status})`);
    cwAnimeMappingRenderStatus(data.status || data || {});
    try { window.CW?.DOM?.showToast?.(update ? "Anime mapping updated" : "Anime mapping index rebuilt", true); } catch {}
  } catch (e) {
    cwAnimeMappingRenderStatus({ ...(window.__animeMappingStatus || {}), error: e?.message || "Action failed" });
    try { window.CW?.DOM?.showToast?.(e?.message || "Anime mapping action failed", false); } catch {}
  } finally {
    _cwAnimeMappingSetBusy(false);
    try { await cwAnimeMappingRefreshStatus(); } catch {}
  }
}

function cwBuildAnimeMappingPanel() {
  const panelHost = document.getElementById("meta-provider-panel");
  if (!panelHost) return;
  if (panelHost.querySelector(`.cw-meta-provider-panel[data-provider="${ANIME_MAPPING_PROVIDER}"]`)) return;

  const wrap = document.createElement("div");
  wrap.className = "section cw-settings-section cw-settings-provider-section cw-meta-provider-panel active";
  wrap.id = "sec-meta-anime-mapping";
  wrap.dataset.provider = ANIME_MAPPING_PROVIDER;
  wrap.innerHTML = `
    <div class="head" data-toggle-section="sec-meta-anime-mapping">
      <span class="chev"></span>
      <div class="cw-meta-provider-head-copy">
        <strong>Anime ID Mapping</strong>
        <span class="cw-meta-provider-help" title="Only needed for anime-specific providers such as AniList." aria-label="Only needed for anime-specific providers such as AniList.">
          <span class="material-symbols-rounded cw-meta-provider-help-icon" aria-hidden="true">info</span>
        </span>
      </div>
      <span class="auth-dot" id="anime-mapping-dot" aria-hidden="true"></span>
    </div>
    <div class="body">
    <div class="cw-panel-head anime-mapping-head">
      <div class="cw-panel-head-main">
        <div class="cw-panel-title">Anime ID Mapping</div>
        <div class="muted">Local anime ID index for AniList watchlist and ratings pairs.</div>
      </div>
      <label class="cx-toggle anime-mapping-toggle">
        <input type="checkbox" id="anime_mapping_enabled">
        <span class="cx-toggle-ui" aria-hidden="true"></span>
        <span class="cx-toggle-text">Enable</span>
        <span class="cx-toggle-state" aria-hidden="true"></span>
      </label>
    </div>
    <div class="auth-card anime-mapping-card">
      <div class="anime-mapping-summary">
        <div>
          <div class="muted">Used for</div>
          <strong id="anime_mapping_used_for">AniList pairs</strong>
        </div>
        <div>
          <div class="muted">Auto-update</div>
          <label class="cx-toggle anime-mapping-inline-toggle">
            <input type="checkbox" id="anime_mapping_auto_update">
            <span class="cx-toggle-ui" aria-hidden="true"></span>
            <span class="cx-toggle-text" id="anime_mapping_auto_update_state">Daily</span>
            <span class="cx-toggle-state" aria-hidden="true"></span>
          </label>
        </div>
      </div>
      <div class="anime-mapping-status-grid">
        <div class="anime-mapping-status">
          <span>Dataset</span>
          <strong id="anime_mapping_dataset">-</strong>
        </div>
        <div class="anime-mapping-status">
          <span>Index</span>
          <strong id="anime_mapping_index">-</strong>
        </div>
        <div class="anime-mapping-status">
          <span>Generated</span>
          <strong class="mono" id="anime_mapping_generated">-</strong>
        </div>
        <div class="anime-mapping-status">
          <span>Size</span>
          <strong id="anime_mapping_counts">-</strong>
        </div>
      </div>
      <div class="anime-mapping-source">
        Dataset source: <a href="https://github.com/anibridge/anibridge-mappings" target="_blank" rel="noopener noreferrer">anibridge/anibridge-mappings</a>. CrossWatch downloads the AniBridge mappings dataset to translate media identifiers between AniList and TMDB, TVDB, IMDb, MyAnimeList, and AniDB.
      </div>
      <div class="auth-card-notes" id="anime_mapping_error"></div>
      <div class="cw-settings-inline-action anime-mapping-actions">
        <button class="btn primary" type="button" id="btn-anime-mapping-update">Update now</button>
        <button class="btn" type="button" id="btn-anime-mapping-rebuild">Rebuild index</button>
      </div>
    </div>
    </div>
  `;

  panelHost.appendChild(wrap);

  const enabled = document.getElementById("anime_mapping_enabled");
  const autoUpdate = document.getElementById("anime_mapping_auto_update");
  const btnUpdate = document.getElementById("btn-anime-mapping-update");
  const btnRebuild = document.getElementById("btn-anime-mapping-rebuild");
  if (enabled && !enabled.__cwAnimeWired) {
    enabled.addEventListener("change", () => cwAnimeMappingSaveSettings());
    enabled.__cwAnimeWired = true;
  }
  if (autoUpdate && !autoUpdate.__cwAnimeWired) {
    autoUpdate.addEventListener("change", () => cwAnimeMappingSaveSettings());
    autoUpdate.__cwAnimeWired = true;
  }
  if (btnUpdate && !btnUpdate.__cwAnimeWired) {
    btnUpdate.addEventListener("click", () => cwAnimeMappingRun("update"));
    btnUpdate.__cwAnimeWired = true;
  }
  if (btnRebuild && !btnRebuild.__cwAnimeWired) {
    btnRebuild.addEventListener("click", () => cwAnimeMappingRun("rebuild"));
    btnRebuild.__cwAnimeWired = true;
  }

  try { cwAnimeMappingRenderStatus(window.__animeMappingStatus || {}); } catch {}
}

function cwBuildTmdbPanel() {
  const panelHost = document.getElementById("meta-provider-panel");
  if (!panelHost) return;

  if (panelHost.querySelector('.cw-meta-provider-panel[data-provider="tmdb"]')) return;

  const wrap = document.createElement("div");
  wrap.className = "section cw-settings-section cw-settings-provider-section cw-meta-provider-panel active";
  wrap.id = "sec-meta-tmdb";
  wrap.dataset.provider = "tmdb";

  const sectionHead = document.createElement("div");
  sectionHead.className = "head";
  sectionHead.dataset.toggleSection = "sec-meta-tmdb";
  sectionHead.innerHTML = `
    <span class="chev"></span>
    <div class="cw-meta-provider-head-copy">
      <strong>TMDb</strong>
      <span class="cw-meta-provider-help" title="Highly recommended for matching, metadata and images." aria-label="Highly recommended for matching, metadata and images.">
        <span class="material-symbols-rounded cw-meta-provider-help-icon" aria-hidden="true">info</span>
      </span>
    </div>
    <span class="auth-dot" id="meta-tmdb-dot" aria-hidden="true"></span>
  `;

  const sectionBody = document.createElement("div");
  sectionBody.className = "body";

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

  const checkRow = document.createElement("div");
  checkRow.className = "inline";
  checkRow.style.display = "flex";
  checkRow.style.marginTop = "10px";
  checkRow.style.gap = "10px";
  checkRow.style.alignItems = "center";
  checkRow.style.justifyContent = "flex-start";
  checkRow.innerHTML = `
    <button type="button" class="btn secondary" id="tmdb_check">Check</button>
    <button type="button" class="btn danger" id="tmdb_delete">Delete</button>
    <div id="tmdb_check_msg" class="msg ok hidden" aria-live="polite" style="margin-left:auto;width:auto;max-width:min(520px,60%);flex:0 1 auto;white-space:normal"></div>
  `;
  checkRow.querySelector("#tmdb_check")?.addEventListener("click", () => {
    try { cwVerifyTmdbKey(); } catch {}
  });
  checkRow.querySelector("#tmdb_delete")?.addEventListener("click", () => {
    try { cwDeleteTmdbKey(); } catch {}
  });
  keyInput.addEventListener("input", () => {
    keyInput.dataset.verified = "";
    const msg = document.getElementById("tmdb_check_msg");
    if (msg) {
      msg.textContent = "";
      msg.classList.add("hidden");
    }
    try { cwMetaSettingsHubUpdate(); } catch {}
  });

  apiFields.appendChild(apiField);
  apiFields.appendChild(checkRow);
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

  sectionBody.appendChild(head);
  sectionBody.appendChild(subTiles);
  sectionBody.appendChild(subPanels);
  wrap.appendChild(sectionHead);
  wrap.appendChild(sectionBody);

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
  try { cwAnimeMappingRefreshStatus(); } catch {}
}

function cwMetaSettingsHubUpdate() {
  const chip = document.getElementById("hub_tmdb_key");

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
  const verifyState = keyEl?.dataset?.verified || "";
  const verified = hasKeyNow && verifyState === "1";
  const failed = hasKeyNow && verifyState === "0";
  if (chip) chip.textContent = `API key: ${verified ? "verified" : failed ? "check failed" : hasKeyNow ? "set" : "missing"}`;

  const dot = document.getElementById("meta-tmdb-dot");
  if (dot) {
    dot.classList.toggle("on", hasKeyNow && !failed);
    dot.title = verified ? "Verified" : failed ? "TMDb key check failed" : hasKeyNow ? "Configured; click Check to validate" : "Not configured";
    dot.setAttribute("aria-label", dot.title);
    dot.closest?.('.cw-meta-provider-panel[data-provider="tmdb"]')?.classList?.toggle("is-configured", hasKeyNow && !failed);
  }

  try { window.syncMetadataProviderDot?.(); } catch {}
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
  _setVal("metadata_ttl_hours", String(Number.isFinite(cfg.metadata?.ttl_hours) ? cfg.metadata.ttl_hours : 720));

  
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
      const on = (typeof ui.show_recent_activity === "boolean")
        ? !!ui.show_recent_activity
        : true;
      _setSelectValue("ui_show_recent_activity", on ? "true" : "false");
    }

    {
      const on = (typeof ui.show_recent_history_widget === "boolean")
        ? !!ui.show_recent_history_widget
        : true;
      _setSelectValue("ui_show_recent_history_widget", on ? "true" : "false");
    }

    {
      const on = (typeof ui.show_latest_ratings_widget === "boolean")
        ? !!ui.show_latest_ratings_widget
        : true;
      _setSelectValue("ui_show_latest_ratings_widget", on ? "true" : "false");
    }

    {
      const on = (typeof ui.show_recent_scrobble_widget === "boolean")
        ? !!ui.show_recent_scrobble_widget
        : true;
      _setSelectValue("ui_show_recent_scrobble_widget", on ? "true" : "false");
    }

    const normalizeDisplay = (value, fallbackLimit) => {
      const raw = String(value || "").trim().toLowerCase();
      const allowed = new Set(["count:3", "count:4", "count:5", "hours:24", "hours:48", "hours:72"]);
      if (allowed.has(raw)) return raw;
      const limit = Math.max(3, Math.min(5, Number.isFinite(fallbackLimit) ? Number(fallbackLimit) : 3));
      return `count:${limit}`;
    };

    _setSelectValue("ui_recent_activity_display", normalizeDisplay(ui.recent_activity_display, Number(ui.recent_activity_limit)));
    _setSelectValue("ui_recent_syncs_display", normalizeDisplay(ui.recent_syncs_display, Number(ui.recent_syncs_limit)));

    {
      const theme = String(ui.theme || "flat-dark").trim().toLowerCase();
      let storedTheme = "";
      try {
        const raw = localStorage.getItem("cw.ui.theme");
        if (raw === "flat-light" || raw === "flat-dark" || raw === "original") storedTheme = raw;
      } catch {}
      const normalizedTheme = storedTheme || ((theme === "flat-light" || theme === "original") ? theme : "flat-dark");
      _setSelectValue("ui_theme", normalizedTheme);
      try { window.CWTheme?.apply?.(normalizedTheme, { persist: true }); } catch {}
    }

    {
      const on = (typeof ui.show_AI === "boolean")
        ? !!ui.show_AI
        : true;
      _setSelectValue("ui_show_AI", on ? "true" : "false");
    }

    {
      const on = (typeof ui.show_quick_add_desktop === "boolean")
        ? !!ui.show_quick_add_desktop
        : true;
      _setSelectValue("ui_show_quick_add_desktop", on ? "true" : "false");
    }

    {
      const on = (typeof ui.show_quick_add_mobile === "boolean")
        ? !!ui.show_quick_add_mobile
        : true;
      _setSelectValue("ui_show_quick_add_mobile", on ? "true" : "false");
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

  
  setRaw("plex_home_pin", val(cfg.plex?.home_pin));

  
  setRaw("simkl_client_id",     val(cfg.simkl?.client_id));
  setRaw("simkl_client_secret", val(cfg.simkl?.client_secret));

  
  setRaw("anilist_client_id",     val(cfg.anilist?.client_id));
  setRaw("anilist_client_secret", val(cfg.anilist?.client_secret));

  
  setRaw("tmdb_api_key",        val(cfg.tmdb?.api_key));

  
  setRaw("mdblist_key",         val(cfg.mdblist?.api_key));
  setRaw("publicmetadb_key",    val(cfg.publicmetadb?.api_key));

  
  setRaw("trakt_client_id",     val(cfg.trakt?.client_id));
  setRaw("trakt_client_secret", val(cfg.trakt?.client_secret));
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
    cwRenderOtherSessions(st);
  } catch {}

  try { await cwMobileDevicesRefresh(); } catch {}
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

function cwSessionBrowserLabel(ua) {
  const text = String(ua || "").trim();
  if (!text) return "";
  if (/edg\//i.test(text)) return "Microsoft Edge";
  if (/chrome\//i.test(text) && !/edg\//i.test(text) && !/opr\//i.test(text)) return "Google Chrome";
  if (/firefox\//i.test(text)) return "Mozilla Firefox";
  if (/safari\//i.test(text) && !/chrome\//i.test(text) && !/chromium\//i.test(text)) return "Safari";
  if (/opr\//i.test(text) || /opera/i.test(text)) return "Opera";
  return "";
}

function cwSessionAgentLabel(ua) {
  const browser = cwSessionBrowserLabel(ua);
  if (browser) return browser;
  const text = String(ua || "").trim();
  if (!text) return "Unknown browser";
  return text.length > 72 ? `${text.slice(0, 69)}...` : text;
}

function cwRenderOtherSessions(st) {
  const stateEl = document.getElementById("app_auth_other_sessions_state");
  const detailEl = document.getElementById("app_auth_other_sessions_detail");
  const btn = document.getElementById("btn-auth-logout-others");
  const sessions = Array.isArray(st?.other_sessions) ? st.other_sessions : [];
  const count = Number.isFinite(st?.other_session_count) ? Number(st.other_session_count) : sessions.length;

  if (stateEl) {
    const label = count === 1 ? "browser session" : "browser sessions";
    stateEl.textContent = `Logged in from: ${count} ${label}`;
  }

  if (detailEl) {
    if (!count) {
      detailEl.textContent = "";
    } else {
      const grouped = new Map();
      for (const session of sessions) {
        const browser = cwSessionAgentLabel(session?.ua);
        const ip = String(session?.ip || "").trim();
        const key = `${browser}|||${ip}`;
        grouped.set(key, (grouped.get(key) || 0) + 1);
      }
      const details = Array.from(grouped.entries())
        .slice(0, 3)
        .map(([key, n]) => {
          const [browser, ip] = key.split("|||");
          const label = ip ? `${browser} on ${ip}` : browser;
          return n > 1 ? `${label} (${n})` : label;
        });
      const remaining = grouped.size - details.length;
      if (remaining > 0) details.push(`+${remaining} more`);
      detailEl.textContent = details.join(" | ");
    }
  }

  if (btn) btn.disabled = !(st && st.authenticated && count > 0);
}

function cwMobileDateLabel(seconds) {
  const n = Number(seconds || 0);
  if (!Number.isFinite(n) || n <= 0) return "";
  try {
    return new Date(n * 1000).toLocaleString(undefined, {
      year: "numeric",
      month: "short",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    });
  } catch {
    return "";
  }
}

async function cwMobileJson(url, options = {}) {
  const res = await fetch(url, {
    cache: "no-store",
    credentials: "same-origin",
    ...options,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data?.ok === false) {
    throw new Error(String(data?.detail || data?.error || `HTTP ${res.status}`));
  }
  return data;
}

function cwMobileSetStatus(text, warn = false) {
  const el = document.getElementById("mobile_auth_state");
  if (!el) return;
  el.textContent = String(text || "");
  el.classList.toggle("warn", !!warn);
}

function cwMobileRenderDevices(devices) {
  const list = document.getElementById("mobile_devices_list");
  const count = Array.isArray(devices) ? devices.length : 0;
  cwMobileSetStatus(`${count} paired ${count === 1 ? "device" : "devices"}`);
  if (!list) return;
  list.textContent = "";

  if (!count) {
    const empty = document.createElement("div");
    empty.className = "sub";
    empty.textContent = "No paired companion devices.";
    list.appendChild(empty);
    return;
  }

  for (const device of devices) {
    const id = String(device?.id || "");
    const name = String(device?.name || "Android device");
    const scopes = Array.isArray(device?.scopes) ? device.scopes.join(", ") : "read";
    const seen = cwMobileDateLabel(device?.last_seen_at);
    const row = document.createElement("div");
    row.className = "cw-mobile-device-row";
    row.dataset.mobileDeviceId = id;

    const meta = document.createElement("div");
    meta.className = "cw-mobile-device-meta";
    const title = document.createElement("strong");
    title.textContent = name;
    const detail = document.createElement("div");
    detail.className = "sub";
    detail.textContent = `${scopes || "read"}${seen ? ` | seen ${seen}` : ""}`;
    meta.append(title, detail);

    const revoke = document.createElement("button");
    revoke.className = "btn";
    revoke.type = "button";
    revoke.textContent = "Revoke";
    revoke.disabled = !id;
    revoke.addEventListener("click", () => cwMobileRevokeDevice(id, name));

    row.append(meta, revoke);
    list.appendChild(row);
  }
}

async function cwMobileDevicesRefresh() {
  try {
    const data = await cwMobileJson("/api/mobile/devices");
    const devices = Array.isArray(data?.devices) ? data.devices : [];
    cwMobileRenderDevices(devices);
    return devices;
  } catch (err) {
    cwMobileSetStatus(String(err?.message || err || "Could not load companion devices."), true);
    return [];
  }
}

function cwMobilePairingPollUntil(expiresAt) {
  try { clearInterval(window.__cwMobilePairingPoll); } catch {}
  const stopAt = Number(expiresAt || 0);
  if (!Number.isFinite(stopAt) || stopAt <= 0) return;
  window.__cwMobilePairingPoll = setInterval(async () => {
    if (Date.now() / 1000 > stopAt) {
      clearInterval(window.__cwMobilePairingPoll);
      return;
    }
    try { await cwMobileDevicesRefresh(); } catch {}
  }, 5000);
}

async function cwMobilePairingStart() {
  const btn = document.getElementById("btn-mobile-pairing-start");
  const box = document.getElementById("mobile_pairing_box");
  const qr = document.getElementById("mobile_pairing_qr");
  const code = document.getElementById("mobile_pairing_code");
  const uri = document.getElementById("mobile_pairing_uri");
  const expiry = document.getElementById("mobile_pairing_expiry");
  const previous = btn?.textContent || "Add device";
  try {
    if (btn) {
      btn.disabled = true;
      btn.textContent = "Creating...";
    }
    const data = await cwMobileJson("/api/mobile/pairing/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        device_name: "CrossWatch companion",
        server_url: window.location?.origin || "",
        scopes: ["read", "actions", "diagnostics", "safe-config"],
      }),
    });
    if (box) box.classList.remove("hidden");
    if (code) code.textContent = String(data?.code || "");
    if (uri) uri.value = String(data?.pairing_uri || "");
    if (expiry) {
      const until = cwMobileDateLabel(data?.expires_at);
      expiry.textContent = until ? `Expires ${until}` : "";
    }
    if (qr) {
      qr.textContent = "";
      const img = document.createElement("img");
      img.alt = "Companion pairing QR code";
      img.addEventListener("error", () => {
        qr.textContent = "";
        const msg = document.createElement("div");
        msg.className = "sub";
        msg.style.color = "#1f2937";
        msg.style.textAlign = "center";
        msg.textContent = "QR generator missing. Install requirements and restart CrossWatch.";
        qr.appendChild(msg);
        cwMobileSetStatus("QR image could not be loaded. Use the code or URI for now.", true);
      });
      img.src = `/api/mobile/pairing/${encodeURIComponent(String(data?.id || ""))}/qr.svg?t=${Date.now()}`;
      qr.appendChild(img);
    }
    cwMobileSetStatus("Pairing code ready");
    cwMobilePairingPollUntil(data?.expires_at);
    try { await cwMobileDevicesRefresh(); } catch {}
  } catch (err) {
    cwMobileSetStatus(String(err?.message || err || "Could not create pairing code."), true);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = previous;
    }
  }
}

async function cwMobileRevokeDevice(id, name = "") {
  const deviceId = String(id || "");
  if (!deviceId) return false;
  if (!confirm(`Revoke ${name || "this companion device"}?`)) return false;
  const row = Array.from(document.querySelectorAll(".cw-mobile-device-row"))
    .find((el) => String(el?.dataset?.mobileDeviceId || "") === deviceId);
  const btn = row?.querySelector("button");
  try {
    if (btn) btn.disabled = true;
    await cwMobileJson(`/api/mobile/devices/${encodeURIComponent(deviceId)}`, { method: "DELETE" });
    await cwMobileDevicesRefresh();
    return true;
  } catch (err) {
    cwMobileSetStatus(String(err?.message || err || "Could not revoke companion device."), true);
    if (btn) btn.disabled = false;
    return false;
  }
}

async function cwRefreshAppAuthStatus() {
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
    if (!st) el.textContent = "-";
    else if (!st.configured || st.reset_required) el.textContent = "Auth: set password";
    else if (st.authenticated) {
      const exp = (st.session_expires_at && st.session_expires_at > 0) ? new Date(st.session_expires_at * 1000) : null;
      el.textContent = !rememberEnabled
        ? "Auth: signed in (browser session)"
        : (exp ? `Auth: signed in (until ${exp.toISOString().replace("T", " ").slice(0, 16)}Z)` : "Auth: signed in");
    } else el.textContent = "Auth: locked";
  }

  const btn = document.getElementById("btn-auth-logout");
  if (btn) btn.disabled = !(st && st.authenticated);
  cwRenderOtherSessions(st);
  try { await cwMobileDevicesRefresh(); } catch {}
}

window.cwAppLogout = async function cwAppLogout() {
  try {
    await fetch("/api/app-auth/logout", { method: "POST", cache: "no-store", credentials: "same-origin" });
  } catch {}
  location.href = "/login";
};

window.cwAppLogoutOthers = async function cwAppLogoutOthers() {
  try {
    await fetch("/api/app-auth/logout-others", { method: "POST", cache: "no-store", credentials: "same-origin" });
  } catch {}
  try { await cwRefreshAppAuthStatus(); } catch {}
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

function cwReadTmdbKeyForVerify() {
  const input = document.getElementById("tmdb_api_key");
  const value = String(input?.value || "").trim();
  const masked = !!value && (input?.dataset?.masked === "1" || value === "********" || /^[*•]+$/.test(value));
  if (masked) return { has: true, value: "********" };
  return { has: !!value, value };
}

function cwSetTmdbCheckMessage(ok, text) {
  const msg = document.getElementById("tmdb_check_msg");
  if (!msg) return;
  msg.textContent = String(text || "");
  msg.classList.toggle("hidden", !msg.textContent);
  msg.classList.toggle("warn", !!msg.textContent && !ok);
  msg.classList.toggle("ok", !!msg.textContent && !!ok);
}

async function cwVerifyTmdbKey() {
  const input = document.getElementById("tmdb_api_key");
  const btn = document.getElementById("tmdb_check");
  const state = cwReadTmdbKeyForVerify();
  if (!state.has) {
    if (input) input.dataset.verified = "0";
    cwSetTmdbCheckMessage(false, "Missing API key");
    try { cwMetaSettingsHubUpdate(); } catch {}
    return false;
  }
  try {
    if (btn) btn.disabled = true;
    cwSetTmdbCheckMessage(true, "Checking...");
    const r = await fetch("/api/tmdb/verify", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      cache: "no-store",
      body: JSON.stringify({ api_key: state.value }),
    });
    const data = await r.json().catch(() => ({}));
    const ok = !!(r.ok && data && data.valid !== false && data.ok !== false);
    if (input) input.dataset.verified = ok ? "1" : "0";
    cwSetTmdbCheckMessage(ok, ok ? "Connected" : (data?.error || "TMDb key check failed."));
    try { cwMetaSettingsHubUpdate(); } catch {}
    return ok;
  } catch {
    if (input) input.dataset.verified = "0";
    cwSetTmdbCheckMessage(false, "TMDb key check failed.");
    try { cwMetaSettingsHubUpdate(); } catch {}
    return false;
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function cwDeleteTmdbKey() {
  const input = document.getElementById("tmdb_api_key");
  const btn = document.getElementById("tmdb_delete");
  const checkBtn = document.getElementById("tmdb_check");
  try {
    if (btn) btn.disabled = true;
    if (checkBtn) checkBtn.disabled = true;
    cwSetTmdbCheckMessage(true, "Deleting...");
    const r = await fetch("/api/tmdb/disconnect", {
      method: "POST",
      cache: "no-store",
      credentials: "same-origin",
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok || data?.ok === false) throw new Error(data?.error || "disconnect_failed");
    if (input) {
      input.value = "";
      input.dataset.masked = "0";
      input.dataset.loaded = "1";
      input.dataset.touched = "";
      input.dataset.clear = "";
      input.dataset.verified = "0";
    }
    try {
      const cfg = window._cfgCache;
      if (cfg?.tmdb && typeof cfg.tmdb === "object") cfg.tmdb.api_key = "";
    } catch {}
    try { window.CW?.Cache?.invalidate?.("config"); } catch {}
    try { window.invalidateConfigCache?.(); } catch {}
    try { window.manualRefreshStatus?.(); } catch {}
    try { updateTmdbHint(); } catch {}
    try { cwMetaSettingsHubUpdate(); } catch {}
    cwSetTmdbCheckMessage(true, "Deleted");
    return true;
  } catch {
    cwSetTmdbCheckMessage(false, "TMDb key delete failed.");
    return false;
  } finally {
    if (btn) btn.disabled = false;
    if (checkBtn) checkBtn.disabled = false;
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
    cwBuildAnimeMappingPanel,
    cwAnimeMappingRenderStatus,
    cwAnimeMappingRefreshStatus,
    cwAnimeMappingSaveSettings,
    cwAnimeMappingRun,
    cwBuildTmdbPanel,
    cwDeleteTmdbKey,
    cwMetaSettingsSelect,
    cwMetaSettingsHubUpdate,
    cwMetaSettingsHubInit,
    cwMetaSettingsHubEnsure,
    cwMobileDevicesRefresh,
    cwMobilePairingStart,
    cwMobileRevokeDevice,
    loadConfig,
    updateTmdbHint,
    setTraktSuccess,
  };

  (window.CW ||= {}).SettingsUI = SettingsUI;
  Object.assign(window, SettingsUI);
})();
