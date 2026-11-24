/* assets/helpers/settings-ui.js */
/* Extracted settings hubs/config hydration from core.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function(){
const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
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

/*! Settings */


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
  document.querySelectorAll(".cw-app-hero [data-target]").forEach((btn) => {
    const on = String(btn.dataset.target || "").toLowerCase() === t;
    btn.classList.toggle("active", on);
    btn.setAttribute("aria-current", on ? "page" : "false");
  });
  document.querySelectorAll(".cw-app-hero-panel").forEach((hero) => hero.classList.toggle("active", String(hero.dataset.appHero || "").toLowerCase() === t));
  document.querySelectorAll(".cw-app-hero-shape").forEach((shape) => shape.classList.toggle("active", String(shape.dataset.appHeroShape || "").toLowerCase() === t));

  if (persist) {
    try { localStorage.setItem(UI_SETTINGS_TAB_KEY, t); } catch {}
  }

  try { cwUiSettingsHubUpdate(); } catch {}
}

function cwUiSettingsHubUpdate() {
  const aaRememberEnabled = (document.getElementById("app_auth_remember_enabled")?.value || "").toString() === "true";

  const authFields = document.getElementById("app_auth_fields");
  if (authFields) authFields.classList.remove("cw-disabled");
  const authSessionFields = document.getElementById("app_auth_session_fields");
  if (authSessionFields) authSessionFields.classList.remove("cw-disabled");
  const rememberDaysWrap = document.getElementById("app_auth_remember_days_wrap");
  if (rememberDaysWrap) rememberDaysWrap.classList.toggle("cw-disabled", !aaRememberEnabled);
  const rememberDays = document.getElementById("app_auth_remember_days");
  if (rememberDays) rememberDays.disabled = !aaRememberEnabled;
  try { cwValidateAppAuthRememberDays(); } catch {}
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
  const message = valid ? "" : T("settings.security.sessions.daysInvalid");

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
    "ui_show_recent_progress_widget",
    "ui_show_recent_playlists_widget",
    "ui_recent_activity_display",
    "ui_recent_syncs_display",
    "ui_show_quick_add_desktop",
    "ui_show_quick_add_mobile",
    "ui_theme",
    "ui_media_card",
    "ui_protocol",
    "app_auth_username",
    "app_auth_password",
    "app_auth_password2",
    "app_auth_remember_enabled",
    "app_auth_remember_days",
    "app_auth_totp_code",
    "trusted_proxies"
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
    if (["ui","security"].includes(saved)) tab = saved;
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
      if (!st || !st.linked) label.textContent = T("settings.security.plex.notLinked");
      else {
        const who = [st.linked_username, st.linked_email].filter(Boolean).join(" - ");
        label.textContent = who || T("settings.security.linked");
      }
    }
    const unlinkBtn = document.getElementById("btn-app-auth-plex-unlink");
    if (unlinkBtn) unlinkBtn.disabled = !(st && st.linked);
    return st;
  } catch {
    const label = document.getElementById("app_auth_plex_state");
    if (label) label.textContent = T("settings.security.unavailable");
    return null;
  }
}

window.cwAppAuthPlexLink = async function cwAppAuthPlexLink() {
  const btn = document.getElementById("btn-app-auth-plex-link");
  const original = btn?.textContent || T("settings.security.plex.link");
  const popup = window.open("about:blank", "cw_plex_link", "width=620,height=760,popup=yes");
  try {
    if (btn) {
      btn.disabled = true;
      btn.textContent = T("settings.security.plex.waiting");
    }
    const r = await fetch("/api/app-auth/plex/link/start", {
      method: "POST",
      cache: "no-store",
      credentials: "same-origin",
    });
    const data = await r.json().catch(() => null);
    if (!r.ok || !data?.ok || !data?.state || !data?.auth_url) {
      if (popup && !popup.closed) popup.close();
      throw new Error(data?.error || T("settings.security.plex.linkFailedStatus", { status: String(r.status) }));
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
      if (!pr.ok || !pd?.ok) throw new Error(pd?.error || T("settings.security.plex.linkFailedStatus", { status: String(pr.status) }));
      if (popup && !popup.closed) popup.close();
      await cwAppAuthPlexRefreshStatus();
      try { window._appAuthStatus && (window._appAuthStatus.plex_sso_enabled = true); } catch {}
      try { _cwShowToast?.(T("settings.security.plex.linked"), true); } catch {}
      return;
    }
  } catch (e) {
    if (popup && !popup.closed) popup.close();
    try { _cwShowToast?.(String(e?.message || e || T("settings.security.plex.linkFailed")), false); } catch {}
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = original;
    }
  }
};

window.cwAppAuthPlexUnlink = async function cwAppAuthPlexUnlink() {
  const ok = (await (window.CW?.MobileUI?.ask || window.confirm)(T("settings.security.plex.unlinkConfirm")));
  if (!ok) return;
  try {
    const r = await fetch("/api/app-auth/plex/unlink", {
      method: "POST",
      cache: "no-store",
      credentials: "same-origin",
    });
    const data = await r.json().catch(() => null);
    if (!r.ok || !data?.ok) throw new Error(data?.error || T("settings.security.plex.unlinkFailedStatus", { status: String(r.status) }));
    await cwAppAuthPlexRefreshStatus();
    try { _cwShowToast?.(T("settings.security.plex.unlinked"), true); } catch {}
  } catch (e) {
    try { _cwShowToast?.(String(e?.message || e || T("settings.security.plex.unlinkFailed")), false); } catch {}
  }
};

function cwSetSelectValue(id, value) {
  const el = document.getElementById(id);
  if (!el) return;
  el.value = String(value);
  try { el.dispatchEvent(new Event("change")); } catch {}
  try { window.CW?.IconSelect?.enhance?.(el, el.__cwIconSelectCfg || { className: "cw-plain-select" }); } catch {}
}

function cwSetInputValue(id, value) {
  const el = document.getElementById(id);
  if (el) el.value = String(value || "");
}

function cwAppAuthOidcProvisionFields() {
  const fields = document.getElementById("app_auth_oidc_provision_fields");
  if (fields) fields.hidden = document.getElementById("app_auth_oidc_provision")?.value !== "true";
}

document.addEventListener("change", (event) => {
  if (event.target?.id === "app_auth_oidc_provision") cwAppAuthOidcProvisionFields();
}, true);

async function cwAppAuthOidcRefreshStatus() {
  let cfg = null;
  try {
    const cr = await fetch("/api/app-auth/oidc/config", { cache: "no-store", credentials: "same-origin" });
    cfg = cr.ok ? await cr.json() : null;
    if (cfg?.ok) {
      cwSetSelectValue("app_auth_oidc_enabled", cfg.enabled ? "true" : "false");
      cwSetInputValue("app_auth_oidc_issuer", cfg.issuer || "");
      cwSetInputValue("app_auth_oidc_client_id", cfg.client_id || "");
      cwSetInputValue("app_auth_oidc_client_secret", "");
      cwSetInputValue("app_auth_oidc_scopes", cfg.scopes || "openid profile email");
      cwSetInputValue("app_auth_oidc_button_label", cfg.button_label && cfg.button_label !== "OIDC" ? cfg.button_label : "");
      cwSetSelectValue("app_auth_oidc_button_icon", cfg.button_icon || "");
      cwSetSelectValue("app_auth_oidc_provision", cfg.provision ? "true" : "false");
      cwSetSelectValue("app_auth_oidc_provision_access", cfg.provision_access === "readonly" ? "readonly" : "full");
      cwSetInputValue("app_auth_oidc_allowed_groups", (Array.isArray(cfg.allowed_groups) ? cfg.allowed_groups : []).join(", "));
      cwSetInputValue("app_auth_oidc_groups_claim", cfg.groups_claim && cfg.groups_claim !== "groups" ? cfg.groups_claim : "");
      cwSetSelectValue("app_auth_oidc_match_identities", cfg.match_identities ? "true" : "false");
      cwSetInputValue("app_auth_oidc_identity_claim", cfg.identity_claim || "");
      const secret = document.getElementById("app_auth_oidc_client_secret");
      if (secret) secret.placeholder = cfg.client_secret_configured ? T("settings.security.password.keepPlaceholder") : T("settings.security.oidc.clientSecret");
    }
  } catch {}
  try {
    const r = await fetch("/api/app-auth/oidc/status", { cache: "no-store", credentials: "same-origin" });
    const st = r.ok ? await r.json() : null;
    const label = document.getElementById("app_auth_oidc_state");
    if (label) {
      if (!cfg?.configured && !st?.configured) label.textContent = T("settings.security.oidc.notConfigured");
      else if (!st?.linked) label.textContent = T("settings.security.oidc.configuredNotLinked");
      else {
        const who = [st.linked_username, st.linked_email].filter(Boolean).join(" - ");
        label.textContent = who || T("settings.security.linked");
      }
    }
    const linkBtn = document.getElementById("btn-app-auth-oidc-link");
    const unlinkBtn = document.getElementById("btn-app-auth-oidc-unlink");
    if (linkBtn) linkBtn.disabled = !(cfg?.configured || st?.configured);
    if (unlinkBtn) unlinkBtn.disabled = !(st && st.linked);
    return st;
  } catch {
    const label = document.getElementById("app_auth_oidc_state");
    if (label) label.textContent = T("settings.security.unavailable");
    return null;
  }
}

function cwAppAuthOidcState(raw) {
  const scopes = [];
  String(raw.scopes || "").split(/\s+/).filter(Boolean).forEach((s) => { if (!scopes.includes(s)) scopes.push(s); });
  if (!scopes.length) scopes.push("openid", "profile", "email");
  if (!scopes.includes("openid")) scopes.unshift("openid");
  const groups = [];
  (Array.isArray(raw.allowed_groups) ? raw.allowed_groups : String(raw.allowed_groups || "").split(/[,\n]/))
    .map((g) => String(g || "").trim()).filter(Boolean).forEach((g) => { if (!groups.includes(g)) groups.push(g); });
  const icon = String(raw.button_icon || "").trim().toLowerCase();
  return {
    enabled: !!raw.enabled,
    issuer: String(raw.issuer || "").trim().replace(/\/+$/, ""),
    client_id: String(raw.client_id || "").trim(),
    scopes: scopes.join(" "),
    button_label: String(raw.button_label || "").split(/\s+/).filter(Boolean).join(" ").slice(0, 40) || "OIDC",
    button_icon: icon,
    provision: !!raw.provision,
    provision_access: raw.provision_access === "readonly" ? "readonly" : "full",
    allowed_groups: groups.slice(0, 50),
    groups_claim: String(raw.groups_claim || "").trim().slice(0, 64) || "groups",
    match_identities: !!raw.match_identities,
    identity_claim: String(raw.identity_claim || "").trim().slice(0, 64),
  };
}

function cwAppAuthOidcFormState() {
  const val = (id) => document.getElementById(id)?.value || "";
  return cwAppAuthOidcState({
    enabled: val("app_auth_oidc_enabled") === "true",
    issuer: val("app_auth_oidc_issuer"),
    client_id: val("app_auth_oidc_client_id"),
    scopes: val("app_auth_oidc_scopes"),
    button_label: val("app_auth_oidc_button_label"),
    button_icon: val("app_auth_oidc_button_icon"),
    provision: val("app_auth_oidc_provision") === "true",
    provision_access: val("app_auth_oidc_provision_access"),
    allowed_groups: val("app_auth_oidc_allowed_groups"),
    groups_claim: val("app_auth_oidc_groups_claim"),
    match_identities: val("app_auth_oidc_match_identities") === "true",
    identity_claim: val("app_auth_oidc_identity_claim"),
  });
}

window.cwAppAuthOidcDraft = function cwAppAuthOidcDraft() {
  if (!document.getElementById("app_auth_oidc_enabled")) return null;
  return { form: cwAppAuthOidcFormState(), secret: String(document.getElementById("app_auth_oidc_client_secret")?.value || "").trim() };
};

window.cwAppAuthOidcSavePending = async function cwAppAuthOidcSavePending(draft) {
  const pending = draft === undefined ? window.cwAppAuthOidcDraft() : draft;
  if (!pending) return { ok: true, changed: false };
  let cr;
  try {
    cr = await fetch("/api/app-auth/oidc/config", { cache: "no-store", credentials: "same-origin" });
  } catch {
    return { ok: false, error: T("settings.security.oidc.readFailed") };
  }
  const current = cr.ok ? await cr.json().catch(() => null) : null;
  if (!current?.ok) return { ok: true, changed: false };
  const { form, secret } = pending;
  if (!secret && JSON.stringify(form) === JSON.stringify(cwAppAuthOidcState(current))) return { ok: true, changed: false };
  let r;
  try {
    r = await fetch("/api/app-auth/oidc/config", {
      method: "POST",
      cache: "no-store",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...form, client_secret: secret, keep_client_secret: !secret }),
    });
  } catch {
    return { ok: false, error: T("settings.security.oidc.saveFailedSettings") };
  }
  const data = await r.json().catch(() => null);
  if (!r.ok || !data?.ok) return { ok: false, error: data?.error || T("settings.security.oidc.saveFailedStatus", { status: String(r.status) }) };
  const secretEl = document.getElementById("app_auth_oidc_client_secret");
  if (secretEl) secretEl.value = "";
  await cwAppAuthOidcRefreshStatus();
  return { ok: true, changed: true };
};

window.cwAppAuthOidcLink = async function cwAppAuthOidcLink() {
  const btn = document.getElementById("btn-app-auth-oidc-link");
  const original = btn?.textContent || T("settings.security.oidc.link");
  const popup = window.open("about:blank", "cw_oidc_link", "width=720,height=760,popup=yes");
  try {
    if (btn) {
      btn.disabled = true;
      btn.textContent = T("settings.security.oidc.waiting");
    }
    const saved = await window.cwAppAuthOidcSavePending();
    if (!saved?.ok) {
      if (popup && !popup.closed) popup.close();
      throw new Error(saved?.error || T("settings.security.oidc.saveFailed"));
    }
    const r = await fetch("/api/app-auth/oidc/link/start", {
      method: "POST",
      cache: "no-store",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    const data = await r.json().catch(() => null);
    if (!r.ok || !data?.ok || !data?.state || !data?.auth_url) {
      if (popup && !popup.closed) popup.close();
      throw new Error(data?.error || T("settings.security.oidc.linkFailedStatus", { status: String(r.status) }));
    }
    if (popup && !popup.closed) popup.location.href = data.auth_url;
    else window.open(data.auth_url, "_blank", "noopener,noreferrer");
    for (;;) {
      await new Promise((resolve) => setTimeout(resolve, 2000));
      const pr = await fetch("/api/app-auth/oidc/link/check", {
        method: "POST",
        cache: "no-store",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ state: data.state }),
      });
      const pd = await pr.json().catch(() => null);
      if (pr.ok && pd?.ok && pd.pending === true) continue;
      if (!pr.ok || !pd?.ok) throw new Error(pd?.error || T("settings.security.oidc.linkFailedStatus", { status: String(pr.status) }));
      if (popup && !popup.closed) popup.close();
      await cwAppAuthOidcRefreshStatus();
      try { _cwShowToast?.(T("settings.security.oidc.linked"), true); } catch {}
      return;
    }
  } catch (e) {
    if (popup && !popup.closed) popup.close();
    try { _cwShowToast?.(String(e?.message || e || T("settings.security.oidc.linkFailed")), false); } catch {}
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = original;
    }
  }
};

window.cwAppAuthOidcUnlink = async function cwAppAuthOidcUnlink() {
  const ok = (await (window.CW?.MobileUI?.ask || window.confirm)(T("settings.security.oidc.unlinkConfirm")));
  if (!ok) return;
  try {
    const r = await fetch("/api/app-auth/oidc/unlink", {
      method: "POST",
      cache: "no-store",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    const data = await r.json().catch(() => null);
    if (!r.ok || !data?.ok) throw new Error(data?.error || T("settings.security.oidc.unlinkFailedStatus", { status: String(r.status) }));
    await cwAppAuthOidcRefreshStatus();
    try { _cwShowToast?.(T("settings.security.oidc.unlinked"), true); } catch {}
  } catch (e) {
    try { _cwShowToast?.(String(e?.message || e || T("settings.security.oidc.unlinkFailed")), false); } catch {}
  }
};

function cwAppAuthTotpRender(st) {
  const enabled = !!st?.totp_enabled;
  const state = document.getElementById("app_auth_totp_state");
  const setup = document.getElementById("app_auth_totp_setup");
  const setupBtn = document.getElementById("btn-app-auth-totp-setup");
  const verifyBtn = document.getElementById("btn-app-auth-totp-verify");
  const disableBtn = document.getElementById("btn-app-auth-totp-disable");
  const secret = document.getElementById("app_auth_totp_secret");
  const code = document.getElementById("app_auth_totp_code");
  const pending = !!(secret && String(secret.value || "").trim());
  if (state) state.textContent = enabled ? T("settings.security.twoFactor.stateOn") : (pending ? T("settings.security.twoFactor.statePending") : T("settings.security.twoFactor.stateOff"));
  if (setup) setup.classList.toggle("hidden", !pending);
  if (setupBtn) setupBtn.classList.toggle("hidden", pending);
  if (verifyBtn) verifyBtn.classList.toggle("hidden", !pending);
  if (disableBtn) disableBtn.disabled = !enabled && !pending;
  if (code && !code.__cwTotpWired) {
    code.addEventListener("input", () => { code.value = String(code.value || "").replace(/\D+/g, "").slice(0, 6); });
    code.__cwTotpWired = true;
  }
}

window.cwAppAuthTotpSetup = async function cwAppAuthTotpSetup() {
  const btn = document.getElementById("btn-app-auth-totp-setup");
  const original = btn?.textContent || T("settings.security.twoFactor.setup");
  try {
    if (btn) {
      btn.disabled = true;
      btn.textContent = T("settings.security.twoFactor.creating");
    }
    const r = await fetch("/api/app-auth/totp/setup", {
      method: "POST",
      cache: "no-store",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ user_id: "administrator" }),
    });
    const data = await r.json().catch(() => null);
    if (!r.ok || !data?.ok) throw new Error(data?.error || T("settings.security.twoFactor.setupFailedStatus", { status: String(r.status) }));
    const secret = document.getElementById("app_auth_totp_secret");
    const code = document.getElementById("app_auth_totp_code");
    if (secret) secret.value = data.secret || "";
    if (code) code.value = "";
    cwAppAuthTotpRender({ totp_enabled: !!data.enabled });
    try { code?.focus?.(); } catch {}
    try { _cwShowToast?.(T("settings.security.twoFactor.created"), true); } catch {}
  } catch (e) {
    try { _cwShowToast?.(String(e?.message || e || T("settings.security.twoFactor.setupFailed")), false); } catch {}
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = original;
    }
  }
};

window.cwAppAuthTotpVerify = async function cwAppAuthTotpVerify() {
  const code = String(document.getElementById("app_auth_totp_code")?.value || "").replace(/\D+/g, "").slice(0, 6);
  if (code.length !== 6) {
    try { _cwShowToast?.(T("settings.security.twoFactor.enterCode"), false); } catch {}
    return;
  }
  try {
    const r = await fetch("/api/app-auth/totp/verify", {
      method: "POST",
      cache: "no-store",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ user_id: "administrator", code }),
    });
    const data = await r.json().catch(() => null);
    if (!r.ok || !data?.ok) throw new Error(data?.error || T("settings.security.twoFactor.verifyFailedStatus", { status: String(r.status) }));
    const secret = document.getElementById("app_auth_totp_secret");
    const codeEl = document.getElementById("app_auth_totp_code");
    if (secret) secret.value = "";
    if (codeEl) codeEl.value = "";
    await cwRefreshAppAuthStatus();
    try { _cwShowToast?.("2FA enabled", true); } catch {}
  } catch (e) {
    try { _cwShowToast?.(String(e?.message || e || T("settings.security.twoFactor.verifyFailed")), false); } catch {}
  }
};

window.cwAppAuthTotpDisable = async function cwAppAuthTotpDisable() {
  const btn = document.getElementById("btn-app-auth-totp-disable");
  if (btn && btn.dataset.confirmTotpDisable !== "1") {
    btn.dataset.confirmTotpDisable = "1";
    const original = btn.innerHTML || TH("settings.security.twoFactor.disable");
    btn.textContent = T("settings.security.twoFactor.confirmDisable");
    setTimeout(() => { try { delete btn.dataset.confirmTotpDisable; btn.innerHTML = original; } catch {} }, 2200);
    return;
  }
  try {
    const r = await fetch("/api/app-auth/totp/disable", {
      method: "POST",
      cache: "no-store",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ user_id: "administrator" }),
    });
    const data = await r.json().catch(() => null);
    if (!r.ok || !data?.ok) throw new Error(data?.error || T("settings.security.twoFactor.disableFailedStatus", { status: String(r.status) }));
    const secret = document.getElementById("app_auth_totp_secret");
    const code = document.getElementById("app_auth_totp_code");
    if (secret) secret.value = "";
    if (code) code.value = "";
    await cwRefreshAppAuthStatus();
    try { _cwShowToast?.("2FA disabled", true); } catch {}
  } catch (e) {
    try { _cwShowToast?.(String(e?.message || e || T("settings.security.twoFactor.disableFailed")), false); } catch {}
  }
};


/* Settings Hub: Scheduling */
const SCHED_SETTINGS_TAB_KEY = "cw.ui.scheduling.tab.v1";
const SCHED_SETTINGS_TABS = ["basic", "advanced", "webhooks"];

let __cwSchedOpen = false;

function cwSchedProviderSelect(open) {
  const panelHost = document.getElementById("sched-provider-panel");
  if (!panelHost) return;

  const wantOpen = (open == null) ? !__cwSchedOpen : !!open;
  __cwSchedOpen = wantOpen;
  panelHost.classList.toggle("hidden", !wantOpen);
}

function cwSchedSettingsSelect(tab, opts = {}) {
  const panelHost = document.getElementById("sched-provider-panel");
  const panel = panelHost?.querySelector('.cw-meta-provider-panel[data-provider="scheduler"]');
  const paneTabs = document.getElementById("sched-pane-tabs");
  if (!panelHost || !panel) return;

  const t = (tab || "basic").toLowerCase();
  const want = SCHED_SETTINGS_TABS.includes(t) ? t : "basic";

  panel.querySelectorAll(".cw-subtile[data-sub]").forEach((btn) => {
    btn.classList.toggle("active", (btn.dataset.sub || "").toLowerCase() === want);
  });
  paneTabs?.querySelectorAll("[data-sub]").forEach((btn) => {
    btn.classList.toggle("active", (btn.dataset.sub || "").toLowerCase() === want);
  });
  panel.querySelectorAll(".cw-subpanel[data-sub]").forEach((sp) => {
    sp.classList.toggle("active", (sp.dataset.sub || "").toLowerCase() === want);
  });
  const webhooks = document.getElementById("schWebhooks");
  if (webhooks) webhooks.classList.toggle("hidden", want !== "webhooks");

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

  const pHooks = document.createElement("div");
  pHooks.className = "cw-subpanel";
  pHooks.dataset.sub = "webhooks";

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

  const f1 = mkField(T("common.actions.enable"), enabledEl);
  const f2 = mkField(T("sync.schedule.frequency"), modeEl, T("sync.schedule.hint.mode"));
  const f3 = mkField(T("sync.schedule.mode.everyN"), nEl, T("sync.schedule.hint.everyN"));
  const f4 = mkField(T("sync.schedule.time"), timeEl, T("sync.schedule.hint.daily"));
  const customWrap = document.createElement("div");
  customWrap.className = "cw-inline-row";
  if (customValueEl) customWrap.appendChild(customValueEl);
  if (customUnitEl) customWrap.appendChild(customUnitEl);
  const f5 = mkField(T("sync.schedule.customInterval"), customWrap.childNodes.length ? customWrap : null, T("sync.schedule.hint.custom"));

  [f1, f2, f3, f4, f5].forEach((x) => x && basicFields.appendChild(x));
  if (basicFields.childNodes.length) basicCard.appendChild(basicFields);
  pBasic.appendChild(basicCard);

  const advMount = detach("sched_advanced_mount") || (() => {
    const d = document.createElement("div");
    d.id = "sched_advanced_mount";
    return d;
  })();

  pAdv.appendChild(advMount);

  const hooksMount = detach("sched_webhooks_mount") || (() => {
    const d = document.createElement("div");
    d.id = "sched_webhooks_mount";
    return d;
  })();

  pHooks.appendChild(hooksMount);

  subPanels.appendChild(pBasic);
  subPanels.appendChild(pAdv);
  subPanels.appendChild(pHooks);

  wrap.appendChild(subPanels);

  panelHost.appendChild(wrap);

  try {
    const raw = document.getElementById("sched-provider-raw");
    if (raw) raw.classList.add("hidden");
  } catch {}

  document.querySelectorAll("#sched-pane-tabs [data-sub]").forEach((btn) => {
    btn.addEventListener("click", () => cwSchedSettingsSelect(btn.dataset.sub));
  });

  const schedRefresh = document.getElementById("cw-sched-refresh");
  if (schedRefresh && !schedRefresh.__cwBound) {
    schedRefresh.__cwBound = true;
    schedRefresh.addEventListener("click", async () => {
      if (schedRefresh.classList.contains("loading")) return;
      schedRefresh.classList.add("loading", "spin");
      try {
        window.CW?.Cache?.invalidate?.(["schedulingStatus"]);
        await window.loadScheduling?.();
        await window.refreshSchedulingBanner?.();
      } catch {}
      schedRefresh.classList.remove("loading", "spin");
    });
  }

  let lastSub = "basic";
  try { lastSub = (localStorage.getItem(SCHED_SETTINGS_TAB_KEY) || "basic").toLowerCase(); } catch {}
  cwSchedSettingsSelect(SCHED_SETTINGS_TABS.includes(lastSub) ? lastSub : "basic", { persist: false });
}

function cwSchedProviderEnsure() {
  const panelHost = document.getElementById("sched-provider-panel");
  if (!panelHost) return;

  if (!panelHost.dataset.__cwSchedBuilt) {
    try { cwBuildSchedulerPanel(); } catch {}
    panelHost.dataset.__cwSchedBuilt = "1";
  }

  cwSchedProviderSelect(true);

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

  set("hub_sch_enabled", T("sync.schedule.hub.status", { value: patch.enabled ? T("common.status.enabled") : T("common.status.disabled") }));

  let modeText = patch.mode || "hourly";
  if (patch.mode === "hourly") modeText = T("sync.schedule.mode.hourly");
  else if (patch.mode === "every_n_hours") modeText = T("sync.schedule.hub.everyHours", { n: patch.every_n_hours || 2 });
  else if (patch.mode === "daily_time") modeText = T("sync.schedule.hub.daily", { time: patch.daily_time || "—" });
  else if (patch.mode === "custom_interval") {
    const minutes = Math.max(15, parseInt(patch.custom_interval_minutes || 60, 10) || 60);
    modeText = minutes % 60 === 0 ? T("sync.schedule.hub.customHours", { n: minutes / 60 }) : T("sync.schedule.hub.customMinutes", { n: minutes });
  }
  set("hub_sch_mode", T("sync.schedule.hub.mode", { value: modeText }));

  const adv = patch.advanced || {};
  const jobs = Array.isArray(adv.jobs) ? adv.jobs : [];
  const workflows = Array.isArray(adv.workflows) ? adv.workflows : [];
  const active = jobs.filter(j => j && j.active !== false).length + workflows.filter(w => w && w.active !== false).length;
  const total = jobs.length + workflows.length;

  set("hub_sch_adv", T("sync.schedule.hub.plan", { value: adv.enabled ? T("common.status.on") : T("common.status.off") }));
  set("hub_sch_steps", total ? T("sync.schedule.hub.steps", { active, total }) : T("sync.schedule.hub.stepsNone"));
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
      if (SCHED_SETTINGS_TABS.includes(saved)) tab = saved;
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
    try { cwBuildFanartPanel(); } catch {}
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

function _cwSetStat(id, value) {
  const el = document.getElementById(id);
  if (!el) return;
  const text = String(value ?? "");
  el.textContent = text;
  if (text && text !== "-") el.title = text;
  else el.removeAttribute("title");
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

function _cwFormatCompactUtc(value) {
  const raw = String(value || "").trim();
  if (!raw) return "-";
  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) return raw;
  const month = new Intl.DateTimeFormat((window.CW?.I18n || window.__cwI18n)?.locale || "en", { month: "short", timeZone: "UTC" }).format(d);
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hour = String(d.getUTCHours()).padStart(2, "0");
  const minute = String(d.getUTCMinutes()).padStart(2, "0");
  return `${day} ${month}, ${hour}:${minute} UTC`;
}

function _cwAnimeMappingSetBusy(on, label = "") {
  animeMappingBusy = !!on;
  ["anime_mapping_enabled", "anime_mapping_enabled_proxy", "anime_mapping_auto_update", "btn-anime-mapping-update", "btn-anime-mapping-rebuild", "btn-anime-mapping-overrides"].forEach((id) => {
    const el = document.getElementById(id);
    if (el) el.disabled = !!on;
  });
  if (on) {
    _cwSetText("anime_mapping_dataset", label || T("settings.anime.state.updating"));
  }
}

function cwAnimeMappingRenderStatus(st = {}) {
  const cfg = window._cfgCache || {};
  const block = cfg.anime_mapping || {};
  const err = String(st.error || st.message || (st.identity_error ? T("settings.anime.identityFailed", { reason: st.identity_error }) : "")).trim();
  const installed = !!st.installed;
  const ready = !!st.index_ready;
  const enabled = st.enabled !== undefined ? !!st.enabled : !!block.enabled;
  const autoUpdate = st.auto_update !== undefined ? !!st.auto_update : block.auto_update !== false;
  const dataset = err ? T("settings.anime.state.error") : (animeMappingBusy ? T("settings.anime.state.updating") : (installed ? T("settings.anime.state.installed") : T("settings.anime.state.missing")));
  const index = ready ? T("settings.anime.indexReady") : T("settings.anime.indexMissing");
  const sources = installed ? T("settings.anime.sources", { count: Number(st.source_count || 0) }) : "-";
  const edges = installed ? T("settings.anime.edges", { count: Number(st.edge_count || 0) }) : "-";
  const updatedSeconds = Number(st.last_updated_at || 0);
  const updatedAt = updatedSeconds > 0 ? new Date(updatedSeconds * 1000).toISOString() : "";
  const updated = _cwFormatUtc(updatedAt);
  const updatedCompact = _cwFormatCompactUtc(updatedAt);

  _cwSetChecked("anime_mapping_enabled", enabled);
  _cwSetChecked("anime_mapping_auto_update", autoUpdate);
  _cwSetText("anime_mapping_dataset", dataset);
  _cwSetStat("anime_mapping_generated", updatedCompact);
  _cwSetText("anime_mapping_index", index);
  _cwSetStat("anime_mapping_sources", sources);
  _cwSetStat("anime_mapping_edges", edges);
  _cwSetStat("anime_mapping_counts", installed ? `${sources} | ${edges}` : "-");
  _cwSetText("anime_mapping_last_update", updated);
  const sourceDate = value => value ? _cwFormatUtc(value).slice(0, 10) : T("settings.anime.dateUnavailable");
  _cwSetStat("anime_mapping_episodes_version", installed ? `${st.release_tag || "v3"} · ${sourceDate(st.dataset_generated_on)}` : T("settings.anime.notInstalled"));
  _cwSetStat("anime_mapping_identity_version", st.identity_installed ? `${st.identity_release_tag || "v3"} · ${sourceDate(st.identity_generated_on)}` : T("settings.anime.notInstalled"));
  const identityVersion = document.getElementById("anime_mapping_identity_version");
  if (identityVersion && st.identity_revision) identityVersion.title = T("settings.anime.identityRevision", { revision: st.identity_revision });
  const cwApplied = Array.isArray(st.cw_applied) ? st.cw_applied.length : 0;
  const cwRedundant = Array.isArray(st.cw_redundant) ? st.cw_redundant.length : 0;
  _cwSetStat("anime_mapping_cw_version", st.cw_revision ? T("settings.anime.cwApplied", { revision: st.cw_revision, count: cwApplied }) : (installed ? T("settings.anime.notLoaded") : T("settings.anime.notInstalled")));
  const cwVersion = document.getElementById("anime_mapping_cw_version");
  if (cwVersion && st.cw_revision) {
    const origin = st.cw_source === "downloaded"
      ? (st.cw_download_generated_on ? T("settings.anime.downloadedOn", { date: sourceDate(st.cw_download_generated_on) }) : T("settings.anime.downloaded"))
      : T("settings.anime.bundled");
    cwVersion.title = [origin, cwRedundant ? T("settings.anime.fixedUpstream", { count: cwRedundant }) : "", st.cw_error || ""].filter(Boolean).join("\n");
  }
  _cwSetText("anime_mapping_meta_status", err ? T("settings.anime.state.error") : (installed && ready ? T("settings.anime.state.upToDate") : (installed ? T("settings.anime.state.needsIndex") : T("settings.anime.state.missing"))));
  const statusPill = document.getElementById("anime_mapping_meta_status");
  if (statusPill) {
    statusPill.classList.toggle("is-ok", !!(installed && ready && !err));
    statusPill.classList.toggle("is-err", !!err);
  }
  _cwSetText("anime_mapping_error", err);

  const dot = document.getElementById("anime-mapping-dot");
  if (dot) {
    dot.classList.toggle("on", !!(enabled && installed && ready && !err));
    dot.classList.toggle("off", !!(!enabled || !installed || !ready || err));
  }

  const heroBadge = document.getElementById("anime_mapping_hero_status");
  if (heroBadge) {
    heroBadge.classList.toggle("is-on", !!enabled);
    _cwSetText("anime_mapping_hero_status_text", enabled ? T("common.status.enabled") : T("common.status.disabled"));
  }
  const enabledProxy = document.getElementById("anime_mapping_enabled_proxy");
  if (enabledProxy) {
    enabledProxy.classList.toggle("is-on", !!enabled);
    enabledProxy.setAttribute("aria-checked", enabled ? "true" : "false");
  }
  const datasetIcon = document.getElementById("anime_mapping_dataset_icon");
  if (datasetIcon) datasetIcon.classList.toggle("is-ok", !!(installed && !err));
  const indexIcon = document.getElementById("anime_mapping_index_icon");
  if (indexIcon) indexIcon.classList.toggle("is-ok", !!(ready && !err));
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
    cwAnimeMappingRenderStatus({ error: e?.message || T("settings.anime.statusFailed") });
    return null;
  }
}

async function cwAnimeMappingSaveSettings() {
  const enabled = !!document.getElementById("anime_mapping_enabled")?.checked;
  const autoUpdate = !!document.getElementById("anime_mapping_auto_update")?.checked;
  const st0 = window.__animeMappingStatus || {};
  const needsBootstrap = enabled && !(st0.installed && st0.index_ready);
  _cwAnimeMappingSetBusy(true, needsBootstrap ? T("settings.anime.state.downloading") : T("settings.anime.state.saving"));
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
      }),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok || data.ok === false) throw new Error(data.message || data.error || T("settings.anime.settingsFailedStatus", { status: String(r.status) }));
    window._cfgCache ||= {};
    window._cfgCache.anime_mapping = data.anime_mapping || {
      ...(window._cfgCache.anime_mapping || {}),
      enabled,
      auto_update: autoUpdate,
      provider: "anibridge",
    };
    if (data.status) window.__animeMappingStatus = data.status;
    cwAnimeMappingRenderStatus(data.status || window.__animeMappingStatus || {});
    if (data.bootstrap_error) throw new Error(data.bootstrap_error);
    try { window.CW?.DOM?.showToast?.(needsBootstrap ? T("settings.anime.enabledDownloaded") : T("settings.anime.saved"), true); } catch {}
  } catch (e) {
    cwAnimeMappingRenderStatus({ ...(window.__animeMappingStatus || {}), error: e?.message || T("settings.saveFailed") });
    try { window.CW?.DOM?.showToast?.(e?.message || T("settings.anime.saveFailed"), false); } catch {}
  } finally {
    _cwAnimeMappingSetBusy(false);
    try { await cwAnimeMappingRefreshStatus(); } catch {}
  }
}

async function cwAnimeMappingRun(action) {
  const update = action === "update";
  _cwAnimeMappingSetBusy(true, update ? T("settings.anime.state.updating") : T("settings.anime.state.rebuilding"));
  try {
    const r = await fetch(update ? "/api/anime-mapping/update" : "/api/anime-mapping/rebuild-index", {
      method: "POST",
      cache: "no-store",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(update ? { force: false } : {}),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok || data.ok === false) throw new Error(data.message || data.error || T(update ? "settings.anime.updateFailedStatus" : "settings.anime.rebuildFailedStatus", { status: String(r.status) }));
    cwAnimeMappingRenderStatus(data.status || data || {});
    const message = !update ? T("settings.anime.rebuilt")
      : data.updated ? (data.cw_mappings_updated && !data.identity_updated && !data.mappings_updated ? T("settings.anime.cwUpdated") : (data.identity_updated && !data.mappings_updated ? T("settings.anime.identityUpdated") : T("settings.anime.updated")))
      : data.rebuilt ? T("settings.anime.rebuilt") : T("settings.anime.alreadyCurrent");
    try { window.CW?.DOM?.showToast?.(message, true); } catch {}
  } catch (e) {
    cwAnimeMappingRenderStatus({ ...(window.__animeMappingStatus || {}), error: e?.message || T("settings.anime.actionFailedShort") });
    try { window.CW?.DOM?.showToast?.(e?.message || T("settings.anime.actionFailed"), false); } catch {}
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
        <strong>${TH("settings.anime.title")}</strong>
        <span class="cw-meta-provider-help" title="${TH("settings.anime.help")}" aria-label="${TH("settings.anime.help")}">
          <span class="material-symbols-rounded cw-meta-provider-help-icon" aria-hidden="true">info</span>
        </span>
      </div>
      <span class="auth-dot" id="anime-mapping-dot" aria-hidden="true"></span>
    </div>
    <div class="body">
      <div class="auth-card anime-mapping-card am-body">
        <header class="am-modal-header">
          <span class="material-symbols-rounded am-modal-icon" aria-hidden="true">shield</span>
          <div class="am-modal-title">
            <h4>${TH("settings.anime.title")}</h4>
            <p>${TH("settings.anime.subtitle")}</p>
          </div>
          <label class="am-header-state" id="anime_mapping_hero_status">
            <span class="am-hero-badge">
              <span class="material-symbols-rounded" aria-hidden="true">check_circle</span>
              <span id="anime_mapping_hero_status_text">${TH("common.status.disabled")}</span>
            </span>
            <span class="cx-toggle am-toggle">
              <input type="checkbox" id="anime_mapping_enabled" aria-label="${TH("settings.anime.enableIndex")}">
              <span class="cx-toggle-ui" aria-hidden="true"></span>
            </span>
          </label>
        </header>

        <section class="am-status-strip" aria-label="${TH("settings.anime.status")}">
          <div class="am-status-item">
            <span class="material-symbols-rounded am-status-icon" id="anime_mapping_dataset_icon" aria-hidden="true">database</span>
            <span id="anime_mapping_dataset">-</span>
          </div>
          <div class="am-status-item">
            <span class="material-symbols-rounded am-status-icon" id="anime_mapping_index_icon" aria-hidden="true">check_circle</span>
            <span id="anime_mapping_index">-</span>
          </div>
          <div class="am-status-item">
            <span class="material-symbols-rounded am-status-icon" aria-hidden="true">database</span>
            <span id="anime_mapping_sources">-</span>
          </div>
          <div class="am-status-item">
            <span class="material-symbols-rounded am-status-icon" aria-hidden="true">account_tree</span>
            <span id="anime_mapping_edges">-</span>
          </div>
          <div class="am-status-item">
            <span class="material-symbols-rounded am-status-icon" aria-hidden="true">schedule</span>
            <span>${TH("settings.anime.updatedLabel")} <span id="anime_mapping_generated">-</span></span>
          </div>
          <span class="am-sr-only" id="anime_mapping_counts">-</span>
          <span class="am-sr-only" id="anime_mapping_last_update">-</span>
        </section>

        <section class="am-panel am-settings-panel">
          <div class="am-setting-row">
            <div>
              <strong>${TH("settings.anime.index")}</strong>
              <span>${TH("settings.anime.indexHint")}</span>
            </div>
            <button class="am-toggle-proxy" id="anime_mapping_enabled_proxy" type="button" role="switch" aria-checked="false" aria-label="${TH("settings.anime.toggleIndex")}">
              <span class="material-symbols-rounded" aria-hidden="true">check</span>
            </button>
          </div>
          <div class="am-setting-row">
            <div>
              <strong>${TH("settings.anime.autoUpdates")}</strong>
              <span>${TH("settings.anime.autoUpdatesHint")}</span>
            </div>
            <label class="cx-toggle am-toggle">
              <input type="checkbox" id="anime_mapping_auto_update" aria-label="${TH("settings.anime.enableAutoUpdates")}">
              <span class="cx-toggle-ui" aria-hidden="true"></span>
            </label>
          </div>
        </section>

        <section class="am-panel am-dataset-panel">
          <div class="am-panel-head">
            <h4>${TH("settings.anime.dataset")}</h4>
            <span class="am-status-pill" id="anime_mapping_meta_status">-</span>
          </div>
          <dl class="am-dataset-list">
            <div>
              <dt>${TH("settings.anime.episodes")}</dt>
              <dd><a href="https://github.com/anibridge/anibridge-mappings" target="_blank" rel="noopener noreferrer">aniBridge/anibridge-mappings<span class="material-symbols-rounded" aria-hidden="true">open_in_new</span></a><span class="am-dataset-version" id="anime_mapping_episodes_version">-</span></dd>
            </div>
            <div>
              <dt>${TH("settings.anime.identity")}</dt>
              <dd><a href="https://github.com/nattadasu/animeApi" target="_blank" rel="noopener noreferrer">nattadasu/animeApi<span class="material-symbols-rounded" aria-hidden="true">open_in_new</span></a><span class="am-dataset-version" id="anime_mapping_identity_version">-</span></dd>
            </div>
            <div>
              <dt>${TH("settings.anime.corrections")}</dt>
              <dd><a href="https://github.com/cenodude/CrossWatch/blob/main/cw_platform/anime_mapping/cw_mappings.json" target="_blank" rel="noopener noreferrer">CW Anime DB<span class="material-symbols-rounded" aria-hidden="true">open_in_new</span></a><span class="am-dataset-version" id="anime_mapping_cw_version">-</span></dd>
            </div>
          </dl>
          <p class="am-details-copy">${TH("settings.anime.datasetCopy")}</p>
          <div class="auth-card-notes" id="anime_mapping_error"></div>
        </section>

        <section class="am-panel am-advanced-panel" id="anime_mapping_advanced">
          <div class="am-advanced-head">
            <span class="material-symbols-rounded am-advanced-icon" aria-hidden="true">settings</span>
            <span>
              <strong>${TH("common.labels.advanced")}</strong>
              <small>${TH("settings.anime.advancedHint")}</small>
            </span>
          </div>
          <div class="am-actions">
            <button class="btn primary" type="button" id="btn-anime-mapping-update">${TH("settings.anime.updateNow")}</button>
            <button class="btn" type="button" id="btn-anime-mapping-rebuild">${TH("settings.anime.rebuildIndex")}</button>
            <button class="btn" type="button" id="btn-anime-mapping-overrides">${TH("settings.anime.customMappings")}</button>
          </div>
        </section>
      </div>
    </div>
  `;

  panelHost.appendChild(wrap);

  const enabled = document.getElementById("anime_mapping_enabled");
  const autoUpdate = document.getElementById("anime_mapping_auto_update");
  const btnUpdate = document.getElementById("btn-anime-mapping-update");
  const btnRebuild = document.getElementById("btn-anime-mapping-rebuild");
  const enabledProxy = document.getElementById("anime_mapping_enabled_proxy");
  if (enabled && !enabled.__cwAnimeWired) {
    enabled.addEventListener("change", () => cwAnimeMappingSaveSettings());
    enabled.__cwAnimeWired = true;
  }
  if (enabledProxy && !enabledProxy.__cwAnimeWired) {
    enabledProxy.addEventListener("click", () => {
      const target = document.getElementById("anime_mapping_enabled");
      if (!target || target.disabled || enabledProxy.disabled) return;
      target.checked = !target.checked;
      target.dispatchEvent(new Event("change", { bubbles: true }));
    });
    enabledProxy.__cwAnimeWired = true;
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
  const btnOverrides = document.getElementById("btn-anime-mapping-overrides");
  if (btnOverrides && !btnOverrides.__cwAnimeWired) {
    btnOverrides.addEventListener("click", () => window.openAnimeOverridesModal?.());
    btnOverrides.__cwAnimeWired = true;
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
      <span class="cw-meta-provider-help" title="${TH("settings.meta.tmdb.help")}" aria-label="${TH("settings.meta.tmdb.help")}">
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
      <div class="cw-panel-title">${TH("settings.meta.tmdb.panelTitle")}</div>
      <div class="muted">${TH("settings.meta.tmdb.panelHint")}</div>
    </div>
  `;

  const subTiles = document.createElement("div");
  subTiles.className = "cw-subtiles";
  subTiles.innerHTML = `
    <button type="button" class="cw-subtile active" data-sub="api">API key</button>
    <button type="button" class="cw-subtile" data-sub="advanced">${TH("common.labels.advanced")}</button>
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
    i.placeholder = T("settings.meta.tmdb.keyPlaceholder");
    return i;
  })();

  const hint = detach("tmdb_hint") || (() => {
    const d = document.createElement("div");
    d.id = "tmdb_hint";
    d.className = "auth-card-notes";
    d.textContent = T("settings.meta.tmdb.addKey");
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
    <button type="button" class="btn good" id="tmdb_check">${TH("settings.meta.check")}</button>
    <button type="button" class="btn danger" id="tmdb_delete">${TH("settings.metaDelete")}</button>
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
    keyInput.dataset.touched = "1";
    keyInput.dataset.masked = "0";
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

  const advCard = document.createElement("div");
  advCard.className = "auth-card";

  const advFields = document.createElement("div");
  advFields.className = "auth-card-fields";

  if (localeEl) {
    const f = document.createElement("div");
    f.className = "field";
    f.innerHTML = `<div class="muted" style="margin-bottom:6px;">${TH("settings.meta.tmdb.locale")}</div>`;
    advFields.appendChild(f);
    f.appendChild(localeEl);
    const note = document.createElement("div");
    note.className = "auth-card-notes";
    note.textContent = T("settings.meta.tmdb.localeHint");
    advFields.appendChild(note);
  }

  if (!localeEl) {
    const note = document.createElement("div");
    note.className = "auth-card-notes";
    note.textContent = T("settings.meta.noAdvanced");
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

function cwBuildFanartPanel() {
  const panelHost = document.getElementById("meta-provider-panel");
  if (!panelHost) return;
  if (panelHost.querySelector('.cw-meta-provider-panel[data-provider="fanart"]')) return;

  const modeOptions = `<option value="fanart">${TH("settings.meta.fanart.modeFanart")}</option><option value="tmdb_first">${TH("settings.meta.fanart.modeTmdbFirst")}</option><option value="tmdb">${TH("settings.meta.fanart.modeTmdb")}</option>`;
  const wrap = document.createElement("div");
  wrap.className = "section cw-settings-section cw-settings-provider-section cw-meta-provider-panel active";
  wrap.id = "sec-meta-fanart";
  wrap.dataset.provider = "fanart";
  wrap.innerHTML = `
    <div class="head" data-toggle-section="sec-meta-fanart">
      <span class="chev"></span>
      <div class="cw-meta-provider-head-copy">
        <strong>Fanart.tv</strong>
        <span class="cw-meta-provider-help" title="${TH("settings.meta.fanart.help")}" aria-label="${TH("settings.meta.fanart.help")}">
          <span class="material-symbols-rounded cw-meta-provider-help-icon" aria-hidden="true">info</span>
        </span>
      </div>
      <span class="auth-dot" id="meta-fanart-dot" aria-hidden="true"></span>
    </div>
    <div class="body">
      <div class="cw-panel-head">
        <div>
          <div class="cw-panel-title">Fanart.tv</div>
          <div class="muted">${TH("settings.meta.fanart.panelHint")}</div>
        </div>
      </div>
      <div class="cw-subtiles">
        <button type="button" class="cw-subtile active" data-sub="api">API key</button>
        <button type="button" class="cw-subtile" data-sub="settings">${TH("navigation.tabs.settings")}</button>
      </div>
      <div class="cw-subpanels">
        <div class="cw-subpanel active" data-sub="api">
          <div class="auth-card">
            <div class="auth-card-notes" id="fanart_hint">${TH("settings.meta.fanart.providedBy")} <a href="https://fanart.tv" target="_blank" rel="noopener noreferrer">fanart.tv</a>. ${TH("settings.meta.fanart.noKeys")}</div>
            <div class="auth-card-notes" id="fanart_tmdb_hint" hidden>${TH("settings.meta.fanart.tmdbFirst")}</div>
            <div class="auth-card-fields">
              <div style="display:flex;gap:14px;flex-wrap:wrap">
                <div class="field" style="flex:1 1 220px;min-width:0">
                  <div class="muted" style="margin-bottom:6px;">${TH("settings.meta.fanart.personalKey")}</div>
                  <input id="fanart_personal_api_key" type="text" placeholder="${TH("settings.meta.fanart.personalPlaceholder")}" autocomplete="off">
                </div>
                <div class="field" style="flex:1 1 220px;min-width:0">
                  <div class="muted" style="margin-bottom:6px;">${TH("settings.meta.fanart.projectKey")}</div>
                  <input id="fanart_api_key" type="text" placeholder="${TH("settings.meta.fanart.projectPlaceholder")}" autocomplete="off">
                </div>
              </div>
              <div class="auth-card-notes">${TH("settings.meta.fanart.keyNote")}</div>
              <div class="inline" style="display:flex;margin-top:10px;gap:10px;align-items:center;justify-content:flex-start">
                <button type="button" class="btn good" id="fanart_check">${TH("settings.meta.fanart.connect")}</button>
                <button type="button" class="btn danger" id="fanart_delete" hidden>${TH("settings.metaDelete")}</button>
                <div id="fanart_check_msg" class="msg ok hidden" aria-live="polite" style="margin-left:auto;width:auto;max-width:min(520px,60%);flex:0 1 auto;white-space:normal"></div>
              </div>
            </div>
          </div>
        </div>
        <div class="cw-subpanel" data-sub="settings">
          <div class="auth-card">
            <div class="auth-card-fields">
              <div class="field">
                <div class="muted" style="margin-bottom:6px;">${TH("settings.meta.fanart.logos")}</div>
                <select id="fanart_pref_logos" aria-label="${TH("settings.meta.fanart.logoSource")}">${modeOptions}</select>
              </div>
              <div class="field">
                <div class="muted" style="margin-bottom:6px;">${TH("settings.meta.fanart.backgrounds")}</div>
                <select id="fanart_pref_backgrounds" aria-label="${TH("settings.meta.fanart.backgroundSource")}">${modeOptions}</select>
              </div>
            </div>
            <div class="auth-card-notes" id="fanart_settings_hint" hidden>${TH("settings.meta.fanart.connectFirst")}</div>
            <div class="auth-card-notes">${TH("settings.meta.fanart.modesNote")}</div>
          </div>
        </div>
      </div>
    </div>
  `;
  panelHost.appendChild(wrap);

  wrap.querySelector("#fanart_check")?.addEventListener("click", () => { cwSaveFanartKey().catch(() => {}); });
  wrap.querySelector("#fanart_delete")?.addEventListener("click", () => { cwDeleteFanartKey().catch(() => {}); });
  wrap.querySelectorAll("#fanart_api_key, #fanart_personal_api_key").forEach((input) => {
    input.addEventListener("input", () => {
      input.dataset.touched = "1";
      input.dataset.masked = "0";
      cwSetFanartMessage(true, "");
      cwFanartStateUpdate();
    });
  });
  wrap.querySelectorAll("#fanart_pref_logos, #fanart_pref_backgrounds").forEach((select) => {
    select.addEventListener("change", () => { select.dataset.touched = "1"; });
  });
  cwRefreshFanartState().catch(() => {});
}

function cwFanartCacheUpdate(values) {
  try { window._cfgCache = { ...(window._cfgCache || {}), fanart: { ...(window._cfgCache?.fanart || {}), ...values } }; } catch {}
  try { window.CW?.Cache?.invalidate?.("config"); } catch {}
  try { window.invalidateConfigCache?.(); } catch {}
}

function cwFanartHasTmdb() {
  const cfg = window._cfgCache || {};
  return !!String(cfg?.tmdb?.api_key || cfg?.metadata?.tmdb_api_key || "").trim();
}

function cwFanartKeysTouched() {
  return ["fanart_api_key", "fanart_personal_api_key"].some((id) => document.getElementById(id)?.dataset?.touched === "1");
}

async function cwSaveFanartPrefs() {
  const body = {
    logos: document.getElementById("fanart_pref_logos")?.value || "fanart",
    backgrounds: document.getElementById("fanart_pref_backgrounds")?.value || "fanart",
  };
  try {
    const r = await fetch("/api/fanart/settings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      cache: "no-store",
      credentials: "same-origin",
      body: JSON.stringify(body),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok || !data?.ok) throw new Error(data?.error || "save_failed");
    cwFanartCacheUpdate({ enabled: data.enabled, logos: data.logos, backgrounds: data.backgrounds });
    ["fanart_pref_logos", "fanart_pref_backgrounds"].forEach((id) => { const select = document.getElementById(id); if (select) select.dataset.touched = ""; });
    cwFanartStateUpdate();
    return true;
  } catch {
    cwSetFanartMessage(false, T("settings.meta.fanart.saveFailed"));
    return false;
  }
}

async function cwSaveFanartModal() {
  const on = window._cfgCache?.fanart?.enabled === true;
  if (cwFanartKeysTouched()) {
    if (!on && !cwFanartHasTmdb()) {
      cwSetFanartMessage(false, T("settings.meta.fanart.tmdbFirstShort"));
      throw new Error("fanart_tmdb_missing");
    }
    if (!await cwSaveFanartKey()) throw new Error("fanart_key_failed");
  } else if (!on) {
    return true;
  }
  if (!await cwSaveFanartPrefs()) throw new Error("fanart_settings_failed");
  return true;
}

function cwSetFanartMessage(ok, text) {
  const msg = document.getElementById("fanart_check_msg");
  if (!msg) return;
  msg.textContent = String(text || "");
  msg.classList.toggle("hidden", !msg.textContent);
  msg.classList.toggle("warn", !!msg.textContent && !ok);
  msg.classList.toggle("ok", !!msg.textContent && !!ok);
}

function cwFanartStateUpdate() {
  const fanart = window._cfgCache?.fanart || {};
  const on = fanart.enabled === true;
  const dot = document.getElementById("meta-fanart-dot");
  if (dot) {
    dot.classList.toggle("on", on);
    dot.title = on ? T("common.status.connected") : T("common.status.notConnected");
    dot.setAttribute("aria-label", dot.title);
    dot.closest?.('.cw-meta-provider-panel[data-provider="fanart"]')?.classList?.toggle("is-configured", on);
  }
  const hasTmdb = cwFanartHasTmdb();
  const blocked = !on && !hasTmdb;
  const connected = on && !cwFanartKeysTouched();
  const btn = document.getElementById("fanart_check");
  if (btn) {
    try { window.CW?.AuthShared?.setConnectLocked?.(["fanart_check"], connected, T("common.status.connected")); } catch {}
    if (!btn.classList.contains("busy")) btn.disabled = blocked;
  }
  const tmdbHint = document.getElementById("fanart_tmdb_hint");
  if (tmdbHint) tmdbHint.hidden = !blocked;
  const settingsHint = document.getElementById("fanart_settings_hint");
  if (settingsHint) settingsHint.hidden = on;
  const msg = document.getElementById("fanart_check_msg");
  if (msg) {
    const text = String(msg.textContent || "").trim();
    if (connected && (!text || msg.classList.contains("hidden"))) cwSetFanartMessage(true, T("common.status.connected"));
    else if (!connected && text === T("common.status.connected")) cwSetFanartMessage(true, "");
  }
  const modeOf = (value) => value === false ? "tmdb" : ["fanart", "tmdb_first", "tmdb"].includes(value) ? value : "fanart";
  [["fanart_pref_logos", "logos"], ["fanart_pref_backgrounds", "backgrounds"]].forEach(([id, name]) => {
    const select = document.getElementById(id);
    if (!select) return;
    if (!select.dataset.touched) select.value = modeOf(fanart[name]);
    select.disabled = !on;
  });
}

async function cwRefreshFanartState() {
  let cfg = window._cfgCache || {};
  try {
    const r = await fetch("/api/config", { cache: "no-store", credentials: "same-origin" });
    if (r.ok) {
      const fresh = await r.json().catch(() => null);
      if (fresh && typeof fresh === "object") {
        window._cfgCache = fresh;
        cfg = fresh;
      }
    }
  } catch {}
  [["fanart_api_key", cfg?.fanart?.api_key], ["fanart_personal_api_key", cfg?.fanart?.personal_api_key]].forEach(([id, value]) => {
    const input = document.getElementById(id);
    if (!input) return;
    const has = !!String(value || "").trim();
    input.value = has ? "********" : "";
    input.dataset.masked = has ? "1" : "0";
    input.dataset.touched = "";
  });
  ["fanart_pref_logos", "fanart_pref_backgrounds"].forEach((id) => { const select = document.getElementById(id); if (select) select.dataset.touched = ""; });
  cwFanartStateUpdate();
  return cfg?.fanart?.enabled === true;
}

async function cwSaveFanartKey() {
  const keyInput = document.getElementById("fanart_api_key");
  const personalInput = document.getElementById("fanart_personal_api_key");
  const btn = document.getElementById("fanart_check");
  const api_key = String(keyInput?.value || "").trim();
  const personal_api_key = String(personalInput?.value || "").trim();
  const wasOn = window._cfgCache?.fanart?.enabled === true;
  try {
    if (btn) btn.disabled = true;
    cwSetFanartMessage(true, T("settings.meta.checking"));
    const r = await fetch("/api/fanart/save", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      cache: "no-store",
      credentials: "same-origin",
      body: JSON.stringify({ api_key, personal_api_key }),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok || !data?.ok) {
      cwSetFanartMessage(false, data?.error || T("settings.meta.fanart.checkFailed"));
      return false;
    }
    cwFanartCacheUpdate({ enabled: true });
    await cwRefreshFanartState();
    cwSetFanartMessage(true, T("common.status.connected"));
    try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    if (!wasOn) {
      try { document.dispatchEvent(new CustomEvent("cw-provider-connected", { bubbles: true, detail: { provider: "fanart", key: "FANART_METADATA" } })); } catch {}
    }
    return true;
  } catch {
    cwSetFanartMessage(false, T("settings.meta.fanart.checkFailed"));
    return false;
  } finally {
    cwFanartStateUpdate();
  }
}

async function cwDeleteFanartKey() {
  try {
    const r = await fetch("/api/fanart/disconnect", { method: "POST", cache: "no-store", credentials: "same-origin" });
    const data = await r.json().catch(() => ({}));
    if (!r.ok || data?.ok === false) throw new Error(data?.error || "disconnect_failed");
    ["fanart_api_key", "fanart_personal_api_key"].forEach((id) => {
      const input = document.getElementById(id);
      if (!input) return;
      input.value = "";
      input.dataset.masked = "0";
      input.dataset.touched = "";
    });
    cwFanartCacheUpdate({ enabled: false, api_key: "", personal_api_key: "" });
    cwSetFanartMessage(true, "");
    cwFanartStateUpdate();
    try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    return true;
  } catch {
    cwSetFanartMessage(false, T("settings.meta.fanart.deleteFailed"));
    return false;
  }
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
  const cfgKey = String(cfg?.tmdb?.api_key || cfg?.metadata?.tmdb_api_key || "").trim();
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
    dot.title = verified ? T("settings.meta.verified") : failed ? T("settings.meta.tmdb.checkFailedShort") : hasKeyNow ? T("settings.meta.tmdb.configuredCheck") : T("common.status.notConfigured");
    dot.setAttribute("aria-label", dot.title);
    dot.closest?.('.cw-meta-provider-panel[data-provider="tmdb"]')?.classList?.toggle("is-configured", hasKeyNow && !failed);
  }

  const tmdbCheckBtn = document.getElementById("tmdb_check");
  if (tmdbCheckBtn) {
    const connected = verified || (cfgHasKey && !uiTouched);
    try { window.CW?.AuthShared?.setConnectLocked?.(["tmdb_check"], connected, T("settings.meta.tmdb.connectedLocked")); }
    catch { tmdbCheckBtn.disabled = connected; }
  }

  try { window.syncMetadataProviderDot?.(); } catch {}
}

async function cwRefreshTmdbMetadataState(opts = {}) {
  const input = document.getElementById("tmdb_api_key");
  if (!input) return false;

  const keyFrom = (cfg) => String(cfg?.tmdb?.api_key || cfg?.metadata?.tmdb_api_key || "").trim();
  let key = keyFrom(window._cfgCache || {});
  const force = opts?.force === true;

  if ((force || !key) && typeof fetch === "function") {
    try {
      const r = await fetch("/api/config", { cache: "no-store", credentials: "same-origin" });
      if (r.ok) {
        const fresh = await r.json().catch(() => null);
        if (fresh && typeof fresh === "object") {
          window._cfgCache = fresh;
          key = keyFrom(fresh);
        }
      }
    } catch {}
  }

  const hasKey = !!key;
  const touched = input.dataset?.touched === "1";
  if (!touched || opts?.overwrite === true) {
    try {
      if (window.CW?.AuthShared?.maskSecret) window.CW.AuthShared.maskSecret(input, hasKey);
      else {
        input.value = hasKey ? "********" : "";
        input.dataset.masked = hasKey ? "1" : "0";
        input.dataset.loaded = "1";
        input.dataset.touched = "";
        input.dataset.clear = "";
      }
    } catch {}
    input.dataset.verified = hasKey ? "" : "0";
  }

  try { cwMetaSettingsHubUpdate(); } catch {}
  try { updateTmdbHint(); } catch {}
  return hasKey;
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
        <div class="cw-hub-desc">${TH("settings.meta.tmdb.desc")}</div>
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
  window.cwRefreshTmdbMetadataState = cwRefreshTmdbMetadataState;
} catch {}

async function loadConfig() {
  const waitForAuthBootstrap = async () => {
    try {
      const pending = typeof window.cwIsAuthSetupPending === "function" && window.cwIsAuthSetupPending() === true;
      const boot = window.__cwAuthBootstrapPromise;
      if (!pending || !boot || typeof boot.then !== "function") return;
      await Promise.race([
        boot.catch(() => null),
        new Promise((resolve) => setTimeout(resolve, 2500)),
      ]);
    } catch {}
  };
  const fetchConfig = async () => {
    const r = await fetch("/api/config", { cache: "no-store", credentials: "same-origin" });
    if (r.status === 401) {
      location.href = "/login";
      return null;
    }
    if (!r.ok) throw new Error(`GET /api/config ${r.status}`);
    return await r.json();
  };

  await waitForAuthBootstrap();
  let cfg = await fetchConfig();
  if (!cfg) return;
  if (!Object.keys(cfg || {}).length) {
    await waitForAuthBootstrap();
    if (!(typeof window.cwIsAuthSetupPending === "function" && window.cwIsAuthSetupPending() === true)) {
      cfg = await fetchConfig();
      if (!cfg) return;
    }
  }
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

  const _setChecked = (id, checked) => {
    const el = document.getElementById(id);
    if (el) el.checked = checked;
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
    if (rt.debug_mods && rt.debug_http) mode = 'full';
    else if (rt.debug_mods) mode = 'mods';
    else if (rt.debug || rt.debug_http) mode = 'on';
    _setSelectValue("debug", mode);
  })();
  _setVal("metadata_locale", cfg.metadata?.locale || "");

  
  (function () {
    const ui = cfg.ui || cfg.user_interface || {};
    const aa = cfg.app_auth || {};

    
    {
      const on = (typeof ui.show_watchlist_preview === "boolean")
        ? !!ui.show_watchlist_preview
        : true;
      _setChecked("ui_show_watchlist_preview", on);
    }

    {
      const on = (typeof ui.show_playingcard === "boolean")
        ? !!ui.show_playingcard
        : true;
      _setChecked("ui_show_playingcard", on);
    }

    {
      const on = (typeof ui.show_recent_activity === "boolean")
        ? !!ui.show_recent_activity
        : true;
      _setChecked("ui_show_recent_activity", on);
    }

    {
      const on = (typeof ui.show_recent_history_widget === "boolean")
        ? !!ui.show_recent_history_widget
        : true;
      _setChecked("ui_show_recent_history_widget", on);
    }

    {
      const on = (typeof ui.show_latest_ratings_widget === "boolean")
        ? !!ui.show_latest_ratings_widget
        : true;
      _setChecked("ui_show_latest_ratings_widget", on);
    }

    {
      const on = (typeof ui.show_recent_scrobble_widget === "boolean")
        ? !!ui.show_recent_scrobble_widget
        : true;
      _setChecked("ui_show_recent_scrobble_widget", on);
    }

    {
      const on = (typeof ui.show_recent_progress_widget === "boolean")
        ? !!ui.show_recent_progress_widget
        : false;
      _setChecked("ui_show_recent_progress_widget", on);
    }

    {
      const on = (typeof ui.show_recent_playlists_widget === "boolean")
        ? !!ui.show_recent_playlists_widget
        : false;
      _setChecked("ui_show_recent_playlists_widget", on);
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
      _setSelectValue("ui_media_card", ui.media_card === "compact" ? "compact" : "full");
      window.CW?.ProfileMediaModal?.setStyle?.(ui.media_card);
      try { window.CWTheme?.apply?.(normalizedTheme, { persist: true }); } catch {}
    }

    {
      const on = (typeof ui.show_quick_add_desktop === "boolean")
        ? !!ui.show_quick_add_desktop
        : true;
      _setChecked("ui_show_quick_add_desktop", on);
    }

    {
      const on = (typeof ui.show_quick_add_mobile === "boolean")
        ? !!ui.show_quick_add_mobile
        : true;
      _setChecked("ui_show_quick_add_mobile", on);
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

  })();

  try { cwUiSettingsHubInit?.(); } catch {}

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

  
  setRaw("tmdb_api_key",        val(cfg.tmdb?.api_key || cfg.metadata?.tmdb_api_key));

  
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
      else if (!st.configured || st.reset_required) el.textContent = T("settings.security.auth.setPassword");
      else if (st.authenticated) {
        const exp = (st.session_expires_at && st.session_expires_at > 0) ? new Date(st.session_expires_at * 1000) : null;
        el.textContent = !rememberEnabled
          ? T("settings.security.auth.browserSession")
          : (exp ? T("settings.security.auth.signedInUntil", { time: `${exp.toISOString().replace('T',' ').slice(0,16)}Z` }) : T("settings.security.auth.signedIn"));
      } else el.textContent = T("settings.security.auth.locked");
    }
    const btn = document.getElementById("btn-auth-logout");
    if (btn) btn.disabled = !(st && st.authenticated);
    cwRenderOtherSessions(st);
    cwAppAuthTotpRender(st);
  } catch {}

  try { await cwAppAuthPlexRefreshStatus(); } catch {}
  try { await cwAppAuthOidcRefreshStatus(); } catch {}
  try { await window.cwAppUsersRefresh?.(); } catch {}
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
  if (!text) return T("settings.security.sessions.unknownBrowser");
  return text.length > 72 ? `${text.slice(0, 69)}...` : text;
}

function cwRenderOtherSessions(st) {
  const stateEl = document.getElementById("app_auth_other_sessions_state");
  const detailEl = document.getElementById("app_auth_other_sessions_detail");
  const btn = document.getElementById("btn-auth-logout-others");
  const sessions = Array.isArray(st?.other_sessions) ? st.other_sessions : [];
  const count = Number.isFinite(st?.other_session_count) ? Number(st.other_session_count) : sessions.length;

  if (stateEl) {
    stateEl.textContent = T("settings.security.sessions.loggedInFrom", { count });
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
    else if (!st.configured || st.reset_required) el.textContent = T("settings.security.auth.setPassword");
    else if (st.authenticated) {
      const exp = (st.session_expires_at && st.session_expires_at > 0) ? new Date(st.session_expires_at * 1000) : null;
      el.textContent = !rememberEnabled
        ? T("settings.security.auth.browserSession")
        : (exp ? T("settings.security.auth.signedInUntil", { time: `${exp.toISOString().replace("T", " ").slice(0, 16)}Z` }) : T("settings.security.auth.signedIn"));
    } else el.textContent = T("settings.security.auth.locked");
  }

  const btn = document.getElementById("btn-auth-logout");
  if (btn) btn.disabled = !(st && st.authenticated);
  cwRenderOtherSessions(st);
  cwAppAuthTotpRender(st);
  try { await window.cwAppUsersRefresh?.(); } catch {}
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
  if (masked) return { has: true, masked: true, value: "********" };
  return { has: !!value, masked: false, value };
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
    cwSetTmdbCheckMessage(false, T("settings.meta.tmdb.missingKey"));
    try { cwMetaSettingsHubUpdate(); } catch {}
    return false;
  }
  try {
    if (btn) btn.disabled = true;
    cwSetTmdbCheckMessage(true, T("settings.meta.checking"));
    const fresh = !state.masked;
    const r = await fetch(fresh ? "/api/tmdb/save" : "/api/tmdb/verify", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      cache: "no-store",
      credentials: "same-origin",
      body: JSON.stringify({ api_key: state.value }),
    });
    const data = await r.json().catch(() => ({}));
    const ok = !!(r.ok && data && data.valid !== false && data.ok !== false);
    if (input) input.dataset.verified = ok ? "1" : "0";
    cwSetTmdbCheckMessage(ok, ok ? T("common.status.connected") : (data?.error || T("settings.meta.tmdb.checkFailed")));
    try { cwMetaSettingsHubUpdate(); } catch {}
    if (ok) {
      if (fresh) {
        try { await cwRefreshTmdbMetadataState({ force: true, overwrite: true }); } catch {}
        if (input) input.dataset.verified = "1";
        try { cwMetaSettingsHubUpdate(); } catch {}
        try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
      }
      try { document.dispatchEvent(new CustomEvent("cw-provider-connected", { bubbles: true, detail: { provider: "tmdb", key: "TMDB_METADATA" } })); } catch {}
    }
    return ok;
  } catch {
    if (input) input.dataset.verified = "0";
    cwSetTmdbCheckMessage(false, T("settings.meta.tmdb.checkFailed"));
    try { cwMetaSettingsHubUpdate(); } catch {}
    return false;
  } finally {
    try { cwMetaSettingsHubUpdate(); } catch { if (btn) btn.disabled = false; }
  }
}

async function cwDeleteTmdbKey() {
  const input = document.getElementById("tmdb_api_key");
  const btn = document.getElementById("tmdb_delete");
  const checkBtn = document.getElementById("tmdb_check");
  try {
    if (btn) btn.disabled = true;
    if (checkBtn) checkBtn.disabled = true;
    cwSetTmdbCheckMessage(true, T("settings.meta.deleting"));
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
    cwSetTmdbCheckMessage(false, T("settings.meta.deleted"));
    return true;
  } catch {
    cwSetTmdbCheckMessage(false, T("settings.meta.tmdb.deleteFailed"));
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
    cwRefreshTmdbMetadataState,
    cwBuildFanartPanel,
    cwRefreshFanartState,
    cwSaveFanartModal,
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

  document.addEventListener("click", async (ev) => {
    const btn = ev.target?.closest?.("#cw-app-refresh");
    if (!btn || btn.classList.contains("loading")) return;
    btn.classList.add("loading", "spin");
    try {
      window.invalidateConfigCache?.();
      await window.loadConfig?.();
      await window.cwAppUsersRefresh?.();
    } catch {}
    btn.classList.remove("loading", "spin");
  });
})();
