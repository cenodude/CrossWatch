/* assets/helpers/settings-save.js */
/* refactored */
/* settings save logic */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const _cwJSONHeaders = { "Content-Type": "application/json" };
const _cwSecretIds = [
  "plex_token", "plex_home_pin", "simkl_client_id", "simkl_client_secret",
  "trakt_client_id", "trakt_client_secret", "anilist_client_id", "anilist_client_secret",
  "tmdb_api_key", "mdblist_key"
];

function _cwEl(id) { return document.getElementById(id); }
function _getVal(id) { return _cwNorm(_cwEl(id)?.value); }
function _cwApi() { return window.CW?.API || null; }
function _cwFn(name, root = globalThis) { const fn = root?.[name]; return typeof fn === "function" ? fn : null; }
function _cwLater(fn) { queueMicrotask(() => Promise.resolve().then(fn).catch(() => {})); }

async function _cwRequest(url, opt = {}, ms = 9000) {
  const api = _cwApi(), req = { cache: "no-store", ...opt };
  return typeof api?.f === "function" ? api.f(url, req, ms) : fetch(url, req);
}

async function _cwReadBody(resp) {
  try { return (resp?.headers?.get?.("content-type") || "").includes("json") ? await resp.json() : await resp.text(); }
  catch { return null; }
}

async function _cwGetConfigFresh() {
  const api = _cwApi();
  if (typeof api?.Config?.load === "function") return api.Config.load(true);
  const resp = await _cwRequest("/api/config");
  if (!resp.ok) throw new Error(`GET /api/config ${resp.status}`);
  return _cwReadBody(resp);
}

async function _cwSaveConfig(cfg) {
  const api = _cwApi(), out = cfg || {};
  if (typeof api?.Config?.save === "function") return api.Config.save(out);
  const resp = await _cwRequest("/api/config", { method: "POST", headers: _cwJSONHeaders, body: JSON.stringify(out) });
  if (!resp.ok) throw new Error(`POST /api/config ${resp.status}`);
  return _cwReadBody(resp);
}

function _cwSetConfigCache(cfg) {
  try {
    const fn = window.CW?.Cache?.setCfg;
    if (typeof fn === "function") return fn(cfg);
  } catch {}
  try { window._cfgCache = JSON.parse(JSON.stringify(cfg)); } catch { window._cfgCache = cfg; }
}

function _cwInvalidateCaches(keys) {
  try { window.CW?.Cache?.invalidate?.(keys); } catch {}
}

function _cwNorm(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  try { return String(v).trim(); } catch { return ""; }
}

function _cwTruthy(v) {
  return ["true", "1", "yes", "on", "enabled", "enable"].includes(_cwNorm(v).toLowerCase());
}

function _cwNormInst(v) {
  const s = _cwNorm(v);
  return s && s.toLowerCase() !== "default" ? s : "default";
}

function _cwSelectedInst(provider, storageKey = "") {
  try { return _cwNormInst(_cwEl(`${provider}_instance`)?.value || (storageKey ? localStorage.getItem(storageKey) : "") || "default"); }
  catch { return "default"; }
}

function _cwInstBlock(root, inst) {
  const base = root && typeof root === "object" ? root : {};
  return inst === "default" ? base : (base.instances?.[inst] && typeof base.instances[inst] === "object" ? base.instances[inst] : {});
}

function _cwEnsureInstBlock(root, inst) {
  const base = root && typeof root === "object" ? root : {};
  if (inst === "default") return base;
  if (!base.instances || typeof base.instances !== "object") base.instances = {};
  if (!base.instances[inst] || typeof base.instances[inst] !== "object") base.instances[inst] = {};
  return base.instances[inst];
}

function _cwApplySecret(target, key, change, clearValue) {
  if (!change?.changed || !target || !key) return;
  if (change.clear) clearValue !== undefined ? (target[key] = clearValue) : delete target[key];
  else target[key] = change.set;
}

function _cwTrustedProxiesEl() {
  return _cwEl("trusted_proxies") || _cwEl("trusted_reverse_proxies") || _cwEl("security_trusted_proxies");
}

function _cwReadFirst(...ids) {
  for (const id of ids) {
    const v = _cwNorm(_cwEl(id)?.value);
    if (v) return v;
  }
  return "";
}

function _cwSameList(a, b, numeric = false) {
  const cast = numeric ? Number : String;
  const A = (a || []).map(cast).filter((v) => numeric ? Number.isFinite(v) : !!v);
  const B = (b || []).map(cast).filter((v) => numeric ? Number.isFinite(v) : !!v);
  (numeric ? [A.sort((x, y) => x - y), B.sort((x, y) => x - y)] : [A.sort(), B.sort()]);
  return A.length === B.length && A.every((v, i) => v === B[i]);
}

function _cwReadLibrarySource(prefix, numeric = false) {
  const cast = (v) => {
    const raw = _cwNorm(v);
    if (!raw) return null;
    if (!numeric) return raw;
    const n = parseInt(raw, 10);
    return Number.isFinite(n) ? n : null;
  };
  const readRows = (rootSelector, rowCls, dotCls) => {
    const rows = document.querySelectorAll(`${rootSelector} .${rowCls}`);
    if (!rows.length) return null;
    const out = { H: [], R: [], S: [] };
    rows.forEach((row) => {
      const id = cast(row.dataset.id);
      if (id == null) return;
      if (row.querySelector(`.${dotCls}hist.on`)) out.H.push(id);
      if (row.querySelector(`.${dotCls}rate.on`)) out.R.push(id);
      if (row.querySelector(`.${dotCls}scr.on`)) out.S.push(id);
    });
    return out;
  };
  const readSelect = (key) => {
    const el = document.querySelector(`#${prefix}_lib_${key}`);
    if (!el) return null;
    const opts = el.selectedOptions ? Array.from(el.selectedOptions) : Array.from(el.querySelectorAll("option:checked"));
    return opts.map((o) => cast(o.value || o.dataset.value || o.textContent)).filter((v) => v != null);
  };
  return readRows(`#${prefix}_lib_matrix`, "lm-row", "lm-dot.")
    || readRows(`#${prefix}_lib_whitelist`, "whrow", "whtog.")
    || { H: readSelect("history"), R: readSelect("ratings"), S: readSelect("scrobble") };
}

function _cwApplyLibraryConfig(target, prev, src, numeric = false) {
  if (!target || !src) return false;
  let dirty = false;
  for (const [shortKey, longKey] of [["H", "history"], ["R", "ratings"], ["S", "scrobble"]]) {
    const nextVals = src[shortKey] || [], prevVals = prev?.[longKey]?.libraries || [];
    if (_cwSameList(nextVals, prevVals, numeric)) continue;
    target[longKey] = { ...(target[longKey] || {}), libraries: nextVals };
    dirty = true;
  }
  return dirty;
}

function _cwHydrated(prefix, sectionId, ...flags) {
  return flags.some(Boolean)
    || _cwEl(sectionId)?.dataset?.hydrated === "1"
    || document.querySelectorAll(`#${prefix}_lib_matrix .lm-row`).length > 0
    || document.querySelectorAll(`#${prefix}_lib_whitelist .whrow`).length > 0
    || !!document.querySelector(`#${prefix}_lib_history option, #${prefix}_lib_ratings option, #${prefix}_lib_scrobble option`);
}

function _cwToNumList(xs) {
  return (Array.isArray(xs) ? xs : xs instanceof Set ? Array.from(xs) : []).map((x) => parseInt(String(x), 10)).filter(Number.isFinite);
}

function _cwSelectNums(id) {
  const el = _cwEl(id);
  return el?.selectedOptions ? Array.from(el.selectedOptions).map((o) => parseInt(String(o.value), 10)).filter(Number.isFinite) : null;
}

function _cwEnsureStyle(id, css) {
  if (_cwEl(id)) return;
  try {
    const style = document.createElement("style");
    style.id = id;
    style.textContent = css;
    document.head.appendChild(style);
  } catch {}
}

function _cwEnsureSaveToast() {
  let el = document.querySelector(".save-toast");
  const inline = _cwEl("save_msg");
  if (!el && inline && !inline.closest("#save-fab")) el = inline;
  if (el) return el;
  try {
    _cwEnsureStyle("cw-save-toast-style", ".save-toast{position:fixed;left:50%;bottom:18px;transform:translateX(-50%);z-index:9999;max-width:calc(100vw - 24px);padding:10px 14px;border-radius:999px;backdrop-filter:blur(10px);background:rgba(20,20,30,.82);border:1px solid rgba(255,255,255,.14);color:#fff;font-size:13px;line-height:1.2;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.save-toast.ok{border-color:rgba(80,220,140,.35)}.save-toast.error{border-color:rgba(255,120,120,.35)}.save-toast.hide{display:none}");
    el = document.createElement("div");
    el.className = "save-toast hide";
    el.setAttribute("aria-live", "polite");
    document.body.appendChild(el);
  } catch {}
  return el;
}

function _cwShowToast(text, ok = true) {
  try {
    const fn = window.CW?.DOM?.showToast || window.showToast;
    if (typeof fn === "function") return fn(String(text || ""), ok);
  } catch {}
  const el = _cwEnsureSaveToast();
  if (!el) return console.log(text);
  el.textContent = String(text || "");
  el.classList.remove("hide", "error", "ok");
  el.classList.add(ok ? "ok" : "error");
  window.setTimeout(() => el.classList.add("hide"), 2000);
}

function _cwEnsureAuthErrorBox() {
  const host = _cwEl("app_auth_fields");
  if (!host) return null;
  let el = _cwEl("app_auth_error");
  if (el) return el;
  try {
    _cwEnsureStyle("cw-inline-error-style", ".cw-inline-error{margin-top:10px;padding:8px 10px;border-radius:12px;background:rgba(255,80,80,.08);border:1px solid rgba(255,80,80,.18);color:rgba(255,220,220,.95);font-size:12px}.cw-inline-error.hidden{display:none}.cw-invalid{border-color:rgba(255,100,100,.55)!important;box-shadow:0 0 0 2px rgba(255,80,80,.12)!important}");
    el = document.createElement("div");
    el.id = "app_auth_error";
    el.className = "cw-inline-error hidden";
    el.setAttribute("role", "alert");
    host.appendChild(el);
  } catch { return null; }
  return el;
}

function _cwSetAuthError(msg) {
  const has = !!_cwNorm(msg), box = _cwEnsureAuthErrorBox();
  for (const id of ["app_auth_password", "app_auth_password2"]) {
    try {
      const el = _cwEl(id);
      if (!el) continue;
      el.classList.toggle("cw-invalid", has);
      has ? el.setAttribute("aria-invalid", "true") : el.removeAttribute("aria-invalid");
    } catch {}
  }
  if (!box) return;
  box.textContent = has ? String(msg) : "";
  box.classList.toggle("hidden", !has);
}

function _cwAbortSave(msg) {
  const err = new Error(String(msg || "Save aborted"));
  err.__cwAbortSave = true;
  throw err;
}

function _cwWireTouched(ids = _cwSecretIds) {
  ids.forEach((id) => {
    const el = _cwEl(id);
    if (el && !el.__touchedWired) {
      el.addEventListener("input", () => { el.dataset.touched = "1"; });
      el.__touchedWired = true;
    }
  });
}

function _cwWireAuthPair() {
  const p1 = _cwEl("app_auth_password"), p2 = _cwEl("app_auth_password2");
  if (!p1 || !p2 || p1.__cwAuthPwWired) return;
  const MIN_PASSWORD_LENGTH = 8;
  const onInput = () => {
    const a = String(p1.value || ""), b = String(p2.value || "");
    const hasA = !!_cwNorm(a), hasB = !!_cwNorm(b);
    if (!hasA && !hasB) return _cwSetAuthError("");
    if (hasA && a.length < MIN_PASSWORD_LENGTH) {
      return _cwSetAuthError(`Password must be at least ${MIN_PASSWORD_LENGTH} characters`);
    }
    if (hasB && a !== b) {
      return _cwSetAuthError("Passwords do not match");
    }
    _cwSetAuthError("");
  };
  p1.addEventListener("input", onInput);
  p2.addEventListener("input", onInput);
  p1.__cwAuthPwWired = true;
}

function _cwReadSecret(id, previousValue) {
  const el = _cwEl(id);
  if (!el) return { changed: false };
  const raw = _cwNorm(el.value), masked = el.dataset?.masked === "1" || raw.startsWith("•");
  if (el.dataset?.clear === "1") return { changed: true, clear: true };
  if (el.dataset?.loaded === "0" || !el.dataset?.touched || masked) return { changed: false };
  if (!raw) return previousValue ? { changed: true, clear: true } : { changed: false };
  return raw !== previousValue ? { changed: true, set: raw } : { changed: false };
}

async function _cwSaveAppAuth(serverCfg) {
  const MIN_PASSWORD_LENGTH = 8;
  const wantEnabled = true;
  const wantUser = _getVal("app_auth_username");
  const pass1 = String(_cwEl("app_auth_password")?.value || "");
  const pass2 = String(_cwEl("app_auth_password2")?.value || "");
  const rememberEnabled = String(_cwEl("app_auth_remember_enabled")?.value || "") === "true";
  const rememberDaysEl = _cwEl("app_auth_remember_days");
  const rememberDaysRaw = String(rememberDaysEl?.value || "").trim();
  const prevEnabled = !!serverCfg?.app_auth?.enabled;
  const prevUser = _cwNorm(serverCfg?.app_auth?.username);
  const prevRememberEnabled = !!serverCfg?.app_auth?.remember_session_enabled;
  const prevRememberDays = Number.isFinite(serverCfg?.app_auth?.remember_session_days)
    ? Number(serverCfg.app_auth.remember_session_days)
    : 30;
  const wantsPwd = !!(_cwNorm(pass1) || _cwNorm(pass2));
  const rememberDaysParsed = rememberDaysRaw === "" ? NaN : parseInt(rememberDaysRaw, 10);
  const rememberDaysValid = Number.isFinite(rememberDaysParsed) && rememberDaysParsed >= 1 && rememberDaysParsed <= 365;
  const rememberDays = rememberDaysValid ? rememberDaysParsed : prevRememberDays;
  const needsCall =
    wantEnabled !== prevEnabled ||
    wantUser !== prevUser ||
    wantsPwd ||
    rememberEnabled !== prevRememberEnabled ||
    rememberDays !== prevRememberDays;

  let status = null;
  try {
    const resp = await _cwRequest("/api/app-auth/status", { credentials: "same-origin" });
    const body = await _cwReadBody(resp);
    status = resp.ok && body && typeof body === "object" ? body : null;
  } catch {}

  _cwSetAuthError("");
  try { _cwEl("app_auth_username")?.classList.remove("cw-invalid"); } catch {}

  if (wantsPwd && pass1 !== pass2) {
    _cwSetAuthError("Passwords do not match");
    _cwShowToast("Password mismatch", false);
    try { _cwEl("app_auth_password2")?.focus?.(); } catch {}
    _cwAbortSave("Password mismatch");
  }
  if (wantsPwd && pass1.length < MIN_PASSWORD_LENGTH) {
    _cwSetAuthError(`Password must be at least ${MIN_PASSWORD_LENGTH} characters`);
    _cwShowToast(`Password must be at least ${MIN_PASSWORD_LENGTH} characters`, false);
    try { _cwEl("app_auth_password")?.focus?.(); } catch {}
    _cwAbortSave("Password too short");
  }
  if (wantEnabled && !wantUser) {
    _cwShowToast("Auth username required", false);
    try { _cwEl("app_auth_username")?.classList.add("cw-invalid"); } catch {}
    _cwAbortSave("Auth username required");
  }
  if (wantEnabled && (!status?.configured || status?.reset_required) && !_cwNorm(pass1)) {
    _cwSetAuthError("Password required to save sign-in");
    _cwShowToast("Set a password to save sign-in", false);
    _cwAbortSave("Password required");
  }
  if (rememberEnabled && (!rememberDaysValid || !rememberDaysRaw)) {
    _cwSetAuthError("Session cache days must be between 1 and 365");
    _cwShowToast("Session cache days must be between 1 and 365", false);
    try { window.cwValidateAppAuthRememberDays?.(); } catch {}
    try { rememberDaysEl?.focus?.(); } catch {}
    _cwAbortSave("Invalid session cache days");
  }
  if (!needsCall) return true;

  const resp = await _cwRequest("/api/app-auth/credentials", {
    method: "POST",
    headers: _cwJSONHeaders,
    credentials: "same-origin",
    body: JSON.stringify({
      enabled: wantEnabled,
      username: wantUser,
      password: pass1 || "",
      remember_session_enabled: rememberEnabled,
      remember_session_days: rememberDays
    })
  });
  const body = await _cwReadBody(resp);
  if (!resp.ok || !body?.ok) {
    _cwShowToast(body?.error || `Auth save failed (${resp.status})`, false);
    return false;
  }
  try { _cwEl("app_auth_password").value = ""; } catch {}
  try { _cwEl("app_auth_password2").value = ""; } catch {}
  try { if (_cwFn("loadConfig")) await _cwFn("loadConfig")(); } catch {}
  return true;
}

async function saveSettings() {
  const fromFab = !!document.activeElement?.closest?.("#save-fab");
  const readToggle = (id) => _cwTruthy(_cwEl(id)?.value || "");
  let schedChanged = false;
  const schedulingSaveError = (message) => {
    const msg = _cwNorm(message) || "Fix the scheduling errors before saving.";
    try {
      window.cwSettingsSelect?.("scheduling");
      window.cwSchedSettingsSelect?.("advanced");
      const sec = document.getElementById("sec-scheduling");
      if (sec && !sec.classList.contains("open")) window.toggleSection?.("sec-scheduling");
      sec?.scrollIntoView?.({ behavior: "smooth", block: "start" });
    } catch {}
    _cwShowToast(msg, false);
    const err = new Error(msg);
    err.__cwAbortSave = true;
    throw err;
  };

  _cwWireTouched();
  _cwWireAuthPair();

  try {
    const serverCfg = await _cwGetConfigFresh();
    const cfg = JSON.parse(JSON.stringify(serverCfg || {}));
    let changed = false;

    const mark = () => { changed = true; };
    const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
    const ensureObj = (root, key) => root[key] && typeof root[key] === "object" ? root[key] : (root[key] = {});

    try { delete cfg.app_auth; } catch {}

    try {
      const ok = await _cwSaveAppAuth(serverCfg);
      if (!ok) return;
    } catch (e) {
      console.warn("saveSettings: app_auth merge failed", e);
      if (e?.__cwAbortSave) throw e;
    }

    try {
      const tpEl = _cwTrustedProxiesEl();
      if (tpEl) {
        const uniq = [];
        const seen = new Set();
        String(tpEl.value || "").split(/[;\n,]+/g).map((s) => _cwNorm(s)).filter(Boolean).forEach((s) => {
          const k = s.toLowerCase();
          if (!seen.has(k)) { seen.add(k); uniq.push(s); }
        });
        const cur = Array.isArray(cfg.security?.trusted_proxies) ? cfg.security.trusted_proxies.map(_cwNorm).filter(Boolean) : [];
        if (!same(cur, uniq)) { ensureObj(cfg, "security").trusted_proxies = uniq; mark(); }
      }
    } catch (e) {
      console.warn("saveSettings: trusted proxies merge failed", e);
    }

    const prevMode = serverCfg?.sync?.bidirectional?.mode || "two-way";
    const prevSource = serverCfg?.sync?.bidirectional?.source_of_truth || "plex";
    const prevDebug = !!serverCfg?.runtime?.debug;
    const prevDebugMods = !!serverCfg?.runtime?.debug_mods;
    const prevDebugHttp = !!serverCfg?.runtime?.debug_http;

    const debugMode = _getVal("debug");
    const [wantDebug, wantMods, wantHttp] =
      debugMode === "full" ? [true, true, true] :
      debugMode === "mods" ? [true, true, false] :
      debugMode === "on" ? [true, false, false] : [false, false, false];

    if (_getVal("mode") !== prevMode) { ensureObj(ensureObj(cfg, "sync"), "bidirectional").mode = _getVal("mode"); mark(); }
    if (_getVal("source") !== prevSource) { ensureObj(ensureObj(cfg, "sync"), "bidirectional").source_of_truth = _getVal("source"); mark(); }
    if (wantDebug !== prevDebug || wantMods !== prevDebugMods || wantHttp !== prevDebugHttp) {
      Object.assign(ensureObj(cfg, "runtime"), { debug: wantDebug, debug_mods: wantMods, debug_http: wantHttp });
      mark();
    }

    const prevMetaLocale = _cwNorm(serverCfg?.metadata?.locale);
    const prevMetaTTL = Number.isFinite(serverCfg?.metadata?.ttl_hours) ? Number(serverCfg.metadata.ttl_hours) : 72;
    const uiMetaLocale = _getVal("metadata_locale");
    const uiMetaTTL = _getVal("metadata_ttl_hours");
    if (uiMetaLocale !== prevMetaLocale) {
      const meta = ensureObj(cfg, "metadata");
      uiMetaLocale ? (meta.locale = uiMetaLocale) : delete meta.locale;
      mark();
    }
    if (uiMetaTTL !== "") {
      const ttl = parseInt(uiMetaTTL, 10);
      if (!Number.isNaN(ttl) && ttl !== prevMetaTTL) { ensureObj(cfg, "metadata").ttl_hours = Math.max(1, ttl); mark(); }
    }

    const prevUi = {
      show_watchlist_preview: typeof serverCfg?.ui?.show_watchlist_preview === "boolean" ? !!serverCfg.ui.show_watchlist_preview : true,
      show_playingcard: typeof serverCfg?.ui?.show_playingcard === "boolean" ? !!serverCfg.ui.show_playingcard : true,
      show_AI: typeof serverCfg?.ui?.show_AI === "boolean" ? !!serverCfg.ui.show_AI : true,
      show_quick_add_desktop: typeof serverCfg?.ui?.show_quick_add_desktop === "boolean" ? !!serverCfg.ui.show_quick_add_desktop : true,
      show_quick_add_mobile: typeof serverCfg?.ui?.show_quick_add_mobile === "boolean" ? !!serverCfg.ui.show_quick_add_mobile : true,
      protocol: _cwNorm(serverCfg?.ui?.protocol).toLowerCase() === "https" ? "https" : "http"
    };

    [["ui_show_watchlist_preview", "show_watchlist_preview"], ["ui_show_playingcard", "show_playingcard"], ["ui_show_AI", "show_AI"], ["ui_show_quick_add_desktop", "show_quick_add_desktop"], ["ui_show_quick_add_mobile", "show_quick_add_mobile"]].forEach(([id, key]) => {
      const el = _cwEl(id);
      if (!el) return;
      const next = el.value !== "false";
      if (next === prevUi[key]) return;
      ensureObj(cfg, "ui")[key] = next;
      if (key === "show_AI") try { window.__cwAskAiChanged = { from: prevUi.show_AI, to: next }; } catch {}
      mark();
    });

    const protoEl = _cwEl("ui_protocol");
    if (protoEl) {
      const nextProto = _cwNorm(protoEl.value).toLowerCase() === "https" ? "https" : "http";
      if (nextProto !== prevUi.protocol) {
        ensureObj(cfg, "ui").protocol = nextProto;
        try { window.__cwProtoChanged = nextProto; } catch {}
        mark();
      }
    }

    try {
      const prevCw = serverCfg?.crosswatch || {};
      const nextCw = { ...(cfg.crosswatch || {}) };
      let cwChanged = false;
      const intOr = (id, prev) => {
        const raw = _getVal(id);
        const n = parseInt(raw, 10);
        return Number.isNaN(n) ? prev : Math.max(0, n);
      };
      const setCw = (key, next, prev) => { if (next !== prev) { nextCw[key] = next; cwChanged = true; } };

      setCw("enabled", _cwEl("cw_enabled") ? _cwTruthy(_cwEl("cw_enabled").value) : (prevCw.enabled !== false), prevCw.enabled !== false);
      setCw("retention_days", intOr("cw_retention_days", Number.isFinite(prevCw.retention_days) ? Number(prevCw.retention_days) : 30), Number.isFinite(prevCw.retention_days) ? Number(prevCw.retention_days) : 30);
      setCw("auto_snapshot", _cwEl("cw_auto_snapshot") ? _cwTruthy(_cwEl("cw_auto_snapshot").value) : (prevCw.auto_snapshot !== false), prevCw.auto_snapshot !== false);
      setCw("max_snapshots", intOr("cw_max_snapshots", Number.isFinite(prevCw.max_snapshots) ? Number(prevCw.max_snapshots) : 64), Number.isFinite(prevCw.max_snapshots) ? Number(prevCw.max_snapshots) : 64);

      const prevRestore = {
        watchlist: _cwNorm(prevCw.restore_watchlist || "latest") || "latest",
        history: _cwNorm(prevCw.restore_history || "latest") || "latest",
        ratings: _cwNorm(prevCw.restore_ratings || "latest") || "latest"
      };
      ["watchlist", "history", "ratings"].forEach((key) => {
        const el = _cwEl(`cw_restore_${key}`);
        if (!el) return;
        const next = _cwNorm(el.value) || "latest";
        if (next !== prevRestore[key]) { nextCw[`restore_${key}`] = next; cwChanged = true; }
      });
      if (cwChanged) { cfg.crosswatch = nextCw; mark(); }
    } catch {}

    try {
      const secrets = {
        plex: _cwInstBlock(serverCfg?.plex, _cwSelectedInst("plex")),
        simkl: _cwInstBlock(serverCfg?.simkl, _cwSelectedInst("simkl")),
        trakt: _cwInstBlock(serverCfg?.trakt, _cwSelectedInst("trakt", "cw.ui.trakt.auth.instance.v1")),
        anilist: _cwInstBlock(serverCfg?.anilist, _cwSelectedInst("anilist")),
        mdblist: _cwInstBlock(serverCfg?.mdblist, _cwSelectedInst("mdblist"))
      };
      [
        ["mdblist", _cwSelectedInst("mdblist"), [["api_key", _cwReadSecret("mdblist_key", _cwNorm(secrets.mdblist?.api_key))]]],
        ["plex", _cwSelectedInst("plex"), [["account_token", _cwReadSecret("plex_token", _cwNorm(secrets.plex?.account_token))], ["home_pin", _cwReadSecret("plex_home_pin", _cwNorm(secrets.plex?.home_pin)), ""]]],
        ["simkl", _cwSelectedInst("simkl"), [["client_id", _cwReadSecret("simkl_client_id", _cwNorm(secrets.simkl?.client_id))], ["client_secret", _cwReadSecret("simkl_client_secret", _cwNorm(secrets.simkl?.client_secret))]]],
        ["trakt", _cwSelectedInst("trakt", "cw.ui.trakt.auth.instance.v1"), [["client_id", _cwReadSecret("trakt_client_id", _cwNorm(secrets.trakt?.client_id))], ["client_secret", _cwReadSecret("trakt_client_secret", _cwNorm(secrets.trakt?.client_secret))]]],
        ["anilist", _cwSelectedInst("anilist"), [["client_id", _cwReadSecret("anilist_client_id", _cwNorm(secrets.anilist?.client_id))], ["client_secret", _cwReadSecret("anilist_client_secret", _cwNorm(secrets.anilist?.client_secret))]]],
        ["tmdb", "default", [["api_key", _cwReadSecret("tmdb_api_key", _cwNorm(serverCfg?.tmdb?.api_key))]]]
      ].forEach(([rootKey, inst, fields]) => {
        const changes = fields.filter(([, ch]) => ch?.changed);
        if (!changes.length) return;
        cfg[rootKey] = cfg[rootKey] && typeof cfg[rootKey] === "object" ? cfg[rootKey] : {};
        const target = _cwEnsureInstBlock(cfg[rootKey], inst);
        changes.forEach(([prop, ch, clearValue]) => _cwApplySecret(target, prop, ch, clearValue));
        mark();
      });
    } catch (e) {
      console.warn("saveSettings: secret merge failed", e);
    }

    try {
      const inst = _cwNormInst(_cwEl("jellyfin_instance")?.value || "");
      const prev = _cwInstBlock(serverCfg?.jellyfin, inst);
      cfg.jellyfin = cfg.jellyfin && typeof cfg.jellyfin === "object" ? cfg.jellyfin : {};
      const next = _cwEnsureInstBlock(cfg.jellyfin, inst);
      const updates = {
        server: _cwReadFirst("jfy_server_url", "jfy_server"),
        username: _cwReadFirst("jfy_username", "jfy_user"),
        user_id: _cwReadFirst("jfy_user_id"),
        verify_ssl: !!(_cwEl("jfy_verify_ssl")?.checked || _cwEl("jfy_verify_ssl_dup")?.checked)
      };
      if (updates.server && updates.server !== _cwNorm(prev?.server)) { next.server = updates.server; mark(); }
      if (updates.username && updates.username !== _cwNorm(prev?.username || prev?.user)) { next.username = next.user = updates.username; mark(); }
      if (updates.user_id && updates.user_id !== _cwNorm(prev?.user_id)) { next.user_id = updates.user_id; mark(); }
      if (updates.verify_ssl !== !!prev?.verify_ssl) { next.verify_ssl = updates.verify_ssl; mark(); }
      const src = _cwHydrated("jfy", "sec-jellyfin", window.__jellyfinHydrated === true, window.__jfyHydrated === true) ? _cwReadLibrarySource("jfy") : null;
      if (_cwApplyLibraryConfig(next, prev, src)) mark();
    } catch (e) {
      console.warn("saveSettings: jellyfin merge failed", e);
    }

    try {
      const inst = _cwNormInst(_cwEl("emby_instance")?.value || "");
      const prev = _cwInstBlock(serverCfg?.emby, inst);
      cfg.emby = cfg.emby && typeof cfg.emby === "object" ? cfg.emby : {};
      const next = _cwEnsureInstBlock(cfg.emby, inst);
      const src = _cwHydrated("emby", "sec-emby", window.__embyHydrated === true) ? _cwReadLibrarySource("emby") : null;
      if (_cwApplyLibraryConfig(next, prev, src)) mark();
    } catch (e) {
      console.warn("saveSettings: emby merge failed", e);
    }

    try {
      const inst = _cwNormInst(_cwEl("plex_instance")?.value || "");
      const prev = _cwInstBlock(serverCfg?.plex, inst);
      cfg.plex = cfg.plex && typeof cfg.plex === "object" ? cfg.plex : {};
      const next = _cwEnsureInstBlock(cfg.plex, inst);
      const uiAid = (() => {
        const n = parseInt(_getVal("plex_account_id"), 10);
        return Number.isFinite(n) && n > 0 ? n : null;
      })();
      const prevAid = (() => {
        const n = parseInt(_cwNorm(prev?.account_id), 10);
        return Number.isFinite(n) && n > 0 ? n : null;
      })();
      if (_getVal("plex_server_url") && _getVal("plex_server_url") !== _cwNorm(prev?.server_url)) { next.server_url = _getVal("plex_server_url"); mark(); }
      if (_getVal("plex_username") && _getVal("plex_username") !== _cwNorm(prev?.username)) { next.username = _getVal("plex_username"); mark(); }
      if (uiAid !== null && uiAid !== prevAid) { next.account_id = uiAid; mark(); }
      if (!!_cwEl("plex_verify_ssl")?.checked !== !!prev?.verify_ssl) { next.verify_ssl = !!_cwEl("plex_verify_ssl")?.checked; mark(); }
      if (_cwHydrated("plex", "sec-plex", window.__plexHydrated === true)) {
        const st = window.__plexState || { hist: new Set(), rate: new Set(), scr: new Set() };
        const src = {
          H: _cwSelectNums("plex_lib_history") ?? _cwToNumList(st.hist),
          R: _cwSelectNums("plex_lib_ratings") ?? _cwToNumList(st.rate),
          S: _cwSelectNums("plex_lib_scrobble") ?? _cwToNumList(st.scr)
        };
        if (_cwApplyLibraryConfig(next, prev, src, true)) mark();
      }
    } catch (e) {
      console.warn("saveSettings: plex merge failed", e);
    }

    try {
      if (_cwFn("getScrobbleConfig", window)) {
        const prev = serverCfg?.scrobble || {};
        const next = window.getScrobbleConfig(prev) || {};
        if (!same(next, prev)) { cfg.scrobble = next; mark(); }
      }
    } catch (e) {
      console.warn("saveSettings: scrobbler merge failed", e);
    }

    try {
      let sched = {
            enabled: readToggle("schEnabled"),
            mode: _getVal("schMode"),
            every_n_hours: parseInt(_getVal("schN") || "12", 10),
            daily_time: _getVal("schTime") || "03:30",
            custom_interval_minutes: Math.max(
              15,
              (_getVal("schCustomUnit") || "minutes") === "hours"
                ? ((parseInt(_getVal("schCustomValue") || "1", 10) || 1) * 60)
                : (parseInt(_getVal("schCustomValue") || "60", 10) || 60)
            ),
            advanced: { enabled: false, jobs: [] }
          };
      if (_cwFn("getSchedulingPatch", window)) {
        const validation = _cwFn("getSchedulingValidation", window)?.() || {};
        const issues = Array.isArray(validation.issues) ? validation.issues.filter(Boolean) : [];
        if (issues.length) schedulingSaveError(issues[0]);
        sched = window.getSchedulingPatch({ strict: true }) || sched;
      }
      if (!same(sched, serverCfg?.scheduling || {})) {
        cfg.scheduling = sched;
        schedChanged = true;
        mark();
      }
    } catch (e) {
      if (e?.__cwAbortSave) throw e;
      console.warn("saveSettings: scheduling merge failed", e);
    }

    if (changed) {
      await _cwSaveConfig(cfg);
      _cwSetConfigCache(cfg);
      const nextAppDebug = !!(cfg?.runtime?.debug || cfg?.runtime?.debug_mods);
      const prevAppDebugLive = !!window.appDebug;
      window.appDebug = nextAppDebug;
      if (prevAppDebugLive !== nextAppDebug) {
        _cwLater(() => {
          const details = document.getElementById("details");
          if (details && !details.classList.contains("hidden")) {
            try { window.openDetailsLog?.(); } catch {}
          }
        });
      }
      try { _cwFn("_invalidatePairsCache")?.(); } catch {}
      _cwLater(() => _cwFn("loadConfig")?.());

      if (schedChanged) {
        _cwLater(() => _cwRequest("/api/scheduling", { method: "POST", headers: _cwJSONHeaders, body: JSON.stringify(cfg.scheduling) }).then(() => _cwInvalidateCaches(["schedulingStatus"])).catch((e) => console.warn("POST /api/scheduling failed", e)));
      } else {
        _cwLater(() => {
          const sc = cfg?.scheduling || window._cfgCache?.scheduling;
          if (!(sc && (sc.enabled || sc.advanced?.enabled))) return;
          _cwRequest("/api/scheduling/replan_now", { method: "POST" }).then(() => _cwInvalidateCaches(["schedulingStatus"])).catch(() => {});
        });
      }
    }

    try {
      const cached = _cwFn("loadStatusCache")?.();
      if (cached?.providers) _cwFn("renderConnectorStatus")?.(cached.providers, { stale: true });
      _cwLater(() => _cwFn("refreshStatus")?.(true));
    } catch {}

    ["updateTmdbHint", "updateSimklState", "updateJellyfinState", "updateTraktHint", "updatePreviewVisibility"].forEach((name) => {
      try { _cwFn(name)?.(); } catch {}
    });

    if (schedChanged) {
      try {
        if (_cwFn("loadScheduling", window)) _cwLater(() => window.loadScheduling());
      } catch (e) {
        console.warn("loadScheduling failed:", e);
      }
    }

    try { window.dispatchEvent(new CustomEvent("settings-changed", { detail: { scope: "settings", reason: "save" } })); } catch {}
    try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    try { document.dispatchEvent(new CustomEvent("config-saved", { detail: { section: "scheduling" } })); } catch {}
    try { document.dispatchEvent(new Event("scheduling-status-refresh")); } catch {}
    try { _cwLater(() => window.refreshSchedulingBanner?.()); } catch {}
    try { window.refreshSettingsInsight?.(); } catch {}

    if (!fromFab) _cwShowToast("Settings saved", true);

    try {
      const reasons = [];
      let kind = "", applyText = "Restart NOW";
      const wantProto = _cwNorm(window.__cwProtoChanged).toLowerCase();
      if (wantProto) {
        try { delete window.__cwProtoChanged; } catch {}
        try { window.cwQueueProtocolApply?.(wantProto, window.cwBuildProtoUrl?.(wantProto)); } catch {}
        reasons.push("Protocol changed");
        kind = "protocol";
        applyText = "Apply NOW";
      }
      if (window.__cwAskAiChanged) {
        const info = window.__cwAskAiChanged;
        try { delete window.__cwAskAiChanged; } catch {}
        reasons.push(`ASK AI ${info?.to ? "shown" : "hidden"}`);
        if (!kind) kind = "restart";
      }
      if (reasons.length) {
        const msg = `${reasons.join(" + ")}: restart required`;
        try { window.cwShowRestartBanner?.(msg, { showApply: true, applyText, kind }); } catch {}
        _cwShowToast(msg, true);
      }
    } catch {}
  } catch (err) {
    console.error("saveSettings failed", err);
    if (err?.__cwAbortSave) throw err;
    if (err && typeof err === "object" && typeof err.message === "string" && err.message.trim()) {
      _cwShowToast(err.message.trim(), false);
      throw err;
    }
    _cwShowToast("Save failed — see console", false);
    throw err;
  }
}

try { window.saveSettings = saveSettings; } catch {}
