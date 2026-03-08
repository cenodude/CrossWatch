/* assets/helpers/settings-save.js */
/* Refactored and expanded settings save logic */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
function _getVal(id) {
  const el = document.getElementById(id);
  return (el && typeof el.value === "string" ? el.value : "").trim();
}

const _cwJSONHeaders = { "Content-Type": "application/json" };

function _cwApi() {
  return window.CW?.API || null;
}

async function _cwRequest(url, opt = {}, ms = 9000) {
  const api = _cwApi();
  const req = { cache: "no-store", ...opt };
  if (typeof api?.f === "function") return api.f(url, req, ms);
  return fetch(url, req);
}

async function _cwReadBody(resp) {
  const ct = resp?.headers?.get?.("content-type") || "";
  try {
    return ct.includes("json") ? await resp.json() : await resp.text();
  } catch {
    return null;
  }
}

async function _cwGetConfigFresh() {
  const api = _cwApi();
  if (typeof api?.Config?.load === "function") return api.Config.load(true);
  const resp = await _cwRequest("/api/config");
  if (!resp.ok) throw new Error(`GET /api/config ${resp.status}`);
  return _cwReadBody(resp);
}

async function _cwSaveConfig(cfg) {
  const out = cfg || {};
  const api = _cwApi();
  if (typeof api?.Config?.save === "function") return api.Config.save(out);
  const resp = await _cwRequest("/api/config", {
    method: "POST",
    headers: _cwJSONHeaders,
    body: JSON.stringify(out),
  });
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
  try {
    const fn = window.CW?.Cache?.invalidate;
    if (typeof fn === "function") fn(keys);
  } catch {}
}


function _cwNorm(v) {
  if (v === null || v === undefined) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number" || typeof v === "boolean" || typeof v === "bigint") return String(v).trim();
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
  try {
    const el = document.getElementById(`${provider}_instance`);
    return _cwNormInst((el && el.value) || (storageKey ? localStorage.getItem(storageKey) : "") || "default");
  } catch {
    return "default";
  }
}

function _cwInstBlock(root, inst) {
  const base = root && typeof root === "object" ? root : {};
  if (inst === "default") return base;
  return base.instances && typeof base.instances === "object" && base.instances[inst] && typeof base.instances[inst] === "object"
    ? base.instances[inst]
    : {};
}

function _cwEnsureInstBlock(root, inst) {
  const base = root && typeof root === "object" ? root : {};
  if (inst === "default") return base;
  if (!base.instances || typeof base.instances !== "object") base.instances = {};
  if (!base.instances[inst] || typeof base.instances[inst] !== "object") base.instances[inst] = {};
  return base.instances[inst];
}

function _cwApplySecret(target, key, change, clearValue) {
  if (!change?.changed || !target || !key) return false;
  if (change.clear) {
    if (clearValue !== undefined) target[key] = clearValue;
    else delete target[key];
  } else {
    target[key] = change.set;
  }
  return true;
}

function _cwReadFirst(...ids) {
  for (const id of ids) {
    const el = document.getElementById(id);
    const v = _cwNorm(el && el.value);
    if (v) return v;
  }
  return "";
}

function _cwSameList(a, b, numeric = false) {
  const cast = numeric ? Number : String;
  const sort = numeric ? (x, y) => x - y : undefined;
  const A = (a || []).map(cast).filter((v) => (numeric ? Number.isFinite(v) : !!v));
  const B = (b || []).map(cast).filter((v) => (numeric ? Number.isFinite(v) : !!v));
  if (sort) { A.sort(sort); B.sort(sort); } else { A.sort(); B.sort(); }
  if (A.length !== B.length) return false;
  for (let i = 0; i < A.length; i++) if (A[i] !== B[i]) return false;
  return true;
}

function _cwReadLibrarySource(prefix, numeric = false) {
  const cast = (v) => {
    const raw = _cwNorm(v);
    if (!raw) return null;
    if (!numeric) return raw;
    const n = parseInt(raw, 10);
    return Number.isFinite(n) ? n : null;
  };
  const readRows = (rootSelector, togglePrefix) => {
    const rows = document.querySelectorAll(`${rootSelector} .${rootSelector.includes('matrix') ? 'lm-row' : 'whrow'}`);
    if (!rows.length) return null;
    const out = { H: [], R: [], S: [] };
    rows.forEach((row) => {
      const id = cast(row.dataset.id);
      if (id === null) return;
      if (row.querySelector(`.${togglePrefix}hist.on`)) out.H.push(id);
      if (row.querySelector(`.${togglePrefix}rate.on`)) out.R.push(id);
      if (row.querySelector(`.${togglePrefix}scr.on`)) out.S.push(id);
    });
    return out;
  };
  const readSelect = (key) => {
    const el = document.querySelector(`#${prefix}_lib_${key}`);
    if (!el) return null;
    const opts = el.selectedOptions ? Array.from(el.selectedOptions) : Array.from(el.querySelectorAll('option:checked'));
    return opts.map((o) => cast(o.value || o.dataset.value || o.textContent)).filter((v) => v !== null);
  };
  return readRows(`#${prefix}_lib_matrix`, 'lm-dot.')
    || readRows(`#${prefix}_lib_whitelist`, 'whtog.')
    || { H: readSelect('history'), R: readSelect('ratings'), S: readSelect('scrobble') };
}

function _cwApplyLibraryConfig(target, prev, src, numeric = false) {
  if (!target || !src) return false;
  let changed = false;
  const map = [['H', 'history'], ['R', 'ratings'], ['S', 'scrobble']];
  for (const [shortKey, longKey] of map) {
    const nextVals = src[shortKey] || [];
    const prevVals = prev?.[longKey]?.libraries || [];
    if (_cwSameList(nextVals, prevVals, numeric)) continue;
    target[longKey] = Object.assign({}, target[longKey] || {}, { libraries: nextVals });
    changed = true;
  }
  return changed;
}

async function saveSettings() {
  let schedChanged = false;
  const fromFab = (() => {
    const ae = document.activeElement;
    return !!(ae && typeof ae.closest === "function" && ae.closest("#save-fab"));
  })();

  const _cwEnsureSaveToast = () => {
    let el = document.querySelector(".save-toast");
    if (!el) {
      const inline = document.getElementById("save_msg");
      if (inline && !inline.closest("#save-fab")) el = inline;
    }

    if (el) return el;
    try {
      el = document.createElement("div");
      el.className = "save-toast hide";
      el.setAttribute("aria-live", "polite");
      document.body.appendChild(el);
      if (!document.getElementById("cw-save-toast-style")) {
        const style = document.createElement("style");
        style.id = "cw-save-toast-style";
        style.textContent = `
          .save-toast{position:fixed;left:50%;bottom:18px;transform:translateX(-50%);z-index:9999;max-width:calc(100vw - 24px);
            padding:10px 14px;border-radius:999px;backdrop-filter:blur(10px);background:rgba(20,20,30,.82);
            border:1px solid rgba(255,255,255,.14);color:#fff;font-size:13px;line-height:1.2;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
          .save-toast.ok{border-color:rgba(80,220,140,.35)}
          .save-toast.error{border-color:rgba(255,120,120,.35)}
          .save-toast.hide{display:none}
        `;
        document.head.appendChild(style);
      }
    } catch {}
    return el;
  };

  const _cwEnsureInlineErrorStyle = () => {
    if (document.getElementById("cw-inline-error-style")) return;
    try {
      const style = document.createElement("style");
      style.id = "cw-inline-error-style";
      style.textContent = `
        .cw-inline-error{margin-top:10px;padding:8px 10px;border-radius:12px;background:rgba(255,80,80,.08);
          border:1px solid rgba(255,80,80,.18);color:rgba(255,220,220,.95);font-size:12px}
        .cw-inline-error.hidden{display:none}
        .cw-invalid{border-color:rgba(255,100,100,.55)!important;box-shadow:0 0 0 2px rgba(255,80,80,.12)!important}
      `;
      document.head.appendChild(style);
    } catch {}
  };

  const _cwEnsureAuthInlineError = () => {
    const host = document.getElementById("app_auth_fields");
    if (!host) return null;
    let el = document.getElementById("app_auth_error");
    if (el) return el;
    try {
      _cwEnsureInlineErrorStyle();
      el = document.createElement("div");
      el.id = "app_auth_error";
      el.className = "cw-inline-error hidden";
      el.setAttribute("role", "alert");
      host.appendChild(el);
      return el;
    } catch {
      return null;
    }
  };

  const setAuthError = (msg) => {
    const p1 = document.getElementById("app_auth_password");
    const p2 = document.getElementById("app_auth_password2");
    const has = !!(msg && String(msg).trim());
    try {
      if (p1) { p1.classList.toggle("cw-invalid", has); has ? p1.setAttribute("aria-invalid", "true") : p1.removeAttribute("aria-invalid"); }
      if (p2) { p2.classList.toggle("cw-invalid", has); has ? p2.setAttribute("aria-invalid", "true") : p2.removeAttribute("aria-invalid"); }
    } catch {}
    const el = _cwEnsureAuthInlineError();
    if (!el) return;
    if (!has) {
      el.textContent = "";
      el.classList.add("hidden");
      return;
    }
    el.textContent = String(msg);
    el.classList.remove("hidden");
  };

  const abortSave = (msg) => {
    const e = new Error(String(msg || "Save aborted"));
    // @ts-ignore
    e.__cwAbortSave = true;
    throw e;
  };

  const showToast = (text, ok = true) => {
    _cwEnsureSaveToast();
    try {
      const fn = window.CW?.DOM?.showToast || window.showToast;
      if (typeof fn === "function") return fn(String(text || ""), ok);
    } catch {}
    const el = _cwEnsureSaveToast();
    if (!el) return console.log(text);
    el.textContent = String(text || "");
    el.classList.remove("hide", "error", "ok");
    el.classList.add(ok ? "ok" : "error");
    el.classList.remove("hide");
    window.setTimeout(() => el.classList.add("hide"), 2000);
  };

  const norm = _cwNorm;
  const readToggle = (id) => _cwTruthy(document.getElementById(id)?.value || "");

  ([
    "plex_token",
    "plex_home_pin",
    "simkl_client_id",
    "simkl_client_secret",
    "trakt_client_id",
    "trakt_client_secret",
    "anilist_client_id",
    "anilist_client_secret",
    "tmdb_api_key",
    "mdblist_key"
  ]).forEach(id => {
    const el = document.getElementById(id);
    if (el && !el.__touchedWired) {
      el.addEventListener("input", () => { el.dataset.touched = "1"; });
      el.__touchedWired = true;
    }
  });

  function readSecretSafe(id, previousValue) {
    const el = document.getElementById(id);
    if (!el) return { changed: false };

    const raw = norm(el.value);
    const masked = el.dataset?.masked === "1" || raw.startsWith("•");
    const touched = el.dataset?.touched === "1";
    const explicitClear = el.dataset?.clear === "1";
    const loadedFlag = el.dataset?.loaded;

    if (explicitClear) return { changed: true, clear: true };
    if (loadedFlag === "0") return { changed: false };
    if (!touched || masked) return { changed: false };

    if (raw === "") {
      return previousValue ? { changed: true, clear: true } : { changed: false };
    }
    if (raw !== previousValue) return { changed: true, set: raw };
    return { changed: false };
  }

  try {
    const serverCfg = await _cwGetConfigFresh();
    const cfg = JSON.parse(JSON.stringify(serverCfg || {}));
    let changed = false;

    try { delete cfg.app_auth; } catch {}

    try {
      const wantEnabled = (document.getElementById("app_auth_enabled")?.value || "").toString() === "true";
      const wantUser = norm(document.getElementById("app_auth_username")?.value || "");
      const pass1 = (document.getElementById("app_auth_password")?.value || "").toString();
      const pass2 = (document.getElementById("app_auth_password2")?.value || "").toString();

      const prevEnabled = !!serverCfg?.app_auth?.enabled;
      const prevUser = norm(serverCfg?.app_auth?.username);

      let st = null;
      try {
        const r = await _cwRequest("/api/app-auth/status", { credentials: "same-origin" });
        const body = await _cwReadBody(r);
        st = r.ok && body && typeof body === "object" ? body : null;
      } catch {}

      const configured = !!(st && st.configured);
      const wantsPwd = norm(pass1) !== "" || norm(pass2) !== "";
      const needsCall = (wantEnabled !== prevEnabled) || (wantUser !== prevUser) || wantsPwd;

      try {
        const p1El = document.getElementById("app_auth_password");
        const p2El = document.getElementById("app_auth_password2");
        if (p1El && p2El && !p1El.__cwAuthPwWired) {
          const onInput = () => {
            const a = (p1El.value || "").toString();
            const b = (p2El.value || "").toString();
            if (!norm(a) && !norm(b)) { setAuthError(""); return; }
            if (a === b) setAuthError("");
          };
          p1El.addEventListener("input", onInput);
          p2El.addEventListener("input", onInput);
          p1El.__cwAuthPwWired = true;
        }
      } catch {}

      setAuthError("");
      try { document.getElementById("app_auth_username")?.classList.remove("cw-invalid"); } catch {}

      if (wantsPwd && pass1 !== pass2) {
        setAuthError("Passwords do not match");
        showToast("Password mismatch", false);
        try { document.getElementById("app_auth_password2")?.focus?.(); } catch {}
        abortSave("Password mismatch");
      }
      if (wantEnabled && !wantUser) {
        showToast("Auth username required", false);
        try { document.getElementById("app_auth_username")?.classList.add("cw-invalid"); } catch {}
        abortSave("Auth username required");
      }
      if (wantEnabled && !configured && !norm(pass1)) {
        setAuthError("Password required to enable auth");
        showToast("Set a password to enable auth", false);
        abortSave("Password required");
      }

      if (needsCall) {
        const resp = await _cwRequest("/api/app-auth/credentials", {
          method: "POST",
          headers: _cwJSONHeaders,
          credentials: "same-origin",
          body: JSON.stringify({ enabled: wantEnabled, username: wantUser, password: pass1 || "" }),
        });
        const j = await _cwReadBody(resp);
        if (!resp.ok || !j || !j.ok) {
          showToast((j && j.error) ? j.error : `Auth save failed (${resp.status})`, false);
          return;
        }
        try { document.getElementById("app_auth_password").value = ""; } catch {}
        try { document.getElementById("app_auth_password2").value = ""; } catch {}
        try { if (typeof loadConfig === "function") await loadConfig(); } catch {}
      }
    } catch (e) {
      console.warn("saveSettings: app_auth merge failed", e);
      // @ts-ignore
      if (e && e.__cwAbortSave) throw e;
    }

  // Trusted reverse proxies (optional)
  try {
    const tpEl = _cwTrustedProxiesEl();
    if (tpEl) {
      const raw = String(tpEl.value || "");
      const parts = raw.split(/[;\n,]+/g).map((s) => String(s || "").trim()).filter((s) => !!s);
      const uniq = [];
      const seen = new Set();
      parts.forEach((s) => {
        const k = s.toLowerCase();
        if (seen.has(k)) return;
        seen.add(k);
        uniq.push(s);
      });

      const cur = (cfg.security && Array.isArray(cfg.security.trusted_proxies)) ? cfg.security.trusted_proxies : [];
      const curNorm = cur.map((x) => String(x || "").trim()).filter((s) => !!s);
      if (JSON.stringify(curNorm) !== JSON.stringify(uniq)) {
        if (!cfg.security || typeof cfg.security !== "object") cfg.security = {};
        cfg.security.trusted_proxies = uniq;
        changed = true;
      }
    }
  } catch (e) {
    console.warn("saveSettings: trusted proxies merge failed", e);
  }

    const plexSecretInst    = _cwSelectedInst("plex");
    const simklSecretInst   = _cwSelectedInst("simkl");
    const traktSecretInst   = _cwSelectedInst("trakt", "cw.ui.trakt.auth.instance.v1");
    const anilistSecretInst = _cwSelectedInst("anilist");
    const mdblistSecretInst = _cwSelectedInst("mdblist");

    const prevMode     = serverCfg?.sync?.bidirectional?.mode || "two-way";
    const prevSource   = serverCfg?.sync?.bidirectional?.source_of_truth || "plex";
    const prevDebug     = !!serverCfg?.runtime?.debug;
    const prevDebugMods = !!serverCfg?.runtime?.debug_mods;
    const prevDebugHttp = !!serverCfg?.runtime?.debug_http;
    const prevPlexBlkSecrets   = _cwInstBlock(serverCfg?.plex, plexSecretInst);
    const prevSimklBlkSecrets  = _cwInstBlock(serverCfg?.simkl, simklSecretInst);
    const prevTraktBlkSecrets  = _cwInstBlock(serverCfg?.trakt, traktSecretInst);
    const prevAnilistBlkSecrets = _cwInstBlock(serverCfg?.anilist, anilistSecretInst);
    const prevMdblistBlkSecrets = _cwInstBlock(serverCfg?.mdblist, mdblistSecretInst);
    const prevPlex     = norm(prevPlexBlkSecrets?.account_token);
    const prevHomePin  = norm(prevPlexBlkSecrets?.home_pin);
    const prevAniCid = norm(prevAnilistBlkSecrets?.client_id);
    const prevAniSec = norm(prevAnilistBlkSecrets?.client_secret);
    const prevCid      = norm(prevSimklBlkSecrets?.client_id);
    const prevSec      = norm(prevSimklBlkSecrets?.client_secret);
    const prevTmdb     = norm(serverCfg?.tmdb?.api_key);
    const prevTraktCid = norm(prevTraktBlkSecrets?.client_id);
    const prevTraktSec = norm(prevTraktBlkSecrets?.client_secret);
    const prevMdbl     = norm(prevMdblistBlkSecrets?.api_key);
    const prevMetaLocale = (serverCfg?.metadata?.locale ?? "").trim();
    const prevMetaTTL    = Number.isFinite(serverCfg?.metadata?.ttl_hours) ? Number(serverCfg.metadata.ttl_hours) : 6;
    const prevUiShow     = (typeof serverCfg?.ui?.show_watchlist_preview === "boolean") ? !!serverCfg.ui.show_watchlist_preview : true;
    const prevUiPlaying  = (typeof serverCfg?.ui?.show_playingcard === "boolean") ? !!serverCfg.ui.show_playingcard : true;
    const prevUiAskAi    = (typeof serverCfg?.ui?.show_AI === "boolean") ? !!serverCfg.ui.show_AI : true;
    const prevUiProtocol = String(serverCfg?.ui?.protocol || "http").trim().toLowerCase() === "https" ? "https" : "http";

    const prevCw = serverCfg?.crosswatch || {};

    const prevCwEnabled  = (prevCw.enabled === false) ? false : true;
    const prevCwRet      = Number.isFinite(prevCw.retention_days) ? Number(prevCw.retention_days) : 30;
    const prevCwAuto     = (prevCw.auto_snapshot === false) ? false : true;
    const prevCwMax      = Number.isFinite(prevCw.max_snapshots) ? Number(prevCw.max_snapshots) : 64;
    const prevCwRestoreWatch  = (prevCw.restore_watchlist || "latest").trim();
    const prevCwRestoreHist   = (prevCw.restore_history || "latest").trim();
    const prevCwRestoreRates  = (prevCw.restore_ratings || "latest").trim();

    const uiMode   = _getVal("mode");
    const uiSource = _getVal("source");
    const uiDebugMode = _getVal("debug"); 
    let wantDebug=false, wantMods=false, wantHttp=false;
    if (uiDebugMode==='on'){wantDebug=true;}
    else if (uiDebugMode==='mods'){wantDebug=true; wantMods=true;}
    else if (uiDebugMode==='full'){wantDebug=true; wantMods=true; wantHttp=true;}

    if (uiMode !== prevMode) {
      cfg.sync = cfg.sync || {};
      cfg.sync.bidirectional = cfg.sync.bidirectional || {};
      cfg.sync.bidirectional.mode = uiMode;
      changed = true;
    }
    if (uiSource !== prevSource) {
      cfg.sync = cfg.sync || {};
      cfg.sync.bidirectional = cfg.sync.bidirectional || {};
      cfg.sync.bidirectional.source_of_truth = uiSource;
      changed = true;
    }
    if (wantDebug!==prevDebug || wantMods!==prevDebugMods || wantHttp!==prevDebugHttp) {
      cfg.runtime = cfg.runtime || {};
      cfg.runtime.debug = wantDebug;
      cfg.runtime.debug_mods = wantMods;
      cfg.runtime.debug_http = wantHttp;
      changed = true;
    }

    
    const uiMetaLocale = (document.getElementById("metadata_locale")?.value || "").trim();
    const uiMetaTTLraw = (document.getElementById("metadata_ttl_hours")?.value || "").trim();
    const uiMetaTTL    = uiMetaTTLraw === "" ? null : parseInt(uiMetaTTLraw, 10);

    if (uiMetaLocale !== prevMetaLocale) {
      cfg.metadata = cfg.metadata || {};
      if (uiMetaLocale) cfg.metadata.locale = uiMetaLocale;
      else delete cfg.metadata.locale; 
      changed = true;
    }
    if (uiMetaTTL !== null && !Number.isNaN(uiMetaTTL) && uiMetaTTL !== prevMetaTTL) {
      cfg.metadata = cfg.metadata || {};
      cfg.metadata.ttl_hours = Math.max(1, uiMetaTTL);
      changed = true;
    }

    
    (function () {
      const norm = (s) => (s ?? "").trim();
      const truthy = (v) => ["true","1","yes","on","enabled","enable"].includes(String(v).toLowerCase());
      const intOr = (el, prev) => {
        if (!el) return prev;
        const n = parseInt(norm(el.value || ""), 10);
        return Number.isNaN(n) ? prev : Math.max(0, n);
      };

      
      const uiSel = document.getElementById("ui_show_watchlist_preview");
      if (uiSel) {
        const uiShow = !truthy(uiSel.value) ? (uiSel.value === "false" ? false : true) : truthy(uiSel.value);
        const finalUiShow = uiSel.value === "false" ? false : true;
        if (finalUiShow !== prevUiShow) {
          cfg.ui = cfg.ui || {};
          cfg.ui.show_watchlist_preview = finalUiShow;
          changed = true;
        }
      }

      
      const uiPlaySel = document.getElementById("ui_show_playingcard");
      if (uiPlaySel) {
        const finalUiPlay = uiPlaySel.value === "false" ? false : true;
        if (finalUiPlay !== prevUiPlaying) {
          cfg.ui = cfg.ui || {};
          cfg.ui.show_playingcard = finalUiPlay;
          changed = true;
        }
      }

      const uiAiSel = document.getElementById("ui_show_AI");
      if (uiAiSel) {
        const finalUiAi = uiAiSel.value === "false" ? false : true;
        if (finalUiAi !== prevUiAskAi) {
          cfg.ui = cfg.ui || {};
          cfg.ui.show_AI = finalUiAi;
          changed = true;
          try { window.__cwAskAiChanged = { from: prevUiAskAi, to: finalUiAi }; } catch {}
        }
      }

      const protoSel = document.getElementById("ui_protocol");
      if (protoSel) {
        const want = String(protoSel.value || "http").trim().toLowerCase() === "https" ? "https" : "http";
        if (want !== prevUiProtocol) {
          cfg.ui = cfg.ui || {};
          cfg.ui.protocol = want;
          changed = true;
          try { window.__cwProtoChanged = want; } catch {}
        }
      }

      const cw = cfg.crosswatch || {};
      let cwChanged = false;


      
      const enabledEl = document.getElementById("cw_enabled");
      const newEnabled = enabledEl ? truthy(enabledEl.value) : prevCwEnabled;
      if (newEnabled !== prevCwEnabled) {
        cw.enabled = newEnabled;
        cwChanged = true;
      }

      
      const newRet = intOr(document.getElementById("cw_retention_days"), prevCwRet);
      if (newRet !== prevCwRet) {
        cw.retention_days = newRet;
        cwChanged = true;
      }

      
      const autoEl = document.getElementById("cw_auto_snapshot");
      const newAuto = autoEl ? truthy(autoEl.value) : prevCwAuto;
      if (newAuto !== prevCwAuto) {
        cw.auto_snapshot = newAuto;
        cwChanged = true;
      }

      
      const newMax = intOr(document.getElementById("cw_max_snapshots"), prevCwMax);
      if (newMax !== prevCwMax) {
        cw.max_snapshots = newMax;
        cwChanged = true;
      }

      
      const prevMap = {
        watchlist: prevCwRestoreWatch,
        history:   prevCwRestoreHist,
        ratings:   prevCwRestoreRates,
      };
      for (const key of ["watchlist", "history", "ratings"]) {
        const el = document.getElementById(`cw_restore_${key}`);
        if (!el) continue;
        const val = norm(el.value || "") || "latest";
        if (val !== prevMap[key]) {
          cw[`restore_${key}`] = val;
          cwChanged = true;
        }
      }

      if (cwChanged) {
        cfg.crosswatch = cw;
        changed = true;
      }
    })();

    
    const sPlex = readSecretSafe("plex_token", prevPlex);
    const sHomePin = readSecretSafe("plex_home_pin", prevHomePin);
    const sCid = readSecretSafe("simkl_client_id", prevCid);
    const sSec = readSecretSafe("simkl_client_secret", prevSec);
    const sTmdb = readSecretSafe("tmdb_api_key", prevTmdb);
    const sTrkCid = readSecretSafe("trakt_client_id", prevTraktCid);
    const sTrkSec = readSecretSafe("trakt_client_secret", prevTraktSec);
    const sMdbl = readSecretSafe("mdblist_key", prevMdbl);
    const sAniCid = readSecretSafe("anilist_client_id", prevAniCid);
    const sAniSec = readSecretSafe("anilist_client_secret", prevAniSec);

    const applySecretGroup = (rootKey, inst, fields) => {
      const changes = fields.filter(([, change]) => change?.changed);
      if (!changes.length) return;
      cfg[rootKey] = cfg[rootKey] || {};
      const target = _cwEnsureInstBlock(cfg[rootKey], inst);
      changes.forEach(([prop, change, clearValue]) => _cwApplySecret(target, prop, change, clearValue));
      changed = true;
    };

    applySecretGroup("mdblist", mdblistSecretInst, [["api_key", sMdbl]]);
    applySecretGroup("plex", plexSecretInst, [["account_token", sPlex], ["home_pin", sHomePin, ""]]);
    applySecretGroup("simkl", simklSecretInst, [["client_id", sCid], ["client_secret", sSec]]);
    applySecretGroup("trakt", traktSecretInst, [["client_id", sTrkCid], ["client_secret", sTrkSec]]);
    applySecretGroup("anilist", anilistSecretInst, [["client_id", sAniCid], ["client_secret", sAniSec]]);
    applySecretGroup("tmdb", "default", [["api_key", sTmdb]]);


    try {
      const jfyInst = _cwNormInst(document.getElementById("jellyfin_instance")?.value || "");
      const prevJfy = _cwInstBlock(serverCfg?.jellyfin, jfyInst);
      cfg.jellyfin = cfg.jellyfin && typeof cfg.jellyfin === "object" ? cfg.jellyfin : {};
      const nextJfy = _cwEnsureInstBlock(cfg.jellyfin, jfyInst);

      const uiSrv = _cwReadFirst("jfy_server_url", "jfy_server");
      const uiUser = _cwReadFirst("jfy_username", "jfy_user");
      const uiUid = _cwReadFirst("jfy_user_id");
      const uiVerify = !!(document.getElementById("jfy_verify_ssl")?.checked || document.getElementById("jfy_verify_ssl_dup")?.checked);

      if (uiSrv && uiSrv !== norm(prevJfy?.server)) { nextJfy.server = uiSrv; changed = true; }
      if (uiUser && uiUser !== norm(prevJfy?.username || prevJfy?.user)) {
        nextJfy.username = uiUser;
        nextJfy.user = uiUser;
        changed = true;
      }
      if (uiUid && uiUid !== norm(prevJfy?.user_id)) { nextJfy.user_id = uiUid; changed = true; }
      if (uiVerify !== !!prevJfy?.verify_ssl) { nextJfy.verify_ssl = uiVerify; changed = true; }

      const jfyHydrated =
        window.__jellyfinHydrated === true ||
        window.__jfyHydrated === true ||
        document.getElementById("sec-jellyfin")?.dataset?.hydrated === "1" ||
        document.querySelectorAll("#jfy_lib_matrix .lm-row").length > 0 ||
        document.querySelectorAll("#jfy_lib_whitelist .whrow").length > 0 ||
        !!document.querySelector("#jfy_lib_history option, #jfy_lib_ratings option, #jfy_lib_scrobble option");

      const src = jfyHydrated ? _cwReadLibrarySource("jfy") : null;
      if (_cwApplyLibraryConfig(nextJfy, prevJfy, src)) changed = true;
    } catch (e) {
      console.warn("saveSettings: jellyfin merge failed", e);
    }

      
    try {
      const embyInst = _cwNormInst(document.getElementById("emby_instance")?.value || "");
      const prevEmby = _cwInstBlock(serverCfg?.emby, embyInst);
      cfg.emby = cfg.emby && typeof cfg.emby === "object" ? cfg.emby : {};
      const nextEmby = _cwEnsureInstBlock(cfg.emby, embyInst);

      const embyHydrated =
        window.__embyHydrated === true ||
        document.getElementById("sec-emby")?.dataset?.hydrated === "1" ||
        document.querySelectorAll("#emby_lib_matrix .lm-row").length > 0 ||
        document.querySelectorAll("#emby_lib_whitelist .whrow").length > 0 ||
        !!document.querySelector("#emby_lib_history option, #emby_lib_ratings option, #emby_lib_scrobble option");

      const src = embyHydrated ? _cwReadLibrarySource("emby") : null;
      if (_cwApplyLibraryConfig(nextEmby, prevEmby, src)) changed = true;
    } catch (e) {
      console.warn("saveSettings: emby merge failed", e);
    }
    try {
      const instRaw = norm(document.getElementById("plex_instance")?.value || "");
      const inst = (instRaw && instRaw.toLowerCase() !== "default") ? instRaw : "default";

      const baseSrv = (serverCfg?.plex && typeof serverCfg.plex === "object") ? serverCfg.plex : {};
      const prevPlex = inst === "default"
        ? baseSrv
        : ((baseSrv.instances && typeof baseSrv.instances === "object" && baseSrv.instances[inst]) ? baseSrv.instances[inst] : {});

      const baseCfg = (cfg.plex && typeof cfg.plex === "object") ? cfg.plex : (cfg.plex = {});
      const hasPlexInstance = !!(inst !== "default" && baseCfg.instances && typeof baseCfg.instances === "object" && baseCfg.instances[inst] && typeof baseCfg.instances[inst] === "object");
      const nextPlex = inst === "default"
        ? baseCfg
        : (hasPlexInstance ? baseCfg.instances[inst] : null);

      const uiUrl  = norm(document.getElementById("plex_server_url")?.value || "");
      const uiUser = norm(document.getElementById("plex_username")?.value   || "");
      const uiAidS = norm(document.getElementById("plex_account_id")?.value || "");

      let uiAid = null;
      if (uiAidS !== "") {
        const n = parseInt(uiAidS, 10);
        uiAid = Number.isFinite(n) && n > 0 ? n : null;
      }

      const prevUrl    = norm(prevPlex?.server_url);
      const prevUser   = norm(prevPlex?.username);
      const prevAidRaw = prevPlex?.account_id;
      const prevAidS   = norm(prevAidRaw);
      const prevAidN   = (() => {
        const n = parseInt(prevAidS, 10);
        return Number.isFinite(n) && n > 0 ? n : null;
      })();

      if (nextPlex && uiUrl && uiUrl !== prevUrl) {
        nextPlex.server_url = uiUrl;
        changed = true;
      }
      if (nextPlex && uiUser && uiUser !== prevUser) {
        nextPlex.username = uiUser;
        changed = true;
      }

      if (nextPlex && uiAid !== null) {
        if (prevAidN === null || uiAid !== prevAidN) {
          nextPlex.account_id = uiAid;
          changed = true;
        }
      }

      const uiVerify = !!document.getElementById("plex_verify_ssl")?.checked;
      const prevVerify = !!(prevPlex?.verify_ssl);
      if (nextPlex && uiVerify !== prevVerify) {
        nextPlex.verify_ssl = uiVerify;
        changed = true;
      }

      const plexHydrated =
        window.__plexHydrated === true ||
        document.getElementById("sec-plex")?.dataset?.hydrated === "1" ||
        document.querySelectorAll("#plex_lib_matrix .lm-row").length > 0 ||
        document.querySelectorAll("#plex_lib_whitelist .whrow").length > 0 ||
        !!document.querySelector("#plex_lib_history option, #plex_lib_ratings option, #plex_lib_scrobble option");

      if (plexHydrated) {
        const st = (window.__plexState || { hist: new Set(), rate: new Set(), scr: new Set() });
        const toNums = (xs) =>
          (Array.isArray(xs) ? xs : xs instanceof Set ? Array.from(xs) : [])
            .map(x => parseInt(String(x), 10))
            .filter(Number.isFinite);

        const fromSelect = (id) => {
          const el = document.getElementById(id);
          if (!el || !el.selectedOptions) return null;
          return Array.from(el.selectedOptions)
            .map(o => parseInt(String(o.value), 10))
            .filter(Number.isFinite);
        };

        // Prefer selects
        const hist = fromSelect("plex_lib_history")  ?? toNums(st.hist);
        const rate = fromSelect("plex_lib_ratings") ?? toNums(st.rate);
        const scr  = fromSelect("plex_lib_scrobble")?? toNums(st.scr);

        const src = { H: hist, R: rate, S: scr };
        if (_cwApplyLibraryConfig(nextPlex, prevPlex, src, true)) changed = true;
      }
    } catch (e) {
      console.warn("saveSettings: plex merge failed", e);
    }
    
    try {
      if (typeof window.getScrobbleConfig === "function") {
        const prev = serverCfg?.scrobble || {};
        const next = window.getScrobbleConfig(prev) || {};
        if (JSON.stringify(next) !== JSON.stringify(prev)) {
          cfg.scrobble = next;
          changed = true;
        }
      }
    } catch (e) {
      console.warn("saveSettings: scrobbler merge failed", e);
    }

    
    try {
      let sched = {};
      if (typeof window.getSchedulingPatch === "function") {
        sched = window.getSchedulingPatch() || {};
      } else {
        sched = {
          enabled: readToggle("schEnabled"),
          mode: _getVal("schMode"),
          every_n_hours: parseInt((_getVal("schN") || "2"), 10),
          daily_time: _getVal("schTime") || "03:30",
          advanced: { enabled: false, jobs: [] }
        };
      }
      const prevSched = serverCfg?.scheduling || {};
      if (JSON.stringify(sched) !== JSON.stringify(prevSched)) {
        cfg.scheduling = sched;
        changed = true;
        schedChanged = true;
      }
    } catch (e) {
      console.warn("saveSettings: scheduling merge failed", e);
    }

    if (changed) {
      await _cwSaveConfig(cfg);
      _cwSetConfigCache(cfg);
      try { if (typeof _invalidatePairsCache === "function") _invalidatePairsCache(); } catch {}

      queueMicrotask(() => {
        try {
          if (typeof loadConfig === "function") {
            Promise.resolve(loadConfig()).catch(() => {});
          }
        } catch {}
      });

      if (schedChanged) {
        queueMicrotask(() => {
          _cwRequest("/api/scheduling", {
            method: "POST",
            headers: _cwJSONHeaders,
            body: JSON.stringify(cfg.scheduling),
          }).then(() => {
            _cwInvalidateCaches(["schedulingStatus"]);
          }).catch((e) => {
            console.warn("POST /api/scheduling failed", e);
          });
        });
      } else {
        queueMicrotask(() => {
          try {
            const sc = (cfg && cfg.scheduling) ? cfg.scheduling : (window._cfgCache && window._cfgCache.scheduling) ? window._cfgCache.scheduling : null;
            const effectiveEnabled = !!(sc && (sc.enabled || (sc.advanced && sc.advanced.enabled)));
            if (!effectiveEnabled) return;
          } catch {}
          _cwRequest("/api/scheduling/replan_now", { method: "POST" }).then(() => {
            _cwInvalidateCaches(["schedulingStatus"]);
          }).catch(() => {});
        });
      }
    }

    
    try {
      const cached = (typeof loadStatusCache === "function") ? loadStatusCache() : null;
      if (cached?.providers && typeof renderConnectorStatus === "function") {
        renderConnectorStatus(cached.providers, { stale: true });
      }
      if (typeof refreshStatus === "function") {
        queueMicrotask(() => { try { Promise.resolve(refreshStatus(true)).catch(() => {}); } catch {} });
      }
    } catch {}

    try { if (typeof updateTmdbHint === "function") updateTmdbHint(); } catch {}
    try { if (typeof updateSimklState === "function") updateSimklState(); } catch {}
    try { if (typeof updateJellyfinState === "function") updateJellyfinState(); } catch {}

    if (schedChanged) {
      try {
        if (typeof window.loadScheduling === "function") {
          queueMicrotask(() => {
            try { Promise.resolve(window.loadScheduling()).catch((e) => console.warn("loadScheduling failed:", e)); } catch (e) { console.warn("loadScheduling failed:", e); }
          });
        } else {
          document.dispatchEvent(new CustomEvent("config-saved", { detail: { section: "scheduling" } }));
          document.dispatchEvent(new Event("scheduling-status-refresh"));
        }
      } catch (e) {
        console.warn("loadScheduling failed:", e);
      }
    }

    try { if (typeof updateTraktHint === "function") updateTraktHint(); } catch {}
    try { if (typeof updatePreviewVisibility === "function") updatePreviewVisibility(); } catch {}

    try {
      window.dispatchEvent(new CustomEvent("settings-changed", {
        detail: { scope: "settings", reason: "save" }
      }));
    } catch {}

    try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}

    try { document.dispatchEvent(new CustomEvent("config-saved", { detail: { section: "scheduling" } })); } catch {}
    try { document.dispatchEvent(new Event("scheduling-status-refresh")); } catch {}

    try { if (typeof window.refreshSchedulingBanner === "function") queueMicrotask(() => { try { Promise.resolve(window.refreshSchedulingBanner()).catch(() => {}); } catch {} }); } catch {}
    try { if (typeof window.refreshSettingsInsight === "function") window.refreshSettingsInsight(); } catch {}

    if (!fromFab) showToast("Settings saved", true);

    (function () {
      const reasons = [];
      let kind = "";
      let applyText = "Restart NOW";

      if (window.__cwProtoChanged) {
        const wantProto = String(window.__cwProtoChanged || "").trim().toLowerCase();
        try { delete window.__cwProtoChanged; } catch {}
        const url = cwBuildProtoUrl(wantProto);
        try { cwQueueProtocolApply(wantProto, url); } catch {}
        reasons.push("Protocol changed");
        kind = "protocol";
        applyText = "Apply NOW";
      }

      let askAiInfo = null;
      if (window.__cwAskAiChanged) {
        askAiInfo = window.__cwAskAiChanged;
        try { delete window.__cwAskAiChanged; } catch {}
        try {
          const to = !!(askAiInfo && askAiInfo.to);
          reasons.push(`ASK AI ${to ? "shown" : "hidden"}`);
        } catch {
          reasons.push("ASK AI changed");
        }
        if (!kind) kind = "restart";
      }

      if (!reasons.length) return;

      const msg = `${reasons.join(" + ")}: restart required`;
      try { cwShowRestartBanner(msg, { showApply: true, applyText, kind }); } catch {}
      showToast(msg, true);
    })();
  } catch (err) {
    console.error("saveSettings failed", err);
    showToast("Save failed — see console", false);
    throw err;
  }
}

try { window.saveSettings = saveSettings; } catch (e) {}

