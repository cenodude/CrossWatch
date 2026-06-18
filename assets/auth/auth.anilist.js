// auth.anilist.js - AniList auth (instance-aware)
(function (w, d) {
  "use strict";

  const Shared = w.CW.AuthShared;
  const $ = Shared.el;
  const Q = (sel, root = d) => root.querySelector(sel);
  const notify = Shared.notify;
  const bust = () => `?ts=${Date.now()}`;
  const profile = Shared.createProfileAdapter({
    provider: "anilist",
    configKey: "anilist",
    label: "AniList",
    sectionId: "sec-anilist",
    selectId: "anilist_instance",
    storageKey: "cw.ui.anilist.auth.instance.v1",
    title: "Select which AniList account this config applies to.",
  });

  function isMaskedSecret(v) {
    return Shared.isMaskedSecret(v);
  }

  function markSecretField(el, value) {
    return Shared.markSecretField(el, value);
  }

  function wireSecretField(el, onChange) {
    return Shared.wireSecretInput(el, { onInput: onChange });
  }

  function readSecretField(el) {
    return Shared.readSecretField(el);
  }

  const SECTION = "#sec-anilist";
  const MAPPING_DISMISS_KEY = "cw.ui.anilist.animeMapping.dismissed.v1";
  let mappingRecommendBusy = false;
  let mappingRecommendStatus = null;
  let anilistConnected = false;

  function normalizeId(v) {
    v = String(v || "").trim();
    if (!v) return "default";
    return v.toLowerCase() === "default" ? "default" : v;
  }

  function getAniListInstance() {
    return profile ? profile.getInstance() : "default";
  }

  function setAniListInstance(v) {
    const id = normalizeId(v);
    if (profile) profile.setInstance(id);
    return id;
  }

  function anilistApi(path) {
    return profile ? profile.api(path) : String(path || "");
  }

  function computeRedirect() {
    return location.origin + "/callback/anilist";
  }

  function setAniListSuccess(on, txt) {
    return Shared.setStatusPill("anilist_msg", on ? "ok" : (txt ? "warn" : null), txt || (on ? "Connected" : ""));
  }

  function toast(message, ok = true) {
    try {
      if (typeof w.CW?.DOM?.showToast === "function") {
        w.CW.DOM.showToast(message, !!ok);
        return;
      }
    } catch {}
    notify(message);
  }

  function mappingRecommendationDismissed() {
    try { return localStorage.getItem(MAPPING_DISMISS_KEY) === "1"; } catch { return false; }
  }

  function setMappingRecommendationDismissed() {
    try { localStorage.setItem(MAPPING_DISMISS_KEY, "1"); } catch {}
  }

  function hasAniListCredentials() {
    try {
      const cidState = readSecretField($("anilist_client_id"));
      const secState = readSecretField($("anilist_client_secret"));
      return !!(cidState.hasValue && secState.hasValue);
    } catch {
      return false;
    }
  }

  function hasAniListConnection() {
    return !!anilistConnected;
  }

  function ensureMappingRecommendation() {
    const sub = Q(SECTION + ' .cw-subpanel[data-sub="auth"]');
    if (!sub) return null;

    let box = $("anilist_mapping_recommendation");
    if (box) return box;

    box = d.createElement("div");
    box.id = "anilist_mapping_recommendation";
    box.className = "anilist-mapping-rec hidden";
    box.innerHTML =
      '<div class="anilist-mapping-rec-copy">' +
        '<div class="anilist-mapping-rec-kicker">Recommended for AniList</div>' +
        '<strong>Use Anime ID Mapping</strong>' +
        '<div class="muted">Improves matching by translating AniList IDs to IDs your media servers and trackers understand.</div>' +
        '<div class="anilist-mapping-rec-state" id="anilist_mapping_recommendation_state"></div>' +
      '</div>' +
      '<div class="anilist-mapping-rec-actions">' +
        '<button class="btn primary" type="button" id="btn-anilist-enable-mapping">Enable Anime ID Mapping</button>' +
        '<button class="btn" type="button" id="btn-anilist-dismiss-mapping">Not now</button>' +
      '</div>';

    const controls = Q(SECTION + " .inline");
    if (controls && controls.parentNode === sub) controls.insertAdjacentElement("afterend", box);
    else sub.appendChild(box);

    const enableBtn = $("btn-anilist-enable-mapping");
    if (enableBtn && !enableBtn.__wired) {
      enableBtn.addEventListener("click", enableAnimeMappingFromAniList);
      enableBtn.__wired = true;
    }

    const dismissBtn = $("btn-anilist-dismiss-mapping");
    if (dismissBtn && !dismissBtn.__wired) {
      dismissBtn.addEventListener("click", () => {
        setMappingRecommendationDismissed();
        renderMappingRecommendation();
      });
      dismissBtn.__wired = true;
    }

    return box;
  }

  function renderMappingRecommendation(status = mappingRecommendStatus) {
    const box = ensureMappingRecommendation();
    if (!box) return;

    const connected = hasAniListConnection();
    const enabled = !!(status?.enabled || w._cfgCache?.anime_mapping?.enabled);
    const show = connected && !enabled && !mappingRecommendationDismissed();
    box.classList.toggle("hidden", !show);
    box.classList.toggle("busy", !!mappingRecommendBusy);

    const btn = $("btn-anilist-enable-mapping");
    if (btn) {
      btn.disabled = !!mappingRecommendBusy;
      btn.textContent = mappingRecommendBusy ? "Enabling..." : "Enable Anime ID Mapping";
    }

    const state = $("anilist_mapping_recommendation_state");
    if (state) {
      const err = String(status?.error || "").trim();
      state.textContent = mappingRecommendBusy ? "Downloading mapping database if needed..." : err;
      state.classList.toggle("hidden", !(mappingRecommendBusy || err));
    }
  }

  let mappingRefreshTimer = null;
  function queueMappingRecommendationRefresh(delay = 120) {
    clearTimeout(mappingRefreshTimer);
    mappingRefreshTimer = setTimeout(() => {
      refreshMappingRecommendation().catch(() => {});
    }, delay);
  }

  async function refreshMappingRecommendation() {
    ensureMappingRecommendation();
    if (!hasAniListConnection()) {
      renderMappingRecommendation();
      return null;
    }

    try {
      const r = await fetch("/api/anime-mapping/status", { cache: "no-store", credentials: "same-origin" });
      if (!r.ok) throw new Error(`Status failed (${r.status})`);
      mappingRecommendStatus = await r.json().catch(() => ({}));
    } catch (e) {
      mappingRecommendStatus = { ...(mappingRecommendStatus || {}), error: e?.message || "Could not check Anime ID Mapping status" };
    }

    renderMappingRecommendation(mappingRecommendStatus);
    return mappingRecommendStatus;
  }

  async function enableAnimeMappingFromAniList() {
    mappingRecommendBusy = true;
    renderMappingRecommendation();

    try {
      const r = await fetch("/api/anime-mapping/settings", {
        method: "POST",
        cache: "no-store",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          enabled: true,
          auto_update: true,
          provider: "anibridge",
          use_for_pairs: ["anilist"],
        }),
      });
      const data = await r.json().catch(() => ({}));
      if (!r.ok || data.ok === false) throw new Error(data.message || data.error || `Enable failed (${r.status})`);

      w._cfgCache ||= {};
      w._cfgCache.anime_mapping = data.anime_mapping || {
        ...(w._cfgCache.anime_mapping || {}),
        enabled: true,
        auto_update: true,
        provider: "anibridge",
        use_for_pairs: ["anilist"],
      };
      mappingRecommendStatus = data.status || { ...(mappingRecommendStatus || {}), enabled: true };

      try { w.cwAnimeMappingRenderStatus?.(mappingRecommendStatus); } catch {}
      try { await w.cwAnimeMappingRefreshStatus?.(); } catch {}
      toast(data.bootstrap_error ? data.bootstrap_error : "Anime ID Mapping enabled", !data.bootstrap_error);
    } catch (e) {
      mappingRecommendStatus = { ...(mappingRecommendStatus || {}), error: e?.message || "Could not enable Anime ID Mapping" };
      toast(mappingRecommendStatus.error, false);
    } finally {
      mappingRecommendBusy = false;
      renderMappingRecommendation();
      queueMappingRecommendationRefresh(0);
    }
  }

  function renderAniListHint() {
    const hint = $("anilist_hint");
    if (!hint || hint.__cwRendered) return;

    hint.innerHTML =
      'You need an AniList API key. Create one at ' +
      '<a href="https://anilist.co/settings/developer" target="_blank" rel="noreferrer">AniList Developer</a>. ' +
      'Set the Redirect URL to <code id="redirect_uri_preview_anilist"></code>.' +
      ' <button id="btn-copy-anilist-redirect" class="btn" type="button" style="margin-left:8px">Copy Redirect URL</button>';

    hint.__cwRendered = true;
  }

  async function refreshAniListInstanceOptions(preserve = true) {
    if (profile) await profile.refreshOptions(preserve);
  }


  function ensureAniListInstanceUI() {
    profile?.ensureUI(async () => {
      await hydrateFromConfig(true);
      updateAniListButtonState();
    });
  }

  function getAniListCfgBlock(cfg) {
    return profile ? profile.cfgBlock(cfg, true) : {};
  }

  async function hydrateFromConfig(force = false) {
    try {
      const cfg = await fetch("/api/config" + bust(), { cache: "no-store" }).then((r) => (r.ok ? r.json() : null));
      if (!cfg) return;

      const blk = getAniListCfgBlock(cfg);
      const cid = String(blk.client_id || "").trim();
      const sec = String(blk.client_secret || "").trim();
      const tok = String(blk.access_token || "").trim();

      const cidEl = $("anilist_client_id");
      const secEl = $("anilist_client_secret");

      if (cidEl && (force || !cidEl.value || cidEl.dataset.masked === "1")) markSecretField(cidEl, cid);
      if (secEl && (force || !secEl.value || secEl.dataset.masked === "1")) markSecretField(secEl, sec);

      anilistConnected = !!tok;
      if (tok) setAniListSuccess(true);
      else setAniListSuccess(false, "");

      updateAniListButtonState();
      queueMappingRecommendationRefresh();
    } catch {}
  }

  function updateAniListButtonState() {
    try {
      ensureAniListInstanceUI();
      renderAniListHint();

      const cidState = readSecretField($("anilist_client_id"));
      const secState = readSecretField($("anilist_client_secret"));
      const ok = cidState.hasValue && secState.hasValue;

      const btn = $("btn-connect-anilist");
      const hint = $("anilist_hint");
      const rid = $("redirect_uri_preview_anilist");

      if (rid) {
        const next = computeRedirect();
        if (rid.textContent !== next) rid.textContent = next;
      }
      if (btn) btn.disabled = !ok;
      if (hint) hint.classList.toggle("hidden", ok);
      renderMappingRecommendation();
    } catch (e) {
      console.warn("updateAniListButtonState failed", e);
    }
  }

  function initAniListAuthUI() {
    ensureAniListInstanceUI();
    renderAniListHint();

    const cid = $("anilist_client_id");
    const sec = $("anilist_client_secret");

    if (cid && !cid.__cwBound) {
      wireSecretField(cid, updateAniListButtonState);
      cid.__cwBound = true;
    }
    if (sec && !sec.__cwBound) {
      wireSecretField(sec, updateAniListButtonState);
      sec.__cwBound = true;
    }

    const copyBtn = $("btn-copy-anilist-redirect");
    if (copyBtn && !copyBtn.__wired) { copyBtn.addEventListener("click", copyAniListRedirect); copyBtn.__wired = true; }
    const connectBtn = $("btn-connect-anilist");
    if (connectBtn && !connectBtn.__wired) { connectBtn.addEventListener("click", startAniList); connectBtn.__wired = true; }
    const deleteBtn = $("btn-delete-anilist");
    if (deleteBtn && !deleteBtn.__wired) { deleteBtn.addEventListener("click", anilistDeleteToken); deleteBtn.__wired = true; }

    updateAniListButtonState();
    queueMappingRecommendationRefresh();
  }

  async function copyAniListRedirect() {
    const uri = computeRedirect();
    return Shared.copyText(uri, $("btn-copy-anilist-redirect"), { successMessage: "Redirect URL copied" });
  }

  async function anilistDeleteToken() {
    const btn = Q(SECTION + " .btn.danger");
    const msg = $("anilist_msg");

    if (btn) {
      btn.disabled = true;
      btn.classList.add("busy");
    }
    if (msg) {
      msg.classList.remove("hidden", "ok", "warn");
      msg.textContent = "";
    }

    try {
      const r = await fetch(anilistApi("/api/anilist/token/delete"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
        cache: "no-store",
      });
      const j = await r.json().catch(() => ({}));

      if (r.ok && j.ok !== false) {
        if (msg) {
          msg.classList.add("warn");
          msg.textContent = "Disconnected";
        }
        notify("AniList disconnected");
        try { w.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
      } else {
        if (msg) {
          msg.classList.add("warn");
          msg.textContent = "Could not disconnect";
        }
      }
    } catch {
      if (msg) {
        msg.classList.add("warn");
        msg.textContent = "Could not disconnect";
      }
    } finally {
      if (btn) {
        btn.disabled = false;
        btn.classList.remove("busy");
      }
      try { setAniListSuccess(false, ""); } catch {}
      anilistConnected = false;
      queueMappingRecommendationRefresh();
    }
  }

  let pollHandle = null;
  async function startAniList() {
    try { setAniListSuccess(false, ""); } catch {}

    const cidState = readSecretField($("anilist_client_id"));
    const secState = readSecretField($("anilist_client_secret"));
    if (!cidState.hasValue || !secState.hasValue) return;

    const payload = {};
    if (cidState.value) payload.client_id = cidState.value;
    if (secState.value) payload.client_secret = secState.value;

    // Save only explicit edits
    if (Object.keys(payload).length) {
      try {
        await fetch(anilistApi("/api/anilist/save"), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
          cache: "no-store",
        });
      } catch {}
    }

    const j = await fetch(anilistApi("/api/anilist/authorize"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ origin: location.origin }),
      cache: "no-store",
    })
      .then((r) => r.json())
      .catch(() => null);

    if (!j?.ok || !j.authorize_url) return;
    w.open(j.authorize_url, "_blank");

    if (pollHandle) {
      clearTimeout(pollHandle);
      pollHandle = null;
    }

    const deadline = Date.now() + 120000;
    const back = [1000, 2500, 5000, 7500, 10000, 15000, 20000, 20000];
    let i = 0;

    const poll = async () => {
      if (Date.now() >= deadline) {
        pollHandle = null;
        return;
      }

      const settingsVisible = !!($("page-settings") && !$("page-settings").classList.contains("hidden"));
      if (d.hidden || !settingsVisible) {
        pollHandle = setTimeout(poll, 5000);
        return;
      }

      let cfg = null;
      try { cfg = await fetch("/api/config" + bust(), { cache: "no-store" }).then((r) => r.json()); } catch {}

      const blk = getAniListCfgBlock(cfg || {});
      const tok = String(blk?.access_token || "").trim();
      if (tok) {
        anilistConnected = true;
        setAniListSuccess(true);

        pollHandle = null;
        queueMappingRecommendationRefresh(0);
        try { w.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
        return;
      }

      pollHandle = setTimeout(poll, back[Math.min(i++, back.length - 1)]);
    };

    pollHandle = setTimeout(poll, 1000);
  }

  let __anilistInitDone = false;
  function initAniListAuthLoader() {
    try { initAniListAuthUI(); } catch (_) {}

    if (__anilistInitDone) return;
    __anilistInitDone = true;

    try { hydrateFromConfig(true); } catch (_) {}
  }

  if (d.readyState === "loading") d.addEventListener("DOMContentLoaded", initAniListAuthLoader, { once: true });
  else initAniListAuthLoader();

  w.cwAuth = w.cwAuth || {};
  w.cwAuth.anilist = w.cwAuth.anilist || {};
  w.cwAuth.anilist.init = initAniListAuthLoader;

  Object.assign(w, {
    setAniListSuccess,
    updateAniListButtonState,
    initAniListAuthUI,
    startAniList,
    copyAniListRedirect,
    anilistDeleteToken,
    refreshAniListMappingRecommendation: refreshMappingRecommendation,
  });
})(window, document);
