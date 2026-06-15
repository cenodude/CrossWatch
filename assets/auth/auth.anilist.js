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

      if (tok) setAniListSuccess(true);
      else setAniListSuccess(false, "");

      updateAniListButtonState();
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
        setAniListSuccess(true);

        pollHandle = null;
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
  });
})(window, document);
