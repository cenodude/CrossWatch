// auth.anilist.js - AniList auth (instance-aware)
(function (w, d) {
  "use strict";

  const $ = (id) => d.getElementById(id);
  const Q = (sel, root = d) => root.querySelector(sel);
  const notify = w.notify || ((m) => console.log("[notify]", m));
  const bust = () => `?ts=${Date.now()}`;

  const INST_KEY = "cw.ui.anilist.auth.instance.v1";
  const SECTION = "#sec-anilist";

  function normalizeId(v) {
    v = String(v || "").trim();
    if (!v) return "default";
    return v.toLowerCase() === "default" ? "default" : v;
  }

  function getAniListInstance() {
    const el = Q(SECTION + " #anilist_instance") || $("#anilist_instance");
    let v = el ? String(el.value || "").trim() : "";
    if (!v) {
      try { v = localStorage.getItem(INST_KEY) || ""; } catch {}
    }
    return normalizeId(v);
  }

  function setAniListInstance(v) {
    const id = normalizeId(v);
    try { localStorage.setItem(INST_KEY, id); } catch {}
    const el = Q(SECTION + " #anilist_instance") || $("#anilist_instance");
    if (el) el.value = id;
    return id;
  }

  function anilistApi(path) {
    const p = String(path || "");
    const sep = p.includes("?") ? "&" : "?";
    return p + sep + "instance=" + encodeURIComponent(getAniListInstance()) + "&ts=" + Date.now();
  }

  function computeRedirect() {
    return location.origin + "/callback/anilist";
  }

  function setAniListSuccess(on, txt) {
    const msg = $("anilist_msg");
    if (!msg) return;
    msg.classList.toggle("hidden", !on && !txt);
    msg.classList.toggle("ok", !!on);
    msg.classList.toggle("warn", !!txt && !on);
    msg.textContent = txt || (on ? "Connected." : "");
  }

  function renderAniListHint() {
    const hint = $("anilist_hint");
    if (!hint || hint.__cwRendered) return;

    hint.innerHTML =
      'You need an AniList API key. Create one at ' +
      '<a href="https://anilist.co/settings/developer" target="_blank" rel="noreferrer">AniList Developer</a>. ' +
      'Set the Redirect URL to <code id="redirect_uri_preview_anilist"></code>.' +
      ' <button class="btn" style="margin-left:8px" onclick="copyAniListRedirect()">Copy Redirect URL</button>';

    hint.__cwRendered = true;
  }

  async function refreshAniListInstanceOptions(preserve = true) {
  const sel = Q(SECTION + " #anilist_instance") || $("#anilist_instance");
  if (!sel) return;

  let want = preserve ? getAniListInstance() : "default";
  sel.innerHTML = "";

  const addOpt = (id, label) => {
    const o = d.createElement("option");
    o.value = String(id);
    o.textContent = String(label || id);
    sel.appendChild(o);
  };

  // Always render Default, even if the API call fails.
  addOpt("default", "Default");

  try {
    const r = await fetch("/api/provider-instances/anilist" + bust(), { cache: "no-store" });
    const data = await r.json().catch(() => null);
    const opts = Array.isArray(data) ? data : (Array.isArray(data?.instances) ? data.instances : []);

    opts.forEach((o) => {
      if (!o || !o.id || o.id === "default") return;
      addOpt(o.id, o.label || o.name || o.id);
    });
  } catch {}

  if (!Array.from(sel.options).some((o) => o.value === want)) want = "default";
  sel.value = want;
  setAniListInstance(want);
}


  function ensureAniListInstanceUI() {
    const panel = Q('#sec-anilist .cw-meta-provider-panel[data-provider="anilist"]') || Q('#sec-anilist .cw-meta-provider-panel') || Q(SECTION);
    const head = panel ? Q(".cw-panel-head", panel) : null;
    if (!head || head.__cwAniListInstanceUI) return;
    head.__cwAniListInstanceUI = true;

    const wrap = d.createElement("div");
    wrap.className = "inline";
    wrap.style.display = "flex";
    wrap.style.gap = "8px";
    wrap.style.alignItems = "center";
    wrap.style.marginLeft = 'auto';
    wrap.style.flexWrap = 'nowrap';
    wrap.title = "Select which AniList account this config applies to.";

    const lab = d.createElement("span");
    lab.className = "muted";
    lab.textContent = "Profile";

    const sel = d.createElement("select");
    sel.id = "anilist_instance";
    sel.className = "input";
    sel.style.minWidth = "160px";

    // Match Trakt: keep it compact and let content drive the width.
    sel.style.width = 'auto';
    sel.style.maxWidth = '220px';
    sel.style.flex = '0 0 auto';
    const btnNew = d.createElement("button");
    btnNew.type = "button";
    btnNew.className = "btn secondary";
    btnNew.id = "anilist_instance_new";
    btnNew.textContent = "New";

    const btnDel = d.createElement("button");
    btnDel.type = "button";
    btnDel.className = "btn secondary";
    btnDel.id = "anilist_instance_del";
    btnDel.textContent = "Delete";

    wrap.appendChild(lab);
    wrap.appendChild(sel);
    wrap.appendChild(btnNew);
    wrap.appendChild(btnDel);
    head.appendChild(wrap);

    refreshAniListInstanceOptions(true);

    sel.addEventListener("change", async () => {
      setAniListInstance(sel.value);
      await hydrateFromConfig(true);
      updateAniListButtonState();
    });

    btnNew.addEventListener("click", async () => {
      try {
        const r = await fetch(`/api/provider-instances/anilist/next?ts=${Date.now()}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}", cache: "no-store" });
        const j = await r.json().catch(() => ({}));
        const id = String(j?.id || "").trim();
        if (!r.ok || j?.ok === false || !id) throw new Error(String(j?.error || "create_failed"));
        setAniListInstance(id);
        await refreshAniListInstanceOptions(true);
        await hydrateFromConfig(true);
        updateAniListButtonState();
      } catch (e) {
        notify("Could not create profile: " + (e?.message || e));
      }
    });

    btnDel.addEventListener("click", async () => {
      const inst = getAniListInstance();
      if (inst === "default") return notify("Default profile cannot be deleted.");
      if (!confirm(`Delete AniList profile '${inst}'?`)) return;
      try {
        const r = await fetch(`/api/provider-instances/anilist/${encodeURIComponent(inst)}`, { method: "DELETE", cache: "no-store" });
        const j = await r.json().catch(() => ({}));
        if (!r.ok || j?.ok === false) throw new Error(String(j?.error || "delete_failed"));
        setAniListInstance("default");
        await refreshAniListInstanceOptions(false);
        await hydrateFromConfig(true);
        updateAniListButtonState();
      } catch (e) {
        notify("Could not delete profile: " + (e?.message || e));
      }
    });
  }

  function getAniListCfgBlock(cfg) {
    cfg = cfg || {};
    const base = (cfg.anilist && typeof cfg.anilist === "object") ? cfg.anilist : (cfg.anilist = {});
    const inst = getAniListInstance();
    if (inst === "default") return base;

    if (!base.instances || typeof base.instances !== "object") base.instances = {};
    if (!base.instances[inst] || typeof base.instances[inst] !== "object") base.instances[inst] = {};
    return base.instances[inst];
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
      const tokEl = $("anilist_access_token");

      if (cidEl && (force || !cidEl.value)) cidEl.value = cid;
      if (secEl && (force || !secEl.value)) secEl.value = sec;
      if (tokEl && (force || !tokEl.value)) tokEl.value = tok;

      if (tok) setAniListSuccess(true);
      else setAniListSuccess(false, "");

      updateAniListButtonState();
    } catch {}
  }

  function updateAniListButtonState() {
    try {
      ensureAniListInstanceUI();
      renderAniListHint();

      const cid = ($("anilist_client_id")?.value || "").trim();
      const sec = ($("anilist_client_secret")?.value || "").trim();
      const ok = cid.length > 0 && sec.length > 0;

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
      cid.addEventListener("input", updateAniListButtonState);
      cid.__cwBound = true;
    }
    if (sec && !sec.__cwBound) {
      sec.addEventListener("input", updateAniListButtonState);
      sec.__cwBound = true;
    }

    updateAniListButtonState();
  }

  async function copyAniListRedirect() {
    const uri = computeRedirect();
    try {
      await navigator.clipboard.writeText(uri);
      notify("Redirect URL copied ✓");
      return;
    } catch {}

    try {
      const ta = d.createElement("textarea");
      ta.value = uri;
      ta.setAttribute("readonly", "");
      ta.style.position = "fixed";
      ta.style.top = "0";
      ta.style.left = "0";
      ta.style.opacity = "0";
      d.body.appendChild(ta);
      ta.focus();
      ta.select();
      ta.setSelectionRange(0, ta.value.length);
      const ok = d.execCommand("copy");
      d.body.removeChild(ta);
      if (ok) notify("Redirect URL copied ✓");
    } catch {}
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
        try {
          const el = $("anilist_access_token");
          if (el) el.value = "";
        } catch {}

        if (msg) {
          msg.classList.add("warn");
          msg.textContent = "Disconnected.";
        }
        notify("AniList token removed.");
        try { w.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
      } else {
        if (msg) {
          msg.classList.add("warn");
          msg.textContent = "Could not remove token.";
        }
      }
    } catch {
      if (msg) {
        msg.classList.add("warn");
        msg.textContent = "Could not remove token.";
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

    const cid = ($("anilist_client_id")?.value || "").trim();
    const sec = ($("anilist_client_secret")?.value || "").trim();
    if (!cid || !sec) return;

    // Save credentials to the selected instance before authorizing.
    try {
      await fetch(anilistApi("/api/anilist/save"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ client_id: cid, client_secret: sec }),
        cache: "no-store",
      });
    } catch {}

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
        try {
          const el = $("anilist_access_token");
          if (el) el.value = tok;
        } catch {}

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
    if (__anilistInitDone) return;
    __anilistInitDone = true;

    try { initAniListAuthUI(); } catch (_) {}
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
