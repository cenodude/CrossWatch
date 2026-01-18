// auth.anilist.js - AniList auth only
(function (w, d) {
  const $ = (s) => d.getElementById(s);
  const q = (sel, root = d) => root.querySelector(sel);
  const notify = w.notify || ((m) => console.log("[notify]", m));
  const bust = () => `?ts=${Date.now()}`;

  const computeRedirect = () => location.origin + "/callback/anilist";

  function setAniListSuccess(on) {
    const msg = $("anilist_msg");
    if (!msg) return;
    msg.classList.toggle("hidden", !on);
    msg.classList.toggle("ok", !!on);
    msg.classList.remove("warn");
    if (on && !msg.textContent) msg.textContent = "Connected.";
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

  function updateAniListButtonState() {
    try {
      renderAniListHint();

      const cid = ($("anilist_client_id")?.value || "").trim();
      const sec = ($("anilist_client_secret")?.value || "").trim();
      const ok = cid.length > 0 && sec.length > 0;

      const btn = $("btn-connect-anilist");
      const hint = $("anilist_hint");
      const rid = $("redirect_uri_preview_anilist");

      if (rid) rid.textContent = computeRedirect();
      if (btn) btn.disabled = !ok;
      if (hint) hint.classList.toggle("hidden", ok);
    } catch (e) {
      console.warn("updateAniListButtonState failed", e);
    }
  }

  function initAniListAuthUI() {
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

  function autoInitAniListAuthUI() {
    if (w.__cwAniListAutoInit) return;
    w.__cwAniListAutoInit = true;

    const root = d.body;
    if (!root) return;

    let pending = false;
    const schedule = () => {
      if (pending) return;
      pending = true;
      setTimeout(() => {
        pending = false;
        initAniListAuthUI();
      }, 0);
    };

    const obs = new MutationObserver(() => {
      if (
        $("anilist_client_id") ||
        $("anilist_client_secret") ||
        $("anilist_hint") ||
        $("redirect_uri_preview_anilist") ||
        $("btn-connect-anilist")
      ) {
        schedule();
      }
    });

    obs.observe(root, { childList: true, subtree: true });
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
    const btn = q("#sec-anilist .btn.danger");
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
      const r = await fetch("/api/anilist/token/delete", {
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
      try {
        setAniListSuccess(false);
      } catch {}
    }
  }

  let pollHandle = null;
  async function startAniList() {
    try {
      setAniListSuccess(false);
    } catch {}
    try {
      await w.saveSettings?.();
    } catch {}

    const j = await fetch("/api/anilist/authorize", {
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
      try {
        cfg = await fetch("/api/config" + bust(), { cache: "no-store" }).then((r) => r.json());
      } catch {}

      const tok = (cfg?.anilist?.access_token || cfg?.auth?.anilist?.access_token || "").trim();
      if (tok) {
        try {
          const el = $("anilist_access_token");
          if (el) el.value = tok;
        } catch {}

        const msg = $("anilist_msg");
        if (msg) {
          msg.classList.remove("hidden", "warn");
          msg.classList.add("ok");
          msg.textContent = "Connected.";
        }

        pollHandle = null;
        return;
      }

      pollHandle = setTimeout(poll, back[Math.min(i++, back.length - 1)]);
    };

    pollHandle = setTimeout(poll, 1000);
  }

  d.addEventListener("DOMContentLoaded", () => {
    initAniListAuthUI();
    autoInitAniListAuthUI();
    (async function hydrateAniListBannerFromConfig() {
      try {
        const cfg = await fetch('/api/config' + bust(), { cache: 'no-store' }).then(r => r.ok ? r.json() : null);
        const tok = String(cfg?.anilist?.access_token || '').trim();
        if (tok) {
          const el = $('anilist_access_token');
          if (el) el.value = tok;
          setAniListSuccess(true);
        }
      } catch (_) {}
    })();
  });

  Object.assign(w, {
    setAniListSuccess,
    updateAniListButtonState,
    initAniListAuthUI,
    autoInitAniListAuthUI,
    startAniList,
    copyAniListRedirect,
    anilistDeleteToken,
  });
})(window, document);
