/* assets/helpers/auth-state.js */
/* CrossWatch - Auth state and managed access policy */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const CW = (window.CW ||= {});
  const DOC = document.documentElement;
  const txt = (value) => String(value ?? "").trim();
  const boolOn = (value) => String(value || "").toLowerCase() === "on";
  let user = null;
  let status = null;
  let pending = null;

  function read() {
    const managed = DOC.dataset.cwRole === "user";
    const write = managed ? boolOn(DOC.dataset.cwPermWrite) : true;
    return {
      user,
      status,
      role: managed ? "user" : "admin",
      isAdmin: !managed,
      isManaged: managed,
      profileId: txt(DOC.dataset.cwProfileId),
      preferences: user && typeof user.preferences === "object" ? user.preferences : {},
      permissions: {
        dashboard: !managed || DOC.dataset.cwPermDashboard !== "off",
        watchlist: !managed || DOC.dataset.cwPermWatchlist !== "off",
        playback: !managed || DOC.dataset.cwPermPlayback !== "off",
        write,
      },
    };
  }

  function apply(nextStatus) {
    if (!nextStatus || typeof nextStatus !== "object") return read();
    status = nextStatus;
    const nextUser = nextStatus.user && typeof nextStatus.user === "object" ? nextStatus.user : null;
    if (!nextUser && nextStatus.authenticated !== true) return read();
    user = nextUser;
    const managed = !!(nextUser && nextUser.is_admin === false);
    DOC.dataset.cwRole = managed ? "user" : "admin";
    if (managed) {
      DOC.dataset.cwProfileId = txt(nextUser.profile_id || DOC.dataset.cwProfileId);
      const perms = nextUser.permissions && typeof nextUser.permissions === "object" ? nextUser.permissions : {};
      DOC.dataset.cwPermDashboard = perms.dashboard === false ? "off" : "on";
      DOC.dataset.cwPermWatchlist = perms.watchlist === false ? "off" : "on";
      DOC.dataset.cwPermPlayback = perms.playback === false ? "off" : "on";
      DOC.dataset.cwPermWrite = perms.write === true ? "on" : "off";
    } else {
      DOC.dataset.cwProfileId = "";
      DOC.dataset.cwPermDashboard = "on";
      DOC.dataset.cwPermWatchlist = "on";
      DOC.dataset.cwPermPlayback = "on";
      DOC.dataset.cwPermWrite = "on";
    }
    try {
      window.dispatchEvent(new CustomEvent("cw:auth-state-changed", { detail: read() }));
    } catch {}
    return read();
  }

  async function refresh() {
    if (pending) return pending;
    pending = fetch("/api/app-auth/status", { cache: "no-store", credentials: "same-origin" })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((data) => apply(data))
      .catch(() => read())
      .finally(() => {
        pending = null;
      });
    return pending;
  }

  CW.AuthState = {
    read,
    apply,
    refresh,
    get user() { return user; },
    get isAdmin() { return read().isAdmin; },
    get isManaged() { return read().isManaged; },
    get canWrite() { return read().permissions.write; },
    get profileId() { return read().profileId; },
  };
})();
