/* assets/js/app-users.js */
/* CrossWatch - App user manager */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function (w, d) {
  if (w.cwAppUsersManager) return;

  const $ = (id) => d.getElementById(id);
  const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m]));
  const txt = (v) => String(v ?? "").trim();
  let profiles = [];
  let users = [];
  let isAdmin = false;
  let authActive = false;

  const ACCESS_LABELS = { readonly: "Read-only", full: "Full access", monitor: "Monitor" };

  async function api(url, opts) {
    const res = await fetch(url, { cache: "no-store", credentials: "same-origin", ...(opts || {}) });
    const data = await res.json().catch(() => null);
    if (!res.ok || data?.ok === false) throw new Error(String(data?.error || `HTTP ${res.status}`));
    return data;
  }

  function profileFor(id) {
    const pid = txt(id);
    return pid ? profiles.find((row) => txt(row.id) === pid) || null : null;
  }

  function accessMode(user) {
    const raw = user?.permissions && typeof user.permissions === "object" ? user.permissions : {};
    if (raw.monitor === true) return "monitor";
    return raw.write === true ? "full" : "readonly";
  }

  function renderUser(user) {
    const protectedUser = user.protected || user.id === "administrator";
    const name = esc(user.username || user.label || user.id);
    if (protectedUser) {
      return `
        <div class="cw-app-user-row is-protected" data-user-id="${esc(user.id)}">
          <div class="cw-app-user-summary">
            <div class="cw-app-user-main">
              <span class="material-symbols-rounded cw-app-user-avatar" aria-hidden="true">shield_person</span>
              <div><strong>${name}</strong><small>Administrator</small></div>
            </div>
            <span class="cw-app-user-pill">Administrator</span>
          </div>
        </div>`;
    }
    const profile = profileFor(user.profile_id);
    const enabled = user.enabled !== false;
    return `
      <div class="cw-app-user-row" data-user-id="${esc(user.id)}">
        <div class="cw-app-user-summary">
          <div class="cw-app-user-main">
            <span class="material-symbols-rounded cw-app-user-avatar" aria-hidden="true">person</span>
            <div><strong>${name}</strong><small>${profile ? `Profile: ${esc(profile.label || profile.id)}` : "No profile"}</small></div>
          </div>
          <div class="cw-app-user-summary-actions">
            ${enabled ? "" : `<span class="cw-app-user-pill is-off">Disabled</span>`}
            ${user.totp_enabled ? `<span class="cw-app-user-pill">2FA</span>` : ""}
            <span class="cw-app-user-pill">${esc(ACCESS_LABELS[accessMode(user)])}</span>
            ${profile ? `<button class="btn cw-app-user-edit" type="button" data-user-action="edit" data-profile-id="${esc(profile.id)}"><span class="material-symbols-rounded" aria-hidden="true">edit</span><span>Edit</span></button>` : ""}
          </div>
        </div>
      </div>`;
  }

  function render() {
    const host = $("app_user_manager");
    const list = $("app_users_list");
    if (!host || !list) return;
    host.classList.toggle("cw-app-user-locked", !isAdmin);
    if (!isAdmin) {
      list.innerHTML = `<div class="cw-app-user-empty">Administrator access required.</div>`;
      return;
    }
    if (!authActive) {
      list.innerHTML = `<div class="cw-app-user-empty">Sign-in is turned off. Add a profile with a login and the setup turns it on for you.</div>`;
      return;
    }
    const managed = users.filter((user) => !(user.protected || user.id === "administrator"));
    list.innerHTML = users.map(renderUser).join("") + (managed.length ? "" : `<div class="cw-app-user-empty">Nobody else can sign in yet. Add a profile and give it a login.</div>`);
  }

  async function refresh() {
    const host = $("app_user_manager");
    if (!host) return;
    try {
      const status = await api("/api/app-auth/status");
      authActive = !!(status?.enabled && status?.configured);
      isAdmin = !!status?.is_admin || !authActive;
      const profileData = await api("/api/user-profiles");
      profiles = Array.isArray(profileData?.items) ? profileData.items : [];
      if (isAdmin && authActive) {
        const userData = await api("/api/app-auth/users");
        users = Array.isArray(userData?.items) ? userData.items : [];
      } else {
        users = [];
      }
    } catch {
      users = [];
      profiles = [];
      isAdmin = false;
    }
    render();
  }

  async function savePending() {
    return true;
  }

  d.addEventListener("click", (event) => {
    const target = event.target?.closest?.('[data-user-action="edit"],#btn-app-user-profile-create');
    if (!target) return;
    if (target.id === "btn-app-user-profile-create") {
      w.cwUserProfilesOpenNew?.({ login: true });
      return;
    }
    w.cwUserProfilesOpen?.(target.dataset.profileId, { step: "login" });
  });

  w.cwAppUsersManager = { refresh };
  w.cwAppUsersRefresh = refresh;
  w.cwAppUsersSavePending = savePending;
  d.addEventListener("DOMContentLoaded", () => { refresh(); });
  w.addEventListener("cw-user-profiles-changed", () => {
    refresh();
    setTimeout(() => refresh(), 160);
  });
  d.addEventListener("cw-settings-pane-changed", (event) => {
    if (txt(event?.detail?.pane).toLowerCase() === "security") refresh();
  });
})(window, document);
