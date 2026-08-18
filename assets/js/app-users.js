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
  const expandedUsers = new Set();
  const dirtyUsers = new Set();
  const deleteTimers = new Map();

  function toast(message, ok = false) {
    try { w._cwShowToast?.(String(message || ""), !!ok); } catch {}
  }

  async function api(url, opts) {
    const res = await fetch(url, { cache: "no-store", credentials: "same-origin", ...(opts || {}) });
    const data = await res.json().catch(() => null);
    if (!res.ok || data?.ok === false) throw new Error(String(data?.error || `HTTP ${res.status}`));
    return data;
  }

  function profileLabel(id) {
    const pid = txt(id);
    return txt(profiles.find((row) => row.id === pid)?.label) || "No profile";
  }

  function profileOptions(selected) {
    const sid = txt(selected);
    if (!profiles.length) return `<option value="">No profiles</option>`;
    return profiles.map((p) => `<option value="${esc(p.id)}"${p.id === sid ? " selected" : ""}>${esc(p.label || p.id)}</option>`).join("");
  }

  function createReady() {
    return !!(isAdmin && profiles.length && txt($("app_user_username")?.value) && String($("app_user_password")?.value || "") && txt($("app_user_profile")?.value));
  }

  function updateCreateState() {
    const createBtn = $("btn-app-user-create");
    if (createBtn) createBtn.disabled = !createReady();
  }

  function createProfileWrap(profileSelect) {
    if (!profileSelect) return null;
    const next = profileSelect.nextElementSibling;
    if (next?.classList?.contains("cw-icon-select") && (!next.__cwNativeSelect || next.__cwNativeSelect === profileSelect)) return next;
    return Array.from(profileSelect.parentElement?.children || [])
      .find((node) => node?.classList?.contains("cw-icon-select") && node.__cwNativeSelect === profileSelect) || null;
  }

  function normalizeCreateProfileWrap(profileSelect) {
    const wrap = createProfileWrap(profileSelect);
    if (wrap && profileSelect.nextElementSibling !== wrap) {
      profileSelect.insertAdjacentElement("afterend", wrap);
    }
    return wrap;
  }

  function enhanceCreateProfileSelect(profileSelect) {
    if (!profileSelect || !profiles.length) return normalizeCreateProfileWrap(profileSelect);
    normalizeCreateProfileWrap(profileSelect);
    try { w.CW?.IconSelect?.enhance?.(profileSelect, { className: "cw-plain-select" }); } catch {}
    return normalizeCreateProfileWrap(profileSelect);
  }

  function renderCreateProfileControl(profileSelect) {
    if (!profileSelect) return;
    let addBtn = $("btn-app-user-profile-create");
    const createForm = profileSelect.closest(".cw-app-user-create");
    let profileWrap = normalizeCreateProfileWrap(profileSelect);
    const usernameInput = $("app_user_username");
    const passwordInput = $("app_user_password");
    const createBtn = $("btn-app-user-create");
    if (!profiles.length) {
      if (createForm) createForm.classList.add("has-no-profiles");
      if (usernameInput) usernameInput.hidden = true;
      if (passwordInput) passwordInput.hidden = true;
      if (createBtn) createBtn.hidden = true;
      profileSelect.hidden = true;
      profileSelect.disabled = true;
      profileSelect.classList.add("cw-app-user-profile-select-hidden");
      profileSelect.style.display = "none";
      profileSelect.setAttribute("aria-hidden", "true");
      if (profileWrap) {
        profileWrap.hidden = true;
        profileWrap.classList.add("cw-app-user-profile-select-wrap-hidden");
        profileWrap.style.display = "none";
        profileWrap.setAttribute("aria-hidden", "true");
      }
      if (!addBtn) {
        addBtn = d.createElement("button");
        addBtn.className = "btn primary cw-app-user-add-profile";
        addBtn.type = "button";
        addBtn.id = "btn-app-user-profile-create";
        addBtn.textContent = "Add profile";
        (profileWrap || profileSelect).insertAdjacentElement("afterend", addBtn);
      }
      addBtn.hidden = false;
      addBtn.classList.remove("hidden");
      return;
    }
    if (createForm) createForm.classList.remove("has-no-profiles");
    if (usernameInput) usernameInput.hidden = false;
    if (passwordInput) passwordInput.hidden = false;
    if (createBtn) createBtn.hidden = false;
    profileSelect.hidden = false;
    profileSelect.disabled = false;
    profileSelect.classList.remove("cw-app-user-profile-select-hidden");
    profileSelect.style.display = "";
    profileSelect.removeAttribute("aria-hidden");
    profileWrap = enhanceCreateProfileSelect(profileSelect);
    if (profileWrap) {
      profileWrap.hidden = false;
      profileWrap.classList.remove("cw-app-user-profile-select-wrap-hidden");
      profileWrap.style.display = "";
      profileWrap.removeAttribute("aria-hidden");
    }
    if (addBtn) {
      addBtn.hidden = true;
      addBtn.classList.add("hidden");
    }
  }

  function accessMode(user) {
    const raw = user?.permissions && typeof user.permissions === "object" ? user.permissions : {};
    return raw.write === true ? "full" : "readonly";
  }

  function accessModeSelect(user) {
    const mode = accessMode(user);
    return `
      <label class="cw-app-user-access">
        <span>Access</span>
        <select data-user-access>
          <option value="readonly"${mode === "readonly" ? " selected" : ""}>Read-only</option>
          <option value="full"${mode === "full" ? " selected" : ""}>Full access</option>
        </select>
      </label>`;
  }

  function renderUser(user) {
    const protectedUser = user.protected || user.id === "administrator";
    const id = txt(user.id);
    const expanded = expandedUsers.has(id);
    const enabled = user.enabled !== false;
    const mode = accessMode(user) === "full" ? "Full access" : "Read-only";
    return `
      <div class="cw-app-user-row${protectedUser ? " is-protected" : ""}${expanded ? " is-expanded" : ""}" data-user-id="${esc(id)}">
        <div class="cw-app-user-summary"${protectedUser ? "" : ` data-user-action="toggle" role="button" tabindex="0"`}>
          <div class="cw-app-user-main">
            <span class="material-symbols-rounded cw-app-user-avatar" aria-hidden="true">${protectedUser ? "shield_person" : "person"}</span>
            <div>
              <strong>${esc(user.username || user.label || user.id)}</strong>
              <small>${protectedUser ? "Administrator" : esc(profileLabel(user.profile_id))}</small>
            </div>
          </div>
          ${protectedUser ? `<span class="cw-app-user-pill">Administrator</span>` : `
            <div class="cw-app-user-summary-actions">
              <span class="cw-app-user-pill">${esc(mode)}</span>
              <button class="cw-app-user-power${enabled ? " is-on" : " is-off"}" type="button" data-user-action="toggle-enabled" data-user-enabled="${enabled ? "true" : "false"}" title="${enabled ? "Disable user" : "Enable user"}" aria-label="${enabled ? "Disable user" : "Enable user"}" aria-pressed="${enabled ? "true" : "false"}">
                <span class="material-symbols-rounded" aria-hidden="true">power_settings_new</span>
              </button>
              <span class="material-symbols-rounded cw-app-user-chevron" aria-hidden="true">expand_more</span>
            </div>`}
        </div>
        ${protectedUser ? "" : `
          <form class="cw-app-user-details" data-user-form autocomplete="off" onsubmit="return false">
          <div class="cw-app-user-controls">
            <input type="text" data-user-name value="${esc(user.username || "")}" autocomplete="username" placeholder="Username">
            <input type="password" data-user-password autocomplete="new-password" placeholder="New password">
            <select data-user-profile>${profileOptions(user.profile_id)}</select>
          </div>
          <div class="cw-app-user-perms">${accessModeSelect(user)}</div>
          <div class="cw-app-user-actions">
            <button class="btn danger cw-app-user-delete cw-danger-confirm" type="button" data-user-action="delete"><span class="material-symbols-rounded" aria-hidden="true">delete</span><span>Delete</span></button>
          </div>
          </form>`}
      </div>`;
  }

  function render() {
    const host = $("app_user_manager");
    const list = $("app_users_list");
    const profileSelect = $("app_user_profile");
    const createBtn = $("btn-app-user-create");
    if (!host || !list) return;
    host.classList.toggle("cw-app-user-locked", !isAdmin);
    if (profileSelect) profileSelect.innerHTML = profileOptions("");
    renderCreateProfileControl(profileSelect);
    if (createBtn) createBtn.disabled = !createReady();
    if (!isAdmin) {
      list.innerHTML = `<div class="cw-app-user-empty">Administrator access required.</div>`;
      return;
    }
    if (!users.length) {
      list.innerHTML = `<div class="cw-app-user-empty">No users yet.</div>`;
      return;
    }
    list.innerHTML = users.map(renderUser).join("");
  }

  async function refresh() {
    const host = $("app_user_manager");
    if (!host) return;
    try {
      const status = await api("/api/app-auth/status");
      isAdmin = !!status?.is_admin;
      const profileData = await api("/api/user-profiles");
      profiles = Array.isArray(profileData?.items) ? profileData.items : [];
      if (isAdmin) {
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

  function rowPayload(row) {
    const full = String(row.querySelector("[data-user-access]")?.value || "readonly") === "full";
    const enabledEl = row.querySelector("[data-user-enabled]");
    const payload = {
      username: txt(row.querySelector("[data-user-name]")?.value),
      profile_id: txt(row.querySelector("[data-user-profile]")?.value),
      enabled: String(enabledEl?.dataset?.userEnabled || enabledEl?.value || "true") === "true",
      permissions: { dashboard: true, watchlist: true, playback: true, write: full },
    };
    const password = String(row.querySelector("[data-user-password]")?.value || "");
    if (password) payload.password = password;
    return payload;
  }

  function updateRowState(row) {
    if (!row) return;
    const id = txt(row.dataset.userId);
    row.classList.toggle("is-dirty", dirtyUsers.has(id));
    const enabledBtn = row.querySelector("[data-user-enabled]");
    const enabled = String(enabledBtn?.dataset?.userEnabled || "true") === "true";
    if (enabledBtn) {
      enabledBtn.classList.toggle("is-on", enabled);
      enabledBtn.classList.toggle("is-off", !enabled);
      enabledBtn.title = enabled ? "Disable user" : "Enable user";
      enabledBtn.setAttribute("aria-label", enabled ? "Disable user" : "Enable user");
      enabledBtn.setAttribute("aria-pressed", enabled ? "true" : "false");
    }
    const mode = String(row.querySelector("[data-user-access]")?.value || "readonly") === "full" ? "Full access" : "Read-only";
    const pill = row.querySelector(".cw-app-user-summary-actions .cw-app-user-pill");
    if (pill) pill.textContent = mode;
  }

  function markDirty(row) {
    const id = txt(row?.dataset?.userId);
    if (!id) return;
    dirtyUsers.add(id);
    updateRowState(row);
  }

  async function createUser() {
    const username = txt($("app_user_username")?.value);
    const profileId = txt($("app_user_profile")?.value);
    const password = String($("app_user_password")?.value || "");
    if (!username || !profileId || !password) {
      toast("Username, profile and password are required");
      return;
    }
    try {
      await api("/api/app-auth/users", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, profile_id: profileId, password, permissions: { dashboard: true, watchlist: true, playback: true, write: true } }),
      });
      if ($("app_user_username")) $("app_user_username").value = "";
      if ($("app_user_password")) $("app_user_password").value = "";
      toast("User created", true);
      await refresh();
    } catch (err) {
      toast(err?.message || "Could not create user");
    }
  }

  async function saveUser(row, refreshAfter = true) {
    const id = txt(row?.dataset?.userId);
    if (!id) return;
    try {
      await api(`/api/app-auth/users/${encodeURIComponent(id)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(rowPayload(row)),
      });
      dirtyUsers.delete(id);
      if (refreshAfter) {
        toast("User saved", true);
        await refresh();
      }
    } catch (err) {
      toast(err?.message || "Could not save user");
      const abort = new Error(err?.message || "Could not save user");
      abort.__cwAbortSave = true;
      throw abort;
    }
  }

  async function deleteUser(row) {
    const id = txt(row?.dataset?.userId);
    if (!id) return;
    const btn = row.querySelector('[data-user-action="delete"]');
    if (btn?.dataset?.cwConfirmDelete !== "1") {
      if (btn) {
        btn.dataset.cwConfirmDelete = "1";
        btn.classList.add("is-confirming");
        btn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">warning</span><span>Confirm delete</span>`;
      }
      if (deleteTimers.has(id)) clearTimeout(deleteTimers.get(id));
      deleteTimers.set(id, setTimeout(() => {
        try {
          btn?.classList.remove("is-confirming");
          if (btn) {
            btn.dataset.cwConfirmDelete = "";
            btn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">delete</span><span>Delete</span>`;
          }
          deleteTimers.delete(id);
        } catch {}
      }, 4200));
      return;
    }
    if (deleteTimers.has(id)) clearTimeout(deleteTimers.get(id));
    deleteTimers.delete(id);
    try {
      await api(`/api/app-auth/users/${encodeURIComponent(id)}`, { method: "DELETE" });
      dirtyUsers.delete(id);
      toast("User deleted", true);
      await refresh();
    } catch (err) {
      toast(err?.message || "Could not delete user");
    }
  }

  function toggleUser(row) {
    const id = txt(row?.dataset?.userId);
    if (!id) return;
    if (expandedUsers.has(id)) expandedUsers.delete(id);
    else expandedUsers.add(id);
    render();
  }

  async function toggleEnabled(row, btn) {
    if (!row || !btn) return;
    const next = String(btn.dataset.userEnabled || "true") !== "true";
    btn.dataset.userEnabled = next ? "true" : "false";
    markDirty(row);
  }

  async function savePending() {
    const rows = Array.from(d.querySelectorAll(".cw-app-user-row")).filter((row) => dirtyUsers.has(txt(row.dataset.userId)));
    if (!rows.length) return true;
    for (const row of rows) await saveUser(row, false);
    await refresh();
    return true;
  }

  async function setupTotp(row) {
    const id = txt(row?.dataset?.userId);
    if (!id) return;
    try {
      const data = await api("/api/app-auth/totp/setup", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: id }),
      });
      const box = row.querySelector(".cw-app-user-totp-pending");
      const secret = row.querySelector("[data-user-totp-secret]");
      const code = row.querySelector("[data-user-totp-code]");
      if (box) box.classList.remove("hidden");
      if (secret) secret.value = data.secret || "";
      if (code) {
        code.value = "";
        code.focus?.();
      }
      toast("2FA secret created", true);
    } catch (err) {
      toast(err?.message || "Could not set up 2FA");
    }
  }

  async function verifyTotp(row) {
    const id = txt(row?.dataset?.userId);
    const codeEl = row?.querySelector("[data-user-totp-code]");
    const code = String(codeEl?.value || "").replace(/\D+/g, "").slice(0, 6);
    if (!id) return;
    if (code.length !== 6) {
      toast("Enter the 6 digit code");
      codeEl?.focus?.();
      return;
    }
    try {
      await api("/api/app-auth/totp/verify", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: id, code }),
      });
      toast("2FA enabled", true);
      await refresh();
    } catch (err) {
      toast(err?.message || "Could not verify 2FA");
    }
  }

  async function disableTotp(row) {
    const id = txt(row?.dataset?.userId);
    if (!id) return;
    const btn = row.querySelector('[data-user-action="totp-disable"]');
    if (btn && btn.dataset.confirmTotpDisable !== "1") {
      btn.dataset.confirmTotpDisable = "1";
      const original = btn.innerHTML;
      btn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">lock_open</span>Confirm`;
      setTimeout(() => { try { delete btn.dataset.confirmTotpDisable; btn.innerHTML = original; } catch {} }, 2200);
      return;
    }
    try {
      await api("/api/app-auth/totp/disable", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: id }),
      });
      toast("2FA disabled", true);
      await refresh();
    } catch (err) {
      toast(err?.message || "Could not disable 2FA");
    }
  }

  d.addEventListener("click", (event) => {
    const target = event.target?.closest?.("[data-user-action],#btn-app-user-create,#btn-app-user-profile-create");
    if (!target) return;
    if (target.id === "btn-app-user-profile-create") {
      w.cwUserProfilesOpenNew?.({ stay: true });
      return;
    }
    if (target.id === "btn-app-user-create") {
      if (target.disabled) return;
      createUser();
      return;
    }
    const row = target.closest(".cw-app-user-row");
    if (target.dataset.userAction === "toggle") toggleUser(row);
    if (target.dataset.userAction === "toggle-enabled") toggleEnabled(row, target);
    if (target.dataset.userAction === "delete") deleteUser(row);
    if (target.dataset.userAction === "totp-setup") setupTotp(row);
    if (target.dataset.userAction === "totp-verify") verifyTotp(row);
    if (target.dataset.userAction === "totp-disable") disableTotp(row);
  });

  d.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    const target = event.target?.closest?.('[data-user-action="toggle"]');
    if (!target) return;
    event.preventDefault();
    toggleUser(target.closest(".cw-app-user-row"));
  });

  d.addEventListener("input", (event) => {
    if (event.target?.id === "app_user_username" || event.target?.id === "app_user_password") updateCreateState();
    const row = event.target?.closest?.(".cw-app-user-row");
    if (row && event.target?.matches?.("[data-user-name],[data-user-password]")) markDirty(row);
    const target = event.target?.closest?.("[data-user-totp-code]");
    if (!target) return;
    target.value = String(target.value || "").replace(/\D+/g, "").slice(0, 6);
  });

  d.addEventListener("change", (event) => {
    if (event.target?.id === "app_user_profile") updateCreateState();
    const row = event.target?.closest?.(".cw-app-user-row");
    if (row && event.target?.matches?.("[data-user-profile],[data-user-access]")) markDirty(row);
  });

  d.addEventListener("submit", (event) => {
    if (event.target?.id === "app_user_create_form") {
      event.preventDefault();
      if (createReady()) createUser();
    }
    if (event.target?.matches?.("[data-user-form]")) {
      event.preventDefault();
      markDirty(event.target.closest(".cw-app-user-row"));
    }
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
