/* assets/auth/auth.sync-users.js */
/* CrossWatch - Users tab for media server connections (Plex, Emby, Jellyfin) */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function (w, d) {
  if (w.CW?.SyncUsers) return;

  const PROVIDERS = new Set(["plex", "emby", "jellyfin"]);
  const LABELS = { plex: "Plex", emby: "Emby", jellyfin: "Jellyfin" };

  const txt = (v) => String(v ?? "").trim();
  const esc = (v) => txt(v).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const inst = (v) => txt(v) || "default";

  async function getJSON(url, opts) {
    const r = await fetch(url, { cache: "no-store", credentials: "same-origin", headers: { Accept: "application/json", ...(opts?.body ? { "Content-Type": "application/json" } : {}) }, ...(opts || {}) });
    let data = null;
    try { data = await r.json(); } catch {}
    if (!r.ok || data?.ok === false) {
      const err = new Error(txt(data?.error || data?.detail || data?.message) || `HTTP ${r.status}`);
      err.data = data;
      throw err;
    }
    return data;
  }

  function serverUsersUrl(provider, instance) {
    const q = `?instance=${encodeURIComponent(instance)}`;
    return provider === "plex" ? `/api/plex/pickusers${q}` : `/api/${provider}/users${q}`;
  }

  function syncUsersUrl(provider, instance) {
    return `/api/provider-instances/${encodeURIComponent(provider)}/${encodeURIComponent(instance)}/sync-users`;
  }

  function normalizeServerUser(provider, u) {
    if (provider === "plex") {
      const key = txt(u?.account_id);
      const type = txt(u?.type).toLowerCase();
      const kind = type === "owner" ? "Owner" : type === "friend" ? "Friend" : "Home";
      return key ? { key, username: txt(u?.username), kind, home: type !== "owner" && type !== "friend", disabled: false, cloud: txt(u?.cloud_account_id) } : null;
    }
    const key = txt(u?.id);
    const admin = u?.IsAdministrator === true;
    const disabled = u?.IsDisabled === true;
    return key ? { key, username: txt(u?.username), kind: disabled ? "Disabled" : admin ? "Admin" : "", home: false, disabled } : null;
  }

  function errorText(provider, err, limit) {
    const code = txt(err?.message);
    if (code === "sync_user_limit_reached") {
      const max = err?.data?.limit || limit;
      return `You can sync up to ${max} users per connection. Raise max_sync_users_per_connection in config.json if you need more.`;
    }
    if (code === "not_found") return "This connection profile no longer exists.";
    if (code === "unsupported_provider") return `${LABELS[provider] || provider} does not support user syncing.`;
    return code || "Something went wrong.";
  }

  function render(state) {
    const { root, provider } = state;
    const label = LABELS[provider] || provider;
    const rows = new Map();
    state.server.forEach((u) => rows.set(u.key, { ...u, missing: false }));
    state.saved.forEach((s) => {
      if (!rows.has(s.key)) rows.set(s.key, { key: s.key, username: s.username, kind: "", home: false, disabled: false, missing: true });
    });
    const list = [...rows.values()].sort((a, b) => a.username.localeCompare(b.username, undefined, { sensitivity: "base" }));
    const savedPin = new Map(state.saved.map((s) => [s.key, !!s.has_pin]));

    root.innerHTML = `
      <div style="max-width:820px">
        <div class="sub">Tick the ${esc(label)} users you want to sync. In a sync pair, turn on <strong>Sync all matched users</strong> to sync each ticked user with the user of the same name on the other server. Ticked users are added to the user profile with the same name, if one exists.</div>
        <div class="inline" style="gap:8px;margin:12px 0 8px;flex-wrap:wrap">
          <button type="button" class="btn" data-act="all">Select all</button>
          <button type="button" class="btn" data-act="none">Clear</button>
          <span class="sub" data-role="count" style="margin-left:auto"></span>
        </div>
        ${state.loadError ? `<div class="msg warn" style="margin-bottom:8px">Could not load users from ${esc(label)}: ${esc(state.loadError)}</div>` : ""}
        <div data-role="list" style="display:grid;gap:6px">
          ${list.length ? list.map((u) => `
            <label style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin:0">
              <input type="checkbox" data-key="${esc(u.key)}" data-name="${esc(u.username)}" ${state.selected.has(u.key) ? "checked" : ""} ${u.disabled && !state.selected.has(u.key) ? "disabled" : ""}>
              <strong>${esc(u.username || u.key)}</strong>
              ${u.kind ? `<span class="sub">${esc(u.kind)}</span>` : ""}
              ${u.missing ? `<span class="sub">Not found on the server</span>` : ""}
              ${provider === "plex" && u.home && state.selected.has(u.key) ? `<input type="password" inputmode="numeric" maxlength="8" autocomplete="off" data-pin="${esc(u.key)}" placeholder="${savedPin.get(u.key) ? "PIN saved" : "PIN (only if needed)"}" style="width:150px;margin-left:auto" data-lpignore="true" data-1p-ignore="true" data-bwignore="true">` : ""}
            </label>`).join("") : `<div class="sub">No users found on this server.</div>`}
        </div>
        <div class="inline" style="gap:12px;margin-top:12px;flex-wrap:wrap">
          <button type="button" class="btn" data-act="save">Save users</button>
          <div class="msg hidden" data-role="msg" role="status" aria-live="polite"></div>
        </div>
      </div>`;
    root.querySelectorAll("input[data-pin]").forEach((box) => { box.value = state.pins.get(box.dataset.pin) || ""; });
    updateCount(state);
  }

  function updateCount(state) {
    const node = state.root.querySelector('[data-role="count"]');
    if (node) node.textContent = `${state.selected.size} of ${state.limit} selected`;
  }

  function showMsg(state, text, ok) {
    const node = state.root.querySelector('[data-role="msg"]');
    if (!node) return;
    node.textContent = text;
    node.classList.remove("hidden", "ok", "warn");
    node.classList.add(ok ? "ok" : "warn");
  }

  function resetConfirm(state) {
    const btn = state.root.querySelector('[data-act="save"]');
    if (!btn) return;
    clearTimeout(state.confirmTimer);
    btn.dataset.confirm = "";
    btn.textContent = "Save users";
  }

  async function save(state) {
    const btn = state.root.querySelector('[data-act="save"]');
    const removed = state.saved.filter((s) => !state.selected.has(s.key));
    if (removed.length && btn?.dataset.confirm !== "1") {
      btn.dataset.confirm = "1";
      btn.textContent = removed.length === 1 ? `Remove ${removed[0].username}? Click again` : `Remove ${removed.length} users? Click again`;
      showMsg(state, "Removed users stop syncing. If you add them again later, CrossWatch starts fresh for them.", false);
      clearTimeout(state.confirmTimer);
      state.confirmTimer = setTimeout(() => resetConfirm(state), 6000);
      return;
    }
    resetConfirm(state);
    const names = new Map();
    state.root.querySelectorAll("input[data-key]").forEach((box) => names.set(box.dataset.key, box.dataset.name || ""));
    state.saved.forEach((s) => { if (!names.get(s.key)) names.set(s.key, s.username); });
    const cloudIds = new Map(state.server.filter((u) => u.cloud).map((u) => [u.key, u.cloud]));
    const users = [...state.selected].map((key) => {
      const row = { key, username: names.get(key) || key };
      const pin = txt(state.pins.get(key));
      if (pin) row.home_pin = pin;
      if (cloudIds.has(key)) row.cloud_account_id = cloudIds.get(key);
      return row;
    });
    btn.disabled = true;
    const current = () => state.root.__cwSyncUsersState === state;
    try {
      const res = await getJSON(syncUsersUrl(state.provider, state.instance), { method: "PUT", body: JSON.stringify({ users }) });
      if (!current()) return;
      state.saved = Array.isArray(res.users) ? res.users : [];
      state.selected = new Set(state.saved.map((s) => txt(s.key)));
      state.limit = res.limit || state.limit;
      state.pins.clear();
      render(state);
      showMsg(state, state.saved.length ? `Saved. ${state.saved.length} user${state.saved.length === 1 ? "" : "s"} selected for syncing.` : "Saved. No users selected.", true);
    } catch (err) {
      if (current()) showMsg(state, errorText(state.provider, err, state.limit), false);
    } finally {
      const again = current() ? state.root.querySelector('[data-act="save"]') : null;
      if (again) again.disabled = false;
    }
  }

  function wire(state) {
    if (state.root.__cwSyncUsersWired) return;
    state.root.__cwSyncUsersWired = true;
    state.root.addEventListener("input", (ev) => {
      const s = state.root.__cwSyncUsersState;
      const box = ev.target?.closest?.("input[data-pin]");
      if (s && box) s.pins.set(box.dataset.pin, box.value);
    });
    state.root.addEventListener("change", (ev) => {
      const s = state.root.__cwSyncUsersState;
      const box = ev.target?.closest?.("input[data-key]");
      if (!s || !box) return;
      if (box.checked) s.selected.add(box.dataset.key);
      else s.selected.delete(box.dataset.key);
      resetConfirm(s);
      if (s.provider === "plex") render(s);
      else updateCount(s);
    });
    state.root.addEventListener("click", (ev) => {
      const s = state.root.__cwSyncUsersState;
      const act = ev.target?.closest?.("[data-act]")?.dataset?.act;
      if (!s || !act) return;
      if (act === "save") { save(s); return; }
      if (act === "all") s.root.querySelectorAll("input[data-key]:not(:disabled)").forEach((box) => s.selected.add(box.dataset.key));
      if (act === "none") s.selected.clear();
      resetConfirm(s);
      render(s);
    });
  }

  async function mount(root, provider, instance) {
    const p = txt(provider).toLowerCase();
    if (!root || !PROVIDERS.has(p)) return;
    const state = { root, provider: p, instance: inst(instance), server: [], saved: [], selected: new Set(), pins: new Map(), limit: 25, loadError: "", confirmTimer: 0 };
    root.__cwSyncUsersState = state;
    wire(state);
    root.innerHTML = `<div class="sub">Loading users…</div>`;
    const [serverRes, savedRes] = await Promise.allSettled([
      getJSON(serverUsersUrl(p, state.instance)),
      getJSON(syncUsersUrl(p, state.instance)),
    ]);
    if (root.__cwSyncUsersState !== state) return;
    if (savedRes.status === "rejected") {
      root.innerHTML = `<div class="msg warn">${esc(errorText(p, savedRes.reason, state.limit))}</div>`;
      return;
    }
    state.saved = Array.isArray(savedRes.value?.users) ? savedRes.value.users.map((s) => ({ ...s, key: txt(s.key), username: txt(s.username) })) : [];
    state.limit = savedRes.value?.limit || state.limit;
    state.selected = new Set(state.saved.map((s) => s.key));
    if (serverRes.status === "fulfilled") {
      const raw = Array.isArray(serverRes.value?.users) ? serverRes.value.users : [];
      const seen = new Set();
      state.server = raw.map((u) => normalizeServerUser(p, u)).filter((u) => u && u.username && !seen.has(u.key) && seen.add(u.key));
    } else {
      state.loadError = txt(serverRes.reason?.message) || "request failed";
    }
    render(state);
  }

  w.CW = w.CW || {};
  w.CW.SyncUsers = { mount };
})(window, document);
