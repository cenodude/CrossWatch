/* assets/auth/auth.sync-users.js */
/* CrossWatch - Users tab for media server connections (Plex, Emby, Jellyfin) */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function (w, d) {
  if (w.CW?.SyncUsers) return;

  const PROVIDERS = new Set(["plex", "emby", "jellyfin"]);
  const LABELS = { plex: "Plex", emby: "Emby", jellyfin: "Jellyfin" };
  const KIND_CLASS = { Owner: "is-owner", Admin: "is-owner", Home: "is-home", Friend: "is-friend", Disabled: "is-disabled" };

  const pending = new Map();
  const pendingKey = (provider, instance) => `${provider}:${instance}`;
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
      return key ? { key, username: txt(u?.username), kind, home: kind === "Home", disabled: false, cloud: txt(u?.cloud_account_id) } : null;
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
    if (code === "match_as_too_long") return "Match as can be at most 40 characters.";
    if (code === "not_found") return "This connection profile no longer exists.";
    if (code === "unsupported_provider") return `${LABELS[provider] || provider} does not support user syncing.`;
    return code || "Something went wrong.";
  }

  function rows(state) {
    const map = new Map();
    state.server.forEach((u) => map.set(u.key, { ...u, missing: false }));
    state.saved.forEach((s) => {
      if (!map.has(s.key)) map.set(s.key, { key: s.key, username: s.username, kind: "", home: false, disabled: false, missing: true });
    });
    return [...map.values()].sort((a, b) => a.username.localeCompare(b.username, undefined, { sensitivity: "base" }));
  }

  function rowHTML(state, u, idx) {
    const on = state.selected.has(u.key);
    const id = `cw-su-${state.provider}-${idx}`;
    const savedPin = state.saved.find((s) => s.key === u.key)?.has_pin;
    const initial = esc((u.username || u.key).charAt(0).toUpperCase());
    const tag = u.missing ? `<span class="cw-su-tag is-missing">Not on server</span>` : u.kind ? `<span class="cw-su-tag ${KIND_CLASS[u.kind] || ""}">${esc(u.kind)}</span>` : "";
    const pin = state.provider === "plex" && u.home
      ? `<input type="password" class="cw-su-pin" inputmode="numeric" maxlength="8" autocomplete="off" data-pin="${esc(u.key)}" placeholder="${savedPin ? "PIN saved" : "PIN if needed"}" ${on ? "" : "hidden"} data-lpignore="true" data-1p-ignore="true" data-bwignore="true">`
      : "";
    const match = `<input type="text" class="cw-su-match" maxlength="40" autocomplete="off" spellcheck="false" data-match="${esc(u.key)}" placeholder="Match as ${esc(u.username || u.key)}" title="Name used to match this user on your other servers. Leave empty to use the user name." ${on ? "" : "hidden"}>`;
    return `
      <label class="cw-wl-row cw-su-row ${on ? "is-on" : ""}" for="${id}" data-row="${esc(u.key)}" data-search="${esc(u.username.toLowerCase())}">
        <span class="cw-wl-ic cw-su-avatar" aria-hidden="true">${initial}</span>
        <span class="cw-su-copy"><span class="cw-wl-name">${esc(u.username || u.key)}</span>${tag}</span>
        ${pin}
        ${match}
        <span class="cw-wl-check cw-wl-scr"><input type="checkbox" id="${id}" data-key="${esc(u.key)}" data-name="${esc(u.username)}" ${on ? "checked" : ""} ${u.disabled && !on ? "disabled" : ""}><span class="cw-wl-box"></span></span>
      </label>`;
  }

  function render(state) {
    const label = LABELS[state.provider] || state.provider;
    const list = rows(state);
    state.root.innerHTML = `
      <div class="cw-wl cw-su">
        <div class="cw-wl-note"><strong>Optional.</strong> Only needed when you want to sync several users of this ${esc(label)} server, each with their own history. Leave everything unticked to keep syncing just the account of this connection, as before.</div>
        <details class="cw-su-help">
          <summary><span class="material-symbols-rounded" aria-hidden="true">help</span>How it works</summary>
          <ol>
            <li>Tick the users to include here, and do the same on the other media server.</li>
            <li>Turn on <strong>Sync all matched users</strong> in a sync pair, watcher route or webhook between the two servers.</li>
            <li>Each ticked user syncs with the user of the same name on the other server, each with their own history. Different names, like Carmen on Plex and carmii on Jellyfin? Tick carmii and set her <strong>Match as</strong> to Carmen.</li>
            <li>Ticked users join the user profile with the same name. Use <strong>Create user profiles</strong> below to make the missing ones, so each person sees only their own data.</li>
          </ol>
          <p><strong>Media servers only.</strong> This works between Plex, Emby and Jellyfin. Trackers such as Trakt or SIMKL need every person to sign in with their own account, so for those create a connection profile per person (pick the user in Settings) and a normal sync pair to that person's tracker profile.</p>
        </details>
        ${state.loadError ? `<div class="cw-wl-empty">Could not load users from ${esc(label)}: ${esc(state.loadError)}</div>` : ""}
        <div class="cw-su-tools">
          <span class="cw-su-search"><span class="material-symbols-rounded" aria-hidden="true">search</span><input type="search" data-role="search" placeholder="Search users" autocomplete="off" value="${esc(state.query)}"></span>
          <span class="cw-su-count" data-role="count"></span>
          <button type="button" class="cw-wl-load" data-act="all"><span class="material-symbols-rounded" aria-hidden="true">done_all</span>Select all</button>
          <button type="button" class="cw-wl-load" data-act="none"><span class="material-symbols-rounded" aria-hidden="true">remove_done</span>Clear</button>
        </div>
        <div class="cw-wl-scroll">
          <div class="cw-wl-rows">${list.length ? list.map((u, idx) => rowHTML(state, u, idx)).join("") : `<div class="cw-wl-empty">No users found on this server.</div>`}</div>
        </div>
        <div class="cw-wl-foot">
          <div class="cw-wl-note" data-role="status" aria-live="polite"></div>
          <button type="button" class="cw-wl-load" data-act="profiles"><span class="material-symbols-rounded" aria-hidden="true">person_add</span>Create user profiles</button>
        </div>
      </div>`;
    state.root.querySelectorAll("input[data-pin]").forEach((box) => { box.value = state.pins.get(box.dataset.pin) || ""; });
    state.root.querySelectorAll("input[data-match]").forEach((box) => { box.value = matchValue(state, box.dataset.match); });
    applySearch(state);
    refresh(state);
  }

  function savedMatch(state, key) {
    return txt(state.saved.find((s) => s.key === key)?.match_as);
  }

  function matchValue(state, key) {
    return state.matches.has(key) ? state.matches.get(key) : savedMatch(state, key);
  }

  function dirty(state) {
    const saved = new Set(state.saved.map((s) => s.key));
    if (saved.size !== state.selected.size || [...state.selected].some((k) => !saved.has(k))) return true;
    if ([...state.matches.entries()].some(([key, value]) => state.selected.has(key) && txt(value) !== savedMatch(state, key))) return true;
    return [...state.pins.values()].some((v) => txt(v));
  }

  function refresh(state) {
    const count = state.root.querySelector('[data-role="count"]');
    if (count) count.textContent = `${state.selected.size} of ${state.limit} selected`;
    const make = state.root.querySelector('[data-act="profiles"]');
    if (make) {
      make.disabled = state.busy || dirty(state) || !state.saved.length;
      make.title = dirty(state) ? "Save changes first" : !state.saved.length ? "Tick and save users first" : "Create a user profile for each ticked user that has none yet";
    }
    state.root.querySelectorAll(".cw-su-row[data-row]").forEach((row) => {
      const on = state.selected.has(row.dataset.row);
      row.classList.toggle("is-on", on);
      const pin = row.querySelector("input[data-pin]");
      if (pin) pin.hidden = !on;
      const matchBox = row.querySelector("input[data-match]");
      if (matchBox) matchBox.hidden = !on;
    });
    if (state.message) return setStatus(state, state.message, state.messageOk);
    const removed = state.saved.filter((s) => !state.selected.has(s.key));
    if (removed.length) {
      const who = removed.length === 1 ? removed[0].username : `${removed.length} users`;
      setStatus(state, `${who} will stop syncing when you save. If added again later, CrossWatch starts fresh for them.`, false);
    } else {
      setStatus(state, dirty(state) ? "Unsaved changes. Click Save changes to apply." : "", true);
    }
  }

  function setStatus(state, text, ok) {
    const node = state.root.querySelector('[data-role="status"]');
    if (!node) return;
    node.textContent = text;
    node.classList.toggle("cw-su-warn", !ok && !!text);
  }

  function applySearch(state) {
    const q = state.query.toLowerCase();
    state.root.querySelectorAll(".cw-su-row[data-row]").forEach((row) => {
      row.hidden = !!q && !String(row.dataset.search || "").includes(q);
    });
  }

  async function save(state) {
    const names = new Map(rows(state).map((u) => [u.key, u.username]));
    const cloudIds = new Map(state.server.filter((u) => u.cloud).map((u) => [u.key, u.cloud]));
    const users = [...state.selected].map((key) => {
      const row = { key, username: names.get(key) || key };
      const pin = txt(state.pins.get(key));
      if (pin) row.home_pin = pin;
      if (cloudIds.has(key)) row.cloud_account_id = cloudIds.get(key);
      row.match_as = txt(matchValue(state, key));
      return row;
    });
    const current = () => state.root.__cwSyncUsersState === state;
    try {
      const res = await getJSON(syncUsersUrl(state.provider, state.instance), { method: "PUT", body: JSON.stringify({ users }) });
      if (!current()) return;
      state.saved = Array.isArray(res.users) ? res.users.map((s) => ({ ...s, key: txt(s.key), username: txt(s.username) })) : [];
      state.selected = new Set(state.saved.map((s) => s.key));
      state.limit = res.limit || state.limit;
      state.pins.clear();
      state.matches.clear();
      state.message = state.saved.length ? `Saved. ${state.saved.length} user${state.saved.length === 1 ? "" : "s"} selected for syncing.` : "Saved. No users selected.";
      state.messageOk = true;
      render(state);
      state.message = "";
    } catch (err) {
      if (current()) setStatus(state, errorText(state.provider, err, state.limit), false);
      throw err;
    }
  }

  async function createProfiles(state) {
    const current = () => state.root.__cwSyncUsersState === state;
    state.busy = true;
    refresh(state);
    try {
      const res = await getJSON(`${syncUsersUrl(state.provider, state.instance)}/profiles`, { method: "POST" });
      if (!current()) return;
      const created = res.created | 0, linked = res.linked | 0, skipped = res.skipped | 0;
      const done = created || linked
        ? `Created ${created} user profile${created === 1 ? "" : "s"} and linked ${linked} user${linked === 1 ? "" : "s"}.`
        : "Every ticked user already has a user profile.";
      state.message = skipped
        ? `${done} Skipped ${skipped} user${skipped === 1 ? "" : "s"}: name longer than 40 characters, or the same Match as name used by more than one user on this server.`
        : done;
      state.messageOk = true;
    } catch (err) {
      if (current()) { state.message = errorText(state.provider, err, state.limit); state.messageOk = false; }
    } finally {
      state.busy = false;
      if (current()) { refresh(state); state.message = ""; }
    }
  }

  async function saveDirty(scope) {
    const host = scope && scope.querySelectorAll ? scope : d;
    for (const root of host.querySelectorAll("[data-cw-sync-users]")) {
      const provider = root.getAttribute("data-cw-sync-users");
      const states = [...pending.values()].filter((state) => state.provider === provider && state !== root.__cwSyncUsersState);
      if (root.__cwSyncUsersState) states.push(root.__cwSyncUsersState);
      for (const state of states) {
        if (!state.ready || !dirty(state)) { pending.delete(pendingKey(state.provider, state.instance)); continue; }
        try {
          await save(state);
        } catch (err) {
          const text = errorText(state.provider, err, state.limit);
          state.message = text;
          state.messageOk = false;
          const visible = root.__cwSyncUsersState;
          if (visible && visible !== state) setStatus(visible, `Users for connection profile "${connectionName(state.instance)}" were not saved: ${text}`, false);
          throw err;
        }
        pending.delete(pendingKey(state.provider, state.instance));
      }
    }
  }

  function connectionName(instance) {
    return instance === "default" ? "Default" : instance;
  }

  function discardPending() {
    pending.clear();
    d.querySelectorAll("[data-cw-sync-users]").forEach((root) => { root.__cwSyncUsersState = null; });
  }

  function wire(root) {
    if (root.__cwSyncUsersWired) return;
    root.__cwSyncUsersWired = true;
    root.addEventListener("input", (ev) => {
      const state = root.__cwSyncUsersState;
      if (!state) return;
      const pin = ev.target?.closest?.("input[data-pin]");
      if (pin) { state.message = ""; state.pins.set(pin.dataset.pin, pin.value); refresh(state); return; }
      const matchBox = ev.target?.closest?.("input[data-match]");
      if (matchBox) { state.message = ""; state.matches.set(matchBox.dataset.match, matchBox.value); refresh(state); return; }
      if (ev.target?.matches?.('[data-role="search"]')) { state.query = ev.target.value || ""; applySearch(state); }
    });
    root.addEventListener("change", (ev) => {
      const state = root.__cwSyncUsersState;
      const box = ev.target?.closest?.("input[data-key]");
      if (!state || !box) return;
      if (box.checked) state.selected.add(box.dataset.key);
      else state.selected.delete(box.dataset.key);
      state.message = "";
      refresh(state);
    });
    root.addEventListener("click", (ev) => {
      const state = root.__cwSyncUsersState;
      const act = ev.target?.closest?.("[data-act]")?.dataset?.act;
      if (!state || !act) return;
      if (act === "profiles") { createProfiles(state); return; }
      root.querySelectorAll(".cw-su-row[data-row]").forEach((row) => {
        if (row.hidden) return;
        const box = row.querySelector("input[data-key]");
        if (!box || box.disabled) return;
        box.checked = act === "all";
        if (act === "all") state.selected.add(box.dataset.key);
        else state.selected.delete(box.dataset.key);
      });
      refresh(state);
    });
  }

  async function mount(root, provider, instance) {
    const p = txt(provider).toLowerCase();
    if (!root || !PROVIDERS.has(p)) return;
    root.setAttribute("data-cw-sync-users", p);
    const prev = root.__cwSyncUsersState;
    if (prev && prev.ready && prev.provider === p && prev.instance === inst(instance) && dirty(prev)) return;
    if (prev && prev.ready && dirty(prev)) pending.set(pendingKey(prev.provider, prev.instance), prev);
    const kept = pending.get(pendingKey(p, inst(instance)));
    if (kept) {
      kept.root = root;
      root.__cwSyncUsersState = kept;
      wire(root);
      render(kept);
      return;
    }
    const state = { root, provider: p, instance: inst(instance), server: [], saved: [], selected: new Set(), pins: new Map(), matches: new Map(), limit: 25, loadError: "", query: "", message: "", messageOk: true, ready: false, busy: false };
    root.__cwSyncUsersState = state;
    wire(root);
    root.innerHTML = `<div class="cw-wl cw-su"><div class="cw-wl-empty">Loading users…</div></div>`;
    const [serverRes, savedRes] = await Promise.allSettled([
      getJSON(serverUsersUrl(p, state.instance)),
      getJSON(syncUsersUrl(p, state.instance)),
    ]);
    if (root.__cwSyncUsersState !== state) return;
    if (savedRes.status === "rejected") {
      root.innerHTML = `<div class="cw-wl cw-su"><div class="cw-wl-empty">${esc(errorText(p, savedRes.reason, state.limit))}</div></div>`;
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
    state.ready = true;
    render(state);
  }

  d.addEventListener("cw-auth-modal-closed", discardPending);

  w.CW = w.CW || {};
  w.CW.SyncUsers = { mount, saveDirty };
})(window, document);
