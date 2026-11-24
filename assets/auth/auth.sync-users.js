/* assets/auth/auth.sync-users.js */
/* CrossWatch - Identities tab for media server connections (Plex, Emby, Jellyfin, Silo, Tofa) */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function (w, d) {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  if (w.CW?.SyncUsers) return;

  const PROVIDERS = new Set(["plex", "emby", "jellyfin", "silo", "tofa"]);
  const USER_PICK = new Set(["plex", "emby", "jellyfin"]);
  const KIND_LABEL = kind => ["Owner", "Friend", "Home", "Connected", "Account", "Shared", "Child", "Disabled", "Admin"].includes(kind) ? T(`connections.auth.su.kind.${kind}`) : kind;
  const LABELS = { plex: "Plex", emby: "Emby", jellyfin: "Jellyfin", silo: "Silo", tofa: "Tofa" };
  const KIND_CLASS = { Owner: "is-owner", Admin: "is-owner", Connected: "is-owner", Home: "is-home", Child: "is-home", Friend: "is-friend", Shared: "is-friend", Account: "is-friend", Disabled: "is-disabled" };

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
    return provider === "plex" ? `/api/plex/pickusers${q}` : provider === "silo" ? `/api/silo/users${q}&accounts=1` : `/api/${provider}/users${q}`;
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
    if (provider === "silo" || provider === "tofa") {
      const key = txt(u?.id);
      const account = txt(u?.account);
      const kind = u?.connected ? "Connected" : account ? "Account" : u?.shared === true ? "Shared" : u?.is_child ? "Child" : "";
      return key ? { key, username: txt(u?.username), kind, account, home: u?.has_pin === true && !account, disabled: false } : null;
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
      return T("connections.auth.su.limit", {max});
    }
    if (code === "not_found") return T("connections.su.notFound");
    if (code === "unsupported_provider") return T("connections.auth.su.unsupported", {name: LABELS[provider] || provider});
    if (code === "invalid_pin") return T("connections.su.invalidPin");
    if (code === "pin_locked") return T("connections.su.pinLocked");
    if (code === "silo_unavailable") return T("connections.su.siloUnavailable");
    if (code === "tofa_unavailable") return T("connections.su.tofaUnavailable");
    if (code === "tofa_sync_key_failed") return T("connections.su.tofaKey");
    if (code === "silo_account_session_failed") return T("connections.su.siloSession");
    if (code === "rate_limited") return T("connections.su.busy");
    return code || T("connections.su.wrong");
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
    const keyLost = state.saved.find((s) => s.key === u.key)?.key_invalid === true;
    const kindTitle = u.kind === "Account" && u.account ? ` title="${TH("connections.auth.su.separate", {name: u.account})}"` : "";
    const tag = u.missing ? `<span class="cw-su-tag is-missing">${TH("connections.su.notOnServer")}</span>` : u.kind ? `<span class="cw-su-tag ${KIND_CLASS[u.kind] || ""}"${kindTitle}>${esc(KIND_LABEL(u.kind))}</span>` : "";
    const keyTag = keyLost && !u.missing
      ? state.provider === "silo"
        ? `<span class="cw-su-tag is-missing" title="${TH("connections.su.sessionTitle")}">${TH("connections.su.sessionEnded")}</span>`
        : `<span class="cw-su-tag is-missing" title="${TH("connections.su.keyTitle")}">${TH("connections.su.keyRevoked")}</span>`
      : "";
    const pin = (state.provider === "plex" || state.provider === "silo" || state.provider === "tofa") && u.home
      ? `<input type="password" class="cw-su-pin" inputmode="numeric" maxlength="8" autocomplete="off" data-pin="${esc(u.key)}" placeholder="${savedPin ? T("connections.su.pinSaved") : (state.provider === "silo" || state.provider === "tofa") ? T("connections.su.profilePin") : T("connections.su.pinIfNeeded")}" ${on ? "" : "hidden"} data-lpignore="true" data-1p-ignore="true" data-bwignore="true">`
      : "";
    const usedIn = state.saved.find((s) => s.key === u.key)?.used_in || [];
    const usage = state.saved.some((s) => s.key === u.key)
      ? `<span class="cw-su-usage ${usedIn.length ? "is-used" : ""}" title="${esc(usedIn.join(", "))}">${usedIn.length ? TH("connections.auth.su.linkedIn", {where: usedIn.join(", ")}) : TH("connections.auth.su.notLinked")}</span>`
      : "";
    return `
      <label class="cw-wl-row cw-su-row ${on ? "is-on" : ""}" for="${id}" data-row="${esc(u.key)}" data-search="${esc(u.username.toLowerCase())}">
        <span class="cw-wl-ic cw-su-avatar" aria-hidden="true">${initial}</span>
        <span class="cw-su-copy"><span class="cw-wl-name">${esc(u.username || u.key)}</span>${tag}${keyTag}${usage}</span>
        ${pin}
        <span class="cw-wl-check cw-wl-scr"><input type="checkbox" id="${id}" data-key="${esc(u.key)}" data-name="${esc(u.username)}" ${on ? "checked" : ""} ${u.disabled && !on ? "disabled" : ""}><span class="cw-wl-box"></span></span>
      </label>`;
  }

  function render(state) {
    const label = LABELS[state.provider] || state.provider;
    const list = rows(state);
    state.root.innerHTML = `
      <div class="cw-wl cw-su">
        <div class="cw-wl-note"><strong>${TH("connections.auth.su.optional")}</strong> ${TH("connections.auth.su.tick", {name: label})}${state.provider === "silo" ? ` ${TH("connections.auth.su.siloAdmins")}` : ""}</div>
        <div class="cw-wl-note cw-su-warn">${TH(USER_PICK.has(state.provider) ? "connections.auth.su.trackerSettings" : "connections.auth.su.trackerAuth")}</div>
        ${state.loadError ? `<div class="cw-wl-empty">${TH("connections.auth.su.loadFailed", {name: label, error: state.loadError})}</div>` : ""}
        <div class="cw-su-tools">
          <span class="cw-su-search"><span class="material-symbols-rounded" aria-hidden="true">search</span><input type="search" data-role="search" placeholder="${TH("connections.su.search")}" autocomplete="off" value="${esc(state.query)}"></span>
          <span class="cw-su-count" data-role="count"></span>
          <button type="button" class="cw-wl-load" data-act="all"><span class="material-symbols-rounded" aria-hidden="true">done_all</span>${TH("connections.su.selectAll")}</button>
          <button type="button" class="cw-wl-load" data-act="none"><span class="material-symbols-rounded" aria-hidden="true">remove_done</span>${TH("connections.su.clear")}</button>
        </div>
        <div class="cw-wl-scroll">
          <div class="cw-wl-rows">${list.length ? list.map((u, idx) => rowHTML(state, u, idx)).join("") : `<div class="cw-wl-empty">${TH("connections.su.noPeople")}</div>`}</div>
        </div>
        <div class="cw-wl-foot">
          <div class="cw-wl-note" data-role="status" aria-live="polite"></div>
        </div>
        <details class="cw-su-more">
          <summary><span class="material-symbols-rounded" aria-hidden="true">person_add</span>${TH("connections.su.optionalProfiles")}</summary>
          <p>${TH("connections.su.profilesHelp")}</p>
          <button type="button" class="cw-wl-load" data-act="profiles"><span class="material-symbols-rounded" aria-hidden="true">person_add</span>${TH("connections.su.createProfiles")}</button>
        </details>
      </div>`;
    state.root.querySelectorAll("input[data-pin]").forEach((box) => { box.value = state.pins.get(box.dataset.pin) || ""; });
    applySearch(state);
    refresh(state);
  }

  function dirty(state) {
    const saved = new Set(state.saved.map((s) => s.key));
    if (saved.size !== state.selected.size || [...state.selected].some((k) => !saved.has(k))) return true;
    if (state.saved.some((s) => s.key_invalid === true && state.selected.has(s.key))) return true;
    return [...state.pins.values()].some((v) => txt(v));
  }

  function refresh(state) {
    const count = state.root.querySelector('[data-role="count"]');
    if (count) count.textContent = T("connections.auth.su.count", {selected: state.selected.size, limit: state.limit});
    const make = state.root.querySelector('[data-act="profiles"]');
    if (make) {
      make.disabled = state.busy || dirty(state) || !state.saved.length;
      make.title = dirty(state) ? T("connections.su.saveFirst") : !state.saved.length ? T("connections.su.tickFirst") : T("connections.su.createEach");
    }
    state.root.querySelectorAll(".cw-su-row[data-row]").forEach((row) => {
      const on = state.selected.has(row.dataset.row);
      row.classList.toggle("is-on", on);
      const pin = row.querySelector("input[data-pin]");
      if (pin) pin.hidden = !on;
    });
    if (state.message) return setStatus(state, state.message, state.messageOk);
    const removed = state.saved.filter((s) => !state.selected.has(s.key));
    if (removed.length) {
      const who = removed.length === 1 ? removed[0].username : T("connections.auth.su.people", {count: removed.length});
      const where = [...new Set(removed.flatMap((r) => r.used_in || []))];
      setStatus(state, `${T("connections.auth.su.willStop", {who})}${where.length ? ` ${T("connections.auth.su.breaks", {where: where.join(", ")})}` : ""} ${T("connections.auth.su.fresh")}`, false);
    } else {
      setStatus(state, dirty(state) ? T("connections.su.unsaved") : "", true);
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
      state.message = state.saved.length ? T("connections.auth.su.saved", {count: state.saved.length}) : T("connections.auth.su.savedNone");
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
        ? T("connections.auth.su.created", {created: T("connections.auth.su.profiles", {count: created}), linked: T("connections.auth.su.identities", {count: linked})})
        : T("connections.su.everyHas");
      state.message = skipped
        ? `${done} ${T("connections.auth.su.skipped", {n: skipped})}`
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
          if (visible && visible !== state) setStatus(visible, T("connections.auth.su.notSaved", {name: connectionName(state.instance), error: text}), false);
          throw err;
        }
        pending.delete(pendingKey(state.provider, state.instance));
      }
    }
  }

  function connectionName(instance) {
    return instance === "default" ? T("connections.auth.default") : instance;
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
    const state = { root, provider: p, instance: inst(instance), server: [], saved: [], selected: new Set(), pins: new Map(), limit: 25, loadError: "", query: "", message: "", messageOk: true, ready: false, busy: false };
    root.__cwSyncUsersState = state;
    wire(root);
    root.innerHTML = `<div class="cw-wl cw-su"><div class="cw-wl-empty">${TH("connections.su.loading")}</div></div>`;
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
