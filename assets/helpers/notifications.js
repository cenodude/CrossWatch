/* assets/helpers/notifications.js */
/* CrossWatch - shared notification menu, with read state, dismissals and alerts shared across devices. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(() => {
  const account = document.getElementById("cw-nav-profile-menu");
  if (!account || window.CW?.Notifications) return;
  const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const base = "/api/profile/notifications";
  const alertKey = "cw.mobile.notificationAlerts";
  const prefixes = {dismissed: "cw.notifications.dismissed.v1:", seen: "cw.notifications.seen.v1:"};
  const sources = new Map();
  const cache = new Map();
  const sending = new Set();
  let server = {items: [], errors: [], loading: true, denied: false};
  let visibleItems = [], openedFresh = null, migration = null;
  const userKey = () => String(window.CW?.AuthState?.user?.id || window.CW?.AuthState?.user?.username || "local");
  const scopeName = (scope = "profile") => String(scope === "account" ? "account" : (window.CW?.OverviewProfile?.id || window.CW?.AuthState?.profileId || "all"));
  const storageKey = (kind, scope, prefix = prefixes[kind]) => prefix + JSON.stringify([userKey(), scopeName(scope)]);
  const pendingKey = (kind, scope) => storageKey(kind, scope, `cw.notifications.pending.v1:${kind}:`);
  const syncedKey = (kind, scope) => storageKey(kind, scope, `cw.notifications.synced.v1:${kind}:`);
  const read = key => {
    try {
      const saved = JSON.parse(localStorage.getItem(key) || "[]");
      return Array.isArray(saved) ? saved.filter(value => typeof value === "string") : [];
    } catch { return []; }
  };
  const write = (key, entries) => { try { localStorage.setItem(key, JSON.stringify([...entries].slice(-2000))); } catch {} };
  function entries(kind, scope, pending = false) {
    const key = pending ? pendingKey(kind, scope) : storageKey(kind, scope);
    if (!cache.has(key)) cache.set(key, new Set(read(key)));
    return cache.get(key);
  }
  function persist(kind, scope) {
    for (const pending of [false, true]) {
      const set = entries(kind, scope, pending);
      while (set.size > 2000) set.delete(set.values().next().value);
      write(pending ? pendingKey(kind, scope) : storageKey(kind, scope), set);
    }
  }
  const has = (kind, item) => entries(kind, item.dismissalScope).has(item.notificationKey);
  const serverReady = () => typeof fetch === "function" && !window.cwIsAuthSetupPending?.() && !server.denied;
  function post(path, body, method = "POST", extra = {}) {
    return fetch(`${base}${path}`, {method, credentials: "same-origin", cache: "no-store",
      headers: {"Content-Type": "application/json", ...extra}, body: JSON.stringify(body)});
  }
  const viewAsProfile = () => document.documentElement.dataset.cwRole === "user" || window.CW?.OverviewProfile?.isDefaultView
    ? "" : String(window.CW?.OverviewProfile?.id || "").trim();
  function upload(kind, scope, keys) {
    const name = scopeName(scope), user = userKey(), id = JSON.stringify([user, name, kind, keys]);
    if (!keys.length || !serverReady() || sending.has(id)) return;
    sending.add(id);
    post(`/${kind}`, {scope: name, keys}).then(response => {
      if (!response.ok || userKey() !== user || scopeName(scope) !== name) return;
      const pending = entries(kind, scope, true);
      for (const key of keys) pending.delete(key);
      try { localStorage.setItem(syncedKey(kind, scope), "1"); } catch {}
      persist(kind, scope);
    }).catch(() => {}).finally(() => sending.delete(id));
  }
  function mark(kind, items) {
    for (const scope of new Set(items.map(item => item.dismissalScope))) {
      const keys = items.filter(item => item.dismissalScope === scope).map(item => item.notificationKey);
      const all = entries(kind, scope), pending = entries(kind, scope, true);
      for (const key of keys) { all.add(key); pending.add(key); }
      persist(kind, scope);
      upload(kind, scope, keys);
    }
  }
  function merge(state) {
    if (!state || typeof state !== "object") return;
    for (const scope of ["account", "profile"]) {
      for (const kind of ["dismissed", "seen"]) {
        const known = Array.isArray(state[scope]?.[kind]) ? state[scope][kind].filter(key => typeof key === "string") : [];
        const all = entries(kind, scope), pending = entries(kind, scope, true);
        let synced = false;
        try { synced = localStorage.getItem(syncedKey(kind, scope)) === "1"; } catch {}
        if (!synced) for (const key of all) pending.add(key);
        const remote = new Set(known);
        for (const key of [...pending]) if (remote.has(key)) pending.delete(key);
        const next = [...known, ...pending];
        all.clear();
        for (const key of next) all.add(key);
        persist(kind, scope);
        if (pending.size) upload(kind, scope, [...pending]);
        else if (!synced) { try { localStorage.setItem(syncedKey(kind, scope), "1"); } catch {} }
      }
    }
  }
  function clearItems(items) {
    mark("dismissed", items);
    render();
    panel.querySelector("[data-notification-clear]")?.focus();
    if (!visibleItems.length) panel.querySelector("[data-notifications-close]").focus();
  }
  let style = document.getElementById("cw-notifications-css");
  if (!style) {
    style = document.createElement("link");
    style.id = "cw-notifications-css";
    style.rel = "stylesheet";
    style.href = `/assets/css/notifications.css?v=${encodeURIComponent(window.APP_VERSION || "1")}`;
    document.head.append(style);
  }
  const bell = document.createElement("button");
  bell.id = "cw-notifications-button";
  bell.type = "button";
  bell.setAttribute("aria-label", "Notifications");
  bell.setAttribute("aria-haspopup", "dialog");
  bell.setAttribute("aria-expanded", "false");
  bell.setAttribute("aria-controls", "cw-notifications-panel");
  bell.innerHTML = '<svg class="cw-notifications-bell" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"><path d="M18 8a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9"/><path d="M9 17a3 3 0 0 0 6 0"/></svg><span class="cw-notifications-count" hidden aria-hidden="true"></span>';
  if (!style.sheet) {
    bell.style.visibility = "hidden";
    style.addEventListener("load", () => { bell.style.visibility = ""; }, { once: true });
  }
  account.before(bell);
  const panel = document.createElement("section");
  panel.id = "cw-notifications-panel";
  panel.hidden = true;
  panel.setAttribute("role", "dialog");
  panel.setAttribute("aria-labelledby", "cw-notifications-title");
  panel.innerHTML = '<div class="cw-notifications-head"><h2 id="cw-notifications-title">Notifications</h2><div class="cw-notifications-tools"><button type="button" data-notifications-clear-all hidden>Clear all</button><button type="button" data-notifications-settings aria-expanded="false" aria-controls="cw-notifications-settings" aria-label="Notification settings" title="Notification settings"><span class="material-symbols-rounded" aria-hidden="true">tune</span></button><button type="button" data-notifications-close aria-label="Close notifications"><span class="material-symbols-rounded" aria-hidden="true">close</span></button></div></div><div class="cw-notifications-settings" id="cw-notifications-settings" hidden><label><input type="checkbox" data-notification-banner><span>Banner for new notifications</span></label><label><input type="checkbox" data-notification-push><span>Push notifications on this device</span></label><p class="cw-notifications-push-note" data-push-note></p><button type="button" data-push-test hidden>Send a test notification</button></div><div class="cw-notifications-body"></div>';
  document.body.append(panel);
  const body = panel.querySelector(".cw-notifications-body");
  const badge = bell.querySelector(".cw-notifications-count");
  const settings = panel.querySelector(".cw-notifications-settings");
  const bannerToggle = panel.querySelector("[data-notification-banner]");
  const pushToggle = panel.querySelector("[data-notification-push]");
  const pushNote = panel.querySelector("[data-push-note]");
  const pushTest = panel.querySelector("[data-push-test]");
  const observed = new Map();
  const alert = document.createElement("div");
  alert.id = "cw-notification-alert";
  alert.hidden = true;
  alert.innerHTML = '<button type="button" data-alert-open><span role="status" aria-live="polite" aria-atomic="true"></span><small>View notifications</small></button><button type="button" data-alert-close aria-label="Dismiss notification banner">&#215;</button>';
  document.body.append(alert);
  let alertTimer;
  const alertsOn = () => { try { return localStorage.getItem(alertKey) === "1"; } catch { return false; } };
  function hideAlert() {
    alert.hidden = true;
    clearTimeout(alertTimer);
  }
  alert.querySelector("[data-alert-close]").addEventListener("click", hideAlert);
  alert.querySelector("[data-alert-open]").addEventListener("click", () => { hideAlert(); setOpen(true); });
  document.addEventListener("visibilitychange", () => { if (document.hidden) hideAlert(); });
  window.addEventListener("cw:mobile-notification-alerts-changed", () => { hideAlert(); bannerToggle.checked = alertsOn(); });

  function collected() {
    return [...sources].flatMap(([name, source]) => (source.items || []).map(item => ({
      ...item, source: name, dismissalScope: source.scope === "account" ? "account" : "profile",
      notificationKey: item.key || JSON.stringify([name, item.id || item.href, item.revision || ""]),
    })));
  }
  function notifyNewItems() {
    const scopeId = JSON.stringify([userKey(), scopeName()]);
    const items = collected();
    const seenBefore = observed.get(scopeId);
    observed.set(scopeId, new Set([...(seenBefore || []), ...items.map(item => item.notificationKey)]));
    if (!seenBefore || document.hidden || !panel.hidden || !alertsOn()) return;
    const fresh = items.filter(item => !item.quiet && !seenBefore.has(item.notificationKey) && !has("dismissed", item) && !has("seen", item));
    if (!fresh.length) return;
    hideAlert();
    alert.hidden = false;
    alert.querySelector("[role=status]").textContent = fresh.length === 1 ? String(fresh[0].title || "New notification") : `${fresh.length} new notifications`;
    alertTimer = setTimeout(hideAlert, 8000);
    try { navigator.vibrate?.([30, 60, 30]); } catch {}
  }

  function position() {
    if (panel.hidden) return;
    const rect = bell.getBoundingClientRect();
    const top = Math.min(rect.bottom + 10, window.innerHeight - 100);
    panel.style.top = `${Math.max(8, top)}px`;
    panel.style.right = `${Math.max(8, Math.min(window.innerWidth - rect.right, window.innerWidth - panel.offsetWidth - 8))}px`;
    panel.style.maxHeight = `${Math.max(80, window.innerHeight - top - 12)}px`;
  }

  function setOpen(open, restoreFocus = false) {
    const was = !panel.hidden;
    panel.hidden = !open;
    bell.setAttribute("aria-expanded", String(open));
    if (open) {
      hideAlert();
      if (!was) {
        const unseen = visibleItems.filter(item => !has("seen", item));
        openedFresh = new Set(unseen.map(item => item.notificationKey));
        if (unseen.length) mark("seen", unseen);
      }
      render();
      position();
      panel.querySelector("[data-notifications-close]").focus();
      window.dispatchEvent(new CustomEvent("cw:notifications-refresh"));
    } else {
      if (openedFresh) { openedFresh = null; render(); }
      if (restoreFocus) bell.focus();
    }
  }

  function render() {
    const values = [...sources.values()];
    const items = visibleItems = collected().filter(item => !has("dismissed", item));
    const unseen = items.filter(item => !has("seen", item));
    if (!panel.hidden && unseen.length) {
      for (const item of unseen) openedFresh?.add(item.notificationKey);
      mark("seen", unseen);
    }
    const count = panel.hidden ? unseen.length : 0;
    panel.querySelector("[data-notifications-clear-all]").hidden = !items.length;
    badge.hidden = !count;
    badge.textContent = count > 99 ? "99+" : String(count);
    bell.setAttribute("aria-label", count ? `Notifications (${count} new)` : "Notifications");
    const loading = server.loading || values.some(source => source.loading);
    const errors = [...server.errors, ...values.map(source => source.error)].filter(Boolean);
    const managed = document.documentElement.dataset.cwRole === "user";
    const canReview = !window.cwIsAuthSetupPending?.() && (!managed || document.documentElement.dataset.cwPermWrite === "on");
    const reviewsHref = document.getElementById("pairs_list") ? "#settings/sync" : `${managed ? "/?main=1" : "/"}#settings/sync`;
    const html = `${items.length ? `<ul>${items.map(item => {
      const href = String(item.href || "");
      let external = false;
      try {
        const url = new URL(href, location.href);
        external = url.origin !== location.origin;
        if (!href || (external && !(url.origin === "https://github.com" && url.pathname.startsWith("/cenodude/CrossWatch/releases/") && !url.username && !url.password))) return "";
      } catch { return ""; }
      const time = item.time ? new Date(item.time * 1000).toLocaleString() : "";
      const fresh = openedFresh?.has(item.notificationKey);
      return `<li${fresh ? ' class="is-new"' : ""}><a class="cw-notification" href="${esc(href)}"${external ? ' target="_blank" rel="noopener noreferrer"' : ""}><span class="material-symbols-rounded cw-notification-icon" aria-hidden="true">${esc(item.icon || "notifications")}</span><span class="cw-notification-copy"><strong>${fresh ? '<span class="cw-notification-new">New</span>' : ""}${esc(item.title)}</strong><span>${esc(item.detail)}</span>${time ? `<small>${esc(time)}</small>` : ""}<span class="cw-notification-action">${esc(item.action || "View")}<span class="material-symbols-rounded" aria-hidden="true">arrow_forward</span></span></span></a><button type="button" data-notification-clear="${esc(item.notificationKey)}" aria-label="${esc(`Clear notification: ${item.title}`)}" title="Clear notification"><span class="material-symbols-rounded" aria-hidden="true">close</span></button></li>`;
    }).join("")}</ul>` : !loading && !errors.length ? '<div class="cw-notifications-empty"><span class="material-symbols-rounded" aria-hidden="true">notifications_none</span><p>You’re all caught up</p><small>New notifications will appear here.</small></div>' : ""}${loading ? '<p class="cw-notifications-message" role="status">Loading notifications…</p>' : ""}${errors.length ? `<div class="cw-notifications-message"><p>${errors.map(esc).join("<br>")}</p><button type="button" data-notifications-retry>Try again</button></div>` : ""}${canReview ? `<a class="cw-notifications-footer" href="${reviewsHref}">View sync reviews<span class="material-symbols-rounded" aria-hidden="true">arrow_forward</span></a>` : ""}`;
    if (body.innerHTML !== html) body.innerHTML = html;
    position();
  }

  function receive(data, context) {
    if (!data || (context && (context.user !== userKey() || context.scope !== scopeName()))) return;
    if (data.denied) {
      server = {items: [], errors: [], loading: false, denied: true};
    } else if (Array.isArray(data.items)) {
      server = {items: data.items, errors: Array.isArray(data.errors) ? data.errors : [], loading: false, denied: false};
      merge(data.state);
      const state = data.signals?.tracker_migration ?? null;
      if (migration === "running" && state !== "running") window.dispatchEvent(new CustomEvent("cw:tracker-migrated"));
      migration = state;
    } else {
      server = {...server, errors: Array.isArray(data.errors) ? data.errors : [], loading: false};
    }
    for (const [name, source] of sources) if (source.server) sources.delete(name);
    for (const item of server.items) {
      if (!item || typeof item !== "object" || typeof item.source !== "string") continue;
      if (!sources.has(item.source)) sources.set(item.source, {server: true, scope: item.scope, items: []});
      sources.get(item.source).items.push(item);
    }
    render();
    notifyNewItems();
  }

  const pushSupported = () => typeof navigator !== "undefined" && "serviceWorker" in navigator && typeof window.PushManager === "function" && typeof window.Notification === "function";
  const keyBytes = value => {
    const text = atob(String(value).replace(/-/g, "+").replace(/_/g, "/") + "=".repeat((4 - String(value).length % 4) % 4));
    return Uint8Array.from(text, char => char.charCodeAt(0));
  };
  async function registration() {
    return (await navigator.serviceWorker.getRegistration("/")) || navigator.serviceWorker.register("/sw.js", {scope: "/"});
  }
  function unsupportedNote() {
    const agent = String(navigator.userAgent || "");
    const apple = /iPhone|iPad|iPod/.test(agent) || (/Macintosh/.test(agent) && Number(navigator.maxTouchPoints) > 1);
    const installed = !!window.matchMedia?.("(display-mode: standalone)")?.matches || navigator.standalone === true;
    if (apple && !installed) return "On iPhone and iPad, add CrossWatch to the Home Screen and open it from there to use push notifications.";
    if (apple) return "Push notifications need iOS or iPadOS 16.4 or later.";
    return "This browser does not support push notifications.";
  }
  async function pushStatus() {
    if (!window.isSecureContext) return {available: false, on: false, note: "Needs HTTPS. Open CrossWatch over a secure address to use push notifications."};
    if (!pushSupported()) return {available: false, on: false, note: unsupportedNote()};
    if (Notification.permission === "denied") return {available: true, on: false, note: "Notifications are blocked for this site. Allow them in the browser's site settings."};
    try {
      const subscription = await (await registration()).pushManager.getSubscription();
      const response = await fetch(`${base}/push`, {credentials: "same-origin", cache: "no-store"});
      const data = response.ok ? await response.json() : {};
      const on = !!subscription && Array.isArray(data.endpoints) && data.endpoints.includes(subscription.endpoint);
      return {available: true, on, note: on ? "This device gets new notifications even when CrossWatch is closed." : "Get new notifications on this device even when CrossWatch is closed."};
    } catch { return {available: true, on: false, note: "Push notifications are not available right now."}; }
  }
  function showPush(status) {
    pushToggle.disabled = !status.available;
    pushToggle.checked = !!status.on;
    pushTest.hidden = !status.on;
    pushNote.textContent = status.note;
  }
  async function pushState() { showPush(await pushStatus()); }
  async function saveSubscription(subscription) {
    const viewAs = viewAsProfile();
    const response = await post("/push", {subscription: subscription.toJSON(), profile: scopeName(), view_profile: String(window.CW?.OverviewProfile?.id || "").trim()},
      "POST", viewAs ? {"X-CW-View-As": viewAs} : {});
    if (!response.ok) throw new Error(((await response.json().catch(() => ({}))).error) || "Push notifications could not be turned on.");
  }
  async function enablePush() {
    if (await Notification.requestPermission() !== "granted") throw new Error("Notifications were not allowed for this site.");
    const response = await fetch(`${base}/push`, {credentials: "same-origin", cache: "no-store"});
    const data = response.ok ? await response.json() : {};
    if (!data.public_key) throw new Error("Push notifications are not available right now.");
    const manager = (await registration()).pushManager;
    const options = {userVisibleOnly: true, applicationServerKey: keyBytes(data.public_key)};
    let subscription;
    try { subscription = await manager.subscribe(options); } catch {
      await (await manager.getSubscription())?.unsubscribe();
      subscription = await manager.subscribe(options);
    }
    await saveSubscription(subscription);
  }
  async function disablePush() {
    const subscription = await (await registration()).pushManager.getSubscription();
    if (!subscription) return;
    await post("/push", {endpoint: subscription.endpoint}, "DELETE").catch(() => {});
    await subscription.unsubscribe().catch(() => {});
  }
  async function updatePushProfile() {
    if (!pushSupported() || !window.isSecureContext || Notification.permission !== "granted") return;
    try {
      const subscription = await (await registration()).pushManager.getSubscription();
      if (subscription) await saveSubscription(subscription);
    } catch {}
  }
  async function setPush(on) {
    let failure = "";
    try {
      if (on) await enablePush(); else await disablePush();
    } catch (error) { failure = String(error?.message || "Push notifications could not be changed."); }
    window.dispatchEvent(new CustomEvent("cw:notification-push-changed"));
    const status = await pushStatus();
    return failure ? {...status, note: failure} : status;
  }
  async function testPush() {
    try {
      const subscription = await (await registration()).pushManager.getSubscription();
      const response = await post("/push/test", {endpoint: subscription?.endpoint || ""});
      const data = await response.json().catch(() => ({}));
      return response.ok ? "Test notification sent. If nothing appears, check that notifications are allowed for this browser in the device's system settings." : String(data.error || "The test notification could not be sent.");
    } catch { return "The test notification could not be sent."; }
  }
  pushToggle.addEventListener("change", async () => {
    pushToggle.disabled = true;
    showPush(await setPush(pushToggle.checked));
  });
  pushTest.addEventListener("click", async () => {
    pushTest.disabled = true;
    pushNote.textContent = await testPush();
    pushTest.disabled = false;
  });
  window.addEventListener("cw:notification-push-changed", () => { if (!settings.hidden) void pushState(); });
  bannerToggle.checked = alertsOn();
  bannerToggle.addEventListener("change", () => {
    try { localStorage.setItem(alertKey, bannerToggle.checked ? "1" : "0"); } catch {}
    window.dispatchEvent(new CustomEvent("cw:mobile-notification-alerts-changed"));
  });
  try {
    navigator.serviceWorker?.addEventListener?.("message", event => {
      if (event.data?.type === "cw:notifications-refresh") window.dispatchEvent(new CustomEvent("cw:notifications-refresh"));
    });
  } catch {}

  bell.addEventListener("click", () => setOpen(panel.hidden));
  panel.addEventListener("click", event => {
    if (event.target.closest("[data-notifications-clear-all]")) clearItems(visibleItems);
    const clear = event.target.closest("[data-notification-clear]");
    if (clear) clearItems(visibleItems.filter(item => item.notificationKey === clear.dataset.notificationClear));
    if (event.target.closest("[data-notifications-close]")) setOpen(false, true);
    if (event.target.closest("[data-notifications-retry]")) window.dispatchEvent(new CustomEvent("cw:notifications-refresh"));
    const toggle = event.target.closest("[data-notifications-settings]");
    if (toggle) {
      settings.hidden = !settings.hidden;
      toggle.setAttribute("aria-expanded", String(!settings.hidden));
      if (!settings.hidden) { bannerToggle.checked = alertsOn(); void pushState(); }
      position();
    }
    if (event.target.closest("a")) setOpen(false);
  });
  document.addEventListener("pointerdown", event => {
    if (!panel.contains(event.target) && !bell.contains(event.target)) setOpen(false);
  }, true);
  document.addEventListener("keydown", event => {
    if (event.key === "Escape" && !panel.hidden) { event.preventDefault(); setOpen(false, true); }
  });
  document.addEventListener("focusin", event => {
    if (!panel.hidden && !panel.contains(event.target) && !bell.contains(event.target)) setOpen(false);
  });
  document.addEventListener("tab-changed", () => setOpen(false));
  window.addEventListener("hashchange", () => setOpen(false));
  window.addEventListener("resize", position);
  window.addEventListener("scroll", position, true);
  function reset() {
    sources.clear(); observed.clear(); cache.clear(); hideAlert(); setOpen(false);
    server = {items: [], errors: [], loading: true, denied: false}; migration = null;
    render();
  }
  window.addEventListener("auth-changed", reset);
  window.addEventListener("cw:auth-state-changed", reset);
  window.addEventListener("cw:overview-profile-changed", () => {
    hideAlert();
    for (const [name, source] of sources) if (source.scope !== "account") sources.delete(name);
    server = {...server, items: server.items.filter(item => item?.scope === "account"), loading: true};
    setOpen(false); render();
    void updatePushProfile();
  });
  window.addEventListener("storage", event => {
    if (event.key === null || String(event.key).startsWith("cw.notifications.")) { cache.clear(); render(); }
    if (event.key === alertKey) bannerToggle.checked = alertsOn();
  });
  (window.CW ||= {}).Notifications = {
    setSource(name, value) { sources.set(name, value); render(); notifyNewItems(); },
    removeSource(name) { sources.delete(name); render(); },
    receive,
    scope: scopeName,
    push: {status: pushStatus, set: setPush, test: testPush},
  };
  const pending = window.CW.NotificationFeed?.last?.();
  if (pending) receive(pending.data, pending.context); else render();
})();
