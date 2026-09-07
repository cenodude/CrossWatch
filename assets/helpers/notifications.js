/* assets/helpers/notifications.js */
/* CrossWatch - shared notification menu, with replaceable notification sources. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(() => {
  const account = document.getElementById("cw-nav-profile-menu");
  if (!account || window.CW?.Notifications) return;
  const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const sources = new Map();
  const dismissedScopes = new Map();
  const storagePrefix = "cw.notifications.dismissed.v1:";
  let visibleItems = [];
  const scopeKey = (scope = "profile") => storagePrefix + JSON.stringify([
    window.CW?.AuthState?.user?.id || window.CW?.AuthState?.user?.username || "local",
    scope === "account" ? "account" : (window.CW?.OverviewProfile?.id || window.CW?.AuthState?.profileId || "all"),
  ]);
  function dismissed(scope) {
    const key = scopeKey(scope);
    if (!dismissedScopes.has(key)) {
      let entries = [];
      try {
        const saved = JSON.parse(localStorage.getItem(key) || "[]");
        if (Array.isArray(saved)) entries = saved.filter(value => typeof value === "string");
      } catch {}
      dismissedScopes.set(key, new Set(entries));
    }
    return dismissedScopes.get(key);
  }
  function clearItems(items) {
    for (const scope of new Set(items.map(item => item.dismissalScope))) {
      const entries = dismissed(scope);
      for (const item of items.filter(item => item.dismissalScope === scope)) entries.add(item.notificationKey);
      // Bound browser storage; the newest dismissals are retained across reloads.
      while (entries.size > 2000) entries.delete(entries.values().next().value);
      try { localStorage.setItem(scopeKey(scope), JSON.stringify([...entries])); } catch {}
    }
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
  // Older shells may still load this stylesheet lazily. Never show an unstyled button.
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
  panel.innerHTML = '<div class="cw-notifications-head"><h2 id="cw-notifications-title">Notifications</h2><div class="cw-notifications-tools"><button type="button" data-notifications-clear-all hidden>Clear all</button><button type="button" data-notifications-close aria-label="Close notifications"><span class="material-symbols-rounded" aria-hidden="true">close</span></button></div></div><div class="cw-notifications-body"></div>';
  document.body.append(panel);
  const body = panel.querySelector(".cw-notifications-body");
  const badge = bell.querySelector(".cw-notifications-count");

  function position() {
    if (panel.hidden) return;
    const rect = bell.getBoundingClientRect();
    const top = Math.min(rect.bottom + 10, window.innerHeight - 100);
    panel.style.top = `${Math.max(8, top)}px`;
    panel.style.right = `${Math.max(8, Math.min(window.innerWidth - rect.right, window.innerWidth - panel.offsetWidth - 8))}px`;
    panel.style.maxHeight = `${Math.max(80, window.innerHeight - top - 12)}px`;
  }

  function setOpen(open, restoreFocus = false) {
    panel.hidden = !open;
    bell.setAttribute("aria-expanded", String(open));
    if (open) {
      position();
      panel.querySelector("[data-notifications-close]").focus();
      window.dispatchEvent(new CustomEvent("cw:notifications-refresh"));
    } else if (restoreFocus) bell.focus();
  }

  function render() {
    const values = [...sources.values()];
    const items = visibleItems = [...sources].flatMap(([name, source]) => (source.items || []).map(item => ({
      ...item, dismissalScope: source.scope === "account" ? "account" : "profile",
      notificationKey: JSON.stringify([name, item.id || item.href, item.revision || ""]),
    }))).filter(item => !dismissed(item.dismissalScope).has(item.notificationKey));
    panel.querySelector("[data-notifications-clear-all]").hidden = !items.length;
    badge.hidden = !items.length;
    badge.textContent = items.length > 99 ? "99+" : String(items.length);
    bell.setAttribute("aria-label", items.length ? `Notifications (${items.length})` : "Notifications");
    const loading = values.some(source => source.loading);
    const errors = values.map(source => source.error).filter(Boolean);
    const html = `${items.length ? `<ul>${items.map(item => {
      // Only local pages and official CrossWatch releases are valid targets.
      const href = String(item.href || "");
      let external = false;
      try {
        const url = new URL(href, location.href);
        external = url.origin !== location.origin;
        if (!href || (external && !(url.origin === "https://github.com" && url.pathname.startsWith("/cenodude/CrossWatch/releases/") && !url.username && !url.password))) return "";
      } catch { return ""; }
      const time = item.time ? new Date(item.time * 1000).toLocaleString() : "";
      return `<li><a class="cw-notification" href="${esc(href)}"${external ? ' target="_blank" rel="noopener noreferrer"' : ""}><span class="material-symbols-rounded cw-notification-icon" aria-hidden="true">${esc(item.icon || "notifications")}</span><span class="cw-notification-copy"><strong>${esc(item.title)}</strong><span>${esc(item.detail)}</span>${time ? `<small>${esc(time)}</small>` : ""}<span class="cw-notification-action">${esc(item.action || "View")}<span class="material-symbols-rounded" aria-hidden="true">arrow_forward</span></span></span></a><button type="button" data-notification-clear="${esc(item.notificationKey)}" aria-label="${esc(`Clear notification: ${item.title}`)}" title="Clear notification"><span class="material-symbols-rounded" aria-hidden="true">close</span></button></li>`;
    }).join("")}</ul>` : !loading && !errors.length ? '<div class="cw-notifications-empty"><span class="material-symbols-rounded" aria-hidden="true">notifications_none</span><p>You’re all caught up</p><small>New notifications will appear here.</small></div>' : ""}${loading ? '<p class="cw-notifications-message" role="status">Loading notifications…</p>' : ""}${errors.length ? `<div class="cw-notifications-message"><p>${errors.map(esc).join("<br>")}</p><button type="button" data-notifications-retry>Try again</button></div>` : ""}${document.getElementById("pairs_list") ? '<a class="cw-notifications-footer" href="#settings/sync">View sync reviews<span class="material-symbols-rounded" aria-hidden="true">arrow_forward</span></a>' : ""}`;
    if (body.innerHTML !== html) body.innerHTML = html;
    position();
  }

  bell.addEventListener("click", () => setOpen(panel.hidden));
  panel.addEventListener("click", event => {
    if (event.target.closest("[data-notifications-clear-all]")) clearItems(visibleItems);
    const clear = event.target.closest("[data-notification-clear]");
    if (clear) clearItems(visibleItems.filter(item => item.notificationKey === clear.dataset.notificationClear));
    if (event.target.closest("[data-notifications-close]")) setOpen(false, true);
    if (event.target.closest("[data-notifications-retry]")) window.dispatchEvent(new CustomEvent("cw:notifications-refresh"));
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
  function reset() { sources.clear(); setOpen(false); render(); }
  window.addEventListener("auth-changed", reset);
  window.addEventListener("cw:auth-state-changed", reset);
  window.addEventListener("cw:overview-profile-changed", () => {
    for (const [name, source] of sources) if (source.scope !== "account") sources.delete(name);
    setOpen(false); render();
  });
  window.addEventListener("storage", event => {
    if (event.key === null || (event.key === scopeKey() || event.key === scopeKey("account"))) {
      dismissedScopes.clear(); render();
    }
  });
  (window.CW ||= {}).Notifications = {
    setSource(name, value) { sources.set(name, value); render(); },
    removeSource(name) { sources.delete(name); render(); },
  };
  render();
})();
