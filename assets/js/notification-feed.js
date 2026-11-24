/* assets/js/notification-feed.js */
/* CrossWatch - One server request that feeds every notification source to the notification menu */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(() => {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  if (window.CW?.NotificationFeed) return;
  const url = "/api/profile/notifications";
  let timer, controller, last = null, delay = 10000;
  const user = () => String(window.CW?.AuthState?.user?.id || window.CW?.AuthState?.user?.username || "local");
  const scope = () => String(window.CW?.OverviewProfile?.id || window.CW?.AuthState?.profileId || "all");
  const viewProfile = () => String(window.CW?.OverviewProfile?.id || "").trim();
  function deliver(data, context) {
    last = {data, context};
    window.CW?.Notifications?.receive?.(data, context);
  }
  async function refresh({force = false} = {}) {
    clearTimeout(timer);
    if (!document.hidden && controller && !controller.signal.aborted && !force) return;
    controller?.abort();
    if (document.hidden) { controller = null; return; }
    const request = controller = new AbortController();
    let allowed = true;
    try {
      await window.CW?.AuthState?.ready?.();
      await window.__cwAuthBootstrapPromise;
      await window.CW?.OverviewProfile?.ready;
      if (request.signal.aborted) return;
      const context = {user: user(), scope: scope()};
      if (window.cwIsAuthSetupPending?.()) { allowed = false; deliver({ok: false, denied: true, items: []}, context); return; }
      const response = await fetch(`${url}?profile=${encodeURIComponent(context.scope)}&view_profile=${encodeURIComponent(viewProfile())}`,
        {credentials: "same-origin", cache: "no-store", signal: request.signal});
      if ([401, 403].includes(response.status)) { allowed = false; deliver({ok: false, denied: true, items: []}, context); return; }
      if (!response.ok) throw new Error(T("common.notifications.unavailable"));
      const data = await response.json();
      if (request.signal.aborted) return;
      delay = Math.max(2, Number(data?.poll) || 10) * 1000;
      deliver(data, context);
    } catch {
      if (!request.signal.aborted) deliver({ok: false, items: null, errors: [T("common.notifications.loadFailed")]}, {user: user(), scope: scope()});
    } finally {
      if (controller === request) {
        controller = null;
        if (!request.signal.aborted && !document.hidden && allowed) timer = setTimeout(refresh, delay);
      }
    }
  }
  const forced = () => refresh({force: true});
  for (const name of ["cw:notifications-refresh", "focus"]) window.addEventListener(name, () => refresh());
  for (const name of ["auth-changed", "cw:auth-state-changed", "cw:overview-profile-changed", "cw:maintenance-job-changed",
    "cw:tracker-check-changed", "cw:plex-recovery-changed"]) window.addEventListener(name, forced);
  document.addEventListener("visibilitychange", () => refresh());
  (window.CW ||= {}).NotificationFeed = {refresh, scope, viewProfile, last: () => last};
  refresh();
})();
