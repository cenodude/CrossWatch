/* assets/helpers/update-notifications.js */
/* CrossWatch - Release announcements in the shared notification menu */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(() => {
  let checkedAt = 0, controller;
  function publish(status) {
    if (!status || status.unavailable) return;
    const latest = String(status.latest || status.latest_version || "").trim().replace(/^v/i, "");
    const current = String(status.current || status.current_version || "").trim();
    const available = status.available ?? status.update_available;
    window.CW?.Notifications?.setSource("updates", {scope:"account", items: available && latest ? [{
      id: latest, title: `CrossWatch v${latest} is available`,
      detail: current ? `Installed version: v${current.replace(/^v/i, "")}` : "A new CrossWatch release is available.",
      icon: "system_update_alt", action: "View release notes",
      href: `https://github.com/cenodude/CrossWatch/releases/tag/v${encodeURIComponent(latest)}`,
    }] : []});
  }
  async function refresh() {
    if (document.hidden || controller || Date.now() - checkedAt < 300000) return;
    const request = controller = new AbortController();
    const timeout = setTimeout(() => request.abort(), 15000);
    try {
      await window.__cwAuthBootstrapPromise;
      if (request.signal.aborted || window.cwIsAuthSetupPending?.()) return;
      const response = await fetch("/api/version", {credentials:"same-origin", cache:"no-store", signal:request.signal});
      checkedAt = Date.now();
      if (response.ok) {
        const status = await response.json();
        if (!request.signal.aborted) publish(status);
      }
    } catch {} finally {
      clearTimeout(timeout);
      if (controller === request) controller = null;
    }
  }
  document.addEventListener("cw-update-status", event => publish(event.detail));
  document.addEventListener("visibilitychange", refresh);
  window.addEventListener("cw:notifications-refresh", refresh);
  const reset = () => { controller?.abort(); controller=null; checkedAt=0; refresh(); };
  window.addEventListener("auth-changed", reset);
  window.addEventListener("cw:auth-state-changed", reset);
  setInterval(refresh, 300000);
  refresh();
})();
