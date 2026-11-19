/* assets/helpers/database-notifications.js */
/* CrossWatch - Local database busy and storage warnings in the shared notification menu */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(() => {
  let timer, controller;
  const duration = seconds => seconds >= 120 ? `${Math.round(seconds / 60)} minutes` : `${seconds} seconds`;
  const MB = 1024 * 1024;
  const size = bytes => bytes >= 1024 * MB ? `${(bytes / (1024 * MB)).toFixed(1)} GB` : `${Math.round(bytes / MB)} MB`;
  function items(status) {
    if (!status) return [];
    const managed = document.documentElement.dataset.cwRole === "user";
    const out = [];
    if (status.busy) out.push({
      id: `busy:${status.operation || ""}:${status.since || ""}`,
      title: "Database busy",
      detail: `A database write (${status.operation || "unknown"}) has been running for ${duration(Number(status.seconds) || 0)}. Pages can load slowly until it finishes.`,
      icon: "hourglass_top", time: status.since || 0,
      href: managed ? "#" : "/#settings/maintenance", action: managed ? "Got it" : "Open maintenance",
    });
    if (status.network_filesystem && !managed) out.push({
      id: `network:${status.filesystem || ""}`,
      title: "Database on a network share",
      detail: `The local database is on ${status.filesystem || "a network filesystem"}. SQLite locking is unreliable there, which can make CrossWatch slow or stuck. Move /config to local storage.`,
      icon: "lan", href: "/#settings/maintenance", action: "Open maintenance",
    });
    const reclaimable = Number(status.reclaimable_bytes) || 0;
    if (status.reclaim_suggested && !managed) out.push({
      id: `reclaim:${Math.floor(reclaimable / (100 * MB))}`,
      title: "Database can be compacted",
      detail: `${size(reclaimable)} of the ${size(Number(status.file_bytes) || 0)} local database is unused space. Run Optimize database under Events in Maintenance to free it.`,
      icon: "compress", href: "/#settings/maintenance", action: "Open maintenance",
    });
    return out;
  }
  function publish(status) {
    window.CW?.Notifications?.setSource("database", {scope: "account", items: items(status)});
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
      if (request.signal.aborted || window.cwIsAuthSetupPending?.()) return;
      const response = await fetch("/api/health/database", {credentials: "same-origin", cache: "no-store", signal: request.signal});
      if ([401, 403].includes(response.status)) { allowed = false; publish(null); return; }
      if (response.ok) {
        const status = await response.json();
        if (!request.signal.aborted) publish(status);
      }
    } catch {} finally {
      if (controller === request) {
        controller = null;
        if (!request.signal.aborted && !document.hidden && allowed) timer = setTimeout(refresh, 15000);
      }
    }
  }
  window.addEventListener("cw:notifications-refresh", refresh);
  document.addEventListener("visibilitychange", refresh);
  for (const event of ["auth-changed", "cw:auth-state-changed"]) window.addEventListener(event, () => { publish(null); refresh({force: true}); });
  refresh();
})();
