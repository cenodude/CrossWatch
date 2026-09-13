/* assets/js/plex-recovery-notifications.js */
/* CrossWatch - Resume background Plex recovery from notifications */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(() => {
  let timer, controller;
  function render(job = null, error = "") {
    const reading = job?.status === "reading";
    const auto=job?.auto_match;
    const matching=auto?.status === "running";
    const failed = ["error", "cancelled"].includes(job?.status);
    window.CW?.Notifications?.setSource("plex-recovery", {scope:"account", error, items:job ? [{
      id:job.id, revision:`${job.status}:${job.imported || 0}:${auto?.id || ""}:${auto?.status || ""}`,
      title:"Plex history recovery",
      detail:reading ? "Scanning older history" : failed ? job.message : auto?.id ? matching ? `Auto match: ${auto.done} / ${auto.total} titles checked` : auto.message : job.imported ? `${job.imported} ${job.imported === 1 ? "item" : "items"} imported into CrossWatch` : "Ready to review. Nothing has been imported.",
      icon:reading || matching ? "sync" : failed || auto?.status === "paused" ? "error_outline" : "history",
      href:"/#import_export?recovery=1", action:reading || matching ? "View progress" : "Open recovery",
    }] : []});
  }
  async function refresh() {
    clearTimeout(timer);
    controller?.abort();
    if (document.hidden) return;
    const request = controller = new AbortController();
    let allowed = true;
    try {
      await window.__cwAuthBootstrapPromise;
      await window.CW?.OverviewProfile?.ready;
      if (request.signal.aborted) return;
      if (window.cwIsAuthSetupPending?.() || (document.documentElement.dataset.cwRole === "user" && document.documentElement.dataset.cwPermWrite !== "on")) { allowed=false; render(); return; }
      const response = await fetch("/api/import/plex-recovery/active", {credentials:"same-origin", cache:"no-store", signal:request.signal});
      if ([401,403].includes(response.status)) { allowed=false; render(); return; }
      if (!response.ok) throw new Error("Recovery unavailable");
      const data = await response.json();
      if (!request.signal.aborted) render(data.job);
    } catch {
      if (!request.signal.aborted) render(null,"Could not load Plex recovery.");
    } finally {
      if (!request.signal.aborted && !document.hidden && allowed) timer=setTimeout(refresh,10000);
    }
  }
  for (const event of ["cw:notifications-refresh","cw:plex-recovery-changed","cw:overview-profile-changed"]) window.addEventListener(event,refresh);
  for (const event of ["auth-changed","cw:auth-state-changed"]) window.addEventListener(event,()=>{render();refresh();});
  for (const event of ["visibilitychange","tab-changed"]) document.addEventListener(event,refresh);
  refresh();
})();
