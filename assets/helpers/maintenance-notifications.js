/* assets/helpers/maintenance-notifications.js */
/* CrossWatch - Background maintenance progress and completion notifications */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(() => {
  const titles = {
    "events-optimize": "Optimize database", "events-health": "Archive health check",
    "database-health": "Database health check", "state-file": "Compact sync state",
    "state-file-prune": "Prune stale state", "events-rebuild": "Rebuild event archive",
    "events-purge": "Clear event data", state: "Rebuild sync state",
    "database-rebuild": "Rebuild local database",
  };
  const steps = {
    queued: "Waiting to start", checkpoint_before: "Preparing database",
    vacuum: "Reclaiming unused space", analyze: "Updating database statistics",
    optimize: "Optimizing queries", checkpoint_after: "Finishing database writes",
  };
  let timer, controller, current;
  function render(job = null, error = "") {
    current = job;
    const running = ["queued", "running"].includes(job?.state);
    const failed = ["failed", "interrupted"].includes(job?.state);
    const issues = job?.result?.healthy === false || job?.result?.checkpoint_pending || job?.result?.pair_scopes?.deferred || job?.result?.warnings?.length;
    let detail = running ? `${steps[job.step] || "Running in the background"}. You can leave this page.`
      : failed ? "Maintenance failed. Open Maintenance for details."
      : issues ? "Finished with issues that need attention." : "Maintenance completed successfully.";
    if (job?.state === "interrupted") detail = "The maintenance worker stopped before recording a result. Check before retrying.";
    if (job?.result?.error === "checkpoint_busy") detail = "The database is busy. Optimization could not start.";
    window.CW?.Notifications?.setSource("maintenance", {scope: "account", error, items: job?.id ? [{
      id: job.id, revision: running ? "running" : `${job.state}:${issues ? "issues" : "done"}`,
      title: `${titles[job.action] || "Database maintenance"} - ${running ? "running" : failed ? "stopped" : issues ? "needs attention" : "completed"}`,
      detail, icon: running ? "sync" : failed || issues ? "error_outline" : "check_circle",
      href: "/#maintenance", action: running ? "View progress" : "View result", time: job.started_at,
    }] : []});
  }
  async function refresh() {
    clearTimeout(timer);
    controller?.abort();
    if (document.hidden) return;
    const request = controller = new AbortController();
    let allowed = true;
    const timeout = setTimeout(() => request.abort(), 15000);
    try {
      await window.__cwAuthBootstrapPromise;
      if (request.signal.aborted) return;
      if (window.cwIsAuthSetupPending?.() || document.documentElement.dataset.cwRole === "user") {
        allowed = false; render(); return;
      }
      const response = await fetch("/api/maintenance/jobs/current", {credentials: "same-origin", cache: "no-store", signal: request.signal});
      if ([401, 403].includes(response.status)) { allowed = false; render(); return; }
      if (!response.ok) throw new Error("Maintenance status unavailable");
      const job = await response.json();
      if (!request.signal.aborted) render(job);
    } catch {
      if (!request.signal.aborted) render(current, "Could not refresh maintenance status.");
    } finally {
      clearTimeout(timeout);
      if (controller === request && !document.hidden && allowed) {
        timer = setTimeout(refresh, ["queued", "running"].includes(current?.state) ? 2000 : 10000);
      }
    }
  }
  for (const event of ["cw:notifications-refresh", "cw:maintenance-job-changed"]) window.addEventListener(event, refresh);
  for (const event of ["auth-changed", "cw:auth-state-changed"]) window.addEventListener(event, () => { render(); refresh(); });
  document.addEventListener("visibilitychange", refresh);
  refresh();
})();
