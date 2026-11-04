/* assets/js/tracker-notifications.js */
/* CrossWatch - Tracker database move and metadata check in notifications */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(() => {
  const errors = {
    tmdb_key_missing: "Add a TMDB API key in Settings and run it again.",
    tmdb_key_invalid: "TMDB rejected the API key. Check it in Settings.",
    tmdb_unreachable: "TMDB could not be reached. Try again later.",
  };
  let timer, controller, current = {};
  const plural = (n, one, many) => `${Number(n || 0).toLocaleString()} ${n === 1 ? one : many}`;
  const mb = bytes => (Number(bytes || 0) / 1e6).toLocaleString(undefined, {maximumFractionDigits: bytes < 1e7 ? 1 : 0});
  const running = data => data?.job?.state === "running" || data?.migration?.state === "running";

  function checkItem(job) {
    if (!job?.id) return null;
    const busy = job.state === "running";
    const attention = Number(job.attention || 0);
    const where = job.instance && job.instance !== "default" ? ` (${job.label || job.instance})` : "";
    let title = "Metadata check", detail = "", icon = "fact_check", href = "/#tracker", action = "Open tracker";
    if (busy) {
      title += ` - running${where}`;
      detail = `${plural(job.done, "record", "records")} of ${Number(job.total || 0).toLocaleString()} checked. You can leave this page.`;
      icon = "sync";
      action = "View progress";
    } else if (job.state === "completed") {
      title += attention ? ` - needs attention${where}` : ` - completed${where}`;
      detail = attention ? `${plural(attention, "record needs", "records need")} attention.` : "Every record matches TMDB.";
      icon = attention ? "error_outline" : "check_circle";
      if (attention) { href = "/#tracker?attention=1"; action = "Review"; }
    } else if (job.state === "cancelled") {
      title += ` - cancelled${where}`;
      detail = "Stopped before the end. Results so far are kept.";
      icon = "block";
    } else {
      title += ` - stopped${where}`;
      detail = job.state === "interrupted" ? "CrossWatch restarted during the check. Run it again." : errors[job.error] || "The check stopped. Run it again from the Tracker.";
      icon = "error_outline";
    }
    return {id: job.id, revision: busy ? "running" : `${job.state}:${attention}`, title, detail, icon, href, action, time: job.finished_at || job.started_at};
  }

  function migrationItem(move) {
    if (!move?.id) return null;
    const results = Array.isArray(move.results) ? move.results : [];
    const failures = Array.isArray(move.errors) ? move.errors : [];
    const name = inst => (inst && inst !== "default" ? ` ${inst}` : "");
    const base = {id: `move:${move.id}`, href: "/#tracker", action: "Open tracker", time: move.finished_at || move.started_at};
    if (move.state === "running") {
      const many = Number(move.instance_count || 1) > 1 ? ` · tracker ${move.instance_index} of ${move.instance_count}` : "";
      const size = move.bytes_total ? ` (${mb(move.bytes_done)} of ${mb(move.bytes_total)} MB)` : "";
      return {...base, revision: "running", icon: "database", action: "View progress", title: "Tracker upgrade - running",
        detail: `Moving the tracker${name(move.instance === "default" ? "" : move.label || move.instance)} to its database: ${Number(move.files_done || 0).toLocaleString()} of ${Number(move.files_total || 0).toLocaleString()} files${size}${many}. Syncs wait until this is done.`};
    }
    if (failures.length) {
      const first = failures[0];
      const moved = results.length ? ` ${plural(results.length, "other tracker was", "other trackers were")} moved.` : "";
      return {...base, revision: `failed:${failures.length}`, icon: "error_outline", title: "Tracker upgrade - needs attention",
        detail: `${first.file || "A file"} could not be read, so the tracker${name(first.instance)} still uses its old files. Fix or remove that file and restart CrossWatch.${moved}`};
    }
    return {...base, revision: "completed", icon: "check_circle", title: "Tracker upgrade - completed",
      detail: results.length > 1 ? `${results.length} trackers now use a database. Their original files are in a json-backup folder.` : `The tracker now uses a database. The original files are in ${results[0]?.backup || "a json-backup folder"}.`};
  }

  function render(data = {}, error = "") {
    current = data || {};
    const items = [migrationItem(current.migration), checkItem(current.job)].filter(Boolean);
    window.CW?.Notifications?.setSource("tracker-check", {scope: "account", error, items});
  }

  async function refresh() {
    clearTimeout(timer);
    controller?.abort();
    if (document.hidden) return;
    const request = controller = new AbortController();
    let allowed = true;
    try {
      await window.__cwAuthBootstrapPromise;
      if (request.signal.aborted) return;
      if (window.cwIsAuthSetupPending?.() || document.documentElement.dataset.cwRole === "user") { allowed = false; render(); return; }
      const response = await fetch("/api/tracker/check/current", {credentials: "same-origin", cache: "no-store", signal: request.signal});
      if ([401, 403].includes(response.status)) { allowed = false; render(); return; }
      if (!response.ok) throw new Error("Tracker status unavailable");
      const data = await response.json();
      if (!request.signal.aborted) {
        const wasMoving = current.migration?.state === "running";
        render({job: data.job || null, migration: data.migration || null});
        if (wasMoving && data.migration?.state !== "running") window.dispatchEvent(new CustomEvent("cw:tracker-migrated"));
      }
    } catch {
      if (!request.signal.aborted) render(current, "Could not refresh the tracker status.");
    } finally {
      if (controller === request && !document.hidden && allowed) timer = setTimeout(refresh, running(current) ? 3000 : 30000);
    }
  }
  for (const event of ["cw:notifications-refresh", "cw:tracker-check-changed"]) window.addEventListener(event, refresh);
  for (const event of ["auth-changed", "cw:auth-state-changed"]) window.addEventListener(event, () => { render(); refresh(); });
  document.addEventListener("visibilitychange", refresh);
  refresh();
})();
