/* assets/js/sync-reviews.js */
/* CrossWatch - Reopen Interactive Sync reviews from notifications and Synchronization */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(() => {
  const style = document.createElement("link");
  style.rel = "stylesheet";
  style.href = `/assets/css/sync-reviews.css?v=${encodeURIComponent(window.APP_VERSION || "1")}`;
  document.head.appendChild(style);
  const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  let timer, controller, jumpToReviews = false;
  const active = () => !document.hidden;

  function roots() {
    const pairs = document.getElementById("pairs_list");
    if (!pairs) return [];
    let root = document.getElementById("cw-sync-reviews");
    if (!root) {
      root = document.createElement("section");
      root.id = "cw-sync-reviews";
      root.className = "cw-sync-reviews";
      root.setAttribute("aria-label", "Sync reviews");
      root.hidden = true;
    }
    if (pairs.nextElementSibling !== root) pairs.after(root);
    return [root];
  }

  function render(sessions, error = false, loading = false) {
    const states = {
      reading: ["Preparing review", "View progress", "sync"],
      applying: ["Applying changes", "View progress", "sync"],
      review: ["Ready to review", "Resume review", "fact_check"],
      complete: ["Finished", "View report", "description"],
      error: ["Needs attention", "Open review", "error_outline"],
    };
    const notifications = [];
    const body = sessions.map(session => {
      const pair = session.pair || {};
      const endpoint = side => `${pair[side] || "Provider"}${pair[`${side}_instance`] && pair[`${side}_instance`] !== "default" ? ` · ${pair[`${side}_instance`]}` : ""}`;
      const label = `${endpoint("source")} ${pair.mode === "two-way" ? "↔" : "→"} ${endpoint("target")}`;
      const [status, action, icon] = states[session.status] || states.error;
      const time = session.planned_at ? new Date(session.planned_at * 1000).toLocaleString() : "";
      notifications.push({
        id: session.id, revision: `${session.status}:${session.planned_at || ""}`, title: pair.name || label, detail: status, icon,
        href: `/#interactive_sync?session=${encodeURIComponent(session.id)}`,
        action, time: session.planned_at,
      });
      return `<li><span class="material-symbols-rounded cw-review-icon" aria-hidden="true">${icon}</span><div class="cw-review-copy"><strong>${esc(pair.name || label)}</strong>${pair.name ? `<span>${esc(label)}</span>` : ""}<small>${esc(status)}${time ? ` · ${esc(time)}` : ""}</small></div><a class="cw-review-link" href="#interactive_sync?session=${encodeURIComponent(session.id)}" aria-label="${esc(`${action}: ${pair.name || label}`)}">${action}<span class="material-symbols-rounded" aria-hidden="true">arrow_forward</span></a></li>`;
    }).join("");
    const html = `<h3>Sync reviews</h3>${error ? '<p>Could not load your reviews. <button type="button" data-reviews-retry>Try again</button></p>' : `<ul>${body}</ul><p>Open reviews stay available for one hour of inactivity, until closed, or until CrossWatch restarts.</p>`}`;
    window.CW?.Notifications?.setSource("sync-reviews", { items: notifications, loading, error: error ? "Could not load sync reviews." : "" });
    for (const root of roots()) {
      root.hidden = !error && !sessions.length;
      if (root.innerHTML !== html) root.innerHTML = html;
      if (jumpToReviews && document.documentElement.dataset.tab === "settings" && window.__cwSettingsPane === "sync") {
        jumpToReviews = false;
        (root.hidden ? document.getElementById("pairs_list") : root).scrollIntoView({ block: "start" });
      }
    }
  }

  async function refresh() {
    clearTimeout(timer);
    controller?.abort();
    if (!active()) return;
    const request = controller = new AbortController();
    let allowed = true;
    try {
      await window.__cwAuthBootstrapPromise;
      await window.CW?.OverviewProfile?.ready;
      if (request.signal.aborted) return;
      if (window.cwIsAuthSetupPending?.() || (document.documentElement.dataset.cwRole === "user" && document.documentElement.dataset.cwPermWrite !== "on")) { allowed = false; render([]); return; }
      const profile = String(window.CW?.OverviewProfile?.id || "").trim();
      const url = `/api/interactive-sync${profile ? `?user_profile=${encodeURIComponent(profile)}` : ""}`;
      const response = await fetch(url, { credentials: "same-origin", cache: "no-store", signal: request.signal });
      if (response.status === 401 || response.status === 403) { allowed = false; render([]); return; }
      if (!response.ok) throw new Error("Reviews unavailable");
      const data = await response.json();
      if (!request.signal.aborted) render(data.sessions || []);
    } catch (error) {
      if (!request.signal.aborted) render([], true);
    } finally {
      if (!request.signal.aborted && active() && allowed) timer = setTimeout(refresh, 10000);
    }
  }
  window.addEventListener("cw:overview-profile-changed", () => { render([], false, true); refresh(); });
  document.addEventListener("tab-changed", refresh);
  document.addEventListener("cw-settings-pane-changed", refresh);
  document.addEventListener("visibilitychange", refresh);
  window.addEventListener("auth-changed", () => { render([]); refresh(); });
  window.addEventListener("cw:auth-state-changed", () => { render([]); refresh(); });
  document.addEventListener("click", event => {
    if (event.target.closest?.("[data-reviews-retry]")) refresh();
    if (event.target.closest?.(".cw-notifications-footer")) { jumpToReviews = true; refresh(); }
  });
  window.SyncReviews = { refresh };
  window.addEventListener("cw:notifications-refresh", refresh);
  window.CW?.Notifications?.setSource("sync-reviews", { items: [], loading: true });
  refresh();
})();
