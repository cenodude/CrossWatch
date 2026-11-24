/* assets/js/sync-reviews.js */
/* CrossWatch - Reopen Interactive Sync reviews from notifications and Synchronization */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(() => {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  const style = document.createElement("link");
  style.rel = "stylesheet";
  style.href = `/assets/css/sync-reviews.css?v=${encodeURIComponent(window.APP_VERSION || "1")}`;
  document.head.appendChild(style);
  const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  let timer, controller, jumpToReviews = false;
  const active = () => !document.hidden && !!document.getElementById("pairs_list");

  function roots() {
    const pairs = document.getElementById("pairs_list");
    if (!pairs) return [];
    let root = document.getElementById("cw-sync-reviews");
    if (!root) {
      root = document.createElement("section");
      root.id = "cw-sync-reviews";
      root.className = "cw-sync-reviews";
      root.setAttribute("aria-label", T("sync.reviews.title"));
      root.hidden = true;
    }
    if (pairs.nextElementSibling !== root) pairs.after(root);
    return [root];
  }

  function render(sessions, error = false) {
    const states = {
      reading: [T("sync.reviews.preparing"), T("sync.reviews.viewProgress"), "sync"],
      applying: [T("sync.reviews.applying"), T("sync.reviews.viewProgress"), "sync"],
      review: [T("sync.reviews.ready"), T("sync.reviews.resume"), "fact_check"],
      complete: [T("sync.reviews.finished"), T("sync.reviews.report"), "description"],
      error: [T("sync.reviews.attention"), T("sync.reviews.open"), "error_outline"],
    };
    const body = sessions.map(session => {
      const pair = session.pair || {};
      const endpoint = side => `${pair[side] || T("sync.reviews.provider")}${pair[`${side}_instance`] && pair[`${side}_instance`] !== "default" ? ` · ${pair[`${side}_instance`]}` : ""}`;
      const label = `${endpoint("source")} ${pair.mode === "two-way" ? "↔" : "→"} ${endpoint("target")}`;
      const route = window.CW?.syncProfileRouteHTML?.(pair) || esc(label);
      const [status, action, icon] = states[session.status] || states.error;
      const time = session.planned_at ? new Date(session.planned_at * 1000).toLocaleString((window.CW?.I18n || window.__cwI18n)?.locale) : "";
      return `<li><span class="material-symbols-rounded cw-review-icon" aria-hidden="true">${icon}</span><div class="cw-review-copy"><strong>${pair.name ? esc(pair.name) : route}</strong>${pair.name ? `<span>${route}</span>` : ""}<small>${esc(status)}${time ? ` · ${esc(time)}` : ""}</small></div><a class="cw-review-link" href="#interactive_sync?session=${encodeURIComponent(session.id)}" aria-label="${esc(`${action}: ${pair.name || label}`)}">${esc(action)}<span class="material-symbols-rounded" aria-hidden="true">arrow_forward</span></a></li>`;
    }).join("");
    const html = `<h3>${TH("sync.reviews.title")}</h3>${error ? `<p>${TH("sync.reviews.loadFailed")} <button type="button" data-reviews-retry>${TH("sync.reviews.retry")}</button></p>` : `<ul>${body}</ul><p>${TH("sync.reviews.stay")}</p>`}`;
    for (const root of roots()) {
      root.hidden = !error && !sessions.length;
      if (root.innerHTML !== html) root.innerHTML = html;
      if (jumpToReviews && document.documentElement.dataset.tab === "settings" && window.__cwSettingsPane === "sync") {
        jumpToReviews = false;
        (root.hidden ? document.getElementById("pairs_list") : root).scrollIntoView({ block: "start" });
      }
    }
  }

  async function refresh({force = false} = {}) {
    clearTimeout(timer);
    if (active() && controller && !controller.signal.aborted && !force) return;
    controller?.abort();
    if (!active()) { controller = null; return; }
    const request = controller = new AbortController();
    let allowed = true;
    try {
      await window.CW?.AuthState?.ready?.();
      await window.__cwAuthBootstrapPromise;
      await window.CW?.OverviewProfile?.ready;
      if (request.signal.aborted) return;
      if (window.cwIsAuthSetupPending?.() || (document.documentElement.dataset.cwRole === "user" && document.documentElement.dataset.cwPermWrite !== "on")) { allowed = false; render([]); return; }
      const profile = String(window.CW?.OverviewProfile?.id || "").trim();
      const url = `/api/interactive-sync${profile ? `?user_profile=${encodeURIComponent(profile)}` : ""}`;
      const response = await fetch(url, { credentials: "same-origin", cache: "no-store", signal: request.signal });
      if (response.status === 401 || response.status === 403) { allowed = false; render([]); return; }
      if (!response.ok) throw new Error(T("sync.reviews.unavailable"));
      const data = await response.json();
      if (!request.signal.aborted) render(data.sessions || []);
    } catch (error) {
      if (!request.signal.aborted) render([], true);
    } finally {
      if (controller === request) {
        controller = null;
        if (!request.signal.aborted && active() && allowed) timer = setTimeout(refresh, 10000);
      }
    }
  }
  window.addEventListener("cw:overview-profile-changed", () => { render([]); refresh({force: true}); });
  document.addEventListener("tab-changed", refresh);
  document.addEventListener("cw-settings-pane-changed", refresh);
  document.addEventListener("visibilitychange", refresh);
  window.addEventListener("auth-changed", () => { render([]); refresh({force: true}); });
  window.addEventListener("cw:auth-state-changed", () => { render([]); refresh({force: true}); });
  document.addEventListener("click", event => {
    if (event.target.closest?.("[data-reviews-retry]")) refresh();
    if (event.target.closest?.(".cw-notifications-footer")) { jumpToReviews = true; refresh(); }
  });
  window.SyncReviews = { refresh };
  refresh();
})();
