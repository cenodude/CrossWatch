/* assets/js/settings-scope.js */
/* CrossWatch - Settings scope while viewing as a profile */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function (w, d) {
  const NS = (w.CW ||= {});
  if (NS.SettingsScope) return;

  const HIDDEN_PANES = ["overview", "app", "maintenance"];
  const BANNER_ID = "cw-view-as-banner";
  const state = { id: "", label: "", instances: {} };
  let loadSeq = 0;

  const txt = (v) => String(v ?? "").trim();
  const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m]));

  function keyOf(provider) {
    try {
      return txt(w.CW?.ProviderMeta?.keyOf?.(provider) || provider).toUpperCase();
    } catch {
      return txt(provider).toUpperCase();
    }
  }

  function managedUser() {
    return d.documentElement.dataset.cwRole === "user";
  }

  function viewedProfileId() {
    if (managedUser()) return txt(d.documentElement.dataset.cwProfileId);
    const overview = w.CW?.OverviewProfile;
    return overview && overview.isAdmin ? txt(overview.id) : "";
  }

  function active() {
    return !!state.id;
  }

  function instancesFor(provider) {
    return state.instances[keyOf(provider)] || [];
  }

  function hasProvider(provider) {
    return instancesFor(provider).length > 0;
  }

  function allowsInstance(provider, instance) {
    return instancesFor(provider).includes(txt(instance) || "default");
  }

  function filterIds(provider, ids) {
    return active() ? (ids || []).filter((id) => allowsInstance(provider, id)) : ids;
  }

  function currentPane() {
    return txt(d.querySelector("#page-settings .cw-settings-nav-btn.active")?.dataset?.pane).toLowerCase();
  }

  function leaveHiddenPane() {
    if (!active()) return;
    const current = currentPane();
    if (current && !HIDDEN_PANES.includes(current)) return;
    const target = Array.from(d.querySelectorAll("#page-settings .cw-settings-nav-btn[data-pane]"))
      .map((btn) => txt(btn.dataset.pane).toLowerCase())
      .find((pane) => pane && !HIDDEN_PANES.includes(pane));
    if (target) {
      try { w.cwSettingsSelect?.(target); } catch {}
    }
  }

  function renderBanner() {
    const existing = d.getElementById(BANNER_ID);
    if (!active() || managedUser()) {
      existing?.remove();
      return;
    }
    const firstPane = d.querySelector("#page-settings .cw-settings-pane");
    if (!firstPane?.parentElement) return;
    const banner = existing || d.createElement("div");
    banner.id = BANNER_ID;
    banner.className = "cw-view-as-banner";
    banner.setAttribute("role", "status");
    banner.innerHTML = `
      <span class="material-symbols-rounded cw-view-as-banner-icon" aria-hidden="true">visibility</span>
      <span class="cw-view-as-banner-copy">
        <strong>Viewing as ${esc(state.label)}</strong>
        <span>Settings only show what belongs to ${esc(state.label)}. New pairs, routes and webhooks are added to ${esc(state.label)}.</span>
      </span>
      <button type="button" class="cw-view-as-banner-stop" data-cw-view-as-stop>Stop viewing as</button>`;
    if (!existing) firstPane.parentElement.insertBefore(banner, firstPane);
  }

  function apply(id, label, instances) {
    const changed = state.id !== id || state.label !== label || JSON.stringify(state.instances) !== JSON.stringify(instances);
    state.id = id;
    state.label = label;
    state.instances = instances;
    if (id) d.documentElement.dataset.cwViewAs = "1";
    else delete d.documentElement.dataset.cwViewAs;
    renderBanner();
    leaveHiddenPane();
    if (changed) {
      try { w.dispatchEvent(new CustomEvent("cw:settings-scope-changed", { detail: { id, label } })); } catch {}
    }
  }

  async function load() {
    const id = viewedProfileId();
    const seq = ++loadSeq;
    if (!id) {
      apply("", "", {});
      return;
    }
    try { await w.__cwAuthBootstrapPromise; } catch {}
    if (seq !== loadSeq) return;
    try {
      const res = await fetch(`/api/user-profiles/${encodeURIComponent(id)}?user_profile=all&ts=${Date.now()}`, { cache: "no-store", credentials: "same-origin" });
      const data = await res.json().catch(() => null);
      if (seq !== loadSeq) return;
      const profile = data?.profile || {};
      const map = {};
      Object.entries(profile.instances && typeof profile.instances === "object" ? profile.instances : {}).forEach(([provider, value]) => {
        const ids = (Array.isArray(value) ? value : [value]).map(txt).filter(Boolean);
        if (ids.length) map[keyOf(provider)] = ids;
      });
      apply(id, txt(profile.label) || id, map);
    } catch {
      if (seq === loadSeq) apply(id, id, {});
    }
  }

  NS.SettingsScope = { active, profileId: () => state.id, label: () => state.label, hasProvider, allowsInstance, filterIds, refresh: load };

  d.addEventListener("click", (event) => {
    if (!event.target?.closest?.("[data-cw-view-as-stop]")) return;
    try { w.CW?.OverviewProfile?.setActive?.(""); } catch {}
  });
  d.addEventListener("cw-settings-pane-changed", leaveHiddenPane);
  w.addEventListener("cw:overview-profile-changed", load);
  w.addEventListener("cw-user-profiles-changed", load);
  Promise.resolve(w.CW?.OverviewProfile?.ready).then(load, load);
  if (d.readyState === "loading") d.addEventListener("DOMContentLoaded", load, { once: true });
})(window, document);
