/* assets/js/overview-profile.js */
/* CrossWatch overview account/profile scope */
(function () {
  const DEFAULT_INSTANCE = "default";
  const $ = (sel, root = document) => root.querySelector(sel);
  const esc = (value) => String(value ?? "").replace(/[&<>"]/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;" }[m]));
  const providerKey = (value) => window.CW?.ProviderMeta?.keyOf?.(value) || String(value || "").trim().toUpperCase();
  const instanceKey = (value) => String(value || DEFAULT_INSTANCE).trim() || DEFAULT_INSTANCE;

  const STORE_KEY = "cw.overview.profile";
  let profiles = [];
  let activeId = "";
  let isAdmin = false;
  let authUser = null;
  let readyResolve = null;
  const SHELL_MANAGED = document.documentElement?.dataset?.cwRole === "user";
  const SHELL_PROFILE_ID = SHELL_MANAGED ? String(document.documentElement?.dataset?.cwProfileId || "").trim() : "";

  const ready = new Promise((resolve) => { readyResolve = resolve; });

  function normalizeInstances(raw) {
    const out = {};
    if (!raw || typeof raw !== "object") return out;
    for (const [provider, value] of Object.entries(raw)) {
      const prov = providerKey(provider);
      if (!prov) continue;
      const values = Array.isArray(value) ? value : [value];
      const insts = values.map(instanceKey).filter(Boolean);
      if (insts.length) out[prov] = [...new Set(insts)].slice(0, 1);
    }
    return out;
  }

  function storedId() {
    try { return String(localStorage.getItem(STORE_KEY) || "").trim(); } catch { return ""; }
  }

  function storeId(id) {
    try {
      if (id) localStorage.setItem(STORE_KEY, id);
      else localStorage.removeItem(STORE_KEY);
    } catch {}
  }

  function activeProfile() {
    return profiles.find((row) => row.id === activeId) || null;
  }

  function activeFilter() {
    return normalizeInstances(activeProfile()?.instances || {});
  }

  function matchesEndpoint(provider, instance) {
    const filter = activeFilter();
    if (!Object.keys(filter).length) return true;
    const prov = providerKey(provider);
    return !!prov && (filter[prov] || []).includes(instanceKey(instance));
  }

  function instanceForProvider(provider) {
    const prov = providerKey(provider);
    if (!prov) return "";
    const values = activeFilter()[prov] || [];
    return values.length ? instanceKey(values[0]) : "";
  }

  function matchesSources(value) {
    const filter = activeFilter();
    if (!Object.keys(filter).length) return true;
    const rows = [];
    const push = (provider, instance = DEFAULT_INSTANCE) => {
      const prov = providerKey(provider);
      if (prov) rows.push({ provider: prov, instance: instanceKey(instance) });
    };
    if (Array.isArray(value)) {
      for (const item of value) {
        if (item && typeof item === "object") push(item.provider || item.source || item.name, item.instance || item.source_instance || item.id);
        else push(item);
      }
    } else if (value && typeof value === "object") {
      const byProvider = value.sources_by_provider || value.sourcesByProvider || value;
      for (const [provider, instances] of Object.entries(byProvider || {})) {
        if (Array.isArray(instances) && instances.length) instances.forEach((instance) => push(provider, instance));
        else push(provider);
      }
    }
    return rows.some((row) => (filter[row.provider] || []).includes(row.instance));
  }

  function emit() {
    document.documentElement.dataset.overviewProfile = activeId ? "scoped" : "all";
    window.dispatchEvent(new CustomEvent("cw:overview-profile-changed", {
      detail: { id: activeId, profile: activeProfile(), filter: activeFilter() },
    }));
  }

  function currentAvatarUrl(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    if (!raw.startsWith("/api/profile/avatar/")) return raw;
    const q = raw.indexOf("?");
    return `/api/profile/avatar${q >= 0 ? raw.slice(q) : ""}`;
  }

  function renderProfileLink() {
    const link = $("#cw-nav-profile-link");
    const avatar = $("#cw-nav-profile-avatar");
    if (!link || !avatar) return;
    link.classList.remove("hidden");
    const fallback = authUser?.is_admin ? "Administrator" : "Profile";
    const label = String(authUser?.display_name || authUser?.label || authUser?.username || fallback).trim();
    link.title = label ? `Open ${label}` : "Open profile";
    link.setAttribute("aria-label", link.title);
    const url = currentAvatarUrl(authUser?.avatar_url);
    avatar.innerHTML = url ? `<img src="${esc(url)}" alt="">` : "person";
    avatar.classList.toggle("material-symbols-rounded", !url);
    const img = avatar.querySelector("img");
    if (img) {
      img.addEventListener("error", () => {
        avatar.innerHTML = "person";
        avatar.classList.add("material-symbols-rounded");
      }, { once: true });
    }
  }

  function setActive(id) {
    if (!isAdmin) return false;
    const next = String(id || "").trim();
    const valid = next && profiles.some((row) => row.id === next) ? next : "";
    if (valid === activeId) return false;
    activeId = valid;
    storeId(valid);
    render();
    emit();
    return true;
  }

  function renderPicker() {
    const host = $("#cw-view-as");
    const select = $("#cw-view-as-select");
    if (!host || !select) return;
    host.classList.toggle("hidden", !isAdmin);
    if (!isAdmin) return;
    const rows = profiles.length ? [{ id: "", label: "All profiles" }, ...profiles] : [{ id: "", label: "No profiles" }];
    const signature = rows.map((row) => `${row.id}::${row.label}`).join("|");
    if (select.dataset.signature !== signature) {
      select.innerHTML = rows.map((row) => `<option value="${esc(row.id)}">${esc(row.label)}</option>`).join("");
      select.dataset.signature = signature;
    }
    select.value = activeId;
    select.disabled = profiles.length === 0;
    if (!select.dataset.bound) {
      select.dataset.bound = "1";
      select.addEventListener("change", () => setActive(select.value));
    }
    try { window.CW?.ProfileSelect?.enhanceProfile?.(select, { className: "cw-view-as-picker" }); } catch {}
  }

  function render() {
    renderProfileLink();
    renderPicker();
  }

  async function hydrateCurrentUserProfile() {
    try {
      const res = await fetch("/api/profile", { cache: "no-store", credentials: "same-origin" });
      if (!res.ok) return;
      const data = await res.json();
      if (!data || typeof data !== "object") return;
      const account = data.user && typeof data.user === "object" ? data.user : data;
      const next = { ...(authUser || {}) };
      const avatarUrl = currentAvatarUrl(account.avatar_url);
      const displayName = String(account.display_name || account.label || "").trim();
      if (avatarUrl) next.avatar_url = avatarUrl;
      if (displayName) {
        next.display_name = displayName;
        next.label = displayName;
      }
      authUser = next;
      if (window.CW?.AuthState?.user) {
        if (authUser?.avatar_url) window.CW.AuthState.user.avatar_url = authUser.avatar_url;
        if (authUser?.display_name) window.CW.AuthState.user.display_name = authUser.display_name;
      }
    } catch {}
  }

  async function fetchJSON(url) {
    if (window.cwIsAuthSetupPending?.() === true) return { ok: true, items: [] };
    if (window.CW?.API?.j) return window.CW.API.j(url);
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  }

  function applyAuthStatus(status) {
    const state = window.CW?.AuthState?.apply ? window.CW.AuthState.apply(status) : null;
    authUser = state?.user || status?.user || null;
    isAdmin = Boolean(state?.isAdmin || status?.is_admin || authUser?.is_admin);
    const managed = Boolean(state?.isManaged || SHELL_MANAGED || (authUser && authUser.is_admin === false));
    if (!isAdmin && managed) activeId = String(state?.profileId || authUser?.profile_id || SHELL_PROFILE_ID || "").trim();
    else activeId = isAdmin ? storedId() : "";
  }

  async function refresh() {
    try { await window.__cwAuthBootstrapPromise; } catch {}
    try {
      try {
        applyAuthStatus(await fetchJSON("/api/app-auth/status"));
      } catch {
        const state = window.CW?.AuthState?.read?.() || {};
        authUser = state.user || (SHELL_PROFILE_ID ? { is_admin: false, profile_id: SHELL_PROFILE_ID, permissions: {} } : null);
        isAdmin = Boolean(state.isAdmin) && !SHELL_MANAGED;
        activeId = isAdmin ? storedId() : (state.profileId || SHELL_PROFILE_ID);
      }
      await hydrateCurrentUserProfile();
      if (activeId || isAdmin) {
        const data = await fetchJSON("/api/user-profiles");
        profiles = (Array.isArray(data?.items) ? data.items : [])
          .map((row) => ({
            id: String(row?.id || "").trim(),
            label: String(row?.label || row?.id || "").trim(),
            instances: normalizeInstances(row?.instances || {}),
          }))
          .filter((row) => row.id && row.label);
      } else {
        profiles = [];
      }
      if (isAdmin && !profiles.length) console.warn("[OverviewProfile] /api/user-profiles returned no usable rows");
      if (isAdmin && activeId && !profiles.some((row) => row.id === activeId)) {
        activeId = "";
        storeId("");
      }
    } catch (err) {
      console.warn("[OverviewProfile] refresh failed", err);
      profiles = [];
    }
    render();
    readyResolve?.(profiles);
    emit();
    return profiles;
  }

  window.CW = window.CW || {};
  window.CW.OverviewProfile = {
    ready,
    refresh,
    render,
    setActive,
    get profiles() { return profiles.slice(); },
    get isAdmin() { return isAdmin; },
    get id() { return activeId; },
    get label() { return activeProfile()?.label || ""; },
    get profile() { return activeProfile(); },
    get filter() { return activeFilter(); },
    instanceForProvider,
    matchesEndpoint,
    matchesSources,
  };

  window.addEventListener("cw-user-profiles-changed", () => refresh());
  window.addEventListener("cw-auth-setup-pending", (event) => {
    if (event?.detail?.pending === false) refresh();
  });
  window.addEventListener("cw:auth-state-changed", (event) => {
    const next = event?.detail?.user;
    if (next && typeof next === "object") {
      authUser = next;
      render();
    }
  });
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", () => refresh(), { once: true });
  else refresh();
})();
