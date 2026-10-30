/* assets/js/user-profiles.js */
/* CrossWatch - User profile management */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function (w, d) {
  if (w.cwUserProfilesManager) return;

  const HOST_ID = "cw-user-profile-manager";
  const MODAL_HOST_ID = "cw-upm-floating-host";
  let providerInstances = {};
  let syncUserLabels = {};
  let userProfiles = [];
  let appUsers = [];
  let loading = false;
  let loadedOnce = false;
  let internalRefresh = false;
  let modalProfileId = null;
  let modalDraft = null;
  let modalDone = null;
  let modalOpening = false;
  let shownPaneKey = "";
  let availableResources = { sync_pairs: [], watcher_routes: [], webhook_routes: [] };
  let deleteConfirmTimer = 0;
  let removeLoginTimer = 0;
  let wizardStep = 0;
  let authActive = false;
  let oidcConfigured = false;
  let showPassword = false;

  const RESOURCE_KEYS = ["sync_pairs", "watcher_routes", "webhook_routes"];
  const RESOURCE_ICONS = { sync_pairs: "sync_alt", watcher_routes: "sensors", webhook_routes: "webhook" };
  const STEPS = [
    { key: "name", label: "Name", note: "Who it is for", icon: "badge" },
    { key: "accounts", label: "Accounts", note: "Provider accounts", icon: "hub" },
    { key: "resources", label: "Pairs and routes", note: "What syncs and scrobbles", icon: "sync_alt" },
    { key: "login", label: "Sign-in", note: "Optional login", icon: "key" },
    { key: "review", label: "Review", note: "Check and create", icon: "fact_check" },
  ];
  const ACCESS_LABELS = { full: "Full access", readonly: "Read-only", monitor: "Monitor" };
  const ACCESS_NOTES = { full: "Can view and change their own data.", readonly: "Can look around, but not change anything.", monitor: "Status screens only." };
  const ACCESS_ICONS = { full: "edit_square", readonly: "visibility", monitor: "monitor_heart" };
  const SETTINGS_GRANTS = [
    ["pairs", "Sync pairs", "Add, change and run their own pairs."],
    ["scrobbler", "Scrobbler", "Their own watcher routes and webhooks."],
    ["schedule", "Schedule", "When their own pairs run."],
    ["connections", "Reconnect", "Sign in again to their own accounts."],
  ];
  const OIDC_CLAIMS = [
    ["preferred_username", "Username", "badge", "Links on their first sign-in with exactly this username, including upper and lower case."],
    ["email", "Email", "mail", "Links on their first sign-in with this email, once the provider has verified it."],
    ["sub", "Subject", "fingerprint", "The account ID from your identity provider. Links right away."],
  ];
  const NAME_IDEAS = ["Me", "Partner", "Kids", "Guests", "Living room"];
  const STRENGTH_LABELS = ["", "Too short", "Okay", "Good", "Strong"];
  const MIN_PASSWORD = 8;

  const $ = (id) => d.getElementById(id);
  const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m]));
  const txt = (v) => String(v ?? "").trim();
  const icon = (name) => `<span class="material-symbols-rounded" aria-hidden="true">${esc(name)}</span>`;
  const plural = (count, word) => `${count} ${word}${count === 1 ? "" : "s"}`;

  function toast(message, ok = false) {
    try { w._cwShowToast?.(String(message || ""), !!ok); } catch {}
  }

  async function json(url, opts) {
    const res = await fetch(url, { cache: "no-store", credentials: "same-origin", ...(opts || {}) });
    const data = await res.json().catch(() => null);
    if (!res.ok || data?.ok === false) throw new Error(String(data?.error || `HTTP ${res.status}`));
    return data;
  }

  function providerLabel(provider) {
    try {
      const label = w.CW?.ProviderMeta?.label?.(provider);
      if (label) return label;
    } catch {}
    return txt(provider).replace(/_/g, " ") || "Provider";
  }

  function providerKey(provider) {
    try { return w.CW?.ProviderMeta?.keyOf?.(provider) || txt(provider).toUpperCase(); }
    catch { return txt(provider).toUpperCase(); }
  }

  function providerMark(provider) {
    const key = providerKey(provider);
    let src = "";
    try { src = w.CW?.ProviderMeta?.logoPath?.(key) || w.CW?.ProviderMeta?.logLogoPath?.(key) || ""; } catch {}
    if (src) return `<span class="cw-upw-logo"><img src="${esc(src)}" alt="" loading="lazy"></span>`;
    return `<span class="cw-upw-logo is-text">${esc((key || "?").slice(0, 2))}</span>`;
  }

  function hueFor(name) {
    let hash = 0;
    for (const ch of txt(name).toLowerCase()) hash = (hash * 31 + ch.charCodeAt(0)) % 3600;
    return txt(name) ? hash % 360 : 250;
  }

  function initialsFor(name) {
    const words = txt(name).split(/\s+/).filter(Boolean);
    if (!words.length) return "";
    if (words.length === 1) return words[0].slice(0, 2).toUpperCase();
    return (words[0][0] + words[1][0]).toUpperCase();
  }

  function avatar(name, size = "", url = "") {
    const letters = initialsFor(name);
    const inner = url ? `<img src="${esc(url)}" alt="">` : (letters ? esc(letters) : icon("person"));
    return `<span class="cw-upw-avatar${size ? ` is-${size}` : ""}${url ? " has-image" : ""}" data-upw-avatar style="--h:${hueFor(name)}">${inner}</span>`;
  }

  function draftAvatarUrl() {
    const change = modalDraft?.avatarChange;
    if (change === "remove") return "";
    if (change && typeof change === "object") return change.data;
    return txt(modalDraft?.avatarUrl);
  }

  function profileById(id) {
    const pid = txt(id);
    return userProfiles.find((row) => row.id === pid) || null;
  }

  function providerKeys() {
    return Object.keys(providerInstances || {})
      .filter((provider) => instanceRows(provider).length)
      .sort((a, b) => providerLabel(a).localeCompare(providerLabel(b)));
  }

  function isDefaultInstanceId(id) {
    return (txt(id || "default") || "default").toLowerCase() === "default";
  }

  function instanceRows(provider) {
    return (Array.isArray(providerInstances[provider]) ? providerInstances[provider] : [])
      .map((row) => ({
        id: txt(row?.id || "default") || "default",
        label: txt(row?.display_label || row?.label || row?.id || "Default"),
      }))
      .filter((row) => row.id && !isDefaultInstanceId(row.id))
      .sort((a, b) => a.label.localeCompare(b.label, undefined, { sensitivity: "base", numeric: true }));
  }

  function assignmentMap(profile) {
    const raw = profile?.instances && typeof profile.instances === "object" ? profile.instances : {};
    const out = {};
    Object.entries(raw).forEach(([provider, value]) => {
      const values = (Array.isArray(value) ? value : [value]).map(txt).filter(Boolean);
      if (values.length) out[provider] = values;
    });
    return out;
  }

  function resourceMap(profile) {
    const raw = profile?.resources && typeof profile.resources === "object" ? profile.resources : {};
    return {
      sync_pairs: Array.isArray(raw.sync_pairs) ? raw.sync_pairs.map(txt).filter(Boolean) : [],
      watcher_routes: Array.isArray(raw.watcher_routes) ? raw.watcher_routes.map(txt).filter(Boolean) : [],
      webhook_routes: Array.isArray(raw.webhook_routes) ? raw.webhook_routes.map(txt).filter(Boolean) : [],
    };
  }

  function profileLogins(profileId) {
    const pid = txt(profileId);
    return pid ? appUsers.filter((user) => !user?.protected && txt(user?.profile_id) === pid) : [];
  }

  function accessMode(user) {
    const raw = user?.permissions && typeof user.permissions === "object" ? user.permissions : {};
    if (raw.monitor === true) return "monitor";
    return raw.write === true ? "full" : "readonly";
  }

  function settingsGrants(user) {
    const raw = user?.permissions && typeof user.permissions === "object" ? user.permissions : {};
    return Object.fromEntries(SETTINGS_GRANTS.map(([key]) => [key, raw[key] !== false]));
  }

  function accessPermissions(mode, grants) {
    if (mode === "monitor") return { dashboard: false, watchlist: false, playback: false, write: false, monitor: true };
    const full = mode === "full";
    return {
      dashboard: true,
      watchlist: true,
      playback: true,
      write: full,
      monitor: false,
      ...Object.fromEntries(SETTINGS_GRANTS.map(([key]) => [key, full && grants?.[key] !== false])),
    };
  }

  function grantsSummary(login) {
    if (login.access !== "full") return "";
    const on = SETTINGS_GRANTS.filter(([key]) => login.grants?.[key] !== false).map(([, label]) => label);
    return on.length ? `Settings: ${on.join(", ")}` : "No settings access";
  }

  function oidcDraft(user) {
    const raw = user?.oidc && typeof user.oidc === "object" ? user.oidc : {};
    const claim = txt(raw.pending_claim) || "preferred_username";
    const value = txt(raw.pending_value);
    return {
      oidcLinked: raw.linked === true,
      oidcLabel: txt(raw.username || raw.email || raw.subject),
      oidcClaim: claim,
      oidcValue: value,
      initialOidcClaim: claim,
      initialOidcValue: value,
    };
  }

  function makeLoginDraft(profile) {
    const user = profile ? profileLogins(profile.id)[0] : null;
    return {
      ...oidcDraft(user),
      wanted: !!user,
      user_id: txt(user?.id),
      username: txt(user?.username || ""),
      initialUsername: txt(user?.username || ""),
      password: "",
      access: user ? accessMode(user) : "full",
      initialAccess: user ? accessMode(user) : "full",
      enabled: user ? user.enabled !== false : true,
      initialEnabled: user ? user.enabled !== false : true,
      grants: settingsGrants(user),
      initialGrants: settingsGrants(user),
      totp: !!user?.totp_enabled,
      adminUsername: "admin",
      adminPassword: "",
    };
  }

  function makeDraft(profile) {
    const manual = profile?.manual_instances && typeof profile.manual_instances === "object" ? profile.manual_instances : assignmentMap(profile);
    return {
      label: txt(profile?.label || ""),
      instances: JSON.parse(JSON.stringify(manual || {})),
      resources: resourceMap(profile),
      login: makeLoginDraft(profile),
      avatarUrl: txt(profile?.avatar_url),
      avatarChange: undefined,
      suggested: !!profile,
      visited: profile ? STEPS.length : 0,
    };
  }

  function draftResourceSet(key) {
    return new Set((modalDraft?.resources?.[key] || []).map(txt).filter(Boolean));
  }

  function resourceRows(key) {
    const rows = Array.isArray(availableResources?.[key]) ? availableResources[key] : [];
    return rows.filter((row) => {
      const refs = Array.isArray(row?.instances) ? row.instances : [];
      return txt(row?.id) && refs.length && refs.every((ref) => !isDefaultInstanceId(ref?.instance));
    });
  }

  function resourceTitle(key) {
    if (key === "sync_pairs") return "Sync pairs";
    if (key === "watcher_routes") return "Watcher routes";
    return "Webhook routes";
  }

  function resourceRequiredMap() {
    const out = {};
    RESOURCE_KEYS.forEach((key) => {
      const selected = draftResourceSet(key);
      resourceRows(key).forEach((row) => {
        if (!selected.has(txt(row.id))) return;
        (Array.isArray(row.instances) ? row.instances : []).forEach((ref) => {
          const provider = providerKey(ref?.provider);
          const instance = txt(ref?.instance || "default") || "default";
          if (!provider || !instance) return;
          out[provider] = out[provider] || {};
          out[provider][instance] = out[provider][instance] || [];
          out[provider][instance].push(resourceTitle(key));
        });
      });
    });
    return out;
  }

  function draftInstances() {
    const out = {};
    Object.entries(modalDraft?.instances || {}).forEach(([provider, values]) => {
      const clean = (Array.isArray(values) ? values : [values]).map(txt).filter(Boolean);
      if (provider && clean.length) out[providerKey(provider)] = clean;
    });
    const required = resourceRequiredMap();
    Object.entries(required).forEach(([provider, rows]) => {
      const key = providerKey(provider);
      const ids = Object.keys(rows || {}).map(txt).filter(Boolean);
      if (ids.length) out[key] = Array.from(new Set([...(out[key] || []), ...ids]));
    });
    return out;
  }

  function draftAccountCount() {
    return Object.values(draftInstances()).reduce((total, values) => total + values.filter((value) => !isDefaultInstanceId(value)).length, 0);
  }

  function draftResourceCount() {
    return RESOURCE_KEYS.reduce((total, key) => {
      const selected = draftResourceSet(key);
      return total + resourceRows(key).filter((row) => selected.has(txt(row.id))).length;
    }, 0);
  }

  function assignmentCount(profile) {
    return Object.values(assignmentMap(profile)).reduce((total, values) => total + values.filter((value) => !isDefaultInstanceId(value)).length, 0);
  }

  function assignmentSummary(profile) {
    const providers = Object.values(assignmentMap(profile)).filter((values) => values.some((value) => !isDefaultInstanceId(value))).length;
    const resources = Object.entries(resourceMap(profile)).reduce((total, [key, values]) => {
      const visibleIds = new Set(resourceRows(key).map((row) => txt(row.id)));
      return total + values.filter((value) => visibleIds.has(txt(value))).length;
    }, 0);
    return `Providers: ${providers} - Resources: ${resources}`;
  }

  function assignedUserSummary(profile) {
    const names = profileLogins(profile?.id)
      .map((user) => txt(user?.username || user?.label || user?.id))
      .filter(Boolean);
    if (!names.length) return "No login";
    const visible = names.slice(0, 3).join(", ");
    return `Can log in as ${visible}${names.length > 3 ? ` +${names.length - 3} more` : ""}`;
  }

  function instanceAssignedProfile(provider, instance) {
    const ownerProvider = providerKey(provider);
    const ownerInstance = txt(instance);
    const currentProfileId = txt(modalProfileId);
    if (!ownerProvider || !ownerInstance) return null;
    return userProfiles.find((profile) => {
      const pid = txt(profile?.id);
      if (!pid || pid === currentProfileId) return false;
      return (assignmentMap(profile)[ownerProvider] || []).map(txt).includes(ownerInstance);
    }) || null;
  }

  function profileLabelForId(profileId) {
    const pid = txt(profileId);
    if (!pid) return "";
    const profile = profileById(pid);
    return txt(profile?.label || profile?.id || pid);
  }

  function renderProfileCard(profile) {
    const count = assignmentCount(profile);
    const canLogin = profileLogins(profile.id).length > 0;
    return `
      <button type="button" class="cw-auth-service-card cw-upm-profile-card" data-action="edit-profile" data-profile-id="${esc(profile.id)}">
        <span class="cw-auth-provider-mark cw-upm-card-icon${profile.avatar_url ? " has-image" : ""}">${profile.avatar_url ? `<img src="${esc(profile.avatar_url)}" alt="">` : `<span class="material-symbols-rounded cw-auth-provider-icon" aria-hidden="true">${canLogin ? "how_to_reg" : "person"}</span>`}</span>
        <span class="cw-auth-service-copy cw-upm-card-copy">
          <strong>${esc(profile.label || profile.id)}</strong>
          <small>${esc(assignedUserSummary(profile))}</small>
          <small>${esc(assignmentSummary(profile))}</small>
        </span>
        <span class="cw-upm-card-pill">${count}</span>
        <span class="cw-upm-card-arrow material-symbols-rounded" aria-hidden="true">chevron_right</span>
      </button>`;
  }

  function renderAddCard() {
    return `
      <button type="button" class="cw-auth-service-card cw-auth-add-card cw-upm-profile-card cw-upm-add-card" data-action="new-profile">
        <span class="cw-auth-add-mark cw-upm-add-mark"><span class="material-symbols-rounded" aria-hidden="true">add</span></span>
        <span class="cw-auth-service-copy cw-upm-card-copy">
          <strong>Add profile</strong>
          <small>Accounts, pairs and an optional login.</small>
        </span>
        <span class="cw-upm-card-arrow material-symbols-rounded" aria-hidden="true">arrow_forward</span>
      </button>`;
  }

  function modalProfile() {
    return modalProfileId ? profileById(modalProfileId) : null;
  }

  function instanceLabel(provider, instance) {
    const id = txt(instance);
    const row = instanceRows(providerKey(provider)).find((item) => item.id === id) || instanceRows(provider).find((item) => item.id === id);
    return row?.label || id;
  }

  function refKey(provider, instance) {
    return `${providerKey(provider)}:${txt(instance || "default") || "default"}`;
  }

  function manualInstanceKeys() {
    const out = new Set();
    Object.entries(modalDraft?.instances || {}).forEach(([provider, values]) => {
      (Array.isArray(values) ? values : [values]).map(txt).filter(Boolean).forEach((value) => out.add(refKey(provider, value)));
    });
    return out;
  }

  function missingAccounts(row, keys) {
    return (Array.isArray(row?.instances) ? row.instances : []).filter((ref) => !keys.has(refKey(ref?.provider, ref?.instance)));
  }

  function resourceAssignedElsewhere(row) {
    const assigned = txt(row?.assigned_profile_id);
    return !!assigned && assigned !== txt(modalProfileId);
  }

  function suggestResources() {
    if (!modalDraft || modalDraft.suggested) return;
    modalDraft.suggested = true;
    const keys = manualInstanceKeys();
    if (!keys.size) return;
    RESOURCE_KEYS.forEach((key) => {
      resourceRows(key).forEach((row) => {
        if (!resourceAssignedElsewhere(row) && !missingAccounts(row, keys).length) setDraftResource(key, row.id, true);
      });
    });
  }

  function modalSteps() {
    return modalProfile() ? STEPS.filter((step) => step.key !== "review") : STEPS;
  }

  function currentStepKey() {
    const steps = modalSteps();
    return steps[Math.max(0, Math.min(wizardStep, steps.length - 1))]?.key || "name";
  }

  function draftName() {
    return txt(modalDraft?.label) || "this profile";
  }

  function passwordScore(password) {
    const pw = String(password || "");
    if (!pw) return 0;
    if (pw.length < MIN_PASSWORD) return 1;
    let score = 2;
    if (pw.length >= 12) score += 1;
    if ((/[a-z]/.test(pw) && /[A-Z]/.test(pw) && /\d/.test(pw)) || /[^A-Za-z0-9]/.test(pw)) score += 1;
    return Math.min(4, score);
  }

  function loginSummary() {
    const login = modalDraft?.login || {};
    if (login.user_id) return login.enabled ? `Logs in as ${login.username}` : `Login ${login.username} is disabled`;
    return login.wanted ? `Will log in as ${login.username || "..."}` : "No login";
  }

  function stepNote(step) {
    if (!modalProfile()) return step.note;
    if (step.key === "name") return txt(modalDraft?.label) || "Unnamed";
    if (step.key === "accounts") return plural(draftAccountCount(), "account");
    if (step.key === "resources") return `${draftResourceCount()} selected`;
    if (step.key === "login") return loginSummary();
    return step.note;
  }

  function renderPreview() {
    const name = txt(modalDraft?.label);
    const login = modalDraft?.login || {};
    return `
      <div class="cw-upw-preview">
        ${avatar(name, "lg", draftAvatarUrl())}
        <div class="cw-upw-preview-text">
          <strong data-upw-name>${esc(name || "New profile")}</strong>
          <small>${esc(loginSummary())}</small>
        </div>
        <div class="cw-upw-preview-stats">
          <span><b>${draftAccountCount()}</b>Accounts</span>
          <span><b>${draftResourceCount()}</b>Pairs and routes</span>
          <span class="${login.wanted ? "is-on" : ""}">${icon(login.wanted ? "lock_person" : "lock_open")}${login.wanted ? "Login" : "No login"}</span>
        </div>
      </div>`;
  }

  function renderRailSteps() {
    const editing = !!modalProfile();
    const visited = modalDraft?.visited || 0;
    return `
      <nav class="cw-upw-steps${editing ? " is-tabs" : ""}" aria-label="${editing ? "Profile sections" : "Steps"}">
        ${modalSteps().map((step, index) => {
          const done = !!modalDone || (!editing && index < wizardStep);
          const current = !modalDone && index === wizardStep;
          const reachable = !modalDone && (editing || index <= visited);
          return `
            <button type="button" class="cw-upw-step${current ? " is-current" : ""}${done ? " is-done" : ""}" data-action="go-step" data-step="${index}" ${reachable ? "" : "disabled"} ${current ? 'aria-current="step"' : ""}>
              <span class="cw-upw-step-dot">${done ? icon("check") : editing ? icon(step.icon) : index + 1}</span>
              <span class="cw-upw-step-text"><strong>${esc(step.label)}</strong><small>${esc(stepNote(step))}</small></span>
            </button>`;
        }).join("")}
      </nav>`;
  }

  function paneHead(title, lead) {
    const steps = modalSteps();
    const eyebrow = modalProfile() ? steps[wizardStep]?.label || "" : `Step ${wizardStep + 1} of ${steps.length}`;
    return `
      <div class="cw-upw-head">
        <span class="cw-upw-eyebrow">${esc(eyebrow)}</span>
        <h2>${esc(title)}</h2>
        ${lead ? `<p>${esc(lead)}</p>` : ""}
      </div>`;
  }

  function renderNameStep() {
    const name = txt(modalDraft?.label);
    const editing = !!modalProfile();
    return `
      ${paneHead(editing ? "Profile name" : "Who is this profile for?", "Give it a name you will recognise. You can always view CrossWatch as any profile.")}
      <div class="cw-upw-name">
        <div class="cw-upw-photo">
          <label class="cw-upw-photo-pick" title="${draftAvatarUrl() ? "Change picture" : "Add picture"}">
            ${avatar(name, "xl", draftAvatarUrl())}
            <span class="cw-upw-photo-badge">${icon("photo_camera")}</span>
            <input type="file" accept="image/png,image/jpeg,image/webp" data-upw-photo hidden aria-label="${draftAvatarUrl() ? "Change picture" : "Add picture"}">
          </label>
          ${draftAvatarUrl() ? `<button type="button" class="cw-upw-photo-remove" data-action="remove-photo" title="Remove picture" aria-label="Remove picture">${icon("close")}</button>` : ""}
        </div>
        <div class="cw-upw-name-field">
          <input class="cw-upw-input is-xl" id="cw-upm-modal-label" type="text" maxlength="40" value="${esc(name)}" placeholder="Type a name" autocomplete="off" spellcheck="false" aria-label="Profile name">
          ${editing ? "" : `<div class="cw-upw-ideas"><span>Ideas</span>${NAME_IDEAS.map((idea) => `<button type="button" class="cw-upw-chip" data-action="name-idea" data-value="${esc(idea)}">${esc(idea)}</button>`).join("")}</div>`}
        </div>
      </div>
      ${editing ? "" : `
        <div class="cw-upw-explain">
          <div>${icon("hub")}<strong>Their accounts</strong><span>Pick which provider accounts belong to them.</span></div>
          <div>${icon("sync_alt")}<strong>Their sync</strong><span>Pairs and routes that use those accounts.</span></div>
          <div>${icon("key")}<strong>Optional login</strong><span>Let them sign in and see only their own profile.</span></div>
        </div>`}`;
  }

  function syncUsersNote(provider, selected) {
    const labels = syncUserLabels[providerKey(provider)] || {};
    const names = [...selected].filter((id) => id.includes("~")).map((id) => labels[id] || id.split("~").slice(1).join("~"));
    return names.length ? `<p class="cw-upw-note">${icon("group")}Identities: ${esc(names.join(", "))}. Managed in the connection's Identities tab.</p>` : "";
  }

  function renderAccountsStep() {
    const insts = draftInstances();
    const required = resourceRequiredMap();
    const keys = providerKeys();
    const head = paneHead(`Which accounts belong to ${draftName()}?`, "Select accounts for this profile. Accounts assigned elsewhere are locked. Default instances cannot be assigned.");
    if (!keys.length) {
      return `${head}${emptyState("link_off", "No named accounts yet", "Default connections stay with the admin. Add a named connection such as PLEX-P01 under Connections, then come back.")}`;
    }
    return `
      ${head}
      <div class="cw-upw-toolbar"><span class="cw-upw-count"><b>${draftAccountCount()}</b> selected</span></div>
      <div class="cw-upw-providers">
        ${keys.map((provider) => {
          const selected = new Set((insts[provider] || []).map(txt));
          const rows = instanceRows(provider);
          const picked = rows.filter((row) => selected.has(row.id)).length;
          return `
            <section class="cw-upw-provider" data-provider="${esc(provider)}">
              <div class="cw-upw-section-head">${providerMark(provider)}<strong>${esc(providerLabel(provider))}</strong><span class="cw-upw-badge${picked ? " is-on" : ""}">${picked} of ${rows.length}</span></div>
              ${syncUsersNote(provider, selected)}
              <div class="cw-upw-tiles">
                ${rows.map((row) => {
                  const neededBy = required[providerKey(provider)]?.[row.id] || [];
                  const owner = instanceAssignedProfile(provider, row.id);
                  const needed = neededBy.length > 0;
                  const isOn = selected.has(row.id) || needed;
                  const state = owner
                    ? `<span class="cw-upw-tag is-lock">${icon("lock")}${esc(owner.label || owner.id)}</span>`
                    : needed ? `<span class="cw-upw-tag is-good">${icon("link")}Needed</span>` : `<span class="cw-upw-tick">${icon("check")}</span>`;
                  return `
                    <label class="cw-upw-tile${isOn ? " is-selected" : ""}${owner ? " is-locked" : ""}${needed ? " is-required" : ""}" title="${esc(owner ? `Belongs to ${owner.label || owner.id}` : needed ? `Needed by ${Array.from(new Set(neededBy)).join(", ")}` : "")}">
                      <input class="cw-upw-check" type="checkbox" name="cw-upm-provider-${esc(provider)}" value="${esc(row.id)}" ${isOn ? "checked" : ""} ${needed || owner ? "disabled" : ""}>
                      <span class="cw-upw-tile-text"><strong>${esc(row.label)}</strong><code>${esc(row.id)}</code></span>
                      ${state}
                    </label>`;
                }).join("")}
              </div>
            </section>`;
        }).join("")}
      </div>`;
  }

  function routeEnd(ref) {
    return `<span class="cw-upw-end">${providerMark(ref?.provider)}<span><strong>${esc(providerLabel(ref?.provider))}</strong><small>${esc(instanceLabel(ref?.provider, ref?.instance))}</small></span></span>`;
  }

  function renderResourceGroup(key, keys) {
    const rows = resourceRows(key).slice().sort((a, b) => (missingAccounts(a, keys).length ? 1 : 0) - (missingAccounts(b, keys).length ? 1 : 0));
    if (!rows.length) return "";
    const selected = draftResourceSet(key);
    const picked = rows.filter((row) => selected.has(txt(row.id))).length;
    return `
      <section class="cw-upw-group">
        <div class="cw-upw-section-head">${icon(RESOURCE_ICONS[key])}<strong>${esc(resourceTitle(key))}</strong><span class="cw-upw-badge${picked ? " is-on" : ""}">${picked} of ${rows.length}</span></div>
        <div class="cw-upw-routes">
          ${rows.map((row) => {
            const owner = resourceAssignedElsewhere(row) ? profileLabelForId(row.assigned_profile_id) || "another profile" : "";
            const isOn = selected.has(txt(row.id));
            const refs = Array.isArray(row.instances) ? row.instances : [];
            const missing = missingAccounts(row, keys);
            const features = Array.isArray(row.features) ? row.features.map(txt).filter(Boolean) : [];
            const meta = owner
              ? `<span class="cw-upw-tag is-lock">${icon("lock")}${esc(owner)}</span>`
              : missing.length
                ? `<span class="cw-upw-tag">${icon("add_link")}Adds ${esc(missing.map((ref) => `${providerLabel(ref?.provider)} ${instanceLabel(ref?.provider, ref?.instance)}`).join(", "))}</span>`
                : `<span class="cw-upw-tag is-good">${icon("auto_awesome")}Matches their accounts</span>`;
            return `
              <label class="cw-upw-route${isOn ? " is-selected" : ""}${owner ? " is-locked" : ""}" data-resource-option>
                <input class="cw-upw-check" type="checkbox" data-resource-key="${esc(key)}" value="${esc(row.id)}" ${isOn ? "checked" : ""} ${owner ? "disabled" : ""}>
                <span class="cw-upw-route-flow">${refs[0] ? routeEnd(refs[0]) : ""}<span class="cw-upw-route-arrow">${icon("arrow_forward")}</span>${refs[1] ? routeEnd(refs[1]) : ""}</span>
                <span class="cw-upw-route-meta">${meta}${features.map((feature) => `<em>${esc(feature)}</em>`).join("")}</span>
                <span class="cw-upw-switch" aria-hidden="true"></span>
              </label>`;
          }).join("")}
        </div>
      </section>`;
  }

  function renderResourcesStep() {
    const keys = manualInstanceKeys();
    const total = RESOURCE_KEYS.reduce((sum, key) => sum + resourceRows(key).length, 0);
    const head = paneHead(`What runs for ${draftName()}?`, total ? "Everything that only uses this profile's accounts is switched on for you. Switching on another one also adds the accounts it needs." : "");
    if (!total) return `${head}${emptyState("route", "Nothing to assign yet", "No sync pairs or routes use named accounts. You can create this profile now and assign them later.")}`;
    return `${head}<div class="cw-upw-groups">${RESOURCE_KEYS.map((key) => renderResourceGroup(key, keys)).join("")}</div>`;
  }

  function emptyState(iconName, title, text) {
    return `<div class="cw-upw-empty"><span class="cw-upw-empty-icon">${icon(iconName)}</span><strong>${esc(title)}</strong><span>${esc(text)}</span></div>`;
  }

  function renderStrength(password) {
    const score = passwordScore(password);
    return `<div class="cw-upw-strength" data-upw-strength data-score="${score}"><span class="cw-upw-strength-bar"><i></i><i></i><i></i><i></i></span><span class="cw-upw-strength-text">${esc(STRENGTH_LABELS[score])}</span></div>`;
  }

  function renderField(label, control, hint = "") {
    return `<label class="cw-upw-field"><span class="cw-upw-label">${esc(label)}</span>${control}${hint}</label>`;
  }

  function renderUsernameField(login) {
    return renderField("Username", `<span class="cw-upw-control">${icon("alternate_email")}<input class="cw-upw-input" id="cw-upm-login-username" type="text" maxlength="64" value="${esc(login.username)}" autocomplete="off" spellcheck="false"></span>`);
  }

  function renderPasswordField(login, existing) {
    const control = `
      <span class="cw-upw-control">
        ${icon("key")}
        <input class="cw-upw-input" id="cw-upm-login-password" type="${showPassword ? "text" : "password"}" value="${esc(login.password)}" placeholder="${existing ? "Leave empty to keep it" : `At least ${MIN_PASSWORD} characters`}" autocomplete="new-password" spellcheck="false">
        <button type="button" class="cw-upw-icon" data-action="toggle-password" title="${showPassword ? "Hide password" : "Show password"}" aria-label="${showPassword ? "Hide password" : "Show password"}">${icon(showPassword ? "visibility_off" : "visibility")}</button>
        <button type="button" class="cw-upw-icon" data-action="copy-password" title="Copy password" aria-label="Copy password">${icon("content_copy")}</button>
      </span>`;
    const hint = `<span class="cw-upw-field-foot">${renderStrength(login.password)}<button type="button" class="cw-upw-link" data-action="generate-password">${icon("magic_button")}Generate a strong one</button></span>`;
    return renderField(existing ? "New password" : "Password", control, hint);
  }

  function renderAccessChoice(login) {
    const modes = login.initialAccess === "monitor" ? ["full", "readonly", "monitor"] : ["full", "readonly"];
    return `
      <div class="cw-upw-field">
        <span class="cw-upw-label">Access</span>
        <div class="cw-upw-segment">
          ${modes.map((mode) => `
            <button type="button" class="cw-upw-option${login.access === mode ? " is-selected" : ""}" data-action="access-choice" data-value="${mode}" aria-pressed="${login.access === mode}">
              ${icon(ACCESS_ICONS[mode])}
              <span><strong>${esc(ACCESS_LABELS[mode])}</strong><small>${esc(ACCESS_NOTES[mode])}</small></span>
            </button>`).join("")}
        </div>
      </div>
      ${login.access === "full" ? `
        <div class="cw-upw-field">
          <span class="cw-upw-label">Settings they can manage</span>
          <div class="cw-upw-grants">
            ${SETTINGS_GRANTS.map(([key, label, note]) => `
              <label class="cw-upw-grant">
                <input class="cw-upw-check" type="checkbox" data-upw-grant="${key}" ${login.grants?.[key] !== false ? "checked" : ""}>
                <span class="cw-upw-toggle-track" aria-hidden="true"></span>
                <span><strong>${esc(label)}</strong><small>${esc(note)}</small></span>
              </label>`).join("")}
          </div>
          <p class="cw-upw-note">${icon("info")}Only their own accounts, pairs, routes and schedule. Adding accounts, provider settings and global settings stay with you.</p>
        </div>` : ""}`;
  }

  function oidcClaimLabel(claim) {
    return (OIDC_CLAIMS.find(([key]) => key === claim) || [, "Username"])[1];
  }

  function renderOidcField(login) {
    if (!oidcConfigured) return "";
    if (login.oidcLinked) {
      return `
        <div class="cw-upw-field">
          <span class="cw-upw-label">OIDC sign-in</span>
          <div class="cw-upw-card-actions is-split">
            <span class="cw-upw-tag is-good">${icon("verified_user")}Linked${login.oidcLabel ? ` to ${esc(login.oidcLabel)}` : ""}</span>
            <button type="button" class="cw-upw-btn is-danger-ghost" data-action="oidc-unlink">${icon("link_off")}<span>Unlink</span></button>
          </div>
        </div>`;
    }
    const current = OIDC_CLAIMS.find(([key]) => key === login.oidcClaim) || OIDC_CLAIMS[0];
    const waiting = login.initialOidcValue && login.oidcValue === login.initialOidcValue && login.oidcClaim === login.initialOidcClaim;
    return `
      <div class="cw-upw-field">
        <span class="cw-upw-label">OIDC sign-in (optional)</span>
        <div class="cw-upw-segment">
          ${OIDC_CLAIMS.map(([key, label, iconName]) => `
            <button type="button" class="cw-upw-option${current[0] === key ? " is-selected" : ""}" data-action="oidc-claim" data-value="${key}" aria-pressed="${current[0] === key}">
              ${icon(iconName)}
              <span><strong>${esc(label)}</strong></span>
            </button>`).join("")}
        </div>
        <span class="cw-upw-control">${icon("passkey")}<input class="cw-upw-input" id="cw-upm-login-oidc" type="text" maxlength="256" value="${esc(login.oidcValue)}" placeholder="${esc(`${current[1]} at your identity provider`)}" autocomplete="off" spellcheck="false"></span>
        <p class="cw-upw-note">${icon(waiting ? "schedule" : "info")}${esc(waiting ? `Waiting for their first OIDC sign-in as ${login.initialOidcValue}.` : current[3])}</p>
      </div>`;
  }

  function renderAuthSetup(login) {
    return `
      <div class="cw-upw-card is-warning">
        <div class="cw-upw-callout">
          <span class="cw-upw-callout-icon">${icon("lock_open")}</span>
          <div><strong>Sign-in is turned off</strong><span>Logins only work once CrossWatch asks for a password. Set the administrator login first; you stay signed in.</span></div>
        </div>
        <div class="cw-upw-grid">
          ${renderField("Administrator username", `<span class="cw-upw-control">${icon("shield_person")}<input class="cw-upw-input" id="cw-upm-admin-username" type="text" value="${esc(login.adminUsername)}" autocomplete="username"></span>`)}
          ${renderField("Administrator password", `<span class="cw-upw-control">${icon("key")}<input class="cw-upw-input" id="cw-upm-admin-password" type="password" value="${esc(login.adminPassword)}" placeholder="At least ${MIN_PASSWORD} characters" autocomplete="new-password"></span>`)}
        </div>
        <div class="cw-upw-card-actions"><button type="button" class="cw-upw-btn is-primary" data-action="enable-auth">${icon("lock")}<span>Turn on sign-in</span></button></div>
      </div>`;
  }

  function renderToggle(id, checked, label) {
    return `<label class="cw-upw-toggle"><input class="cw-upw-check" type="checkbox" id="${esc(id)}" ${checked ? "checked" : ""}><span class="cw-upw-toggle-track" aria-hidden="true"></span><span>${esc(label)}</span></label>`;
  }

  function renderExistingLogin(login) {
    const others = profileLogins(modalProfileId).filter((user) => txt(user?.id) !== login.user_id);
    if (!authActive) {
      return `${paneHead(`${draftName()} has a login`, "")}${emptyState("lock_open", "Sign-in is turned off", "This login cannot be used or changed until sign-in is turned on in Security.")}`;
    }
    return `
      ${paneHead("Sign-in", `${draftName()} can log in to CrossWatch. Change the username, reset the password or switch access.`)}
      <div class="cw-upw-card">
        <div class="cw-upw-account">
          ${avatar(login.username)}
          <div><strong>${esc(login.initialUsername)}</strong><small>${login.totp ? "Two-factor sign-in on" : "Password sign-in"}</small></div>
          <span class="cw-upw-tag${login.initialEnabled ? " is-good" : " is-off"}">${icon(login.initialEnabled ? "check_circle" : "block")}${login.initialEnabled ? "Active" : "Disabled"}</span>
        </div>
        <div class="cw-upw-grid">
          ${renderUsernameField(login)}
          ${renderPasswordField(login, true)}
        </div>
        ${renderAccessChoice(login)}
        ${renderOidcField(login)}
        <div class="cw-upw-card-actions is-split">
          ${renderToggle("cw-upm-login-enabled", login.enabled, "Login enabled")}
          <button type="button" class="cw-upw-btn is-danger-ghost" data-action="remove-login">${icon("person_remove")}<span>Remove login</span></button>
        </div>
        ${others.length ? `<p class="cw-upw-note">${icon("group")}Also linked: ${esc(others.map((user) => txt(user?.username || user?.id)).join(", "))}.</p>` : ""}
      </div>`;
  }

  function renderLoginStep() {
    const login = modalDraft?.login || makeLoginDraft(null);
    if (login.user_id) return renderExistingLogin(login);
    const choice = (value, iconName, title, note) => `
      <button type="button" class="cw-upw-choice${login.wanted === value ? " is-selected" : ""}" data-action="login-choice" data-value="${value ? "yes" : "no"}" aria-pressed="${login.wanted === value}">
        <span class="cw-upw-choice-icon">${icon(iconName)}</span>
        <strong>${esc(title)}</strong>
        <small>${esc(note)}</small>
        <span class="cw-upw-radio" aria-hidden="true"></span>
      </button>`;
    return `
      ${paneHead(`Can ${draftName()} log in to CrossWatch?`, "Without a login only you can use this profile, by viewing CrossWatch as it.")}
      <div class="cw-upw-choices">
        ${choice(false, "visibility", "No login", "Just a profile you can switch to.")}
        ${choice(true, "login", "Yes, give them a login", "They sign in with their own username and only see this profile.")}
      </div>
      ${login.wanted ? (authActive ? `
        <div class="cw-upw-card">
          <div class="cw-upw-grid">
            ${renderUsernameField(login)}
            ${renderPasswordField(login, false)}
          </div>
          ${renderAccessChoice(login)}
          ${renderOidcField(login)}
        </div>` : renderAuthSetup(login)) : ""}`;
  }

  function reviewTile(stepKey, iconName, title, body) {
    const index = modalSteps().findIndex((step) => step.key === stepKey);
    return `
      <section class="cw-upw-review-tile">
        <div class="cw-upw-section-head">${icon(iconName)}<strong>${esc(title)}</strong><button type="button" class="cw-upw-link" data-action="go-step" data-step="${index}">Change</button></div>
        ${body}
      </section>`;
  }

  function renderReviewStep() {
    const instances = draftInstances();
    const accounts = Object.entries(instances).flatMap(([provider, values]) => values.map((value) => ({ provider, label: instanceLabel(provider, value) })));
    const routes = [];
    RESOURCE_KEYS.forEach((key) => {
      const selected = draftResourceSet(key);
      resourceRows(key).filter((row) => selected.has(txt(row.id))).forEach((row) => routes.push({ key, row }));
    });
    const login = modalDraft?.login || {};
    const none = (text) => `<p class="cw-upw-none">${esc(text)}</p>`;
    const accountList = accounts.length
      ? `<ul class="cw-upw-list">${accounts.map((item) => `<li>${providerMark(item.provider)}<span><strong>${esc(providerLabel(item.provider))}</strong><small>${esc(item.label)}</small></span></li>`).join("")}</ul>`
      : none("No accounts picked");
    const routeList = routes.length
      ? `<ul class="cw-upw-list">${routes.map(({ key, row }) => {
          const refs = Array.isArray(row.instances) ? row.instances : [];
          return `<li>${icon(RESOURCE_ICONS[key])}<span><strong>${esc(refs.map((ref) => providerLabel(ref?.provider)).join(" → ") || row.label)}</strong><small>${esc(resourceTitle(key).replace(/s$/, ""))}</small></span></li>`;
        }).join("")}</ul>`
      : none("Nothing switched on");
    const oidcItem = login.wanted && oidcConfigured && txt(login.oidcValue)
      ? `<li>${icon("passkey")}<span><strong>${esc(txt(login.oidcValue))}</strong><small>${esc(`OIDC ${oidcClaimLabel(login.oidcClaim).toLowerCase()}`)}</small></span></li>`
      : "";
    const loginBody = login.wanted
      ? `<ul class="cw-upw-list"><li>${icon("alternate_email")}<span><strong>${esc(login.username)}</strong><small>${esc([ACCESS_LABELS[login.access] || "", grantsSummary(login)].filter(Boolean).join(" · "))}</small></span></li>${oidcItem}</ul>`
      : none("No login, view only");
    return `
      ${paneHead("Ready to create", "Have a last look. You can change all of this later.")}
      <div class="cw-upw-summary">
        <div class="cw-upw-summary-hero">
          ${avatar(modalDraft?.label, "lg", draftAvatarUrl())}
          <div><strong>${esc(modalDraft?.label || "")}</strong><small>New profile</small></div>
        </div>
        <div class="cw-upw-review">
          ${reviewTile("accounts", "hub", "Accounts", accountList)}
          ${reviewTile("resources", "sync_alt", "Pairs and routes", routeList)}
          ${reviewTile("login", "key", "Sign-in", loginBody)}
        </div>
      </div>`;
  }

  function renderDone() {
    const done = modalDone || {};
    const copyRow = (label, value, iconName) => `
      <div class="cw-upw-cred">
        ${icon(iconName)}
        <span><small>${esc(label)}</small><strong>${esc(value)}</strong></span>
        <button type="button" class="cw-upw-icon" data-action="copy" data-value="${esc(value)}" data-label="${esc(label)}" title="Copy ${esc(label.toLowerCase())}" aria-label="Copy ${esc(label.toLowerCase())}">${icon("content_copy")}</button>
      </div>`;
    const canViewAs = !!w.CW?.OverviewProfile?.setActive && done.id;
    return `
      <div class="cw-upw-done">
        <div class="cw-upw-done-mark">
          ${avatar(done.label, "xl", done.avatarUrl)}
          <span class="cw-upw-done-check"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 12.5l4.5 4.5L19 7.5"/></svg></span>
        </div>
        <h2>${esc(done.label)} is ready</h2>
        <p>${done.username ? "Share these sign-in details. The password is not shown again." : "The profile is saved. You can view CrossWatch as it at any time."}</p>
        ${done.username ? `<div class="cw-upw-creds">${copyRow("Username", done.username, "alternate_email")}${done.password ? copyRow("Password", done.password, "key") : ""}</div>` : ""}
        <div class="cw-upw-done-actions">
          <button type="button" class="cw-upw-btn" data-action="add-another">${icon("person_add")}<span>Add another</span></button>
          ${canViewAs ? `<button type="button" class="cw-upw-btn" data-action="view-as">${icon("visibility")}<span>View as ${esc(done.label)}</span></button>` : ""}
          <button type="button" class="cw-upw-btn is-primary" data-action="close-modal">${icon("check")}<span>Done</span></button>
        </div>
      </div>`;
  }

  function renderStepPane(key) {
    if (key === "name") return renderNameStep();
    if (key === "accounts") return renderAccountsStep();
    if (key === "resources") return renderResourcesStep();
    if (key === "login") return renderLoginStep();
    return renderReviewStep();
  }

  function renderFooter(editing) {
    if (modalDone) return "";
    const steps = modalSteps();
    const last = wizardStep >= steps.length - 1;
    const left = editing
      ? `<button type="button" class="cw-upw-btn is-danger-ghost" data-action="delete-profile">${icon("delete")}<span>Delete profile</span></button><span class="cw-upw-warn" role="alert" aria-live="polite" hidden></span>`
      : wizardStep > 0
        ? `<button type="button" class="cw-upw-btn is-ghost" data-action="prev-step">${icon("arrow_back")}<span>Back</span></button>`
        : `<button type="button" class="cw-upw-btn is-ghost" data-action="close-modal"><span>Cancel</span></button>`;
    const dots = editing ? "<span></span>" : `<span class="cw-upw-dots" aria-hidden="true">${steps.map((_, index) => `<i class="${index === wizardStep ? "is-current" : index < wizardStep ? "is-done" : ""}"></i>`).join("")}</span>`;
    const primary = editing
      ? `<button type="button" class="cw-upw-btn is-ghost" data-action="close-modal"><span>Cancel</span></button><button type="button" class="cw-upw-btn is-primary" data-action="save-profile">${icon("save")}<span>Save changes</span></button>`
      : last
        ? `<button type="button" class="cw-upw-btn is-primary" data-action="save-profile">${icon("check")}<span>Create profile</span></button>`
        : `<button type="button" class="cw-upw-btn is-primary" data-action="next-step"><span>Continue</span>${icon("arrow_forward")}</button>`;
    return `
      <div class="cw-upw-foot">
        <div class="cw-upw-foot-side">${left}</div>
        ${dots}
        <div class="cw-upw-foot-side is-end">${primary}</div>
      </div>`;
  }

  function renderModal() {
    if (modalProfileId === null) return "";
    const editing = !!modalProfile() && !modalDone;
    const active = modalDone ? "done" : currentStepKey();
    const entering = active !== shownPaneKey;
    const steps = modalSteps();
    const progress = modalDone ? 100 : Math.round(((wizardStep + 1) / steps.length) * 100);
    const body = modalDone
      ? `<section class="cw-upw-pane is-entering" data-step-pane="done">${renderDone()}</section>`
      : steps.map((step) => `<section class="cw-upw-pane${step.key === active && entering ? " is-entering" : ""}" data-step-pane="${esc(step.key)}" ${step.key === active ? "" : "hidden"}>${renderStepPane(step.key)}</section>`).join("");
    return `
      <div class="cw-upw-overlay${modalOpening ? " is-opening" : ""}">
        <div class="cw-upw${editing ? " is-editing" : ""}" role="dialog" aria-modal="true" aria-labelledby="cw-upw-title" data-upw-root data-modal-root>
          <aside class="cw-upw-rail">
            <div class="cw-upw-rail-head">
              <span class="cw-upw-kicker">${icon("group")}Profiles</span>
              <strong id="cw-upw-title">${editing ? "Edit profile" : "New profile"}</strong>
            </div>
            ${renderPreview()}
            ${renderRailSteps()}
          </aside>
          <section class="cw-upw-main">
            <div class="cw-upw-top">
              <div class="cw-upw-progress" aria-hidden="true"><span style="width:${progress}%"></span></div>
              <button type="button" class="cw-upw-close" data-action="close-modal" title="Close" aria-label="Close">${icon("close")}</button>
            </div>
            <div class="cw-upw-body" data-upw-body>${body}</div>
            ${renderFooter(editing)}
          </section>
        </div>
      </div>`;
  }

  function setHostHtml(host, html) {
    if (host.__cwUpmHtml === html) return false;
    host.__cwUpmHtml = html;
    host.innerHTML = html;
    return true;
  }

  function invalidateHostHtml() {
    [$(HOST_ID), $(MODAL_HOST_ID)].forEach((node) => {
      if (node) node.__cwUpmHtml = null;
    });
  }

  function listHtml() {
    return `
      <div class="cw-auth-service-grid cw-upm-service-grid">
        ${[...userProfiles].sort((a, b) => a.label.localeCompare(b.label, undefined, { sensitivity: "base", numeric: true })).map(renderProfileCard).join("")}
        ${renderAddCard()}
      </div>
      <div class="cw-upm-status" id="cw-upm-status"></div>`;
  }

  function renderModalHost(host) {
    const bodyBefore = host.querySelector("[data-upw-body]");
    const scroll = bodyBefore ? bodyBefore.scrollTop : 0;
    const paneBefore = shownPaneKey;
    const focusId = d.activeElement?.id && host.contains(d.activeElement) ? d.activeElement.id : "";
    const selection = focusId && typeof d.activeElement.selectionStart === "number" ? [d.activeElement.selectionStart, d.activeElement.selectionEnd] : null;
    const html = renderModal();
    if (!setHostHtml(host, html)) return;
    shownPaneKey = modalProfileId === null ? "" : modalDone ? "done" : currentStepKey();
    modalOpening = false;
    const bodyAfter = host.querySelector("[data-upw-body]");
    if (bodyAfter && paneBefore === shownPaneKey) bodyAfter.scrollTop = scroll;
    if (focusId) {
      const node = $(focusId);
      if (node && host.contains(node)) {
        try {
          node.focus({ preventScroll: true });
          if (selection && typeof node.setSelectionRange === "function") node.setSelectionRange(selection[0], selection[1]);
        } catch {}
      }
    }
  }

  function render() {
    const listHost = $(HOST_ID);
    if (listHost) setHostHtml(listHost, listHtml());
    const modalHost = $(MODAL_HOST_ID);
    if (modalHost) renderModalHost(modalHost);
  }

  function patchPreview() {
    const root = d.querySelector("[data-upw-root]");
    if (!root || !modalDraft) return;
    const name = txt(modalDraft.label);
    root.querySelectorAll(".cw-upw-preview [data-upw-avatar]:not(.has-image), .cw-upw-name [data-upw-avatar]:not(.has-image)").forEach((node) => {
      node.style.setProperty("--h", String(hueFor(name)));
      node.innerHTML = initialsFor(name) ? esc(initialsFor(name)) : icon("person");
    });
    const label = root.querySelector("[data-upw-name]");
    if (label) label.textContent = name || "New profile";
    invalidateHostHtml();
  }

  function patchStrength() {
    const node = d.querySelector("[data-upw-root] [data-upw-strength]");
    if (!node || !modalDraft?.login) return;
    const score = passwordScore(modalDraft.login.password);
    node.dataset.score = String(score);
    const label = node.querySelector(".cw-upw-strength-text");
    if (label) label.textContent = STRENGTH_LABELS[score];
    invalidateHostHtml();
  }

  async function loadLogins() {
    try {
      const status = await json(`/api/app-auth/status?ts=${Date.now()}`);
      authActive = !!(status?.enabled && status?.configured);
    } catch {
      authActive = false;
    }
    try {
      const userData = await json(`/api/app-auth/users?ts=${Date.now()}`);
      appUsers = Array.isArray(userData?.items) ? userData.items : [];
      oidcConfigured = userData?.oidc_configured === true;
    } catch {
      appUsers = [];
      oidcConfigured = false;
    }
  }

  async function load(force = false) {
    if (!($(HOST_ID) || $(MODAL_HOST_ID)) || (loading && !force)) return;
    loading = true;
    render();
    try {
      const [instances, profiles, syncLabels] = await Promise.all([
        json(`/api/provider-instances?configured_only=true&user_profile=all&ts=${Date.now()}`),
        json(`/api/user-profiles?ts=${Date.now()}`),
        json(`/api/sync-user-labels?user_profile=all&ts=${Date.now()}`).catch(() => ({})),
      ]);
      providerInstances = instances && typeof instances === "object" ? instances : {};
      syncUserLabels = syncLabels && typeof syncLabels === "object" ? syncLabels : {};
      availableResources = profiles?.resources && typeof profiles.resources === "object" ? profiles.resources : { sync_pairs: [], watcher_routes: [], webhook_routes: [] };
      userProfiles = Array.isArray(profiles?.items) ? profiles.items.map((row) => ({
        id: txt(row?.id),
        label: txt(row?.label || row?.id),
        instances: row?.instances && typeof row.instances === "object" ? row.instances : {},
        manual_instances: row?.manual_instances && typeof row.manual_instances === "object" ? row.manual_instances : {},
        resources: row?.resources && typeof row.resources === "object" ? row.resources : {},
        resource_counts: row?.resource_counts && typeof row.resource_counts === "object" ? row.resource_counts : {},
        avatar_url: txt(row?.avatar_url),
      })).filter((row) => row.id && row.label) : [];
      await loadLogins();
      loadedOnce = true;
    } catch (e) {
      const status = $("cw-upm-status");
      if (status) {
        status.textContent = String(e?.message || e || "Failed to load profiles");
        invalidateHostHtml();
      }
      toast("Could not load profiles: " + String(e?.message || e), false);
    } finally {
      loading = false;
      render();
    }
  }

  async function afterSave(message, keepOpen = false) {
    try { w.invalidateConfigCache?.(); } catch {}
    await load(true);
    internalRefresh = true;
    try { w.dispatchEvent(new CustomEvent("cw-user-profiles-changed")); } catch {}
    try { w.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    internalRefresh = false;
    toast(message, true);
    if (!keepOpen) closeModal();
  }

  function ensureFloatingHost() {
    let host = $(MODAL_HOST_ID);
    if (!host) {
      host = d.createElement("div");
      host.id = MODAL_HOST_ID;
      host.className = "cw-upw-host";
      d.body.appendChild(host);
      attach(host);
    }
    d.documentElement.classList.add("cw-upw-lock");
    return host;
  }

  function focusFirstField() {
    setTimeout(() => {
      const target = d.querySelector("[data-upw-root] [data-step-pane]:not([hidden]) input.cw-upw-input");
      try { target?.focus({ preventScroll: true }); } catch {}
    }, 60);
  }

  async function openProfile(id) {
    ensureFloatingHost();
    await load(true);
    modalProfileId = txt(id);
    modalDraft = makeDraft(modalProfile());
    resetWizard();
    render();
    focusFirstField();
  }

  function openNewProfile() {
    ensureFloatingHost();
    modalProfileId = "";
    modalDraft = makeDraft(null);
    resetWizard();
    render();
    focusFirstField();
  }

  function resetWizard() {
    wizardStep = 0;
    showPassword = false;
    modalDone = null;
    modalOpening = true;
    shownPaneKey = "";
  }

  function closeModal() {
    resetDeleteConfirm();
    resetRemoveLogin();
    modalProfileId = null;
    modalDraft = null;
    modalDone = null;
    shownPaneKey = "";
    $(MODAL_HOST_ID)?.remove();
    d.documentElement.classList.remove("cw-upw-lock");
    render();
  }

  function collectModalInstances(root) {
    const instances = {};
    root.querySelectorAll(".cw-upw-provider[data-provider]").forEach((group) => {
      const provider = txt(group.dataset.provider);
      const values = Array.from(group.querySelectorAll("input:checked:not(:disabled)"))
        .map((input) => txt(input.value))
        .filter(Boolean);
      if (provider && values.length) instances[provider] = values;
    });
    return instances;
  }

  function collectModalResources(root) {
    const resources = { sync_pairs: [], watcher_routes: [], webhook_routes: [] };
    root.querySelectorAll("input[data-resource-key]:checked").forEach((input) => {
      const key = txt(input.dataset.resourceKey);
      const value = txt(input.value);
      if (resources[key] && value) resources[key].push(value);
    });
    return resources;
  }

  function setDraftResource(key, value, enabled) {
    if (!modalDraft) return;
    const cleanKey = txt(key);
    const cleanValue = txt(value);
    if (!cleanValue || !RESOURCE_KEYS.includes(cleanKey)) return;
    if (!modalDraft.resources || typeof modalDraft.resources !== "object") modalDraft.resources = { sync_pairs: [], watcher_routes: [], webhook_routes: [] };
    const next = draftResourceSet(cleanKey);
    if (enabled) next.add(cleanValue);
    else next.delete(cleanValue);
    modalDraft.resources[cleanKey] = Array.from(next);
  }

  function syncLoginFromDom(root) {
    const login = modalDraft?.login;
    if (!login) return;
    const field = (id) => root.querySelector(`#${id}`);
    if (field("cw-upm-login-username")) login.username = txt(field("cw-upm-login-username").value);
    if (field("cw-upm-login-password")) login.password = String(field("cw-upm-login-password").value || "");
    if (field("cw-upm-login-enabled")) login.enabled = !!field("cw-upm-login-enabled").checked;
    if (field("cw-upm-login-oidc")) login.oidcValue = txt(field("cw-upm-login-oidc").value);
    if (field("cw-upm-admin-username")) login.adminUsername = txt(field("cw-upm-admin-username").value);
    if (field("cw-upm-admin-password")) login.adminPassword = String(field("cw-upm-admin-password").value || "");
    root.querySelectorAll("input[data-upw-grant]").forEach((input) => {
      login.grants = { ...(login.grants || {}), [input.dataset.upwGrant]: !!input.checked };
    });
  }

  function syncDraftFromDom() {
    const root = d.querySelector("[data-upw-root]");
    if (!root || !modalDraft || modalDone) return;
    const previous = modalDraft.instances || {};
    const lockedBefore = resourceRequiredMap();
    syncLoginFromDom(root);
    const labelInput = $("cw-upm-modal-label");
    if (labelInput) modalDraft.label = txt(labelInput.value);
    modalDraft.resources = collectModalResources(root);
    const manual = collectModalInstances(root);
    Object.entries(previous).forEach(([provider, values]) => {
      const key = providerKey(provider);
      const locked = lockedBefore[key] || {};
      const kept = (Array.isArray(values) ? values : [values]).map(txt).filter((value) => value && (locked[value] || value.includes("~")));
      if (kept.length) manual[key] = Array.from(new Set([...(manual[key] || []), ...kept]));
    });
    modalDraft.instances = manual;
  }

  function goToStep(key, message) {
    const index = modalSteps().findIndex((step) => step.key === key);
    if (index >= 0) wizardStep = index;
    render();
    if (message) {
      toast(message, false);
      const pane = d.querySelector("[data-upw-root] [data-step-pane]:not([hidden])");
      pane?.classList.remove("is-shaking");
      void pane?.offsetWidth;
      pane?.classList.add("is-shaking");
      invalidateHostHtml();
    }
  }

  function stepProblem(key) {
    const login = modalDraft?.login || {};
    if (key === "name" && !txt(modalDraft?.label)) return "Enter a profile name";
    if (key !== "login" || !login.wanted) return "";
    if (login.user_id) {
      if (!authActive) return "";
      if (!txt(login.username)) return "Enter a username";
      if (login.password && login.password.length < MIN_PASSWORD) return `Password must be at least ${MIN_PASSWORD} characters`;
      return "";
    }
    if (!authActive) return "Turn on sign-in first, or choose No login";
    if (!txt(login.username)) return "Enter a username";
    if (login.password.length < MIN_PASSWORD) return `Password must be at least ${MIN_PASSWORD} characters`;
    return "";
  }

  function firstProblem() {
    for (const step of modalSteps()) {
      const problem = stepProblem(step.key);
      if (problem) return { key: step.key, problem };
    }
    return null;
  }

  function oidcPayload(login) {
    if (!oidcConfigured || login.oidcLinked) return null;
    const value = txt(login.oidcValue);
    if (!value) return login.initialOidcValue ? { claim: "", value: "" } : null;
    if (value === login.initialOidcValue && login.oidcClaim === login.initialOidcClaim) return null;
    return { claim: login.oidcClaim, value };
  }

  function loginPayload() {
    const login = modalDraft?.login || {};
    if (!login.wanted || !authActive) return null;
    const oidc = oidcPayload(login);
    if (!login.user_id) return { username: txt(login.username), password: login.password, permissions: accessPermissions(login.access, login.grants), ...(oidc ? { oidc } : {}) };
    const out = oidc ? { oidc } : {};
    if (txt(login.username) !== login.initialUsername) out.username = txt(login.username);
    if (login.password) out.password = login.password;
    const grantsChanged = SETTINGS_GRANTS.some(([key]) => (login.grants?.[key] !== false) !== (login.initialGrants?.[key] !== false));
    if (login.access !== login.initialAccess || (login.access === "full" && grantsChanged)) out.permissions = accessPermissions(login.access, login.grants);
    if (!!login.enabled !== login.initialEnabled) out.enabled = !!login.enabled;
    return Object.keys(out).length ? { user_id: login.user_id, ...out } : null;
  }

  function nextStep() {
    syncDraftFromDom();
    const key = currentStepKey();
    const problem = stepProblem(key);
    if (problem) {
      goToStep(key, problem);
      return;
    }
    wizardStep = Math.min(wizardStep + 1, modalSteps().length - 1);
    if (modalDraft) modalDraft.visited = Math.max(modalDraft.visited || 0, wizardStep);
    if (currentStepKey() === "resources") suggestResources();
    if (currentStepKey() === "login") prefillUsername();
    render();
    focusFirstField();
  }

  function setStep(index) {
    syncDraftFromDom();
    const steps = modalSteps();
    const target = Math.max(0, Math.min(Number(index) || 0, steps.length - 1));
    if (!modalProfile() && target > (modalDraft?.visited || 0)) return;
    wizardStep = target;
    if (currentStepKey() === "resources") suggestResources();
    render();
  }

  function primaryAction() {
    if (modalDone) return closeModal();
    if (modalProfile() || wizardStep >= modalSteps().length - 1) return saveProfile();
    return nextStep();
  }

  function prefillUsername() {
    const login = modalDraft?.login;
    if (!login || login.user_id || txt(login.username)) return;
    login.username = txt(modalDraft.label).toLowerCase().replace(/\s+/g, "");
  }

  function generatePassword() {
    const chars = "abcdefghjkmnpqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ23456789";
    const bytes = new Uint32Array(16);
    w.crypto.getRandomValues(bytes);
    return Array.from(bytes, (value) => chars[value % chars.length]).join("");
  }

  async function copyText(value, label) {
    const text = String(value || "");
    if (!text) {
      toast(`Nothing to copy yet`, false);
      return;
    }
    try {
      await w.navigator.clipboard.writeText(text);
      toast(`${label} copied`, true);
    } catch {
      toast("Copy is not available here", false);
    }
  }

  function chooseLogin(wanted) {
    syncDraftFromDom();
    if (!modalDraft?.login) return;
    modalDraft.login.wanted = wanted;
    if (wanted) prefillUsername();
    render();
  }

  function chooseAccess(mode) {
    syncDraftFromDom();
    if (!modalDraft?.login || !ACCESS_LABELS[mode]) return;
    modalDraft.login.access = mode;
    render();
  }

  function chooseOidcClaim(claim) {
    syncDraftFromDom();
    if (!modalDraft?.login || !OIDC_CLAIMS.some(([key]) => key === claim)) return;
    modalDraft.login.oidcClaim = claim;
    render();
  }

  async function unlinkOidc() {
    const userId = txt(modalDraft?.login?.user_id);
    if (!userId) return;
    syncDraftFromDom();
    await json("/api/app-auth/oidc/unlink", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ user_id: userId }),
    });
    await loadLogins();
    const user = appUsers.find((row) => txt(row?.id) === userId);
    Object.assign(modalDraft.login, oidcDraft(user));
    render();
    toast("OIDC account unlinked", true);
  }

  function chooseNameIdea(value) {
    if (!modalDraft) return;
    modalDraft.label = txt(value);
    render();
    const input = $("cw-upm-modal-label");
    try {
      input?.focus();
      input?.setSelectionRange(input.value.length, input.value.length);
    } catch {}
  }

  function togglePassword(generate) {
    syncDraftFromDom();
    if (generate && modalDraft?.login) {
      modalDraft.login.password = generatePassword();
      showPassword = true;
    } else {
      showPassword = !showPassword;
    }
    render();
  }

  async function enableAuth() {
    syncDraftFromDom();
    const login = modalDraft?.login;
    if (!login) return;
    if (!txt(login.adminUsername)) {
      toast("Enter an administrator username", false);
      return;
    }
    if (login.adminPassword.length < MIN_PASSWORD) {
      toast(`Administrator password must be at least ${MIN_PASSWORD} characters`, false);
      return;
    }
    await json("/api/app-auth/credentials", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: true, username: login.adminUsername, password: login.adminPassword }),
    });
    login.adminPassword = "";
    await loadLogins();
    prefillUsername();
    try { w.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    render();
    toast("Sign-in turned on", true);
  }

  function resetRemoveLogin(btn) {
    if (removeLoginTimer) clearTimeout(removeLoginTimer);
    removeLoginTimer = 0;
    const target = btn || d.querySelector('[data-upw-root] [data-action="remove-login"]');
    if (!target) return;
    target.dataset.cwConfirmDelete = "";
    target.classList.remove("is-confirming");
    target.innerHTML = `${icon("person_remove")}<span>Remove login</span>`;
    invalidateHostHtml();
  }

  async function removeLogin(btn) {
    const userId = txt(modalDraft?.login?.user_id);
    if (!userId) return;
    if (btn?.dataset?.cwConfirmDelete !== "1") {
      btn.dataset.cwConfirmDelete = "1";
      btn.classList.add("is-confirming");
      btn.innerHTML = `${icon("warning")}<span>Confirm remove login</span>`;
      invalidateHostHtml();
      if (removeLoginTimer) clearTimeout(removeLoginTimer);
      removeLoginTimer = setTimeout(() => resetRemoveLogin(btn), 4200);
      return;
    }
    resetRemoveLogin(btn);
    syncDraftFromDom();
    await json(`/api/app-auth/users/${encodeURIComponent(userId)}`, { method: "DELETE" });
    await loadLogins();
    modalDraft.login = makeLoginDraft(modalProfile());
    internalRefresh = true;
    try { w.dispatchEvent(new CustomEvent("cw-user-profiles-changed")); } catch {}
    try { w.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    internalRefresh = false;
    render();
    toast("Login removed", true);
  }

  async function saveAvatar(profileId) {
    const change = modalDraft?.avatarChange;
    const fallback = txt(modalDraft?.avatarUrl);
    if (!profileId || change === undefined) return fallback;
    const url = `/api/user-profiles/${encodeURIComponent(profileId)}/avatar`;
    try {
      if (change === "remove") {
        await json(url, { method: "DELETE" });
        return "";
      }
      const saved = await json(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ data: change.data, content_type: change.type }) });
      return txt(saved?.avatar_url);
    } catch (e) {
      toast(`Picture not saved: ${e?.message || e}`, false);
      return fallback;
    }
  }

  function readPhoto(input) {
    const file = input?.files?.[0];
    input.value = "";
    if (!file || !modalDraft) return;
    if (!["image/png", "image/jpeg", "image/webp"].includes(file.type)) {
      toast("Use a PNG, JPG or WebP image", false);
      return;
    }
    if (file.size > 5 * 1024 * 1024) {
      toast("The picture must be 5 MB or smaller", false);
      return;
    }
    const reader = new FileReader();
    reader.onload = () => {
      if (!modalDraft) return;
      syncDraftFromDom();
      modalDraft.avatarChange = { data: String(reader.result || ""), type: file.type };
      render();
    };
    reader.readAsDataURL(file);
  }

  async function saveProfile() {
    if (!d.querySelector("[data-upw-root]") || modalDone) return;
    clearDeleteWarning();
    syncDraftFromDom();
    const problem = firstProblem();
    if (problem) {
      goToStep(problem.key, problem.problem);
      return;
    }
    const label = txt(modalDraft?.label);
    const instances = modalDraft?.instances || {};
    const resources = modalDraft?.resources || { sync_pairs: [], watcher_routes: [], webhook_routes: [] };
    const login = loginPayload();
    const body = JSON.stringify({ label, instances, resources, ...(login ? { login } : {}) });
    const profile = modalProfile();
    const saved = await json(profile ? `/api/user-profiles/${encodeURIComponent(profile.id)}` : "/api/user-profiles", {
      method: profile ? "PUT" : "POST",
      headers: { "Content-Type": "application/json" },
      body,
    });
    const avatarUrl = await saveAvatar(profile ? profile.id : txt(saved?.profile?.id));
    if (profile) {
      modalProfileId = null;
      modalDraft = null;
      await afterSave("Profile saved");
      return;
    }
    modalDone = {
      id: txt(saved?.profile?.id),
      label,
      avatarUrl,
      username: login ? txt(saved?.login?.username || login.username) : "",
      password: login ? login.password : "",
    };
    await afterSave("Profile created", true);
  }

  function addAnother() {
    modalProfileId = "";
    modalDraft = makeDraft(null);
    resetWizard();
    modalOpening = false;
    render();
    focusFirstField();
  }

  async function viewAs() {
    const done = modalDone || {};
    closeModal();
    try {
      await w.CW?.OverviewProfile?.refresh?.();
      if (w.CW?.OverviewProfile?.setActive?.(done.id)) toast(`Viewing CrossWatch as ${done.label}`, true);
    } catch {}
  }

  function resetDeleteConfirm(btn) {
    if (deleteConfirmTimer) clearTimeout(deleteConfirmTimer);
    deleteConfirmTimer = 0;
    const target = btn || d.querySelector('[data-upw-root] [data-action="delete-profile"]');
    if (!target) return;
    target.dataset.cwConfirmDelete = "";
    target.classList.remove("is-confirming");
    target.innerHTML = `${icon("delete")}<span>Delete profile</span>`;
    invalidateHostHtml();
  }

  function armDeleteConfirm(btn) {
    if (!btn) return;
    btn.dataset.cwConfirmDelete = "1";
    btn.classList.add("is-confirming");
    const hasLogin = profileLogins(modalProfile()?.id).length > 0;
    btn.innerHTML = `${icon("warning")}<span>${hasLogin ? "Delete profile and login" : "Confirm delete"}</span>`;
    invalidateHostHtml();
    if (deleteConfirmTimer) clearTimeout(deleteConfirmTimer);
    deleteConfirmTimer = setTimeout(() => resetDeleteConfirm(btn), 4200);
  }

  function clearDeleteWarning() {
    const node = d.querySelector("[data-upw-root] .cw-upw-warn");
    if (!node) return;
    node.hidden = true;
    node.textContent = "";
    invalidateHostHtml();
  }

  function showDeleteWarning(message) {
    const text = txt(message) || "Could not delete profile";
    const node = d.querySelector("[data-upw-root] .cw-upw-warn");
    if (!node) {
      toast(text, false);
      return;
    }
    node.innerHTML = `${icon("warning")}<span></span>`;
    node.lastElementChild.textContent = text;
    node.hidden = false;
    invalidateHostHtml();
  }

  async function deleteProfile(btn) {
    const profile = modalProfile();
    if (!profile) return;
    clearDeleteWarning();
    if (btn?.dataset?.cwConfirmDelete !== "1") {
      armDeleteConfirm(btn);
      return;
    }
    resetDeleteConfirm(btn);
    try {
      const result = await json(`/api/user-profiles/${encodeURIComponent(profile.id)}`, { method: "DELETE" });
      modalProfileId = null;
      modalDraft = null;
      const removed = Array.isArray(result?.removed_logins) ? result.removed_logins : [];
      await afterSave(removed.length ? `Profile and login ${removed.join(", ")} deleted` : "Profile deleted");
    } catch (e) {
      showDeleteWarning(String(e?.message || e || "Could not delete profile"));
    }
  }

  function attach(host) {
    if (!host || host.__cwUserProfilesBound) return;
    host.__cwUserProfilesBound = true;
    host.addEventListener("click", (event) => {
      const resourceOption = event.target?.closest?.(".cw-upw-route[data-resource-option]");
      if (resourceOption && modalDraft) {
        event.preventDefault();
        const input = resourceOption.querySelector("input[data-resource-key]");
        if (!input || input.disabled) return;
        syncDraftFromDom();
        setDraftResource(input.dataset.resourceKey, input.value, !input.checked);
        render();
        return;
      }
      const actionNode = event.target?.closest?.("[data-action]");
      const action = actionNode?.dataset?.action || "";
      if (!action || actionNode.disabled) return;
      Promise.resolve()
        .then(() => {
          if (action === "refresh") return load(true);
          if (action === "new-profile") return load(true).then(() => openNewProfile());
          if (action === "edit-profile") return openProfile(actionNode.dataset.profileId);
          if (action === "close-modal") return closeModal();
          if (action === "save-profile") return saveProfile();
          if (action === "delete-profile") return deleteProfile(actionNode);
          if (action === "next-step") return nextStep();
          if (action === "prev-step") return setStep(wizardStep - 1);
          if (action === "go-step") return setStep(actionNode.dataset.step);
          if (action === "name-idea") return chooseNameIdea(actionNode.dataset.value);
          if (action === "remove-photo") {
            syncDraftFromDom();
            modalDraft.avatarChange = modalDraft.avatarUrl ? "remove" : undefined;
            return render();
          }
          if (action === "login-choice") return chooseLogin(actionNode.dataset.value === "yes");
          if (action === "access-choice") return chooseAccess(actionNode.dataset.value);
          if (action === "oidc-claim") return chooseOidcClaim(actionNode.dataset.value);
          if (action === "oidc-unlink") return unlinkOidc();
          if (action === "toggle-password") return togglePassword(false);
          if (action === "generate-password") return togglePassword(true);
          if (action === "copy-password") return copyText($("cw-upm-login-password")?.value, "Password");
          if (action === "copy") return copyText(actionNode.dataset.value, actionNode.dataset.label || "Text");
          if (action === "enable-auth") return enableAuth();
          if (action === "remove-login") return removeLogin(actionNode);
          if (action === "add-another") return addAnother();
          if (action === "view-as") return viewAs();
          return null;
        })
        .catch((e) => toast(String(e?.message || e || "Save failed"), false));
    });
    host.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" || !event.target?.matches?.("[data-upw-root] input.cw-upw-input")) return;
      event.preventDefault();
      const run = String(event.target.id || "").startsWith("cw-upm-admin-") ? enableAuth() : Promise.resolve(primaryAction());
      run.catch((e) => toast(String(e?.message || e || "Save failed"), false));
    });
    host.addEventListener("input", (event) => {
      if (!modalDraft || !event.target?.closest?.("[data-upw-root]")) return;
      if (event.target.id === "cw-upm-modal-label") {
        modalDraft.label = txt(event.target.value);
        patchPreview();
      }
      if (event.target.id === "cw-upm-login-password") {
        modalDraft.login.password = String(event.target.value || "");
        patchStrength();
      }
      if (event.target.id === "cw-upm-login-username") modalDraft.login.username = txt(event.target.value);
    });
    host.addEventListener("change", (event) => {
      if (!modalDraft || !event.target?.closest?.("[data-upw-root]")) return;
      if (event.target?.matches?.("input[data-upw-photo]")) return readPhoto(event.target);
      clearDeleteWarning();
      syncDraftFromDom();
      if (event.target?.matches?.(".cw-upw-tile input, input[data-resource-key], #cw-upm-login-enabled")) render();
    });
  }

  function init(force = false) {
    const host = $(HOST_ID);
    if (!host) return;
    attach(host);
    if (force || !loadedOnce) load(!!force);
  }

  w.cwUserProfilesJump = function cwUserProfilesJump() {
    try { w.cwSettingsSelect?.("providers"); } catch {}
    setTimeout(() => {
      init(true);
      $("cw-user-profile-manager")?.closest?.(".cw-auth-service-section")?.scrollIntoView({ behavior: "smooth", block: "start" });
    }, 0);
  };
  w.cwUserProfilesNew = async function cwUserProfilesNew() {
    try { w.cwSettingsSelect?.("providers"); } catch {}
    ensureFloatingHost();
    await load(true);
    openNewProfile();
  };

  w.cwUserProfilesOpenNew = async function cwUserProfilesOpenNew(opts) {
    ensureFloatingHost();
    await load(true);
    openNewProfile();
    if (opts?.login && modalDraft?.login) {
      modalDraft.login.wanted = true;
      render();
    }
  };

  w.cwUserProfilesOpen = async function cwUserProfilesOpen(id, opts) {
    await openProfile(id);
    const index = modalSteps().findIndex((step) => step.key === txt(opts?.step));
    if (index >= 0) {
      wizardStep = index;
      render();
    }
  };

  w.cwUserProfilesManager = { init, load };

  if (d.readyState === "loading") d.addEventListener("DOMContentLoaded", () => init(false), { once: true });
  else init(false);

  d.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && modalProfileId !== null && $(MODAL_HOST_ID)) {
      event.preventDefault();
      closeModal();
    }
  });
  d.addEventListener("cw-settings-pane-changed", (event) => {
    if (txt(event?.detail?.pane).toLowerCase() === "providers") init(false);
  });
  w.addEventListener("cw-user-profiles-changed", () => {
    if (!internalRefresh) load(true);
  });
})(window, document);
