/* assets/js/user-profiles.js */
/* CrossWatch - User profile management */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function (w, d) {
  if (w.cwUserProfilesManager) return;

  const HOST_ID = "cw-user-profile-manager";
  let providerInstances = {};
  let userProfiles = [];
  let appUsers = [];
  let loading = false;
  let loadedOnce = false;
  let internalRefresh = false;
  let modalProfileId = null;
  let modalDraft = null;
  let availableResources = { sync_pairs: [], watcher_routes: [], webhook_routes: [] };
  let floatingModal = false;
  let deleteConfirmTimer = 0;
  let openResourceSections = new Set();

  const $ = (id) => d.getElementById(id);
  const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m]));
  const txt = (v) => String(v ?? "").trim();

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
    if (src) {
      return `<span class="cw-upm-provider-mark"><img class="cw-upm-provider-logo" src="${esc(src)}" alt="" loading="lazy"></span>`;
    }
    return `<span class="cw-upm-provider-mark cw-upm-provider-fallback">${esc((key || "?").slice(0, 2))}</span>`;
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
      .filter((row) => row.id && !isDefaultInstanceId(row.id));
  }

  function allInstanceRows() {
    return providerKeys().flatMap((provider) => instanceRows(provider).map((row) => ({ provider, ...row })));
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

  function makeDraft(profile) {
    const manual = profile?.manual_instances && typeof profile.manual_instances === "object" ? profile.manual_instances : assignmentMap(profile);
    return {
      label: txt(profile?.label || ""),
      instances: JSON.parse(JSON.stringify(manual || {})),
      resources: resourceMap(profile),
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
    ["sync_pairs", "watcher_routes", "webhook_routes"].forEach((key) => {
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
      const first = Object.keys(rows || {}).map(txt).filter(Boolean)[0];
      if (first) out[providerKey(provider)] = [first];
    });
    return out;
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
    const pid = txt(profile?.id);
    const names = appUsers
      .filter((user) => !user?.protected && txt(user?.profile_id) === pid)
      .map((user) => txt(user?.username || user?.label || user?.id))
      .filter(Boolean);
    if (!names.length) return "Assigned to: None";
    const visible = names.slice(0, 3).join(", ");
    return `Assigned to: ${visible}${names.length > 3 ? ` +${names.length - 3} more` : ""}`;
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
    return `
      <button type="button" class="cw-auth-service-card cw-upm-profile-card" data-action="edit-profile" data-profile-id="${esc(profile.id)}">
        <span class="cw-auth-provider-mark cw-upm-card-icon"><span class="material-symbols-rounded cw-auth-provider-icon" aria-hidden="true">person</span></span>
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
          <strong>Add user profile</strong>
          <small>Assign configured provider instances.</small>
        </span>
        <span class="cw-upm-card-arrow material-symbols-rounded" aria-hidden="true">arrow_forward</span>
      </button>`;
  }

  function modalProfile() {
    return modalProfileId ? profileById(modalProfileId) : null;
  }

  function renderInstanceChecks() {
    const insts = draftInstances();
    const required = resourceRequiredMap();
    const keys = providerKeys();
    if (!keys.length) {
      return `<div class="cw-upm-empty">No named provider instances available. Default instances are reserved for the admin workspace here; create named instances such as PLEX-P01 for managed users.</div>`;
    }
    return `<div class="cw-upm-provider-grid">${keys.map((provider) => {
      const selected = new Set((insts[provider] || []).map(txt));
      const rows = instanceRows(provider);
      return `
        <div class="cw-upm-provider-group" data-provider="${esc(provider)}">
          <div class="cw-upm-provider-head">
            ${providerMark(provider)}
            <strong>${esc(providerLabel(provider))}</strong>
            <span>${rows.length}</span>
          </div>
          <div class="cw-upm-instance-grid">
            ${rows.map((row) => {
              const lockedBy = required[providerKey(provider)]?.[row.id] || [];
              const assignedProfile = instanceAssignedProfile(provider, row.id);
              const requiredProvider = !!required[providerKey(provider)];
              const locked = lockedBy.length > 0;
              const blocked = (requiredProvider && !locked) || !!assignedProfile;
              const note = assignedProfile ? `Assigned to: ${assignedProfile.label || assignedProfile.id}` : (locked ? `Required by ${Array.from(new Set(lockedBy)).join(", ")}` : "");
              return `
              <label class="cw-upm-instance-option ${selected.has(row.id) || locked ? "is-selected" : ""} ${locked ? "is-required" : ""} ${blocked ? "is-blocked" : ""}">
                <input type="checkbox" name="cw-upm-provider-${esc(provider)}" value="${esc(row.id)}" ${selected.has(row.id) || locked ? "checked" : ""} ${requiredProvider || assignedProfile ? "disabled" : ""} style="position:absolute;opacity:0;pointer-events:none">
                <span class="cw-upm-instance-copy">
                  <strong>${esc(row.label)}</strong>
                  <code>${esc(row.id)}</code>
                  ${note ? `<em>${esc(note)}</em>` : ""}
                </span>
              </label>`;
            }).join("")}
          </div>
        </div>`;
    }).join("")}</div>`;
  }

  function renderResourceSection(key) {
    const rows = resourceRows(key);
    const selected = draftResourceSet(key);
    const isOpen = openResourceSections.has(key);
    const assignedProfileLabel = (row) => {
      const assigned = txt(row?.assigned_profile_id);
      return assigned && assigned !== txt(modalProfileId) ? profileLabelForId(assigned) || "another profile" : "";
    };
    return `
      <details class="cw-upm-resource-section" data-resource-section="${esc(key)}" ${isOpen ? "open" : ""}>
        <summary>
          <span>${esc(resourceTitle(key))}</span>
          <strong>${rows.filter((row) => selected.has(txt(row.id))).length}/${rows.length}</strong>
        </summary>
        <div class="cw-upm-resource-grid">
          ${rows.length ? rows.map((row) => {
            const lockedLabel = assignedProfileLabel(row);
            const disabled = !!lockedLabel;
            const isSelected = selected.has(txt(row.id));
            const features = key === "sync_pairs" && Array.isArray(row.features) ? row.features.map(txt).filter(Boolean) : [];
            return `
              <label class="cw-upm-resource-option ${isSelected ? "is-selected" : ""} ${disabled ? "is-disabled" : ""}" data-resource-option>
                <input type="checkbox" data-resource-key="${esc(key)}" value="${esc(row.id)}" ${isSelected ? "checked" : ""} ${disabled ? "disabled" : ""} style="position:absolute;opacity:0;pointer-events:none">
                <span class="cw-upm-resource-copy">
                  <strong>${esc(row.label || row.id)}</strong>
                  <small>${disabled ? `Assigned to: ${esc(lockedLabel)}` : `${(Array.isArray(row.instances) ? row.instances.length : 0)} required instance${(Array.isArray(row.instances) ? row.instances.length : 0) === 1 ? "" : "s"}`}</small>
                  ${features.length ? `<span class="cw-upm-resource-features">${features.map((feature) => `<em>${esc(feature)}</em>`).join("")}</span>` : ""}
                </span>
              </label>`;
          }).join("") : `<div class="cw-upm-empty">No ${esc(resourceTitle(key).toLowerCase())} available for managed users. Resources using default instances are hidden here; create resources with named instances first.</div>`}
        </div>
      </details>`;
  }

  function renderModal() {
    if (modalProfileId === null) return "";
    const profile = modalProfile();
    const editing = !!profile;
    const title = editing ? "Edit user profile" : "Add user profile";
    const currentLabel = modalDraft?.label ?? profile?.label ?? "";
    return `
      <div class="cw-upm-overlay" data-action="close-modal">
        <div class="cw-upm-dialog" role="dialog" aria-modal="true" aria-labelledby="cw-upm-dialog-title" data-modal-root>
          <div class="cw-upm-dialog-head">
            <span class="cw-auth-provider-mark cw-upm-dialog-icon" aria-hidden="true"><span class="material-symbols-rounded cw-auth-provider-icon">person</span></span>
            <div>
              <div class="cw-upm-dialog-kicker">User profiles</div>
              <h3 id="cw-upm-dialog-title">${esc(title)}</h3>
              <p>Assign only configured provider instances to this person.</p>
            </div>
            <button type="button" class="cw-upm-icon-btn" data-action="close-modal" title="Close" aria-label="Close"><span class="material-symbols-rounded" aria-hidden="true">close</span></button>
          </div>
          <div class="cw-upm-dialog-body">
            <label class="cw-upm-field">
              <span>Name</span>
              <input class="input" id="cw-upm-modal-label" type="text" maxlength="40" value="${esc(currentLabel)}" placeholder="Profile name">
            </label>
            <section class="cw-upm-modal-section">
              <div class="cw-upm-modal-section-head">
                <strong>Assigned instances</strong>
                <span>${allInstanceRows().length} configured instance${allInstanceRows().length === 1 ? "" : "s"}</span>
              </div>
              ${renderInstanceChecks()}
            </section>
            <section class="cw-upm-modal-section">
              <div class="cw-upm-modal-section-head">
                <strong>Assigned resources</strong>
                <span>Auto-lock required instances</span>
              </div>
              <div class="cw-upm-resource-stack">
                ${renderResourceSection("sync_pairs")}
                ${renderResourceSection("watcher_routes")}
                ${renderResourceSection("webhook_routes")}
              </div>
            </section>
          </div>
          <div class="cw-upm-dialog-foot">
            ${editing ? '<button type="button" class="cw-upm-btn btn danger cw-connection-footer-delete cw-danger-confirm" data-action="delete-profile"><span class="material-symbols-rounded" aria-hidden="true">delete</span><span>Delete profile</span></button><span class="cw-connection-footer-warn hidden" role="alert" aria-live="polite"></span>' : '<span></span><span></span>'}
            <span class="cw-upm-dialog-spacer"></span>
            <button type="button" class="cw-upm-btn btn primary cw-connection-footer-save" data-action="save-profile">Save profile</button>
          </div>
        </div>
      </div>`;
  }

  function render() {
    const host = (floatingModal ? $("cw-upm-floating-host") : null) || $(HOST_ID);
    if (!host) return;
    if (floatingModal) {
      host.innerHTML = renderModal();
      return;
    }
    host.innerHTML = `
      <div class="cw-auth-service-grid cw-upm-service-grid">
        ${userProfiles.map(renderProfileCard).join("")}
        ${renderAddCard()}
      </div>
      <div class="cw-upm-status" id="cw-upm-status"></div>
      ${renderModal()}`;
  }

  async function load(force = false) {
    const host = (floatingModal ? $("cw-upm-floating-host") : null) || $(HOST_ID);
    if (!host || (loading && !force)) return;
    loading = true;
    render();
    try {
      const [instances, profiles] = await Promise.all([
        json(`/api/provider-instances?configured_only=true&ts=${Date.now()}`),
        json(`/api/user-profiles?ts=${Date.now()}`),
      ]);
      providerInstances = instances && typeof instances === "object" ? instances : {};
      availableResources = profiles?.resources && typeof profiles.resources === "object" ? profiles.resources : { sync_pairs: [], watcher_routes: [], webhook_routes: [] };
      userProfiles = Array.isArray(profiles?.items) ? profiles.items.map((row) => ({
        id: txt(row?.id),
        label: txt(row?.label || row?.id),
        instances: row?.instances && typeof row.instances === "object" ? row.instances : {},
        manual_instances: row?.manual_instances && typeof row.manual_instances === "object" ? row.manual_instances : {},
        resources: row?.resources && typeof row.resources === "object" ? row.resources : {},
        resource_counts: row?.resource_counts && typeof row.resource_counts === "object" ? row.resource_counts : {},
      })).filter((row) => row.id && row.label) : [];
      try {
        const userData = await json(`/api/app-auth/users?ts=${Date.now()}`);
        appUsers = Array.isArray(userData?.items) ? userData.items : [];
      } catch {
        appUsers = [];
      }
      loadedOnce = true;
    } catch (e) {
      const status = $("cw-upm-status");
      if (status) status.textContent = String(e?.message || e || "Failed to load user profiles");
      toast("Could not load user profiles: " + String(e?.message || e), false);
    } finally {
      loading = false;
      render();
    }
  }

  async function afterSave(message) {
    try { w.invalidateConfigCache?.(); } catch {}
    await load(true);
    internalRefresh = true;
    try { w.dispatchEvent(new CustomEvent("cw-user-profiles-changed")); } catch {}
    try { w.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    internalRefresh = false;
    toast(message, true);
    if (floatingModal) closeModal();
  }

  function openProfile(id) {
    modalProfileId = txt(id);
    modalDraft = makeDraft(modalProfile());
    openResourceSections = new Set();
    render();
    setTimeout(() => $("cw-upm-modal-label")?.focus(), 0);
  }

  function openNewProfile(opts) {
    floatingModal = !!opts?.floating;
    modalProfileId = "";
    modalDraft = makeDraft(null);
    openResourceSections = new Set();
    render();
    setTimeout(() => $("cw-upm-modal-label")?.focus(), 0);
  }

  function closeModal() {
    resetDeleteConfirm();
    clearDeleteWarning();
    modalProfileId = null;
    modalDraft = null;
    openResourceSections = new Set();
    if (floatingModal) {
      $("cw-upm-floating-host")?.remove();
      floatingModal = false;
      return;
    }
    render();
  }

  function collectModalInstances(root) {
    const instances = {};
    root.querySelectorAll(".cw-upm-provider-group[data-provider]").forEach((group) => {
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

  function collectOpenResourceSections(root) {
    openResourceSections = new Set(
      Array.from(root.querySelectorAll(".cw-upm-resource-section[data-resource-section][open]"))
        .map((node) => txt(node.dataset.resourceSection))
        .filter(Boolean)
    );
  }

  function setDraftResource(key, value, enabled) {
    if (!modalDraft) return;
    const cleanKey = txt(key);
    const cleanValue = txt(value);
    if (!cleanValue || !["sync_pairs", "watcher_routes", "webhook_routes"].includes(cleanKey)) return;
    if (!modalDraft.resources || typeof modalDraft.resources !== "object") modalDraft.resources = { sync_pairs: [], watcher_routes: [], webhook_routes: [] };
    const next = draftResourceSet(cleanKey);
    if (enabled) next.add(cleanValue);
    else next.delete(cleanValue);
    modalDraft.resources[cleanKey] = Array.from(next);
  }

  function syncDraftFromDom() {
    const root = d.querySelector(".cw-upm-dialog");
    if (!root || !modalDraft) return;
    collectOpenResourceSections(root);
    const previous = modalDraft.instances || {};
    modalDraft.label = txt($("cw-upm-modal-label")?.value);
    modalDraft.resources = collectModalResources(root);
    const manual = collectModalInstances(root);
    const required = resourceRequiredMap();
    Object.entries(required).forEach(([provider, rows]) => {
      const requiredInstance = Object.keys(rows || {}).map(txt).filter(Boolean)[0];
      const previousInstance = (Array.isArray(previous[provider]) ? previous[provider] : [previous[provider]]).map(txt).filter(Boolean)[0];
      const currentInstance = (Array.isArray(manual[provider]) ? manual[provider] : [manual[provider]]).map(txt).filter(Boolean)[0];
      if (requiredInstance && (previousInstance === requiredInstance || currentInstance === requiredInstance)) manual[provider] = [requiredInstance];
      else delete manual[provider];
    });
    modalDraft.instances = manual;
  }

  async function saveProfile() {
    const root = d.querySelector(".cw-upm-dialog");
    if (!root) return;
    clearDeleteWarning();
    syncDraftFromDom();
    const label = txt(modalDraft?.label);
    if (!label) {
      toast("Enter a user profile name", false);
      $("cw-upm-modal-label")?.focus();
      return;
    }
    const instances = modalDraft?.instances || {};
    const resources = modalDraft?.resources || { sync_pairs: [], watcher_routes: [], webhook_routes: [] };
    const profile = modalProfile();
    if (profile) {
      await json(`/api/user-profiles/${encodeURIComponent(profile.id)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ label, instances, resources }),
      });
      modalProfileId = null;
      modalDraft = null;
      await afterSave("User profile saved");
      return;
    }
    await json("/api/user-profiles", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ label, instances, resources }),
    });
    modalProfileId = null;
    modalDraft = null;
    await afterSave("User profile created");
  }

  function resetDeleteConfirm(btn) {
    if (deleteConfirmTimer) clearTimeout(deleteConfirmTimer);
    deleteConfirmTimer = 0;
    const target = btn || d.querySelector('.cw-upm-btn[data-action="delete-profile"]');
    if (!target) return;
    target.dataset.cwConfirmDelete = "";
    target.classList.remove("is-confirming");
    target.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">delete</span><span>Delete profile</span>`;
  }

  function armDeleteConfirm(btn) {
    if (!btn) return;
    btn.dataset.cwConfirmDelete = "1";
    btn.classList.add("is-confirming");
    btn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">warning</span><span>Confirm delete</span>`;
    if (deleteConfirmTimer) clearTimeout(deleteConfirmTimer);
    deleteConfirmTimer = setTimeout(() => resetDeleteConfirm(btn), 4200);
  }

  function clearDeleteWarning() {
    const node = d.querySelector(".cw-upm-dialog .cw-connection-footer-warn");
    if (!node) return;
    clearTimeout(node.__cwWarnFade);
    clearTimeout(node.__cwWarnHide);
    node.__cwWarnFade = 0;
    node.__cwWarnHide = 0;
    node.classList.add("hidden");
    node.classList.remove("is-fading");
    node.textContent = "";
  }

  function showDeleteWarning(message) {
    const text = txt(message) || "Could not delete user profile";
    const node = d.querySelector(".cw-upm-dialog .cw-connection-footer-warn");
    if (!node) {
      toast(text, false);
      return;
    }
    clearDeleteWarning();
    node.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">warning</span><span class="cw-connection-footer-warn-text"></span>`;
    node.lastElementChild.textContent = text;
    node.classList.remove("hidden", "is-fading");
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
      await json(`/api/user-profiles/${encodeURIComponent(profile.id)}`, { method: "DELETE" });
      modalProfileId = null;
      modalDraft = null;
      await afterSave("User profile deleted");
    } catch (e) {
      showDeleteWarning(String(e?.message || e || "Could not delete user profile"));
    }
  }

  function attach(host) {
    if (!host || host.__cwUserProfilesBound) return;
    host.__cwUserProfilesBound = true;
    host.addEventListener("click", (event) => {
      const resourceOption = event.target?.closest?.(".cw-upm-resource-option[data-resource-option]");
      if (resourceOption && modalDraft && resourceOption.closest(".cw-upm-dialog")) {
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
      if (!action) return;
      if (action === "close-modal" && event.target.closest("[data-modal-root]") && !event.target.closest("button")) return;
      Promise.resolve()
        .then(() => {
          if (action === "refresh") return load(true);
          if (action === "new-profile") return openNewProfile();
          if (action === "edit-profile") return openProfile(actionNode.dataset.profileId);
          if (action === "close-modal") return closeModal();
          if (action === "save-profile") return saveProfile();
          if (action === "delete-profile") return deleteProfile(actionNode);
          return null;
        })
        .catch((e) => toast(String(e?.message || e || "Save failed"), false));
    });
    host.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && modalProfileId !== null) {
        event.preventDefault();
        closeModal();
      }
      if (event.key === "Enter" && event.target?.id === "cw-upm-modal-label") {
        event.preventDefault();
        saveProfile().catch((e) => toast(String(e?.message || e || "Save failed"), false));
      }
    });
    host.addEventListener("change", (event) => {
      if (!modalDraft || !event.target?.closest?.(".cw-upm-dialog")) return;
      clearDeleteWarning();
      syncDraftFromDom();
      if (event.target?.matches?.("input[data-resource-key]")) render();
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
  w.cwUserProfilesNew = function cwUserProfilesNew() {
    try { w.cwSettingsSelect?.("providers"); } catch {}
    setTimeout(() => {
      init(true);
      $("cw-user-profile-manager")?.closest?.(".cw-auth-service-section")?.scrollIntoView({ behavior: "smooth", block: "start" });
      openNewProfile();
    }, 0);
  };

  w.cwUserProfilesOpenNew = async function cwUserProfilesOpenNew() {
    let host = $("cw-upm-floating-host");
    if (!host) {
      host = d.createElement("div");
      host.id = "cw-upm-floating-host";
      host.className = "cw-user-profile-manager cw-upm-floating-host";
      d.body.appendChild(host);
      attach(host);
    }
    floatingModal = true;
    await load(true);
    openNewProfile({ floating: true });
  };

  w.cwUserProfilesManager = { init, load };

  if (d.readyState === "loading") d.addEventListener("DOMContentLoaded", () => init(false), { once: true });
  else init(false);

  d.addEventListener("cw-settings-pane-changed", (event) => {
    if (txt(event?.detail?.pane).toLowerCase() === "providers") init(false);
  });
  w.addEventListener("cw-user-profiles-changed", () => {
    if (!internalRefresh) load(true);
  });
})(window, document);
