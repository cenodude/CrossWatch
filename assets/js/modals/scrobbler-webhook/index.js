/* CrossWatch - Scrobbler Webhook Modal */
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const label = (v) => ({ plex: "Plex", jellyfin: "Jellyfin", emby: "Emby", trakt: "Trakt", simkl: "SIMKL", mdblist: "MDBList" }[String(v || "").toLowerCase()] || String(v || "").toUpperCase());
const sinks = ["trakt", "simkl", "mdblist"];

function flashCopied(btn) {
  if (!btn) return;
  if (btn.__cwCopyTimer) clearTimeout(btn.__cwCopyTimer);
  if (!btn.dataset.copyIcon) btn.dataset.copyIcon = (btn.textContent || "content_copy").trim();
  btn.textContent = "check";
  btn.classList.add("is-copied");
  btn.__cwCopyTimer = setTimeout(() => {
    btn.textContent = btn.dataset.copyIcon || "content_copy";
    btn.classList.remove("is-copied");
    delete btn.dataset.copyIcon;
    btn.__cwCopyTimer = 0;
  }, 1400);
}

let root = null;
let props = {};
let saving = false;
let destructive = "";
let dangerTimer = 0;
let originalSink = "";
let activeTab = "source";
let modalKey = "";
const lastTab = {};
let boundRoot = null;
let clickHandler = null;
let changeHandler = null;

function detachHandlers() {
  if (boundRoot?.__cwScrobblerWebhookAbort) {
    try { boundRoot.__cwScrobblerWebhookAbort.abort(); } catch {}
    delete boundRoot.__cwScrobblerWebhookAbort;
  }
  if (boundRoot && clickHandler) boundRoot.removeEventListener("click", clickHandler);
  if (boundRoot && changeHandler) boundRoot.removeEventListener("change", changeHandler);
  boundRoot = null;
  clickHandler = null;
  changeHandler = null;
}

async function api(url, body) {
  const res = await fetch(url, {
    method: "POST",
    credentials: "same-origin",
    cache: "no-store",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data?.ok === false) {
    const err = new Error(data?.error || `HTTP ${res.status}`);
    err.payload = data;
    throw err;
  }
  return data;
}

function allProfiles() {
  return (props.overview?.eligible_sources || []).flatMap((group) => (group.profiles || []).filter((x) => x.eligible).map((x) => ({ ...x, provider: x.provider || group.provider })));
}

function sinkProfiles(sink) {
  const group = (props.overview?.destination_availability || []).find((x) => x.provider === sink);
  return (group?.profiles || []).filter((x) => x.configured);
}

function selectedWebhook() {
  if (props.webhook) return props.webhook;
  const first = allProfiles()[0] || { provider: "plex", instance: "default" };
  return {
    provider: first.provider,
    provider_instance: first.instance,
    enabled: true,
    endpoint_url: "",
    sink: "",
    sink_instance: "",
    effective_settings: {},
    explicit_settings: {},
  };
}

function explicitHas(key) {
  return Object.prototype.hasOwnProperty.call(selectedWebhook().explicit_settings || {}, key);
}

function filterKey(provider) {
  return provider === "plex" ? "filters_plex" : provider === "jellyfin" ? "filters_jellyfin" : "filters_emby";
}

function valuesText(values) {
  return (Array.isArray(values) ? values : []).join("\n");
}

function splitValues(text) {
  return String(text || "").split(/[\n,]/).map((x) => x.trim()).filter(Boolean);
}

function appendListValue(id, value) {
  const el = root?.querySelector(`#${id}`);
  const next = String(value || "").trim();
  if (!el || !next) return;
  const values = splitValues(el.value);
  if (!values.some((x) => x.toLowerCase() === next.toLowerCase())) values.push(next);
  el.value = values.join("\n");
}

function currentSource() {
  const current = selectedWebhook();
  if (props.mode === "edit") {
    return {
      provider: String(current.provider || ""),
      instance: String(current.provider_instance || "default"),
    };
  }
  return {
    provider: String(root?.querySelector("#scw-source-provider")?.value || current.provider || ""),
    instance: String(root?.querySelector("#scw-source-instance")?.value || current.provider_instance || "default"),
  };
}

async function pickUser(btn, keepTab = currentActiveTab()) {
  activeTab = normalizeActiveTab(keepTab);
  if (!window.cwMediaUserPicker?.open) return;
  const source = currentSource();
  if (!source.provider) return;
  window.cwMediaUserPicker.open({
    provider: source.provider,
    instance: source.instance,
    anchorEl: btn,
    title: `Pick ${label(source.provider)} user`,
    overlay: false,
    onPick: (user) => appendListValue("scw-users", user.name || user.id),
  });
  preserveVisiblePanel(keepTab);
}

async function fetchServerUuid(targetId, btn, keepTab = currentActiveTab()) {
  activeTab = normalizeActiveTab(keepTab);
  const source = currentSource();
  if (source.provider !== "plex" || saving) return;
  const labelEl = btn?.querySelector("span:not(.material-symbols-rounded)");
  const old = labelEl?.textContent;
  if (btn) {
    btn.disabled = true;
    if (labelEl) labelEl.textContent = "Fetching";
  }
  try {
    const res = await fetch(`/api/plex/server_uuid?instance=${encodeURIComponent(source.instance)}`, { cache: "no-store", credentials: "same-origin" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data?.ok === false) throw new Error(data?.error || data?.message || `HTTP ${res.status}`);
    appendListValue(targetId, data.server_uuid);
    preserveVisiblePanel(keepTab);
  } catch (err) {
    render([{ message: err.message || "Could not fetch Plex server UUID." }]);
    preserveVisiblePanel(keepTab);
  } finally {
    if (btn && root?.contains(btn)) {
      btn.disabled = false;
      if (labelEl) labelEl.textContent = old || "Fetch";
    }
  }
}

function settings() {
  return selectedWebhook().effective_settings || {};
}

function sourceProviders() {
  const out = [];
  for (const p of allProfiles()) if (!out.includes(p.provider)) out.push(p.provider);
  return out;
}

function sourceProfiles(provider) {
  return allProfiles().filter((p) => p.provider === provider);
}

function sourceProviderSelect(current) {
  const provs = sourceProviders();
  const dis = props.mode === "edit" ? "disabled" : "";
  if (!provs.length) return `<select class="input" id="scw-source-provider" disabled><option value="">No configured media profile</option></select>`;
  return `<select class="input" id="scw-source-provider" ${dis}>${provs.map((p) => `<option value="${esc(p)}" ${p === current.provider ? "selected" : ""}>${esc(label(p))}</option>`).join("")}</select>`;
}

function sourceProfileSelect(provider, currentInst) {
  const list = sourceProfiles(provider);
  const dis = props.mode === "edit" ? "disabled" : "";
  const cur = String(currentInst || "default");
  if (!list.length) return `<select class="input" id="scw-source-instance" ${dis}><option value="default">Default</option></select>`;
  return `<select class="input" id="scw-source-instance" ${dis}>${list.map((p) => `<option value="${esc(p.instance)}" ${p.instance === cur ? "selected" : ""}>${esc(profileName(p.instance))}</option>`).join("")}</select>`;
}

function profileLabel(provider, instance) {
  const p = allProfiles().find((x) => x.provider === provider && x.instance === instance);
  return profileName(p?.instance || instance);
}

function profileName(instance) {
  const value = String(instance || "").trim();
  if (!value || value === "default") return "Default";
  return value;
}

function normInst(v) {
  return String(v == null ? "default" : v).trim().toLowerCase() || "default";
}

function duplicateWebhook(current) {
  const p = String(current.provider || "").toLowerCase();
  const i = normInst(current.provider_instance);
  const s = String(selectedSinkKey() || "").toLowerCase();
  const orig = String(originalSink || "").toLowerCase();
  return (props.overview?.webhooks || []).some((x) =>
    String(x.provider || "").toLowerCase() === p &&
    normInst(x.provider_instance) === i &&
    String(x.sink || "").toLowerCase() === s &&
    !(props.mode === "edit" && String(x.sink || "").toLowerCase() === orig)
  );
}

function logo(provider) {
  return ({
    plex: "/assets/img/PLEX.svg",
    jellyfin: "/assets/img/JELLYFIN.svg",
    emby: "/assets/img/EMBY.svg",
    trakt: "/assets/img/TRAKT.svg",
    simkl: "/assets/img/SIMKL.svg",
    mdblist: "/assets/img/MDBLIST.svg",
  }[String(provider || "").toLowerCase()] || "");
}

function providerIcon(provider) {
  const src = logo(provider);
  return src ? `<img src="${esc(src)}" alt="">` : `<span>${esc(label(provider).slice(0, 1))}</span>`;
}

function providerClass(provider) {
  return `provider-${esc(String(provider || "").toLowerCase())}`;
}

function modalTitle() {
  return props.mode === "create" ? "Add webhook" : "Edit webhook";
}

function navButton(key, icon, title, copy) {
  return `<button type="button" class="cw-subtile scrm-nav-item ${activeTab === key ? "active" : ""}" data-tab="${esc(key)}" aria-selected="${activeTab === key ? "true" : "false"}"><span class="material-symbols-rounded cw-connection-nav-icon">${esc(icon)}</span><span class="cw-connection-nav-copy"><strong>${esc(title)}</strong><small>${esc(copy)}</small></span><span class="material-symbols-rounded cw-connection-nav-chev">chevron_right</span></button>`;
}

function field(labelText, html, extra = "") {
  return `<label class="scrm-field ${extra}"><span>${esc(labelText)}</span>${html}</label>`;
}

function sinksUsedBySource() {
  const cur = selectedWebhook();
  const p = String(cur.provider || "").toLowerCase();
  const i = normInst(cur.provider_instance);
  return new Set((props.overview?.webhooks || [])
    .filter((w) => String(w.provider || "").toLowerCase() === p && normInst(w.provider_instance) === i && normInst(w.sink) !== normInst(originalSink))
    .map((w) => String(w.sink || "").toLowerCase()));
}

function selectedSinkKey() {
  const cur = selectedWebhook();
  if (cur.sink) return String(cur.sink).toLowerCase();
  const used = sinksUsedBySource();
  return sinks.find((s) => sinkProfiles(s).length && !used.has(s)) || sinks.find((s) => sinkProfiles(s).length) || "trakt";
}

function selectedSinkInstance(sink) {
  const cur = selectedWebhook();
  if (String(cur.sink || "").toLowerCase() === sink && cur.sink_instance) return String(cur.sink_instance);
  return String(sinkProfiles(sink)[0]?.instance || "default");
}

function sinkSelect(current) {
  return sinks.map((s) => {
    const ok = sinkProfiles(s).length > 0;
    return `<option value="${esc(s)}" ${s === current ? "selected" : ""} ${ok ? "" : "disabled"}>${esc(label(s))}${ok ? "" : " (not connected)"}</option>`;
  }).join("");
}

function sinkProfileSelect(sink, currentInst) {
  const profiles = sinkProfiles(sink);
  if (!profiles.length) return `<option value="default">Not connected</option>`;
  const cur = String(currentInst || "default");
  return profiles.map((p) => `<option value="${esc(p.instance)}" ${p.instance === cur ? "selected" : ""}>${esc(profileName(p.instance))}</option>`).join("");
}

function endpointBlock(current) {
  return `
    <div class="scrm-provider-card scwm-endpoint-card">
      <div class="scrm-card-head"><span class="scrm-provider-mark"><span class="material-symbols-rounded">link</span></span><div><strong>Endpoint URL</strong><small>Use this URL in ${esc(label(current.provider))}</small></div></div>
      <div class="scrm-endpoint scwm-endpoint">
        <code title="${esc(current.endpoint_url || "")}">${esc(current.endpoint_url || "Saved profile will receive a URL")}</code>
        <button type="button" class="btn small sc2-icon-btn material-symbols-rounded" data-copy="${esc(current.endpoint_url || "")}" title="Copy URL">content_copy</button>
        <button type="button" class="btn small danger sc2-icon-btn cw-danger-confirm" data-regenerate title="Regeneration is destructive for the old endpoint URL."><span class="material-symbols-rounded">sync_lock</span></button>
      </div>
    </div>
  `;
}

function journeyHelp(key) {
  const url = window.CW?.HelpLinks?.url?.(key) || "";
  if (!url) return "";
  return `<a class="scrm-journey-help" href="${esc(url)}" target="_blank" rel="noopener noreferrer" aria-label="Open guide" title="Open guide"><span class="material-symbols-rounded" aria-hidden="true">help</span></a>`;
}

function fieldIcon(icon, labelText, html) {
  return `<label class="scrm-field scrm-field-row" title="${esc(labelText)}"><span class="material-symbols-rounded scrm-field-ico" aria-hidden="true">${esc(icon)}</span>${html}</label>`;
}

function sourcePanel(current) {
  const sink = selectedSinkKey();
  const sinkInst = selectedSinkInstance(sink);
  return `
    <section class="scrm-panel ${activeTab === "source" ? "active" : ""}" data-panel="source">
      <div class="scrm-journey scrm-journey-compact">
        <span class="material-symbols-rounded scrm-journey-icon">webhook</span>
        <div><strong>Attach a webhook to a media profile</strong><p>Forward inbound Plex, Jellyfin, or Emby webhook events to one tracker.</p></div>
        ${journeyHelp("scrobbler-webhooks")}
      </div>
      <div class="scrm-route-grid">
        <div class="scrm-provider-card ${providerClass(current.provider)}">
          <div class="scrm-card-head"><span class="scrm-provider-mark">${providerIcon(current.provider)}</span><div><strong>Source</strong><small>${esc(label(current.provider))}</small></div></div>
          <div class="scrm-fields">
            ${fieldIcon("dns", "Media server", sourceProviderSelect(current))}
            ${fieldIcon("person", "Profile", sourceProfileSelect(current.provider, current.provider_instance))}
          </div>
        </div>
        <div class="scrm-route-arrow"><span class="material-symbols-rounded">arrow_forward</span></div>
        <div class="scrm-provider-card ${providerClass(sink)}">
          <div class="scrm-card-head"><span class="scrm-provider-mark">${providerIcon(sink)}</span><div><strong>Destination</strong><small>${esc(label(sink))}</small></div></div>
          <div class="scrm-fields">
            ${fieldIcon("gps_fixed", "Tracker", `<select class="input" id="scw-sink">${sinkSelect(sink)}</select>`)}
            ${fieldIcon("person", "Profile", `<select class="input" id="scw-sink-instance">${sinkProfileSelect(sink, sinkInst)}</select>`)}
          </div>
        </div>
      </div>
      ${endpointBlock(current)}
    </section>
  `;
}

function addFilterEntry(id, addLabel) {
  const ta = root?.querySelector(`#${id}`);
  if (!ta) return;
  const val = (window.prompt(`Add ${addLabel}`) || "").trim();
  if (val) {
    const lines = ta.value.split("\n").map((s) => s.trim()).filter(Boolean);
    if (!lines.some((l) => l.toLowerCase() === val.toLowerCase())) lines.push(val);
    ta.value = lines.join("\n");
  }
  ta.focus();
}

function filterRow(o) {
  const dot = o.dot ? `<i class="scrm-dot ${o.dot}"></i>` : "";
  const addLabel = o.addLabel || "entry";
  const action = o.actionAttr
    ? `<button type="button" class="scwm-filter-icon material-symbols-rounded" ${o.actionAttr} title="${esc(o.actionTitle)}" aria-label="${esc(o.actionTitle)}">${o.actionIcon}</button>`
    : "";
  return `
    <div class="scrm-filter-row">
      <div class="scrm-filter-row-copy"><strong>${dot}${esc(o.title)}</strong><small>${esc(o.subtitle)}</small></div>
      <textarea class="input scrm-filter-text" id="${o.id}" placeholder="${esc(o.placeholder)}">${esc(o.value)}</textarea>
      <div class="scrm-filter-tools">
        <button type="button" class="scwm-filter-icon material-symbols-rounded" data-add-filter="${o.id}" data-add-label="${esc(addLabel)}" title="Add ${esc(addLabel)}" aria-label="Add ${esc(addLabel)}">add</button>
        ${action}
        <button type="button" class="scwm-filter-icon material-symbols-rounded" data-clear-filter="${o.id}" title="Clear" aria-label="Clear">close</button>
      </div>
    </div>`;
}

function filtersPanel(provider, filt) {
  const isPlex = provider === "plex";
  const rows = [filterRow({ title: "Username whitelist", subtitle: "Only forward events from these usernames.", id: "scw-users", value: valuesText(filt.username_whitelist), placeholder: "One username per line", addLabel: "username", actionAttr: "data-pick-user", actionIcon: "person_search", actionTitle: "Find users" })];
  if (isPlex) {
    rows.push(filterRow({ title: "Server UUID allowlist", subtitle: "Control which servers are allowed.", id: "scw-allow", value: valuesText(filt.server_uuid_whitelist || (filt.server_uuid ? [filt.server_uuid] : [])), placeholder: "One server UUID per line", dot: "scrm-dot-allow", addLabel: "server UUID", actionAttr: `data-fetch-uuid="scw-allow"`, actionIcon: "search", actionTitle: "Fetch server UUID" }));
    rows.push(filterRow({ title: "Server UUID blocklist", subtitle: "Control which servers are blocked.", id: "scw-block", value: valuesText(filt.server_uuid_blacklist), placeholder: "One server UUID per line", dot: "scrm-dot-block", addLabel: "server UUID", actionAttr: `data-fetch-uuid="scw-block"`, actionIcon: "search", actionTitle: "Fetch server UUID" }));
  }
  return `
    <section class="scrm-panel ${activeTab === "filters" ? "active" : ""}" data-panel="filters">
      <div class="scrm-journey scrm-journey-compact">
        <span class="material-symbols-rounded scrm-journey-icon">filter_alt</span>
        <div><strong>Filter inbound webhook events</strong><p>Only forward events that match these filters. Leave empty to accept everything from this source.</p></div>
        ${journeyHelp("scrobbler-filters")}
      </div>
      <div class="scrm-filter-rows">${rows.join("")}</div>
    </section>
  `;
}

function globalRatingTargets() {
  const g = props.overview?.source_state?.global_plex_ratings || {};
  return ["trakt", "simkl", "mdblist"].filter((s) => g[s]);
}

function ratingsPanel(provider, ratingsTargets) {
  if (provider !== "plex") return "";
  const globalTargets = globalRatingTargets();
  const globalWarn = globalTargets.length
    ? `<div class="scrm-note is-warn"><span class="material-symbols-rounded">warning</span><span>Global Plex ratings is on and already forwarding to <strong>${esc(globalTargets.map(label).join(", "))}</strong>. This webhook sends ratings <em>in addition</em> to the global one — only enable trackers here if this profile needs different destinations.</span></div>`
    : "";
  return `
    <section class="scrm-panel ${activeTab === "ratings" ? "active" : ""}" data-panel="ratings">
      <div class="scrm-journey scrm-journey-compact">
        <span class="material-symbols-rounded scrm-journey-icon">star</span>
        <div><strong>Plex ratings</strong><p>Forward Plex ratings received on this webhook to the selected trackers.</p></div>
      </div>
      ${globalWarn}
      <div class="scrm-targets">${sinks.map((sink) => `<label class="scrm-target"><input type="checkbox" data-rating="${esc(sink)}" ${ratingsTargets.includes(sink) ? "checked" : ""}><span class="scrm-target-mark">${providerIcon(sink)}</span><span>${esc(label(sink))}</span></label>`).join("")}</div>
    </section>
  `;
}

function optionsPanel() {
  return `
    <section class="scrm-panel ${activeTab === "options" ? "active" : ""}" data-panel="options">
      <div class="scrm-journey scrm-journey-compact">
        <span class="material-symbols-rounded scrm-journey-icon">tune</span>
        <div><strong>Profile webhook options</strong><p>These values remain inherited unless you enable this override for the profile.</p></div>
      </div>
      <label class="scrm-check"><input type="checkbox" id="scw-override-options" ${["pause_debounce_seconds", "suppress_start_at"].some(explicitHas) ? "checked" : ""}> <span>Override profile options</span></label>
      <div class="scrm-fields two">
        ${field("Pause debounce", `<input class="input" id="scw-pause" type="number" min="0" max="3600" value="${esc(settings().pause_debounce_seconds ?? "")}" placeholder="Inherited">`)}
        ${field("Suppress start", `<input class="input" id="scw-suppress" type="number" min="0" max="3600" value="${esc(settings().suppress_start_at ?? "")}" placeholder="Inherited">`)}
      </div>
    </section>
  `;
}

function switchTab(tab) {
  activeTab = normalizeActiveTab(tab || "source");
  ensureVisiblePanel();
}

function normalizeActiveTab(tab = activeTab) {
  const next = String(tab || "source");
  return ["source", "filters", "ratings", "options"].includes(next) ? next : "source";
}

function currentActiveTab() {
  return normalizeActiveTab(
    root?.querySelector(".scrm-nav-item.active")?.dataset.tab ||
    root?.querySelector(".scrm-panel.active")?.dataset.panel ||
    activeTab
  );
}

function ensureVisiblePanel(tab = activeTab) {
  activeTab = normalizeActiveTab(tab);
  if (modalKey) lastTab[modalKey] = activeTab;
  if (root) root.dataset.scrmTab = activeTab;
  root?.querySelectorAll("[data-tab]").forEach((n) => {
    const on = n.dataset.tab === activeTab;
    n.classList.toggle("active", on);
    n.setAttribute("aria-selected", on ? "true" : "false");
  });
  root?.querySelectorAll(".scrm-panel[data-panel]").forEach((n) => {
    const on = n.dataset.panel === activeTab;
    n.classList.toggle("active", on);
    n.style.display = on ? "grid" : "none";
  });
}

function preserveVisiblePanel(tab) {
  const keep = normalizeActiveTab(tab);
  ensureVisiblePanel(keep);
  requestAnimationFrame(() => ensureVisiblePanel(keep));
  setTimeout(() => ensureVisiblePanel(keep), 0);
}

function render(errs = []) {
  activeTab = normalizeActiveTab(root?.dataset.scrmTab || activeTab);
  const current = selectedWebhook();
  const provider = current.provider;
  const fKey = filterKey(provider);
  const effective = settings();
  const filt = effective[fKey] || {};
  const ratingsTargets = ["trakt", "simkl", "mdblist"].filter((sink) => effective[`plex_${sink}_ratings`]);
  if (provider !== "plex" && activeTab === "ratings") activeTab = "source";
  const dup = duplicateWebhook(current);
  root.innerHTML = `
    <div class="cx-card scrm-modal scwm-modal ${providerClass(provider)}">
      <div class="cx-head scrm-head">
        <span class="scrm-head-logo">${providerIcon(provider)}</span>
        <div class="scrm-head-copy"><strong>${esc(modalTitle())}</strong><span>${esc(label(provider))} ${esc(profileLabel(provider, current.provider_instance))} webhook endpoint</span></div>
        <button type="button" class="scrm-close" data-close aria-label="Close"><span class="material-symbols-rounded">close</span></button>
      </div>
      <div class="scrm-body">
        <div class="scrm-panel-shell" data-scrm-provider="${esc(provider)}">
          <nav class="cw-subtiles scrm-nav" aria-label="Webhook sections">
            ${navButton("source", "webhook", "Source", "Profile, destinations, URL")}
            ${navButton("filters", "filter_alt", "Filters", "Users and server IDs")}
            ${navButton("options", "tune", "Options", "Thresholds")}
            ${provider === "plex" ? navButton("ratings", "star", "Ratings", "Plex ratings") : ""}
          </nav>
          <div class="scrm-content">
            ${sourcePanel(current)}
            ${filtersPanel(provider, filt)}
            ${optionsPanel()}
            ${ratingsPanel(provider, ratingsTargets)}
          </div>
          <div class="cw-connection-modal-footer scrm-footer">
            ${props.mode === "edit" ? `<button type="button" class="btn danger cw-connection-footer-delete cw-danger-confirm" data-delete-webhook><span class="material-symbols-rounded">delete</span><span>Delete webhook</span></button>` : `<span></span>`}
            ${errs.length
              ? `<span class="scrm-footer-error" role="alert" aria-live="polite"><span class="material-symbols-rounded">warning</span><span class="scrm-footer-error-text">${errs.map((x) => esc(x.message || x.code || x.field)).join(" ")}</span></span>`
              : dup ? `<span class="scrm-footer-warn"><span class="material-symbols-rounded">warning</span>This media profile already has a webhook</span>` : `<span></span>`}
            <button type="button" class="btn cw-connection-footer-cancel" data-close>Cancel</button>
            <button type="button" class="btn primary cw-connection-footer-save" data-save ${dup ? "disabled" : ""}>${saving ? "Saving..." : "Save changes"}</button>
          </div>
        </div>
      </div>
    </div>
  `;
  ensureVisiblePanel();
}

function setBusy(flag) {
  saving = flag;
  window.cxSetModalDismissible?.(!flag);
  root.querySelectorAll("button,input,select,textarea").forEach((n) => { n.disabled = flag || n.closest(".is-disabled"); });
}

function payload() {
  const src = currentSource();
  const provider = String(src.provider || "").toLowerCase();
  const instance = src.instance || "default";
  const body = { provider, provider_instance: instance };
  const sink = String(root.querySelector("#scw-sink")?.value || "").trim().toLowerCase();
  if (sink && sinks.includes(sink)) {
    body.sinks = [sink];
    body.sink_instances = { [sink]: root.querySelector("#scw-sink-instance")?.value || "default" };
    if (originalSink && originalSink !== sink) body.prev_sink = originalSink;
  }
  body.filters = { username_whitelist: splitValues(root.querySelector("#scw-users")?.value) };
  if (provider === "plex") {
    body.filters.server_uuid_whitelist = splitValues(root.querySelector("#scw-allow")?.value);
    body.filters.server_uuid_blacklist = splitValues(root.querySelector("#scw-block")?.value);
    body.filters.server_uuid = body.filters.server_uuid_whitelist[0] || "";
  }
  if (provider === "plex") {
    for (const sink of sinks) body[`plex_${sink}_ratings`] = !!root.querySelector(`[data-rating="${sink}"]`)?.checked;
  }
  if (root.querySelector("#scw-override-options")?.checked) {
    const pause = root.querySelector("#scw-pause")?.value;
    const suppress = root.querySelector("#scw-suppress")?.value;
    if (pause !== "") body.pause_debounce_seconds = Number(pause);
    if (suppress !== "") body.suppress_start_at = Number(suppress);
  }
  return body;
}

async function save() {
  if (saving) return;
  if (duplicateWebhook(selectedWebhook())) {
    render();
    return;
  }
  const body = payload();
  setBusy(true);
  try {
    const data = await api("/api/scrobbler/webhooks/profile", body);
    props.overview = data;
    props.onSaved?.(data);
    const sink = String((body.sinks || [])[0] || "").toLowerCase();
    const saved = (data.webhooks || []).find((w) =>
      String(w.provider || "").toLowerCase() === body.provider &&
      normInst(w.provider_instance) === normInst(body.provider_instance) &&
      String(w.sink || "").toLowerCase() === sink
    );
    props.mode = "edit";
    props.webhook = saved || { provider: body.provider, provider_instance: body.provider_instance, sink, sink_instance: (body.sink_instances || {})[sink] || "default", enabled: true, endpoint_url: "", effective_settings: {}, explicit_settings: {} };
    originalSink = String(props.webhook.sink || "").toLowerCase();
    activeTab = "source";
    setBusy(false);
    render();
  } catch (err) {
    setBusy(false);
    render(err.payload?.errors || [{ message: err.message }]);
  }
}

function clearDanger() {
  if (dangerTimer) clearTimeout(dangerTimer);
  dangerTimer = 0;
  destructive = "";
  const del = root?.querySelector("[data-delete-webhook]");
  if (del) {
    del.classList.remove("is-confirming");
    del.innerHTML = `<span class="material-symbols-rounded">delete</span><span>Delete webhook</span>`;
  }
  const regen = root?.querySelector("[data-regenerate]");
  if (regen) {
    regen.classList.remove("is-confirming");
    regen.innerHTML = `<span class="material-symbols-rounded">sync_lock</span>`;
  }
}

function armDanger(kind, btn) {
  clearDanger();
  destructive = kind;
  if (!btn) return;
  btn.classList.add("is-confirming");
  if (kind === "delete") btn.innerHTML = `<span class="material-symbols-rounded">warning</span><span>Confirm delete</span>`;
  else btn.innerHTML = `<span class="material-symbols-rounded">check</span>`;
  dangerTimer = setTimeout(clearDanger, 4200);
}

async function deleteWebhook(btn) {
  if (destructive !== "delete") {
    armDanger("delete", btn);
    return;
  }
  clearDanger();
  setBusy(true);
  try {
    const current = selectedWebhook();
    const data = await api("/api/scrobbler/webhooks/profile/disable", { provider: current.provider, provider_instance: current.provider_instance, sink: selectedSinkKey(), remove: true });
    props.onSaved?.(data);
    window.cxCloseModal?.();
  } catch (err) {
    setBusy(false);
    render(err.payload?.errors || [{ message: err.message }]);
  }
}

async function regenerate(btn) {
  if (destructive !== "regenerate") {
    armDanger("regenerate", btn);
    return;
  }
  clearDanger();
  setBusy(true);
  try {
    const current = selectedWebhook();
    const data = await api("/api/scrobbler/webhooks/profile/regenerate", { provider: current.provider, provider_instance: current.provider_instance });
    props.onSaved?.(data);
    window.cxCloseModal?.();
  } catch (err) {
    setBusy(false);
    render(err.payload?.errors || [{ message: err.message }]);
  }
}

export async function mount(shell, incoming = {}) {
  detachHandlers();
  root = shell;
  props = incoming;
  saving = false;
  destructive = "";
  modalKey = String(props.mode === "create" ? "__new__" : `${props.webhook?.provider || ""}:${props.webhook?.provider_instance || ""}`);
  activeTab = normalizeActiveTab(lastTab[modalKey] || "source");
  if (root) root.dataset.scrmTab = activeTab;
  originalSink = String(props.webhook?.sink || "").toLowerCase();
  render();
  boundRoot = root;
  const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
  if (controller) boundRoot.__cwScrobblerWebhookAbort = controller;
  clickHandler = async (e) => {
    if (e.target.closest("[data-close]")) {
      e.preventDefault();
      window.cxCloseModal?.();
      return;
    }
    if (e.target.closest("[data-save]")) {
      e.preventDefault();
      await save();
      return;
    }
    if (e.target.closest("[data-delete-webhook]")) {
      e.preventDefault();
      await deleteWebhook(e.target.closest("[data-delete-webhook]"));
      return;
    }
    if (e.target.closest("[data-regenerate]")) {
      e.preventDefault();
      await regenerate(e.target.closest("[data-regenerate]"));
      return;
    }
    const pick = e.target.closest("[data-pick-user]");
    if (pick) {
      e.preventDefault();
      e.stopPropagation();
      e.stopImmediatePropagation?.();
      const keepTab = currentActiveTab();
      await pickUser(pick, keepTab);
      preserveVisiblePanel(keepTab);
      return;
    }
    const uuid = e.target.closest("[data-fetch-uuid]");
    if (uuid) {
      e.preventDefault();
      e.stopPropagation();
      e.stopImmediatePropagation?.();
      const keepTab = currentActiveTab();
      await fetchServerUuid(uuid.dataset.fetchUuid, uuid, keepTab);
      preserveVisiblePanel(keepTab);
      return;
    }
    const copy = e.target.closest("[data-copy]");
    if (copy?.dataset.copy) {
      e.preventDefault();
      await navigator.clipboard?.writeText(copy.dataset.copy);
      flashCopied(copy);
      return;
    }
    const add = e.target.closest("[data-add-filter]");
    if (add) {
      e.preventDefault();
      e.stopPropagation();
      addFilterEntry(add.getAttribute("data-add-filter"), add.getAttribute("data-add-label") || "entry");
      return;
    }
    const clear = e.target.closest("[data-clear-filter]");
    if (clear) {
      e.preventDefault();
      e.stopPropagation();
      const target = root.querySelector(`#${clear.getAttribute("data-clear-filter")}`);
      if (target) target.value = "";
      return;
    }
    const passiveControl = e.target.closest(".scrm-content input, .scrm-content select, .scrm-content textarea, .scrm-content label");
    if (passiveControl) {
      e.stopPropagation();
      return;
    }
    const tab = e.target.closest(".scrm-nav-item[data-tab]");
    if (tab) {
      e.preventDefault();
      e.stopPropagation();
      switchTab(tab.dataset.tab || "source");
      return;
    }
    e.stopPropagation();
  };
  changeHandler = (e) => {
    if (e.target.id === "scw-source-provider") {
      const provider = String(e.target.value || "plex").toLowerCase();
      const instance = sourceProfiles(provider)[0]?.instance || "default";
      props.webhook = { provider, provider_instance: instance, enabled: true, endpoint_url: "", sink: "", sink_instance: "", effective_settings: {}, explicit_settings: {} };
      render();
      return;
    }
    if (e.target.id === "scw-source-instance") {
      props.webhook = { ...selectedWebhook(), provider_instance: String(e.target.value || "default"), endpoint_url: "" };
      render();
      return;
    }
    if (e.target.id === "scw-sink") {
      const sink = String(e.target.value || "").toLowerCase();
      props.webhook = { ...selectedWebhook(), sink, sink_instance: sinkProfiles(sink)[0]?.instance || "default" };
      render();
      return;
    }
    if (e.target.id === "scw-sink-instance") {
      props.webhook = { ...selectedWebhook(), sink: selectedSinkKey(), sink_instance: e.target.value || "default" };
      return;
    }
    if (e.target.closest(".scrm-content input, .scrm-content select, .scrm-content textarea")) {
      e.stopPropagation();
    }
  };
  boundRoot.addEventListener("click", clickHandler, controller ? { signal: controller.signal } : undefined);
  boundRoot.addEventListener("change", changeHandler, controller ? { signal: controller.signal } : undefined);
}

export function unmount() {
  window.cwMediaUserPicker?.close?.();
  detachHandlers();
  if (dangerTimer) clearTimeout(dangerTimer);
  dangerTimer = 0;
  root = null;
  props = {};
  saving = false;
  destructive = "";
  originalSink = "";
  activeTab = "source";
}

export default { mount, unmount };
