/* CrossWatch - Scrobbler Route Modal */
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const label = (v) => ({ plex: "Plex", jellyfin: "Jellyfin", emby: "Emby", trakt: "Trakt", simkl: "SIMKL", mdblist: "MDBList" }[String(v || "").toLowerCase()] || String(v || "").toUpperCase());
const sources = ["plex", "jellyfin", "emby"];
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
let draft = null;
let saving = false;
let confirmDelete = false;
let deleteTimer = 0;
let activeTab = "route";
let modalKey = "";
const lastTab = {};
let boundRoot = null;
let clickHandler = null;
let changeHandler = null;

function detachHandlers() {
  if (boundRoot?.__cwScrobblerRouteAbort) {
    try { boundRoot.__cwScrobblerRouteAbort.abort(); } catch {}
    delete boundRoot.__cwScrobblerRouteAbort;
  }
  if (boundRoot && clickHandler) boundRoot.removeEventListener("click", clickHandler);
  if (boundRoot && changeHandler) boundRoot.removeEventListener("change", changeHandler);
  boundRoot = null;
  clickHandler = null;
  changeHandler = null;
}

async function request(url, method, body) {
  const res = await fetch(url, {
    method,
    credentials: "same-origin",
    cache: "no-store",
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data?.ok === false) {
    const err = new Error(data?.error || `HTTP ${res.status}`);
    err.payload = data;
    throw err;
  }
  return data;
}

function clone(v) {
  return JSON.parse(JSON.stringify(v || {}));
}

function allSourceProfiles(provider) {
  const group = (props.overview?.eligible_sources || []).find((x) => x.provider === provider);
  return (group?.profiles || []).filter((x) => x.eligible);
}

function allSinkProfiles(sink) {
  const group = (props.overview?.destination_availability || []).find((x) => x.provider === sink);
  return (group?.profiles || []).filter((x) => x.configured);
}

function sourceProviders(selected = "") {
  const available = sources.filter((p) => allSourceProfiles(p).length);
  const current = String(selected || "").toLowerCase();
  return current && sources.includes(current) && !available.includes(current) ? [current, ...available] : available;
}

function sinkProviders(selected = "") {
  const available = sinks.filter((p) => allSinkProfiles(p).length);
  const current = String(selected || "").toLowerCase();
  return current && sinks.includes(current) && !available.includes(current) ? [current, ...available] : available;
}

function ratingSinkProviders(selected = []) {
  const available = sinkProviders();
  const selectedList = [...selected].map((x) => String(x || "").toLowerCase()).filter((x) => sinks.includes(x));
  return [...selectedList.filter((x) => !available.includes(x)), ...available];
}

function nextId() {
  const used = new Set((props.overview?.routes || []).map((r) => String(r.id || "")));
  let i = 1;
  while (used.has(`R${i}`)) i += 1;
  return `R${i}`;
}

function defaultRoute() {
  const srcProvider = sourceProviders()[0] || "";
  const sink = sinkProviders()[0] || "";
  return {
    id: nextId(),
    enabled: true,
    provider: srcProvider,
    provider_instance: allSourceProfiles(srcProvider)[0]?.instance || "",
    sink,
    sink_instance: allSinkProfiles(sink)[0]?.instance || "",
    filters: {},
    options: { auto_remove_watchlist: "inherit", ratings: { mode: "off", targets: [] }, scrobble: {}, watch: {} },
  };
}

function ensureDraft() {
  if (draft) return draft;
  draft = props.mode === "create" ? defaultRoute() : clone(props.route || defaultRoute());
  draft.filters = draft.filters || {};
  draft.options = draft.options || {};
  draft.options.ratings = draft.options.ratings || { mode: "off", targets: [] };
  draft.options.scrobble = draft.options.scrobble || {};
  draft.options.watch = draft.options.watch || {};
  return draft;
}

function optionsForProfiles(provider, selected, kind) {
  const list = kind === "source" ? allSourceProfiles(provider) : allSinkProfiles(provider);
  const current = String(selected || "");
  const selectedExists = list.some((p) => p.instance === current);
  const missing = current && !selectedExists ? `<option value="${esc(current)}" selected disabled>${esc(profileLabel(provider, current, kind))} (not configured)</option>` : "";
  const options = list.map((p) => `<option value="${esc(p.instance)}" ${p.instance === current ? "selected" : ""}>${esc(profileName(p.instance))}</option>`).join("");
  return missing + options || `<option value="">No configured profile</option>`;
}

function optionsForProviders(kind, selected) {
  const current = String(selected || "").toLowerCase();
  const list = kind === "source" ? sourceProviders(current) : sinkProviders(current);
  const configured = kind === "source" ? allSourceProfiles : allSinkProfiles;
  const empty = kind === "source" ? "No configured source provider" : "No configured destination provider";
  if (!list.length) return `<option value="">${empty}</option>`;
  return list.map((p) => {
    const unavailable = !configured(p).length;
    return `<option value="${esc(p)}" ${p === current ? "selected" : ""} ${unavailable ? "disabled" : ""}>${esc(label(p))}${unavailable ? " (not configured)" : ""}</option>`;
  }).join("");
}

function profileLabel(provider, instance, kind) {
  const list = kind === "source" ? allSourceProfiles(provider) : allSinkProfiles(provider);
  const p = list.find((x) => x.instance === instance);
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

function routeKey(provider, instance, sink, sinkInstance) {
  return `${String(provider || "").toLowerCase()}|${normInst(instance)}|${String(sink || "").toLowerCase()}|${normInst(sinkInstance)}`;
}

function duplicateRoute(r) {
  const cur = routeKey(r.provider, r.provider_instance, r.sink, r.sink_instance);
  return (props.overview?.routes || []).some((x) => String(x.id || "") !== String(r.id || "") && routeKey(x.provider, x.provider_instance, x.sink, x.sink_instance) === cur);
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

function routeTitle(r) {
  if (props.mode === "create") return "Add Watcher route";
  if (props.mode === "delete") return "Delete Watcher route";
  return "Edit Watcher route";
}

function listText(values) {
  return (Array.isArray(values) ? values : []).join("\n");
}

function split(text) {
  return String(text || "").split(/[\n,]/).map((x) => x.trim()).filter(Boolean);
}

function appendListValue(id, value) {
  const el = root?.querySelector(`#${id}`);
  const next = String(value || "").trim();
  if (!el || !next) return;
  const values = split(el.value);
  if (!values.some((x) => x.toLowerCase() === next.toLowerCase())) values.push(next);
  el.value = values.join("\n");
}

function currentProvider() {
  return root?.querySelector("#scr-provider")?.value || ensureDraft().provider || "";
}

function currentProviderInstance() {
  return root?.querySelector("#scr-provider-instance")?.value || ensureDraft().provider_instance || "";
}

async function pickUser(btn, keepTab = currentActiveTab()) {
  activeTab = normalizeActiveTab(keepTab);
  if (!currentProvider() || !window.cwMediaUserPicker?.open) return;
  window.cwMediaUserPicker.open({
    provider: currentProvider(),
    instance: currentProviderInstance(),
    anchorEl: btn,
    title: `Pick ${label(currentProvider())} user`,
    overlay: false,
    onPick: (user) => appendListValue("scr-users", user.name || user.id),
  });
  preserveVisiblePanel(keepTab);
}

async function fetchServerUuid(targetId, btn, keepTab = currentActiveTab()) {
  activeTab = normalizeActiveTab(keepTab);
  if (currentProvider() !== "plex" || saving) return;
  const labelEl = btn?.querySelector("span:not(.material-symbols-rounded)");
  const old = labelEl?.textContent;
  if (btn) {
    btn.disabled = true;
    if (labelEl) labelEl.textContent = "Fetching";
  }
  try {
    const res = await fetch(`/api/plex/server_uuid?instance=${encodeURIComponent(currentProviderInstance())}`, { cache: "no-store", credentials: "same-origin" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data?.ok === false) throw new Error(data?.error || data?.message || `HTTP ${res.status}`);
    appendListValue(targetId, data.server_uuid);
    preserveVisiblePanel(keepTab);
  } catch (err) {
    syncDraftFromDom();
    render([{ message: err.message || "Could not fetch Plex server UUID." }]);
    preserveVisiblePanel(keepTab);
  } finally {
    if (btn && root?.contains(btn)) {
      btn.disabled = false;
      if (labelEl) labelEl.textContent = old || "Fetch";
    }
  }
}

function navButton(key, icon, title, copy) {
  return `<button type="button" class="cw-subtile scrm-nav-item ${activeTab === key ? "active" : ""}" data-tab="${esc(key)}" aria-selected="${activeTab === key ? "true" : "false"}"><span class="material-symbols-rounded cw-connection-nav-icon">${esc(icon)}</span><span class="cw-connection-nav-copy"><strong>${esc(title)}</strong><small>${esc(copy)}</small></span><span class="material-symbols-rounded cw-connection-nav-chev">chevron_right</span></button>`;
}

function fieldHelp(text) {
  const tip = esc(text);
  return `<span class="scrm-field-help material-symbols-rounded" tabindex="0" role="img" aria-label="Help: ${tip}" title="${tip}">help</span>`;
}

function field(labelText, html, extra = "", helpText = "") {
  return `<label class="scrm-field ${extra}"><span class="scrm-label-text">${esc(labelText)}${helpText ? fieldHelp(helpText) : ""}</span>${html}</label>`;
}

function journeyHelp(key) {
  const url = window.CW?.HelpLinks?.url?.(key) || "";
  if (!url) return "";
  return `<a class="scrm-journey-help" href="${esc(url)}" target="_blank" rel="noopener noreferrer" aria-label="Open guide" title="Open guide"><span class="material-symbols-rounded" aria-hidden="true">help</span></a>`;
}

function fieldIcon(icon, labelText, html) {
  return `<label class="scrm-field scrm-field-row" title="${esc(labelText)}"><span class="material-symbols-rounded scrm-field-ico" aria-hidden="true">${esc(icon)}</span>${html}</label>`;
}

function routePanel(r) {
  return `
    <section class="scrm-panel ${activeTab === "route" ? "active" : ""}" data-panel="route">
      <div class="scrm-journey">
        <span class="material-symbols-rounded scrm-journey-icon">route</span>
        <div><strong>Choose the source, then send it to a tracker</strong><p>Watcher routes listen to one configured media profile and forward matching play events to one configured destination profile.</p></div>
        ${journeyHelp("scrobbler-watcher")}
      </div>
      <div class="scrm-route-grid">
        <div class="scrm-provider-card ${providerClass(r.provider)}">
          <div class="scrm-card-head"><span class="scrm-provider-mark">${providerIcon(r.provider)}</span><div><strong>Source</strong><small>${esc(label(r.provider))}</small></div></div>
          <div class="scrm-fields">
            ${fieldIcon("dns", "Provider", `<select class="input" id="scr-provider">${optionsForProviders("source", r.provider)}</select>`)}
            ${fieldIcon("person", "Profile", `<select class="input" id="scr-provider-instance">${optionsForProfiles(r.provider, r.provider_instance, "source")}</select>`)}
          </div>
        </div>
        <div class="scrm-route-arrow"><span class="material-symbols-rounded">arrow_forward</span></div>
        <div class="scrm-provider-card ${providerClass(r.sink)}">
          <div class="scrm-card-head"><span class="scrm-provider-mark">${providerIcon(r.sink)}</span><div><strong>Destination</strong><small>${esc(label(r.sink))}</small></div></div>
          <div class="scrm-fields">
            ${fieldIcon("gps_fixed", "Provider", `<select class="input" id="scr-sink">${optionsForProviders("sink", r.sink)}</select>`)}
            ${fieldIcon("person", "Profile", `<select class="input" id="scr-sink-instance">${optionsForProfiles(r.sink, r.sink_instance, "sink")}</select>`)}
          </div>
        </div>
      </div>
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

function filtersPanel(r, f) {
  const isPlex = r.provider === "plex";
  const rows = [filterRow({ title: "Username whitelist", subtitle: "Only events from these usernames will be scrobbled.", id: "scr-users", value: listText(f.username_whitelist), placeholder: "One username per line", addLabel: "username", actionAttr: "data-pick-user", actionIcon: "person_search", actionTitle: "Find users" })];
  if (isPlex) {
    rows.push(filterRow({ title: "Server UUID allowlist", subtitle: "Control which servers are allowed.", id: "scr-allow", value: listText(f.server_uuid_whitelist || (f.server_uuid ? [f.server_uuid] : [])), placeholder: "One server UUID per line", dot: "scrm-dot-allow", addLabel: "server UUID", actionAttr: `data-fetch-uuid="scr-allow"`, actionIcon: "search", actionTitle: "Fetch server UUID" }));
    rows.push(filterRow({ title: "Server UUID blocklist", subtitle: "Control which servers are blocked.", id: "scr-block", value: listText(f.server_uuid_blacklist), placeholder: "One server UUID per line", dot: "scrm-dot-block", addLabel: "server UUID", actionAttr: `data-fetch-uuid="scr-block"`, actionIcon: "search", actionTitle: "Fetch server UUID" }));
  }
  const toggle = isPlex
    ? `<label class="scrm-toggle-row"><span class="scrm-toggle-copy"><span class="material-symbols-rounded">live_tv</span><span><strong>Ignore Plex Live TV &amp; DVR</strong><small>Skip scrobbles from live channels and DVR recordings.</small></span></span><span class="scrm-switch"><input type="checkbox" id="scr-live" ${f.ignore_live_tv_dvr ? "checked" : ""}><span class="scrm-switch-track"></span></span></label>`
    : "";
  return `
    <section class="scrm-panel ${activeTab === "filters" ? "active" : ""}" data-panel="filters">
      <div class="scrm-journey scrm-journey-compact">
        <span class="material-symbols-rounded scrm-journey-icon">filter_alt</span>
        <div><strong>Only scrobble the activity you want</strong><p>Leave filters empty to accept every matching event from this source profile.</p></div>
        ${journeyHelp("scrobbler-filters")}
      </div>
      <div class="scrm-filter-rows">${rows.join("")}</div>
      ${toggle}
    </section>
  `;
}

function optionsPanel(r) {
  return `
    <section class="scrm-panel ${activeTab === "options" ? "active" : ""}" data-panel="options">
      <div class="scrm-journey scrm-journey-compact">
        <span class="material-symbols-rounded scrm-journey-icon">tune</span>
        <div><strong>Route-level behavior</strong><p>These settings apply to this route and override the global defaults.<br><b>Leave them at their defaults unless you know what you are doing.</b></p></div>
      </div>
      <div class="scrm-fields three">
        ${field("Auto remove from Watchlists", `<select class="input" id="scr-auto"><option value="inherit" ${r.options.auto_remove_watchlist === "inherit" ? "selected" : ""}>Inherit</option><option value="on" ${r.options.auto_remove_watchlist === "on" ? "selected" : ""}>On</option><option value="off" ${r.options.auto_remove_watchlist === "off" ? "selected" : ""}>Off</option></select>`, "", "Override whether watched items are removed from watchlists for this route.")}
        ${field("Watched threshold (%)", `<input class="input" id="scr-watched" type="number" min="0" max="100" value="${esc(r.options.scrobble?.watched_at ?? "")}" placeholder="Global">`, "", "Progress percentage that counts an item as watched for this route.")}
        ${field("Final-stop trust (%)", `<input class="input" id="scr-force" type="number" min="0" max="100" value="${esc(r.options.scrobble?.force_stop_at ?? "")}" placeholder="Global">`, "", "At or above this progress, a final stop event is trusted for this route.")}
        ${field("Progress step (%)", `<input class="input" id="scr-progress-step" type="number" min="1" max="25" value="${esc(r.options.scrobble?.progress_step ?? "")}" placeholder="Global">`, "", "Minimum progress change required before sending another watching update. Leave this at the default, as it significantly affects API usage.")}
        ${field("Pause debounce (s)", `<input class="input" id="scr-watch-pause" type="number" min="0" max="3600" value="${esc(r.options.watch?.pause_debounce_seconds ?? "")}" placeholder="Global">`, "", "Seconds to ignore tiny pause/start flaps just after playback starts for this route.")}
        ${field("Suppress start (%)", `<input class="input" id="scr-watch-suppress" type="number" min="0" max="100" value="${esc(r.options.watch?.suppress_start_at ?? "")}" placeholder="Global">`, "", "Ignore new start events at or above this progress for this route.")}
      </div>
    </section>
  `;
}

function globalRatingTargets() {
  const g = props.overview?.source_state?.global_plex_ratings || {};
  return ["trakt", "simkl", "mdblist"].filter((s) => g[s]);
}

function ratingsPanel(r, ratings, ratingTargets) {
  if (r.provider !== "plex") return "";
  const ratingSinkList = ratingSinkProviders(ratingTargets);
  const isOff = ratings.mode !== "custom";
  const globalTargets = globalRatingTargets();
  const globalWarn = globalTargets.length
    ? `<div class="scrm-note is-warn"><span class="material-symbols-rounded">warning</span><span>Global Plex ratings is configured and already forwarding to <strong>${esc(globalTargets.map(label).join(", "))}</strong>. A route-specific webhook sends ratings <em>in addition</em> to the global one - only add one if this route needs different destinations.</span></div>`
    : "";
  const targets = ratingSinkList.length
    ? ratingSinkList.map((sink) => {
        const configured = allSinkProfiles(sink).length > 0;
        return `<label class="scrm-target${configured ? "" : " is-disabled"}"><input type="checkbox" data-rating-target="${sink}" ${ratingTargets.has(sink) ? "checked" : ""} ${configured && !isOff ? "" : "disabled"}><span class="scrm-target-mark">${providerIcon(sink)}</span><span>${label(sink)}${configured ? "" : " · not configured"}</span></label>`;
      }).join("")
    : `<span class="scrm-muted">No configured rating destinations</span>`;
  const endpoint = r.ratings_webhook_url
    ? `<div class="scrm-endpoint"><code title="${esc(r.ratings_webhook_url)}">${esc(r.ratings_webhook_url)}</code><button type="button" class="btn small material-symbols-rounded" data-copy="${esc(r.ratings_webhook_url)}" title="Copy URL" aria-label="Copy URL">content_copy</button><button type="button" class="btn small danger material-symbols-rounded" data-regen-route title="Regenerate URL" aria-label="Regenerate URL">sync_lock</button></div>`
    : `<div class="scrm-note"><span class="material-symbols-rounded">link</span><span>Save the route to generate a route-specific ratings webhook URL.</span></div>`;
  return `
    <section class="scrm-panel ${activeTab === "ratings" ? "active" : ""}" data-panel="ratings">
      <div class="scrm-journey scrm-journey-compact">
        <span class="material-symbols-rounded scrm-journey-icon">star</span>
        <div><strong>Plex ratings webhook</strong><p>Route-specific ratings are Plex-only and can target the destinations selected below.</p></div>
      </div>
      ${globalWarn}
      <div class="scrm-filter-group">
        <div class="scrm-filter-head"><span class="material-symbols-rounded">star</span><div><strong>Route ratings</strong><small>Forward this route's Plex ratings to specific trackers.</small></div></div>
        ${field("Ratings mode", `<select class="input" id="scr-ratings-mode"><option value="off" ${isOff ? "selected" : ""}>Off</option><option value="custom" ${!isOff ? "selected" : ""}>Custom route webhook</option></select>`)}
        <div class="scrm-field"><span>Destinations</span><div class="scrm-targets${isOff ? " is-muted" : ""}">${targets}</div></div>
      </div>
      ${isOff ? "" : endpoint}
    </section>
  `;
}

function normalizeActiveTab(tab = activeTab) {
  const next = String(tab || "route");
  return ["route", "filters", "options", "ratings"].includes(next) ? next : "route";
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

function render(errors = []) {
  activeTab = normalizeActiveTab(root?.dataset.scrmTab || activeTab);
  const r = ensureDraft();
  if (r.provider !== "plex" && activeTab === "ratings") activeTab = "route";
  const f = r.filters || {};
  const ratings = r.options?.ratings || {};
  const ratingTargets = new Set(ratings.targets || []);
  const dup = props.mode !== "delete" && duplicateRoute(r);
  root.innerHTML = `
    <div class="cx-card scrm-modal ${providerClass(r.provider)}">
      <div class="cx-head scrm-head">
        <span class="scrm-head-logo">${providerIcon(r.provider)}</span>
        <div class="scrm-head-copy"><strong>${esc(routeTitle(r))}</strong><span>${esc(label(r.provider))} ${esc(profileLabel(r.provider, r.provider_instance, "source"))} to ${esc(label(r.sink))} ${esc(profileLabel(r.sink, r.sink_instance, "sink"))}</span></div>
        <button type="button" class="scrm-close" data-close aria-label="Close"><span class="material-symbols-rounded">close</span></button>
      </div>
      <div class="scrm-body">
        <div class="scrm-panel-shell" data-scrm-provider="${esc(r.provider)}">
          <nav class="cw-subtiles scrm-nav" aria-label="Route sections">
            ${navButton("route", "route", "Route", "Source and destination")}
            ${navButton("filters", "filter_alt", "Filters", "Users and server IDs")}
            ${navButton("options", "tune", "Options", "Watchlist and thresholds")}
            ${r.provider === "plex" ? navButton("ratings", "star", "Ratings", "Plex webhook") : ""}
          </nav>
          <div class="scrm-content">
            ${routePanel(r)}
            ${filtersPanel(r, f)}
            ${optionsPanel(r)}
            ${ratingsPanel(r, ratings, ratingTargets)}
          </div>
          <div class="cw-connection-modal-footer scrm-footer">
            ${props.mode !== "create" ? `<button type="button" class="btn danger cw-connection-footer-delete cw-danger-confirm" data-delete><span class="material-symbols-rounded">delete</span><span>Delete route</span></button>` : `<span></span>`}
            ${errors.length
              ? `<span class="scrm-footer-error" role="alert" aria-live="polite"><span class="material-symbols-rounded">warning</span><span class="scrm-footer-error-text">${errors.map((x) => esc(x.message || x.code || x.field)).join(" ")}</span></span>`
              : dup ? `<span class="scrm-footer-warn"><span class="material-symbols-rounded">warning</span>This source → destination route already exists</span>` : `<span></span>`}
            <button type="button" class="btn cw-connection-footer-cancel" data-close>Cancel</button>
            ${props.mode !== "delete" ? `<button type="button" class="btn primary cw-connection-footer-save" data-save ${dup ? "disabled" : ""}>${saving ? "Saving..." : "Save changes"}</button>` : ""}
          </div>
        </div>
      </div>
    </div>
  `;
  ensureVisiblePanel();
}

function collect() {
  const providerEl = root.querySelector("#scr-provider");
  const providerInstanceEl = root.querySelector("#scr-provider-instance");
  const sinkEl = root.querySelector("#scr-sink");
  const sinkInstanceEl = root.querySelector("#scr-sink-instance");
  const provider = providerEl ? providerEl.value : draft.provider || "";
  const allow = split(root.querySelector("#scr-allow")?.value);
  const filters = {
    username_whitelist: split(root.querySelector("#scr-users")?.value),
    server_uuid: allow[0] || "",
    server_uuid_whitelist: allow,
    server_uuid_blacklist: split(root.querySelector("#scr-block")?.value),
  };
  if (provider === "plex") filters.ignore_live_tv_dvr = !!root.querySelector("#scr-live")?.checked;
  const scrobble = {};
  const watched = root.querySelector("#scr-watched")?.value;
  const force = root.querySelector("#scr-force")?.value;
  const progressStep = root.querySelector("#scr-progress-step")?.value;
  if (watched !== "") scrobble.watched_at = Number(watched);
  if (force !== "") scrobble.force_stop_at = Number(force);
  if (progressStep !== "") scrobble.progress_step = Number(progressStep);
  const watch = {};
  const pause = root.querySelector("#scr-watch-pause")?.value;
  const suppress = root.querySelector("#scr-watch-suppress")?.value;
  if (pause !== "") watch.pause_debounce_seconds = Number(pause);
  if (suppress !== "") watch.suppress_start_at = Number(suppress);
  const ratingsMode = root.querySelector("#scr-ratings-mode")?.value || "off";
  return {
    id: draft.id,
    enabled: props.mode === "create" ? true : draft.enabled !== false,
    provider,
    provider_instance: providerInstanceEl ? providerInstanceEl.value : draft.provider_instance || "",
    sink: sinkEl ? sinkEl.value : draft.sink || "",
    sink_instance: sinkInstanceEl ? sinkInstanceEl.value : draft.sink_instance || "",
    filters,
    options: {
      auto_remove_watchlist: root.querySelector("#scr-auto")?.value || "inherit",
      scrobble,
      watch,
      ratings: provider === "plex" ? {
        mode: ratingsMode,
        targets: [...root.querySelectorAll("[data-rating-target]:checked")].map((x) => x.dataset.ratingTarget),
      } : { mode: "off", targets: [] },
    },
  };
}

function busy(flag) {
  saving = flag;
  window.cxSetModalDismissible?.(!flag);
  root.querySelectorAll("button,input,select,textarea").forEach((n) => { n.disabled = flag; });
}

async function save(regenerate = false) {
  if (saving) return;
  syncDraftFromDom();
  if (duplicateRoute(ensureDraft())) {
    render();
    return;
  }
  busy(true);
  try {
    const body = collect();
    if (regenerate) body.regenerate_ratings_webhook = true;
    const data = props.mode === "create"
      ? await request("/api/scrobbler/routes", "POST", body)
      : await request(`/api/scrobbler/routes/${encodeURIComponent(draft.id)}`, "PUT", body);
    props.onSaved?.(data);
    if (data && Array.isArray(data.routes)) props.overview = data;
    props.mode = "edit";
    busy(false);
    render();
    flashSave(true);
  } catch (err) {
    busy(false);
    render(err.payload?.errors || [{ message: err.message }]);
    flashSave(false);
  }
}

let flashTimer = 0;
function flashSave(ok) {
  const btn = root?.querySelector(".cw-connection-footer-save");
  if (!btn) return;
  const label = btn.textContent || "Save changes";
  const grad = ok ? "#00e084,#2ea859" : "#ff5a6a,#d1342f";
  const bd = ok ? "rgba(0,224,132,.6)" : "rgba(255,112,124,.6)";
  btn.classList.remove("is-saved", "is-failed");
  btn.classList.add(ok ? "is-saved" : "is-failed");
  btn.innerHTML = `<span class="material-symbols-rounded cw-save-result-icon">${ok ? "check" : "close"}</span>`;
  btn.style.cssText = `background:linear-gradient(135deg,${grad})!important;border-color:${bd}!important;color:#fff!important;-webkit-text-fill-color:#fff!important;pointer-events:none!important`;
  const ic = btn.querySelector(".cw-save-result-icon");
  if (ic) ic.style.cssText = "color:#fff!important;-webkit-text-fill-color:#fff!important";
  if (flashTimer) clearTimeout(flashTimer);
  flashTimer = setTimeout(() => {
    const b = root?.querySelector(".cw-connection-footer-save");
    if (!b) return;
    b.classList.remove("is-saved", "is-failed");
    b.style.cssText = "";
    b.textContent = label;
  }, 1500);
}

function resetDeleteConfirm(btn) {
  confirmDelete = false;
  if (deleteTimer) clearTimeout(deleteTimer);
  deleteTimer = 0;
  if (!btn) return;
  btn.classList.remove("is-confirming");
  btn.innerHTML = `<span class="material-symbols-rounded">delete</span><span>Delete route</span>`;
}

function armDeleteConfirm(btn) {
  confirmDelete = true;
  if (!btn) return;
  btn.classList.add("is-confirming");
  btn.innerHTML = `<span class="material-symbols-rounded">warning</span><span>Confirm delete</span>`;
  if (deleteTimer) clearTimeout(deleteTimer);
  deleteTimer = setTimeout(() => resetDeleteConfirm(btn), 4200);
}

async function removeRoute(btn) {
  if (!confirmDelete) {
    armDeleteConfirm(btn);
    return;
  }
  resetDeleteConfirm(btn);
  busy(true);
  try {
    const data = await request(`/api/scrobbler/routes/${encodeURIComponent(draft.id)}`, "DELETE");
    props.onSaved?.(data);
    window.cxCloseModal?.();
  } catch (err) {
    busy(false);
    render(err.payload?.errors || [{ message: err.message }]);
  }
}

function syncDraftFromDom() {
  draft = { ...draft, ...collect() };
}

export async function mount(shell, incoming = {}) {
  detachHandlers();
  root = shell;
  props = incoming;
  draft = null;
  saving = false;
  confirmDelete = false;
  modalKey = String(props.mode === "create" ? "__new__" : (props.route?.id || "__new__"));
  activeTab = normalizeActiveTab("route");
  if (root) root.dataset.scrmTab = activeTab;
  render();
  if (props.mode === "delete") armDeleteConfirm(root.querySelector("[data-delete]"));
  boundRoot = root;
  const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
  if (controller) boundRoot.__cwScrobblerRouteAbort = controller;
  clickHandler = async (e) => {
    if (e.target.closest("[data-close]")) {
      e.preventDefault();
      window.cxCloseModal?.();
      return;
    }
    if (e.target.closest("[data-save]")) {
      e.preventDefault();
      await save(false);
      return;
    }
    if (e.target.closest("[data-delete]")) {
      e.preventDefault();
      await removeRoute(e.target.closest("[data-delete]"));
      return;
    }
    if (e.target.closest("[data-regen-route]")) {
      e.preventDefault();
      await save(true);
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
      e.stopPropagation();
      e.stopImmediatePropagation?.();
      await navigator.clipboard?.writeText(copy.dataset.copy);
      flashCopied(copy);
      preserveVisiblePanel(currentActiveTab());
      return;
    }
    const add = e.target.closest("[data-add-filter]");
    if (add) {
      e.preventDefault();
      e.stopPropagation();
      e.stopImmediatePropagation?.();
      addFilterEntry(add.getAttribute("data-add-filter"), add.getAttribute("data-add-label") || "entry");
      preserveVisiblePanel(currentActiveTab());
      return;
    }
    const clear = e.target.closest("[data-clear-filter]");
    if (clear) {
      e.preventDefault();
      e.stopPropagation();
      e.stopImmediatePropagation?.();
      const target = root.querySelector(`#${clear.getAttribute("data-clear-filter")}`);
      if (target) target.value = "";
      preserveVisiblePanel(currentActiveTab());
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
      syncDraftFromDom();
      ensureVisiblePanel(tab.dataset.tab || "route");
      return;
    }
    e.stopPropagation();
  };
  changeHandler = (e) => {
    if (["scr-provider", "scr-sink"].includes(e.target.id)) {
      const keepTab = currentActiveTab();
      syncDraftFromDom();
      if (e.target.id === "scr-provider") {
        draft.provider_instance = allSourceProfiles(draft.provider)[0]?.instance || "";
        if (draft.provider !== "plex") draft.options.ratings = { mode: "off", targets: [] };
      }
      if (e.target.id === "scr-sink") {
        draft.sink_instance = allSinkProfiles(draft.sink)[0]?.instance || "";
      }
      render();
      preserveVisiblePanel(keepTab);
      return;
    }
    if (["scr-provider-instance", "scr-sink-instance", "scr-ratings-mode"].includes(e.target.id)) {
      const keepTab = currentActiveTab();
      syncDraftFromDom();
      render();
      preserveVisiblePanel(keepTab);
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
  root = null;
  props = {};
  draft = null;
  saving = false;
  confirmDelete = false;
  if (deleteTimer) clearTimeout(deleteTimer);
  deleteTimer = 0;
  activeTab = "route";
}

export default { mount, unmount };
