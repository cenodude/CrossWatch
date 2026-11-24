/* CrossWatch - Scrobbler Route Modal */
import { createIdentityMap, identityMapSupported } from "../core/identity-map.js";
const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const label = (v) => ({ plex: "Plex", jellyfin: "Jellyfin", emby: "Emby", kodi: "Kodi", trakt: "Trakt", simkl: "SIMKL", mdblist: "MDBList", crosswatch: "CrossWatch", floppy: "Floppy", punchplay: "PunchPlay", bingebase: "BingeBase", flicklist: "FlickList", wetrakr: "WeTrakr", anilist: "AniList", myanimelist: "MyAnimeList", kitsu: "Kitsu", scrob: "Scrob", stremio: "Stremio", silo: "Silo", tofa: "Tofa" }[String(v || "").toLowerCase()] || String(v || "").toUpperCase());
const sources = ["plex", "jellyfin", "emby", "kodi", "scrob", "stremio", "silo", "tofa"];
const sinks = ["myanimelist", "kitsu", "plex", "jellyfin", "emby", "kodi", "crosswatch", "trakt", "simkl", "mdblist", "floppy", "punchplay", "bingebase", "flicklist", "wetrakr", "anilist", "scrob", "silo", "tofa"];
const ratingSinks = ["kitsu", "myanimelist", "crosswatch", "trakt", "simkl", "mdblist", "floppy", "punchplay", "flicklist", "wetrakr", "anilist", "scrob", "silo"];
const mediaSinks = ["plex", "jellyfin", "emby", "kodi", "silo", "tofa"];

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

let tofaRule = { key: "", data: null, busy: false };
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
let identityMap = null;
let userProfiles = [];
let selectedUserProfileId = "";
const destinationLibraries = new Map();

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

async function loadUserProfiles() {
  try {
    const res = await fetch("/api/user-profiles", { cache: "no-store", credentials: "same-origin" });
    const data = res.ok ? await res.json() : {};
    const rows = Array.isArray(data?.items) ? data.items : [];
    userProfiles = rows
      .map((row) => ({
        id: String(row?.id || "").trim(),
        label: String(row?.label || row?.id || "").trim(),
        instances: row?.instances && typeof row.instances === "object" ? row.instances : {},
      }))
      .filter((row) => row.id && row.label);
  } catch {
    userProfiles = [];
  }
}

function clone(v) {
  return JSON.parse(JSON.stringify(v || {}));
}

function allSourceProfiles(provider) {
  const group = (props.overview?.eligible_sources || []).find((x) => x.provider === provider);
  return (group?.profiles || []).filter((x) => x.eligible);
}

function allSinkProfiles(sink, source = draft?.provider || "", instance = draft?.provider_instance || "default") {
  const group = (props.overview?.destination_availability || []).find((x) => x.provider === sink);
  return (group?.profiles || []).filter((x) => x.configured && !(sink === source && normInst(x.instance) === normInst(instance)));
}

function sourceProviders(selected = "") {
  const available = sources.filter((p) => allSourceProfiles(p).length);
  const current = String(selected || "").toLowerCase();
  return current && sources.includes(current) && !available.includes(current) ? [current, ...available] : available;
}

function sinkProviders(selected = "", source = "", instance = draft?.provider_instance || "default") {
  const self = String(source || "").toLowerCase();
  const available = sinks.filter((p) => allSinkProfiles(p, self, instance).length);
  const current = String(selected || "").toLowerCase();
  if (current === self && !allSinkProfiles(current, self, instance).length) return available;
  return current && sinks.includes(current) && !available.includes(current) ? [current, ...available] : available;
}

function ratingSinkProviders(selected = [], source = "") {
  const self = String(source || "").toLowerCase();
  const available = sinkProviders("", self).filter((x) => ratingSinks.includes(x));
  const selectedList = [...selected].map((x) => String(x || "").toLowerCase()).filter((x) => ratingSinks.includes(x) && x !== self);
  return [...selectedList.filter((x) => !available.includes(x)), ...available];
}

function globalAnimeMappingEnabled() {
  return !!props.overview?.source_state?.global_anime_mapping_enabled;
}

function nextId() {
  const used = new Set((props.overview?.routes || []).map((r) => String(r.id || "")));
  let i = 1;
  while (used.has(`R${i}`)) i += 1;
  return `R${i}`;
}

function defaultRoute() {
  const srcProvider = sourceProviders()[0] || "";
  const srcInstance = allSourceProfiles(srcProvider)[0]?.instance || "";
  const sink = sinkProviders("", srcProvider, srcInstance)[0] || "";
  return {
    id: nextId(),
    enabled: true,
    provider: srcProvider,
    provider_instance: srcInstance,
    sink,
    sink_instance: allSinkProfiles(sink, srcProvider, srcInstance)[0]?.instance || "",
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
  const list = (kind === "source" ? allSourceProfiles(provider) : allSinkProfiles(provider))
    .sort((a, b) => profileOptionLabel(a).localeCompare(profileOptionLabel(b), undefined, { sensitivity: "base", numeric: true }));
  const current = String(selected || "");
  const selectedExists = list.some((p) => p.instance === current);
  const missing = current && !selectedExists ? `<option value="${esc(current)}" selected disabled>${esc(profileLabel(provider, current, kind))} (not configured)</option>` : "";
  const options = list.map((p) => `<option value="${esc(p.instance)}" title="${esc(p.instance)}" ${p.instance === current ? "selected" : ""}>${esc(profileOptionLabel(p))}</option>`).join("");
  return missing + options || `<option value="">${TH("scrobbler.routeModal.noInstance")}</option>`;
}

function optionsForProviders(kind, selected, source = "") {
  const current = String(selected || "").toLowerCase();
  const list = (kind === "source" ? sourceProviders(current) : sinkProviders(current, source))
    .sort((a, b) => label(a).localeCompare(label(b), undefined, { sensitivity: "base", numeric: true }));
  const configured = kind === "source" ? allSourceProfiles : allSinkProfiles;
  const empty = kind === "source" ? T("scrobbler.routeModal.noSource") : T("scrobbler.routeModal.noDestination");
  if (!list.length) return `<option value="">${empty}</option>`;
  return list.map((p) => {
    const unavailable = !configured(p).length;
    return `<option value="${esc(p)}" ${p === current ? "selected" : ""} ${unavailable ? "disabled" : ""}>${esc(label(p))}${unavailable ? " (not configured)" : ""}</option>`;
  }).join("");
}

function profileLabel(provider, instance, kind) {
  const list = kind === "source" ? allSourceProfiles(provider) : allSinkProfiles(provider);
  const p = list.find((x) => x.instance === instance);
  return profileOptionLabel(p || { instance });
}

function profileOptionLabel(profile) {
  const label = String(profile?.display_label || profile?.label || profile?.profile_label || profile?.sink_label || "").trim();
  return label || profileName(profile?.instance);
}

function profileName(instance) {
  const value = String(instance || "").trim();
  if (!value || value === "default") return T("scrobbler.routeModal.default");
  return value;
}

function userProfileField() {
  const current = String(selectedUserProfileId || draft?.profile_id || "");
  if (!current) return "";
  const profile = userProfiles.find((p) => p.id === current);
  if (!profile || !profileCanOwnRoute(profile, draft || {})) return "";
  return `<div class="scrm-profile-row">${fieldIcon("account_circle", T("scrobbler.routeModal.assignedProfile"), `<div class="scrm-profile-display" aria-label="${TH("scrobbler.routeModal.assignedProfile")}"><span>${esc(profile.label)}</span></div>`)}</div>`;
}

function profileInstanceValues(profile, provider) {
  const key = String(provider || "").toUpperCase();
  const raw = profile?.instances?.[key];
  return (Array.isArray(raw) ? raw : [raw]).map((x) => String(x || "").trim()).filter(Boolean);
}

function profileCanOwnRoute(profile, route) {
  if (!profile || !route) return false;
  const src = String(route.provider || "");
  const sink = String(route.sink || "");
  const srcInst = String(route.provider_instance || "default").trim() || "default";
  const sinkInst = String(route.sink_instance || "default").trim() || "default";
  if (!src || !sink) return false;
  return profileInstanceValues(profile, src).includes(srcInst) && profileInstanceValues(profile, sink).includes(sinkInst);
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
  return window.CW?.ProviderMeta?.logoPath?.(provider) || "";
}

function providerIcon(provider) {
  const src = logo(provider);
  return src ? `<img src="${esc(src)}" alt="">` : `<span>${esc(label(provider).slice(0, 1))}</span>`;
}

function providerClass(provider) {
  return `provider-${esc(String(provider || "").toLowerCase())}`;
}

function cssUrl(value) {
  return String(value || "").replace(/['"\\\n\r\f]/g, "");
}

function cssRgb(value, fallback = "95,141,255") {
  const rgb = String(value || "").trim();
  return /^(?:\d{1,3}\s*,\s*){2}\d{1,3}$/.test(rgb) ? rgb : fallback;
}

function providerRgb(provider) {
  return cssRgb(window.CW?.ProviderMeta?.tone?.(provider)?.rgb);
}

function providerCardAttrs(provider) {
  const src = cssUrl(logo(provider));
  const styles = [`--scrm-provider-rgb:${providerRgb(provider)}`];
  if (src) styles.push(`--scrm-provider-watermark:url('${esc(src)}')`);
  const style = ` style="${styles.join(";")}"`;
  return `class="scrm-provider-card ${providerClass(provider)}"${style}`;
}

function modalAttrs(provider) {
  const rgb = providerRgb(provider);
  return `class="cx-card scrm-modal ${providerClass(provider)}" style="--scrm-c1:${rgb};--scrm-c2:${rgb}"`;
}

function routeTitle(r) {
  if (props.mode === "create") return T("scrobbler.routeModal.add");
  if (props.mode === "delete") return T("scrobbler.routeModal.delete");
  return T("scrobbler.routeModal.edit");
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
    title: T("scrobbler.routeModal.pickUser", {provider: label(currentProvider())}),
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
    if (labelEl) labelEl.textContent = T("scrobbler.routeModal.fetching");
  }
  try {
    const res = await fetch(`/api/plex/server_uuid?instance=${encodeURIComponent(currentProviderInstance())}`, { cache: "no-store", credentials: "same-origin" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data?.ok === false) throw new Error(data?.error || data?.message || `HTTP ${res.status}`);
    appendListValue(targetId, data.server_uuid);
    preserveVisiblePanel(keepTab);
  } catch (err) {
    syncDraftFromDom();
    render([{ message: err.message || T("scrobbler.routeModal.uuidFailed") }]);
    preserveVisiblePanel(keepTab);
  } finally {
    if (btn && root?.contains(btn)) {
      btn.disabled = false;
      if (labelEl) labelEl.textContent = old || T("scrobbler.routeModal.fetch");
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
  return `<a class="scrm-journey-help" href="${esc(url)}" target="_blank" rel="noopener noreferrer" aria-label="${TH("scrobbler.routeModal.openGuide")}" title="${TH("scrobbler.routeModal.openGuide")}"><span class="material-symbols-rounded" aria-hidden="true">help</span></a>`;
}

function fieldIcon(icon, labelText, html) {
  return `<label class="scrm-field scrm-field-row" title="${esc(labelText)}"><span class="material-symbols-rounded scrm-field-ico" aria-hidden="true">${esc(icon)}</span>${html}</label>`;
}

function routePanel(r) {
  return `
    <section class="scrm-panel ${activeTab === "route" ? "active" : ""}" data-panel="route">
      <div class="scrm-journey">
        <span class="material-symbols-rounded scrm-journey-icon">route</span>
        <div><strong>${TH("scrobbler.routeModal.chooseSource")}</strong><p>${TH("scrobbler.routeModal.chooseSourceHint")}</p></div>
        ${journeyHelp("scrobbler-watcher")}
      </div>
      ${userProfileField()}
      <div class="scrm-route-grid">
        <div ${providerCardAttrs(r.provider)}>
          <div class="scrm-card-head"><span class="scrm-provider-mark">${providerIcon(r.provider)}</span><div><strong>${TH("scrobbler.routeModal.source")}</strong><small>${esc(label(r.provider))}</small></div></div>
          <div class="scrm-fields">
            ${fieldIcon("dns", T("scrobbler.routeModal.provider"), `<select class="input" id="scr-provider">${optionsForProviders("source", r.provider)}</select>`)}
            ${fieldIcon("badge", T("scrobbler.routeModal.profile"), `<select class="input" id="scr-provider-instance">${optionsForProfiles(r.provider, r.provider_instance, "source")}</select>`)}
          </div>
        </div>
        <div class="scrm-route-arrow"><span class="material-symbols-rounded">arrow_forward</span></div>
        <div ${providerCardAttrs(r.sink)}>
          <div class="scrm-card-head"><span class="scrm-provider-mark">${providerIcon(r.sink)}</span><div><strong>${TH("scrobbler.routeModal.destination")}</strong><small>${esc(label(r.sink))}</small></div></div>
          <div class="scrm-fields">
            ${fieldIcon("gps_fixed", T("scrobbler.routeModal.provider"), `<select class="input" id="scr-sink">${optionsForProviders("sink", r.sink, r.provider)}</select>`)}
            ${fieldIcon("badge", T("scrobbler.routeModal.profile"), `<select class="input" id="scr-sink-instance">${optionsForProfiles(r.sink, r.sink_instance, "sink")}</select>`)}
          </div>
        </div>
      </div>
      ${matchUsersSupported(r) ? `<div id="scr-identities" class="scrm-identities"></div>` : ""}
    </section>
  `;
}

async function addFilterEntry(id, addLabel) {
  const ta = root?.querySelector(`#${id}`);
  if (!ta) return;
  const val = ((await (window.CW?.MobileUI?.prompt || window.prompt)(T("scrobbler.routeModal.addPrompt", {what: addLabel}))) || "").trim();
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
        <button type="button" class="scwm-filter-icon material-symbols-rounded" data-clear-filter="${o.id}" title="${TH("scrobbler.routeModal.clear")}" aria-label="${TH("scrobbler.routeModal.clear")}">close</button>
      </div>
    </div>`;
}

function matchUsersSupported(r) {
  return identityMapSupported(r?.provider, r?.provider_instance, r?.sink, r?.sink_instance);
}

function mountIdentityMap(r) {
  const host = root?.querySelector("#scr-identities");
  identityMap = host ? createIdentityMap(host, {
    source: r.provider,
    sourceInstance: r.provider_instance,
    target: r.sink,
    targetInstance: r.sink_instance,
    exclude: props.mode !== "create" && r.id ? `route:${r.id}` : "",
    intro: T("scrobbler.routeModal.identityIntro", {provider: label(r.sink)}),
    note: T("scrobbler.routeModal.identityNote"),
    enabled: !!r.match_users,
    mapping: r.user_matches || {},
    onChange: (v) => {
      const d = ensureDraft();
      d.match_users = v.enabled;
      d.user_matches = v.mapping;
    },
  }) : null;
}

function filtersPanel(r, f) {
  const isPlex = r.provider === "plex";
  const isStremio = r.provider === "stremio";
  const rows = isStremio ? [] : [filterRow({ title: T("scrobbler.routeModal.userWhitelist"), subtitle: T("scrobbler.routeModal.userWhitelistHint"), id: "scr-users", value: listText(f.username_whitelist), placeholder: T("scrobbler.routeModal.oneUsername"), addLabel: T("scrobbler.routeModal.what.username"), actionAttr: "data-pick-user", actionIcon: "person_search", actionTitle: T("scrobbler.routeModal.findUsers") })];
  if (isPlex) {
    rows.push(filterRow({ title: T("scrobbler.routeModal.uuidAllow"), subtitle: T("scrobbler.routeModal.uuidAllowHint"), id: "scr-allow", value: listText(f.server_uuid_whitelist || (f.server_uuid ? [f.server_uuid] : [])), placeholder: T("scrobbler.routeModal.oneUuid"), dot: "scrm-dot-allow", addLabel: T("scrobbler.routeModal.what.uuid"), actionAttr: `data-fetch-uuid="scr-allow"`, actionIcon: "search", actionTitle: T("scrobbler.routeModal.fetchUuid") }));
    rows.push(filterRow({ title: T("scrobbler.routeModal.uuidBlock"), subtitle: T("scrobbler.routeModal.uuidBlockHint"), id: "scr-block", value: listText(f.server_uuid_blacklist), placeholder: T("scrobbler.routeModal.oneUuid"), dot: "scrm-dot-block", addLabel: T("scrobbler.routeModal.what.uuid"), actionAttr: `data-fetch-uuid="scr-block"`, actionIcon: "search", actionTitle: T("scrobbler.routeModal.fetchUuid") }));
  }
  if (!isStremio) rows.push(filterRow({ title: T("scrobbler.routeModal.ignorePaths"), subtitle: T("scrobbler.routeModal.ignorePathsHint"), id: "scr-ignore-paths", value: listText(f.ignored_path_prefixes), placeholder: T("scrobbler.routeModal.onePath"), dot: "scrm-dot-block", addLabel: T("scrobbler.routeModal.what.path") }));
  if (!isStremio) rows.push(filterRow({ title: T("scrobbler.routeModal.ignorePatterns"), subtitle: T("scrobbler.routeModal.ignorePatternsHint"), id: "scr-ignore-patterns", value: listText(f.ignored_filename_patterns), placeholder: "{edition-Trailer}", dot: "scrm-dot-block", addLabel: T("scrobbler.routeModal.what.pattern") }));
  if (!isStremio) rows.push(filterRow({ title: T("scrobbler.routeModal.ignoreEditions"), subtitle: T("scrobbler.routeModal.ignoreEditionsHint"), id: "scr-ignore-editions", value: listText(f.ignored_editions), placeholder: "Trailer", dot: "scrm-dot-block", addLabel: T("scrobbler.routeModal.what.edition") }));
  const toggles = [
    isStremio ? "" : `<label class="scrm-toggle-row"><span class="scrm-toggle-copy"><span class="material-symbols-rounded">movie_filter</span><span><strong>${TH("scrobbler.routeModal.agregarr")}</strong><small>${TH("scrobbler.routeModal.agregarrHint")}</small></span></span><span class="scrm-switch"><input type="checkbox" id="scr-ignore-agregarr" ${f.ignore_agregarr_trailers ? "checked" : ""}><span class="scrm-switch-track"></span></span></label>`,
    `<label class="scrm-toggle-row"><span class="scrm-toggle-copy"><span class="material-symbols-rounded">star</span><span><strong>${TH("scrobbler.routeModal.specials")}</strong><small>${TH("scrobbler.routeModal.specialsHint")}</small></span></span><span class="scrm-switch"><input type="checkbox" id="scr-ignore-specials" ${f.ignore_specials ? "checked" : ""}><span class="scrm-switch-track"></span></span></label>`,
  ];
  if (isPlex) toggles.push(`<label class="scrm-toggle-row"><span class="scrm-toggle-copy"><span class="material-symbols-rounded">live_tv</span><span><strong>${TH("scrobbler.routeModal.plexLive")}</strong><small>${TH("scrobbler.routeModal.plexLiveHint")}</small></span></span><span class="scrm-switch"><input type="checkbox" id="scr-live" ${f.ignore_live_tv_dvr !== false ? "checked" : ""}><span class="scrm-switch-track"></span></span></label>`);
  if (r.provider === "jellyfin" || r.provider === "emby") toggles.push(`<label class="scrm-toggle-row"><span class="scrm-toggle-copy"><span class="material-symbols-rounded">live_tv</span><span><strong>${TH("scrobbler.routeModal.live")}</strong><small>${TH("scrobbler.routeModal.liveHint")}</small></span></span><span class="scrm-switch"><input type="checkbox" id="scr-live" ${f.ignore_live_tv_dvr !== false ? "checked" : ""}><span class="scrm-switch-track"></span></span></label>`);
  return `
    <section class="scrm-panel ${activeTab === "filters" ? "active" : ""}" data-panel="filters">
      <div class="scrm-journey scrm-journey-compact">
        <span class="material-symbols-rounded scrm-journey-icon">filter_alt</span>
        <div><strong>${TH("scrobbler.routeModal.onlyWanted")}</strong><p>${isStremio ? T("scrobbler.routeModal.stremioNoFilter") : T("scrobbler.routeModal.emptyFilters")}</p></div>
        ${journeyHelp("scrobbler-filters")}
      </div>
      <div class="scrm-filter-rows">${rows.join("")}</div>
      ${toggles.join("")}
    </section>
  `;
}

function destinationKey(sink, instance) {
  return `${String(sink || "").toLowerCase()}|${normInst(instance)}`;
}

function librariesPanel(r) {
  if (!mediaSinks.includes(r.sink)) return "";
  const dest = r.options.destination || {};
  return `
    <section class="scrm-panel ${activeTab === "libraries" ? "active" : ""}" data-panel="libraries">
      <div class="scrm-journey scrm-journey-compact">
        <span class="material-symbols-rounded scrm-journey-icon">video_library</span>
        <div><strong>${TH("scrobbler.routeModal.libraries", {provider: label(r.sink)})}</strong><p>${TH("scrobbler.routeModal.librariesHint", {provider: label(r.sink)})}</p></div>
      </div>
      <div id="scr-destination-libraries"></div>
      ${["silo", "tofa"].includes(r.sink) ? "" : `<label class="scrm-toggle-row"><span class="scrm-toggle-copy"><span class="material-symbols-rounded">library_add_check</span><span><strong>${TH("scrobbler.routeModal.allCopies")}</strong><small>${TH("scrobbler.routeModal.allCopiesHint")}</small></span></span><span class="scrm-switch"><input type="checkbox" id="scr-update-all-copies" ${dest.update_all_copies === false ? "" : "checked"}><span class="scrm-switch-track"></span></span></label>`}
    </section>
  `;
}

async function fetchTofaRule(instance) {
  try {
    const res = await fetch(`/api/tofa/watched-rule?instance=${encodeURIComponent(instance || "default")}`, { cache: "no-store", credentials: "same-origin" });
    return res.ok ? await res.json() : { ok: false };
  } catch {
    return { ok: false };
  }
}

function routeWatchedThreshold() {
  const raw = root?.querySelector("#scr-watched")?.value;
  if (raw !== undefined && raw !== "") return Number(raw);
  const global = props.overview?.source_state?.watch_defaults?.watched_at;
  return Number.isFinite(Number(global)) && global !== null && global !== "" ? Number(global) : 90;
}

function paintTofaRule() {
  const host = root?.querySelector("#scr-tofa-rule");
  const data = tofaRule.data;
  if (!host || !data) return;
  if (!data.ok) {
    host.className = "scrm-note is-warn";
    host.innerHTML = `<span class="material-symbols-rounded">warning</span><span>${TH("scrobbler.routeModal.tofa.readFailed")}</span>`;
    return;
  }
  const tofa = Number(data.percent);
  const route = routeWatchedThreshold();
  const source = data.custom ? T("scrobbler.routeModal.tofa.profile") : T("scrobbler.routeModal.tofa.serverDefault");
  const credits = data.credits ? ` ${T("scrobbler.routeModal.tofa.credits")}` : "";
  let detail = T("scrobbler.routeModal.tofa.same");
  if (route < tofa) detail = T("scrobbler.routeModal.tofa.lower", {route: String(route), tofa: String(tofa)});
  else if (route > tofa) detail = T("scrobbler.routeModal.tofa.higher", {route: String(route)});
  const choices = Array.isArray(data.choices) ? data.choices.map(Number) : [];
  const apply = route !== tofa
    ? (choices.includes(route)
      ? `<button type="button" class="btn small" data-tofa-apply="${esc(route)}" ${tofaRule.busy ? "disabled" : ""}>${tofaRule.busy ? TH("scrobbler.routeModal.tofa.applying") : TH("scrobbler.routeModal.tofa.apply", {percent: String(route)})}</button>`
      : `<small>${TH("scrobbler.routeModal.tofa.offers", {choices: choices.join(", ")})}</small>`)
    : "";
  host.className = `scrm-note ${route < tofa ? "is-warn" : ""}`.trim();
  const error = tofaRule.error ? ` <strong>${esc(tofaRule.error)}</strong>` : "";
  host.innerHTML = `<span class="material-symbols-rounded">${route < tofa ? "warning" : "task_alt"}</span><span>${TH("scrobbler.routeModal.tofa.marks", {percent: String(tofa), credits, source})} ${esc(detail)}${error} ${apply}</span>`;
}

async function mountTofaRule() {
  const r = ensureDraft();
  if (!root?.querySelector("#scr-tofa-rule") || r.provider !== "tofa") return;
  const key = r.provider_instance || "default";
  if (tofaRule.key !== key) {
    tofaRule = { key, data: null, busy: false, error: "" };
    const data = await fetchTofaRule(key);
    if (tofaRule.key !== key) return;
    tofaRule.data = data;
  }
  paintTofaRule();
}

async function applyTofaRule(percent) {
  const key = tofaRule.key;
  tofaRule.busy = true;
  paintTofaRule();
  try {
    const res = await fetch(`/api/tofa/watched-rule?instance=${encodeURIComponent(key || "default")}`, {
      method: "POST", credentials: "same-origin", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ percent: Number(percent) }),
    });
    const data = res.ok ? await res.json() : { ok: false };
    if (data?.ok) {
      tofaRule.data = data;
      tofaRule.error = "";
      window.notify?.(T("scrobbler.routeModal.tofa.now", {percent: String(data.percent)}));
    } else {
      tofaRule.error = T("scrobbler.routeModal.tofa.rejected");
    }
  } catch {
    tofaRule.error = T("scrobbler.routeModal.tofa.unreachable");
  } finally {
    tofaRule.busy = false;
    paintTofaRule();
  }
}

async function fetchDestinationLibraries(sink, instance) {
  try {
    const res = await fetch(`/api/${encodeURIComponent(sink)}/libraries?instance=${encodeURIComponent(instance || "default")}`, { cache: "no-store", credentials: "same-origin" });
    const data = res.ok ? await res.json() : {};
    return Array.isArray(data?.libraries) ? data.libraries : [];
  } catch {
    return [];
  }
}

function mountDestinationTable() {
  const host = root?.querySelector("#scr-destination-libraries");
  const r = ensureDraft();
  if (!host || !window.cwWhitelistTable || !mediaSinks.includes(r.sink)) return;
  const key = destinationKey(r.sink, r.sink_instance);
  const selected = () => new Set((ensureDraft().options.destination?.libraries || []).map(String));
  window.cwWhitelistTable.mount({
    host,
    features: [{ key: "scr", label: T("scrobbler.routeModal.scrobble"), icon: "sensors", title: T("scrobbler.routeModal.libScopeHint") }],
    note: T("scrobbler.routeModal.libEmpty"),
    getLibs: () => destinationLibraries.get(key) || [],
    isOn: (_feature, id) => selected().has(String(id)),
    selectedIds: () => [...selected()],
    setOn: (_feature, id, on) => {
      const next = selected();
      if (on) next.add(String(id));
      else next.delete(String(id));
      const current = ensureDraft();
      current.options.destination = { ...(current.options.destination || {}), libraries: [...next] };
    },
    load: async () => { destinationLibraries.set(key, await fetchDestinationLibraries(r.sink, r.sink_instance)); },
  });
}

function anilistMappingHint(animeGlobalOn) {
  return animeGlobalOn
    ? `<div class="scrm-journey scrm-journey-compact"><span class="material-symbols-rounded scrm-journey-icon">animation</span><div><strong>${TH("scrobbler.routeModal.anilistUses")}</strong><p>${TH("scrobbler.routeModal.anilistUsesHint")}</p></div></div>`
    : `<div class="scrm-journey scrm-journey-compact"><span class="material-symbols-rounded scrm-journey-icon">warning</span><div><strong>${TH("scrobbler.routeModal.anilistNeeds")}</strong><p>${TH("scrobbler.routeModal.anilistNeedsHint")}</p></div></div>`;
}

function optionsPanel(r) {
  const unresolvedFallback = r.provider === "plex"
    ? `<label class="scrm-toggle-row"><span class="scrm-toggle-copy"><span class="material-symbols-rounded">person_alert</span><span><strong>${TH("scrobbler.routeModal.userFallback")}</strong><small>${TH("scrobbler.routeModal.userFallbackHint")}</small></span></span><span class="scrm-switch"><input type="checkbox" id="scr-unresolved-user-fallback" ${r.options.watch?.unresolved_user_fallback ? "checked" : ""}><span class="scrm-switch-track"></span></span></label>`
    : "";
  const pkcSupport = r.provider === "plex"
    ? `<label class="scrm-toggle-row"><span class="scrm-toggle-copy"><span class="material-symbols-rounded">connected_tv</span><span><strong>${TH("scrobbler.routeModal.pkc")}</strong><small>${TH("scrobbler.routeModal.pkcHint")}</small></span></span><span class="scrm-switch"><input type="checkbox" id="scr-plexkodiconnect-support" ${r.options.watch?.plexkodiconnect_support === true ? "checked" : ""}><span class="scrm-switch-track"></span></span></label>`
    : "";
  const manualMarks = ["plex", "jellyfin", "emby", "silo", "tofa"].includes(r.provider)
    ? `<label class="scrm-toggle-row"><span class="scrm-toggle-copy"><span class="material-symbols-rounded">task_alt</span><span><strong>${TH("scrobbler.routeModal.manualMarks")}</strong><small>${TH("scrobbler.routeModal.manualMarksHint")}</small></span></span><span class="scrm-switch"><input type="checkbox" id="scr-manual-marks" ${r.options.watch?.manual_marks !== false ? "checked" : ""}><span class="scrm-switch-track"></span></span></label>`
    : "";
  const animeGlobalOn = globalAnimeMappingEnabled();
  const animeMapping = ["crosswatch", "simkl"].includes(r.sink)
    ? `<label class="scrm-toggle-row ${animeGlobalOn ? "" : "is-disabled"}"><span class="scrm-toggle-copy"><span class="material-symbols-rounded">animation</span><span><strong>${TH("scrobbler.routeModal.animeMapping")}</strong><small>${animeGlobalOn ? T("scrobbler.routeModal.animeMappingHint") : T("scrobbler.routeModal.enableAnimeFirst")}</small></span></span><span class="scrm-switch"><input type="checkbox" id="scr-anime-mapping" ${r.options.watch?.anime_mapping ? "checked" : ""} ${animeGlobalOn ? "" : "disabled"}><span class="scrm-switch-track"></span></span></label>`
    : "";
  const anilistHint = r.sink === "anilist" ? anilistMappingHint(animeGlobalOn) : "";
  return `
    <section class="scrm-panel ${activeTab === "options" ? "active" : ""}" data-panel="options">
      ${anilistHint}
      <div class="scrm-journey scrm-journey-compact">
        <span class="material-symbols-rounded scrm-journey-icon">tune</span>
        <div><strong>${TH("scrobbler.routeModal.routeLevel")}</strong><p>${TH("scrobbler.routeModal.routeLevelHint")}<br><b>${TH("scrobbler.routeModal.leaveDefaults")}</b></p></div>
      </div>
      <div class="scrm-fields three">
        ${field(T("scrobbler.routeModal.autoRemove"), `<select class="input" id="scr-auto"><option value="inherit" ${r.options.auto_remove_watchlist === "inherit" ? "selected" : ""}>${TH("scrobbler.routeModal.inherit")}</option><option value="on" ${r.options.auto_remove_watchlist === "on" ? "selected" : ""}>${TH("scrobbler.routeModal.on")}</option><option value="off" ${r.options.auto_remove_watchlist === "off" ? "selected" : ""}>${TH("scrobbler.routeModal.off")}</option></select>`, "", T("scrobbler.routeModal.autoRemoveHint"))}
        ${field(T("scrobbler.routeModal.watched"), `<input class="input" id="scr-watched" type="number" min="0" max="100" value="${esc(r.options.scrobble?.watched_at ?? "")}" placeholder="${TH("scrobbler.routeModal.global")}">`, "", T("scrobbler.routeModal.watchedHint"))}
        ${field(T("scrobbler.routeModal.finalStop"), `<input class="input" id="scr-force" type="number" min="0" max="100" value="${esc(r.options.scrobble?.force_stop_at ?? "")}" placeholder="${TH("scrobbler.routeModal.global")}">`, "", T("scrobbler.routeModal.finalStopHint"))}
        ${field(T("scrobbler.routeModal.progressStep"), `<input class="input" id="scr-progress-step" type="number" min="15" max="50" value="${esc(r.options.scrobble?.progress_step ?? "")}" placeholder="${TH("scrobbler.routeModal.global")}">`, "", T("scrobbler.routeModal.progressStepHint"))}
        ${field(T("scrobbler.routeModal.pauseDebounce"), `<input class="input" id="scr-watch-pause" type="number" min="0" max="3600" value="${esc(r.options.watch?.pause_debounce_seconds ?? "")}" placeholder="${TH("scrobbler.routeModal.global")}">`, "", T("scrobbler.routeModal.pauseDebounceHint"))}
        ${field(T("scrobbler.routeModal.suppressStart"), `<input class="input" id="scr-watch-suppress" type="number" min="0" max="100" value="${esc(r.options.watch?.suppress_start_at ?? "")}" placeholder="${TH("scrobbler.routeModal.global")}">`, "", T("scrobbler.routeModal.suppressStartHint"))}
        ${["emby", "jellyfin", "kodi", "scrob", "silo"].includes(r.provider) ? field(T("scrobbler.routeModal.poll"), `<input class="input" id="scr-watch-poll" type="number" min="5" max="60" value="${esc(r.options.watch?.poll_seconds ?? "")}" placeholder="10">`, "", T("scrobbler.routeModal.pollHint")) : ""}
        ${["emby", "jellyfin", "kodi", "scrob", "silo"].includes(r.provider) ? field(T("scrobbler.routeModal.idlePoll"), `<input class="input" id="scr-watch-idle-poll" type="number" min="5" max="120" value="${esc(r.options.watch?.idle_poll_seconds ?? "")}" placeholder="30">`, "", T("scrobbler.routeModal.idlePollHint")) : ""}
      </div>
      ${r.provider === "tofa" ? `<div class="scrm-note" id="scr-tofa-rule"><span class="material-symbols-rounded">sync</span><span>${TH("scrobbler.routeModal.tofa.reading")}</span></div>` : ""}
      ${unresolvedFallback}
      ${pkcSupport}
      ${manualMarks}
      ${animeMapping}
      ${r.sink === "simkl" ? `<label class="scrm-toggle-row"><span class="scrm-toggle-copy"><span class="material-symbols-rounded">replay</span><span><strong>${TH("scrobbler.routeModal.trackRewatches")}</strong><small>${TH("scrobbler.routeModal.simklRewatchHint")}</small></span></span><span class="scrm-switch"><input type="checkbox" id="scr-simkl-rewatches" ${r.options.watch?.simkl_rewatches === true ? "checked" : ""}><span class="scrm-switch-track"></span></span></label>` : ""}
      ${r.sink === "crosswatch" ? `<label class="scrm-toggle-row"><span class="scrm-toggle-copy"><span class="material-symbols-rounded">replay</span><span><strong>${TH("scrobbler.routeModal.trackRewatches")}</strong><small>${TH("scrobbler.routeModal.cwRewatchHint")}</small></span></span><span class="scrm-switch"><input type="checkbox" id="scr-cw-rewatches" ${r.options.watch?.crosswatch_rewatches === true ? "checked" : ""}><span class="scrm-switch-track"></span></span></label>
      <div class="scrm-fields two">${field(T("scrobbler.routeModal.rewatchWindow"), `<input class="input" id="scr-cw-rewatch-window" type="number" min="1" max="168" value="${esc(r.options.watch?.crosswatch_rewatch_window_hours ?? "")}" placeholder="12">`, "", T("scrobbler.routeModal.rewatchWindowHint"))}</div>` : ""}
    </section>
  `;
}

function globalRatingTargets() {
  const g = props.overview?.source_state?.global_plex_ratings || {};
  return Object.keys(g).filter((s) => g[s] === true);
}

function ratingsPanel(r, ratings, ratingTargets) {
  if (r.provider !== "plex") return "";
  const ratingSinkList = ratingSinkProviders(ratingTargets, r.provider)
    .sort((a, b) => label(a).localeCompare(label(b), undefined, { sensitivity: "base", numeric: true }));
  const isOff = ratings.mode !== "custom";
  const globalTargets = globalRatingTargets();
  const globalWarn = globalTargets.length
    ? `<div class="scrm-note is-warn"><span class="material-symbols-rounded">warning</span><span>${TH("scrobbler.routeModal.globalRatings", {targets: globalTargets.map(label).join(", ")})}</span></div>`
    : "";
  const targets = ratingSinkList.length
    ? ratingSinkList.map((sink) => {
        const configured = allSinkProfiles(sink).length > 0;
        return `<label class="scrm-target${configured ? "" : " is-disabled"}"><input type="checkbox" data-rating-target="${sink}" ${ratingTargets.has(sink) ? "checked" : ""} ${configured && !isOff ? "" : "disabled"}><span class="scrm-target-mark">${providerIcon(sink)}</span><span>${label(sink)}${configured ? "" : " · not configured"}</span></label>`;
      }).join("")
    : `<span class="scrm-muted">${TH("scrobbler.routeModal.noRatingDest")}</span>`;
  const endpoint = r.ratings_webhook_url
    ? `<div class="scrm-endpoint"><input type="text" readonly value="${esc(r.ratings_webhook_url)}" title="${esc(r.ratings_webhook_url)}" aria-label="${TH("scrobbler.routeModal.ratingsUrl")}" spellcheck="false" data-copy-field><button type="button" class="btn small material-symbols-rounded" data-copy="${esc(r.ratings_webhook_url)}" title="${TH("scrobbler.routeModal.copyUrl")}" aria-label="${TH("scrobbler.routeModal.copyUrl")}">content_copy</button><button type="button" class="btn small danger material-symbols-rounded" data-regen-route title="${TH("scrobbler.routeModal.regenUrl")}" aria-label="${TH("scrobbler.routeModal.regenUrl")}">sync_lock</button></div>`
    : `<div class="scrm-note"><span class="material-symbols-rounded">link</span><span>${TH("scrobbler.routeModal.saveFirst")}</span></div>`;
  return `
    <section class="scrm-panel ${activeTab === "ratings" ? "active" : ""}" data-panel="ratings">
      <div class="scrm-journey scrm-journey-compact">
        <span class="material-symbols-rounded scrm-journey-icon">star</span>
        <div><strong>${TH("scrobbler.routeModal.plexRatings")}</strong><p>${TH("scrobbler.routeModal.plexRatingsHint")}</p></div>
      </div>
      ${globalWarn}
      <div class="scrm-filter-group">
        <div class="scrm-filter-head"><span class="material-symbols-rounded">star</span><div><strong>${TH("scrobbler.routeModal.routeRatings")}</strong><small>${TH("scrobbler.routeModal.routeRatingsHint")}</small></div></div>
        ${field(T("scrobbler.routeModal.ratingsMode"), `<select class="input" id="scr-ratings-mode"><option value="off" ${isOff ? "selected" : ""}>${TH("scrobbler.routeModal.off")}</option><option value="custom" ${!isOff ? "selected" : ""}>${TH("scrobbler.routeModal.customWebhook")}</option></select>`)}
        <div class="scrm-field"><span>${TH("scrobbler.routeModal.destinations")}</span><div class="scrm-targets${isOff ? " is-muted" : ""}">${targets}</div></div>
      </div>
      ${isOff ? "" : endpoint}
    </section>
  `;
}

function enableSelectedUnavailableRatingTargets() {
  root?.querySelectorAll(".scrm-target.is-disabled input[data-rating-target]:checked").forEach((input) => {
    input.disabled = false;
    const target = input.closest(".scrm-target");
    target?.classList.remove("is-disabled");
    target?.classList.add("is-unconfigured");
  });
}

function normalizeActiveTab(tab = activeTab) {
  const next = String(tab || "route");
  return ["route", "filters", "options", "libraries", "ratings"].includes(next) ? next : "route";
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
  window.CW?.MobileUI?.sectionPicker(root?.querySelector(".scrm-nav"));
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
  if (!mediaSinks.includes(r.sink) && activeTab === "libraries") activeTab = "route";
  const f = r.filters || {};
  const ratings = r.options?.ratings || {};
  const ratingTargets = new Set(ratings.targets || []);
  const dup = props.mode !== "delete" && duplicateRoute(r);
  root.innerHTML = `
    <div ${modalAttrs(r.provider)}>
      <div class="cx-head scrm-head">
        <span class="scrm-head-logo">${providerIcon(r.provider)}</span>
        <div class="scrm-head-copy"><strong>${esc(routeTitle(r))}</strong><span>${esc(label(r.provider))} ${esc(profileLabel(r.provider, r.provider_instance, "source"))} to ${esc(label(r.sink))} ${esc(profileLabel(r.sink, r.sink_instance, "sink"))}</span></div>
        <button type="button" class="scrm-close" data-close aria-label="${TH("scrobbler.routeModal.close")}"><span class="material-symbols-rounded">close</span></button>
      </div>
      <div class="scrm-body">
        <div class="scrm-panel-shell" data-scrm-provider="${esc(r.provider)}">
          <nav class="cw-subtiles scrm-nav" aria-label="${TH("scrobbler.routeModal.sections")}">
            ${navButton("route", "route", T("scrobbler.routeModal.nav.route"), T("scrobbler.routeModal.nav.routeHint"))}
            ${navButton("filters", "filter_alt", T("scrobbler.routeModal.nav.filters"), T("scrobbler.routeModal.nav.filtersHint"))}
            ${navButton("options", "tune", T("scrobbler.routeModal.nav.options"), T("scrobbler.routeModal.nav.optionsHint"))}
            ${mediaSinks.includes(r.sink) ? navButton("libraries", "video_library", T("scrobbler.routeModal.nav.libraries"), T("scrobbler.routeModal.nav.librariesHint")) : ""}
            ${r.provider === "plex" ? navButton("ratings", "star", T("scrobbler.routeModal.nav.ratings"), T("scrobbler.routeModal.nav.ratingsHint")) : ""}
          </nav>
          <div class="scrm-content">
            ${routePanel(r)}
            ${filtersPanel(r, f)}
            ${optionsPanel(r)}
            ${librariesPanel(r)}
            ${ratingsPanel(r, ratings, ratingTargets)}
          </div>
          <div class="cw-connection-modal-footer scrm-footer">
            ${props.mode !== "create" ? `<button type="button" class="btn danger cw-connection-footer-delete cw-danger-confirm" data-delete><span class="material-symbols-rounded">delete</span><span>${TH("scrobbler.routeModal.deleteRoute")}</span></button>` : `<span></span>`}
            ${errors.length
              ? `<span class="scrm-footer-error" role="alert" aria-live="polite"><span class="material-symbols-rounded">warning</span><span class="scrm-footer-error-text">${errors.map((x) => esc(x.message || x.code || x.field)).join(" ")}</span></span>`
              : dup ? `<span class="scrm-footer-warn"><span class="material-symbols-rounded">warning</span>${TH("scrobbler.routeModal.exists")}</span>` : `<span></span>`}
            <button type="button" class="btn cw-connection-footer-cancel" data-close>${TH("scrobbler.routeModal.cancel")}</button>
            ${props.mode !== "delete" ? `<button type="button" class="btn primary cw-connection-footer-save" data-save ${dup ? "disabled" : ""}>${saving ? T("scrobbler.routeModal.saving") : T("scrobbler.routeModal.saveChanges")}</button>` : ""}
          </div>
        </div>
      </div>
    </div>
  `;
  ensureVisiblePanel();
  enableSelectedUnavailableRatingTargets();
  mountDestinationTable();
  mountIdentityMap(r);
  void mountTofaRule();
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
    ignore_agregarr_trailers: !!root.querySelector("#scr-ignore-agregarr")?.checked,
    ignore_specials: !!root.querySelector("#scr-ignore-specials")?.checked,
    ignored_path_prefixes: split(root.querySelector("#scr-ignore-paths")?.value),
    ignored_filename_patterns: split(root.querySelector("#scr-ignore-patterns")?.value),
    ignored_editions: split(root.querySelector("#scr-ignore-editions")?.value),
  };
  if (provider === "plex" || provider === "jellyfin" || provider === "emby") filters.ignore_live_tv_dvr = !!root.querySelector("#scr-live")?.checked;
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
  const poll = root.querySelector("#scr-watch-poll")?.value ?? "";
  if (poll !== "") watch.poll_seconds = Number(poll);
  const idlePoll = root.querySelector("#scr-watch-idle-poll")?.value ?? "";
  if (idlePoll !== "") watch.idle_poll_seconds = Number(idlePoll);
  if (provider === "plex") watch.unresolved_user_fallback = !!root.querySelector("#scr-unresolved-user-fallback")?.checked;
  if (provider === "plex") watch.plexkodiconnect_support = !!root.querySelector("#scr-plexkodiconnect-support")?.checked;
  const manualMarksEl = root.querySelector("#scr-manual-marks");
  if (manualMarksEl && ["plex", "jellyfin", "emby", "silo", "tofa"].includes(provider)) watch.manual_marks = !!manualMarksEl.checked;
  if (["crosswatch", "simkl"].includes(String(root.querySelector("#scr-sink")?.value || draft.sink || "").toLowerCase())) watch.anime_mapping = !!root.querySelector("#scr-anime-mapping")?.checked;
  if (String(root.querySelector("#scr-sink")?.value || draft.sink || "").toLowerCase() === "simkl") watch.simkl_rewatches = !!root.querySelector("#scr-simkl-rewatches")?.checked;
  if (String(root.querySelector("#scr-sink")?.value || draft.sink || "").toLowerCase() === "crosswatch") {
    watch.crosswatch_rewatches = !!root.querySelector("#scr-cw-rewatches")?.checked;
    const rewatchWindow = root.querySelector("#scr-cw-rewatch-window")?.value ?? "";
    if (rewatchWindow !== "") watch.crosswatch_rewatch_window_hours = Number(rewatchWindow);
  }
  const ratingsMode = root.querySelector("#scr-ratings-mode")?.value || "off";
  const sinkValue = String(sinkEl?.value || draft.sink || "").toLowerCase();
  const previousDestination = draft?.options?.destination || {};
  const copiesEl = root.querySelector("#scr-update-all-copies");
  const destination = mediaSinks.includes(sinkValue) ? {
    libraries: [...(previousDestination.libraries || [])],
    update_all_copies: copiesEl ? !!copiesEl.checked : previousDestination.update_all_copies !== false,
  } : null;
  return {
    id: draft.id,
    enabled: props.mode === "create" ? true : draft.enabled !== false,
    provider,
    provider_instance: providerInstanceEl ? providerInstanceEl.value : draft.provider_instance || "",
    sink: sinkEl ? sinkEl.value : draft.sink || "",
    sink_instance: sinkInstanceEl ? sinkInstanceEl.value : draft.sink_instance || "",
    profile_id: selectedUserProfileId || draft.profile_id || "",
    match_users: !!identityMap?.value.enabled,
    user_matches: identityMap?.value.enabled ? identityMap.value.mapping : {},
    filters,
    options: {
      auto_remove_watchlist: root.querySelector("#scr-auto")?.value || "inherit",
      scrobble,
      watch,
      ...(destination ? { destination } : {}),
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
  if (draft.provider === draft.sink && normInst(draft.provider_instance) === normInst(draft.sink_instance)) {
    render([{ message: T("scrobbler.routeModal.different") }]);
    return;
  }
  if (duplicateRoute(ensureDraft())) {
    render();
    return;
  }
  busy(true);
  try {
    const body = collect();
    if (props.mode === "create") delete body.id;
    if (regenerate) body.regenerate_ratings_webhook = true;
    const data = props.mode === "create"
      ? await request("/api/scrobbler/routes", "POST", body)
      : await request(`/api/scrobbler/routes/${encodeURIComponent(draft.id)}`, "PUT", body);
    props.onSaved?.(data);
    if (!regenerate) {
      window.cxCloseModal?.();
      return;
    }
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
  const label = btn.textContent || T("scrobbler.routeModal.saveChanges");
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
  btn.innerHTML = `<span class="material-symbols-rounded">delete</span><span>${TH("scrobbler.routeModal.deleteRoute")}</span>`;
}

function armDeleteConfirm(btn) {
  confirmDelete = true;
  if (!btn) return;
  btn.classList.add("is-confirming");
  btn.innerHTML = `<span class="material-symbols-rounded">warning</span><span>${TH("scrobbler.routeModal.confirmDelete")}</span>`;
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
  selectedUserProfileId = "";
  destinationLibraries.clear();
  modalKey = String(props.mode === "create" ? "__new__" : (props.route?.id || "__new__"));
  activeTab = normalizeActiveTab("route");
  if (root) root.dataset.scrmTab = activeTab;
  await loadUserProfiles();
  selectedUserProfileId = String(props.route?.profile_id || "");
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
    if (e.target.closest("[data-tofa-apply]")) {
      e.preventDefault();
      await applyTofaRule(e.target.closest("[data-tofa-apply]").dataset.tofaApply);
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
      if (await window.cwCopyText?.(copy.dataset.copy)) flashCopied(copy);
      else window.cwSelectCopyField?.(copy.parentElement?.querySelector("[data-copy-field]"));
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
  const resetDestinationLibraries = (beforeKey) => {
    if (destinationKey(draft.sink, draft.sink_instance) === beforeKey) return;
    draft.options.destination = { ...(draft.options.destination || {}), libraries: [] };
  };
  changeHandler = (e) => {
    if (e.target.id === "scr-watched") paintTofaRule();
    if (["scr-provider", "scr-sink"].includes(e.target.id)) {
      const keepTab = currentActiveTab();
      const beforeKey = destinationKey(ensureDraft().sink, ensureDraft().sink_instance);
      syncDraftFromDom();
      if (e.target.id === "scr-provider") {
        draft.provider_instance = allSourceProfiles(draft.provider)[0]?.instance || "";
        if (!allSinkProfiles(draft.sink).length) draft.sink = sinkProviders("", draft.provider)[0] || "";
        draft.sink_instance = allSinkProfiles(draft.sink)[0]?.instance || "";
        const keptTargets = (draft.options.ratings?.targets || []).filter((t) => t !== draft.provider);
        if (draft.options.ratings) draft.options.ratings.targets = keptTargets;
        if (draft.provider !== "plex") draft.options.ratings = { mode: "off", targets: [] };
      }
      if (e.target.id === "scr-sink") {
        draft.sink_instance = allSinkProfiles(draft.sink)[0]?.instance || "";
      }
      resetDestinationLibraries(beforeKey);
      render();
      preserveVisiblePanel(keepTab);
      return;
    }
    if (["scr-provider-instance", "scr-sink-instance", "scr-ratings-mode"].includes(e.target.id)) {
      const keepTab = currentActiveTab();
      const beforeKey = destinationKey(ensureDraft().sink, ensureDraft().sink_instance);
      syncDraftFromDom();
      if (e.target.id === "scr-provider-instance" && draft.provider === draft.sink) {
        if (!allSinkProfiles(draft.sink).length) draft.sink = sinkProviders("", draft.provider)[0] || "";
        if (!allSinkProfiles(draft.sink).some((p) => normInst(p.instance) === normInst(draft.sink_instance))) draft.sink_instance = allSinkProfiles(draft.sink)[0]?.instance || "";
      }
      resetDestinationLibraries(beforeKey);
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
  userProfiles = [];
  selectedUserProfileId = "";
  activeTab = "route";
  tofaRule = { key: "", data: null, busy: false };
}

export default { mount, unmount };
