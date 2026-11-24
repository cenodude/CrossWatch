/* assets/js/modals/core/identity-map.js */
/* Identity mapping card shared by sync pairs, watcher routes and webhooks. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
const IDENTITY_PROVIDERS = new Set(["plex", "emby", "jellyfin", "silo", "tofa"]);
const NAMES = { plex: "Plex", emby: "Emby", jellyfin: "Jellyfin", silo: "Silo", tofa: "Tofa" };

let stylesReady = null;

function ensureStyles() {
  const id = "cw-identity-map-css";
  if (document.getElementById(id)) return stylesReady || Promise.resolve();
  const cssUrl = new URL("./identity-map.css", import.meta.url);
  const version = new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__;
  if (version) cssUrl.searchParams.set("v", version);
  const link = document.createElement("link");
  link.id = id;
  link.rel = "stylesheet";
  link.href = cssUrl.href;
  stylesReady = new Promise((resolve) => {
    link.addEventListener("load", resolve, { once: true });
    link.addEventListener("error", resolve, { once: true });
  });
  document.head.appendChild(link);
  return stylesReady;
}

const txt = (v) => String(v ?? "").trim();
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const low = (v) => txt(v).toLowerCase();
const inst = (v) => txt(v) || "default";

export function identityMapSupported(source, sourceInstance, target, targetInstance) {
  const a = low(source), b = low(target);
  if (!IDENTITY_PROVIDERS.has(a) || !IDENTITY_PROVIDERS.has(b)) return false;
  return !(a === b && inst(sourceInstance) === inst(targetInstance));
}

function providerName(p) {
  return window.CW?.ProviderMeta?.label?.(p) || NAMES[low(p)] || txt(p);
}

function logo(p) {
  const src = window.CW?.ProviderMeta?.logoPath?.(low(p));
  return src
    ? `<img class="cw-idm-logo" src="${esc(src)}" alt="" width="20" height="20" loading="lazy">`
    : `<span class="cw-idm-logo is-text" aria-hidden="true">${esc(providerName(p).charAt(0))}</span>`;
}

function cleanMap(raw) {
  const out = {};
  if (!raw || typeof raw !== "object") return out;
  for (const [k, v] of Object.entries(raw)) {
    const a = txt(k), b = txt(v);
    if (a && b) out[a] = b;
  }
  return out;
}

export function createIdentityMap(host, opts = {}) {
  if (!host) return null;
  ensureStyles();
  const state = {
    source: low(opts.source),
    sourceInstance: inst(opts.sourceInstance),
    target: low(opts.target),
    targetInstance: inst(opts.targetInstance),
    exclude: txt(opts.exclude),
    title: txt(opts.title) || T("pairs.identities.title"),
    intro: txt(opts.intro) || T("pairs.identities.intro"),
    note: txt(opts.note),
    enabled: !!opts.enabled,
    mapping: cleanMap(opts.mapping),
    loaded: false,
    error: "",
    src: [],
    dst: [],
    suggestions: {},
    flash: "",
    token: 0,
  };

  const notify = () => { try { opts.onChange?.(api.value); } catch {} };

  function dstName(key) {
    return state.dst.find((d) => d.key === key)?.username || "";
  }

  function srcName(key) {
    return state.src.find((s) => s.key === key)?.username || "";
  }

  function takenBy(dstKey, exceptSrc) {
    const owner = Object.entries(state.mapping).find(([s, d]) => d === dstKey && s !== exceptSrc && state.src.some((x) => x.key === s));
    return owner ? owner[0] : "";
  }

  function suggestionFor(srcKey) {
    const sug = state.suggestions[srcKey];
    if (!sug || !state.dst.some((d) => d.key === sug.key)) return null;
    return sug;
  }

  function reasonText(sug) {
    if (!sug) return "";
    return sug.reason === "linked" && sug.where ? T("pairs.identities.alsoIn", {where: sug.where}) : T("pairs.identities.sameName");
  }

  function statusHTML(srcKey) {
    const picked = state.mapping[srcKey];
    const sug = suggestionFor(srcKey);
    if (picked && state.dst.some((d) => d.key === picked)) {
      const text = sug && sug.key === picked ? reasonText(sug) : T("pairs.identities.linked");
      return `<span class="cw-idm-chip is-on"><span class="material-symbols-rounded" aria-hidden="true">link</span>${esc(text)}</span>`;
    }
    if (sug && !takenBy(sug.key, srcKey)) {
      return `<button type="button" class="cw-idm-chip is-suggest" data-idm-use="${esc(srcKey)}" title="${TH("pairs.identities.suggested", {reason: reasonText(sug).toLowerCase()})}"><span class="material-symbols-rounded" aria-hidden="true">auto_fix_high</span>${TH("pairs.identities.use", {name: dstName(sug.key), reason: sug.reason === "linked" && sug.where ? T("pairs.identities.asIn", {where: sug.where}) : T("pairs.identities.sameNameLower")})}</button>`;
    }
    return `<span class="cw-idm-chip">${TH("pairs.identities.notSynced")}</span>`;
  }

  function rowHTML(s) {
    const picked = state.dst.some((d) => d.key === state.mapping[s.key]) ? state.mapping[s.key] : "";
    const options = [`<option value="" ${picked ? "" : "selected"}>${TH("pairs.identities.dontSync")}</option>`].concat(state.dst.map((d) => {
      const owner = takenBy(d.key, s.key);
      const label = owner ? T("pairs.identities.usedBy", {name: d.username, owner: srcName(owner)}) : d.username;
      return `<option value="${esc(d.key)}" ${picked === d.key ? "selected" : ""} ${owner ? "disabled" : ""}>${esc(label)}</option>`;
    })).join("");
    return `
      <div class="cw-idm-row ${picked ? "is-mapped" : ""}">
        <span class="cw-idm-person">${logo(state.source)}<span class="cw-idm-name" title="${esc(s.username)}">${esc(s.username)}</span></span>
        <span class="material-symbols-rounded cw-idm-arrow" aria-hidden="true">arrow_forward</span>
        <span class="cw-idm-pick">${logo(state.target)}<select class="cw-idm-select" data-idm-src="${esc(s.key)}" aria-label="${TH("pairs.identities.identityFor", {provider: providerName(state.target), name: s.username})}">${options}</select></span>
        <span class="cw-idm-status">${statusHTML(s.key)}</span>
      </div>`;
  }

  function openableSuggestions() {
    return state.src.filter((s) => !state.dst.some((d) => d.key === state.mapping[s.key])).map((s) => [s.key, suggestionFor(s.key)]).filter(([key, sug]) => sug && !takenBy(sug.key, key));
  }

  function emptyHTML() {
    const missing = [];
    if (!state.src.length) missing.push(state.source);
    if (!state.dst.length) missing.push(state.target);
    const names = missing.map((p) => `<strong>${esc(providerName(p))}</strong>`).join(` ${TH("pairs.identities.and")} `);
    return `<div class="cw-idm-empty"><span class="material-symbols-rounded" aria-hidden="true">person_search</span><div><strong>${TH("pairs.identities.noneOn", {names: missing.map((p) => providerName(p)).join(` ${T("pairs.identities.or")} `)})}</strong><p>${TH("pairs.identities.openConnections1")} ${names}${TH("pairs.identities.openConnections2")} <strong>${TH("pairs.identities.title")}</strong>. ${TH("pairs.identities.openConnections3")}</p></div></div>`;
  }

  function bodyHTML() {
    if (!state.enabled) return "";
    if (!state.loaded) return `<div class="cw-idm-body"><div class="cw-idm-muted">${state.error ? esc(state.error) : TH("pairs.identities.loading")}</div></div>`;
    if (!state.src.length || !state.dst.length) return `<div class="cw-idm-body">${emptyHTML()}</div>`;
    const mapped = state.src.filter((s) => state.mapping[s.key] && state.dst.some((d) => d.key === state.mapping[s.key])).length;
    const open = openableSuggestions();
    const stale = Object.entries(state.mapping).filter(([s, d]) => !state.src.some((x) => x.key === s) || !state.dst.some((x) => x.key === d)).length;
    return `
      <div class="cw-idm-body">
        <div class="cw-idm-rows">${state.src.map(rowHTML).join("")}</div>
        <div class="cw-idm-foot">
          <span class="cw-idm-count">${mapped ? TH("pairs.identities.syncing", {mapped, count: state.src.length}) : TH("pairs.identities.nobody")}</span>
          ${open.length ? `<button type="button" class="cw-idm-btn" data-idm-use-all><span class="material-symbols-rounded" aria-hidden="true">auto_fix_high</span>${TH("pairs.identities.useSuggestions", {count: open.length})}</button>` : ""}
        </div>
        ${state.flash ? `<div class="cw-idm-flash" role="status">${esc(state.flash)}</div>` : ""}
        ${stale ? `<div class="cw-idm-warn"><span class="material-symbols-rounded" aria-hidden="true">warning</span>${TH("pairs.identities.stale", {count: stale})}</div>` : ""}
        <p class="cw-idm-hint">${TH("pairs.identities.hint", {source: providerName(state.source), target: providerName(state.target)})}${state.note ? ` ${esc(state.note)}` : ""}</p>
      </div>`;
  }

  function render() {
    host.innerHTML = `
      <section class="cw-idm ${state.enabled ? "is-enabled" : ""}">
        <label class="cw-idm-top">
          <span class="cw-idm-icon material-symbols-rounded" aria-hidden="true">group</span>
          <span class="cw-idm-copy"><strong>${esc(state.title)}</strong><small>${esc(state.intro)}</small></span>
          <span class="cw-idm-switch"><input type="checkbox" data-idm-toggle ${state.enabled ? "checked" : ""} aria-label="${esc(state.title)}"><span class="cw-idm-track"></span></span>
        </label>
        ${bodyHTML()}
      </section>`;
  }

  function applySuggestions() {
    let count = 0;
    for (const [key, sug] of openableSuggestions()) {
      if (takenBy(sug.key, key)) continue;
      state.mapping[key] = sug.key;
      count += 1;
    }
    return count;
  }

  async function load() {
    const token = ++state.token;
    state.loaded = false;
    state.error = "";
    render();
    const q = new URLSearchParams({ source: state.source, source_instance: state.sourceInstance, target: state.target, target_instance: state.targetInstance });
    if (state.exclude) q.set("exclude", state.exclude);
    try {
      const r = await fetch(`/api/identities/suggestions?${q}`, { cache: "no-store", credentials: "same-origin" });
      const data = await r.json();
      if (!r.ok || data?.ok === false) throw new Error(data?.error || `HTTP ${r.status}`);
      if (token !== state.token) return;
      state.src = Array.isArray(data?.source?.identities) ? data.source.identities : [];
      state.dst = Array.isArray(data?.target?.identities) ? data.target.identities : [];
      state.suggestions = data?.suggestions && typeof data.suggestions === "object" ? data.suggestions : {};
      state.loaded = true;
    } catch {
      if (token !== state.token) return;
      state.error = T("pairs.identities.loadFailed");
    }
    if (state.loaded && state.enabled && !Object.keys(state.mapping).length) {
      const n = applySuggestions();
      if (n) {
        state.flash = T("pairs.identities.autoLinked", {count: n});
        notify();
      }
    }
    render();
  }

  host.addEventListener("change", (ev) => {
    const toggle = ev.target.closest?.("[data-idm-toggle]");
    if (toggle) {
      state.enabled = toggle.checked;
      state.flash = "";
      if (state.enabled && !state.loaded) { load(); notify(); return; }
      if (state.enabled && !Object.keys(state.mapping).length) {
        const n = applySuggestions();
        if (n) state.flash = T("pairs.identities.autoLinked", {count: n});
      }
      render();
      notify();
      return;
    }
    const sel = ev.target.closest?.("[data-idm-src]");
    if (sel) {
      const key = sel.dataset.idmSrc;
      if (sel.value) state.mapping[key] = sel.value;
      else delete state.mapping[key];
      state.flash = "";
      render();
      notify();
    }
  });

  host.addEventListener("click", (ev) => {
    const one = ev.target.closest?.("[data-idm-use]");
    if (one) {
      ev.preventDefault();
      const key = one.dataset.idmUse;
      const sug = suggestionFor(key);
      if (sug && !takenBy(sug.key, key)) state.mapping[key] = sug.key;
      state.flash = "";
      render();
      notify();
      return;
    }
    if (ev.target.closest?.("[data-idm-use-all]")) {
      ev.preventDefault();
      const n = applySuggestions();
      state.flash = n ? T("pairs.identities.linkedMore", {count: n}) : "";
      render();
      notify();
    }
  });

  const api = {
    get value() {
      if (!state.loaded) return { enabled: state.enabled, mapping: { ...state.mapping } };
      const mapping = {};
      for (const [s, d] of Object.entries(state.mapping)) {
        if (state.src.some((x) => x.key === s) && state.dst.some((x) => x.key === d)) mapping[s] = d;
      }
      return { enabled: state.enabled, mapping };
    },
    reload: load,
  };

  render();
  if (state.enabled) load();
  return api;
}
