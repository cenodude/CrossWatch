/* assets/js/modals/core/identity-map.js */
/* Identity mapping card shared by sync pairs, watcher routes and webhooks. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const IDENTITY_PROVIDERS = new Set(["plex", "emby", "jellyfin", "silo"]);
const NAMES = { plex: "Plex", emby: "Emby", jellyfin: "Jellyfin", silo: "Silo" };

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
    title: txt(opts.title) || "Identities",
    intro: txt(opts.intro) || "Sync several people, each with their own history.",
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
    return sug.reason === "linked" && sug.where ? `Also in ${sug.where}` : "Same name";
  }

  function statusHTML(srcKey) {
    const picked = state.mapping[srcKey];
    const sug = suggestionFor(srcKey);
    if (picked && state.dst.some((d) => d.key === picked)) {
      const text = sug && sug.key === picked ? reasonText(sug) : "Linked";
      return `<span class="cw-idm-chip is-on"><span class="material-symbols-rounded" aria-hidden="true">link</span>${esc(text)}</span>`;
    }
    if (sug && !takenBy(sug.key, srcKey)) {
      return `<button type="button" class="cw-idm-chip is-suggest" data-idm-use="${esc(srcKey)}" title="${esc(`Suggested: ${reasonText(sug).toLowerCase()}`)}"><span class="material-symbols-rounded" aria-hidden="true">auto_fix_high</span>Use ${esc(dstName(sug.key))} · ${esc(sug.reason === "linked" && sug.where ? `as in ${sug.where}` : "same name")}</button>`;
    }
    return `<span class="cw-idm-chip">Not synced</span>`;
  }

  function rowHTML(s) {
    const picked = state.dst.some((d) => d.key === state.mapping[s.key]) ? state.mapping[s.key] : "";
    const options = [`<option value="" ${picked ? "" : "selected"}>Don't sync</option>`].concat(state.dst.map((d) => {
      const owner = takenBy(d.key, s.key);
      const label = owner ? `${d.username} (used by ${srcName(owner)})` : d.username;
      return `<option value="${esc(d.key)}" ${picked === d.key ? "selected" : ""} ${owner ? "disabled" : ""}>${esc(label)}</option>`;
    })).join("");
    return `
      <div class="cw-idm-row ${picked ? "is-mapped" : ""}">
        <span class="cw-idm-person">${logo(state.source)}<span class="cw-idm-name" title="${esc(s.username)}">${esc(s.username)}</span></span>
        <span class="material-symbols-rounded cw-idm-arrow" aria-hidden="true">arrow_forward</span>
        <span class="cw-idm-pick">${logo(state.target)}<select class="cw-idm-select" data-idm-src="${esc(s.key)}" aria-label="${esc(`${providerName(state.target)} identity for ${s.username}`)}">${options}</select></span>
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
    const names = missing.map((p) => `<strong>${esc(providerName(p))}</strong>`).join(" and ");
    return `<div class="cw-idm-empty"><span class="material-symbols-rounded" aria-hidden="true">person_search</span><div><strong>No identities on ${missing.map((p) => esc(providerName(p))).join(" or ")} yet.</strong><p>Open Connections, choose ${names}, and pick the people under <strong>Identities</strong>. Then come back here to link them.</p></div></div>`;
  }

  function bodyHTML() {
    if (!state.enabled) return "";
    if (!state.loaded) return `<div class="cw-idm-body"><div class="cw-idm-muted">${state.error ? esc(state.error) : "Loading identities…"}</div></div>`;
    if (!state.src.length || !state.dst.length) return `<div class="cw-idm-body">${emptyHTML()}</div>`;
    const mapped = state.src.filter((s) => state.mapping[s.key] && state.dst.some((d) => d.key === state.mapping[s.key])).length;
    const open = openableSuggestions();
    const stale = Object.entries(state.mapping).filter(([s, d]) => !state.src.some((x) => x.key === s) || !state.dst.some((x) => x.key === d)).length;
    return `
      <div class="cw-idm-body">
        <div class="cw-idm-rows">${state.src.map(rowHTML).join("")}</div>
        <div class="cw-idm-foot">
          <span class="cw-idm-count">${mapped ? `${mapped} of ${state.src.length} ${state.src.length === 1 ? "person" : "people"} syncing` : "Nobody syncs yet. Pick an identity for each person."}</span>
          ${open.length ? `<button type="button" class="cw-idm-btn" data-idm-use-all><span class="material-symbols-rounded" aria-hidden="true">auto_fix_high</span>Use ${open.length} suggestion${open.length === 1 ? "" : "s"}</button>` : ""}
        </div>
        ${state.flash ? `<div class="cw-idm-flash" role="status">${esc(state.flash)}</div>` : ""}
        ${stale ? `<div class="cw-idm-warn"><span class="material-symbols-rounded" aria-hidden="true">warning</span>${stale} link${stale === 1 ? " points" : "s point"} to an identity that was removed from its connection. ${stale === 1 ? "It is" : "They are"} dropped when you save.</div>` : ""}
        <p class="cw-idm-hint">Only the people linked here sync. Your own account is included only if it is an identity on both sides. Missing someone? Add them under Identities in the ${esc(providerName(state.source))} or ${esc(providerName(state.target))} connection.${state.note ? ` ${esc(state.note)}` : ""}</p>
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
      state.error = "Could not load identities. Your current links are kept.";
    }
    if (state.loaded && state.enabled && !Object.keys(state.mapping).length) {
      const n = applySuggestions();
      if (n) {
        state.flash = `Linked ${n} ${n === 1 ? "person" : "people"} automatically. Check the list and save.`;
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
        if (n) state.flash = `Linked ${n} ${n === 1 ? "person" : "people"} automatically. Check the list and save.`;
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
      state.flash = n ? `Linked ${n} more ${n === 1 ? "person" : "people"}.` : "";
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
