/* assets/js/modals/insight-settings/index.js */
/* Modal for configuring which features and provider instances contribute to the insights statistics. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const PREF_KEY = "insights.settings.v1";
const FeatureMeta = () => window.CW?.FeatureMeta || {};
const ProviderMeta = () => window.CW?.ProviderMeta || {};
const FEAT_COPY = {
  watchlist: "Show watchlist tiles.",
  ratings: "Show ratings tiles.",
  history: "Show history tiles.",
  progress: "Show in-progress playback tiles.",
  playlists: "Show playlist sync tiles.",
};
const FEAT_UI = {
  watchlist: "movie",
  ratings: "star",
  history: "play_arrow",
  progress: "timelapse",
  playlists: "queue_music",
};
const FEATS = FeatureMeta().order || ["watchlist", "ratings", "history", "progress", "playlists"];
const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => [...r.querySelectorAll(s)];
const esc = (s) => window.CSS?.escape ? window.CSS.escape(String(s ?? "")) : String(s ?? "").replace(/[^\w-]/g, "\\$&");
const h = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const close = () => window.cxCloseModal?.();
const changed = () => window.dispatchEvent(new CustomEvent("insights:settings-changed", { detail: { force: true } }));
const loadPrefs = () => { try { return JSON.parse(localStorage.getItem(PREF_KEY) || "{}") || {}; } catch { return {}; } };
const savePrefs = (v) => { try { localStorage.setItem(PREF_KEY, JSON.stringify(v || {})); } catch {} };
const jget = async (url) => {
  try {
    const r = await fetch(url, { cache: "no-store", credentials: "same-origin" });
    if (!r.ok) throw 0;
    if (r.status === 204) return {};
    return await r.json().catch(() => ({}));
  } catch { return null; }
};

const canonProv = (v) => {
  const up = String(v || "").trim().toUpperCase();
  return !up ? "" : up === "TMDB_SYNC" ? "TMDB" : ["MDB", "MDB_LIST", "MDBLIST"].includes(up) ? "MDBLIST" : up;
};
const provKey = (v) => canonProv(v).toLowerCase();
const instKey = (v) => String(v || "default").trim() || "default";
const provLabel = (v) => ProviderMeta().label?.(canonProv(v)) || canonProv(v);

const HTML = `
  <div class="cx-head">
    <div class="head-left">
      <div class="head-copy">
        <div class="head-title">Insights settings</div>
        <div class="head-sub">Choose which features and profiles shape the statistics panel.</div>
      </div>
    </div>
    <div class="head-actions">
      <div class="head-chip"><span class="material-symbols-rounded" aria-hidden="true">stars</span><span id="is-head-chip">Preparing</span></div>
      <button class="close-btn" id="is-close" type="button"><span class="material-symbols-rounded" aria-hidden="true">close</span><span>Close</span></button>
    </div>
  </div>
  <div class="body">
    <div class="layout">
      <section class="panel">
        <div class="panel-head"><div class="panel-title">Features</div><div class="panel-chip">View</div></div>
        <div class="panel-body"><div class="feature-list" id="is-feat-grid"></div></div>
      </section>
      <section class="panel profiles-panel">
        <div class="panel-body providers-shell">
          <div class="loading" id="is-loading">Loading configured providers…</div>
          <div class="prov-grid" id="is-prov-grid" style="display:none"></div>
        </div>
      </section>
    </div>
  </div>
  <div class="actions">
    <div class="footer-note"><span class="material-symbols-rounded" aria-hidden="true">info</span><span class="toast" id="is-toast">Changes apply to the statistics panel.</span></div>
    <div class="action-row"><button class="btn danger" id="is-reset" type="button"><span class="material-symbols-rounded" aria-hidden="true">restart_alt</span>Reset</button><button class="btn good" id="is-apply" type="button"><span class="material-symbols-rounded" aria-hidden="true">check_circle</span>Apply</button></div>
  </div>`;

const parseInstanceList = (raw) => {
  const out = { ids: [], labels: { default: "Default" } };
  for (const it of Array.isArray(raw) ? raw : []) {
    const id = typeof it === "string" ? it : String(it?.id || "").trim();
    if (!id || out.ids.includes(id)) continue;
    out.ids.push(id);
    const label = typeof it === "object" && it ? String(it.label || "").trim() : "";
    if (label) out.labels[id] = label;
  }
  if (!out.ids.includes("default")) out.ids.unshift("default");
  return out;
};

const normalizePrefs = (prefs, byProvider = {}) => {
  const out = prefs && typeof prefs === "object" ? JSON.parse(JSON.stringify(prefs)) : {};
  const f = out.features && typeof out.features === "object" ? out.features : {};
  out.features = { watchlist: f.watchlist !== false, ratings: f.ratings !== false, history: f.history !== false, progress: f.progress !== false, playlists: f.playlists !== false };
  out.instances = out.instances && typeof out.instances === "object" ? out.instances : {};
  out.known_instances = out.known_instances && typeof out.known_instances === "object" ? out.known_instances : {};
  for (const [prov, list] of Object.entries(byProvider || {})) {
    const key = String(prov || "").toLowerCase();
    const all = Array.isArray(list) && list.length ? list.map(String) : ["default"];
    const cur = out.instances[key];
    out.instances[key] = cur === undefined ? [...all] : Array.isArray(cur) ? cur.map(String).filter((x) => all.includes(x)) : [];
    out.known_instances[key] = [...all];
  }
  return out;
};

const renderFeatures = (prefs) => FEATS.map((key) => {
  const label = FeatureMeta().label?.(key) || key;
  const copy = FEAT_COPY[key] || "";
  const icon = FEAT_UI[key] || "tune";
  const checked = prefs.features?.[key] !== false;
  return `<div class="feature-row"><div class="feature-icon"><span class="material-symbols-rounded" aria-hidden="true">${h(icon)}</span></div><div class="feature-text"><div class="feature-name">${h(label)}</div><div class="feature-copy">${h(copy)}</div></div><label class="switch" for="is-feat-${esc(key)}"><input type="checkbox" id="is-feat-${esc(key)}" data-feat="${h(key)}" ${checked ? "checked" : ""}><span class="slider"></span></label></div>`;
}).join("");

const renderProviderCard = (provider, all, selected, labels) => {
  const key = String(provider || "").toLowerCase(), picked = new Set(selected), count = all.filter((id) => picked.has(id)).length;
  const label = provLabel(key), logo = ProviderMeta().logoPath?.(key) || "";
  const icon = logo ? `<span class="prov-icon"><img src="${h(logo)}" alt="${h(label)} logo" loading="lazy"></span>` : `<span class="prov-icon"><span class="prov-icon-fallback">${h((label || key || "?").slice(0, 2).toUpperCase())}</span></span>`;
  return `<section class="prov-card" data-provider="${h(key)}" data-empty="${count ? 0 : 1}" data-single="${all.length === 1 ? 1 : 0}"><div class="prov-top"><div class="prov-brand">${icon}<div class="prov-title">${h(label)}</div></div><div class="prov-tools"><span class="prov-badge" data-badge>${count}/${all.length}</span><button class="mini" type="button" data-all>All</button><button class="mini" type="button" data-none>None</button></div></div><div data-list>${all.map((id) => `<label class="pill" for="is-${esc(key)}-${esc(id)}"><input type="checkbox" id="is-${esc(key)}-${esc(id)}" data-inst="${h(id)}" ${picked.has(id) ? "checked" : ""}><span class="lab"><span>${h(labels?.[key]?.[id] || (id === "default" ? "Default" : id))}</span><span class="material-symbols-rounded" aria-hidden="true">check</span></span></label>`).join("")}</div></section>`;
};

const decorateProviderCard = (card) => {
  const provider = card?.dataset?.provider || "";
  const meta = ProviderMeta();
  const rgb = meta.tone?.(provider)?.rgb || "124,92,255";
  const logo = meta.logoPath?.(provider) || "";
  card?.style.setProperty("--provider-rgb", rgb);
  if (logo) card?.style.setProperty("--provider-wm", `url(${JSON.stringify(logo)})`);
};

const pathGet = (obj, path) => (path || []).reduce((acc, key) => (acc && typeof acc === "object" ? acc[key] : undefined), obj);
const hasValue = (v) => typeof v === "string" ? v.trim().length > 0 : !!v;
const hasAnyConfigValue = (root, keys = []) => {
  if (!root || typeof root !== "object") return false;
  if (keys.some((key) => hasValue(root[key]))) return true;
  const inst = root.instances;
  return !!(inst && typeof inst === "object" && Object.values(inst).some((row) => row && typeof row === "object" && keys.some((key) => hasValue(row[key]))));
};
const hasTmdbConfig = (root) => {
  const match = (block) => !!(block && typeof block === "object" && ((hasValue(block.api_key) && hasValue(block.session_id)) || hasValue(block.account_id)));
  if (match(root)) return true;
  const inst = root?.instances;
  return !!(inst && typeof inst === "object" && Object.values(inst).some(match));
};
const hasNuvioConfig = (root) => {
  const match = (block) => !!(block && typeof block === "object" && hasValue(block.profile_id) && (hasValue(block.access_token) || hasValue(block.refresh_token)));
  if (match(root)) return true;
  const inst = root?.instances;
  return !!(inst && typeof inst === "object" && Object.values(inst).some(match));
};
const getAllowedProviders = (cfg = window._cfgCache || {}) => {
  try {
    if (typeof window.getConfiguredProviders === "function") return new Set(Array.from(window.getConfiguredProviders(cfg) || []).map(canonProv).filter(Boolean));
  } catch {}
  const set = new Set(), checks = [
    { key: "PLEX", paths: [["plex"]], keys: ["account_token", "token"] },
    { key: "SIMKL", paths: [["simkl"], ["auth", "simkl"]], keys: ["access_token"] },
    { key: "TRAKT", paths: [["trakt"], ["auth", "trakt"]], keys: ["access_token"] },
    { key: "ANILIST", paths: [["anilist"], ["auth", "anilist"]], keys: ["access_token", "token"] },
    { key: "JELLYFIN", paths: [["jellyfin"], ["auth", "jellyfin"]], keys: ["access_token"] },
    { key: "EMBY", paths: [["emby"], ["auth", "emby"]], keys: ["access_token", "api_key", "token"] },
    { key: "MDBLIST", paths: [["mdblist"], ["auth", "mdblist"]], keys: ["api_key", "access_token"] },
    { key: "PUBLICMETADB", paths: [["publicmetadb"], ["auth", "publicmetadb"]], keys: ["api_key"] },
  ];
  for (const def of checks) if (def.paths.some((path) => hasAnyConfigValue(pathGet(cfg, path), def.keys))) set.add(def.key);
  if ([cfg?.nuvio, cfg?.auth?.nuvio].some(hasNuvioConfig)) set.add("NUVIO");
  if ([cfg?.tmdb_sync, cfg?.tmdb, cfg?.auth?.tmdb_sync].some(hasTmdbConfig)) set.add("TMDB");
  if ([cfg?.tautulli, cfg?.auth?.tautulli].some((block) => hasAnyConfigValue(block, ["api_key", "server_url", "server"]))) set.add("TAUTULLI");
  if ((cfg?.crosswatch || cfg?.CrossWatch || {}).enabled !== false) set.add("CROSSWATCH");
  return set;
};

const buildProviders = async () => {
  const labels = {}, byProvider = {}, [instApi, cfg] = await Promise.all([jget(`/api/provider-instances?cb=${Date.now()}`), jget(`/api/config?cb=${Date.now()}`)]);
  const metaOrder = ProviderMeta().order || [];
  const relevant = new Set((Array.isArray(metaOrder) ? metaOrder : []).map(canonProv));
  const instMap = instApi || {}, allowed = getAllowedProviders(cfg || window._cfgCache || {});
  const getRaw = async (key) => {
    const up = canonProv(key), candidates = [up, key, up.toLowerCase(), ...(up === "TMDB" ? ["TMDB_SYNC", "tmdb_sync"] : [])];
    for (const k of candidates) if (k && Object.prototype.hasOwnProperty.call(instMap, k)) return instMap[k];
    return await jget(`/api/provider-instances/${encodeURIComponent(key)}?cb=${Date.now()}`);
  };
  for (const prov of Array.from(allowed).filter((key) => relevant.has(key)).map(provKey).filter(Boolean).sort((a, b) => a.localeCompare(b))) {
    const parsed = parseInstanceList(await getRaw(prov));
    if (!parsed.ids.length) continue;
    byProvider[prov] = parsed.ids;
    labels[prov] = parsed.labels;
  }
  return { byProvider, labels };
};

export default {
  async mount(root) {
    root.classList.add("modal-root", "cw-insight-set");
    root.style.setProperty("--cxModalMaxW", "1080px");
    root.style.setProperty("--cxModalMaxH", "640px");
    root.style.setProperty("--cxModalW", "min(var(--cxModalMaxW,1080px),calc(100vw - 20px))");
    root.innerHTML = HTML;

    const toast = $("#is-toast", root), chip = $("#is-head-chip", root), count = $("#is-providers-count", root), loading = $("#is-loading", root), grid = $("#is-prov-grid", root);
    const setToast = (msg = "") => { if (toast) toast.textContent = msg || "Changes apply to the statistics panel."; };
    const refreshStats = () => {
      const providers = $$(".prov-card", root).length;
      if (chip) chip.textContent = `${providers} providers`;
      if (count) count.textContent = `${providers} provider${providers === 1 ? "" : "s"}`;
    };
    const updateCard = (card) => {
      if (!card) return;
      const checks = $$('input[data-inst]', card), on = checks.filter((c) => c.checked).length;
      const badge = $('[data-badge]', card);
      if (badge) badge.textContent = `${on}/${checks.length}`;
      card.dataset.empty = on ? 0 : 1;
      refreshStats();
    };

    $("#is-close", root)?.addEventListener("click", close);
    $("#is-reset", root)?.addEventListener("click", () => { try { localStorage.removeItem(PREF_KEY); } catch {} changed(); close(); });

    try {
      const { byProvider, labels } = await buildProviders();
      const prefs = normalizePrefs(loadPrefs(), byProvider);
      const featGrid = $("#is-feat-grid", root);
      if (featGrid) featGrid.innerHTML = renderFeatures(prefs);

      const provKeys = Object.keys(byProvider).sort((a, b) => a.localeCompare(b));
      if (!provKeys.length) {
        if (loading) loading.textContent = "No configured providers yet.";
      } else {
        if (loading) loading.style.display = "none";
        if (grid) {
          grid.style.display = "grid";
          grid.innerHTML = provKeys.map((prov) => {
            const all = byProvider[prov]?.map(String) || ["default"];
            const picked = prefs.instances[prov] === undefined ? all : Array.isArray(prefs.instances[prov]) ? prefs.instances[prov].map(String) : [];
            return renderProviderCard(prov, all, picked, labels);
          }).join("");
          $$(".prov-card", grid).forEach((card) => {
            decorateProviderCard(card);
            updateCard(card);
          });
        }
      }
      refreshStats();

      root.addEventListener("click", (ev) => {
        const btn = ev.target?.closest?.("[data-all],[data-none]");
        if (!btn) return;
        const card = btn.closest(".prov-card");
        $$('input[data-inst]', card).forEach((c) => { c.checked = btn.hasAttribute("data-all"); });
        updateCard(card);
      });

      root.addEventListener("change", (ev) => {
        const t = ev.target;
        if (!(t instanceof Element)) return;
        if (t.matches('input[data-feat]')) return setToast(""), refreshStats();
        if (t.matches('input[data-inst]')) updateCard(t.closest('.prov-card'));
      });

      $("#is-apply", root)?.addEventListener("click", () => {
        const next = normalizePrefs(loadPrefs(), byProvider);
        for (const c of $$('input[data-feat]', root)) next.features[c.dataset.feat] = !!c.checked;
        if (!Object.values(next.features).some(Boolean)) {
          next.features.watchlist = true;
          const fallback = $("#is-feat-watchlist", root);
          if (fallback) fallback.checked = true;
          setToast("At least one feature stays enabled.");
        }
        next.instances = next.instances && typeof next.instances === "object" ? next.instances : {};
        for (const card of $$(".prov-card", root)) {
          const prov = String(card.dataset.provider || "").toLowerCase();
          const checks = $$('input[data-inst]', card), all = checks.map((c) => String(c.dataset.inst || "")), selected = checks.filter((c) => c.checked).map((c) => String(c.dataset.inst || ""));
          if (selected.length === all.length) delete next.instances[prov];
          else next.instances[prov] = selected;
        }
        savePrefs(next);
        changed();
        close();
      });
    } catch (e) {
      console.error("Insight settings mount failed:", e);
      if (chip) chip.textContent = "Error";
      if (loading) loading.textContent = `Failed to load providers: ${String(e?.message || e)}`;
      setToast("Failed to load insight settings. See console.");
    }
  }
};
