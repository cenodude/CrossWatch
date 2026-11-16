/* assets/js/modals/insight-settings/index.js */
/* Modal for configuring which features and provider instances contribute to the insights statistics. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const PREF_KEY = "insights.settings.v1";
const FeatureMeta = () => window.CW?.FeatureMeta || {};
const ProviderMeta = () => window.CW?.ProviderMeta || {};
const FEATS = FeatureMeta().order || ["watchlist", "ratings", "history", "progress", "playlists", "collection"];
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
const overviewFilter = () => window.CW?.OverviewProfile?.filter || {};
const overviewHasScope = () => !!Object.keys(overviewFilter() || {}).length;
const overviewInstancesFor = (provider) => {
  const filter = overviewFilter();
  const up = canonProv(provider);
  const low = provKey(provider);
  return Array.isArray(filter[up]) ? filter[up].map(instKey) : Array.isArray(filter[low]) ? filter[low].map(instKey) : null;
};

const HTML = `
  <div class="cw-page-preferences">
    <header class="pf-head"><div><h2>Insights settings</h2><p>Choose the features and instances shown in statistics.</p></div><button class="pf-button" id="is-close" type="button">Close</button></header>
    <div class="pf-body">
      <section class="pf-section"><h3 id="is-feat-heading">Features</h3><div class="feature-list" id="is-feat-grid" role="group" aria-labelledby="is-feat-heading"></div></section>
      <section class="pf-section"><h3>Provider instances <span id="is-head-chip"></span></h3><div id="is-loading" class="pf-note">Loading providers...</div><div class="prov-grid" id="is-prov-grid" style="display:none"></div></section>
    </div>
    <footer class="pf-footer"><span class="pf-note" id="is-toast" role="status">Changes apply to statistics.</span><div class="pf-actions"><button class="pf-button pf-reset" id="is-reset" type="button">Restore defaults</button><button class="pf-button primary" id="is-apply" type="button">Apply changes</button></div></footer>
  </div>`;

const parseInstanceList = (raw) => {
  const out = { ids: [], labels: { default: "Default" } };
  for (const it of Array.isArray(raw) ? raw : []) {
    const id = typeof it === "string" ? it : String(it?.id || "").trim();
    if (!id || out.ids.includes(id)) continue;
    out.ids.push(id);
    const label = typeof it === "object" && it ? String(it.friendly_label || it.display_label || it.label || "").trim() : "";
    if (label) out.labels[id] = label;
  }
  return out;
};

const normalizePrefs = (prefs, byProvider = {}) => {
  const out = prefs && typeof prefs === "object" ? JSON.parse(JSON.stringify(prefs)) : {};
  const f = out.features && typeof out.features === "object" ? out.features : {};
  out.features = { watchlist: f.watchlist !== false, ratings: f.ratings !== false, history: f.history !== false, progress: f.progress !== false, playlists: f.playlists !== false, collection: f.collection !== false };
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
  return `<button type="button" class="pf-feature" id="is-feat-${esc(key)}" data-feat="${h(key)}" aria-pressed="${prefs.features?.[key] !== false}">${h(label)}</button>`;
}).join("");

const renderProviderCard = (provider, all, selected, labels) => {
  const key = String(provider || "").toLowerCase(), picked = new Set(selected), count = all.filter((id) => picked.has(id)).length;
  const label = provLabel(key), logo = ProviderMeta().logoPath?.(key) || "";
  const single = all.length === 1;
  const instanceLabel = id => labels?.[key]?.[id] || (id === "default" ? "Default" : id);
  return `<section class="pf-provider prov-card" data-provider="${h(key)}" data-single="${single ? 1 : 0}"><div class="pf-provider-head"><label class="pf-option"><input type="checkbox" ${single ? `data-inst="${h(all[0])}"` : "data-provider-all"} ${count === all.length ? "checked" : ""}>${logo ? `<img src="${h(logo)}" alt="">` : ""}<strong>${h(label)}</strong></label>${single ? `<span class="pf-provider-count">${h(instanceLabel(all[0]))}</span>` : `<span class="pf-provider-count" data-badge>${count} / ${all.length}</span>`}</div>${single ? "" : `<div class="pf-options">${all.map((id) => `<label class="pf-option"><input type="checkbox" data-inst="${h(id)}" ${picked.has(id) ? "checked" : ""}><span>${h(instanceLabel(id))}</span></label>`).join("")}</div>`}</section>`;
};

const getAllowedProviders = (cfg = window._cfgCache || {}) => {
  try {
    if (typeof window.getConfiguredProviders === "function") return new Set(Array.from(window.getConfiguredProviders(cfg) || []).map(canonProv).filter(Boolean));
  } catch {}
  return new Set(["CROSSWATCH"]);
};

const buildProviders = async () => {
  const labels = {}, byProvider = {}, [instApi, cfg] = await Promise.all([jget(`/api/provider-instances?configured_only=true&cb=${Date.now()}`), jget(`/api/config?cb=${Date.now()}`)]);
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
    const scoped = overviewInstancesFor(prov);
    if (overviewHasScope() && !Array.isArray(scoped)) continue;
    const ids = Array.isArray(scoped)
      ? parsed.ids.filter((id) => scoped.includes(instKey(id)))
      : parsed.ids;
    if (!ids.length) continue;
    byProvider[prov] = ids;
    labels[prov] = parsed.labels;
  }
  return { byProvider, labels };
};

export default {
  async mount(root) {
    root.classList.add("cw-page-dialog", "cw-page-insights");
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
      if (badge) badge.textContent = `${on} / ${checks.length}`;
      const parent = $('[data-provider-all]', card);
      if (parent) {
        parent.checked = on === checks.length;
        parent.indeterminate = on > 0 && on < checks.length;
      }
      card.dataset.empty = on ? 0 : 1;
      refreshStats();
    };

    $("#is-close", root)?.addEventListener("click", close);
    $("#is-reset", root)?.addEventListener("click", () => {
      $$("button[data-feat]", root).forEach(button => button.setAttribute("aria-pressed", "true"));
      $$("input[data-inst]", root).forEach(input => { input.checked = true; });
      $$(".prov-card", root).forEach(updateCard);
      setToast("Defaults restored. Apply changes to save.");
    });

    try {
      const { byProvider, labels } = await buildProviders();
      const prefs = normalizePrefs(loadPrefs(), byProvider);
      const featGrid = $("#is-feat-grid", root);
      if (featGrid) featGrid.innerHTML = renderFeatures(prefs);

      const provKeys = Object.keys(byProvider).sort((a, b) => a.localeCompare(b));
      if (!provKeys.length) {
        if (loading) loading.textContent = overviewHasScope() ? "No provider instances assigned to this profile." : "No configured providers yet.";
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
            updateCard(card);
          });
        }
      }
      refreshStats();

      root.addEventListener("click", (ev) => {
        const btn = ev.target?.closest?.("button[data-feat]");
        if (!btn) return;
        btn.setAttribute("aria-pressed", String(btn.getAttribute("aria-pressed") !== "true"));
        setToast("Unsaved changes.");
      });

      root.addEventListener("change", (ev) => {
        const t = ev.target;
        if (!(t instanceof Element)) return;
        if (t.matches('input[data-provider-all]')) {
          const card = t.closest('.prov-card');
          $$('input[data-inst]', card).forEach(input => { input.checked = t.checked; });
          updateCard(card);
        }
        if (t.matches('input[data-inst]')) updateCard(t.closest('.prov-card'));
        setToast("Unsaved changes.");
      });

      $("#is-apply", root)?.addEventListener("click", () => {
        const next = normalizePrefs(loadPrefs(), byProvider);
        for (const button of $$('button[data-feat]', root)) next.features[button.dataset.feat] = button.getAttribute("aria-pressed") === "true";
        if (!Object.values(next.features).some(Boolean)) {
          next.features.watchlist = true;
          const fallback = $("#is-feat-watchlist", root);
          if (fallback) fallback.setAttribute("aria-pressed", "true");
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
