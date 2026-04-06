/* assets/js/modals/manual-watched/index.js */
/* Manual watched item modal */

const STORAGE_KEY = "cw.manualWatched.providers";

const fjson = async (url, opts = {}) => {
  const r = await fetch(url, { cache: "no-store", ...opts });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(data?.error || `${r.status}`);
  return data || {};
};

const esc = (value) => String(value ?? "")
  .replaceAll("&", "&amp;")
  .replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;")
  .replaceAll('"', "&quot;");

const providerKey = (item) => `${String(item?.provider || "").toUpperCase()}:${String(item?.instance || "default")}`;
const MEDIA_SERVER_PROVIDERS = new Set(["PLEX", "JELLYFIN", "EMBY"]);
const mediaChip = (type) => (type === "show" ? "Show" : "Movie");
const artType = (type) => (type === "show" ? "tv" : "movie");
const todayLocal = () => new Date().toISOString().slice(0, 10);

function injectCSS() {
  if (document.getElementById("cw-manual-watched-css")) return;
  const el = document.createElement("style");
  el.id = "cw-manual-watched-css";
  el.textContent = `.cw-mw{box-sizing:border-box;width:min(920px,calc(100vw - 28px));max-height:min(860px,calc(100vh - 28px));display:flex;flex-direction:column;overflow:hidden;border-radius:24px;border:1px solid rgba(255,255,255,.07);background:radial-gradient(115% 120% at 0% 0%,rgba(87,72,182,.17),transparent 38%),radial-gradient(96% 110% at 100% 100%,rgba(35,60,110,.12),transparent 54%),linear-gradient(180deg,rgba(7,10,17,.985),rgba(3,5,10,.985));color:#f3f6ff;box-shadow:inset 0 1px 0 rgba(255,255,255,.03),0 28px 64px rgba(0,0,0,.42)}.cw-mw .cx-head{display:flex;align-items:center;gap:14px;padding:14px 16px 12px;border-bottom:1px solid rgba(255,255,255,.06);background:linear-gradient(180deg,rgba(255,255,255,.022),rgba(255,255,255,.008))}.cw-mw-head{display:flex;align-items:center;gap:12px;min-width:0}.cw-mw-orb{width:40px;height:40px;border-radius:14px;flex:none;display:grid;place-items:center;border:1px solid rgba(139,149,255,.18);background:linear-gradient(145deg,rgba(21,26,48,.96),rgba(10,14,27,.94));box-shadow:inset 0 1px 0 rgba(255,255,255,.04),0 14px 26px rgba(0,0,0,.22)}.cw-mw-orb .material-symbols-rounded{font-size:21px;color:#c4ceff}.cw-mw-title{font-size:17px;font-weight:850;letter-spacing:-.02em;color:#f8fbff}.cw-mw-sub{margin-top:3px;font-size:12px;color:rgba(204,213,230,.72)}.cw-mw .cx-body{flex:1;min-height:0;overflow:auto;padding:16px;display:grid;grid-template-columns:minmax(0,1.04fr) minmax(320px,.96fr);gap:14px;align-items:start}.cw-mw-col{min-width:0;display:grid;gap:14px;align-content:start}.cw-mw-panel,.cw-mw-card{border:1px solid rgba(255,255,255,.07);background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.016));box-shadow:inset 0 1px 0 rgba(255,255,255,.025)}.cw-mw-panel{position:relative;border-radius:22px;padding:14px;display:block;min-width:0;width:100%;overflow:hidden}.cw-mw-panel>*+*{margin-top:12px}.cw-mw-card{position:relative;border-radius:18px;padding:12px;display:block;overflow:hidden}.cw-mw-label{font-size:11px;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:rgba(205,214,229,.68)}.cw-mw-row{display:flex;align-items:center;gap:10px;flex-wrap:wrap}.cw-mw-search-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:10px}.cw-mw-input,.cw-mw-date{width:100%;min-height:42px;padding:10px 13px;border-radius:14px;border:1px solid rgba(255,255,255,.08);background:rgba(3,6,11,.9);color:#f3f6ff;font:inherit;outline:none;transition:border-color .16s ease,box-shadow .16s ease,background .16s ease}.cw-mw-input:focus,.cw-mw-date:focus{border-color:rgba(122,130,244,.3);box-shadow:0 0 0 3px rgba(108,116,236,.1);background:rgba(5,8,14,.96)}.cw-mw-pills{display:flex;gap:8px;flex-wrap:wrap}.cw-mw-pill{border-radius:16px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.022));color:rgba(225,232,245,.84);cursor:pointer;transition:transform .16s ease,border-color .16s ease,background .16s ease;min-height:38px;padding:0 14px;font-size:12px;font-weight:800}.cw-mw-pill:hover{transform:translateY(-1px);border-color:rgba(255,255,255,.14)}.cw-mw-pill.active{border-color:rgba(137,146,255,.28);background:linear-gradient(180deg,rgba(84,92,214,.28),rgba(44,52,114,.16));color:#f9fbff;box-shadow:0 12px 22px rgba(38,45,108,.18),inset 0 1px 0 rgba(255,255,255,.04)}.cw-mw-provider-list{display:grid;gap:8px;max-height:280px;overflow:auto;padding-right:2px}.cw-mw-provider-group{border:1px solid rgba(255,255,255,.07);border-radius:16px;background:linear-gradient(180deg,rgba(255,255,255,.028),rgba(255,255,255,.012));overflow:hidden}.cw-mw-provider-group summary{list-style:none;display:flex;align-items:center;justify-content:space-between;gap:10px;padding:11px 12px;cursor:pointer;user-select:none}.cw-mw-provider-group summary::-webkit-details-marker{display:none}.cw-mw-provider-group-title{font-size:12px;font-weight:800;color:#f3f6ff;letter-spacing:.03em}.cw-mw-provider-group-meta{font-size:11px;color:rgba(201,210,228,.66)}.cw-mw-provider-group-caret{font-size:18px;color:rgba(214,222,240,.7);transition:transform .16s ease}.cw-mw-provider-group[open] .cw-mw-provider-group-caret{transform:rotate(90deg)}.cw-mw-provider-group-body{display:grid;gap:8px;padding:0 8px 8px;border-top:1px solid rgba(255,255,255,.05)}.cw-mw-provider-row{display:grid;grid-template-columns:20px minmax(0,1fr);align-items:start;gap:12px;min-width:0;padding:10px 12px;border-radius:14px;border:1px solid rgba(255,255,255,.07);background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015));cursor:pointer;transition:border-color .16s ease,background .16s ease}.cw-mw-provider-row.active,.cw-mw-provider-row:hover{border-color:rgba(137,146,255,.2);background:linear-gradient(180deg,rgba(74,82,196,.16),rgba(36,44,102,.10))}.cw-mw-provider-check{width:16px;height:16px;margin:0;accent-color:#8d96ff}.cw-mw-provider-copy{min-width:0;display:grid;gap:8px}.cw-mw-provider-name{font-size:13px;font-weight:800;color:#f6f9ff;line-height:1.25}.cw-mw-provider-badges{display:flex;gap:6px;flex-wrap:wrap}.cw-mw-badge{display:inline-flex;align-items:center;justify-content:center;min-height:22px;padding:0 8px;border-radius:999px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.05);font-size:10px;font-weight:800;letter-spacing:.05em;text-transform:uppercase;color:rgba(228,235,247,.78)}.cw-mw-badge.is-accent{border-color:rgba(133,142,255,.2);color:#eff3ff;background:rgba(90,100,220,.16)}.cw-mw-actions{display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap}.cw-mw-link{border:0;background:transparent;color:rgba(198,207,229,.86);font:inherit;font-size:12px;font-weight:700;cursor:pointer;padding:0}.cw-mw-link:hover{color:#f5f8ff}.cw-mw-provider-tools{display:flex;align-items:center;gap:10px;flex-wrap:wrap}.cw-mw-results{border-radius:18px;border:1px solid rgba(255,255,255,.06);overflow:auto;min-height:120px;max-height:260px;background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015))}.cw-mw-result{width:100%;border:0;border-bottom:1px solid rgba(255,255,255,.05);background:transparent;color:inherit;display:grid;grid-template-columns:56px minmax(0,1fr);gap:12px;padding:12px;text-align:left;cursor:pointer}.cw-mw-result:last-child{border-bottom:0}.cw-mw-result:hover{background:rgba(255,255,255,.04)}.cw-mw-poster,.cw-mw-selected-poster{overflow:hidden;border-radius:12px;background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.02));border:1px solid rgba(255,255,255,.07)}.cw-mw-poster{width:56px;height:82px}.cw-mw-selected-poster{width:88px;height:128px;flex:none}.cw-mw-poster img,.cw-mw-selected-poster img{width:100%;height:100%;object-fit:cover;display:block}.cw-mw-ghost{width:100%;height:100%;display:grid;place-items:center;color:rgba(208,216,230,.62);font-size:11px;font-weight:700}.cw-mw-result-title{font-size:14px;font-weight:800;color:#f5f8ff}.cw-mw-result-meta,.cw-mw-muted{font-size:12px;color:rgba(202,211,228,.72)}.cw-mw-result-overview{margin-top:4px;font-size:12px;line-height:1.45;color:rgba(224,231,243,.76)}.cw-mw-selected{display:grid;grid-template-columns:88px minmax(0,1fr);gap:12px;align-items:start}.cw-mw-selected-copy{display:grid;gap:8px;min-width:0}.cw-mw-selected-head{display:flex;align-items:flex-start;justify-content:space-between;gap:10px}.cw-mw-selected-title{font-size:18px;font-weight:850;letter-spacing:-.02em;color:#f8fbff}.cw-mw-selected-overview{font-size:12px;line-height:1.5;color:rgba(218,226,241,.76)}.cw-mw-date-wrap{display:grid;gap:8px}.cw-mw-date-wrap.hidden{display:none}.cw-mw-rating-wrap{display:grid;gap:10px}.cw-mw-rating-top{display:flex;align-items:center;justify-content:space-between;gap:12px}.cw-mw-rating-value{min-width:72px;min-height:28px;padding:0 10px;border-radius:999px;border:1px solid rgba(133,142,255,.2);background:linear-gradient(180deg,rgba(84,92,214,.2),rgba(44,52,114,.12));color:#f7f9ff;font-size:12px;font-weight:800;display:inline-flex;align-items:center;justify-content:center;box-shadow:inset 0 1px 0 rgba(255,255,255,.05)}.cw-mw-rating-slider-wrap{position:relative;padding:6px 0 0}.cw-mw-rating-slider{appearance:none;-webkit-appearance:none;width:100%;height:12px;border-radius:999px;outline:none;border:1px solid rgba(255,255,255,.08);background:linear-gradient(90deg,rgba(104,114,246,.92) 0%,rgba(132,140,255,.95) var(--rating-progress,0%),rgba(255,255,255,.06) var(--rating-progress,0%),rgba(255,255,255,.04) 100%);box-shadow:inset 0 1px 2px rgba(0,0,0,.24),0 10px 24px rgba(42,49,120,.14);cursor:pointer}.cw-mw-rating-slider::-webkit-slider-thumb{appearance:none;-webkit-appearance:none;width:22px;height:22px;border-radius:50%;border:1px solid rgba(236,240,255,.7);background:radial-gradient(circle at 30% 30%,#fff,#dfe5ff 58%,#bcc5ff 100%);box-shadow:0 10px 18px rgba(62,69,166,.32),inset 0 1px 0 rgba(255,255,255,.8)}.cw-mw-rating-slider::-moz-range-thumb{width:22px;height:22px;border-radius:50%;border:1px solid rgba(236,240,255,.7);background:radial-gradient(circle at 30% 30%,#fff,#dfe5ff 58%,#bcc5ff 100%)}.cw-mw-rating-scale{display:grid;grid-template-columns:repeat(11,minmax(0,1fr));gap:4px;margin-top:10px}.cw-mw-rating-mark{text-align:center;font-size:10px;font-weight:800;color:rgba(202,211,228,.66);user-select:none}.cw-mw-status{min-height:22px;font-size:12px;color:rgba(201,210,228,.74)}.cw-mw-status.error{color:#ffc7d1}.cw-mw-status.success{color:#c7ffe1}.cw-mw-empty{padding:12px 14px;border-radius:16px;border:1px dashed rgba(255,255,255,.1);color:rgba(202,211,228,.7);font-size:12px}.cw-mw .cx-foot{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;padding:12px 16px 16px;border-top:1px solid rgba(255,255,255,.06)}.cw-mw-btn{min-height:40px;padding:0 15px;border-radius:999px;border:1px solid rgba(255,255,255,.1);background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.03));color:#eef3ff;font:inherit;font-size:12px;font-weight:800;cursor:pointer}.cw-mw-btn:hover{border-color:rgba(255,255,255,.15);background:linear-gradient(180deg,rgba(255,255,255,.1),rgba(255,255,255,.04))}.cw-mw-btn.primary{background:linear-gradient(180deg,rgba(93,102,228,.44),rgba(62,69,166,.28));border-color:rgba(138,146,255,.24);box-shadow:0 12px 24px rgba(52,60,152,.18)}.cw-mw-btn[disabled]{opacity:.46;cursor:not-allowed}.cw-mw.is-mobile{width:min(calc(100dvw - 40px),396px)!important;max-width:calc(100dvw - 40px)!important;max-height:calc(100dvh - 20px)!important;border-radius:18px!important;margin:0 auto!important}.cw-mw.is-mobile .cx-body{display:block!important;grid-template-columns:1fr!important;padding:12px 14px!important;overflow:auto!important;-webkit-overflow-scrolling:touch}.cw-mw.is-mobile.is-search-collapsed .cw-mw-col-search{display:none!important}.cw-mw.is-mobile .cw-mw-col{display:block!important}.cw-mw.is-mobile .cw-mw-col+.cw-mw-col{margin-top:12px!important}.cw-mw.is-mobile .cx-head{align-items:flex-start!important;padding:12px 14px 10px!important}.cw-mw.is-mobile .cw-mw-head{gap:10px!important;min-width:0!important}.cw-mw.is-mobile .cw-mw-title{font-size:15px!important}.cw-mw.is-mobile .cw-mw-sub{font-size:11px!important;line-height:1.4!important}.cw-mw.is-mobile .cw-mw-panel{display:flex!important;flex-direction:column!important;width:100%!important;min-width:0!important;gap:10px!important;padding:12px!important;border-radius:18px!important}.cw-mw.is-mobile .cw-mw-card{width:100%!important;min-width:0!important;padding:10px!important;border-radius:16px!important}.cw-mw.is-mobile .cw-mw-col-side .cw-mw-card+.cw-mw-card{margin-top:12px!important}.cw-mw.is-mobile .cw-mw-search-row{grid-template-columns:1fr!important}.cw-mw.is-mobile .cw-mw-search-row .cw-mw-btn{width:100%!important}.cw-mw.is-mobile .cw-mw-pills{gap:6px!important}.cw-mw.is-mobile .cw-mw-pill{min-height:36px!important;padding:0 12px!important;font-size:11px!important}.cw-mw.is-mobile .cw-mw-results{min-height:120px!important;max-height:220px!important}.cw-mw.is-mobile .cw-mw-result{grid-template-columns:48px minmax(0,1fr)!important;gap:10px!important;padding:10px!important}.cw-mw.is-mobile .cw-mw-poster{width:48px!important;height:70px!important}.cw-mw.is-mobile .cw-mw-result-title{font-size:13px!important}.cw-mw.is-mobile .cw-mw-result-meta,.cw-mw.is-mobile .cw-mw-muted{font-size:11px!important}.cw-mw.is-mobile .cw-mw-result-overview,.cw-mw.is-mobile .cw-mw-selected-overview{font-size:11px!important;line-height:1.4!important;display:-webkit-box!important;-webkit-box-orient:vertical!important;overflow:hidden!important}.cw-mw.is-mobile .cw-mw-result-overview{-webkit-line-clamp:3!important}.cw-mw.is-mobile .cw-mw-selected-overview{-webkit-line-clamp:4!important}.cw-mw.is-mobile .cw-mw-provider-list{max-height:220px!important}.cw-mw.is-mobile .cw-mw-actions{flex-direction:column!important;align-items:flex-start!important}.cw-mw.is-mobile .cw-mw-provider-tools{width:100%!important;justify-content:flex-start!important}.cw-mw.is-mobile .cw-mw-provider-row{padding:10px!important;gap:10px!important}.cw-mw.is-mobile .cw-mw-provider-name{font-size:12px!important}.cw-mw.is-mobile .cw-mw-provider-badges{gap:5px!important}.cw-mw.is-mobile .cw-mw-badge{min-height:20px!important;padding:0 7px!important;font-size:9px!important}.cw-mw.is-mobile .cw-mw-selected{grid-template-columns:64px minmax(0,1fr)!important;gap:10px!important}.cw-mw.is-mobile .cw-mw-selected-poster{width:64px!important;height:94px!important}.cw-mw.is-mobile .cw-mw-selected-copy{gap:6px!important}.cw-mw.is-mobile .cw-mw-selected-head{gap:8px!important}.cw-mw.is-mobile .cw-mw-selected-title{font-size:16px!important;line-height:1.2!important}.cw-mw.is-mobile .cw-mw-rating-top{gap:8px!important}.cw-mw.is-mobile .cw-mw-rating-value{min-width:50px!important;font-size:10px!important}.cw-mw.is-mobile .cw-mw-rating-slider{height:7px!important}.cw-mw.is-mobile .cw-mw-rating-slider::-webkit-slider-thumb{width:16px!important;height:16px!important}.cw-mw.is-mobile .cw-mw-rating-scale{gap:2px!important}.cw-mw.is-mobile .cw-mw-rating-mark{font-size:8px!important}.cw-mw.is-mobile .cx-foot{padding:10px 14px 12px!important}.cw-mw.is-mobile .cx-foot .cw-mw-row{width:100%!important;display:grid!important;grid-template-columns:repeat(2,minmax(0,1fr))!important;gap:8px!important}.cw-mw.is-mobile .cx-foot .cw-mw-btn{width:100%!important}@media (max-width:980px){.cw-mw .cx-body{grid-template-columns:1fr}}`;
  document.head.appendChild(el);
}

export default {
  async mount(root) {
    injectCSS();

    const state = {
      type: "movie",
      providers: [],
      selectedProviders: new Set(),
      remembered: [],
      query: "",
      results: [],
      searching: false,
      selectedItem: null,
      searchExpanded: true,
      providersExpanded: false,
      dateMode: "today",
      watchedOn: todayLocal(),
      actions: { history: true, watchlist: false, rating: false },
      rating: null,
      saving: false,
      status: "",
      statusTone: "",
    };

    const renderPoster = (item, klass = "cw-mw-poster") => {
      if (!item?.tmdb) return `<div class="${klass}"><div class="cw-mw-ghost">No art</div></div>`;
      return `<div class="${klass}"><img src="/art/tmdb/${artType(item.type)}/${encodeURIComponent(String(item.tmdb))}?size=w185" alt=""></div>`;
    };

    const isPhoneLayout = () => {
      try {
        return window.innerWidth <= 900 || !!window.matchMedia?.("(pointer: coarse)")?.matches;
      } catch {
        return window.innerWidth <= 900;
      }
    };

    const syncProviderCardPlacement = () => {
      const card = root.querySelector("[data-role=providers-card]");
      const leftAnchor = root.querySelector("[data-role=providers-anchor-left]");
      const sideAnchor = root.querySelector("[data-role=providers-anchor-side]");
      if (!card || !leftAnchor || !sideAnchor) return;
      (isPhoneLayout() ? sideAnchor : leftAnchor).appendChild(card);
    };

    const renderProvidersCard = () => {
      const card = root.querySelector("[data-role=providers-card]");
      const body = root.querySelector("[data-role=providers-body]");
      const toggle = root.querySelector("[data-role=providers-toggle]");
      if (!card || !body || !toggle) return;
      const mobile = isPhoneLayout();
      const expanded = !mobile || !!state.providersExpanded;
      card.classList.toggle("is-collapsed", !expanded);
      body.style.display = expanded ? "" : "none";
      toggle.style.display = mobile ? "" : "none";
      toggle.textContent = expanded ? "Hide" : "Show";
    };

    const applyResponsiveLayout = () => {
      const el = root.querySelector(".cw-mw");
      if (!el) return;
      const mobile = isPhoneLayout();
      el.classList.toggle("is-mobile", mobile);
      el.classList.toggle("is-search-collapsed", mobile && !!state.selectedItem && !state.searchExpanded);
      syncProviderCardPlacement();
      renderProvidersCard();
    };

    const saveSelection = () => {
      try { localStorage.setItem(STORAGE_KEY, JSON.stringify([...state.selectedProviders])); } catch {}
    };

    const loadRemembered = () => {
      try {
        const raw = JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]");
        return Array.isArray(raw) ? raw.map((v) => String(v || "")) : [];
      } catch {
        return [];
      }
    };

    const selectedTargets = () => state.providers.filter((item) => state.selectedProviders.has(providerKey(item)));
    const enabledActions = () => ({
      history: !!state.actions.history,
      watchlist: !!state.actions.watchlist,
      rating: !!state.actions.rating,
    });
    const hasAnyAction = () => {
      const actions = enabledActions();
      return actions.history || actions.watchlist || actions.rating;
    };
    const providerSupportsSelectedActions = (item) => {
      const actions = enabledActions();
      return !!(
        (actions.history && item.history_enabled) ||
        (actions.watchlist && item.watchlist_enabled) ||
        (actions.rating && item.ratings_enabled)
      );
    };
    const effectiveTargetCount = () => selectedTargets().filter(providerSupportsSelectedActions).length;

    const setStatus = (text = "", tone = "") => {
      state.status = text;
      state.statusTone = tone;
      const el = root.querySelector("[data-role=status]");
      if (!el) return;
      el.textContent = text;
      el.className = `cw-mw-status${tone ? ` ${tone}` : ""}`;
    };

    const updateSubmit = () => {
      const btn = root.querySelector("[data-role=submit]");
      if (!btn) return;
      const blocked = !state.selectedItem || !hasAnyAction() || effectiveTargetCount() === 0 || state.saving || (state.actions.history && state.dateMode === "custom" && !state.watchedOn) || (state.actions.rating && state.rating == null);
      btn.disabled = blocked;
    };

    const renderProviders = () => {
      const note = root.querySelector("[data-role=provider-note]");
      const wrap = root.querySelector("[data-role=providers]");
      if (!wrap || !note) return;
      if (!state.providers.length) {
        wrap.innerHTML = `<div class="cw-mw-empty">No configured providers with history or watchlist support are available yet.</div>`;
        note.textContent = "";
        updateSubmit();
        return;
      }
      note.textContent = state.remembered.length ? "Last-used providers were restored when available." : "Select the configured providers that should receive the item.";
      const renderProviderRow = (item) => {
        const active = state.selectedProviders.has(providerKey(item)) ? " active" : "";
        const badges = [];
        if (item.history_enabled) badges.push(`<span class="cw-mw-badge is-accent">History</span>`);
        if (item.watchlist_enabled) badges.push(`<span class="cw-mw-badge">Watchlist</span>`);
        if (item.ratings_enabled) badges.push(`<span class="cw-mw-badge">Rating</span>`);
        return `
          <label class="cw-mw-provider-row${active}" data-provider-key="${esc(providerKey(item))}">
            <input type="checkbox" class="cw-mw-provider-check" ${active ? "checked" : ""} tabindex="-1" aria-hidden="true">
            <div class="cw-mw-provider-copy">
              <div class="cw-mw-provider-name">${esc(item.display || item.label || item.provider)}</div>
              <div class="cw-mw-provider-badges">${badges.join("")}</div>
            </div>
          </label>
        `;
      };
      const directProviders = state.providers.filter((item) => !MEDIA_SERVER_PROVIDERS.has(String(item?.provider || "").toUpperCase()));
      const mediaServers = state.providers.filter((item) => MEDIA_SERVER_PROVIDERS.has(String(item?.provider || "").toUpperCase()));
      const blocks = [];
      if (directProviders.length) blocks.push(directProviders.map(renderProviderRow).join(""));
      if (mediaServers.length) {
        blocks.push(`
          <details class="cw-mw-provider-group">
            <summary>
              <div>
                <div class="cw-mw-provider-group-title">Media servers</div>
                <div class="cw-mw-provider-group-meta">${mediaServers.length} provider${mediaServers.length === 1 ? "" : "s"}</div>
              </div>
              <span class="material-symbols-rounded cw-mw-provider-group-caret" aria-hidden="true">chevron_right</span>
            </summary>
            <div class="cw-mw-provider-group-body">
              ${mediaServers.map(renderProviderRow).join("")}
            </div>
          </details>
        `);
      }
      wrap.innerHTML = blocks.join("");
      updateSubmit();
    };

    const renderResults = () => {
      const wrap = root.querySelector("[data-role=results]");
      if (!wrap) return;
      if (state.searching) {
        wrap.innerHTML = `<div class="cw-mw-empty">Searching TMDb...</div>`;
        return;
      }
      if (!state.query.trim()) {
        wrap.innerHTML = `<div class="cw-mw-empty">Search TMDb for a movie or show you watched outside your library.</div>`;
        return;
      }
      if (!state.results.length) {
        wrap.innerHTML = `<div class="cw-mw-empty">No results found for this search.</div>`;
        return;
      }
      wrap.innerHTML = state.results.map((item) => `
        <button type="button" class="cw-mw-result" data-result-tmdb="${esc(item.tmdb)}">
          ${renderPoster(item)}
          <div>
            <div class="cw-mw-result-title">${esc(item.title || "Untitled")}</div>
            <div class="cw-mw-result-meta">${esc(mediaChip(item.type))}${item.year ? ` • ${esc(item.year)}` : ""}</div>
            <div class="cw-mw-result-overview">${esc(item.overview || "No overview available.")}</div>
          </div>
        </button>
      `).join("");
    };

    const renderSelected = () => {
      const wrap = root.querySelector("[data-role=selected]");
      if (!wrap) return;
      if (!state.selectedItem) {
        state.searchExpanded = true;
        applyResponsiveLayout();
        wrap.innerHTML = `<div class="cw-mw-empty">Choose a result to continue.</div>`;
        updateSubmit();
        return;
      }
      applyResponsiveLayout();
      const mobileAction = isPhoneLayout()
        ? `<button type="button" class="cw-mw-link" data-role="change-selection">Change selection</button>`
        : "";
      wrap.innerHTML = `
        <div class="cw-mw-selected">
          ${renderPoster(state.selectedItem, "cw-mw-selected-poster")}
          <div class="cw-mw-selected-copy">
            <div class="cw-mw-selected-head">
              <div class="cw-mw-row">
                <div class="cw-mw-selected-title">${esc(state.selectedItem.title || "Untitled")}</div>
                <span class="cw-mw-badge">${esc(mediaChip(state.selectedItem.type))}</span>
                ${state.selectedItem.year ? `<span class="cw-mw-badge">${esc(state.selectedItem.year)}</span>` : ""}
              </div>
              ${mobileAction}
            </div>
            <div class="cw-mw-selected-overview">${esc(state.selectedItem.overview || "No overview available.")}</div>
          </div>
        </div>
      `;
      updateSubmit();
    };

    const renderDate = () => {
      const pills = root.querySelectorAll("[data-date-mode]");
      pills.forEach((el) => el.classList.toggle("active", el.getAttribute("data-date-mode") === state.dateMode));
      const wrap = root.querySelector("[data-role=custom-date-wrap]");
      if (wrap) wrap.classList.toggle("hidden", state.dateMode !== "custom");
      const dateInput = root.querySelector("[data-role=custom-date]");
      if (dateInput) dateInput.value = state.watchedOn || todayLocal();
      updateSubmit();
    };

    const renderRating = () => {
      const slider = root.querySelector("[data-role=rating-slider]");
      const value = root.querySelector("[data-role=rating-value]");
      const numeric = state.rating == null ? 0 : Number(state.rating);
      if (slider) {
        slider.value = String(numeric);
        slider.style.setProperty("--rating-progress", `${(numeric / 10) * 100}%`);
      }
      if (value) value.textContent = numeric === 0 ? "None" : String(numeric);
      updateSubmit();
    };

    const renderActions = () => {
      root.querySelectorAll("[data-action]").forEach((el) => {
        const key = String(el.getAttribute("data-action") || "");
        const active = !!state.actions[key];
        el.checked = active;
        el.closest(".cw-mw-provider-row")?.classList.toggle("active", active);
      });
      const whenCard = root.querySelector("[data-role=when-card]");
      if (whenCard) whenCard.style.opacity = state.actions.history ? "1" : ".62";
      root.querySelectorAll("[data-date-mode]").forEach((el) => { el.disabled = !state.actions.history; });
      const dateInput = root.querySelector("[data-role=custom-date]");
      if (dateInput) dateInput.disabled = !state.actions.history;
      const ratingCard = root.querySelector("[data-role=rating-card]");
      if (ratingCard) ratingCard.style.opacity = state.actions.rating ? "1" : ".62";
      const ratingSlider = root.querySelector("[data-role=rating-slider]");
      if (ratingSlider) ratingSlider.disabled = !state.actions.rating;
      updateSubmit();
    };

    const search = async () => {
      const q = state.query.trim();
      if (q.length < 2) {
        state.results = [];
        state.searching = false;
        renderResults();
        return;
      }
      state.searching = true;
      renderResults();
      setStatus("");
      try {
        const data = await fjson(`/api/metadata/search?q=${encodeURIComponent(q)}&typ=${encodeURIComponent(state.type)}&limit=12`);
        state.results = Array.isArray(data.results) ? data.results : [];
      } catch (err) {
        state.results = [];
        setStatus(String(err?.message || "Search failed"), "error");
      } finally {
        state.searching = false;
        renderResults();
      }
    };

    let searchTimer = 0;
    const queueSearch = () => {
      window.clearTimeout(searchTimer);
      searchTimer = window.setTimeout(search, 240);
    };

    const loadProviders = async () => {
      try {
        const data = await fjson("/api/manual/providers");
        state.providers = Array.isArray(data.providers) ? data.providers : [];
        state.remembered = loadRemembered();
        const allowed = new Set(state.providers.map(providerKey));
        const rememberedHits = state.remembered.filter((key) => allowed.has(key));
        state.selectedProviders = new Set(rememberedHits);
        renderProviders();
      } catch (err) {
        state.providers = [];
        state.selectedProviders = new Set();
        renderProviders();
        setStatus(String(err?.message || "Failed to load providers"), "error");
      }
    };

    const submit = async () => {
      if (!state.selectedItem || !state.selectedProviders.size || state.saving) return;
      state.saving = true;
      updateSubmit();
      setStatus("Saving watched entry...");

      const selectedProviders = state.providers
        .filter((item) => state.selectedProviders.has(providerKey(item)))
        .map((item) => ({ provider: item.provider, instance: item.instance }));

      try {
        const data = await fjson("/api/manual/watched", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            item: state.selectedItem,
            date_mode: state.dateMode,
            watched_on: state.dateMode === "custom" ? state.watchedOn : null,
            actions: enabledActions(),
            rating: state.rating,
            providers: selectedProviders,
          }),
        });
        saveSelection();
        setStatus(`Saved to ${effectiveTargetCount()} provider${effectiveTargetCount() === 1 ? "" : "s"}.`, "success");
        window.dispatchEvent(new CustomEvent("cw:manual-watched-saved", { detail: data }));
        window.setTimeout(() => window.cxCloseModal?.(), 520);
      } catch (err) {
        setStatus(String(err?.message || "Save failed"), "error");
      } finally {
        state.saving = false;
        updateSubmit();
      }
    };

    root.innerHTML = `
      <div class="cw-mw">
        <div class="cx-head">
          <div class="cw-mw-head">
            <div class="cw-mw-orb"><span class="material-symbols-rounded">theaters</span></div>
            <div>
              <div class="cw-mw-title">Quick Add Item</div>
              <div class="cw-mw-sub">Manually add a movie or show and send it to your selected providers.</div>
            </div>
          </div>
        </div>
        <div class="cx-body">
          <section class="cw-mw-col cw-mw-col-search">
            <div class="cw-mw-panel is-search">
            <div class="cw-mw-card">
              <div class="cw-mw-label">Search TMDb</div>
              <div class="cw-mw-row" style="margin-top:10px">
                <div class="cw-mw-pills">
                  <button type="button" class="cw-mw-pill active" data-type="movie">Movies</button>
                  <button type="button" class="cw-mw-pill" data-type="show">Shows</button>
                </div>
              </div>
              <div class="cw-mw-search-row" style="margin-top:10px">
                <input class="cw-mw-input" data-role="query" placeholder="Search title..." autocomplete="off">
                <button type="button" class="cw-mw-btn" data-role="search">Search</button>
              </div>
            </div>
            <div class="cw-mw-results" data-role="results"></div>
            <div data-role="providers-anchor-left"></div>
            </div>
          </section>
          <section class="cw-mw-col cw-mw-col-side">
            <div class="cw-mw-card">
              <div class="cw-mw-label">Selected Item</div>
              <div data-role="selected" style="margin-top:10px"></div>
            </div>
            <div class="cw-mw-card">
              <div class="cw-mw-label">Actions</div>
              <div style="display:grid;gap:8px;margin-top:10px">
                <label class="cw-mw-provider-row" style="grid-template-columns:20px minmax(0,1fr)">
                  <input type="checkbox" class="cw-mw-provider-check" data-action="history">
                  <div class="cw-mw-provider-copy">
                    <div class="cw-mw-provider-name">History</div>
                    <div class="cw-mw-muted">Mark the selected item as watched</div>
                  </div>
                </label>
                <label class="cw-mw-provider-row" style="grid-template-columns:20px minmax(0,1fr)">
                  <input type="checkbox" class="cw-mw-provider-check" data-action="watchlist">
                  <div class="cw-mw-provider-copy">
                    <div class="cw-mw-provider-name">Watchlist</div>
                    <div class="cw-mw-muted">Add the selected item to watchlists</div>
                  </div>
                </label>
                <label class="cw-mw-provider-row" style="grid-template-columns:20px minmax(0,1fr)">
                  <input type="checkbox" class="cw-mw-provider-check" data-action="rating">
                  <div class="cw-mw-provider-copy">
                    <div class="cw-mw-provider-name">Rating</div>
                    <div class="cw-mw-muted">Send a score to providers</div>
                  </div>
                </label>
              </div>
            </div>
            <div class="cw-mw-card" data-role="when-card">
              <div class="cw-mw-label">When Watched</div>
              <div class="cw-mw-pills" style="margin-top:10px">
                <button type="button" class="cw-mw-pill active" data-date-mode="today">Today</button>
                <button type="button" class="cw-mw-pill" data-date-mode="release">Release date</button>
                <button type="button" class="cw-mw-pill" data-date-mode="custom">Choose date</button>
              </div>
              <div class="cw-mw-date-wrap hidden" data-role="custom-date-wrap">
                <input type="date" class="cw-mw-date" data-role="custom-date">
              </div>
            </div>
            <div data-role="providers-anchor-side"></div>
            <div class="cw-mw-card" data-role="providers-card">
              <div class="cw-mw-actions">
                <div>
                  <div class="cw-mw-label">Send To</div>
                  <div class="cw-mw-muted" data-role="provider-note" style="margin-top:6px"></div>
                </div>
                <div class="cw-mw-provider-tools">
                  <button type="button" class="cw-mw-link" data-role="providers-toggle">Show</button>
                  <button type="button" class="cw-mw-link" data-role="use-last">Use last</button>
                  <button type="button" class="cw-mw-link" data-role="select-all">Select all</button>
                  <button type="button" class="cw-mw-link" data-role="clear-providers">Clear</button>
                </div>
              </div>
              <div data-role="providers-body">
                <div class="cw-mw-provider-list" data-role="providers" style="margin-top:12px"></div>
              </div>
            </div>
            <div class="cw-mw-card" data-role="rating-card">
              <div class="cw-mw-label">Rating</div>
              <div class="cw-mw-rating-wrap" style="margin-top:10px">
                <div class="cw-mw-rating-top">
                  <div class="cw-mw-muted">Enable Rating in Actions, then slide from none to 10.</div>
                  <div class="cw-mw-rating-value" data-role="rating-value">None</div>
                </div>
                <div class="cw-mw-rating-slider-wrap">
                  <input type="range" min="0" max="10" step="1" value="0" class="cw-mw-rating-slider" data-role="rating-slider" aria-label="Optional rating">
                  <div class="cw-mw-rating-scale" aria-hidden="true">
                    <span class="cw-mw-rating-mark">-</span>
                    ${Array.from({ length: 10 }, (_, i) => `<span class="cw-mw-rating-mark">${i + 1}</span>`).join("")}
                  </div>
                </div>
              </div>
            </div>
          </section>
        </div>
        <div class="cx-foot">
          <div class="cw-mw-status" data-role="status"></div>
          <div class="cw-mw-row">
            <button type="button" class="cw-mw-btn" data-role="close-foot">Cancel</button>
            <button type="button" class="cw-mw-btn primary" data-role="submit">Send to providers</button>
          </div>
        </div>
      </div>
    `;

    root.querySelectorAll("[data-role=close-foot]").forEach((el) => {
      el.addEventListener("click", () => window.cxCloseModal?.());
    });

    root.querySelector("[data-role=query]")?.addEventListener("input", (e) => {
      state.query = e.currentTarget.value || "";
      queueSearch();
    });
    root.querySelector("[data-role=search]")?.addEventListener("click", search);
    root.querySelector("[data-role=providers-toggle]")?.addEventListener("click", () => {
      if (!isPhoneLayout()) return;
      state.providersExpanded = !state.providersExpanded;
      renderProvidersCard();
    });

    root.querySelectorAll("[data-type]").forEach((btn) => {
      btn.addEventListener("click", () => {
        state.type = btn.getAttribute("data-type") || "movie";
        root.querySelectorAll("[data-type]").forEach((el) => el.classList.toggle("active", el === btn));
        queueSearch();
      });
    });

    root.querySelectorAll("[data-date-mode]").forEach((btn) => {
      btn.addEventListener("click", () => {
        state.dateMode = btn.getAttribute("data-date-mode") || "today";
        renderDate();
      });
    });

    root.querySelector("[data-role=custom-date]")?.addEventListener("input", (e) => {
      state.watchedOn = e.currentTarget.value || "";
      updateSubmit();
    });

    root.querySelector("[data-role=rating-slider]")?.addEventListener("input", (e) => {
      const raw = Number(e.currentTarget.value || 0);
      state.rating = raw <= 0 ? null : raw;
      renderRating();
    });
    root.querySelectorAll("[data-action]").forEach((el) => {
      el.addEventListener("change", (e) => {
        const key = String(e.currentTarget.getAttribute("data-action") || "");
        if (!key) return;
        state.actions[key] = !!e.currentTarget.checked;
        renderActions();
      });
    });

    root.addEventListener("click", (e) => {
      const resultBtn = e.target.closest("[data-result-tmdb]");
      if (resultBtn) {
        const tmdb = String(resultBtn.getAttribute("data-result-tmdb") || "");
        state.selectedItem = state.results.find((item) => String(item.tmdb) === tmdb) || null;
        if (state.selectedItem && isPhoneLayout()) state.searchExpanded = false;
        renderSelected();
        return;
      }

      const changeSelectionBtn = e.target.closest("[data-role=change-selection]");
      if (changeSelectionBtn) {
        state.searchExpanded = true;
        applyResponsiveLayout();
        const searchInput = root.querySelector("[data-role=query]");
        searchInput?.focus();
        root.querySelector(".cw-mw-col-search")?.scrollIntoView({ behavior: "smooth", block: "start" });
        return;
      }

      const providerBtn = e.target.closest("[data-provider-key]");
      if (providerBtn) {
        const key = providerBtn.getAttribute("data-provider-key") || "";
        if (state.selectedProviders.has(key)) state.selectedProviders.delete(key);
        else state.selectedProviders.add(key);
        renderProviders();
        return;
      }
    });

    root.querySelector("[data-role=use-last]")?.addEventListener("click", () => {
      const allowed = new Set(state.providers.map(providerKey));
      const next = state.remembered.filter((key) => allowed.has(key));
      if (next.length) state.selectedProviders = new Set(next);
      renderProviders();
    });
    root.querySelector("[data-role=select-all]")?.addEventListener("click", () => {
      state.selectedProviders = new Set(state.providers.map(providerKey));
      renderProviders();
    });
    root.querySelector("[data-role=clear-providers]")?.addEventListener("click", () => {
      state.selectedProviders = new Set();
      renderProviders();
    });
    root.querySelector("[data-role=submit]")?.addEventListener("click", submit);

    renderResults();
    renderSelected();
    renderDate();
    renderRating();
    renderActions();
    applyResponsiveLayout();
    window.addEventListener("resize", applyResponsiveLayout, { passive: true });
    await loadProviders();
  },
  unmount() {},
};
