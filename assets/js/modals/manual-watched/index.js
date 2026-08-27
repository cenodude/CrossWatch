/* assets/js/modals/manual-watched/index.js */
/* Manual watched item modal */

const STORAGE_KEY = "cw.manualWatched.providers";
const MEDIA_SERVER_PROVIDERS = new Set(["PLEX", "JELLYFIN", "EMBY"]);
const STEPS = [
  ["Search", "Find your movie or show"],
  ["Actions", "Choose what to do"],
  ["Providers", "Select where to send it"],
  ["Details", "Dates and rating"],
  ["Review", "Confirm and send"],
];
const ACTIONS = [
  ["history", "History", "history", "Mark the item as watched", "history_enabled"],
  ["watchlist", "Watchlist", "bookmark_add", "Add the item to watchlists", "watchlist_enabled"],
  ["rating", "Rating", "star", "Send a score to providers", "ratings_enabled"],
];

const fjson = async (url, opts = {}) => {
  const r = await fetch(url, { cache: "no-store", ...opts });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(data?.error || `${r.status}`);
  return data || {};
};

const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m]));
const providerKey = (item) => `${String(item?.provider || "").toUpperCase()}:${String(item?.instance || "default")}`;
const mediaLabel = (type) => String(type || "").toLowerCase() === "show" ? "Show" : "Movie";
const artType = (type) => String(type || "").toLowerCase() === "show" ? "tv" : "movie";
const todayLocal = () => new Date().toISOString().slice(0, 10);
const closeModal = () => window.cxCloseModal?.();
let activeCleanup = null;
let closeShieldCleanup = null;

const installCloseShield = () => {
  closeShieldCleanup?.();
  const shield = document.createElement("div");
  const until = Date.now() + 900;
  window.__cwSuppressUiModeUntil = Math.max(Number(window.__cwSuppressUiModeUntil || 0), until);
  const swallow = (e) => {
    if (Date.now() > until) return cleanup();
    e.preventDefault();
    e.stopPropagation();
    e.stopImmediatePropagation?.();
  };
  const cleanup = () => {
    events.forEach((name) => document.removeEventListener(name, swallow, true));
    shield.remove();
    if (closeShieldCleanup === cleanup) closeShieldCleanup = null;
  };
  const events = ["pointerdown", "pointerup", "mousedown", "mouseup", "touchstart", "touchend", "click"];
  Object.assign(shield.style, {
    position: "fixed",
    inset: "0",
    zIndex: "2147483647",
    background: "transparent",
    pointerEvents: "auto",
    touchAction: "none",
  });
  shield.setAttribute("aria-hidden", "true");
  document.body?.appendChild(shield);
  events.forEach((name) => document.addEventListener(name, swallow, true));
  window.setTimeout(cleanup, 900);
  closeShieldCleanup = cleanup;
};

const closeModalAfterTap = () => {
  installCloseShield();
  window.setTimeout(closeModal, 80);
};

export default {
  async mount(root) {
    const shell = root.closest(".cx-modal-shell");
    shell?.classList.add("cw-manual-watched-modal");
    root.style.setProperty("--cxModalMaxW", "1360px");
    root.style.setProperty("--cxModalMaxH", "88vh");
    activeCleanup?.();

    const updateVisualViewport = () => {
      const height = Math.max(
        320,
        Math.round(window.visualViewport?.height || window.innerHeight || document.documentElement.clientHeight || 0)
      );
      if (height) root.style.setProperty("--mwVisualViewportH", `${height}px`);
    };
    updateVisualViewport();
    window.visualViewport?.addEventListener("resize", updateVisualViewport, { passive: true });
    window.visualViewport?.addEventListener("scroll", updateVisualViewport, { passive: true });
    window.addEventListener("resize", updateVisualViewport, { passive: true });
    activeCleanup = () => {
      window.visualViewport?.removeEventListener("resize", updateVisualViewport);
      window.visualViewport?.removeEventListener("scroll", updateVisualViewport);
      window.removeEventListener("resize", updateVisualViewport);
      root.style.removeProperty("--mwVisualViewportH");
    };

    const state = {
      step: 0,
      type: "movie",
      providers: [],
      selectedProviders: new Set(),
      remembered: [],
      query: "",
      results: [],
      searching: false,
      selectedItem: null,
      dateMode: "today",
      watchedOn: todayLocal(),
      actions: { history: true, watchlist: false, rating: false },
      rating: 8,
      saving: false,
      status: "",
      statusTone: "",
    };

    const PM = window.CW?.ProviderMeta;
    const logoHtml = (p) => {
      const src = PM?.logLogoPath?.(p) || PM?.logoPath?.(p) || "";
      const label = PM?.label?.(p) || p;
      return src ? `<img class="cw-mw-provider-logo" src="${esc(src)}" alt="">` : `<span>${esc(String(label).slice(0, 2).toUpperCase())}</span>`;
    };
    const loadRemembered = () => {
      try {
        const raw = JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]");
        return Array.isArray(raw) ? raw.map((v) => String(v || "")) : [];
      } catch {
        return [];
      }
    };
    const saveSelection = () => {
      try { localStorage.setItem(STORAGE_KEY, JSON.stringify([...state.selectedProviders])); } catch {}
    };
    const renderPoster = (item) => !item?.tmdb
      ? `<div class="cw-mw-poster"><div class="cw-mw-ghost">No art</div></div>`
      : `<div class="cw-mw-poster"><img src="/art/tmdb/${artType(item.type)}/${encodeURIComponent(String(item.tmdb))}?size=w185" alt=""></div>`;
    const selectedTargets = () => state.providers.filter((item) => state.selectedProviders.has(providerKey(item)));
    const selectedActionKeys = () => ACTIONS.map(([key]) => key).filter((key) => !!state.actions[key]);
    const supportsAction = (item, key) => {
      const action = ACTIONS.find(([k]) => k === key);
      return !!(action && item?.[action[4]]);
    };
    const actionScope = () => selectedTargets().length ? selectedTargets() : state.providers;
    const availableActions = () => ACTIONS.filter(([key]) => actionScope().some((p) => supportsAction(p, key)));
    const providerCompatible = (item) => selectedActionKeys().every((key) => supportsAction(item, key));
    const compatibleProviders = () => state.providers.filter(providerCompatible);
    const selectedCompatibleTargets = () => selectedTargets().filter(providerCompatible);
    const actionLabels = () => selectedActionKeys().map((key) => ACTIONS.find(([k]) => k === key)?.[1] || key);
    const normalizeActions = () => {
      const allowed = new Set(availableActions().map(([key]) => key));
      ACTIONS.forEach(([key]) => { if (!allowed.has(key)) state.actions[key] = false; });
      if (!selectedActionKeys().length && allowed.size) state.actions[[...allowed][0]] = true;
      for (const key of [...state.selectedProviders]) {
        const item = state.providers.find((p) => providerKey(p) === key);
        if (!item || !providerCompatible(item)) state.selectedProviders.delete(key);
      }
      if (!state.actions.rating) state.rating = 8;
    };
    const dateText = () => state.dateMode === "custom" ? (state.watchedOn || "Choose date") : state.dateMode === "release" ? "Release date" : "Today";
    const validation = () => {
      if (!state.selectedItem) return "Select a movie or show first.";
      if (!selectedActionKeys().length) return "Select at least one action.";
      if (!selectedCompatibleTargets().length) return "Select at least one compatible provider.";
      if (state.actions.history && state.dateMode === "custom" && !state.watchedOn) return "Choose a watched date.";
      if (state.actions.rating && !(Number(state.rating) >= 1 && Number(state.rating) <= 10)) return "Choose a rating from 1 to 10.";
      return "";
    };
    const stepReady = (idx) => {
      if (idx <= 0) return true;
      if (!state.selectedItem) return false;
      if (idx <= 1) return true;
      if (!selectedActionKeys().length) return false;
      if (idx <= 2) return true;
      if (!selectedCompatibleTargets().length) return false;
      if (idx <= 3) return true;
      return !validation();
    };
    const maxStep = () => STEPS.findIndex((_, i) => !stepReady(i)) < 0 ? STEPS.length - 1 : Math.max(0, STEPS.findIndex((_, i) => !stepReady(i)) - 1);
    const setStatus = (text = "", tone = "") => {
      state.status = text;
      state.statusTone = tone;
      render();
    };
    const setStep = (idx) => {
      state.step = Math.max(0, Math.min(STEPS.length - 1, idx));
      render();
    };

    const selectedItemHtml = (compact = false) => state.selectedItem ? `
      <div class="cw-mw-selected ${compact ? "is-compact" : ""}">
        ${renderPoster(state.selectedItem)}
        <div>
          <div class="cw-mw-selected-title">
            <span>${esc(state.selectedItem.title || "Untitled")}</span>
            <span class="cw-mw-badge">${esc(mediaLabel(state.selectedItem.type))}</span>
            ${state.selectedItem.year ? `<span class="cw-mw-badge">${esc(state.selectedItem.year)}</span>` : ""}
          </div>
          <div class="cw-mw-overview">${esc(state.selectedItem.overview || "No overview available.")}</div>
        </div>
      </div>
    ` : `<div class="cw-mw-empty">No item selected yet.</div>`;

    const searchStep = () => `
      <div class="cw-mw-form">
        <div><div class="cw-mw-headline"><span>1.</span><b>Search, find and select a movie or show</b></div><div class="cw-mw-copy">Search TMDb and pick the exact item you want to add.</div></div>
        <div class="cw-mw-media-toggle">
          <button type="button" class="cw-mw-chip ${state.type === "movie" ? "active" : ""}" data-type="movie">Movies</button>
          <button type="button" class="cw-mw-chip ${state.type === "show" ? "active" : ""}" data-type="show">Shows</button>
        </div>
        <div class="cw-mw-search">
          <input class="cw-mw-input" data-role="query" type="search" placeholder="Search title..." autocomplete="off" autocapitalize="none" spellcheck="false" inputmode="search" enterkeyhint="search" value="${esc(state.query)}">
          <button type="button" class="cw-mw-btn primary" data-role="search"><span class="material-symbols-rounded">search</span><span>Search</span></button>
        </div>
        <div class="cw-mw-results">${resultsHtml()}</div>
      </div>
    `;

    const resultsHtml = () => {
      if (state.searching) return `<div class="cw-mw-empty">Searching TMDb...</div>`;
      if (!state.query.trim()) return `<div class="cw-mw-empty">Search TMDb for a movie or show.</div>`;
      if (!state.results.length) return `<div class="cw-mw-empty">No results found.</div>`;
      return state.results.map((item) => {
        const active = state.selectedItem && String(state.selectedItem.tmdb) === String(item.tmdb) && state.selectedItem.type === item.type;
        return `<button type="button" class="cw-mw-result ${active ? "active" : ""}" data-result-tmdb="${esc(item.tmdb)}">
          ${renderPoster(item)}
          <div>
            <div class="cw-mw-result-title"><span>${esc(item.title || "Untitled")}</span><span class="cw-mw-badge">${esc(mediaLabel(item.type))}</span></div>
            <div class="cw-mw-meta">${item.year ? esc(item.year) : "Unknown year"}</div>
            <div class="cw-mw-overview">${esc(item.overview || "No overview available.")}</div>
          </div>
          <span class="cw-mw-radio"><span class="material-symbols-rounded">check</span></span>
        </button>`;
      }).join("");
    };

    const actionsStep = () => {
      normalizeActions();
      const rows = availableActions().map(([key, label, icon, note]) => `
        <button type="button" class="cw-mw-action ${state.actions[key] ? "active" : ""}" data-action="${esc(key)}" aria-pressed="${state.actions[key] ? "true" : "false"}">
          <span class="cw-mw-action-icon"><span class="material-symbols-rounded">${esc(icon)}</span></span>
          <span class="cw-mw-action-copy"><span class="cw-mw-action-title">${esc(label)}</span><span class="cw-mw-muted">${esc(note)}</span></span>
        </button>
      `).join("");
      return `<div class="cw-mw-form">
        <div><div class="cw-mw-headline"><span>2.</span><b>Actions</b></div><div class="cw-mw-copy">Select History, Watchlist, Rating, or a valid combination.</div></div>
        ${selectedItemHtml()}
        <div class="cw-mw-actions-grid">${rows || `<div class="cw-mw-empty">No configured provider supports Quick Add actions.</div>`}</div>
      </div>`;
    };

    const providersStep = () => {
      normalizeActions();
      const providers = compatibleProviders();
      const direct = providers.filter((item) => !MEDIA_SERVER_PROVIDERS.has(String(item.provider || "").toUpperCase()));
      const servers = providers.filter((item) => MEDIA_SERVER_PROVIDERS.has(String(item.provider || "").toUpperCase()));
      const rows = [...direct, ...servers].map(providerRow).join("");
      return `<div class="cw-mw-form">
        <div><div class="cw-mw-headline"><span>3.</span><b>Providers</b></div><div class="cw-mw-copy">Select configured providers compatible with ${esc(actionLabels().join(", ") || "your actions")}.</div></div>
        <div class="cw-mw-tools">
          <div class="cw-mw-muted">${selectedCompatibleTargets().length} selected</div>
          <div class="cw-mw-row">
            <button type="button" class="cw-mw-link" data-role="use-last">Use last</button>
            <button type="button" class="cw-mw-link" data-role="select-all">Select all</button>
            <button type="button" class="cw-mw-link" data-role="clear-providers">Clear</button>
          </div>
        </div>
        <div class="cw-mw-providers">${rows || `<div class="cw-mw-empty">No configured provider supports this action combination.</div>`}</div>
      </div>`;
    };

    const providerRow = (item) => {
      const key = providerKey(item);
      const active = state.selectedProviders.has(key);
      const logo = PM?.logLogoPath?.(item.provider) || PM?.logoPath?.(item.provider) || "";
      const badges = ACTIONS.filter(([action]) => supportsAction(item, action)).map(([, label]) => `<span class="cw-mw-badge">${esc(label)}</span>`).join("");
      return `<button type="button" class="cw-mw-provider ${active ? "active" : ""}" data-provider-key="${esc(key)}" aria-pressed="${active ? "true" : "false"}" ${logo ? `style="--mw-provider-wm:url(&quot;${esc(logo)}&quot;)"` : ""}>
        <span class="cw-mw-provider-icon">${logoHtml(item.provider)}</span>
        <span class="cw-mw-provider-copy">
          <span class="cw-mw-provider-title">${esc(item.display || item.label || item.provider)}</span>
          <span class="cw-mw-provider-badges">${badges}</span>
        </span>
      </button>`;
    };

    const detailsStep = () => `
      <div class="cw-mw-form">
        <div><div class="cw-mw-headline"><span>4.</span><b>Details</b></div><div class="cw-mw-copy">Configure watched date and rating only when relevant.</div></div>
        ${selectedItemHtml()}
        <div class="cw-mw-details-grid">
          ${state.actions.history ? `<div class="cw-mw-card">
            <label class="cw-mw-label">Watched date</label>
            <div class="cw-mw-row">
              <button type="button" class="cw-mw-chip ${state.dateMode === "today" ? "active" : ""}" data-date-mode="today">Today</button>
              <button type="button" class="cw-mw-chip ${state.dateMode === "release" ? "active" : ""}" data-date-mode="release">Release date</button>
              <button type="button" class="cw-mw-chip ${state.dateMode === "custom" ? "active" : ""}" data-date-mode="custom">Choose date</button>
            </div>
            ${state.dateMode === "custom" ? `<div style="margin-top:12px"><input type="date" class="cw-mw-date" data-role="custom-date" value="${esc(state.watchedOn || "")}"></div>` : ""}
          </div>` : ""}
          ${state.actions.rating ? `<div class="cw-mw-card">
            <label class="cw-mw-label">Rating</label>
            <div class="cw-mw-row" style="justify-content:space-between;margin-bottom:12px"><span class="cw-mw-muted">Score from 1 to 10.</span><span class="cw-mw-rating-value">${esc(state.rating)}</span></div>
            <input type="range" min="1" max="10" step="1" class="cw-mw-slider" data-role="rating" value="${esc(state.rating)}" style="--rating-progress:${(Number(state.rating || 1) / 10) * 100}%">
          </div>` : ""}
        </div>
        ${!state.actions.history && !state.actions.rating ? `<div class="cw-mw-empty">No extra details are needed for Watchlist only.</div>` : ""}
      </div>
    `;

    const reviewStep = () => `
      <div class="cw-mw-form cw-mw-form-review">
        <div><div class="cw-mw-headline"><span>5.</span><b>Review</b></div><div class="cw-mw-copy">Check the final item, actions, providers, dates, and rating before sending.</div></div>
        ${selectedItemHtml(true)}
        <div class="cw-mw-review">
          <div class="cw-mw-review-row"><div class="cw-mw-review-k">Actions</div><div class="cw-mw-review-v">${actionLabels().map((x) => `<span class="cw-mw-badge">${esc(x)}</span>`).join("")}</div></div>
          <div class="cw-mw-review-row"><div class="cw-mw-review-k">Providers</div><div class="cw-mw-review-v">${selectedCompatibleTargets().map((p) => `<span class="cw-mw-badge">${esc(p.display || p.label || p.provider)}</span>`).join("")}</div></div>
          <div class="cw-mw-review-row"><div class="cw-mw-review-k">Dates</div><div class="cw-mw-review-v">${state.actions.history ? esc(dateText()) : "Not needed"}</div></div>
          <div class="cw-mw-review-row"><div class="cw-mw-review-k">Rating</div><div class="cw-mw-review-v">${state.actions.rating ? esc(`${state.rating}/10`) : "Not included"}</div></div>
        </div>
      </div>
    `;

    const stepContent = () => [searchStep, actionsStep, providersStep, detailsStep, reviewStep][state.step]();

    const render = (focusQuery = false) => {
      normalizeActions();
      const allowed = maxStep();
      if (state.step > allowed) state.step = allowed;
      const err = validation();
      root.innerHTML = `<div class="cw-mw" data-step="${state.step}">
        <div class="cx-head">
          <div class="cw-mw-head">
            <span class="cw-mw-head-icon"><span class="material-symbols-rounded">add_circle</span></span>
            <div><div class="cw-mw-title">Quick Add Item</div><div class="cw-mw-sub">Add a movie or show to your providers in a few simple steps.</div></div>
          </div>
          <button type="button" class="cw-mw-close" data-role="close" aria-label="Close"><span class="material-symbols-rounded">close</span></button>
        </div>
        <div class="cw-mw-body">
          <nav class="cw-mw-steps" aria-label="Quick Add steps">
            ${STEPS.map(([title, sub], i) => `<button type="button" class="cw-mw-step ${i === state.step ? "active" : ""} ${i < state.step && stepReady(i + 1) ? "done" : ""}" data-step="${i}" ${i > allowed ? "disabled" : ""}>
              <span class="cw-mw-step-num">${i + 1}</span><span><span class="cw-mw-step-title">${esc(title)}</span><span class="cw-mw-step-sub">${esc(sub)}</span></span>
            </button>`).join("")}
          </nav>
          <main class="cw-mw-main"><section class="cw-mw-stage"><div class="cw-mw-stage-shell">${stepContent()}</div></section></main>
        </div>
        <footer class="cw-mw-foot">
          <div class="cw-mw-status ${state.statusTone || (err ? "error" : "")}">${esc(state.status || (state.step === 4 ? err : ""))}</div>
          <div class="cw-mw-foot-actions">
            <button type="button" class="cw-mw-btn" data-role="cancel">Cancel</button>
            <button type="button" class="cw-mw-btn" data-role="prev" ${state.step === 0 || state.saving ? "disabled" : ""}>Previous</button>
            <button type="button" class="cw-mw-btn primary" data-role="next" ${state.step >= 4 || !stepReady(state.step + 1) || state.saving ? "disabled" : ""}>Next<span class="material-symbols-rounded">chevron_right</span></button>
            <button type="button" class="cw-mw-btn primary" data-role="send" ${state.step !== 4 || !!err || state.saving ? "disabled" : ""}>${state.saving ? "Sending..." : "Send"}</button>
          </div>
        </footer>
      </div>`;
      if (focusQuery) {
        const q = root.querySelector("[data-role=query]");
        q?.focus?.({ preventScroll: true });
        try { q?.setSelectionRange?.(q.value.length, q.value.length); } catch {}
      }
    };

    const syncFooter = () => {
      const err = validation();
      const status = root.querySelector(".cw-mw-status");
      if (status) {
        status.className = `cw-mw-status ${state.statusTone || (err && state.step === 4 ? "error" : "")}`.trim();
        status.textContent = state.status || (state.step === 4 ? err : "");
      }
      const prev = root.querySelector("[data-role=prev]");
      const next = root.querySelector("[data-role=next]");
      const send = root.querySelector("[data-role=send]");
      if (prev) prev.disabled = state.step === 0 || state.saving;
      if (next) next.disabled = state.step >= 4 || !stepReady(state.step + 1) || state.saving;
      if (send) send.disabled = state.step !== 4 || !!err || state.saving;
    };

    const syncSearchDom = (focusQuery = false) => {
      if (state.step !== 0 || !root.querySelector(".cw-mw-results")) return render(focusQuery);
      const results = root.querySelector(".cw-mw-results");
      const searchBtn = root.querySelector("[data-role=search]");
      if (results) results.innerHTML = resultsHtml();
      if (searchBtn) {
        searchBtn.disabled = !!state.searching;
        searchBtn.setAttribute("aria-busy", state.searching ? "true" : "false");
      }
      syncFooter();
      if (focusQuery) {
        const q = root.querySelector("[data-role=query]");
        q?.focus?.({ preventScroll: true });
        try { q?.setSelectionRange?.(q.value.length, q.value.length); } catch {}
      }
    };

    let searchSeq = 0;
    const search = async () => {
      const q = state.query.trim();
      if (q.length < 2) {
        searchSeq += 1;
        state.results = [];
        state.searching = false;
        syncSearchDom(true);
        return;
      }
      const seq = ++searchSeq;
      state.searching = true;
      syncSearchDom(root.querySelector("[data-role=query]") === document.activeElement);
      try {
        const data = await fjson(`/api/metadata/search?q=${encodeURIComponent(q)}&typ=${encodeURIComponent(state.type)}&limit=12`);
        if (seq !== searchSeq || q !== state.query.trim()) return;
        if (data?.ok === false) throw new Error(data.error || "Search failed");
        state.results = Array.isArray(data.results) ? data.results : [];
        state.status = "";
        state.statusTone = "";
      } catch (err) {
        if (seq !== searchSeq) return;
        state.results = [];
        state.status = String(err?.message || "Search failed");
        state.statusTone = "error";
      } finally {
        if (seq !== searchSeq) return;
        state.searching = false;
        syncSearchDom(false);
      }
    };

    let searchTimer = 0;
    const queueSearch = () => {
      window.clearTimeout(searchTimer);
      searchTimer = window.setTimeout(search, 260);
    };

    const loadProviders = async () => {
      try {
        const data = await fjson("/api/manual/providers");
        state.providers = Array.isArray(data.providers) ? data.providers : [];
        state.remembered = loadRemembered();
        const allowed = new Set(state.providers.map(providerKey));
        state.selectedProviders = new Set(state.remembered.filter((key) => allowed.has(key)));
        normalizeActions();
      } catch (err) {
        state.providers = [];
        state.selectedProviders = new Set();
        state.status = String(err?.message || "Failed to load providers");
        state.statusTone = "error";
      }
      if (state.step === 0 && root.querySelector(".cw-mw-results")) {
        syncSearchDom(root.querySelector("[data-role=query]") === document.activeElement);
      } else {
        render();
      }
    };

    const submit = async () => {
      const err = validation();
      if (err || state.saving) return setStatus(err, "error");
      state.saving = true;
      render();
      const providers = selectedCompatibleTargets().map((item) => ({ provider: item.provider, instance: item.instance }));
      try {
        const data = await fjson("/api/manual/watched", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            item: state.selectedItem,
            date_mode: state.actions.history ? state.dateMode : "today",
            watched_on: state.actions.history && state.dateMode === "custom" ? state.watchedOn : null,
            actions: {
              history: !!state.actions.history,
              watchlist: !!state.actions.watchlist,
              rating: !!state.actions.rating,
            },
            rating: state.actions.rating ? state.rating : null,
            providers,
          }),
        });
        saveSelection();
        state.saving = false;
        setStatus(`Saved to ${providers.length} provider${providers.length === 1 ? "" : "s"}.`, "success");
        window.dispatchEvent(new CustomEvent("cw:manual-watched-saved", { detail: data }));
        window.setTimeout(closeModal, 520);
      } catch (err2) {
        state.saving = false;
        setStatus(String(err2?.message || "Save failed"), "error");
      }
    };

    root.addEventListener("input", (e) => {
      if (e.target.matches("[data-role=query]")) {
        state.query = e.target.value || "";
        queueSearch();
      } else if (e.target.matches("[data-role=custom-date]")) {
        state.watchedOn = e.target.value || "";
        render();
      } else if (e.target.matches("[data-role=rating]")) {
        state.rating = Math.max(1, Math.min(10, Number(e.target.value || 8)));
        render();
      }
    });

    root.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && e.target.matches("[data-role=query]")) {
        e.preventDefault();
        search();
      }
    });

    const selectResult = (resultBtn) => {
      const tmdb = String(resultBtn.getAttribute("data-result-tmdb") || "");
      state.selectedItem = state.results.find((item) => String(item.tmdb) === tmdb) || null;
      if (state.selectedItem) state.step = 1;
      render();
    };

    const tapTargetSelector = [
      "[data-role=close]",
      "[data-role=cancel]",
      "[data-role=prev]",
      "[data-role=next]",
      "[data-role=send]",
      "[data-role=search]",
      ".cw-mw-step[data-step]",
      "[data-type]",
      "[data-result-tmdb]",
      "[data-action]",
      "[data-date-mode]",
      "[data-provider-key]",
      "[data-role=use-last]",
      "[data-role=select-all]",
      "[data-role=clear-providers]",
    ].join(",");
    const tapTarget = (target) => target?.closest?.(tapTargetSelector);
    let pointerTap = null;
    let suppressClickUntil = 0;

    const activate = (target) => {
      if (!target) return false;
      if (target.closest("button:disabled")) return true;
      if (target.closest("[data-role=close],[data-role=cancel]")) { closeModalAfterTap(); return true; }
      if (target.closest("[data-role=prev]")) { setStep(state.step - 1); return true; }
      if (target.closest("[data-role=next]")) { if (stepReady(state.step + 1)) setStep(state.step + 1); return true; }
      if (target.closest("[data-role=send]")) { submit(); return true; }
      if (target.closest("[data-role=search]")) { search(); return true; }
      const stepBtn = target.closest(".cw-mw-step[data-step]");
      if (stepBtn) {
        const idx = Number(stepBtn.getAttribute("data-step") || 0);
        if (!stepBtn.disabled) setStep(idx);
        return true;
      }
      const typeBtn = target.closest("[data-type]");
      if (typeBtn) {
        state.type = typeBtn.getAttribute("data-type") || "movie";
        state.selectedItem = null;
        state.results = [];
        if (state.query.trim().length >= 2) queueSearch();
        render(true);
        return true;
      }
      const resultBtn = target.closest("[data-result-tmdb]");
      if (resultBtn) { selectResult(resultBtn); return true; }
      const actionBtn = target.closest("[data-action]");
      if (actionBtn) {
        const key = actionBtn.getAttribute("data-action") || "";
        state.actions[key] = !state.actions[key];
        if (key === "rating" && state.actions[key] && !state.rating) state.rating = 8;
        normalizeActions();
        render();
        return true;
      }
      const modeBtn = target.closest("[data-date-mode]");
      if (modeBtn) {
        state.dateMode = modeBtn.getAttribute("data-date-mode") || "today";
        render();
        return true;
      }
      const providerBtn = target.closest("[data-provider-key]");
      if (providerBtn) {
        const key = providerBtn.getAttribute("data-provider-key") || "";
        if (state.selectedProviders.has(key)) state.selectedProviders.delete(key);
        else state.selectedProviders.add(key);
        normalizeActions();
        render();
        return true;
      }
      if (target.closest("[data-role=use-last]")) {
        const allowed = new Set(compatibleProviders().map(providerKey));
        state.selectedProviders = new Set(state.remembered.filter((key) => allowed.has(key)));
        render();
        return true;
      }
      if (target.closest("[data-role=select-all]")) {
        state.selectedProviders = new Set(compatibleProviders().map(providerKey));
        render();
        return true;
      }
      if (target.closest("[data-role=clear-providers]")) {
        state.selectedProviders = new Set();
        render();
        return true;
      }
      return false;
    };

    root.addEventListener("pointerdown", (e) => {
      const target = e.target;
      const query = target.closest?.("[data-role=query]");
      if (query) {
        query.focus?.({ preventScroll: true });
        return;
      }
      const activator = tapTarget(target);
      pointerTap = activator ? { target: activator, x: e.clientX, y: e.clientY, id: e.pointerId } : null;
      const searchBtn = target.closest?.("[data-role=search]");
      if (searchBtn) {
        e.preventDefault();
        const q = root.querySelector("[data-role=query]");
        q?.focus?.({ preventScroll: true });
        try { q?.setSelectionRange?.(q.value.length, q.value.length); } catch {}
        return;
      }
    }, true);

    root.addEventListener("pointerup", (e) => {
      if (!pointerTap || (pointerTap.id != null && e.pointerId !== pointerTap.id)) return;
      const dx = Math.abs(e.clientX - pointerTap.x);
      const dy = Math.abs(e.clientY - pointerTap.y);
      const activator = tapTarget(e.target) || pointerTap.target;
      pointerTap = null;
      if (dx > 12 || dy > 12 || !activator) return;
      if (e.pointerType === "touch" || e.pointerType === "pen" || window.matchMedia?.("(pointer: coarse)")?.matches) {
        e.preventDefault();
        suppressClickUntil = Date.now() + 700;
        activate(activator);
      }
    }, true);

    root.addEventListener("click", (e) => {
      const target = e.target;
      if (Date.now() < suppressClickUntil) {
        e.preventDefault();
        return;
      }
      if (activate(target)) e.preventDefault();
    });

    render();
    await loadProviders();
  },
  unmount() {
    activeCleanup?.();
    activeCleanup = null;
  },
};
