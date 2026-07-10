/* assets/js/modals/events/index.js */
/* CrossWatch - Events modal */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => (
  { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
));

const fjson = async (u, o = {}) => {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort("timeout"), 30000);
  try {
    const r = await fetch(u, { ...o, signal: ctrl.signal });
    if (!r.ok) throw new Error(r.status);
    return await r.json();
  } finally { clearTimeout(t); }
};

const Q = (s, r = document) => r.querySelector(s);

const TS = (v) => {
  const n = Number(v || 0);
  if (!n) return "";
  return new Date(n * 1000).toLocaleString();
};

const maybeTS = (v) => {
  if (v == null || v === "") return "";
  const n = Number(v);
  if (Number.isFinite(n) && n > 1000000000) return TS(n);
  return String(v);
};

const PAGE_SIZE = 25;
const RUN_ITEMS_PAGE_SIZE = 100;

const FEATURE_LABEL = { history: "History", ratings: "Ratings", watchlist: "Watchlist", progress: "Progress" };
const REASON_LABEL = {
  not_in_plex_catalog: "Not in library",
  not_in_library: "Not in library",
};
const reasonLabel = (reason) => REASON_LABEL[String(reason || "").toLowerCase()] || String(reason || "");

const provShort = (src, dst) => {
  const a = (src || "").toUpperCase();
  const b = (dst || "").toUpperCase();
  if (a && b) return `${a} → ${b}`;
  return b || a || "";
};

const instLabel = (prov, inst) => {
  const p = String(prov || "").toUpperCase();
  if (!p) return "";
  const i = String(inst || "").trim();
  return i ? `${p} ${i}` : `${p} default`;
};

const routeOf = (e) => {
  const a = instLabel(e.source_provider, e.source_instance);
  const b = instLabel(e.destination_provider, e.destination_instance);
  if (a && b) return `${a} → ${b}`;
  return b || a || "";
};

const titleOf = (e) => {
  let t = e.title || e.item_key || "";
  if (e.media_type === "episode" && e.season != null && e.episode != null) {
    t = `${t} S${String(e.season).padStart(2, "0")}E${String(e.episode).padStart(2, "0")}`;
  }
  return t;
};

const isRating = (e) => e.event_type === "write_succeeded" && e.feature === "ratings" && (e.old_value != null || e.new_value != null);

const BADGE = {
  write_succeeded: "Write succeeded",
  write_attempted: "Write attempted",
  write_failed: "Write failed",
  unresolved_recorded: "Unresolved recorded",
  unresolved_cleared: "Unresolved cleared",
  blackbox_promoted: "Blackbox promoted",
  blackbox_blocked: "Blackbox blocked",
  tombstone_created: "Tombstone created",
  tombstone_pruned: "Tombstone pruned",
  plan_created: "Item planned",
  provider_health: "Provider health",
  sync_run_started: "Sync started",
  sync_run_finished: "Sync finished",
  api_rate_limit: "Rate limit",
  watcher_event: "Watcher",
  webhook_event: "Webhook",
};

const badgeOf = (e) => {
  if (isRating(e)) return "Rating updated";
  return BADGE[e.event_type] || String(e.event_type || "").replace(/_/g, " ");
};

const ICON = {
  write_succeeded: "check_circle",
  write_attempted: "upload",
  write_failed: "cancel",
  unresolved_recorded: "shield",
  unresolved_cleared: "verified",
  blackbox_promoted: "inventory_2",
  blackbox_blocked: "block",
  tombstone_created: "delete",
  tombstone_pruned: "delete_sweep",
  plan_created: "description",
  provider_health: "favorite",
  sync_run_started: "play_circle",
  sync_run_finished: "done_all",
  api_rate_limit: "speed",
  watcher_event: "visibility",
  webhook_event: "webhook",
};

const iconOf = (e) => {
  if (isRating(e)) return "edit";
  return ICON[e.event_type] || "bolt";
};

const sevOf = (e) => {
  if (e.event_type === "write_failed") return "error";
  if (e.event_type === "write_succeeded") return isRating(e) ? "rating" : "ok";
  if (["blackbox_promoted", "blackbox_blocked"].includes(e.event_type)) return "error";
  if (["unresolved_recorded", "plan_created", "api_rate_limit"].includes(e.event_type)) return "warn";
  if (["tombstone_created", "tombstone_pruned"].includes(e.event_type)) return "warn";
  if (e.event_type === "unresolved_cleared") return "ok";
  const s = String(e.severity || "").toLowerCase();
  if (s === "error" || s === "warn" || s === "ok") return s;
  return "info";
};

const titleLine = (e) => {
  switch (e.event_type) {
    case "write_succeeded":
      return isRating(e) ? "Rating update" : "Write succeeded";
    case "write_attempted": return "Add attempted";
    case "write_failed": return `${(e.destination_provider || "Destination").toUpperCase()} rejected item · ${reasonLabel(e.reason_code || "failed")}`;
    case "unresolved_recorded": {
      const r = e.reason_code || e.operation || "";
      return r ? `Recorded as unresolved · ${reasonLabel(r)}` : "Recorded as unresolved";
    }
    case "unresolved_cleared": return "Unresolved cleared";
    case "blackbox_promoted": return e.reason_code ? `Item blackboxed · ${e.reason_code}` : "Item blackboxed";
    case "blackbox_blocked": return "Blocked by blackbox";
    case "tombstone_created": return "Tombstone created";
    case "tombstone_pruned": return "Tombstone pruned";
    case "plan_created": return "Add attempted";
    case "provider_health": return `Provider health · ${(e.source_provider || "").toUpperCase()}`;
    case "sync_run_started": return "Sync run started";
    case "sync_run_finished": return "Sync run finished";
    default: return String(e.event_type || "").replace(/_/g, " ");
  }
};

const EVENT_TYPES = [
  "", "write_succeeded", "write_attempted", "write_failed", "unresolved_recorded",
  "blackbox_promoted", "blackbox_blocked", "tombstone_created", "plan_created",
  "provider_health", "sync_run_started", "sync_run_finished",
];

// ---- group helpers ---------------------------------------------------------
const GROUP_SEV = { success: "ok", warning: "warn", error: "error", info: "info" };
const groupSev = (g) => {
  if (String(g.status || "").toLowerCase() === "blackboxed") return "error";
  return GROUP_SEV[String(g.severity || "").toLowerCase()] || "info";
};
const STATUS_ICON = {
  completed: "task_alt", resolved: "check_circle", blackboxed: "inventory_2", unresolved: "shield",
  failed: "cancel", running: "sync", pending: "hourglass_top", informational: "bolt",
};
const groupIcon = (g) => STATUS_ICON[String(g.status || "").toLowerCase()] || "bolt";
const statusLabel = (s) => { const t = String(s || "informational"); return t.charAt(0).toUpperCase() + t.slice(1); };
const modeLabel = (m) => {
  const s = String(m || "").toLowerCase();
  if (s.includes("two")) return "Two-way";
  if (s.includes("one")) return "One-way";
  return m ? String(m) : "";
};

function injectCSS() {
  if (document.getElementById("cw-events-css")) return Promise.resolve();
  const link = document.createElement("link");
  const url = new URL("./styles.css", import.meta.url);
  const v = new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__;
  if (v) url.searchParams.set("v", v);
  link.id = "cw-events-css";
  link.rel = "stylesheet";
  link.href = url.href;
  return new Promise((res) => {
    link.addEventListener("load", res, { once: true });
    link.addEventListener("error", res, { once: true });
    document.head.appendChild(link);
  });
}

function createDropdown({ label, items, value, onChange, align = "auto" }) {
  let opts = items.slice();
  let cur = value ?? (opts[0]?.value ?? "");
  const wrap = document.createElement("div");
  wrap.className = "ev-dd";
  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "ev-dd-btn";
  wrap.appendChild(btn);

  const labelFor = (v) => opts.find((i) => i.value === v)?.label ?? label;
  const render = () => {
    btn.innerHTML = `<span class="ev-dd-text">${esc(labelFor(cur))}</span><span class="material-symbols-rounded ev-dd-caret" aria-hidden="true">expand_more</span>`;
  };
  render();

  let menu = null;
  const position = () => {
    if (!menu) return;
    const r = btn.getBoundingClientRect();
    menu.style.position = "fixed";
    menu.style.minWidth = `${Math.max(r.width, 160)}px`;
    menu.style.maxHeight = "260px";
    menu.style.visibility = "hidden";
    menu.style.left = "0px";
    menu.style.top = "0px";
    const mw = menu.offsetWidth;
    const mh = menu.offsetHeight;
    let left = align === "right" ? r.right - mw : r.left;
    if (left + mw > window.innerWidth - 8) left = r.right - mw;
    left = Math.max(8, Math.min(left, window.innerWidth - mw - 8));
    let top = r.bottom + 4;
    if (top + mh > window.innerHeight - 8) {
      const up = r.top - 4 - mh;
      top = up > 8 ? up : Math.max(8, window.innerHeight - mh - 8);
    }
    menu.style.left = `${left}px`;
    menu.style.top = `${top}px`;
    menu.style.visibility = "visible";
  };
  const onDocDown = (e) => {
    if (!menu) return;
    if (btn.contains(e.target) || menu.contains(e.target)) return;
    close();
  };
  const onKey = (e) => { if (e.key === "Escape") { e.stopPropagation(); close(); } };
  const close = () => {
    if (!menu) return;
    menu.remove();
    menu = null;
    btn.setAttribute("aria-expanded", "false");
    document.removeEventListener("pointerdown", onDocDown, true);
    document.removeEventListener("keydown", onKey, true);
    window.removeEventListener("resize", close);
    window.removeEventListener("scroll", close, true);
  };
  const hasSearch = () => opts.length > 8;
  const renderItems = (filter = "") => {
    const f = filter.trim().toLowerCase();
    const list = f ? opts.filter((i) => String(i.label).toLowerCase().includes(f)) : opts;
    const body = list.map((i) => `<button type="button" class="ev-dd-item${i.value === cur ? " sel" : ""}" data-v="${esc(i.value)}"><span class="ev-dd-item-main">${esc(i.label)}</span>${i.hint ? `<span class="ev-dd-item-hint">${esc(i.hint)}</span>` : ""}${i.value === cur ? `<span class="material-symbols-rounded ev-dd-check" aria-hidden="true">check</span>` : ""}</button>`).join("");
    return body || `<div class="ev-dd-empty">No matches</div>`;
  };
  const bindItems = () => {
    menu.querySelectorAll(".ev-dd-item").forEach((el) => el.addEventListener("click", () => {
      cur = el.dataset.v;
      render();
      close();
      onChange?.(cur);
    }));
  };
  const open = () => {
    if (menu) { close(); return; }
    menu = document.createElement("div");
    menu.className = "ev-dd-menu cw-scrollbars";
    const search = hasSearch()
      ? `<label class="ev-dd-search"><span class="material-symbols-rounded" aria-hidden="true">search</span><input type="text" placeholder="Search…"></label>` : "";
    menu.innerHTML = `${search}<div class="ev-dd-body cw-scrollbars">${renderItems()}</div>`;
    document.body.appendChild(menu);
    btn.setAttribute("aria-expanded", "true");
    const bodyEl = menu.querySelector(".ev-dd-body");
    const searchEl = menu.querySelector(".ev-dd-search input");
    bindItems();
    searchEl?.addEventListener("input", () => { bodyEl.innerHTML = renderItems(searchEl.value); bindItems(); position(); });
    position();
    searchEl?.focus();
    document.addEventListener("pointerdown", onDocDown, true);
    document.addEventListener("keydown", onKey, true);
    window.addEventListener("resize", close);
    window.addEventListener("scroll", close, true);
  };
  btn.addEventListener("click", (e) => { e.stopPropagation(); open(); });

  return {
    el: wrap,
    get value() { return cur; },
    set value(v) { cur = v; render(); },
    reset() { cur = opts[0]?.value ?? ""; render(); },
    setItems(next) { opts = next.slice(); if (!opts.some((i) => i.value === cur)) cur = opts[0]?.value ?? ""; render(); },
    labelOf(v) { return opts.find((i) => i.value === v)?.label ?? String(v ?? ""); },
    close,
  };
}

function createSegmented({ items, value, onChange, cls = "" }) {
  const wrap = document.createElement("div");
  wrap.className = `ev-seg ${cls}`.trim();
  wrap.setAttribute("role", "tablist");
  let cur = value ?? items[0]?.value;
  const render = () => {
    wrap.innerHTML = items.map((i) => `<button type="button" role="tab" class="ev-seg-btn${i.value === cur ? " on" : ""}" data-v="${esc(i.value)}" aria-selected="${i.value === cur}">${i.icon ? `<span class="material-symbols-rounded" aria-hidden="true">${i.icon}</span>` : ""}${esc(i.label)}</button>`).join("");
    wrap.querySelectorAll(".ev-seg-btn").forEach((b) => b.addEventListener("click", () => {
      if (b.dataset.v === cur) return;
      cur = b.dataset.v; render(); onChange?.(cur);
    }));
  };
  render();
  return { el: wrap, get value() { return cur; }, set value(v) { cur = v; render(); } };
}

let cleanup = null;

export default {
  async mount(root) {
    cleanup?.();
    await injectCSS();
    root.classList.add("modal-root", "ev-modal");
    const shell = root.closest(".cx-modal-shell");
    if (shell) {
      shell.classList.add("events-modal-shell");
      shell.style.setProperty("--cxModalMaxW", "1240px");
      shell.style.setProperty("--cxModalMaxH", "760px");
    }

    const ls = (k, d) => { try { return localStorage.getItem(k) || d; } catch { return d; } };
    const lset = (k, v) => { try { localStorage.setItem(k, v); } catch {} };
    let visibility = ls("cw.events.visibility", "open");
    let order = ls("cw.events.order", "newest");
    let mode = ls("cw.events.mode", "grouped");
    let detailTab = ls("cw.events.detailtab", "timeline");

    const state = { items: [], page: 0, total: 0, selected: null, relExpanded: false, collapsed: new Set() };
    const dropdowns = [];

    root.innerHTML = `
      <div class="ev-app">
        <div class="cx-head">
          <div class="ev-head-left">
            <div class="ev-head-text">
              <div class="ev-title">Events</div>
              <div class="ev-sub">Searchable history of what synced, what failed, and why.</div>
            </div>
          </div>
          <div class="ev-actions">
            <button class="close-btn" id="ev-close" type="button"><span class="material-symbols-rounded" aria-hidden="true">close</span><span>Close</span></button>
          </div>
        </div>

        <div class="ev-toolbar">
          <div class="ev-toolbar-row">
            <label class="ev-search"><span class="material-symbols-rounded" aria-hidden="true">search</span><input id="ev-q" type="text" placeholder="Search title, item, reason, run…"></label>
            <div class="ev-vis"><span id="ev-seg"></span></div>
            <span id="ev-cat"></span>
            <span id="ev-mode"></span>
            <button class="ev-tbtn" id="ev-more" type="button" aria-expanded="false"><span class="material-symbols-rounded" aria-hidden="true">tune</span><span>More filters</span><span class="ev-more-dot" id="ev-more-dot" hidden></span><span class="material-symbols-rounded ev-tbtn-caret" aria-hidden="true">expand_more</span></button>
            <button class="ev-tbtn ev-tbtn-refresh" id="ev-refresh" type="button"><span class="material-symbols-rounded" aria-hidden="true">refresh</span><span>Refresh</span></button>
            <button class="ev-tbtn" id="ev-clear" type="button" title="Delete all events from the archive"><span class="material-symbols-rounded" aria-hidden="true">delete_sweep</span><span>Clear</span></button>
          </div>
          <div class="ev-morefilters" id="ev-morefilters" hidden>
            <div class="ev-mf-row">
              <span id="ev-range"></span>
              <label class="ev-date" id="ev-since-wrap" hidden><span class="material-symbols-rounded" aria-hidden="true">event</span><input id="ev-since" type="date" aria-label="Since"></label>
              <label class="ev-date" id="ev-until-wrap" hidden><span class="material-symbols-rounded" aria-hidden="true">event</span><input id="ev-until" type="date" aria-label="Until"></label>
              <div class="ev-filters" id="ev-filters"></div>
              <button class="ev-linkbtn" id="ev-clear-hidden" type="button">Clear filters</button>
            </div>
            <div class="ev-mf-summary" id="ev-mf-summary" hidden></div>
          </div>
        </div>

        <div class="ev-layout">
          <div class="ev-col-list">
            <div class="ev-list-head">
              <div class="ev-list-head-left"><div class="ev-count" id="ev-count">—</div></div>
              <div class="ev-list-head-right">
                <span id="ev-sort"></span>
                <div class="ev-pagemini">
                  <button class="ev-pg" id="ev-prev-top" type="button" aria-label="Previous page"><span class="material-symbols-rounded" aria-hidden="true">chevron_left</span></button>
                  <span class="ev-pagemini-label" id="ev-pagelabel">Page 1</span>
                  <button class="ev-pg" id="ev-next-top" type="button" aria-label="Next page"><span class="material-symbols-rounded" aria-hidden="true">chevron_right</span></button>
                </div>
              </div>
            </div>
            <div class="ev-list cw-scrollbars" id="ev-list"><div class="ev-empty">Loading…</div></div>
            <div class="ev-list-foot" id="ev-foot"></div>
          </div>
          <div class="ev-split" id="ev-split" role="separator" aria-orientation="vertical" title="Drag to resize"></div>
          <div class="ev-detail cw-scrollbars" id="ev-detail"><div class="ev-empty ev-empty-detail"><span class="material-symbols-rounded" aria-hidden="true">touch_app</span><div>Select an event thread to see details and current context.</div></div></div>
        </div>

        <div class="ev-toast" id="ev-toast" hidden></div>
      </div>`;

    const listEl = Q("#ev-list", root);
    const detailEl = Q("#ev-detail", root);
    const footEl = Q("#ev-foot", root);
    const filtersEl = Q("#ev-filters", root);
    const toastEl = Q("#ev-toast", root);
    const qEl = Q("#ev-q", root);
    const countEl = Q("#ev-count", root);
    const sinceEl = Q("#ev-since", root);
    const untilEl = Q("#ev-until", root);
    const sinceWrap = Q("#ev-since-wrap", root);
    const untilWrap = Q("#ev-until-wrap", root);
    const moreWrap = Q("#ev-morefilters", root);
    const moreBtn = Q("#ev-more", root);
    const moreDotEl = Q("#ev-more-dot", root);
    const summaryEl = Q("#ev-mf-summary", root);
    const pageLabelEl = Q("#ev-pagelabel", root);
    const layoutEl = Q(".ev-layout", root);
    const splitEl = Q("#ev-split", root);

    let alive = true;

    // ---- resizable list/detail split -------------------------------------
    const savedSplit = ls("cw.events.split", "");
    if (savedSplit) layoutEl.style.setProperty("--ev-list-w", savedSplit);
    let splitDrag = false;
    const onSplitMove = (ev) => {
      if (!splitDrag) return;
      const r = layoutEl.getBoundingClientRect();
      if (r.width <= 0) return;
      let pct = ((ev.clientX - r.left) / r.width) * 100;
      pct = Math.max(30, Math.min(72, pct));
      layoutEl.style.setProperty("--ev-list-w", `${pct.toFixed(1)}%`);
    };
    const endSplit = () => {
      if (!splitDrag) return;
      splitDrag = false;
      splitEl.classList.remove("drag");
      document.body.style.userSelect = "";
      window.removeEventListener("pointermove", onSplitMove, true);
      window.removeEventListener("pointerup", endSplit, true);
      lset("cw.events.split", layoutEl.style.getPropertyValue("--ev-list-w"));
    };
    splitEl.addEventListener("pointerdown", (ev) => {
      splitDrag = true;
      splitEl.classList.add("drag");
      document.body.style.userSelect = "none";
      window.addEventListener("pointermove", onSplitMove, true);
      window.addEventListener("pointerup", endSplit, true);
      ev.preventDefault();
    });

    let dateRange = ls("cw.events.range", "all");
    const ddRange = createDropdown({
      label: "All time", value: dateRange,
      items: [
        { value: "all", label: "All time" },
        { value: "24h", label: "Last 24 hours" },
        { value: "7d", label: "Last 7 days" },
        { value: "30d", label: "Last 30 days" },
        { value: "custom", label: "Custom range" },
      ],
      onChange: (v) => { dateRange = v; lset("cw.events.range", v); syncRangeUI(); load(0); },
    });
    const ddType = createDropdown({ label: "All types", items: EVENT_TYPES.map((t) => ({ value: t, label: t ? BADGE[t] || t.replace(/_/g, " ") : "All types" })), onChange: () => load(0) });
    const ddProvider = createDropdown({ label: "Any provider", items: [{ value: "", label: "Any provider" }], onChange: () => load(0) });
    const ddOrigin = createDropdown({ label: "Any origin", items: [{ value: "", label: "Any origin" }], onChange: () => load(0) });
    const ddFeature = createDropdown({ label: "Any feature", items: [{ value: "", label: "Any feature" }, ...["history", "ratings", "watchlist", "progress"].map((f) => ({ value: f, label: FEATURE_LABEL[f] }))], onChange: () => load(0) });
    const ddPair = createDropdown({ label: "Any pair", items: [{ value: "", label: "Any pair" }], onChange: () => load(0) });
    const ddCategory = createDropdown({
      label: "Any outcome", align: "right",
      items: [{ value: "", label: "Any outcome" }, { value: "successful", label: "Successful" }, { value: "problems", label: "Problems" }, { value: "informational", label: "Informational" }],
      onChange: () => load(0),
    });
    Q("#ev-cat", root).appendChild(ddCategory.el);
    Q("#ev-range", root).appendChild(ddRange.el);
    const hiddenDds = [ddType, ddProvider, ddOrigin, ddFeature, ddPair];
    for (const dd of hiddenDds) filtersEl.appendChild(dd.el);
    dropdowns.push(ddCategory, ddRange, ...hiddenDds);

    const ddSort = createDropdown({
      label: "Newest first", value: order,
      items: [{ value: "newest", label: "Newest first" }, { value: "oldest", label: "Oldest first" }],
      onChange: (v) => { order = v; lset("cw.events.order", v); load(0); },
      align: "right",
    });
    Q("#ev-sort", root).appendChild(ddSort.el);

    const seg = createSegmented({
      items: [{ value: "open", label: "Open" }, { value: "acknowledged", label: "Acknowledged" }, { value: "all", label: "All" }],
      value: visibility,
      onChange: (v) => { visibility = v; lset("cw.events.visibility", v); load(0); },
    });
    Q("#ev-seg", root).appendChild(seg.el);

    const modeSeg = createSegmented({
      cls: "ev-seg-mode",
      items: [{ value: "grouped", label: "Grouped", icon: "account_tree" }, { value: "raw", label: "Raw events", icon: "list" }],
      value: mode,
      onChange: (v) => { mode = v; lset("cw.events.mode", v); ddCategory.el.style.display = (v === "grouped") ? "" : "none"; state.selected = null; clearDetail(); load(0); },
    });
    Q("#ev-mode", root).appendChild(modeSeg.el);
    ddCategory.el.style.display = (mode === "grouped") ? "" : "none";

    const dateToEpoch = (v, end = false) => {
      if (!v) return "";
      const d = new Date(v + (end ? "T23:59:59" : "T00:00:00"));
      const n = Math.floor(d.getTime() / 1000);
      return Number.isFinite(n) ? String(n) : "";
    };

    const syncRangeUI = () => {
      const custom = dateRange === "custom";
      sinceWrap.hidden = !custom;
      untilWrap.hidden = !custom;
    };

    const rangeEpoch = () => {
      const nowSec = Math.floor(Date.now() / 1000);
      if (dateRange === "24h") return { since: String(nowSec - 86400), until: "" };
      if (dateRange === "7d") return { since: String(nowSec - 7 * 86400), until: "" };
      if (dateRange === "30d") return { since: String(nowSec - 30 * 86400), until: "" };
      if (dateRange === "custom") return { since: dateToEpoch(sinceEl.value, false), until: dateToEpoch(untilEl.value, true) };
      return { since: "", until: "" };
    };

    const RANGE_LABEL = { "24h": "Last 24 hours", "7d": "Last 7 days", "30d": "Last 30 days" };
    const customRangeLabel = () => {
      const s = sinceEl.value, u = untilEl.value;
      if (s && u) return `${s} → ${u}`;
      if (s) return `From ${s}`;
      if (u) return `Until ${u}`;
      return "Custom range";
    };

    const hiddenActive = () => !!(ddType.value || ddProvider.value || ddOrigin.value || ddFeature.value || ddPair.value || (dateRange && dateRange !== "all"));
    const filtersActive = () => !!(qEl.value.trim() || (grouped() && ddCategory.value) || hiddenActive());

    const clearHidden = (k) => {
      const all = !k;
      if (all || k === "range") { dateRange = "all"; lset("cw.events.range", "all"); ddRange.value = "all"; sinceEl.value = ""; untilEl.value = ""; syncRangeUI(); }
      if (all || k === "type") ddType.reset();
      if (all || k === "provider") ddProvider.reset();
      if (all || k === "origin") ddOrigin.reset();
      if (all || k === "feature") ddFeature.reset();
      if (all || k === "pair") ddPair.reset();
      load(0);
    };

    const updateHidden = () => {
      const chips = [];
      if (dateRange && dateRange !== "all") chips.push({ k: "range", label: dateRange === "custom" ? customRangeLabel() : RANGE_LABEL[dateRange] });
      if (ddType.value) chips.push({ k: "type", label: ddType.labelOf(ddType.value) });
      if (ddProvider.value) chips.push({ k: "provider", label: ddProvider.labelOf(ddProvider.value) });
      if (ddOrigin.value) chips.push({ k: "origin", label: `Origin: ${ddOrigin.labelOf(ddOrigin.value)}` });
      if (ddFeature.value) chips.push({ k: "feature", label: ddFeature.labelOf(ddFeature.value) });
      if (ddPair.value) chips.push({ k: "pair", label: ddPair.labelOf(ddPair.value) });
      const active = chips.length > 0;
      moreDotEl.hidden = !active;
      moreBtn.classList.toggle("has-active", active);
      if (!active) { summaryEl.hidden = true; summaryEl.innerHTML = ""; return; }
      summaryEl.hidden = false;
      summaryEl.innerHTML = chips.map((c) => `<button type="button" class="ev-chip" data-k="${esc(c.k)}"><span>${esc(c.label)}</span><span class="material-symbols-rounded" aria-hidden="true">close</span></button>`).join("")
        + `<button type="button" class="ev-chip ev-chip-clear" id="ev-chip-clear">Clear all</button>`;
      summaryEl.querySelectorAll(".ev-chip[data-k]").forEach((b) => b.addEventListener("click", () => clearHidden(b.dataset.k)));
      Q("#ev-chip-clear", summaryEl)?.addEventListener("click", () => clearHidden());
    };

    const buildQuery = (page) => {
      const p = new URLSearchParams();
      const q = qEl.value.trim();
      if (q) p.set("q", q);
      if (ddType.value) p.set("event_type", ddType.value);
      if (ddProvider.value) p.set("provider", ddProvider.value);
      if (ddOrigin.value) p.set("origin_provider", ddOrigin.value);
      if (ddFeature.value) p.set("feature", ddFeature.value);
      if (ddPair.value) p.set("pair_key", ddPair.value);
      if (grouped() && ddCategory.value) p.set("category", ddCategory.value);
      const { since, until } = rangeEpoch();
      if (since) p.set("since", since);
      if (until) p.set("until", until);
      p.set("visibility", visibility);
      p.set("order", order);
      p.set("limit", String(PAGE_SIZE));
      p.set("offset", String((page || 0) * PAGE_SIZE));
      return p.toString();
    };

    const pageCount = () => Math.max(1, Math.ceil((state.total || 0) / PAGE_SIZE));

    // ---- rows --------------------------------------------------------------
    const groupRowHTML = (g, opts = {}) => {
      const sv = groupSev(g);
      const feat = FEATURE_LABEL[g.feature] || g.feature || "";
      const route = routeOf(g);
      const count = `${g.event_count || 1} event${g.event_count === 1 ? "" : "s"}`;
      const meta = [count, feat, route].filter(Boolean).join(" · ");
      const item = titleOf(g);
      const s = g.summary || "";
      let headline, detail;
      if (item) {
        headline = item;
        detail = (s && s !== item) ? s : "";
      } else {
        const i = s.indexOf(", ");
        if (i > 0) { headline = s.slice(0, i); detail = s.slice(i + 2); }
        else { headline = s; detail = ""; }
      }
      headline = headline || statusLabel(g.status);
      const twist = opts.expandable
        ? `<button class="ev-twist${opts.expanded ? " open" : ""}" type="button" data-twist="${g.id}" aria-label="${opts.expanded ? "Collapse" : "Expand"}"><span class="material-symbols-rounded" aria-hidden="true">chevron_right</span></button>`
        : `<span class="ev-twist-sp" aria-hidden="true"></span>`;
      return `
      <div class="ev-row${opts.child ? " ev-row-child" : ""}${g.acknowledged_at ? " acked" : ""}" data-id="${g.id}">
        ${twist}
        <span class="ev-ic ${sv}"><span class="material-symbols-rounded" aria-hidden="true">${groupIcon(g)}</span></span>
        <span class="ev-line">
          <span class="ev-primary"><span class="ev-badge ${sv}">${esc(statusLabel(g.status))}</span><span class="ev-ptext">${esc(headline)}</span></span>
          ${detail ? `<span class="ev-summary">${esc(detail)}</span>` : ""}
          <span class="ev-meta"><span class="material-symbols-rounded ev-meta-ic" aria-hidden="true">format_list_bulleted</span>${esc(meta)}</span>
        </span>
        <span class="ev-time">${esc(TS(g.last_event_at))}</span>
        <span class="ev-rowact">
          <button class="ev-ack" type="button" title="${g.acknowledged_at ? "Acknowledged — click to undo" : "Acknowledge thread"}" aria-label="${g.acknowledged_at ? "Unacknowledge thread" : "Acknowledge thread"}" data-ack="${g.id}"><span class="material-symbols-rounded" aria-hidden="true">${g.acknowledged_at ? "check_circle" : "check"}</span></button>
        </span>
      </div>`;
    };

    const eventRowHTML = (e) => {
      const sv = sevOf(e);
      const feat = FEATURE_LABEL[e.feature] || e.feature || "";
      const route = routeOf(e);
      const meta = [feat, route].filter(Boolean).join(" · ");
      const item = titleOf(e);
      return `
      <div class="ev-row${e.acknowledged_at ? " acked" : ""}" data-id="${e.id}">
        <span class="ev-twist-sp" aria-hidden="true"></span>
        <span class="ev-ic ${sv}"><span class="material-symbols-rounded" aria-hidden="true">${iconOf(e)}</span></span>
        <span class="ev-line">
          <span class="ev-primary"><span class="ev-badge ${sv}">${esc(badgeOf(e))}</span><span class="ev-ptext">${esc(titleLine(e))}</span></span>
          <span class="ev-meta">${esc(meta)}</span>
          ${item ? `<span class="ev-item">${esc(item)}</span>` : ""}
        </span>
        <span class="ev-time">${esc(TS(e.created_at))}</span>
        <span class="ev-rowact">
          <button class="ev-ack" type="button" title="${e.acknowledged_at ? "Acknowledged — click to undo" : "Acknowledge"}" aria-label="${e.acknowledged_at ? "Unacknowledge event" : "Acknowledge event"}" data-ack="${e.id}"><span class="material-symbols-rounded" aria-hidden="true">${e.acknowledged_at ? "check_circle" : "check"}</span></button>
        </span>
      </div>`;
    };

    const grouped = () => mode === "grouped";

    const findGroup = (id) => {
      const s = String(id);
      for (const g of state.items) {
        if (String(g.id) === s) return g;
        for (const k of (g.children || [])) if (String(k.id) === s) return k;
      }
      return null;
    };

    const buildRows = () => {
      if (!grouped()) return state.items.map(eventRowHTML).join("");
      const out = [];
      for (const g of state.items) {
        const kids = g.children || [];
        const expandable = kids.length > 0;
        const expanded = expandable && !state.collapsed.has(String(g.id));
        out.push(groupRowHTML(g, { expandable, expanded }));
        if (expanded) for (const k of kids) out.push(groupRowHTML(k, { child: true }));
      }
      return out.join("");
    };

    const applyDefaultCollapse = () => {
      state.collapsed.clear();
      const runs = state.items.filter((g) => (g.children || []).length > 0);
      if (runs.length < 2) return;
      let latest = runs[0];
      for (const g of runs) if (Number(g.last_event_at || 0) > Number(latest.last_event_at || 0)) latest = g;
      for (const g of runs) if (String(g.id) !== String(latest.id)) state.collapsed.add(String(g.id));
    };

    const bindRow = (el) => {
      el.addEventListener("click", (ev) => {
        if (ev.target.closest(".ev-rowact") || ev.target.closest(".ev-twist")) return;
        select(el.dataset.id, el);
      });
      el.querySelector(".ev-ack")?.addEventListener("click", (ev) => {
        ev.stopPropagation();
        const item = findGroup(el.dataset.id);
        if (item && item.acknowledged_at) unacknowledgeRow(el.dataset.id);
        else acknowledgeRow(el.dataset.id);
      });
    };

    const clearDetail = () => {
      state.selected = null;
      detailEl.innerHTML = `<div class="ev-empty ev-empty-detail"><span class="material-symbols-rounded" aria-hidden="true">touch_app</span><div>Select an ${grouped() ? "event thread" : "event"} to see details and current context.</div></div>`;
    };

    const noun = () => grouped() ? "thread" : "event";

    const emptyState = () => {
      let icon = "inbox", head = "No events", hint = "", actions = "";
      const n = noun();
      if (filtersActive()) {
        icon = "filter_alt_off"; head = `No ${n}s match your filters.`;
        hint = "Try clearing filters or widening the date range.";
        actions = `<button class="ev-linkbtn" id="ev-empty-reset">Clear filters</button>`;
      } else if (visibility === "open") {
        icon = "task_alt"; head = `No open ${n}s.`;
        hint = "You're all caught up. Acknowledged items are hidden here.";
        actions = `<button class="ev-linkbtn" id="ev-empty-all">Switch to All</button>`;
      } else if (visibility === "acknowledged") {
        icon = "done_all"; head = `No acknowledged ${n}s yet.`;
        hint = `Acknowledge a ${n} to move it out of the Open view.`;
        actions = `<button class="ev-linkbtn" id="ev-empty-open">Back to Open</button>`;
      } else {
        icon = "inbox"; head = "No events in the archive.";
        hint = "Run a sync to start recording events here.";
        actions = "";
      }
      return `<div class="ev-empty ev-empty-list"><span class="material-symbols-rounded" aria-hidden="true">${icon}</span><div class="ev-empty-head">${esc(head)}</div><div class="ev-empty-hint">${esc(hint)}</div><div class="ev-empty-actions">${actions}</div></div>`;
    };

    const renderPager = () => {
      const pc = pageCount();
      const total = state.total || 0;
      if (!total) { footEl.innerHTML = ""; pageLabelEl.textContent = "Page 0 of 0"; return; }
      const start = state.page * PAGE_SIZE + 1;
      const end = Math.min(total, state.page * PAGE_SIZE + state.items.length);
      pageLabelEl.textContent = `Page ${state.page + 1} of ${pc}`;

      const nums = [];
      const cur = state.page;
      const win = new Set([0, pc - 1, cur - 1, cur, cur + 1]);
      let last = -1;
      for (let i = 0; i < pc; i++) {
        if (!win.has(i)) continue;
        if (i - last > 1) nums.push("…");
        nums.push(i);
        last = i;
      }
      const numHTML = nums.map((n) => n === "…"
        ? `<span class="ev-pg-gap">…</span>`
        : `<button class="ev-pg ev-pg-num${n === cur ? " on" : ""}" type="button" data-p="${n}">${n + 1}</button>`).join("");

      const label = grouped() ? "threads" : "events";
      footEl.innerHTML = `
        <div class="ev-showing">Showing ${start.toLocaleString()} to ${end.toLocaleString()} of ${total.toLocaleString()} ${label}</div>
        <div class="ev-pagenums">
          <button class="ev-pg" id="ev-first" type="button" aria-label="First page" ${cur === 0 ? "disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">first_page</span></button>
          <button class="ev-pg" id="ev-prev" type="button" aria-label="Previous page" ${cur === 0 ? "disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">chevron_left</span></button>
          ${numHTML}
          <button class="ev-pg" id="ev-next" type="button" aria-label="Next page" ${cur >= pc - 1 ? "disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">chevron_right</span></button>
          <button class="ev-pg" id="ev-last" type="button" aria-label="Last page" ${cur >= pc - 1 ? "disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">last_page</span></button>
        </div>`;

      const go = (p) => { const np = Math.max(0, Math.min(pc - 1, p)); if (np !== state.page) load(np); };
      Q("#ev-first", footEl)?.addEventListener("click", () => go(0));
      Q("#ev-prev", footEl)?.addEventListener("click", () => go(cur - 1));
      Q("#ev-next", footEl)?.addEventListener("click", () => go(cur + 1));
      Q("#ev-last", footEl)?.addEventListener("click", () => go(pc - 1));
      footEl.querySelectorAll(".ev-pg-num").forEach((b) => b.addEventListener("click", () => go(Number(b.dataset.p))));
    };

    const updateHeadPager = () => {
      const pc = pageCount();
      Q("#ev-prev-top", root).disabled = state.page <= 0;
      Q("#ev-next-top", root).disabled = state.page >= pc - 1;
    };

    const renderList = () => {
      const label = grouped() ? "thread" : "event";
      countEl.textContent = `${(state.total || 0).toLocaleString()} ${label}${state.total === 1 ? "" : "s"}`;
      updateHeadPager();
      if (!state.items.length) {
        listEl.innerHTML = emptyState();
        renderPager();
        Q("#ev-empty-reset", listEl)?.addEventListener("click", resetFilters);
        Q("#ev-empty-all", listEl)?.addEventListener("click", () => { seg.value = "all"; visibility = "all"; lset("cw.events.visibility", "all"); load(0); });
        Q("#ev-empty-open", listEl)?.addEventListener("click", () => { seg.value = "open"; visibility = "open"; lset("cw.events.visibility", "open"); load(0); });
        return;
      }
      listEl.innerHTML = buildRows();
      listEl.querySelectorAll(".ev-row").forEach(bindRow);
      listEl.querySelectorAll(".ev-twist").forEach((b) => b.addEventListener("click", (ev) => {
        ev.stopPropagation();
        const id = String(b.dataset.twist);
        if (state.collapsed.has(id)) state.collapsed.delete(id); else state.collapsed.add(id);
        renderList();
      }));
      if (state.selected) listEl.querySelector(`.ev-row[data-id="${state.selected}"]`)?.classList.add("sel");
      renderPager();
    };

    const load = async (page) => {
      if (page != null) state.page = page;
      updateHidden();
      const url = grouped() ? `/api/events/tree?${buildQuery(state.page)}` : `/api/events/search?view=events&${buildQuery(state.page)}`;
      try {
        const data = await fjson(url);
        if (!alive) return;
        state.items = data.items || [];
        state.total = data.total || 0;
        applyDefaultCollapse();
        const pc = pageCount();
        if (state.page > 0 && state.page >= pc) { return load(pc - 1); }
        renderList();
        if (page === 0) listEl.scrollTop = 0;
      } catch (err) {
        listEl.innerHTML = `<div class="ev-empty ev-empty-list"><span class="material-symbols-rounded" aria-hidden="true">error</span><div class="ev-empty-head">Failed to load</div><div class="ev-empty-hint">${esc(err.message)}</div></div>`;
      }
    };

    let toastTimer = null;
    const showToast = (msg, undoFn) => {
      clearTimeout(toastTimer);
      toastEl.hidden = false;
      toastEl.innerHTML = `<span class="material-symbols-rounded ev-toast-ic" aria-hidden="true">check_circle</span><span>${esc(msg)}</span>${undoFn ? `<button type="button" class="ev-toast-undo">Undo</button>` : ""}`;
      toastEl.querySelector(".ev-toast-undo")?.addEventListener("click", async () => {
        clearTimeout(toastTimer);
        toastEl.hidden = true;
        try { await undoFn(); } catch {}
      });
      toastTimer = setTimeout(() => { toastEl.hidden = true; }, 6000);
    };

    const ackURL = (id, action) => grouped()
      ? `/api/events/groups/${encodeURIComponent(id)}/${action}`
      : `/api/events/${encodeURIComponent(id)}/${action}`;

    const animateRowOut = async (id) => {
      const row = listEl.querySelector(`.ev-row[data-id="${id}"]`);
      if (!row) return;
      row.classList.add("ev-row-leave");
      await new Promise((resolve) => {
        let done = false;
        const finish = () => { if (!done) { done = true; resolve(); } };
        row.addEventListener("animationend", finish, { once: true });
        setTimeout(finish, 260);
      });
    };

    const acknowledgeRow = async (id) => {
      let res;
      try { res = await fjson(ackURL(id, "acknowledge"), { method: "POST" }); } catch { return; }
      if (!res?.ok) return;
      if (visibility === "open") await animateRowOut(id);
      const idx = state.items.findIndex((x) => String(x.id) === String(id));
      if (idx < 0) {
        await load(state.page);
        showToast(grouped() ? "Thread acknowledged." : "Event acknowledged.", async () => {
          try { await fjson(ackURL(id, "unacknowledge"), { method: "POST" }); } catch { return; }
          await load(state.page); select(id);
        });
        return;
      }
      const item = idx >= 0 ? state.items[idx] : null;
      if (item) item.acknowledged_at = res.acknowledged_at;

      if (visibility === "open") {
        const wasSelected = String(state.selected) === String(id);
        if (idx >= 0) { state.items.splice(idx, 1); state.total = Math.max(0, state.total - 1); }
        if (!state.items.length && state.total > 0) { await load(state.page); }
        else {
          renderList();
          if (wasSelected) {
            const next = state.items[Math.min(idx, state.items.length - 1)];
            if (next) select(next.id); else clearDetail();
          }
        }
      } else {
        const row = listEl.querySelector(`.ev-row[data-id="${id}"]`);
        if (row) { row.classList.add("acked"); const b = row.querySelector(".ev-ack .material-symbols-rounded"); if (b) b.textContent = "check_circle"; }
      }
      showToast(grouped() ? "Thread acknowledged." : "Event acknowledged.", async () => {
        try { await fjson(ackURL(id, "unacknowledge"), { method: "POST" }); } catch { return; }
        await load(state.page);
        select(id);
      });
    };

    const unacknowledgeRow = async (id) => {
      let res;
      try { res = await fjson(ackURL(id, "unacknowledge"), { method: "POST" }); } catch { return; }
      if (!res?.ok) return;
      const idx = state.items.findIndex((x) => String(x.id) === String(id));
      if (idx < 0) { await load(state.page); showToast("Restored."); return; }
      const item = idx >= 0 ? state.items[idx] : null;
      if (item) item.acknowledged_at = null;
      if (visibility === "acknowledged") {
        if (idx >= 0) { state.items.splice(idx, 1); state.total = Math.max(0, state.total - 1); }
        if (!state.items.length && state.total > 0) await load(state.page); else renderList();
      } else {
        const row = listEl.querySelector(`.ev-row[data-id="${id}"]`);
        if (row) { row.classList.remove("acked"); const b = row.querySelector(".ev-ack .material-symbols-rounded"); if (b) b.textContent = "check"; }
      }
      showToast("Restored.");
    };

    // ---- context cards (shared) -------------------------------------------
    const pill = (text, cls) => `<span class="ev-pill ${cls}">${esc(text)}</span>`;
    const card = (title, pillHTML, lines, extra = "") => `
      <div class="ev-card">
        <div class="ev-card-head"><span class="ev-card-title">${esc(title)}</span>${pillHTML || ""}</div>
        <div class="ev-card-body">${lines.filter(Boolean).map((l) => `<div class="ev-card-line">${l}</div>`).join("")}</div>
        ${extra}
      </div>`;

    const renderCards = (c, ref) => {
      const cards = [];
      const ps = c.pair_state;
      if (ps && ps.matched !== false) {
        cards.push(card("Pair state", pill(ps.enabled ? "Active" : "Disabled", ps.enabled ? "ok" : "neutral"),
          [esc(FEATURE_LABEL[ref.feature] || ref.feature || ""), esc(ps.label || ""), esc(modeLabel(ps.mode))]));
      } else {
        cards.push(card("Pair state", pill("Unmatched", "neutral"),
          ["Pair not matched in current configuration", ps && ps.label ? `<span class="mono">${esc(ps.label)}</span>` : ""]));
      }

      const ph = c.provider_health;
      if (!ph || ph.status_available === false) {
        cards.push(card("Provider health", pill("Unknown", "neutral"), ["Status unavailable"]));
      } else {
        const entries = Object.entries(ph.providers || {});
        const anyDown = entries.some(([, h]) => h.configured !== false && h.status !== "unknown" && !h.connected);
        const anyUnknown = entries.some(([, h]) => h.status === "unknown" || h.configured === false);
        const allOk = entries.length > 0 && entries.every(([, h]) => h.connected || h.status === "ok");
        const st = anyDown ? pill("Degraded", "bad") : (allOk ? pill("Healthy", "ok") : pill(anyUnknown ? "Unknown" : "Healthy", anyUnknown ? "neutral" : "ok"));
        const names = entries.map(([n, h]) => h.configured === false ? `${esc(n)} — not configured` : esc(instLabel(n, h.instance)));
        cards.push(card("Provider health", st, [names.join("<br>") || "—", ph.checked_at ? `Last check: ${esc(TS(ph.checked_at))}` : ""]));
      }

      const us = c.unresolved_state;
      const uPresent = !!(us && us.present);
      const meta = (us && us.meta) || {};
      const since = meta.since ?? meta.first_seen ?? meta.first_seen_at ?? meta.ts ?? meta.created_at;
      const attempts = meta.attempts ?? meta.count ?? meta.tries;
      cards.push(card("Unresolved state", pill(uPresent ? "Yes" : "No", uPresent ? "warn" : "neutral"),
        uPresent ? [since ? `Since: ${esc(maybeTS(since))}` : "", attempts != null ? `Attempts: ${esc(attempts)}` : ""] : ["Not currently unresolved"]));

      const bb = c.blackbox_state;
      const bPresent = !!(bb && bb.present);
      cards.push(card("Blackbox state", pill(bPresent ? "Yes" : "No", bPresent ? "error" : "neutral"), [bPresent ? "Blackboxed" : "Not blackboxed"]));

      const tb = c.tombstone_state;
      const tPresent = !!(tb && tb.present);
      cards.push(card("Tombstone state", pill(tPresent ? "Yes" : "No", tPresent ? "warn" : "neutral"), [tPresent ? "Tombstoned" : "No tombstone"]));

      const af = c.analyzer_findings;
      const cnt = (af && af.count) || 0;
      cards.push(card("Analyzer findings", pill(cnt ? String(cnt) : "None", cnt ? "ok" : "neutral"),
        [cnt ? `Present in ${cnt} provider baseline(s)` : "Not in analyzer baselines"],
        ref.item_key ? `<button class="ev-card-link" id="ev-open-analyzer" type="button"><span class="material-symbols-rounded" aria-hidden="true">monitoring</span>View in Analyzer</button>` : ""));

      return `<div class="ev-cards">${cards.join("")}</div>`;
    };

    // context summary 
    const EVENT_DESC = {
      write_failed: "Provider write failed while adding the item.",
      write_succeeded: "Item written successfully.",
      write_attempted: "Write attempted on destination.",
      unresolved_recorded: "Item marked unresolved and will be retried in future sync runs.",
      unresolved_cleared: "Unresolved state cleared.",
      blackbox_promoted: "Promoted to blackbox after repeated failures.",
      blackbox_blocked: "Blocked by blackbox.",
      plan_created: "Included in the sync plan.",
      provider_health: "Provider health checked.",
      sync_run_started: "Sync run started.",
      sync_run_finished: "Sync run completed.",
      tombstone_created: "Tombstone created.",
      tombstone_pruned: "Tombstone pruned.",
    };
    const eventDesc = (e) => EVENT_DESC[e.event_type] || "";

    const providerLines = (c) => {
      const ph = c.provider_health;
      if (!ph || ph.status_available === false) return "Status unavailable";
      const entries = Object.entries(ph.providers || {});
      return entries.map(([n, h]) => h.configured === false ? `${esc(n)} — not configured` : esc(instLabel(n, h.instance))).join("<br>") || "–";
    };

    const ctxSummary = (c) => {
      const ps = c.pair_state;
      const pairState = (ps && ps.matched !== false) ? (ps.enabled ? "Active" : "Disabled") : "Unmatched";
      const ph = c.provider_health;
      let health = "Unknown", healthCls = "neutral", lastCheck = "";
      if (ph && ph.status_available !== false) {
        const entries = Object.entries(ph.providers || {});
        const anyDown = entries.some(([, h]) => h.configured !== false && h.status !== "unknown" && !h.connected);
        const allOk = entries.length > 0 && entries.every(([, h]) => h.connected || h.status === "ok");
        health = anyDown ? "Degraded" : (allOk ? "Healthy" : "Unknown");
        healthCls = anyDown ? "bad" : (allOk ? "ok" : "neutral");
        lastCheck = ph.checked_at ? TS(ph.checked_at) : "";
      }
      const us = c.unresolved_state;
      const uPresent = !!(us && us.present);
      const meta = (us && us.meta) || {};
      const uSince = meta.since ?? meta.first_seen ?? meta.first_seen_at ?? meta.ts ?? meta.created_at;
      return { pairState, pairEnabled: ps && ps.enabled, pairLabel: ps && ps.label, pairMode: ps && ps.mode, health, healthCls, lastCheck, uPresent, uSince };
    };

    const flashCopy = (btn) => {
      const ic = btn?.querySelector(".material-symbols-rounded");
      if (ic) { const t = ic.textContent; ic.textContent = "check"; setTimeout(() => { ic.textContent = t; }, 1200); }
    };

    // ---- group detail ------------------------------------------------------
    const renderGroupDetail = (detail) => {
      const g = detail.group || {};
      const c = detail.context || {};
      const events = detail.events || [];
      const related = detail.related_groups || [];
      const sv = groupSev(g);
      const ps = c.pair_state;
      const routeShort = provShort(g.source_provider, g.destination_provider) || "–";
      const pairFull = (ps && ps.matched && ps.label) ? ps.label : (g.pair_key || "–");
      const origin = g.origin_provider ? instLabel(g.origin_provider, g.origin_instance) : "unknown";
      const dest = instLabel(g.destination_provider, g.destination_instance) || "–";
      const item = titleOf(g);
      const feat = FEATURE_LABEL[g.feature] || g.feature || "–";
      const reason = g.reason || g.reason_code || "";
      const runId = events.map((e) => e.run_id).find(Boolean) || "";
      const cx = ctxSummary(c);
      const modeTxt = modeLabel(cx.pairMode) || "–";
      const st = String(g.status || "").toLowerCase();
      const isProblem = ["failed", "unresolved", "blackboxed"].includes(st);

      const pill = (text, cls) => `<span class="ev-pill ${cls}">${esc(text)}</span>`;
      const card = (title, pillHTML, lines, extra = "") => `
        <div class="ev-card">
          <div class="ev-card-head"><span class="ev-card-title">${esc(title)}</span>${pillHTML || ""}</div>
          <div class="ev-card-body">${lines.filter(Boolean).map((l) => `<div class="ev-card-line">${l}</div>`).join("")}</div>
          ${extra}
        </div>`;

      const problemSub = st === "failed" ? `${g.operation || "sync"} operation failed on destination provider`
        : st === "unresolved" ? `${g.operation || "sync"} failed, recorded unresolved for retry`
        : st === "blackboxed" ? "Promoted to blackbox after repeated failures"
        : "Completed";
      const scard = (icon, cls, title, mainHTML, subs, badge = "") => `
        <div class="ev-scard">
          <div class="ev-scard-head"><span class="ev-scard-ic ${cls}"><span class="material-symbols-rounded" aria-hidden="true">${icon}</span></span><span class="ev-scard-title ${cls}">${esc(title)}</span></div>
          <div class="ev-scard-main">${mainHTML}</div>
          ${subs.filter(Boolean).map((s) => `<div class="ev-scard-sub">${s}</div>`).join("")}
          ${badge}
        </div>`;
      const STATUS_EVENT_TYPES = {
        resolved: ["write_succeeded", "unresolved_cleared"], blackboxed: ["blackbox_promoted", "blackbox_blocked"],
        unresolved: ["unresolved_recorded"], failed: ["write_failed"], pending: ["write_attempted"],
        completed: ["sync_run_finished"], running: ["sync_run_started"],
      };
      const statusAt = (() => {
        const wanted = STATUS_EVENT_TYPES[st] || [];
        let ts = 0;
        for (const e of events) { if (wanted.includes(e.event_type)) { const t = Number(e.created_at || 0); if (t >= ts) ts = t; } }
        return ts || g.last_event_at || g.first_event_at;
      })();
      const summaryCards =
        scard(isProblem ? "error" : "check_circle", isProblem ? "error" : "ok", isProblem ? "Problem" : "Outcome",
          `<span>${esc(reasonLabel(reason) || statusLabel(g.status))}</span>`, [esc(problemSub)]) +
        scard("swap_horiz", "info", "Route", esc(routeShort), [esc(modeTxt)],
          feat && feat !== "–" ? `<span class="ev-badge info">${esc(feat)}</span>` : "") +
        scard("inventory_2", "info", "Item", g.item_key ? `<span class="mono">${esc(g.item_key)}</span>` : "–", [item ? esc(item) : ""]) +
        scard(groupIcon(g), sv, "State", esc(statusLabel(g.status)), [`Since ${esc(TS(statusAt))}`]);

      const timeline = events.slice().reverse().map((e) => {
        const es = sevOf(e);
        const desc = eventDesc(e);
        return `
        <div class="ev-tl" data-id="${e.id}">
          <span class="ev-tl-time">${esc(TS(e.created_at))}</span>
          <span class="ev-tl-dot ${es}"></span>
          <span class="ev-tl-body">
            <span class="ev-tl-head"><span class="ev-tl-title">${esc(titleLine(e))}</span><span class="ev-badge ${es}">${esc(badgeOf(e))}</span></span>
            ${desc ? `<span class="ev-tl-desc">${esc(desc)}</span>` : ""}
            ${e.reason_code ? `<span class="ev-tl-code mono">${esc(e.reason_code)}</span>` : ""}
          </span>
        </div>`;
      }).join("");

      const livePill = (label, val, cls) => `<span class="ev-livepill ${cls}"><span class="ev-livepill-k">${esc(label)}</span><span class="ev-livepill-v">${esc(val)}</span></span>`;
      const liveBar = `
        <div class="ev-livebar">
          ${livePill("Pair", cx.pairState, cx.pairState === "Active" ? "ok" : "neutral")}
          ${livePill("Provider", cx.health, cx.healthCls)}
          ${livePill("Unresolved", cx.uPresent ? "Yes" : "No", cx.uPresent ? "warn" : "neutral")}
          <button class="ev-livemore" id="ev-live-details" type="button"><span>Full context</span><span class="material-symbols-rounded" aria-hidden="true">chevron_right</span></button>
        </div>`;

      const kvGrid = (rows) => `<div class="ev-kv">${rows.map(([k, v]) => `<div class="k">${esc(k)}</div><div class="v">${v}</div>`).join("")}</div>`;

      const detailsPane =
        `<div class="ev-dsec"><h5>Event</h5>${kvGrid([
          ["Status", `<span class="ev-pill ${sv}">${esc(statusLabel(g.status))}</span>`],
          ["Title", esc(g.summary || "–")],
          ["Item", item ? esc(item) : "–"],
          ["Reason", esc(reason || "–")],
          ["Events", esc(String(g.event_count || events.length))],
          ["First seen", esc(TS(g.first_event_at))],
          ["Last seen", esc(TS(g.last_event_at))],
        ])}</div>` +
        `<div class="ev-dsec"><h5>Route</h5>${kvGrid([
          ["Feature", esc(feat)],
          ["Operation", esc(g.operation || "–")],
          ["Route", esc(routeShort)],
          ["Origin", esc(origin)],
          ["Destination", esc(dest)],
          ["Pair", esc(pairFull)],
          ["Mode", esc(modeTxt)],
        ])}</div>` +
        `<div class="ev-dsec"><h5>Current context <span class="ev-h4-note">live now</span></h5>${kvGrid([
          ["Pair state", esc(cx.pairState)],
          ["Provider health", esc(cx.health)],
          ["Unresolved state", cx.uPresent ? "Yes" : "No"],
          ["Last provider check", cx.lastCheck ? esc(cx.lastCheck) : "–"],
        ])}</div>`;

      const eventIds = events.map((e) => e.id).join(", ");
      const rawFields = {
        status: g.status, feature: g.feature, operation: g.operation, route: routeShort,
        origin, destination: dest, reason, first_seen: TS(g.first_event_at), last_seen: TS(g.last_event_at), event_count: g.event_count,
      };
      const rawPane =
        `<div class="ev-dsec"><h5>Identifiers</h5>${kvGrid([
          ["Thread ID", `<span class="mono">${esc(String(g.id))}</span>`],
          ["Run ID", runId ? `<span class="mono">${esc(runId)}</span>` : "–"],
          ["Event IDs", `<span class="mono">${esc(eventIds || "–")}</span>`],
          ["Pair ID", `<span class="mono">${esc((ps && ps.id) || g.pair_key || "–")}</span>`],
          ["Item key", g.item_key ? `<span class="mono">${esc(g.item_key)}</span>` : "–"],
        ])}</div>` +
        `<div class="ev-dsec"><h5>Raw fields</h5>
          <details class="ev-tech"><summary>Fields</summary><div class="ev-tech-body"><pre class="cw-scrollbars">${esc(JSON.stringify(rawFields, null, 2))}</pre></div></details>
          <details class="ev-tech"><summary>Full payload</summary><div class="ev-tech-body"><div class="ev-tech-kv"><span>Group hash</span><span class="mono">${esc(g.group_hash || "–")}</span></div><pre class="cw-scrollbars">${esc(JSON.stringify({ group: g, context: c, events }, null, 2))}</pre></div></details>
        </div>` +
        `<div class="ev-dsec"><h5>Actions</h5><div class="ev-rawacts">
          <button class="ev-tbtn" id="ev-copy-raw" type="button"><span class="material-symbols-rounded" aria-hidden="true">content_copy</span><span>Copy raw data</span></button>
          <button class="ev-tbtn" id="ev-copy-item" type="button"><span class="material-symbols-rounded" aria-hidden="true">content_copy</span><span>Copy item key</span></button>
          <button class="ev-tbtn" id="ev-copy-run" type="button"><span class="material-symbols-rounded" aria-hidden="true">content_copy</span><span>Copy run ID</span></button>
        </div></div>`;

      const relRow = (r, primary) => `
          <button class="ev-rel" type="button" data-gid="${r.id}">
            <span class="ev-rel-ic ${groupSev(r)}"><span class="material-symbols-rounded" aria-hidden="true">${groupIcon(r)}</span></span>
            <span class="ev-rel-badge ${groupSev(r)}">${esc(statusLabel(r.status))}</span>
            <span class="ev-rel-title">${esc(primary || "")}</span>
            <span class="ev-rel-time">${esc(TS(r.last_event_at))}</span>
          </button>`;
      const relHTML = related.length
        ? related.slice(0, 12).map((r) => relRow(r, r.summary || titleOf(r))).join("")
        : "";

      const runItemsSectionHTML = (items, total, limit, offset) => {
        const shown = items.length;
        const safeTotal = Math.max(Number(total || 0), shown);
        if (!safeTotal) return "";
        const safeLimit = Math.max(1, Number(limit || RUN_ITEMS_PAGE_SIZE));
        const safeOffset = Math.max(0, Number(offset || 0));
        const start = shown ? safeOffset + 1 : 0;
        const end = shown ? Math.min(safeOffset + shown, safeTotal) : 0;
        const hasPrev = safeOffset > 0;
        const hasNext = safeOffset + shown < safeTotal;
        const range = safeTotal > safeLimit
          ? `Showing ${start}-${end} of ${safeTotal} items`
          : `${safeTotal} item${safeTotal === 1 ? "" : "s"}`;
        return `<section id="ev-runitems-section" class="ev-runitems-section">
          <div class="ev-runitems-head">
            <h4>Problem items in this run <span class="ev-h4-note">${esc(range)}</span></h4>
            ${safeTotal > safeLimit ? `<div class="ev-runitems-controls">
              <button class="ev-pg" id="ev-runitems-prev" type="button" aria-label="Previous problem items" ${hasPrev ? "" : "disabled"}><span class="material-symbols-rounded" aria-hidden="true">chevron_left</span></button>
              <button class="ev-pg" id="ev-runitems-next" type="button" aria-label="Next problem items" ${hasNext ? "" : "disabled"}><span class="material-symbols-rounded" aria-hidden="true">chevron_right</span></button>
            </div>` : ""}
          </div>
          <div class="ev-related">${shown ? items.map((r) => relRow(r, titleOf(r) || r.summary)).join("") : `<div class="ev-empty ev-empty-inline">No problem items on this page.</div>`}</div>
        </section>`;
      };
      const runItems = detail.run_items || [];
      const runItemsHTML = runItemsSectionHTML(
        runItems,
        detail.run_items_total,
        detail.run_items_limit,
        detail.run_items_offset,
      );

      const ackLabel = g.acknowledged_at ? "Acknowledged" : "Acknowledge";
      const ackIcon = g.acknowledged_at ? "check_circle" : "check";
      const tabAttr = (t) => detailTab === t ? "" : " hidden";

      detailEl.innerHTML = `
        <div class="ev-dhead">
          <div class="ev-dhead-row">
            <span class="ev-badge ${sv}">${esc(statusLabel(g.status))}</span>
            <span class="ev-dhead-spacer"></span>
            <span class="ev-dtime">${esc(TS(g.last_event_at))}</span>
            <button class="ev-hbtn" id="ev-copy" type="button" title="Copy thread"><span class="material-symbols-rounded" aria-hidden="true">content_copy</span><span>Copy</span></button>
            <button class="ev-hbtn ev-hbtn-accent${g.acknowledged_at ? " on" : ""}" id="ev-ack-detail" type="button"><span class="material-symbols-rounded" aria-hidden="true">${ackIcon}</span><span>${ackLabel}</span></button>
          </div>
          <div class="ev-dtitle">${esc(g.summary || item || "")}</div>
          ${item ? `<div class="ev-ditem">${esc(item)}</div>` : ""}
        </div>
        <div class="ev-scards">${summaryCards}</div>
        <div class="ev-tabs" role="tablist">
          <button class="ev-tab${detailTab === "timeline" ? " on" : ""}" data-tab="timeline" type="button"><span class="material-symbols-rounded" aria-hidden="true">timeline</span>Timeline</button>
          <button class="ev-tab${detailTab === "details" ? " on" : ""}" data-tab="details" type="button"><span class="material-symbols-rounded" aria-hidden="true">description</span>Details</button>
          <button class="ev-tab${detailTab === "raw" ? " on" : ""}" data-tab="raw" type="button"><span class="material-symbols-rounded" aria-hidden="true">data_object</span>Raw data</button>
        </div>
        <div class="ev-tabpanes">
          <div class="ev-tabpane" data-pane="timeline"${tabAttr("timeline")}>
            ${g.item_key ? liveBar : runItemsHTML}
            <div class="ev-timeline">${timeline || `<div class="ev-empty ev-empty-inline">No events in this thread.</div>`}</div>
          </div>
          <div class="ev-tabpane" data-pane="details"${tabAttr("details")}>${detailsPane}</div>
          <div class="ev-tabpane" data-pane="raw"${tabAttr("raw")}>${rawPane}</div>
        </div>
        ${relHTML ? `<h4>Related threads</h4><div class="ev-related">${relHTML}</div>` : ""}`;
      detailEl.scrollTop = 0;

      const panes = detailEl.querySelectorAll(".ev-tabpane");
      const switchTab = (name) => {
        detailTab = name; lset("cw.events.detailtab", name);
        detailEl.querySelectorAll(".ev-tab").forEach((x) => x.classList.toggle("on", x.dataset.tab === name));
        panes.forEach((p) => { p.hidden = p.dataset.pane !== name; });
      };
      detailEl.querySelectorAll(".ev-tab").forEach((t) => t.addEventListener("click", () => switchTab(t.dataset.tab)));
      Q("#ev-live-details", detailEl)?.addEventListener("click", () => switchTab("details"));

      Q("#ev-ack-detail", detailEl)?.addEventListener("click", () => {
        if (g.acknowledged_at) unacknowledgeRow(g.id); else acknowledgeRow(g.id);
      });
      Q("#ev-copy", detailEl)?.addEventListener("click", (ev) => {
        navigator.clipboard?.writeText([
          `Thread: ${g.summary || ""}`, `Status: ${g.status}`,
          `Feature: ${g.feature || ""}`, `Route: ${routeShort}`, `Pair: ${pairFull}`,
          `Item: ${g.item_key || ""}`, `Events: ${g.event_count}`,
          `First seen: ${TS(g.first_event_at)}`, `Last seen: ${TS(g.last_event_at)}`,
          `Reason: ${reason}`, "",
          ...events.map((e) => `  ${TS(e.created_at)} · ${badgeOf(e)} · ${titleLine(e)}`),
        ].join("\n"));
        flashCopy(ev.currentTarget);
      });
      Q("#ev-copy-raw", detailEl)?.addEventListener("click", (ev) => { navigator.clipboard?.writeText(JSON.stringify({ group: g, context: c, events }, null, 2)); flashCopy(ev.currentTarget); });
      Q("#ev-copy-item", detailEl)?.addEventListener("click", (ev) => { navigator.clipboard?.writeText(g.item_key || ""); flashCopy(ev.currentTarget); });
      Q("#ev-copy-run", detailEl)?.addEventListener("click", (ev) => { navigator.clipboard?.writeText(runId); flashCopy(ev.currentTarget); });
      const bindRelRows = (root = detailEl) => {
        root.querySelectorAll(".ev-rel").forEach((a) => a.addEventListener("click", () => a.dataset.gid && select(a.dataset.gid)));
      };
      const bindRunItemsPager = () => {
        const section = Q("#ev-runitems-section", detailEl);
        if (!section) return;
        const limit = Number(detail.run_items_limit || RUN_ITEMS_PAGE_SIZE);
        const offset = Number(detail.run_items_offset || 0);
        const load = async (nextOffset) => {
          section.querySelectorAll(".ev-pg").forEach((b) => { b.disabled = true; });
          try {
            const page = await fjson(`/api/events/groups/${encodeURIComponent(g.id)}/run-items?limit=${RUN_ITEMS_PAGE_SIZE}&offset=${Math.max(0, nextOffset)}`);
            if (!alive) return;
            if (!page || page.ok === false) throw new Error("not found");
            detail.run_items = page.items || [];
            detail.run_items_total = page.total || 0;
            detail.run_items_limit = page.limit || RUN_ITEMS_PAGE_SIZE;
            detail.run_items_offset = page.offset || 0;
            section.outerHTML = runItemsSectionHTML(detail.run_items, detail.run_items_total, detail.run_items_limit, detail.run_items_offset);
            const nextSection = Q("#ev-runitems-section", detailEl);
            if (nextSection) bindRelRows(nextSection);
            bindRunItemsPager();
          } catch {
            section.outerHTML = runItemsSectionHTML(detail.run_items || [], detail.run_items_total, detail.run_items_limit, detail.run_items_offset);
            const nextSection = Q("#ev-runitems-section", detailEl);
            if (nextSection) bindRelRows(nextSection);
            bindRunItemsPager();
          }
        };
        Q("#ev-runitems-prev", section)?.addEventListener("click", () => load(offset - limit));
        Q("#ev-runitems-next", section)?.addEventListener("click", () => load(offset + limit));
      };
      bindRelRows();
      bindRunItemsPager();
    };

    // ---- event detail (raw mode) ------------------------------------------
    const renderEventDetail = (ctx, e) => {
      const c = ctx.context || {};
      const rel = ctx.related || {};
      const sv = sevOf(e);
      const ps = c.pair_state;
      const routeShort = provShort(e.source_provider, e.destination_provider) || "–";
      const pairLabel = (ps && ps.matched && ps.label) ? `${ps.label}${ps.mode ? ` (${modeLabel(ps.mode).toLowerCase()})` : ""}` : (ps && ps.label ? `${ps.label} — not matched` : (e.pair_key || "–"));
      const origin = e.origin_provider ? instLabel(e.origin_provider, e.origin_instance) : "unknown";
      const conf = e.origin_confidence ? `<span class="ev-conf">${esc(String(e.origin_confidence))} confidence</span>` : "";
      const dest = instLabel(e.destination_provider, e.destination_instance) || "–";
      const runTime = e.source_mtime ? TS(e.source_mtime) : "";

      const kv = [
        ["Type", esc(String(e.event_type || "").replace(/_/g, " "))],
        ["Feature", esc(FEATURE_LABEL[e.feature] || e.feature || "–")],
        ["Operation", esc(e.operation || "–")],
        ["Route", esc(routeShort)],
        ["Origin", `${esc(origin)}${conf ? ` ${conf}` : ""}`],
        ["Destination", esc(dest)],
        ["Pair", esc(pairLabel)],
        ["Run", e.run_id ? `<span class="mono">${esc(e.run_id)}</span>` : "–"],
        ["Reason", esc(e.reason || e.reason_code || "–")],
        ["Source", e.source_file ? `<span class="mono">${esc(e.source_file)}</span>` : esc(e.source_kind || "–")],
        ["Run time", runTime ? esc(runTime) : "–"],
      ].map(([k, v]) => `<div class="k">${esc(k)}</div><div class="v">${v}</div>`).join("");

      const item = titleOf(e);
      const parts = [];
      parts.push(`
        <div class="ev-dhead">
          <div class="ev-dhead-row">
            <span class="ev-badge ${sv}">${esc(badgeOf(e))}</span>
            <span class="ev-dhead-spacer"></span>
            <span class="ev-dtime">${esc(TS(e.created_at))}</span>
            <button class="ev-icon-btn" id="ev-copy" type="button" title="Copy event" aria-label="Copy event"><span class="material-symbols-rounded" aria-hidden="true">content_copy</span></button>
          </div>
          <div class="ev-dtitle">${esc(titleLine(e))}</div>
          ${item ? `<div class="ev-ditem">${esc(item)}</div>` : ""}
        </div>`);
      parts.push(`<h4>Event</h4><div class="ev-kv">${kv}</div>`);
      if (isRating(e)) parts.push(`<div class="ev-ratline">Rating changed from <b>${esc(e.old_value ?? "–")}</b> to <b>${esc(e.new_value ?? "–")}</b>${e.origin_provider ? `, originating at <b>${esc(instLabel(e.origin_provider, e.origin_instance))}</b>` : ""}.</div>`);
      parts.push(`<h4>Current context</h4>`);
      parts.push(renderCards(c, e));
      parts.push(`<details class="ev-tech"><summary>Technical details</summary><div class="ev-tech-body"><div class="ev-tech-kv"><span>Pair key</span><span class="mono">${esc(e.pair_key || "–")}</span></div><pre class="cw-scrollbars">${esc(JSON.stringify({ event: e, context: c }, null, 2))}</pre></div></details>`);

      detailEl.innerHTML = parts.join("");
      detailEl.scrollTop = 0;
      Q("#ev-copy", detailEl)?.addEventListener("click", () => {
        navigator.clipboard?.writeText([`Event: ${titleLine(e)}`, `Type: ${e.event_type}`, `Route: ${routeShort}`, `Pair: ${pairLabel}`, `Item: ${e.item_key || ""}`, `When: ${TS(e.created_at)}`, `Reason: ${e.reason || e.reason_code || ""}`].join("\n"));
        const b = Q("#ev-copy", detailEl); if (b) { const ic = b.querySelector(".material-symbols-rounded"); if (ic) { ic.textContent = "check"; setTimeout(() => { ic.textContent = "content_copy"; }, 1200); } }
      });
      Q("#ev-open-analyzer", detailEl)?.addEventListener("click", () => { try { window.cxCloseModal?.(); window.openAnalyzer?.(); } catch {} });
    };

    const select = async (id, el) => {
      state.selected = String(id);
      state.relExpanded = false;
      listEl.querySelectorAll(".ev-row.sel").forEach((n) => n.classList.remove("sel"));
      (el || listEl.querySelector(`.ev-row[data-id="${id}"]`))?.classList.add("sel");
      detailEl.innerHTML = `<div class="ev-empty ev-empty-detail"><span class="material-symbols-rounded ev-spin" aria-hidden="true">progress_activity</span><div>Loading context…</div></div>`;
      try {
        if (grouped()) {
          const detail = await fjson(`/api/events/groups/${encodeURIComponent(id)}?run_items_limit=${RUN_ITEMS_PAGE_SIZE}&run_items_offset=0`);
          if (!alive) return;
          if (!detail || detail.ok === false) throw new Error("not found");
          renderGroupDetail(detail);
        } else {
          const ctx = await fjson(`/api/events/context?event_id=${encodeURIComponent(id)}`);
          if (!alive) return;
          const e = ctx.event || state.items.find((x) => String(x.id) === String(id)) || {};
          renderEventDetail(ctx, e);
        }
      } catch (err) {
        if (!alive) return;
        if (!grouped()) {
          const e = state.items.find((x) => String(x.id) === String(id));
          if (e) { renderEventDetail({ event: e, context: {}, related: {} }, e); return; }
        }
        detailEl.innerHTML = `<div class="ev-empty ev-empty-detail"><span class="material-symbols-rounded" aria-hidden="true">error</span><div>Failed to load details (${esc(err.message)}).</div></div>`;
      }
    };

    const populateFilters = async () => {
      try {
        const cfg = await fjson("/api/config").catch(() => ({}));
        const provs = new Set();
        const pairs = [];
        const seenPairs = new Set();
        for (const p of (cfg?.pairs || [])) {
          const src = String(p.source || "").toUpperCase();
          const tgt = String(p.target || "").toUpperCase();
          if (src) provs.add(src);
          if (tgt) provs.add(tgt);
          if (src && tgt) {
            const key = [src, tgt].sort().join("-");
            if (!seenPairs.has(key)) {
              seenPairs.add(key);
              const si = String(p.source_instance || "").trim();
              const ti = String(p.target_instance || "").trim();
              const md = String(p.mode || "").toLowerCase();
              const arrow = md.includes("two") ? "↔" : "→";
              pairs.push({ value: key, label: `${instLabel(src, si)} ${arrow} ${instLabel(tgt, ti)}`, hint: md.includes("two") ? "two-way" : "one-way" });
            }
          }
        }
        const provItems = [...provs].sort();
        ddProvider.setItems([{ value: "", label: "Any provider" }, ...provItems.map((p) => ({ value: p, label: p }))]);
        ddOrigin.setItems([{ value: "", label: "Any origin" }, ...provItems.map((p) => ({ value: p, label: p }))]);
        ddPair.setItems([{ value: "", label: "Any pair" }, ...pairs]);
      } catch {}
    };

    const resetFilters = () => {
      qEl.value = "";
      ddCategory.reset(); ddType.reset(); ddProvider.reset(); ddOrigin.reset(); ddFeature.reset(); ddPair.reset();
      dateRange = "all"; lset("cw.events.range", "all"); ddRange.value = "all";
      sinceEl.value = ""; untilEl.value = ""; syncRangeUI();
      load(0);
    };

    let refreshing = false;
    const doRefresh = async () => {
      if (refreshing) return;
      refreshing = true;
      const btn = Q("#ev-refresh", root);
      btn?.classList.add("busy");
      try {
        await populateFilters();
        if (!alive) return;
        await load(state.page);
      } finally { refreshing = false; btn?.classList.remove("busy"); }
    };

    let searchTimer = null;
    qEl.addEventListener("input", () => { clearTimeout(searchTimer); searchTimer = setTimeout(() => load(0), 250); });
    sinceEl.addEventListener("change", () => load(0));
    untilEl.addEventListener("change", () => load(0));
    Q("#ev-clear-hidden", root).addEventListener("click", () => clearHidden());

    Q("#ev-more", root).addEventListener("click", (ev) => {
      const btn = ev.currentTarget;
      const open = moreWrap.hasAttribute("hidden");
      if (open) moreWrap.removeAttribute("hidden"); else moreWrap.setAttribute("hidden", "");
      btn.setAttribute("aria-expanded", String(open));
      btn.classList.toggle("on", open);
    });
    Q("#ev-refresh", root).addEventListener("click", () => doRefresh());
    Q("#ev-clear", root).addEventListener("click", async () => {
      if (!window.confirm("Delete all events from the archive?\n\nThis clears the SQLite event archive. New syncs will record events again.\nThis cannot be undone.")) return;
      const btn = Q("#ev-clear", root);
      btn?.classList.add("busy");
      try {
        const res = await fjson("/api/events/clear", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ confirm: true }) });
        if (!alive) return;
        state.selected = null; state.collapsed.clear(); clearDetail();
        await load(0);
        showToast(res?.ok ? `Archive cleared (${state.total} remaining).` : "Clear failed.");
      } catch (err) { showToast(`Clear failed: ${err.message}`); } finally { btn?.classList.remove("busy"); }
    });
    Q("#ev-prev-top", root).addEventListener("click", () => { if (state.page > 0) load(state.page - 1); });
    Q("#ev-next-top", root).addEventListener("click", () => { if (state.page < pageCount() - 1) load(state.page + 1); });
    Q("#ev-close", root).addEventListener("click", () => { window.cxCloseModal?.(); });

    cleanup = () => {
      alive = false;
      clearTimeout(searchTimer);
      clearTimeout(toastTimer);
      window.removeEventListener("pointermove", onSplitMove, true);
      window.removeEventListener("pointerup", endSplit, true);
      document.body.style.userSelect = "";
      for (const dd of [...dropdowns, ddSort]) dd.close();
      cleanup = null;
    };

    syncRangeUI();
    await populateFilters();
    await load(0);
  },
  unmount() { cleanup?.(); },
};
