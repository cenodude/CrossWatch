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

const TZ = (() => { try { return Intl.DateTimeFormat().resolvedOptions().timeZone || ""; } catch { return ""; } })();

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

const FEATURE_LABEL = { history: "History", ratings: "Ratings", watchlist: "Watchlist", progress: "Progress" };

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

const epTag = (s, e) => `S${String(s).padStart(2, "0")}E${String(e).padStart(2, "0")}`;

const titleOf = (e) => {
  const hasEp = e.season != null && e.episode != null && (e.media_type === "episode" || !e.media_type);
  const tag = hasEp ? epTag(e.season, e.episode) : "";
  let t = String(e.title || "").trim();
  // Drop a title that is just the episode code (or empty) to avoid "S27E06 S27E06".
  if (tag && t.toUpperCase() === tag) t = "";
  if (hasEp) return t ? `${t} ${tag}` : tag;
  if (e.media_type === "movie" && e.year && t) return `${t} (${e.year})`;
  return t || e.item_key || "";
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
  if (["unresolved_recorded", "blackbox_promoted", "blackbox_blocked", "plan_created", "api_rate_limit"].includes(e.event_type)) return "warn";
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
    case "write_failed": return `${(e.destination_provider || "Destination").toUpperCase()} rejected item · ${e.reason_code || "failed"}`;
    case "unresolved_recorded": {
      const r = e.reason_code || e.operation || "";
      return r ? `Recorded as unresolved · ${r}` : "Recorded as unresolved";
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
const groupSev = (g) => GROUP_SEV[String(g.severity || "").toLowerCase()] || "info";
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
let lastAutoRefresh = 0;

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

    const state = { items: [], page: 0, total: 0, selected: null, relExpanded: false };
    const dropdowns = [];

    root.innerHTML = `
      <div class="ev-app">
        <div class="cx-head">
          <div class="ev-head-left">
            <div class="ev-title">Events</div>
          </div>
          <div class="ev-actions">
            <button class="close-btn" id="ev-close" type="button"><span class="material-symbols-rounded" aria-hidden="true">close</span><span>Close</span></button>
          </div>
        </div>

        <div class="ev-toolbar">
          <div class="ev-toolbar-row">
            <label class="ev-search"><span class="material-symbols-rounded" aria-hidden="true">search</span><input id="ev-q" type="text" placeholder="Search title, item, reason, run…"></label>
            <div class="ev-filters" id="ev-filters"></div>
          </div>
          <div class="ev-toolbar-row ev-toolbar-sub">
            <div class="ev-vis"><span class="ev-vis-label">Visibility:</span><span id="ev-seg"></span></div>
            <label class="ev-date"><span class="material-symbols-rounded" aria-hidden="true">event</span><input id="ev-since" type="date" aria-label="Since"></label>
            <label class="ev-date"><span class="material-symbols-rounded" aria-hidden="true">event</span><input id="ev-until" type="date" aria-label="Until"></label>
            <button class="ev-tbtn" id="ev-more" type="button" aria-expanded="false"><span class="material-symbols-rounded" aria-hidden="true">tune</span><span>More filters</span><span class="material-symbols-rounded ev-tbtn-caret" aria-hidden="true">expand_more</span></button>
            <button class="ev-tbtn" id="ev-refresh" type="button"><span class="material-symbols-rounded" aria-hidden="true">refresh</span><span>Refresh</span></button>
          </div>
          <div class="ev-morefilters" id="ev-morefilters" hidden>
            <label class="ev-mini"><span>Reason code</span><input id="ev-reason" type="text" placeholder="e.g. simkl_not_found"></label>
            <label class="ev-mini"><span>Run id</span><input id="ev-run" type="text" placeholder="e.g. 9f3c2d8e"></label>
          </div>
        </div>

        <div class="ev-layout">
          <div class="ev-col-list">
            <div class="ev-list-head">
              <div class="ev-list-head-left"><span id="ev-mode"></span><div class="ev-count" id="ev-count">—</div></div>
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

        <div class="ev-note">Times are shown in your local time${TZ ? ` (${esc(TZ)})` : ""}.</div>
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
    const reasonEl = Q("#ev-reason", root);
    const runEl = Q("#ev-run", root);
    const moreWrap = Q("#ev-morefilters", root);
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

    const ddType = createDropdown({ label: "All types", items: EVENT_TYPES.map((t) => ({ value: t, label: t ? BADGE[t] || t.replace(/_/g, " ") : "All types" })), onChange: () => load(0) });
    const ddProvider = createDropdown({ label: "Any provider", items: [{ value: "", label: "Any provider" }], onChange: () => load(0) });
    const ddOrigin = createDropdown({ label: "Any origin", items: [{ value: "", label: "Any origin" }], onChange: () => load(0) });
    const ddFeature = createDropdown({ label: "Any feature", items: [{ value: "", label: "Any feature" }, ...["history", "ratings", "watchlist", "progress"].map((f) => ({ value: f, label: FEATURE_LABEL[f] }))], onChange: () => load(0) });
    const ddPair = createDropdown({ label: "Any pair", items: [{ value: "", label: "Any pair" }], onChange: () => load(0), align: "right" });
    const ddCategory = createDropdown({
      label: "Any outcome", align: "right",
      items: [{ value: "", label: "Any outcome" }, { value: "successful", label: "Successful" }, { value: "problems", label: "Problems" }, { value: "informational", label: "Informational" }],
      onChange: () => load(0),
    });
    dropdowns.push(ddCategory, ddType, ddProvider, ddOrigin, ddFeature, ddPair);
    for (const dd of dropdowns) filtersEl.appendChild(dd.el);

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

    const filtersActive = () => !!(qEl.value.trim() || ddType.value || ddProvider.value || ddOrigin.value ||
      ddFeature.value || ddPair.value || (grouped() && ddCategory.value) || sinceEl.value || untilEl.value || reasonEl.value.trim() || runEl.value.trim());

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
      if (reasonEl.value.trim()) p.set("reason_code", reasonEl.value.trim());
      if (runEl.value.trim()) p.set("run_id", runEl.value.trim());
      const s = dateToEpoch(sinceEl.value, false);
      const u = dateToEpoch(untilEl.value, true);
      if (s) p.set("since", s);
      if (u) p.set("until", u);
      p.set("visibility", visibility);
      p.set("order", order);
      p.set("limit", String(PAGE_SIZE));
      p.set("offset", String((page || 0) * PAGE_SIZE));
      return p.toString();
    };

    const pageCount = () => Math.max(1, Math.ceil((state.total || 0) / PAGE_SIZE));

    // ---- rows --------------------------------------------------------------
    const groupRowHTML = (g) => {
      const sv = groupSev(g);
      const feat = FEATURE_LABEL[g.feature] || g.feature || "";
      const route = routeOf(g);
      const meta = [feat, route, `${g.event_count || 1} event${g.event_count === 1 ? "" : "s"}`].filter(Boolean).join(" · ");
      const item = titleOf(g);
      const primary = item || g.summary || "";
      const showSummary = item && g.summary && g.summary !== item;
      return `
      <div class="ev-row${g.acknowledged_at ? " acked" : ""}" data-id="${g.id}">
        <span class="ev-dot ${sv}"></span>
        <span class="ev-ic ${sv}"><span class="material-symbols-rounded" aria-hidden="true">${groupIcon(g)}</span></span>
        <span class="ev-line">
          <span class="ev-primary"><span class="ev-badge ${sv}">${esc(statusLabel(g.status))}</span><span class="ev-ptext">${esc(primary)}</span></span>
          ${showSummary ? `<span class="ev-summary">${esc(g.summary)}</span>` : ""}
          <span class="ev-meta">${esc(meta)}</span>
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
        <span class="ev-dot ${sv}"></span>
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
    const rowHTML = (x) => grouped() ? groupRowHTML(x) : eventRowHTML(x);

    const bindRow = (el) => {
      el.addEventListener("click", (ev) => {
        if (ev.target.closest(".ev-rowact")) return;
        select(el.dataset.id, el);
      });
      el.querySelector(".ev-ack")?.addEventListener("click", (ev) => {
        ev.stopPropagation();
        const item = state.items.find((x) => String(x.id) === String(el.dataset.id));
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
        hint = "Run a sync to generate events, or rebuild the archive from runtime state.";
        actions = `<button class="ev-linkbtn" id="ev-empty-rebuild">Rebuild archive</button>`;
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
        Q("#ev-empty-rebuild", listEl)?.addEventListener("click", rebuildArchive);
        return;
      }
      listEl.innerHTML = state.items.map(rowHTML).join("");
      listEl.querySelectorAll(".ev-row").forEach(bindRow);
      if (state.selected) listEl.querySelector(`.ev-row[data-id="${state.selected}"]`)?.classList.add("sel");
      renderPager();
    };

    const load = async (page) => {
      if (page != null) state.page = page;
      const url = grouped() ? `/api/events/groups?${buildQuery(state.page)}` : `/api/events/search?view=events&${buildQuery(state.page)}`;
      try {
        const data = await fjson(url);
        if (!alive) return;
        state.items = data.items || [];
        state.total = data.total || 0;
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

    const acknowledgeRow = async (id) => {
      let res;
      try { res = await fjson(ackURL(id, "acknowledge"), { method: "POST" }); } catch { return; }
      if (!res?.ok) return;
      const idx = state.items.findIndex((x) => String(x.id) === String(id));
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
      cards.push(card("Blackbox state", pill(bPresent ? "Yes" : "No", bPresent ? "warn" : "neutral"), [bPresent ? "Blackboxed" : "Not blackboxed"]));

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

    // ---- group detail ------------------------------------------------------
    const renderGroupDetail = (detail) => {
      const g = detail.group || {};
      const c = detail.context || {};
      const events = detail.events || [];
      const related = detail.related_groups || [];
      const sv = groupSev(g);
      const ps = c.pair_state;
      const routeShort = provShort(g.source_provider, g.destination_provider) || "–";
      const pairLabel = (ps && ps.matched && ps.label) ? `${ps.label}${ps.mode ? ` (${modeLabel(ps.mode).toLowerCase()})` : ""}` : (g.pair_key || "–");
      const origin = g.origin_provider ? instLabel(g.origin_provider, g.origin_instance) : "unknown";
      const dest = instLabel(g.destination_provider, g.destination_instance) || "–";
      const item = titleOf(g);

      const kv = [
        ["Status", `<span class="ev-pill ${sv}">${esc(statusLabel(g.status))}</span>`],
        ["Feature", esc(FEATURE_LABEL[g.feature] || g.feature || "–")],
        ["Operation", esc(g.operation || "–")],
        ["Route", esc(routeShort)],
        ["Origin", esc(origin)],
        ["Destination", esc(dest)],
        ["Pair", esc(pairLabel)],
        ["Item", g.item_key ? `<span class="mono">${esc(g.item_key)}</span>` : "–"],
        ["Events", esc(String(g.event_count || events.length))],
        ["First seen", esc(TS(g.first_event_at))],
        ["Last seen", esc(TS(g.last_event_at))],
        ["Reason", esc(g.reason || g.reason_code || "–")],
      ].map(([k, v]) => `<div class="k">${esc(k)}</div><div class="v">${v}</div>`).join("");

      const timeline = events.map((e) => `
        <div class="ev-tl" data-id="${e.id}">
          <span class="ev-tl-time">${esc(TS(e.created_at))}</span>
          <span class="ev-tl-dot ${sevOf(e)}"></span>
          <span class="ev-tl-body">
            <span class="ev-tl-head"><span class="ev-badge ${sevOf(e)}">${esc(badgeOf(e))}</span><span class="ev-tl-title">${esc(titleLine(e))}</span></span>
            ${e.reason && e.reason !== e.reason_code ? `<span class="ev-tl-reason">${esc(e.reason)}</span>` : ""}
          </span>
        </div>`).join("");

      const relHTML = related.length
        ? related.slice(0, 12).map((r) => `
          <button class="ev-rel" type="button" data-gid="${r.id}">
            <span class="ev-rel-ic ${groupSev(r)}"><span class="material-symbols-rounded" aria-hidden="true">${groupIcon(r)}</span></span>
            <span class="ev-rel-badge ${groupSev(r)}">${esc(statusLabel(r.status))}</span>
            <span class="ev-rel-title">${esc(r.summary || titleOf(r) || "")}</span>
            <span class="ev-rel-time">${esc(TS(r.last_event_at))}</span>
          </button>`).join("")
        : `<div class="ev-empty ev-empty-inline">No related groups found.</div>`;

      detailEl.innerHTML = `
        <div class="ev-dhead">
          <div class="ev-dhead-row">
            <span class="ev-badge ${sv}">${esc(statusLabel(g.status))}</span>
            <span class="ev-dhead-spacer"></span>
            <span class="ev-dtime">${esc(TS(g.last_event_at))}</span>
            <button class="ev-icon-btn" id="ev-copy" type="button" title="Copy thread" aria-label="Copy thread"><span class="material-symbols-rounded" aria-hidden="true">content_copy</span></button>
          </div>
          <div class="ev-dtitle">${esc(g.summary || item || "")}</div>
          ${item ? `<div class="ev-ditem">${esc(item)}</div>` : ""}
        </div>
        <h4>Overview</h4><div class="ev-kv">${kv}</div>
        <h4>Current context <span class="ev-h4-note">live now — may differ from the timeline</span></h4>
        ${renderCards(c, g)}
        <h4>Thread timeline <span class="ev-h4-note">${events.length} event${events.length === 1 ? "" : "s"}, in order</span></h4>
        <div class="ev-timeline">${timeline || `<div class="ev-empty ev-empty-inline">No events in this thread.</div>`}</div>
        <h4>Related groups</h4>
        <div class="ev-related">${relHTML}</div>
        <details class="ev-tech"><summary>Technical details</summary><div class="ev-tech-body"><div class="ev-tech-kv"><span>Group hash</span><span class="mono">${esc(g.group_hash || "–")}</span></div><div class="ev-tech-kv"><span>Pair key</span><span class="mono">${esc(g.pair_key || "–")}</span></div><pre class="cw-scrollbars">${esc(JSON.stringify({ group: g, context: c }, null, 2))}</pre></div></details>`;
      detailEl.scrollTop = 0;

      Q("#ev-copy", detailEl)?.addEventListener("click", () => {
        const summary = [
          `Thread: ${g.summary || ""}`, `Status: ${g.status}`,
          `Feature: ${g.feature || ""}`, `Route: ${routeShort}`, `Pair: ${pairLabel}`,
          `Item: ${g.item_key || ""}`, `Events: ${g.event_count}`,
          `First seen: ${TS(g.first_event_at)}`, `Last seen: ${TS(g.last_event_at)}`,
          `Reason: ${g.reason || g.reason_code || ""}`,
          "",
          ...events.map((e) => `  ${TS(e.created_at)} · ${badgeOf(e)} · ${titleLine(e)}`),
        ].join("\n");
        navigator.clipboard?.writeText(summary);
        const b = Q("#ev-copy", detailEl);
        if (b) { const ic = b.querySelector(".material-symbols-rounded"); if (ic) { ic.textContent = "check"; setTimeout(() => { ic.textContent = "content_copy"; }, 1200); } }
      });
      Q("#ev-open-analyzer", detailEl)?.addEventListener("click", () => { try { window.cxCloseModal?.(); window.openAnalyzer?.(); } catch {} });
      detailEl.querySelectorAll(".ev-rel").forEach((a) => a.addEventListener("click", () => a.dataset.gid && select(a.dataset.gid)));
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
          const detail = await fjson(`/api/events/groups/${encodeURIComponent(id)}`);
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
      sinceEl.value = ""; untilEl.value = "";
      reasonEl.value = ""; runEl.value = "";
      load(0);
    };

    const rebuildArchive = async () => {
      const ok = window.confirm(
        "Rebuild the events archive?\n\n" +
        "This clears the SQLite event archive and rebuilds it from current runtime state (sync reports and provider state). " +
        "Acknowledgements will be reset.\n\n" +
        "This does not change any sync behavior and cannot be undone."
      );
      if (!ok) return;
      const btn = Q("#ev-rebuild", root);
      btn?.classList.add("busy");
      try {
        await fjson("/api/maintenance/events-rebuild", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ confirm: true }) });
        await populateFilters();
        clearDetail();
        await load(0);
      } catch {} finally { btn?.classList.remove("busy"); }
    };

    let refreshing = false;
    const doRefresh = async (force) => {
      if (refreshing) return;
      const now = Date.now();
      if (!force && now - lastAutoRefresh < 8000) { await load(state.page); return; }
      refreshing = true;
      lastAutoRefresh = now;
      const btn = Q("#ev-refresh", root);
      btn?.classList.add("busy");
      let warn = false;
      try {
        try { await fjson("/api/events/import", { method: "POST" }); } catch { warn = true; }
        if (!alive) return;
        await populateFilters();
        if (!alive) return;
        await load(state.page);
      } finally { refreshing = false; btn?.classList.remove("busy"); }
      if (alive && warn) showToast("Couldn't fetch latest — showing archived events.");
    };

    let searchTimer = null;
    qEl.addEventListener("input", () => { clearTimeout(searchTimer); searchTimer = setTimeout(() => load(0), 250); });
    let filterTimer = null;
    const deb = () => { clearTimeout(filterTimer); filterTimer = setTimeout(() => load(0), 250); };
    reasonEl.addEventListener("input", deb);
    runEl.addEventListener("input", deb);
    sinceEl.addEventListener("change", () => load(0));
    untilEl.addEventListener("change", () => load(0));

    Q("#ev-more", root).addEventListener("click", (ev) => {
      const btn = ev.currentTarget;
      const open = moreWrap.hasAttribute("hidden");
      if (open) moreWrap.removeAttribute("hidden"); else moreWrap.setAttribute("hidden", "");
      btn.setAttribute("aria-expanded", String(open));
      btn.classList.toggle("on", open);
    });
    Q("#ev-refresh", root).addEventListener("click", () => doRefresh(true));
    Q("#ev-prev-top", root).addEventListener("click", () => { if (state.page > 0) load(state.page - 1); });
    Q("#ev-next-top", root).addEventListener("click", () => { if (state.page < pageCount() - 1) load(state.page + 1); });
    Q("#ev-close", root).addEventListener("click", () => { window.cxCloseModal?.(); });

    cleanup = () => {
      alive = false;
      clearTimeout(searchTimer);
      clearTimeout(filterTimer);
      clearTimeout(toastTimer);
      window.removeEventListener("pointermove", onSplitMove, true);
      window.removeEventListener("pointerup", endSplit, true);
      document.body.style.userSelect = "";
      for (const dd of [...dropdowns, ddSort]) dd.close();
      cleanup = null;
    };

    await populateFilters();
    await load(0);
    doRefresh(false);
  },
  unmount() { cleanup?.(); },
};
