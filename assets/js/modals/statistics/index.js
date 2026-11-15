/* assets/js/modals/statistics/index.js */
/* CrossWatch - Sync activity modal */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import {statisticsReturn} from '../../page-return.js';

const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => (
  { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
));

const Q = (s, r = document) => r.querySelector(s);
const nf = new Intl.NumberFormat();
const fmt = (n) => nf.format(Math.round(Number(n || 0)));

const TZ = -new Date().getTimezoneOffset() * 60;
const DAY = 86400;
const dayIndexOf = (ms) => Math.floor((ms / 1000 + TZ) / DAY);
const dateOfIndex = (d) => new Date((d * DAY - TZ) * 1000);

const RANGES = [
  { value: "3m", label: "3M", days: 98, weeks: 14 },
  { value: "6m", label: "6M", days: 189, weeks: 27 },
  { value: "12m", label: "1Y", days: 371, weeks: 53 },
];
const METRICS = [
  { value: "changes", label: "Changes", icon: "swap_horiz" },
  { value: "runs", label: "Runs", icon: "sync" },
  { value: "failed", label: "Failures", icon: "error" },
];
const WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

const PAD_T = 20;

const ls = (k, d) => { try { return localStorage.getItem(k) || d; } catch { return d; } };
const lset = (k, v) => { try { localStorage.setItem(k, v); } catch {} };

const fmtDur = (s) => {
  const n = Number(s || 0);
  if (!Number.isFinite(n) || n <= 0) return "";
  if (n < 60) return `${n}s`;
  if (n < 3600) return `${Math.floor(n / 60)}m ${n % 60}s`;
  return `${(n / 3600).toFixed(1)}h`;
};

const fmtTime = (epoch) => {
  const n = Number(epoch || 0);
  if (!n) return "--:--";
  return new Date(n * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
};

const fmtDate = (d) => dateOfIndex(d).toLocaleDateString([], { weekday: "long", day: "numeric", month: "long", year: "numeric" });

const brand = (name) => {
  try { return window.CW?.ProviderMeta?.brandInfo?.(name) || null; } catch { return null; }
};

function providerChip(name) {
  const key = String(name || "").trim().toUpperCase();
  if (!key) return "";
  const info = brand(key);
  const label = info?.label || key;
  const rgb = info?.tone?.rgb || "255,255,255";
  const src = info?.icon || "";
  const square = /\.png(?:[?#]|$)/i.test(src);
  const media = src
    ? `<img class="sc-prov-img${square ? " sq" : ""}" src="${esc(src)}" alt="" loading="lazy">`
    : `<span class="sc-prov-abbr">${esc((info?.shortLabel || key).slice(0, 2))}</span>`;
  return `<span class="sc-prov" style="--sc-prov-rgb:${esc(rgb)}">${media}<span class="sc-prov-name">${esc(label)}</span></span>`;
}

async function fjson(url, ms = 20000) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort("timeout"), ms);
  try {
    const res = await fetch(url, { signal: ctrl.signal, cache: "no-store", credentials: "same-origin" });
    if (!res.ok) throw new Error(String(res.status));
    return await res.json();
  } finally { clearTimeout(timer); }
}



function levels(values) {
  const nonZero = values.filter((v) => v > 0).sort((a, b) => a - b);
  if (!nonZero.length) return [1, 2, 3, 4];
  const at = (p) => nonZero[Math.min(nonZero.length - 1, Math.floor(p * (nonZero.length - 1)))];
  const raw = [at(0.25), at(0.5), at(0.75), at(0.92)];
  const out = [];
  let prev = 0;
  for (const v of raw) {
    const next = Math.max(prev + 1, Math.round(v));
    out.push(next);
    prev = next;
  }
  return out;
}

function levelOf(value, steps) {
  if (value <= 0) return 0;
  for (let i = 0; i < steps.length; i += 1) if (value <= steps[i]) return i + 1;
  return 4;
}

function streaks(byIndex, firstIndex, lastIndex) {
  let best = 0;
  let current = 0;
  let running = 0;
  for (let d = firstIndex; d <= lastIndex; d += 1) {
    if ((byIndex.get(d)?.runs || 0) > 0) {
      running += 1;
      if (running > best) best = running;
    } else running = 0;
  }
  for (let d = lastIndex; d >= firstIndex; d -= 1) {
    if ((byIndex.get(d)?.runs || 0) > 0) current += 1;
    else break;
  }
  return { best, current };
}

export default {
  async mount(root, props = {}) {

    const shell = root.closest(".cx-modal-shell");
    if (shell) {
      shell.classList.add("cw-page-dialog", "cw-page-statistics");
    }

    let alive = true;
    const narrowViewport = matchMedia("(max-width:600px)");
    let restored = statisticsReturn(props);
    let range = restored?.range || (RANGES.some((r) => r.value === ls("cw.stats.range", "")) ? ls("cw.stats.range", "12m") : (narrowViewport.matches ? "3m" : "12m"));
    let metric = restored?.metric || (METRICS.some((m) => m.value === ls("cw.stats.metric", "")) ? ls("cw.stats.metric", "changes") : "changes");
    let instanceLabels = {};
    const instanceName = (provider, id, explicit) => {
      const key = String(provider || "").toUpperCase();
      return instanceLabels[key]?.[id] || explicit || (id === "default" || !id ? "Default" : id);
    };
    const loadLabels = async () => {
      try {
        const data = await fjson("/api/provider-instances?configured_only=true");
        instanceLabels = Object.fromEntries(Object.entries(data || {}).filter(([, rows]) => Array.isArray(rows)).map(([provider, rows]) => [provider.toUpperCase(), Object.fromEntries(rows.map(row => [row.id, row.friendly_label || row.display_label || row.label || row.id]))]));
      } catch { instanceLabels = {}; }
    };
    let payload = null;
    let byIndex = new Map();
    let selected = Number(props?.day) || null;
    let pendingDay = selected;
    let dayState = { index: null, runs: [], loading: false, error: "", filter: "", expanded: new Set() };

    root.innerHTML = `
      <div class="cw-sc cw-page-preferences">
        <header class="pf-head"><div><h2>Sync activity</h2><p id="sc-sub">Every sync run, day by day.</p></div><div class="pf-actions"><button type="button" class="pf-button" id="sc-refresh" aria-label="Refresh activity"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button><button type="button" class="pf-button" id="sc-close">Close</button></div></header>
        <div class="pf-body sc-body">
          <section class="sc-cal-card">
            <div class="sc-controls"><label>Show<select id="sc-metric" class="cw-page-input" data-cw-native-select="true" aria-label="Activity metric"></select></label><label>Period<select id="sc-range" class="cw-page-input" data-cw-native-select="true" aria-label="Activity period"></select></label></div>
            <div class="sc-cal-layout"><div class="sc-weekdays" aria-hidden="true">${WEEKDAYS.map(day=>`<span>${day}</span>`).join("")}</div><div class="sc-cal-scroll" tabindex="0" aria-label="Activity calendar, scroll to see more weeks"><div id="sc-cal" class="sc-cal"></div></div></div>
            <div class="sc-cal-foot"><span>Choose a day to see its runs.</span><span class="sc-legend"><span>Less</span><i class="lv0"></i><i class="lv1"></i><i class="lv2"></i><i class="lv3"></i><i class="lv4"></i><span>More</span></span></div>
          </section>
          <details class="sc-summary"><summary id="sc-summary">Activity summary</summary><div class="sc-tiles" id="sc-tiles"></div></details>
          <section class="sc-day-card" id="sc-day"></section>
        </div>
      </div>`;

    const tip = document.createElement("div");
    tip.className = "sc-tip";
    tip.hidden = true;
    document.body.appendChild(tip);

    const showTip = (html, x, y) => {
      tip.innerHTML = html;
      tip.hidden = false;
      const r = tip.getBoundingClientRect();
      tip.style.left = `${Math.max(8, Math.min(x - r.width / 2, window.innerWidth - r.width - 8))}px`;
      tip.style.top = `${Math.max(8, y - r.height - 12)}px`;
    };
    const hideTip = () => { tip.hidden = true; };

    const renderSegs = () => {
      Q("#sc-metric", root).innerHTML = METRICS.map(m => `<option value="${m.value}" ${m.value === metric ? "selected" : ""}>${esc(m.label)}</option>`).join("");
      Q("#sc-range", root).innerHTML = RANGES.map(r => `<option value="${r.value}" ${r.value === range ? "selected" : ""}>${({"3m":"3 months","6m":"6 months","12m":"1 year"})[r.value]}</option>`).join("");
    };
    Q("#sc-metric", root).addEventListener("change", ev => { metric = ev.target.value; lset("cw.stats.metric", metric); renderCalendar(); renderTiles(); });
    Q("#sc-range", root).addEventListener("change", ev => { range = ev.target.value; lset("cw.stats.range", range); void load(); });

    const metricValue = (entry) => {
      if (!entry) return 0;
      if (metric === "runs") return entry.runs || 0;
      if (metric === "failed") return entry.failed || 0;
      return entry.changes || 0;
    };

    const renderTiles = () => {
      const host = Q("#sc-tiles", root);
      const totals = payload?.totals || {};
      Q("#sc-summary", root).textContent = `${fmt(totals.runs)} runs · ${fmt(totals.changes)} changes · ${fmt(totals.failed)} failed`;
      const cfg = RANGES.find((r) => r.value === range) || RANGES[2];
      const todayIdx = dayIndexOf(Date.now());
      const firstIdx = todayIdx - cfg.days + 1;
      const st = streaks(byIndex, firstIdx, todayIdx);
      const tiles = [
        { k: "Sync runs", v: fmt(totals.runs), s: `${fmt(totals.failed || 0)} with errors` },
        { k: "Changes", v: fmt(totals.changes), s: `+${fmt(totals.added)} / -${fmt(totals.removed)} / ~${fmt(totals.updated)}` },
        { k: "Active days", v: `${fmt(totals.active_days)}`, s: `of ${fmt(cfg.days)} days` },
        { k: "Current streak", v: `${fmt(st.current)}`, s: st.current === 1 ? "day" : "days" },
        { k: "Longest streak", v: `${fmt(st.best)}`, s: st.best === 1 ? "day" : "days" },
      ];
      host.innerHTML = tiles.map((t) => `<div class="sc-tile"><div class="sc-tile-k">${esc(t.k)}</div><div class="sc-tile-v">${t.v}</div><div class="sc-tile-s">${esc(t.s)}</div></div>`).join("");
    };

    const renderCalendar = () => {
      const host = Q("#sc-cal", root);
      const cfg = RANGES.find((r) => r.value === range) || RANGES[2];
      const todayIdx = dayIndexOf(Date.now());
      const todayDow = (dateOfIndex(todayIdx).getDay() + 6) % 7;
      const lastCol = todayIdx - todayDow;
      const firstCol = lastCol - (cfg.weeks - 1) * 7;

      const narrow = narrowViewport.matches;
      const PAD_L = narrow ? 0 : 32;
      const CELL = narrow ? 22 : 15;
      const STEP = narrow ? 28 : 20;
      const steps = levels([...byIndex.values()].map(metricValue));
      const width = PAD_L + cfg.weeks * STEP + 6;
      const height = PAD_T + 7 * STEP + 4;

      const cells = [];
      const monthLabels = [];
      let lastMonth = -1;
      for (let w = 0; w < cfg.weeks; w += 1) {
        const colStart = firstCol + w * 7;
        const colDate = dateOfIndex(colStart);
        if (colDate.getMonth() !== lastMonth) {
          lastMonth = colDate.getMonth();
          monthLabels.push(`<text class="sc-ax" x="${PAD_L + w * STEP}" y="12">${MONTHS[lastMonth]}</text>`);
        }
        for (let d = 0; d < 7; d += 1) {
          const index = colStart + d;
          if (index > todayIdx) continue;
          const entry = byIndex.get(index);
          const value = metricValue(entry);
          const lv = levelOf(value, steps);
          const bad = (entry?.failed || 0) > 0;
          const cls = `sc-cell lv${lv}${bad ? " bad" : ""}${index === selected ? " on" : ""}`;
          cells.push(`<rect class="${cls}" x="${PAD_L + w * STEP}" y="${PAD_T + d * STEP}" width="${CELL}" height="${CELL}" rx="2" data-d="${index}" role="button" tabindex="${index === (selected ?? todayIdx) ? 0 : -1}" aria-label="${esc(fmtDate(index))}: ${fmt(entry?.runs)} runs, ${fmt(value)} ${esc(METRICS.find(m => m.value === metric)?.label.toLowerCase())}${bad ? ", includes failures" : ""}" aria-pressed="${index === selected}"></rect>`);
        }
      }
      const dayAxis = (narrow ? [] : [0, 2, 4]).map((d) => `<text class="sc-ax" x="0" y="${PAD_T + d * STEP + 9}">${WEEKDAYS[d]}</text>`).join("");

      host.innerHTML = `<svg class="sc-svg" viewBox="0 0 ${width} ${height}" width="${width}" height="${height}" role="group" aria-label="Sync activity calendar">${monthLabels.join("")}${dayAxis}${cells.join("")}</svg>`;

      host.querySelectorAll(".sc-cell").forEach((cell) => {
        const index = Number(cell.dataset.d);
        cell.addEventListener("mousemove", (ev) => {
          const entry = byIndex.get(index);
          const runs = entry?.runs || 0;
          const parts = [];
          if (entry?.added) parts.push(`+${fmt(entry.added)} added`);
          if (entry?.removed) parts.push(`-${fmt(entry.removed)} removed`);
          if (entry?.updated) parts.push(`~${fmt(entry.updated)} updated`);
          if (entry?.failed) parts.push(`${fmt(entry.failed)} failed`);
          const body = runs
            ? `<b>${fmt(runs)}</b> ${runs === 1 ? "run" : "runs"} &middot; <b>${fmt(entry.changes)}</b> ${entry.changes === 1 ? "change" : "changes"}${parts.length ? `<br><span class="sc-tip-sub">${parts.join(" &middot; ")}</span>` : ""}`
            : `<span class="sc-tip-sub">No sync runs</span>`;
          showTip(`<div class="sc-tip-d">${esc(fmtDate(index))}</div>${body}`, ev.clientX, cell.getBoundingClientRect().top);
        });
        cell.addEventListener("mouseleave", hideTip);
        cell.addEventListener("click", () => { hideTip(); selectDay(index); });
        cell.addEventListener("keydown", ev => {
          if (["Enter", " "].includes(ev.key)) { ev.preventDefault(); hideTip(); void selectDay(index); return; }
          const delta = {ArrowLeft:-7, ArrowRight:7, ArrowUp:-1, ArrowDown:1}[ev.key];
          if (!delta) return;
          const target = host.querySelector(`[data-d="${index + delta}"]`);
          if (target) { ev.preventDefault(); cell.setAttribute("tabindex", "-1"); target.setAttribute("tabindex", "0"); target.focus(); }
        });
      });
    };

    const runMatches = (run, needle) => {
      if (!needle) return true;
      const bits = [
        fmtTime(run.started_at), run.status, run.dry_run ? "dry run" : "",
        ...(run.pair_rows || []).flatMap((p) => [p.pair_key, p.feature, p.src_provider, p.dst_provider, p.src_instance, p.dst_instance, instanceName(p.src_provider,p.src_instance,p.src_instance_label), instanceName(p.dst_provider,p.dst_instance,p.dst_instance_label)]),
      ];
      return bits.join(" ").toLowerCase().includes(needle);
    };

    const pairChips = (run) => {
      const seen = new Map();
      for (const row of run.pair_rows || []) {
        const key = `${row.src_provider}>${row.dst_provider}`;
        const entry = seen.get(key) || { src: row.src_provider, dst: row.dst_provider, features: new Set() };
        if (row.feature) entry.features.add(row.feature);
        seen.set(key, entry);
      }
      if (!seen.size) return `<span class="sc-run-nopair">no changes</span>`;
      return [...seen.values()].map((e) => `<span class="sc-pair" title="${esc([...e.features].join(", "))}">${providerChip(e.src)}<span class="material-symbols-rounded sc-pair-arrow" aria-hidden="true">arrow_forward</span>${providerChip(e.dst)}</span>`).join("");
    };

    const featureChips = (run) => {
      const feats = [...new Set((run.pair_rows || []).map((p) => String(p.feature || "").trim()).filter(Boolean))];
      return feats.map((f) => `<span class="sc-chip">${esc(f)}</span>`).join("");
    };

    const counters = (row) => {
      const bits = [];
      if (row.added) bits.push(`<span class="sc-n add">+${fmt(row.added)}</span>`);
      if (row.removed) bits.push(`<span class="sc-n del">-${fmt(row.removed)}</span>`);
      if (row.updated) bits.push(`<span class="sc-n upd">~${fmt(row.updated)}</span>`);
      if (row.errors) bits.push(`<span class="sc-n err">${fmt(row.errors)} err</span>`);
      return bits.join("") || `<span class="sc-n zero">0</span>`;
    };

    const renderRuns = () => {
      const host = Q("#sc-run-host", root);
      if (!host) return;
      if (dayState.loading) { host.innerHTML = `<div class="sc-day-msg">Loading runs...</div>`; return; }
      if (dayState.error) { host.innerHTML = `<div class="sc-day-msg sc-day-msg--err">${esc(dayState.error)}</div>`; return; }

      const needle = dayState.filter.trim().toLowerCase();
      const rows = dayState.runs.filter((run) => runMatches(run, needle));
      if (!rows.length) {
        host.innerHTML = `<div class="sc-day-msg">${dayState.runs.length ? "No runs match that filter." : "No sync runs on this day."}</div>`;
        return;
      }

      host.innerHTML = `<div class="sc-runs">${rows.map((run) => {
        const open = dayState.expanded.has(run.run_id);
        const status = run.errors ? "err" : (run.status === "running" ? "run" : "ok");
        const detail = open
          ? `<div class="sc-run-detail">${(run.pair_rows || []).length
              ? `<div class="sc-route-list">${run.pair_rows.map(p => `<div class="sc-route-row"><div><span>${esc(brand(p.src_provider)?.label || p.src_provider)} &middot; ${esc(instanceName(p.src_provider,p.src_instance,p.src_instance_label))}</span><span class="sc-inst">to ${esc(brand(p.dst_provider)?.label || p.dst_provider)} &middot; ${esc(instanceName(p.dst_provider,p.dst_instance,p.dst_instance_label))}</span></div><span class="sc-chip">${esc(p.feature || "-")}</span><span class="sc-run-counts">${counters(p)}</span></div>`).join("")}</div>`
              : `<div class="sc-day-msg">No recorded changes for this run.</div>`}</div>`
          : "";
        return `
          <div class="sc-run${open ? " open" : ""}" data-run="${esc(run.run_id)}">
            <div class="sc-run-bar">
            <button type="button" class="sc-run-head" aria-expanded="${open}">
              <span class="sc-run-time">${esc(fmtTime(run.started_at))}</span>
              <span class="sc-dot ${status}" aria-hidden="true"></span>
              <span class="sc-run-pairs">${pairChips(run)}</span>
              <span class="sc-run-chips">${featureChips(run)}${run.dry_run ? `<span class="sc-chip dry">dry run</span>` : ""}</span>
              <span class="sc-run-counts">${counters(run)}</span>
              <span class="sc-run-dur">${esc(fmtDur(run.duration))}</span>
              <span class="material-symbols-rounded sc-run-caret" aria-hidden="true">expand_more</span>
            </button>
            <button type="button" class="sc-run-events" title="Open events for this run" aria-label="Open events for run ${esc(run.run_id)}">
              <span class="material-symbols-rounded" aria-hidden="true">manage_search</span>
            </button>
            </div>
            ${detail}
          </div>`;
      }).join("")}</div>`;

      host.querySelectorAll(".sc-run-head").forEach((btn) => btn.addEventListener("click", () => {
        const id = btn.closest(".sc-run")?.dataset?.run;
        if (!id) return;
        if (dayState.expanded.has(id)) dayState.expanded.delete(id);
        else dayState.expanded.add(id);
        renderRuns();
      }));
      host.querySelectorAll(".sc-run-events").forEach((btn) => btn.addEventListener("click", (ev) => {
        ev.stopPropagation();
        const id = btn.closest(".sc-run")?.dataset?.run;
        const openEvents = window.openEvents;
        if (!id || typeof openEvents !== "function") return;
        openEvents({ runId: id, domain: "sync", visibility: "all", mode: "grouped", returnContext:{modal:'statistics',day:selected,range,metric,filter:dayState.filter,expanded:[...dayState.expanded]} });
      }));
    };

    const renderDay = () => {
      const host = Q("#sc-day", root);
      if (dayState.index == null) {
        host.innerHTML = `<div class="sc-day-empty"><span class="material-symbols-rounded" aria-hidden="true">touch_app</span><div><strong>No day selected</strong><span>Choose a square above to list that day's sync runs.</span></div></div>`;
        return;
      }
      const entry = byIndex.get(dayState.index);
      const live = !dayState.loading && !dayState.error;
      const runCount = live ? dayState.runs.length : (entry?.runs || 0);
      const changeCount = live ? dayState.runs.reduce((n, r) => n + (r.changes || 0), 0) : (entry?.changes || 0);
      const failCount = live ? dayState.runs.filter((r) => r.errors).length : (entry?.failed || 0);
      host.innerHTML = `
        <div class="sc-day-head">
          <div>
            <div class="sc-day-title">${esc(fmtDate(dayState.index))}</div>
            <div class="sc-day-sub">${fmt(runCount)} runs &middot; ${fmt(changeCount)} changes${failCount ? ` &middot; ${fmt(failCount)} with errors` : ""}</div>
          </div>
          <div class="sc-day-tools" ${dayState.runs.length > 5 || dayState.filter ? "" : "hidden"}>
            <label class="sc-search"><span class="material-symbols-rounded" aria-hidden="true">search</span><input type="search" id="sc-filter" aria-label="Filter runs" placeholder="Filter runs" value="${esc(dayState.filter)}" autocomplete="off"></label>
          </div>
        </div>
        <div id="sc-run-host"></div>`;

      Q("#sc-filter", host)?.addEventListener("input", (ev) => {
        dayState.filter = ev.target.value;
        renderRuns();
      });
      renderRuns();
    };

    const selectDay = async (index) => {
      const keyboardFocus = document.activeElement?.classList?.contains("sc-cell");
      selected = index;
      dayState = { index, runs: [], loading: true, error: "", filter: "", expanded: new Set() };
      if (restored?.day === index) {
        dayState.filter = restored.filter;
        dayState.expanded = new Set(restored.expanded);
      }
      restored = null;
      renderCalendar();
      if (keyboardFocus) Q(`.sc-cell[data-d="${index}"]`, root)?.focus();
      renderDay();
      const since = index * DAY - TZ;
      try {
        const data = await fjson(`/api/events/calendar/day?since=${since}&until=${since + DAY}&limit=300`);
        if (!alive || dayState.index !== index) return;
        if (!data?.ok) throw new Error(data?.error || "failed");
        dayState.runs = Array.isArray(data.runs) ? data.runs : [];
      } catch (err) {
        if (!alive || dayState.index !== index) return;
        dayState.error = `Could not load runs (${err.message}).`;
      } finally {
        if (alive && dayState.index === index) {
          dayState.loading = false;
          renderDay();
        }
      }
    };

    const load = async () => {
      const cfg = RANGES.find((r) => r.value === range) || RANGES[2];
      const btn = Q("#sc-refresh", root);
      btn?.classList.add("busy");
      try {
        const data = await fjson(`/api/events/calendar?days=${cfg.days}&tz=${TZ}`);
        if (!alive) return;
        if (!data?.ok) throw new Error(data?.error || "unavailable");
        payload = data;
        byIndex = new Map((data.days || []).map((row) => [Number(row.d), row]));
        Q("#sc-sub", root).textContent = data.scoped ? "Runs visible to the selected profile." : "Every sync run, day by day.";
      } catch (err) {
        if (!alive) return;
        payload = { totals: {} };
        byIndex = new Map();
        Q("#sc-sub", root).textContent = `Could not load activity (${err.message}).`;
      } finally {
        btn?.classList.remove("busy");
      }
      if (!alive) return;
      renderTiles();
      renderCalendar();
      const scroll = Q(".sc-cal-scroll", root);
      scroll.scrollLeft = scroll.scrollWidth;

      if (pendingDay != null) {
        const want = pendingDay;
        pendingDay = null;
        await selectDay(want);
        return;
      }
      const cal = RANGES.find((r) => r.value === range) || RANGES[2];
      const todayIdx = dayIndexOf(Date.now());
      const firstIdx = todayIdx - cal.days + 1;
      const inRange = dayState.index != null && dayState.index >= firstIdx && dayState.index <= todayIdx;
      const days = payload?.days || [];
      const latest = days.length ? Number(days[days.length - 1].d) : null;
      if (!inRange && latest != null) {
        await selectDay(latest);
        return;
      }
      renderDay();
    };

    const onProfileChange = async () => { selected = null; pendingDay = null; dayState = { index: null, runs: [], loading: false, error: "", filter: "", expanded: new Set() }; await Promise.all([loadLabels(),load()]); if (alive) renderRuns(); };
    const onViewportChange = () => {
      renderCalendar();
      const scroll = Q(".sc-cal-scroll", root);
      scroll.scrollLeft = scroll.scrollWidth;
    };
    narrowViewport.addEventListener("change", onViewportChange);
    window.addEventListener("cw:overview-profile-changed", onProfileChange);

    Q("#sc-close", root).addEventListener("click", () => window.cxCloseModal?.());
    Q("#sc-refresh", root).addEventListener("click", () => load());

    renderSegs();
    renderDay();
    this._cleanup = () => {
      alive = false;
      narrowViewport.removeEventListener("change", onViewportChange);
      window.removeEventListener("cw:overview-profile-changed", onProfileChange);
      tip.remove();
    };
    await Promise.all([loadLabels(), load()]);
    if (alive) renderRuns();
  },
  unmount() {
    try { this._cleanup?.(); } catch {}
    this._cleanup = null;
  },
};
