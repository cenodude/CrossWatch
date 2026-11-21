/* assets/js/events/titles.js */
/* CrossWatch - Events titles, sync activity and runs views */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import {renderItemOrigin} from '../editor/origin.js';

const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => (
  { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
));
const nf = new Intl.NumberFormat();
const fmt = (n) => nf.format(Math.round(Number(n || 0)));
const DAY = 86400;
const TZ = -new Date().getTimezoneOffset() * 60;
const dayIndexOf = (sec) => Math.floor((Number(sec || 0) + TZ) / DAY);
const dateOfIndex = (d) => new Date((d * DAY - TZ) * 1000);
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

export const STATE_LABEL = { ok: "Done", fail: "Failed", skip: "Skipped", same: "Already there", pending: "In progress" };
const STATE_ICON = { ok: "check", fail: "close", skip: "remove", same: "radio_button_unchecked", pending: "schedule" };
const FEATURE_LABEL = { history: "History", ratings: "Ratings", watchlist: "Watchlist", progress: "Progress", collection: "Collections", scrobble: "Scrobble" };
const ORIGIN_FEATURES = ["history", "watchlist", "ratings", "progress", "collection"];

const meta = () => window.CW?.ProviderMeta || {};
export const providerLabel = (p) => meta().label?.(p) || String(p || "").toUpperCase();

export function instanceName(provider, instance) {
  const id = String(instance || "default").trim() || "default";
  if (id.toLowerCase() === "default") return "";
  try { return meta().instanceLabel?.(provider, id) || id; } catch { return id; }
}

export function providerChipHTML(provider, instance, extra = "") {
  const logo = meta().logoPath?.(provider) || "";
  const inst = instanceName(provider, instance);
  return `<span class="ev-pchip"${extra}>${logo ? `<img class="ev-pchip-logo" src="${esc(logo)}" alt="" aria-hidden="true" loading="lazy">` : ""}<span class="ev-pchip-name">${esc(providerLabel(provider))}</span>${inst ? `<span class="ev-pchip-inst">${esc(inst)}</span>` : ""}</span>`;
}

const pad2 = (n) => String(n).padStart(2, "0");
const coordOf = (item) => item.season != null && item.episode != null ? `S${pad2(item.season)}E${pad2(item.episode)}` : "";
const isEpisode = (item) => item.media_type === "episode" || (item.season != null && item.episode != null);

const KIND_LABEL = {
  "history:add": "Watched", "history:remove": "Unwatched", "history:rewatch_reset": "Marked unwatched",
  "history:tag_dropped": "Marked dropped", "watchlist:add": "Watchlist add", "watchlist:remove": "Watchlist removal",
  "progress:add": "Progress", "progress:update": "Progress", "progress:remove": "Progress removed",
  "progress:hide_continue_watching": "Hidden from Continue Watching",
  "collection:add": "Collection add", "collection:remove": "Collection removal",
  "ratings:remove": "Rating removed", "scrobble:watch": "Watched",
};

export function whatOf(item) {
  const kinds = Array.isArray(item.kinds) ? item.kinds : [];
  const first = kinds.find((k) => k.startsWith("scrobble:")) || kinds[0] || "";
  let label = KIND_LABEL[first];
  if (first === "scrobble:watch") {
    if (["running", "paused"].includes(item.status)) label = "Watching";
    else if (item.status === "stopped") label = "Stopped watching";
  }
  if (first.startsWith("ratings:") && first !== "ratings:remove" || first === "scrobble:rating") label = item.rating != null && item.rating !== "" ? `Rated ${item.rating}` : "Rating";
  if (!label) label = first ? first.split(":").map((p) => p.charAt(0).toUpperCase() + p.slice(1)).join(" ") : "Activity";
  const more = kinds.length > 1 ? ` +${kinds.length - 1}` : "";
  return `${label}${more}`;
}

export function viaOf(item) {
  if (item.domain === "scrobble") return item.progress != null && item.progress !== "" ? `Scrobble · ${item.progress}%` : "Scrobble";
  const feats = [...new Set((item.kinds || []).map((k) => k.split(":")[0]).filter(Boolean))];
  return `Sync${feats.length ? ` · ${feats.map((f) => FEATURE_LABEL[f] || f).join(", ")}` : ""}`;
}

export function coverURL(item, size = "w185") {
  const ids = item.ids || {};
  const tv = isEpisode(item) || item.media_type === "show" || item.media_type === "season";
  const kind = tv ? "tv" : "movie";
  const tmdb = tv ? (ids.show_tmdb || (item.media_type === "show" ? ids.tmdb : "")) : ids.tmdb;
  if (tmdb && /^\d{1,10}$/.test(String(tmdb))) {
    const choice = window.CW?.ArtChoice?.param?.(kind, tmdb) || "";
    return `/art/tmdb/${kind}/${encodeURIComponent(String(tmdb))}?fallback=2&size=${encodeURIComponent(size)}${choice}`;
  }
  const imdb = String((tv ? ids.show_imdb : ids.imdb) || "").toLowerCase();
  const tvdb = String((tv ? ids.show_tvdb : ids.tvdb) || "");
  const qs = new URLSearchParams();
  if (/^tt\d{1,10}$/.test(imdb)) qs.set("imdb", imdb);
  if (/^\d{1,10}$/.test(tvdb)) qs.set("tvdb", tvdb);
  if (![...qs.keys()].length) return "";
  qs.set("size", size);
  return `/art/lookup/${kind}?${qs.toString()}`;
}

const coverHTML = (item, tmdbOn, size = "w185") => {
  const url = tmdbOn ? coverURL(item, size) : "";
  const icon = isEpisode(item) || item.media_type === "show" ? "live_tv" : "movie";
  return `<span class="ev-tcover" aria-hidden="true"><span class="material-symbols-rounded">${icon}</span>${url ? `<img src="${esc(url)}" alt="" loading="lazy" decoding="async">` : ""}</span>`;
};

export function bindCovers(root) {
  root.querySelectorAll(".ev-tcover img").forEach((img) => {
    if (img.dataset.bound) return;
    img.dataset.bound = "1";
    const hide = () => img.remove();
    if (img.complete && img.naturalWidth === 0) hide();
    else img.addEventListener("error", hide, { once: true });
  });
}

export function destChipHTML(d) {
  const state = STATE_LABEL[d.state] ? d.state : "skip";
  const reason = d.reason_label || d.reason || d.reason_code || "";
  const feature = FEATURE_LABEL[d.feature] || d.feature || "";
  const tip = [STATE_LABEL[state], feature, reason].filter(Boolean).join(" · ");
  return `<span class="ev-dchip ${state}" title="${esc(tip)}"><span class="material-symbols-rounded" aria-hidden="true">${STATE_ICON[state]}</span>${providerChipHTML(d.provider, d.instance)}<span class="ev-sr">${esc(STATE_LABEL[state])}</span></span>`;
}

export function titleText(item) {
  return item.title || item.item_key || "Unknown title";
}

export function subText(item) {
  const bits = [];
  const coord = coordOf(item);
  if (coord) bits.push(coord);
  else if (item.media_type) bits.push(item.media_type === "movie" ? "Movie" : item.media_type.charAt(0).toUpperCase() + item.media_type.slice(1));
  if (item.year && !coord) bits.push(String(item.year));
  return bits.join(" · ");
}

export function sectionLabel(sec) {
  const d = dayIndexOf(sec);
  const today = dayIndexOf(Date.now() / 1000);
  if (d === today) return "Today";
  if (d === today - 1) return "Yesterday";
  if (today - d < 7) return dateOfIndex(d).toLocaleDateString([], { weekday: "long" });
  return dateOfIndex(d).toLocaleDateString([], { day: "numeric", month: "long", year: "numeric" });
}

const rowTime = (sec) => {
  const n = Number(sec || 0);
  if (!n) return "";
  const today = dayIndexOf(Date.now() / 1000);
  const d = new Date(n * 1000);
  const time = d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  return today - dayIndexOf(n) < 7 ? time : `${d.toLocaleDateString([], { day: "numeric", month: "short" })} ${time}`;
};

export function titleRowHTML(item, { tmdbOn = false, selected = false } = {}) {
  const dests = (item.destinations || []).map(destChipHTML).join("");
  const name = titleText(item);
  const attention = Number(item.attention) === 1;
  return `
    <div class="ev-trow${selected ? " sel" : ""}${item.acknowledged_at ? " acked" : ""}" data-tid="${esc(item.id)}" tabindex="0" role="button" aria-label="${esc(`${name} ${subText(item)}`)}">
      ${coverHTML(item, tmdbOn)}
      <span class="ev-tmain">
        <span class="ev-ttop">
          ${attention ? `<span class="ev-tattn" title="Needs attention"></span>` : ""}
          <span class="ev-ttitle">${esc(name)}</span>
          <time class="ev-ttime">${esc(rowTime(item.last_event_at))}</time>
        </span>
        <span class="ev-tsub">${esc([subText(item), whatOf(item), viaOf(item)].filter(Boolean).join(" · "))}</span>
        <span class="ev-troute">${item.source_provider ? `${providerChipHTML(item.source_provider, item.source_instance, ' data-role="source"')}<span class="material-symbols-rounded ev-tarrow" aria-hidden="true">arrow_forward</span>` : ""}<span class="ev-tdests">${dests || `<span class="ev-tnone">No destination yet</span>`}</span></span>
      </span>
    </div>`;
}

export function titleRowsHTML(items, opts = {}) {
  const out = [];
  let lastLabel = "";
  for (const item of items) {
    const label = sectionLabel(item.last_event_at);
    if (label !== lastLabel) {
      out.push(`<div class="ev-tsection" role="presentation">${esc(label)}</div>`);
      lastLabel = label;
    }
    out.push(titleRowHTML(item, { ...opts, selected: String(opts.selected || "") === String(item.id) }));
  }
  return out.join("");
}

const fullTime = (sec) => {
  const n = Number(sec || 0);
  return n ? new Date(n * 1000).toLocaleString([], { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" }) : "";
};

const ID_LABELS = { tmdb: "TMDB", imdb: "IMDb", tvdb: "TVDB", show_tmdb: "Show TMDB", show_imdb: "Show IMDb", show_tvdb: "Show TVDB" };

function originItem(item) {
  const ids = item.ids || {};
  const own = {};
  const show = {};
  for (const key of ["tmdb", "imdb", "tvdb"]) {
    if (ids[key]) own[key] = String(ids[key]);
    if (ids[`show_${key}`]) show[key] = String(ids[`show_${key}`]);
  }
  const type = isEpisode(item) ? "episode" : (item.media_type || "movie");
  const out = { type, title: item.title || "", ids: own };
  if (item.year) out.year = Number(item.year);
  if (type === "episode") {
    out.season = item.season;
    out.episode = item.episode;
    out.series_title = item.title || "";
    if (Object.keys(show).length) out.show_ids = show;
  }
  return out;
}

export function renderTitleDetail(host, item, ctx) {
  const tab = ctx.tab || "path";
  const related = Array.isArray(item.related) ? item.related : [];
  const later = related.filter((r) => Number(r.last_event_at) > Number(item.last_event_at)).slice(-4).reverse();
  const failed = item.status === "failed";
  const reviewed = !!item.acknowledged_at;
  const ids = Object.entries(item.ids || {}).filter(([k, v]) => ID_LABELS[k] && v).map(([k, v]) => `<span class="ev-tid">${esc(ID_LABELS[k])} ${esc(v)}</span>`).join("");
  const sourceLine = item.domain === "scrobble"
    ? `Played on ${providerLabel(item.source_provider)}${item.progress != null && item.progress !== "" ? ` · stopped at ${item.progress}%` : ""}`
    : `From ${providerLabel(item.source_provider)} by sync`;
  const steps = [];
  if (item.source_provider) {
    steps.push(`<li class="ev-pstep"><span class="ev-pdot src"><span class="material-symbols-rounded" aria-hidden="true">play_arrow</span></span><div><div class="ev-pline">${providerChipHTML(item.source_provider, item.source_instance)}<time>${esc(fullTime(item.first_event_at))}</time></div><p>${esc(sourceLine)}</p></div></li>`);
  }
  for (const d of item.destinations || []) {
    const state = STATE_LABEL[d.state] ? d.state : "skip";
    const reason = d.state === "ok" ? "" : (d.reason_label || d.reason || d.reason_code || "");
    const feature = FEATURE_LABEL[d.feature] || d.feature || "";
    steps.push(`<li class="ev-pstep"><span class="ev-pdot ${state}"><span class="material-symbols-rounded" aria-hidden="true">${STATE_ICON[state]}</span></span><div><div class="ev-pline">${providerChipHTML(d.provider, d.instance)}<time>${esc(fullTime(d.at))}</time></div><p class="${state}">${esc([STATE_LABEL[state], feature, reason].filter(Boolean).join(" · "))}</p></div></li>`);
  }
  for (const r of later) {
    steps.push(`<li class="ev-pstep"><span class="ev-pdot next"><span class="material-symbols-rounded" aria-hidden="true">east</span></span><div><button type="button" class="ev-plink" data-tid="${esc(r.id)}"><span>${esc(`${r.domain === "scrobble" ? "Later scrobble" : "Later sync"}: ${whatOf(r)}`)}</span><time>${esc(fullTime(r.last_event_at))}</time></button><div class="ev-tdests">${(r.destinations || []).map(destChipHTML).join("")}</div></div></li>`);
  }
  const originDests = (item.destinations || []).filter((d) => d.provider);
  const originHTML = originDests.length
    ? originDests.map((d, i) => `<details class="ev-origin" data-i="${i}"${i === 0 ? " open" : ""}><summary>${providerChipHTML(d.provider, d.instance)}<span class="ev-origin-feat">${esc(FEATURE_LABEL[d.feature] || d.feature || "")}</span></summary><div class="ev-origin-body cw-origin-body">Looking this up…</div></details>`).join("")
    : `<div class="ev-empty ev-empty-inline">No destination to explain yet.</div>`;
  const earlierHTML = related.length
    ? related.map((r) => `<button type="button" class="ev-erow" data-tid="${esc(r.id)}"><span class="ev-erow-main"><span>${esc(whatOf(r))}</span><span class="ev-erow-sub">${esc(viaOf(r))}</span></span><span class="ev-tdests">${(r.destinations || []).map(destChipHTML).join("")}</span><time>${esc(fullTime(r.last_event_at))}</time></button>`).join("")
    : `<div class="ev-empty ev-empty-inline">Nothing earlier for this title.</div>`;
  const tabBtn = (name, label, count) => `<button class="ev-tab${tab === name ? " on" : ""}" data-tab="${name}" type="button" role="tab" aria-selected="${tab === name}">${esc(label)}${count != null ? ` <span class="ev-h4-note">${esc(count)}</span>` : ""}</button>`;
  host.innerHTML = `
    <div class="ev-thead">
      ${coverHTML(item, ctx.tmdbOn, "w342")}
      <div class="ev-thead-main">
        <div class="ev-dhead-row">
          <span class="ev-badge ${failed ? "error" : item.status === "completed" ? "ok" : "info"}">${esc(failed ? "Failed" : whatOf(item))}</span>
          <span class="ev-dhead-spacer"></span>
          ${failed || reviewed ? `<button class="ev-hbtn ev-hbtn-accent${reviewed ? " on" : ""}" id="ev-tack" type="button"><span class="material-symbols-rounded" aria-hidden="true">${reviewed ? "check_circle" : "check"}</span><span>${reviewed ? "Reviewed" : "Mark reviewed"}</span></button>` : ""}
        </div>
        <div class="ev-dtitle">${esc(titleText(item))}</div>
        <p class="ev-outcome">${esc([subText(item), viaOf(item)].filter(Boolean).join(" · "))}</p>
        ${ids ? `<div class="ev-tids">${ids}</div>` : ""}
      </div>
    </div>
    <div class="ev-tabs" role="tablist">${tabBtn("path", "Path")}${tabBtn("origin", "Where it comes from")}${tabBtn("earlier", "Earlier", related.length)}</div>
    <div class="ev-tabpane" data-pane="path"${tab === "path" ? "" : " hidden"}><ol class="ev-path">${steps.join("") || `<li class="ev-empty ev-empty-inline">Nothing recorded yet.</li>`}</ol>${item.run_id && item.domain === "sync" ? `<div class="ev-path-acts"><button type="button" class="ev-tbtn" id="ev-trun"><span class="material-symbols-rounded" aria-hidden="true">sync</span><span>Show this run</span></button><button type="button" class="ev-tbtn" id="ev-tlogs"><span class="material-symbols-rounded" aria-hidden="true">terminal</span><span>View logs</span></button></div>` : ""}</div>
    <div class="ev-tabpane" data-pane="origin"${tab === "origin" ? "" : " hidden"}><p class="ev-origin-note">Likely path, rebuilt from current data.</p>${originHTML}</div>
    <div class="ev-tabpane" data-pane="earlier"${tab === "earlier" ? "" : " hidden"}><div class="ev-earlier">${earlierHTML}</div></div>`;
  bindCovers(host);

  const loaded = new Set();
  const loadOrigin = (details) => {
    const i = Number(details.dataset.i);
    if (loaded.has(i)) return;
    loaded.add(i);
    const d = originDests[i];
    const body = details.querySelector(".ev-origin-body");
    const feature = ORIGIN_FEATURES.includes(d.feature) ? d.feature : "history";
    ctx.fetchJson("/api/editor/origin", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ provider: d.provider, instance: d.instance || "default", feature, key: item.item_key || "", original: originItem(item) }),
    }).then((data) => {
      if (body.isConnected) renderItemOrigin(body, data, { snapshot: d.provider, instance: d.instance || "default" });
    }).catch(() => {
      if (body.isConnected) body.textContent = "Could not look this up. You may not have access to this provider.";
    });
  };
  const switchTab = (name) => {
    ctx.onTab?.(name);
    host.querySelectorAll(".ev-tab").forEach((b) => { const on = b.dataset.tab === name; b.classList.toggle("on", on); b.setAttribute("aria-selected", String(on)); });
    host.querySelectorAll(".ev-tabpane").forEach((p) => { p.hidden = p.dataset.pane !== name; });
    if (name === "origin") host.querySelectorAll(".ev-origin[open]").forEach(loadOrigin);
  };
  host.querySelectorAll(".ev-tab").forEach((b) => b.addEventListener("click", () => switchTab(b.dataset.tab)));
  host.querySelectorAll(".ev-origin").forEach((details) => details.addEventListener("toggle", () => { if (details.open && !details.closest(".ev-tabpane").hidden) loadOrigin(details); }));
  if (tab === "origin") host.querySelectorAll(".ev-origin[open]").forEach(loadOrigin);
  host.querySelectorAll("[data-tid]").forEach((b) => b.addEventListener("click", () => ctx.openTitle?.(b.dataset.tid)));
  host.querySelector("#ev-tack")?.addEventListener("click", () => ctx.review?.(item));
  host.querySelector("#ev-trun")?.addEventListener("click", () => ctx.showRun?.(item));
  host.querySelector("#ev-tlogs")?.addEventListener("click", () => window.openLogs?.({ runId: item.run_id, channel: "sync" }));
}

function levels(values) {
  const nonZero = values.filter((v) => v > 0).sort((a, b) => a - b);
  if (!nonZero.length) return [1, 2, 3, 4];
  const at = (p) => nonZero[Math.min(nonZero.length - 1, Math.floor(p * (nonZero.length - 1)))];
  const out = [];
  let prev = 0;
  for (const v of [at(0.25), at(0.5), at(0.75), at(0.92)]) {
    const next = Math.max(prev + 1, Math.round(v));
    out.push(next);
    prev = next;
  }
  return out;
}

const levelOf = (value, steps) => {
  if (value <= 0) return 0;
  for (let i = 0; i < steps.length; i += 1) if (value <= steps[i]) return i + 1;
  return 4;
};

const RANGES = [{ value: "3m", label: "3 months", days: 98, weeks: 14 }, { value: "6m", label: "6 months", days: 189, weeks: 27 }, { value: "12m", label: "1 year", days: 371, weeks: 53 }];
const METRICS = [{ value: "changes", label: "Changes" }, { value: "runs", label: "Runs" }, { value: "failed", label: "Failures" }];

export const dayRange = (index) => ({ since: index * DAY - TZ, until: index * DAY - TZ + DAY - 1 });
export const dayLabel = (index) => dateOfIndex(index).toLocaleDateString([], { weekday: "short", day: "numeric", month: "short" });

export function createActivityCard(host, { fetchJson, onPickDay, store }) {
  const get = (k, d) => store?.get?.(k, d) ?? d;
  const narrow = matchMedia("(max-width:760px)");
  let range = RANGES.some((r) => r.value === get("range", "")) ? get("range", "12m") : (narrow.matches ? "3m" : "12m");
  let metric = METRICS.some((m) => m.value === get("metric", "")) ? get("metric", "changes") : "changes";
  let collapsed = get("collapsed", narrow.matches ? "1" : "0") === "1";
  let byIndex = new Map();
  let totals = {};
  let selected = null;
  let alive = true;
  const tip = document.createElement("div");
  tip.className = "ev-hm-tip";
  tip.hidden = true;
  document.body.appendChild(tip);

  host.innerHTML = `
    <section class="ev-activity${collapsed ? " collapsed" : ""}" aria-label="Sync activity">
      <div class="ev-activity-head">
        <div class="ev-activity-title"><strong>Sync activity</strong><span class="ev-activity-sum" id="ev-act-sum"></span></div>
        <div class="ev-activity-tools">
          <div class="ev-seg" role="group" aria-label="Show" id="ev-act-metric">${METRICS.map((m) => `<button type="button" class="ev-seg-btn${m.value === metric ? " on" : ""}" data-v="${m.value}" aria-pressed="${m.value === metric}">${esc(m.label)}</button>`).join("")}</div>
          <label class="ev-act-range"><span class="ev-sr">Period</span><select id="ev-act-range" class="cw-page-input" data-cw-native-select="true">${RANGES.map((r) => `<option value="${r.value}"${r.value === range ? " selected" : ""}>${esc(r.label)}</option>`).join("")}</select></label>
        </div>
        <button type="button" class="ev-icon-btn ev-act-toggle" id="ev-act-toggle" aria-expanded="${!collapsed}" aria-label="${collapsed ? "Show sync activity" : "Hide sync activity"}"><span class="material-symbols-rounded" aria-hidden="true">${collapsed ? "expand_more" : "expand_less"}</span></button>
      </div>
      <div class="ev-activity-body"${collapsed ? " hidden" : ""}>
        <div class="ev-hm-wrap"><div class="ev-hm-scroll" tabindex="0" aria-label="Activity calendar, scroll to see more weeks"><div id="ev-hm"></div></div>
          <div class="ev-hm-foot"><span>Pick a day to filter the list.</span><span class="ev-hm-legend"><span>Less</span><i class="lv0"></i><i class="lv1"></i><i class="lv2"></i><i class="lv3"></i><i class="lv4"></i><span>More</span><i class="lv0 bad"></i><span>Had a failure</span></span></div>
        </div>
        <div class="ev-tiles" id="ev-act-tiles"></div>
      </div>
    </section>`;
  const Q = (s) => host.querySelector(s);
  const value = (e) => !e ? 0 : metric === "runs" ? e.runs || 0 : metric === "failed" ? e.failed || 0 : e.changes || 0;

  const renderTiles = () => {
    const last = [...byIndex.values()].filter((e) => e.runs).map((e) => Number(e.d)).sort((a, b) => b - a)[0];
    Q("#ev-act-sum").textContent = collapsed ? `${fmt(totals.runs)} runs · ${fmt(totals.changes)} changes · ${fmt(totals.failed)} failed` : "";
    Q("#ev-act-tiles").innerHTML = [
      [fmt(totals.runs), "Runs"],
      [fmt(totals.changes), "Changes"],
      [fmt(totals.failed), "Failed runs", totals.failed ? " bad" : ""],
      [last != null ? dateOfIndex(last).toLocaleDateString([], { day: "numeric", month: "short" }) : "–", "Last active day"],
    ].map(([v, k, cls = ""]) => `<div class="ev-tile${cls}"><b>${esc(v)}</b><span>${esc(k)}</span></div>`).join("");
  };

  const renderMap = () => {
    const cfg = RANGES.find((r) => r.value === range) || RANGES[2];
    const today = dayIndexOf(Date.now() / 1000);
    const lastCol = today - ((dateOfIndex(today).getDay() + 6) % 7);
    const firstCol = lastCol - (cfg.weeks - 1) * 7;
    const [STEP, CELL] = narrow.matches ? [27, 23] : [17, 13];
    const PAD_L = 28, PAD_T = 16;
    const steps = levels([...byIndex.values()].map(value));
    const cells = [];
    const months = [];
    let lastMonth = -1;
    for (let w = 0; w < cfg.weeks; w += 1) {
      const col = firstCol + w * 7;
      const month = dateOfIndex(col).getMonth();
      if (month !== lastMonth) { lastMonth = month; months.push(`<text class="ev-hm-ax" x="${PAD_L + w * STEP}" y="10">${MONTHS[month]}</text>`); }
      for (let d = 0; d < 7; d += 1) {
        const index = col + d;
        if (index > today) continue;
        const entry = byIndex.get(index);
        const bad = (entry?.failed || 0) > 0;
        const cls = `ev-hm-cell lv${levelOf(value(entry), steps)}${bad ? " bad" : ""}${index === selected ? " on" : ""}${index === today ? " today" : ""}`;
        cells.push(`<rect class="${cls}" x="${PAD_L + w * STEP}" y="${PAD_T + d * STEP}" width="${CELL}" height="${CELL}" rx="3.5" data-d="${index}" role="button" tabindex="${index === (selected ?? today) ? 0 : -1}" aria-pressed="${index === selected}" aria-label="${esc(`${dayLabel(index)}: ${fmt(entry?.runs)} runs, ${fmt(entry?.changes)} changes${bad ? ", had a failure" : ""}`)}"></rect>`);
      }
    }
    const axis = [0, 2, 4].map((d) => `<text class="ev-hm-ax" x="0" y="${PAD_T + d * STEP + 10}">${WEEKDAYS[d]}</text>`).join("");
    const width = PAD_L + cfg.weeks * STEP;
    const height = PAD_T + 7 * STEP;
    Q("#ev-hm").innerHTML = `<svg class="ev-hm-svg" viewBox="0 0 ${width} ${height}" width="${width}" height="${height}" role="group" aria-label="Sync activity calendar">${months.join("")}${axis}${cells.join("")}</svg>`;
    Q("#ev-hm").querySelectorAll(".ev-hm-cell").forEach((cell) => {
      const index = Number(cell.dataset.d);
      cell.addEventListener("mousemove", (ev) => {
        const e = byIndex.get(index);
        tip.replaceChildren();
        const head = document.createElement("div");
        head.className = "ev-hm-tip-d";
        head.textContent = dateOfIndex(index).toLocaleDateString([], { weekday: "long", day: "numeric", month: "long", year: "numeric" });
        const body = document.createElement("div");
        body.textContent = e?.runs ? `${fmt(e.runs)} ${e.runs === 1 ? "run" : "runs"} · ${fmt(e.changes)} changes${e.failed ? ` · ${fmt(e.failed)} failed` : ""}` : "No sync runs";
        tip.append(head, body);
        tip.hidden = false;
        const r = tip.getBoundingClientRect();
        tip.style.left = `${Math.max(8, Math.min(ev.clientX - r.width / 2, window.innerWidth - r.width - 8))}px`;
        tip.style.top = `${Math.max(8, cell.getBoundingClientRect().top - r.height - 10)}px`;
      });
      cell.addEventListener("mouseleave", () => { tip.hidden = true; });
      const pick = () => { tip.hidden = true; onPickDay?.(index === selected ? null : index); };
      cell.addEventListener("click", pick);
      cell.addEventListener("keydown", (ev) => {
        if (["Enter", " "].includes(ev.key)) { ev.preventDefault(); pick(); return; }
        const delta = { ArrowLeft: -7, ArrowRight: 7, ArrowUp: -1, ArrowDown: 1 }[ev.key];
        const target = delta && Q(`.ev-hm-cell[data-d="${index + delta}"]`);
        if (target) { ev.preventDefault(); cell.setAttribute("tabindex", "-1"); target.setAttribute("tabindex", "0"); target.focus(); }
      });
    });
    pinScroll();
  };

  let userScrolled = false;
  const pinScroll = () => {
    const scroll = Q(".ev-hm-scroll");
    if (!userScrolled) scroll.scrollLeft = scroll.scrollWidth;
  };
  const scrollEl = Q(".ev-hm-scroll");
  scrollEl.addEventListener("scroll", () => {
    userScrolled = scrollEl.scrollLeft + scrollEl.clientWidth < scrollEl.scrollWidth - 4;
  }, { passive: true });
  const resizeObserver = typeof ResizeObserver === "function" ? new ResizeObserver(() => pinScroll()) : null;
  resizeObserver?.observe(scrollEl);
  const onNarrow = () => { if (!collapsed) renderMap(); };
  narrow.addEventListener("change", onNarrow);

  const load = async () => {
    const cfg = RANGES.find((r) => r.value === range) || RANGES[2];
    try {
      const data = await fetchJson(`/api/events/calendar?days=${cfg.days}&tz=${TZ}`);
      if (!alive) return;
      byIndex = new Map((data?.days || []).map((row) => [Number(row.d), row]));
      totals = data?.totals || {};
    } catch {
      if (!alive) return;
      byIndex = new Map();
      totals = {};
    }
    renderTiles();
    renderMap();
  };

  Q("#ev-act-metric").querySelectorAll(".ev-seg-btn").forEach((b) => b.addEventListener("click", () => {
    metric = b.dataset.v;
    store?.set?.("metric", metric);
    Q("#ev-act-metric").querySelectorAll(".ev-seg-btn").forEach((x) => { const on = x === b; x.classList.toggle("on", on); x.setAttribute("aria-pressed", String(on)); });
    renderMap();
  }));
  Q("#ev-act-range").addEventListener("change", (ev) => { range = ev.target.value; store?.set?.("range", range); void load(); });
  Q("#ev-act-toggle").addEventListener("click", (ev) => {
    collapsed = !collapsed;
    store?.set?.("collapsed", collapsed ? "1" : "0");
    Q(".ev-activity-body").hidden = collapsed;
    Q(".ev-activity").classList.toggle("collapsed", collapsed);
    const btn = ev.currentTarget;
    btn.setAttribute("aria-expanded", String(!collapsed));
    btn.setAttribute("aria-label", collapsed ? "Show sync activity" : "Hide sync activity");
    btn.querySelector(".material-symbols-rounded").textContent = collapsed ? "expand_more" : "expand_less";
    renderTiles();
    if (!collapsed) renderMap();
  });

  return {
    load,
    latestDay: () => [...byIndex.values()].filter((e) => e.runs).map((e) => Number(e.d)).sort((a, b) => b - a)[0] ?? null,
    setSelected(index) { selected = index; if (!collapsed) renderMap(); },
    destroy() { alive = false; resizeObserver?.disconnect(); narrow.removeEventListener("change", onNarrow); tip.remove(); },
  };
}

const runTime = (epoch) => {
  const n = Number(epoch || 0);
  return n ? new Date(n * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : "--:--";
};
const runDur = (s) => {
  const n = Number(s || 0);
  if (!Number.isFinite(n) || n <= 0) return "";
  if (n < 60) return `${n}s`;
  if (n < 3600) return `${Math.floor(n / 60)}m ${n % 60}s`;
  return `${(n / 3600).toFixed(1)}h`;
};
const counts = (row) => {
  const bits = [];
  if (row.added) bits.push(`<span class="ev-n add">+${fmt(row.added)}</span>`);
  if (row.removed) bits.push(`<span class="ev-n del">−${fmt(row.removed)}</span>`);
  if (row.updated) bits.push(`<span class="ev-n upd">~${fmt(row.updated)}</span>`);
  if (row.errors) bits.push(`<span class="ev-n err">${fmt(row.errors)} failed</span>`);
  return bits.join("") || `<span class="ev-n zero">0</span>`;
};

const runPairs = (run) => {
  const seen = new Map();
  for (const row of run.pair_rows || []) {
    const key = `${row.src_provider}>${row.dst_provider}`;
    const entry = seen.get(key) || { src: row.src_provider, dst: row.dst_provider, added: 0, removed: 0, updated: 0, errors: 0 };
    for (const k of ["added", "removed", "updated", "errors"]) entry[k] += Number(row[k] || 0);
    seen.set(key, entry);
  }
  return [...seen.values()];
};

export function runRowsHTML(runs, { selected = "", showQuiet = false } = {}) {
  const visible = showQuiet ? runs : runs.filter((r) => (r.changes || 0) > 0 || r.errors || r.status === "running");
  const rows = visible.map((run) => {
    const status = run.errors ? "error" : run.status === "running" ? "info" : (run.changes ? "ok" : "idle");
    const pairs = runPairs(run).filter((p) => p.added || p.removed || p.updated || p.errors);
    const what = run.errors ? `${fmt(run.errors)} failed` : run.changes ? `${fmt(run.changes)} changes` : (run.status === "running" ? "Running" : "No changes");
    return `<div class="ev-rrow${String(selected) === String(run.run_id) ? " sel" : ""}" data-run="${esc(run.run_id)}" tabindex="0" role="button">
      <span class="ev-rtime">${esc(runTime(run.started_at))}</span>
      <span class="ev-rdot ${status}" aria-hidden="true"></span>
      <span class="ev-rmain"><span class="ev-rwhat${run.errors ? " bad" : ""}">${esc(what)}${run.dry_run ? " · dry run" : ""}</span><span class="ev-rpairs">${pairs.map((p) => `<span class="ev-rpair">${providerChipHTML(p.src)}<span class="material-symbols-rounded ev-tarrow" aria-hidden="true">arrow_forward</span>${providerChipHTML(p.dst)}${counts(p)}</span>`).join("")}</span></span>
      <span class="ev-rdur">${esc(runDur(run.duration))}</span>
    </div>`;
  });
  const quiet = runs.length - visible.length;
  const foot = quiet ? `<div class="ev-rfoot">${esc(`${quiet} quiet ${quiet === 1 ? "run" : "runs"} ${showQuiet ? "shown" : "hidden"}`)} · <button type="button" class="ev-linkbtn" id="ev-runs-quiet">${showQuiet ? "Hide" : "Show"}</button></div>` : "";
  return (rows.join("") || `<div class="ev-empty ev-empty-inline">${runs.length ? "Only runs without changes on this day." : "No sync runs on this day."}</div>`) + foot;
}

export function renderRunDetail(host, run, ctx) {
  const rows = run.pair_rows || [];
  const features = ["history", "ratings", "watchlist", "progress", "collection"].filter((f) => rows.some((r) => r.feature === f));
  const routes = new Map();
  for (const r of rows) {
    const key = `${r.src_provider}|${r.src_instance}|${r.dst_provider}|${r.dst_instance}`;
    const entry = routes.get(key) || { r, by: {} };
    entry.by[r.feature] = r;
    routes.set(key, entry);
  }
  const table = routes.size ? `<div class="ev-rtable" role="table">
      <div class="ev-rtr ev-rth" role="row"><span role="columnheader">Pair</span>${features.map((f) => `<span role="columnheader">${esc(FEATURE_LABEL[f] || f)}</span>`).join("")}</div>
      ${[...routes.values()].map(({ r, by }) => `<div class="ev-rtr" role="row"><span role="cell" class="ev-rroute">${providerChipHTML(r.src_provider, r.src_instance)}<span class="material-symbols-rounded ev-tarrow" aria-hidden="true">arrow_forward</span>${providerChipHTML(r.dst_provider, r.dst_instance)}</span>${features.map((f) => `<span role="cell">${by[f] ? counts(by[f]) : `<span class="ev-n zero">–</span>`}</span>`).join("")}</div>`).join("")}
    </div>` : `<div class="ev-empty ev-empty-inline">No recorded changes for this run.</div>`;
  const status = run.errors ? "error" : run.status === "running" ? "info" : "ok";
  host.innerHTML = `
    <div class="ev-dhead">
      <div class="ev-dhead-row"><span class="ev-badge ${status}">${esc(run.errors ? "Had failures" : run.status === "running" ? "Running" : "Finished")}</span><span class="ev-dhead-spacer"></span><span class="ev-dtime">${esc(fullTime(run.started_at))}</span></div>
      <div class="ev-dtitle">${esc(`${fmt(run.changes)} changes`)}${run.errors ? ` · ${esc(fmt(run.errors))} failed` : ""}</div>
      <p class="ev-outcome">${esc([runDur(run.duration) ? `Took ${runDur(run.duration)}` : "", run.dry_run ? "Dry run" : ""].filter(Boolean).join(" · "))}</p>
    </div>
    ${table}
    <div class="ev-path-acts">
      <button type="button" class="ev-tbtn" id="ev-run-titles"><span class="material-symbols-rounded" aria-hidden="true">movie</span><span>Show titles from this run</span></button>
      <button type="button" class="ev-tbtn" id="ev-run-logs"><span class="material-symbols-rounded" aria-hidden="true">terminal</span><span>View logs</span></button>
    </div>`;
  host.querySelector("#ev-run-titles")?.addEventListener("click", () => ctx.showTitles?.(run.run_id));
  host.querySelector("#ev-run-logs")?.addEventListener("click", () => window.openLogs?.({ runId: run.run_id, channel: "sync" }));
}

