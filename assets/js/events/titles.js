/* assets/js/events/titles.js */
/* CrossWatch - Events titles, sync activity and runs views */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import {renderItemOrigin} from '../editor/origin.js';

const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
const LOC = () => I18N()?.locale || [];

const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => (
  { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
));
const nf = new Intl.NumberFormat(I18N()?.locale);
const fmt = (n) => nf.format(Math.round(Number(n || 0)));
const DAY = 86400;
const TZ = -new Date().getTimezoneOffset() * 60;
const dayIndexOf = (sec) => Math.floor((Number(sec || 0) + TZ) / DAY);
const dateOfIndex = (d) => new Date((d * DAY - TZ) * 1000);
const MONTHS = Array.from({ length: 12 }, (_, m) => new Date(2024, m, 15).toLocaleDateString(I18N()?.locale || "en", { month: "short" }));
const WEEKDAYS = Array.from({ length: 7 }, (_, d) => new Date(2024, 0, 1 + d).toLocaleDateString(I18N()?.locale || "en", { weekday: "short" }));

export const STATE_LABEL = { ok: T("events.outcome.done"), fail: T("events.outcome.failed"), skip: T("events.outcome.skipped"), same: T("events.outcome.already"), pending: T("events.outcome.inProgress") };
const STATE_ICON = { ok: "check", fail: "close", skip: "remove", same: "radio_button_unchecked", pending: "schedule" };
const FEATURE_LABEL = { history: T("events.feature.history"), ratings: T("events.feature.ratings"), watchlist: T("events.feature.watchlist"), progress: T("events.feature.progress"), collection: T("events.feature.collections"), scrobble: T("events.titles.scrobble") };
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
  "history:add": T("events.badge.watched"), "history:remove": T("events.kind.unwatched"), "history:rewatch_reset": T("events.kind.markedUnwatched"),
  "history:tag_dropped": T("events.title.markDroppedPast"), "watchlist:add": T("events.kind.watchlistAdd"), "watchlist:remove": T("events.kind.watchlistRemoval"),
  "progress:add": T("events.feature.progress"), "progress:update": T("events.feature.progress"), "progress:remove": T("events.kind.progressRemoved"),
  "progress:hide_continue_watching": T("events.kind.hiddenContinue"),
  "collection:add": T("events.kind.collectionAdd"), "collection:remove": T("events.kind.collectionRemoval"),
  "ratings:remove": T("events.kind.ratingRemoved"), "scrobble:watch": T("events.badge.watched"),
};

export function whatOf(item) {
  const kinds = Array.isArray(item.kinds) ? item.kinds : [];
  const first = kinds.find((k) => k.startsWith("scrobble:")) || kinds[0] || "";
  let label = KIND_LABEL[first];
  if (first === "scrobble:watch") {
    if (["running", "paused"].includes(item.status)) label = T("events.kind.watching");
    else if (item.status === "stopped") label = T("events.outcome.stoppedWatching");
  }
  if (first.startsWith("ratings:") && first !== "ratings:remove" || first === "scrobble:rating") label = item.rating != null && item.rating !== "" ? T("events.titles.ratedValue", {rating: String(item.rating)}) : T("events.k.rating");
  if (!label) label = first ? first.split(":").map((p) => p.charAt(0).toUpperCase() + p.slice(1)).join(" ") : T("events.pageTitle");
  const more = kinds.length > 1 ? ` +${kinds.length - 1}` : "";
  return `${label}${more}`;
}

export function viaOf(item) {
  if (item.domain === "scrobble") return item.progress != null && item.progress !== "" ? `${T("events.titles.scrobble")} · ${item.progress}%` : T("events.titles.scrobble");
  return T("events.tabs.sync");
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
  const showIds = tv && (ids.show_imdb || ids.show_tvdb);
  const imdb = String((showIds ? ids.show_imdb : ids.imdb) || "").toLowerCase();
  const tvdb = String((showIds ? ids.show_tvdb : ids.tvdb) || "");
  const qs = new URLSearchParams();
  if (/^tt\d{1,10}$/.test(imdb)) qs.set("imdb", imdb);
  if (/^\d{1,10}$/.test(tvdb)) qs.set("tvdb", tvdb);
  if (![...qs.keys()].length) return "";
  if (tv && !showIds && isEpisode(item)) qs.set("episode", "1");
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

const SHORT_REASONS = [
  [/blackbox|blocked/, "blocked"],
  [/unresolved|not.?found|unmatched|no.?match|missing/, "not found"],
  [/rate|429|throttl|quota/, "rate limited"],
  [/auth|401|403|token|forbidden/, "no access"],
  [/server|5\d\d|timeout|unavailable/, "server error"],
];

export function shortReason(d) {
  if (d.state !== "fail" && d.state !== "skip") return "";
  const code = String(d.reason_code || d.reason || "").toLowerCase();
  if (!code) return "";
  const hit = SHORT_REASONS.find(([re]) => re.test(code));
  return hit ? hit[1] : code.replace(/[_-]+/g, " ").slice(0, 24);
}

export function destChipHTML(d) {
  const state = STATE_LABEL[d.state] ? d.state : "skip";
  const reason = d.reason_label || d.reason || d.reason_code || "";
  const feature = FEATURE_LABEL[d.feature] || d.feature || "";
  const tip = [STATE_LABEL[state], feature, reason].filter(Boolean).join(" · ");
  const short = shortReason({ ...d, state });
  return `<span class="ev-dchip ${state}" title="${esc(tip)}"><span class="material-symbols-rounded" aria-hidden="true">${STATE_ICON[state]}</span>${providerChipHTML(d.provider, d.instance)}<span class="ev-sr">${esc(STATE_LABEL[state])}</span>${short ? `<span class="ev-dreason">${esc(short)}</span>` : ""}</span>`;
}

const rowVia = (item) => {
  if (item.domain !== "scrobble") return "";
  const n = Number(item.sessions || 0);
  return [item.progress != null && item.progress !== "" ? `${item.progress}%` : "", n > 1 ? `${fmt(n)} sessions` : ""].filter(Boolean).join(" · ");
};

const sessionTime = (sec) => Number(sec || 0) ? new Date(Number(sec) * 1000).toLocaleTimeString(LOC(), { hour: "2-digit", minute: "2-digit" }) : "";

const sessionRowHTML = (s) => {
  const what = s.finished ? T("events.k.finished") : s.progress != null && s.progress !== "" ? T("events.titles.stoppedAt", {progress: `${s.progress}%`}) : T("events.badge.started");
  const start = sessionTime(s.started_at);
  const end = sessionTime(s.last_at);
  return `<div class="ev-erow ev-srow"><span class="ev-erow-main"><span>${esc(what)}</span><span class="ev-erow-sub">${esc(start === end ? start : `${start} – ${end}`)}</span></span></div>`;
};

export function titleText(item) {
  return item.title || item.item_key || T("events.unknownTitle");
}

export function subText(item) {
  const bits = [];
  const coord = coordOf(item);
  if (coord) bits.push(coord);
  else if (item.media_type) bits.push(item.media_type === "movie" ? T("events.movie") : item.media_type.charAt(0).toUpperCase() + item.media_type.slice(1));
  if (item.year && !coord) bits.push(String(item.year));
  return bits.join(" · ");
}

export function sectionLabel(sec) {
  const d = dayIndexOf(sec);
  const today = dayIndexOf(Date.now() / 1000);
  if (d === today) return T("events.today");
  if (d === today - 1) return T("events.yesterday");
  if (today - d < 7) return dateOfIndex(d).toLocaleDateString(LOC(), { weekday: "long" });
  return dateOfIndex(d).toLocaleDateString(LOC(), { day: "numeric", month: "long", year: "numeric" });
}

const rowTime = (sec) => {
  const n = Number(sec || 0);
  if (!n) return "";
  const today = dayIndexOf(Date.now() / 1000);
  const d = new Date(n * 1000);
  const time = d.toLocaleTimeString(LOC(), { hour: "2-digit", minute: "2-digit" });
  return today - dayIndexOf(n) < 7 ? time : `${d.toLocaleDateString(LOC(), { day: "numeric", month: "short" })} ${time}`;
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
          ${attention ? `<span class="ev-tattn" title="${TH("events.quick.attention")}"></span>` : ""}
          <span class="ev-ttitle">${esc(name)}</span>
          <time class="ev-ttime">${esc(rowTime(item.last_event_at))}</time>
        </span>
        <span class="ev-tsub">${esc([subText(item), whatOf(item), rowVia(item)].filter(Boolean).join(" · "))}</span>
        <span class="ev-troute">${item.source_provider ? `${providerChipHTML(item.source_provider, item.source_instance, ' data-role="source"')}<span class="material-symbols-rounded ev-tarrow" aria-hidden="true">arrow_forward</span>` : ""}<span class="ev-tdests">${dests || `<span class="ev-tnone">${TH("events.titles.noDestination")}</span>`}</span></span>
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
  return n ? new Date(n * 1000).toLocaleString(LOC(), { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" }) : "";
};

const ID_LABELS = { tmdb: "TMDB", imdb: "IMDb", tvdb: "TVDB", show_tmdb: T("events.titles.showTmdb"), show_imdb: T("events.titles.showImdb"), show_tvdb: T("events.titles.showTvdb") };

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
  const sessions = Array.isArray(item.session_list) ? item.session_list : [];
  const tab = ctx.tab === "sessions" && !sessions.length ? "path" : (ctx.tab || "path");
  const related = Array.isArray(item.related) ? item.related : [];
  const later = related.filter((r) => Number(r.last_event_at) > Number(item.last_event_at)).slice(-4).reverse();
  const failed = item.status === "failed";
  const reviewed = !!item.acknowledged_at;
  const ids = Object.entries(item.ids || {}).filter(([k, v]) => ID_LABELS[k] && v).map(([k, v]) => `<span class="ev-tid">${esc(ID_LABELS[k])} ${esc(v)}</span>`).join("");
  const sourceLine = item.domain === "scrobble"
    ? `${T("events.titles.playedOn", {provider: providerLabel(item.source_provider)})}${item.progress != null && item.progress !== "" ? ` · ${T("events.titles.stoppedAtLower", {progress: `${item.progress}%`})}` : ""}`
    : T("events.titles.fromBySync", {provider: providerLabel(item.source_provider)});
  const steps = [];
  if (item.source_provider) {
    steps.push(`<li class="ev-pstep"><span class="ev-pdot src"><span class="material-symbols-rounded" aria-hidden="true">play_arrow</span></span><div><div class="ev-pline">${providerChipHTML(item.source_provider, item.source_instance)}<time>${esc(fullTime(item.first_event_at))}</time></div><p>${esc(sourceLine)}</p></div></li>`);
  }
  for (const d of item.destinations || []) {
    const state = STATE_LABEL[d.state] ? d.state : "skip";
    const reason = d.state === "ok" ? "" : (d.reason_label || d.reason || d.reason_code || "");
    const feature = FEATURE_LABEL[d.feature] || d.feature || "";
    steps.push(`<li class="ev-pstep"><span class="ev-pdot ${state}"><span class="material-symbols-rounded" aria-hidden="true">${STATE_ICON[state]}</span></span><div><div class="ev-pline">${providerChipHTML(d.provider, d.instance)}<time>${esc(fullTime(d.at))}</time></div><p${state === "fail" ? ` class="ev-pfail"` : ""}>${esc([STATE_LABEL[state], feature, reason].filter(Boolean).join(" · "))}</p></div></li>`);
  }
  for (const r of later) {
    steps.push(`<li class="ev-pstep"><span class="ev-pdot next"><span class="material-symbols-rounded" aria-hidden="true">east</span></span><div><button type="button" class="ev-plink" data-tid="${esc(r.id)}"><span>${esc(`${r.domain === "scrobble" ? T("events.titles.laterScrobble") : T("events.titles.laterSync")}: ${whatOf(r)}`)}</span><time>${esc(fullTime(r.last_event_at))}</time></button><div class="ev-tdests">${(r.destinations || []).map(destChipHTML).join("")}</div></div></li>`);
  }
  const originDests = (item.destinations || []).filter((d) => d.provider);
  const originHTML = originDests.length
    ? originDests.map((d, i) => `<details class="ev-origin" data-i="${i}"${i === 0 ? " open" : ""}><summary>${providerChipHTML(d.provider, d.instance)}<span class="ev-origin-feat">${esc(FEATURE_LABEL[d.feature] || d.feature || "")}</span></summary><div class="ev-origin-body cw-origin-body">${TH("events.titles.lookingUp")}</div></details>`).join("")
    : `<div class="ev-empty ev-empty-inline">${TH("events.titles.noExplain")}</div>`;
  const earlierHTML = related.length
    ? related.map((r) => `<button type="button" class="ev-erow" data-tid="${esc(r.id)}"><span class="ev-erow-main"><span>${esc(whatOf(r))}</span><span class="ev-erow-sub">${esc(viaOf(r))}</span></span><span class="ev-tdests">${(r.destinations || []).map(destChipHTML).join("")}</span><time>${esc(fullTime(r.last_event_at))}</time></button>`).join("")
    : `<div class="ev-empty ev-empty-inline">${TH("events.titles.nothingEarlier")}</div>`;
  const tabBtn = (name, label, count) => `<button class="ev-tab${tab === name ? " on" : ""}" data-tab="${name}" type="button" role="tab" aria-selected="${tab === name}">${esc(label)}${count != null ? ` <span class="ev-h4-note">${esc(count)}</span>` : ""}</button>`;
  host.innerHTML = `
    <div class="ev-thead">
      ${coverHTML(item, ctx.tmdbOn, "w342")}
      <div class="ev-thead-main">
        <div class="ev-dhead-row">
          <span class="ev-badge ${failed ? "error" : item.status === "completed" ? "ok" : "info"}">${esc(failed ? T("events.outcome.failed") : whatOf(item))}</span>
          <span class="ev-dhead-spacer"></span>
          ${failed || reviewed ? `<button class="ev-hbtn ev-hbtn-accent${reviewed ? " on" : ""}" id="ev-tack" type="button"><span class="material-symbols-rounded" aria-hidden="true">${reviewed ? "check_circle" : "check"}</span><span>${TH(reviewed ? "events.titles.reviewed" : "events.titles.markReviewed")}</span></button>` : ""}
        </div>
        <div class="ev-dtitle">${esc(titleText(item))}</div>
        <p class="ev-outcome">${esc([subText(item), rowVia(item)].filter(Boolean).join(" · "))}</p>
        ${ids ? `<div class="ev-tids">${ids}</div>` : ""}
      </div>
    </div>
    <div class="ev-tabs" role="tablist">${tabBtn("path", T("events.k.path"))}${tabBtn("origin", T("events.titles.whereFrom"))}${sessions.length ? tabBtn("sessions", T("events.titles.sessions"), sessions.length) : ""}${tabBtn("earlier", T("events.titles.earlier"), related.length || null)}</div>
    <div class="ev-tabpane" data-pane="path"${tab === "path" ? "" : " hidden"}><ol class="ev-path">${steps.join("") || `<li class="ev-empty ev-empty-inline">${TH("events.titles.nothingRecorded")}</li>`}</ol>${item.run_id && item.domain === "sync" ? `<div class="ev-path-acts"><button type="button" class="ev-tbtn" id="ev-trun"><span class="material-symbols-rounded" aria-hidden="true">sync</span><span>${TH("events.titles.showRun")}</span></button><button type="button" class="ev-tbtn" id="ev-tlogs"><span class="material-symbols-rounded" aria-hidden="true">terminal</span><span>${TH("events.viewLogs")}</span></button></div>` : ""}</div>
    <div class="ev-tabpane" data-pane="origin"${tab === "origin" ? "" : " hidden"}><p class="ev-origin-note">${TH("events.titles.likelyPath")}</p>${originHTML}</div>
    ${sessions.length ? `<div class="ev-tabpane" data-pane="sessions"${tab === "sessions" ? "" : " hidden"}><div class="ev-earlier">${sessions.map(sessionRowHTML).join("")}</div></div>` : ""}
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
      if (body.isConnected) body.textContent = T("events.titles.lookupFailed");
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

const RANGES = [{ value: "3m", label: T("events.range.3m"), days: 98, weeks: 14 }, { value: "6m", label: T("events.range.6m"), days: 189, weeks: 27 }, { value: "12m", label: T("events.range.1y"), days: 371, weeks: 53 }];
const METRICS = [{ value: "changes", label: T("events.col.changes") }, { value: "runs", label: T("events.view.runs") }, { value: "failed", label: T("events.titles.failures") }];

export const dayRange = (index) => ({ since: index * DAY - TZ, until: index * DAY - TZ + DAY - 1 });
export const dayLabel = (index) => dateOfIndex(index).toLocaleDateString(LOC(), { weekday: "short", day: "numeric", month: "short" });

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
    <section class="ev-activity${collapsed ? " collapsed" : ""}" aria-label="${TH("events.titles.syncActivity")}">
      <div class="ev-activity-head">
        <div class="ev-activity-title"><strong>${TH("events.titles.syncActivity")}</strong><span class="ev-activity-sum" id="ev-act-sum"></span></div>
        <div class="ev-activity-tools">
          <div class="ev-seg" role="group" aria-label="${TH("events.titles.show")}" id="ev-act-metric">${METRICS.map((m) => `<button type="button" class="ev-seg-btn${m.value === metric ? " on" : ""}" data-v="${m.value}" aria-pressed="${m.value === metric}">${esc(m.label)}</button>`).join("")}</div>
          <label class="ev-act-range"><span class="ev-sr">${TH("events.titles.period")}</span><select id="ev-act-range" class="cw-page-input" data-cw-native-select="true">${RANGES.map((r) => `<option value="${r.value}"${r.value === range ? " selected" : ""}>${esc(r.label)}</option>`).join("")}</select></label>
        </div>
        <button type="button" class="ev-icon-btn ev-act-toggle" id="ev-act-toggle" aria-expanded="${!collapsed}" aria-label="${collapsed ? T("events.titles.showActivity") : T("events.titles.hideActivity")}"><span class="material-symbols-rounded" aria-hidden="true">${collapsed ? "expand_more" : "expand_less"}</span></button>
      </div>
      <div class="ev-activity-body"${collapsed ? " hidden" : ""}>
        <div class="ev-hm-wrap"><div class="ev-hm-scroll" tabindex="0" aria-label="${TH("events.titles.calendarLabel")}"><div id="ev-hm"></div></div>
          <div class="ev-hm-foot"><span>${TH("events.titles.pickDayHint")}</span><span class="ev-hm-legend"><span>${TH("events.titles.less")}</span><i class="lv0"></i><i class="lv1"></i><i class="lv2"></i><i class="lv3"></i><i class="lv4"></i><span>${TH("events.titles.more")}</span><i class="lv0 bad"></i><span>${TH("events.titles.hadFailure")}</span></span></div>
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
      [fmt(totals.runs), T("events.view.runs")],
      [fmt(totals.changes), T("events.col.changes")],
      [fmt(totals.failed), T("events.titles.failedRuns"), totals.failed ? " bad" : ""],
      [last != null ? dateOfIndex(last).toLocaleDateString(LOC(), { day: "numeric", month: "short" }) : "–", T("events.titles.lastActive")],
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
        cells.push(`<rect class="${cls}" x="${PAD_L + w * STEP}" y="${PAD_T + d * STEP}" width="${CELL}" height="${CELL}" rx="3.5" data-d="${index}" role="button" tabindex="${index === (selected ?? today) ? 0 : -1}" aria-pressed="${index === selected}" aria-label="${esc(`${dayLabel(index)}: ${T(bad ? "events.titles.cellFailed" : "events.titles.cell", {runs: Number(entry?.runs || 0), changes: Number(entry?.changes || 0)})}`)}"></rect>`);
      }
    }
    const axis = [0, 2, 4].map((d) => `<text class="ev-hm-ax" x="0" y="${PAD_T + d * STEP + 10}">${WEEKDAYS[d]}</text>`).join("");
    const width = PAD_L + cfg.weeks * STEP;
    const height = PAD_T + 7 * STEP;
    Q("#ev-hm").innerHTML = `<svg class="ev-hm-svg" viewBox="0 0 ${width} ${height}" width="${width}" height="${height}" role="group" aria-label="${TH("events.titles.calendarAria")}">${months.join("")}${axis}${cells.join("")}</svg>`;
    Q("#ev-hm").querySelectorAll(".ev-hm-cell").forEach((cell) => {
      const index = Number(cell.dataset.d);
      cell.addEventListener("mousemove", (ev) => {
        const e = byIndex.get(index);
        tip.replaceChildren();
        const head = document.createElement("div");
        head.className = "ev-hm-tip-d";
        head.textContent = dateOfIndex(index).toLocaleDateString(LOC(), { weekday: "long", day: "numeric", month: "long", year: "numeric" });
        const body = document.createElement("div");
        body.textContent = e?.runs ? `${T("events.count.run", {count: e.runs})} · ${T("events.titles.changes", {count: e.changes})}${e.failed ? ` · ${T("events.titles.failedCount", {count: e.failed})}` : ""}` : T("events.titles.noSyncRuns");
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
    btn.setAttribute("aria-label", collapsed ? T("events.titles.showActivity") : T("events.titles.hideActivity"));
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
  return n ? new Date(n * 1000).toLocaleTimeString(LOC(), { hour: "2-digit", minute: "2-digit" }) : "--:--";
};
const runDur = (s) => {
  const n = Number(s || 0);
  if (!Number.isFinite(n) || n <= 0) return "";
  if (n < 60) return `${n}s`;
  if (n < 3600) return `${Math.floor(n / 60)}m ${n % 60}s`;
  return `${(n / 3600).toFixed(1)}h`;
};
const RUN_PHRASES = new Set(["history", "ratings", "watchlist", "progress", "collection"]);

const runRoutes = (run) => {
  const routes = new Map();
  for (const r of run.pair_rows || []) {
    const key = `${r.src_provider}|${r.src_instance || ""}|${r.dst_provider}|${r.dst_instance || ""}`;
    const entry = routes.get(key) || { src: r.src_provider, srcInst: r.src_instance, dst: r.dst_provider, dstInst: r.dst_instance, parts: [] };
    const known = RUN_PHRASES.has(r.feature);
    const feature = String(FEATURE_LABEL[r.feature] || r.feature || "").toLowerCase();
    for (const k of ["added", "removed", "updated"]) {
      const n = Number(r[k] || 0);
      if (n) entry.parts.push({ text: known ? T(`events.run.${r.feature}.${k}`, {count: n}) : T(`events.run.other.${k}`, {count: n, feature}) });
    }
    const errors = Number(r.errors || 0);
    if (errors) entry.parts.push({ text: feature ? T("events.run.failedIn", {count: errors, feature}) : T("events.titles.failedCount", {count: errors}), bad: true });
    routes.set(key, entry);
  }
  return [...routes.values()].filter((e) => e.parts.length);
};

const routeLineHTML = (route) => `<span class="ev-rpair"><span class="ev-rroute">${providerChipHTML(route.src, route.srcInst)}<span class="material-symbols-rounded ev-tarrow" aria-hidden="true">arrow_forward</span>${providerChipHTML(route.dst, route.dstInst)}</span><span class="ev-rsum">${route.parts.map((p) => `<span${p.bad ? ` class="bad"` : ""}>${esc(p.text)}</span>`).join("")}</span></span>`;

const RUN_TITLE_LIMIT = 10;

const runTitleHTML = (item, tmdbOn) => `<button type="button" class="ev-rtitle" data-tid="${esc(item.id)}">${coverHTML(item, tmdbOn)}<span class="ev-rtitle-main"><span class="ev-rtitle-name">${esc(titleText(item))}</span><span class="ev-tsub">${esc([subText(item), whatOf(item)].filter(Boolean).join(" · "))}</span><span class="ev-tdests">${(item.destinations || []).map(destChipHTML).join("")}</span></span></button>`;

const endpointKey = (provider, instance) => `${String(provider || "").toUpperCase()}|${String(instance || "default").toLowerCase()}`;

export function runGroups(runs) {
  const groups = new Map();
  const loose = { key: "", runs: [], features: new Set(), dirs: new Set() };
  for (const run of runs) {
    const entries = new Map();
    const rows = [...(run.pair_rows || []), ...(run.plans || []).map((p) => ({ ...p, plan: true }))];
    for (const r of rows) {
      const a = endpointKey(r.src_provider, r.src_instance);
      const b = endpointKey(r.dst_provider, r.dst_instance);
      const key = [a, b].sort().join(">");
      let group = groups.get(key);
      if (!group) groups.set(key, group = { key, src: r.src_provider, srcInst: r.src_instance, dst: r.dst_provider, dstInst: r.dst_instance, dirs: new Set(), features: new Set(), runs: [] });
      group.dirs.add(`${a}>${b}`);
      if (r.feature) group.features.add(r.feature);
      const entry = entries.get(key) || { run, rows: [] };
      if (!r.plan) entry.rows.push(r);
      entries.set(key, entry);
    }
    if (!entries.size) loose.runs.push({ run, rows: [] });
    for (const [key, entry] of entries) groups.get(key).runs.push(entry);
  }
  const out = [...groups.values()];
  if (loose.runs.length) out.push(loose);
  for (const group of out) {
    for (const entry of group.runs) {
      const loneRun = !group.key;
      entry.changes = loneRun ? Number(entry.run.changes || 0) : entry.rows.reduce((n, r) => n + Number(r.added || 0) + Number(r.removed || 0) + Number(r.updated || 0), 0);
      entry.errors = loneRun ? Number(entry.run.errors || 0) : entry.rows.reduce((n, r) => n + Number(r.errors || 0), 0);
      if (!entry.errors && Number(entry.run.errors || 0) && !(entry.run.pair_rows || []).some((r) => Number(r.errors || 0))) entry.errors = Number(entry.run.errors);
      entry.active = entry.changes > 0 || entry.errors > 0 || entry.run.status === "running";
    }
    const latest = (list) => Math.max(0, ...list.map((e) => Number(e.run.started_at || 0)));
    group.lastActive = latest(group.runs.filter((e) => e.active));
    group.last = latest(group.runs);
  }
  return out.sort((x, y) => (!x.key) - (!y.key) || y.lastActive - x.lastActive || y.last - x.last);
}

const groupRouteHTML = (group) => group.key
  ? `<span class="ev-rroute">${providerChipHTML(group.src, group.srcInst)}<span class="material-symbols-rounded ev-tarrow" aria-hidden="true">${group.dirs.size > 1 ? "sync_alt" : "arrow_forward"}</span>${providerChipHTML(group.dst, group.dstInst)}</span>`
  : `<span class="ev-rwhat">${TH("events.titles.otherRuns")}</span>`;

const entryParts = (group, entry) => {
  const routes = runRoutes({ pair_rows: entry.rows });
  const named = group.dirs.size > 1;
  return routes.flatMap((route) => route.parts.map((p) => ({ ...p, text: named ? `${providerLabel(route.dst)}: ${p.text}` : p.text })));
};

const childRowHTML = (group, entry, selected, selectedPair) => {
  const { run } = entry;
  const status = entry.errors ? "error" : run.status === "running" ? "info" : entry.changes ? "ok" : "idle";
  const parts = entry.active ? entryParts(group, entry) : [];
  const what = parts.length ? parts.map((p) => `<span${p.bad ? ` class="bad"` : ""}>${esc(p.text)}</span>`).join("")
    : esc(entry.errors ? T("events.titles.failedCount", {count: entry.errors}) : entry.changes ? T("events.titles.changes", {count: entry.changes}) : run.status === "running" ? T("events.running") : T("events.changes.none"));
  return `<div class="ev-rrow ev-rchild${String(selected) === String(run.run_id) && (!selectedPair || selectedPair === group.key) ? " sel" : ""}" data-run="${esc(run.run_id)}" data-pair="${esc(group.key)}" tabindex="0" role="button">
      <span class="ev-rbranch" aria-hidden="true"></span>
      <span class="ev-rtime">${esc(runTime(run.started_at))}</span>
      <span class="ev-rdot ${status}" aria-hidden="true"></span>
      <span class="ev-rmain"><span class="ev-rsum${entry.active ? " on" : ""}">${what}${run.dry_run ? `<span>dry run</span>` : ""}</span></span>
      <span class="ev-rdur">${esc(runDur(run.duration))}</span>
    </div>`;
};

export function runRowsHTML(runs, { selected = "", selectedPair = "", open = new Set(), openAll = false } = {}) {
  if (!runs.length) return `<div class="ev-empty ev-empty-inline">${TH("events.titles.noRunsDay")}</div>`;
  return runGroups(runs).map((group) => {
    const active = group.runs.filter((e) => e.active);
    const quiet = group.runs.filter((e) => !e.active);
    const features = [...group.features].map((f) => FEATURE_LABEL[f] || f).join(", ");
    const count = T("events.count.run", {count: group.runs.length});
    const meta = [features, active.length ? count : T("events.titles.runsNoChanges", {count: group.runs.length})].filter(Boolean).join(" · ");
    const shown = openAll || open.has(group.key);
    const quietLine = quiet.length ? `<button type="button" class="ev-rquiet" data-group="${esc(group.key)}" aria-expanded="${shown}">
        <span class="ev-rbranch" aria-hidden="true"></span>
        <span class="ev-rquiet-text">${esc(T("events.titles.runsWithoutChanges", {count: quiet.length}))}</span>
        <span class="ev-rquiet-act">${shown ? T("events.titles.hide") : T("events.titles.show")}</span>
      </button>` : "";
    return `<div class="ev-rgroup">
      <div class="ev-rghead" data-run="${esc(group.runs[0].run.run_id)}" data-pair="${esc(group.key)}" tabindex="0" role="button">${groupRouteHTML(group)}<span class="ev-rgmeta">${esc(meta)}</span></div>
      ${active.map((e) => childRowHTML(group, e, selected, selectedPair)).join("")}
      ${quietLine}
      ${shown ? `<div class="ev-rquiet-list">${quiet.map((e) => childRowHTML(group, e, selected, selectedPair)).join("")}</div>` : ""}
    </div>`;
  }).join("");
}

export function renderRunDetail(host, run, ctx) {
  const groups = runGroups([run]).filter((g) => g.key);
  const pair = groups.find((g) => g.key === ctx.pair);
  const entry = pair?.runs[0];
  const scopedRun = entry ? {...run, pair_rows: entry.rows, changes: entry.changes, errors: entry.errors} : run;
  const routes = runRoutes(scopedRun);
  const lead = pair || groups.find((g) => g.runs[0]?.active) || groups[0];
  const single = (pair || groups.length === 1) && routes.length <= 1;
  const result = single && routes.length ? routes[0].parts.map((p) => p.text).join(", ")
    : scopedRun.changes ? T("events.titles.changes", {count: scopedRun.changes}) : run.status === "running" ? T("events.running") : T("events.changes.none");
  const failed = scopedRun.errors && !(single && routes.length) ? T("events.titles.failedCount", {count: scopedRun.errors}) : "";
  const outcome = [result, failed, runDur(run.duration) ? T("events.titles.took", {duration: runDur(run.duration)}) : "", run.dry_run ? T("events.titles.dryRun") : ""].filter(Boolean).join(" · ");
  const summary = !single && routes.length ? `<div class="ev-rsums">${routes.map(routeLineHTML).join("")}</div>` : "";
  const status = scopedRun.errors ? "error" : run.status === "running" ? "info" : "ok";
  host.innerHTML = `
    <div class="ev-dhead">
      <div class="ev-dhead-row"><span class="ev-badge ${status}">${esc(scopedRun.errors ? T("events.titles.hadFailures") : run.status === "running" ? T("events.running") : T("events.k.finished"))}</span><span class="ev-dhead-spacer"></span><span class="ev-dtime">${esc(fullTime(run.started_at))}</span></div>
      <div class="ev-dtitle ev-rdtitle">${lead ? groupRouteHTML(lead) : T("events.titles.syncRun")}${!pair && groups.length > 1 ? `<span class="ev-rdmore">${esc(T("events.titles.more", {count: groups.length - 1}))}</span>` : ""}</div>
      <p class="ev-outcome">${esc(outcome.charAt(0).toUpperCase() + outcome.slice(1))}</p>
      ${groups.length > 1 ? `<div class="ev-path-acts" role="group" aria-label="${TH("events.titles.pairsInRun")}"><button type="button" class="ev-tbtn" data-run-pair="" aria-pressed="${!pair}">${TH("events.titles.allPairs")}</button>${groups.map(g => `<button type="button" class="ev-tbtn" data-run-pair="${esc(g.key)}" aria-pressed="${g === pair}">${groupRouteHTML(g)}</button>`).join("")}</div>` : ""}
    </div>
    ${summary}
    <h4>${TH("events.view.titles")} <span class="ev-h4-note" id="ev-run-total"></span></h4>
    <div class="ev-rtitles" id="ev-run-list"><div class="ev-empty ev-empty-inline">${TH("events.titles.loadingTitles")}</div></div>
    <div class="ev-path-acts">
      <button type="button" class="ev-tbtn" id="ev-run-titles" hidden><span class="material-symbols-rounded" aria-hidden="true">movie</span><span>${TH("events.titles.showAll")}</span></button>
      <button type="button" class="ev-tbtn" id="ev-run-logs"><span class="material-symbols-rounded" aria-hidden="true">terminal</span><span>${TH("events.viewLogs")}</span></button>
    </div>`;
  host.querySelector("#ev-run-titles")?.addEventListener("click", () => ctx.showTitles?.(run.run_id));
  host.querySelector("#ev-run-logs")?.addEventListener("click", () => window.openLogs?.({ runId: run.run_id, channel: "sync" }));
  host.querySelectorAll("[data-run-pair]").forEach(button => button.addEventListener("click", () => ctx.selectPair?.(button.dataset.runPair)));
  void loadRunTitles(host, scopedRun, {...ctx, pair: pair?.key || ""});
}

async function loadRunTitles(host, run, ctx) {
  const listEl = host.querySelector("#ev-run-list");
  if (!listEl || !ctx.fetchJson) return;
  const query = (extra = "") => ctx.fetchJson(`/api/events/titles?domain=sync&run_id=${encodeURIComponent(run.run_id)}&limit=${RUN_TITLE_LIMIT}${ctx.pair ? `&pair=${encodeURIComponent(ctx.pair)}` : ""}${extra}`);
  try {
    const [all, failed] = ctx.pair && !run.changes && !run.errors ? [{ok: true, items: [], total: 0}, null] : await Promise.all([query(), run.errors ? query("&outcome=failed") : Promise.resolve(null)]);
    if (!listEl.isConnected) return;
    if (!all?.ok) throw new Error("failed");
    const seen = new Set();
    const items = [...(failed?.items || []), ...(all.items || [])].filter((it) => !seen.has(String(it.id)) && seen.add(String(it.id))).slice(0, RUN_TITLE_LIMIT).map(item => {
      if (!ctx.pair) return item;
      const source = endpointKey(item.source_provider, item.source_instance);
      const destination = ctx.pair.split(">").find(endpoint => endpoint !== source);
      return {...item, destinations: (item.destinations || []).filter(d => endpointKey(d.provider, d.instance) === destination)};
    });
    const total = Number(all.total || 0);
    listEl.innerHTML = items.length
      ? items.map((it) => runTitleHTML(it, ctx.tmdbOn)).join("")
      : `<div class="ev-empty ev-empty-inline">${all.preparing ? TH("events.titles.preparing") : TH("events.titles.noneForRun")}</div>`;
    const totalEl = host.querySelector("#ev-run-total");
    if (totalEl && total) totalEl.textContent = total > items.length ? `${fmt(items.length)} of ${fmt(total)}` : fmt(total);
    const more = host.querySelector("#ev-run-titles");
    if (more && total > items.length) {
      more.hidden = false;
      more.querySelector("span:last-child").textContent = ctx.pair ? T("events.titles.showAllInRun") : T("events.titles.showAllCount", {count: total});
    }
    bindCovers(listEl);
    listEl.querySelectorAll(".ev-rtitle").forEach((btn) => btn.addEventListener("click", () => ctx.openTitle?.(run.run_id, btn.dataset.tid)));
  } catch {
    if (listEl.isConnected) listEl.innerHTML = `<div class="ev-empty ev-empty-inline">${TH("events.titles.runTitlesFailed")}</div>`;
  }
}

