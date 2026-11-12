/* assets/js/tracker-month.js */
/* CrossWatch - Artwork-led monthly reviews and interactive watch activity */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import {themes, holidays, readPreferences, savePreferences, resolveTheme, paintTheme, preferenceKey} from "./tracker-month-themes.js";
import {monthNotifications} from "./tracker-month-notifications.js";

const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const paths = {back:"m15 18-6-6 6-6",next:"m9 18 6-6-6-6",play:"m9 5 11 7-11 7Z",close:"m6 6 12 12M6 18 18 6",spark:"m12 3 2.5 6.5L21 12l-6.5 2.5L12 21l-2.5-6.5L3 12l6.5-2.5Z",clock:"M12 8v5l3 2M22 12a10 10 0 1 1-20 0 10 10 0 0 1 20 0",movie:"M3 5h18v15H3ZM3 9h18M8 5l3 4m4-4 3 4",tv:"M3 5h18v13H3Zm5 17h8",arrow:"M4 12h16m-6-6 6 6-6 6"};
const icon = name => `<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="${paths[name] || paths.spark}"/></svg>`;
const fmtMonth = month => new Date(`${month}-02T12:00:00`).toLocaleDateString(undefined,{month:"long"});
const duration = minutes => minutes ? `${Math.floor(minutes / 60) ? `${Math.floor(minutes / 60)}h ` : ""}${minutes % 60 ? `${minutes % 60}m` : ""}`.trim() : "—";
const count = (n, word) => `${n.toLocaleString()} ${word}${n === 1 ? "" : "s"}`;
const cache = new Map();
let active = null;
const notifications = monthNotifications(() => profileContext().scope);

function profileContext() {
  const auth = window.CW?.AuthState?.read?.() || {};
  const view = window.CW?.OverviewProfile;
  const viewed = auth.isAdmin && view?.id ? view.profile : null;
  const user = viewed || auth.user || {};
  let avatar = "";
  try {const url = new URL(user.avatar_url || "", location.origin); if (user.avatar_url && url.origin === location.origin) avatar = url.href;} catch {}
  return {name:String(user.display_name || user.label || user.username || ""), avatar,
    scope:JSON.stringify([auth.user?.id || auth.user?.username,auth.profileId,auth.role,auth.permissions,viewed ? view.id : ""])};
}

function art(row, type = "backdrop", eager = false) {
  const url = row?.[type] || row?.[type === "poster" ? "backdrop" : "poster"];
  if (!url || !/^https:\/\/image\.tmdb\.org\/t\/p\//.test(url)) return `<span class="mr-art-fallback" aria-hidden="true">${icon(row?.type === "movie" ? "movie" : "tv")}</span>`;
  const sized = url.replace(/\/t\/p\/[^/]+\//, `/t/p/${type === "poster" ? "w342" : "w1280"}/`);
  return `<img src="${esc(sized)}" alt="" loading="${eager ? "eager" : "lazy"}" decoding="async"${eager ? ' fetchpriority="high"' : ""}>`;
}

export function openMonthReview({root, instance, label, openMedia, onClose, month = "", returnLabel = "Overview"}) {
  active?.close(false);
  const profile = profileContext(), trackerLabel = label;
  label = profile.name || label;
  const cacheKey = value => JSON.stringify([profile.scope,instance,value]);
  const previousFocus = document.activeElement, scroll = window.scrollY;
  const host = document.createElement("section");
  host.className = "mr-page";
  host.setAttribute("aria-label","Month in review");
  root.append(host);
  root.classList.add("is-month-review");
  let report = null, response = null, filter = "all", genre = "", selectedDay = 0, expandedDay = false, shown = 3;
  let prefs = readPreferences(instance), effects = prefs.effects;
  let controller = null, timer = 0, request = 0, frame = 0, attempts = 0;
  const observer = new IntersectionObserver(entries => entries.forEach(entry => {
    entry.target.classList.toggle("is-onscreen",entry.isIntersecting);
    if (entry.isIntersecting) entry.target.classList.add("is-in");
  }), {threshold:.06});
  const hidden = () => {host.classList.toggle("mr-paused",document.hidden);};
  document.addEventListener("visibilitychange",hidden);
  const changed = () => {if (profileContext().scope !== profile.scope) {cache.clear(); close(false);}};
  window.addEventListener("cw:auth-state-changed",changed);
  window.addEventListener("cw:overview-profile-changed",changed);
  const gone = new MutationObserver(() => {if (root.hidden || root.classList.contains("hidden")) close(false);});
  gone.observe(root,{attributes:true,attributeFilter:["class","hidden"]});
  const restyle = event => {
    if (event.type === "storage" && event.key !== preferenceKey(instance) || event.type !== "storage" && event.detail?.key !== preferenceKey(instance)) return;
    const focused=document.activeElement;
    const attribute=["data-mr-effects","data-mr-region","data-mr-thanksgiving","data-mr-theme","data-mr-holiday"].find(name=>focused?.hasAttribute(name));
    const value=attribute ? focused.getAttribute(attribute) : "";
    render();
    if (attribute) host.querySelector(`[${attribute}="${value}"]`)?.focus({preventScroll:true});
  };
  window.addEventListener("cw:month-theme-changed",restyle);
  window.addEventListener("storage",restyle);
  const api = active = {close};

  function close(restore = true) {
    if (!response || response.pending || response.stale || response.error === "review_timeout") notifications.leave(instance,month,trackerLabel,profile.scope);
    request++; controller?.abort(); clearTimeout(timer); cancelAnimationFrame(frame);
    observer.disconnect(); gone.disconnect(); document.removeEventListener("visibilitychange",hidden);
    window.removeEventListener("cw:auth-state-changed",changed);
    window.removeEventListener("cw:overview-profile-changed",changed);
    window.removeEventListener("cw:month-theme-changed",restyle); window.removeEventListener("storage",restyle);
    host.remove(); root.classList.remove("is-month-review");
    if (active === api) active = null;
    onClose?.(restore);
    if (restore) {window.scrollTo({top:scroll,behavior:"instant"}); previousFocus?.focus({preventScroll:true});}
  }

  function controls() {
    return `<nav class="mr-nav" aria-label="Monthly review"><button type="button" data-mr="back" class="mr-back">${icon("back")}<span>${esc(returnLabel)}</span></button><div class="mr-month-switch"><button type="button" data-mr="previous" aria-label="Previous month"${month <= "1900-01" || !month ? " disabled" : ""}>${icon("back")}</button><label class="mr-month-input"><span class="mr-sr">Choose month</span><input type="month" value="${esc(month)}" min="1900-01" max="${esc(response?.current_month || "")}" aria-label="Choose month"></label><button type="button" data-mr="next" aria-label="Next month"${!month || month >= (response?.current_month || month) ? " disabled" : ""}>${icon("next")}</button></div><details class="mr-options"><summary>${icon("spark")}<span>Atmosphere</span></summary><div><label>Theme<select data-cw-native-select="true" data-mr-theme aria-label="Month theme"><option value="auto">Automatic</option>${themes.map(t=>`<option value="${t.id}"${prefs.overrides[month] === t.id ? " selected" : ""}>${esc(t.name)}</option>`).join("")}</select></label><p class="mr-theme-caption">${esc(resolveTheme(month,prefs).name)} &middot; banner and poster</p><label>Seasons<select data-cw-native-select="true" data-mr-region aria-label="Hemisphere"><option value="north"${prefs.hemisphere === "north" ? " selected" : ""}>Northern hemisphere</option><option value="south"${prefs.hemisphere === "south" ? " selected" : ""}>Southern hemisphere</option></select></label><label><input type="checkbox" data-mr-effects${effects ? " checked" : ""}> Seasonal effects</label><details class="mr-holiday-options"><summary>Automatic holidays</summary><div class="mr-holiday-grid">${holidays.map(t=>`<label><input type="checkbox" data-mr-holiday="${t.id}"${prefs.enabled.includes(t.id) ? " checked" : ""}>${esc(t.name)}</label>`).join("")}</div><label>Thanksgiving<select data-cw-native-select="true" data-mr-thanksgiving aria-label="Thanksgiving calendar"><option value="us"${prefs.thanksgiving === "us" ? " selected" : ""}>United States</option><option value="canada"${prefs.thanksgiving === "canada" ? " selected" : ""}>Canada</option></select></label><p class="mr-theme-caption">Observances can vary locally. Pick a theme above for your month.${prefs.enabled.includes("diwali") && ![2024,2025,2026,2027].includes(Number(month.slice(0,4))) ? " Diwali needs a manual choice for this year." : ""}</p></details></div></details></nav>`;
  }

  function theme() {
    prefs = readPreferences(instance); effects = prefs.effects;
    paintTheme(host,month,instance);
  }

  function hero() {
    const top = report.titles.slice(0,5), year = month.slice(0,4), m = Number(month.slice(5));
    return `<header class="mr-poster mr-section"><div class="mr-poster-bg">${top[0] ? art(top[0],"backdrop",true) : ""}</div><div class="mr-ambient" aria-hidden="true"></div><div class="mr-decoration"></div><div class="mr-dust" aria-hidden="true">${Array.from({length:12},(_,i)=>`<i style="--i:${i};--x:${(i * 37 + 9) % 100}%"></i>`).join("")}</div><div class="mr-poster-heading"><span class="mr-eyebrow">CROSSWATCH · ${report.current ? "MONTH SO FAR" : "MONTH IN REVIEW"}</span><span class="mr-year">${year}</span><h1 tabindex="-1">${esc(fmtMonth(month))}<span class="mr-title-dot">.</span></h1><p class="mr-byline"><span class="mr-person">${profile.avatar ? `<img class="mr-avatar" src="${esc(profile.avatar)}" alt="" width="48" height="48">` : ""}<strong>${esc(label)}</strong></span><span>${count(report.plays,"play")} · ${report.active_days} active ${report.active_days === 1 ? "day" : "days"}${profile.name && trackerLabel !== label ? ` &middot; ${esc(trackerLabel)} tracker` : ""}</span></p></div><div class="mr-collage">${top.map((row,i)=>`<button type="button" class="mr-cover" style="--i:${i};--angle:${[-9,-4,0,5,10][i]}deg" data-mr-title="${esc(row.key)}" aria-label="Open ${esc(row.title)}">${art(row,"poster",i < 3)}<span class="mr-cover-caption">${esc(row.title)}<small>${count(row.plays,"play")}</small></span></button>`).join("")}</div><div class="mr-poster-foot"><span>${report.current ? "Still rolling" : "A month in pictures"}</span><span>${String(m).padStart(2,"0")} / ${year}</span></div></header>`;
  }

  function summary() {
    return `<section class="mr-totals mr-section" aria-label="Month totals">${[[report.plays,"Plays","play"],[duration(report.minutes),"Estimated watch time","clock"],[report.movies,"Movies","movie"],[report.shows,"Shows","tv"],[`${report.streak}d`,"Longest streak","spark"]].map(([value,title,glyph])=>`<div>${icon(glyph)}<strong>${value}</strong><span>${title}</span></div>`).join("")}</section>`;
  }

  const selectedTitles = () => report.titles.filter(row => (filter === "all" || row.type === filter) && (!genre || row.genres.includes(genre)));
  function activityDays() {
    return report.days.map(day => ({...day, value:filter === "movie" ? day.movies : filter === "show" ? day.episodes : day.movies + day.episodes, time:filter === "movie" ? day.movie_minutes : filter === "show" ? day.show_minutes : day.minutes}));
  }

  function sparkline(values, id) {
    const max = Math.max(1,...values), points = values.map((v,i)=>`${(i * 600 / Math.max(1,values.length-1)).toFixed(1)},${(90-v*72/max).toFixed(1)}`).join(" ");
    return `<svg class="mr-sparkline" viewBox="0 0 600 100" preserveAspectRatio="none" aria-hidden="true"><defs><linearGradient id="mr-gradient-${id}" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="var(--mr-b)" stop-opacity=".5"/><stop offset="1" stop-color="var(--mr-b)" stop-opacity="0"/></linearGradient></defs><polygon points="0,100 ${points} 600,100" fill="url(#mr-gradient-${id})"/><polyline points="${points}" fill="none" stroke="var(--mr-b)" stroke-width="2" vector-effect="non-scaling-stroke" pathLength="1"/></svg>`;
  }

  function activity() {
    const days = activityDays(), max = Math.max(1,...days.map(d=>d.value)), total = days.reduce((n,d)=>n+d.value,0), minutes = days.reduce((n,d)=>n+d.time,0);
    const titles = report.titles.filter(t=>filter === "all" || t.type === filter).length;
    const best = days.reduce((a,b)=>a.value >= b.value ? a : b);
    const subject = filter === "all" ? "titles" : filter === "movie" ? "movies" : "shows";
    return `<section class="mr-activity mr-section" aria-labelledby="mr-activity-title"><div class="mr-section-top"><span class="mr-eyebrow">THE RHYTHM OF YOUR MONTH</span><div class="mr-segments" role="group" aria-label="Activity type">${[["all","All"],["movie","Movies"],["show","Shows"]].map(([id,name])=>`<button type="button" data-mr-filter="${id}" aria-pressed="${filter === id}">${name}</button>`).join("")}</div></div><div class="mr-activity-main"><div class="mr-activity-copy"><h2 id="mr-activity-title">${duration(minutes)}<span>across ${titles} ${subject}</span></h2><p>${total ? `<strong>${esc(fmtMonth(month))} ${best.day}</strong> was your busiest day.<br>${count(best.value,"play")}.` : "No watches in this selection."}</p></div><div class="mr-chart" role="group" aria-label="Daily plays. Select a day to see its titles.">${days.map(day=>`<button type="button" class="mr-bar${selectedDay === day.day ? " is-selected" : ""}" data-mr-day="${day.day}" aria-pressed="${selectedDay === day.day}" aria-label="${esc(fmtMonth(month))} ${day.day}: ${count(day.value,"play")}${day.time ? `, ${duration(day.time)}` : ""}" style="--bar:${day.value * 100 / max}%;--i:${day.day}"><span class="mr-bar-track"><i></i></span><span class="mr-bar-day">${day.day}</span><span class="mr-tooltip" role="tooltip"><strong>${count(day.value,"play")}</strong>${esc(fmtMonth(month))} ${day.day}${day.time ? `<small>${duration(day.time)}</small>` : ""}</span></button>`).join("")}</div></div><label class="mr-day-picker">Explore a day<select data-mr-day-select aria-label="Choose a day"><option value="0">Choose a day</option>${days.map(day=>`<option value="${day.day}"${selectedDay === day.day ? " selected" : ""}>${esc(fmtMonth(month))} ${day.day} ? ${count(day.value,"play")}</option>`).join("")}</select></label><div data-mr-day-panel>${dayPanel()}</div><div class="mr-trends"><div><div class="mr-trend-title"><strong>${duration(minutes)}</strong>${icon("clock")}</div>${sparkline(days.map(d=>d.time),"time")}<dl><div><dt>Hours per week</dt><dd>${(minutes/60/report.elapsed_days*7).toFixed(1)}</dd></div><div><dt>Hours per day</dt><dd>${(minutes/60/report.elapsed_days).toFixed(1)}</dd></div></dl></div><div><div class="mr-trend-title"><strong>${count(total,"play")}</strong>${icon("play")}</div>${sparkline(days.map(d=>d.value),"plays")}<dl><div><dt>Plays per week</dt><dd>${(total/report.elapsed_days*7).toFixed(1)}</dd></div><div><dt>Plays per day</dt><dd>${(total/report.elapsed_days).toFixed(1)}</dd></div></dl></div></div><p class="mr-coverage">Watch time is estimated from known runtimes (${report.timed} of ${report.plays} plays). Dates use ${esc(report.timezone)}.</p></section>`;
  }

  function dayPanel() {
    if (!selectedDay) return "";
    const day = report.days[selectedDay - 1], rows = report.titles.filter(row=>day.titles.includes(row.key) && (filter === "all" || row.type === filter));
    return `<div class="mr-day-panel"><div class="mr-section-top"><h3>${esc(fmtMonth(month))} ${selectedDay}</h3><button type="button" data-mr="clear-day" aria-label="Close selected day">${icon("close")}</button></div>${rows.length ? `<div class="mr-day-titles">${rows.slice(0,expandedDay ? rows.length : 4).map(row=>`<button type="button" data-mr-title="${esc(row.key)}">${art(row,"poster")}<span>${esc(row.title)}<small>${row.type === "show" ? "TV series" : "Movie"}</small></span>${icon("arrow")}</button>`).join("")}</div>${rows.length > 4 ? `<button class="mr-text-button" data-mr="more-day">${expandedDay ? "Show less" : `Show all ${rows.length} titles`}</button>` : ""}` : `<p>No ${filter === "movie" ? "movies" : filter === "show" ? "episodes" : "watches"} on this day.</p>`}</div>`;
  }

  function bookends() {
    return `<section class="mr-bookends mr-section" aria-label="First and last watches">${[[report.first,"Opening scene","First watch"],[report.last,"Closing scene",report.current ? "Latest watch" : "Last watch"]].map(([event,heading,label])=>{
      const row = report.titles.find(t=>t.key === event?.title_key); if (!row) return "";
      const date = new Date(`${event.date.slice(0,10)}T12:00:00Z`).toLocaleDateString(undefined,{month:"short",day:"numeric",timeZone:"UTC"});
      return `<button type="button" class="mr-bookend" data-mr-title="${esc(row.key)}">${art(row)}<span class="mr-bookend-top"><span class="mr-eyebrow">${heading}</span>${icon("arrow")}</span><span class="mr-bookend-copy"><small>${label} · ${esc(date)}${event.episode ? ` · S${event.season} E${event.episode}` : ""}</small><strong>${esc(row.title)}</strong></span></button>`;
    }).join("")}</section>`;
  }

  function ranked(kind) {
    const rows = selectedTitles().filter(t=>t.type === kind);
    if (!rows.length) return "";
    return `<section class="mr-ranked mr-section" aria-label="${kind === "show" ? "Most watched shows" : "Movies watched"}"><div class="mr-section-top"><h2>${kind === "show" ? "Most watched shows" : "Movies watched"}</h2><span class="mr-count">${rows.length} ${kind === "show" ? "shows" : "movies"}</span></div><div class="mr-rank-grid">${rows.slice(0,shown).map((row,i)=>`<button type="button" class="mr-rank-card" data-mr-title="${esc(row.key)}" style="--i:${i};--rank-hue:${(i*37 + Number(month.slice(5))*23)%360}"><span class="mr-rank-backdrop">${art(row)}</span><span class="mr-rank-number">${String(i+1).padStart(2,"0")}</span><span class="mr-rank-copy"><span class="mr-eyebrow">${count(row.plays,"play")}${row.minutes ? ` <b>·</b> ${duration(row.minutes)}` : ""}</span><strong>${esc(row.title)}</strong><span>${kind === "show" ? count(row.episodes,"episode") : esc(row.year || "Movie")}${row.genres[0] ? ` · ${esc(row.genres[0])}` : ""}</span></span><span class="mr-rank-poster">${art(row,"poster")}</span><span class="mr-rank-open">Open media card ${icon("arrow")}</span></button>`).join("")}</div>${rows.length > shown ? `<button type="button" class="mr-text-button" data-mr="more-titles">Show more ${kind === "show" ? "shows" : "movies"}${icon("next")}</button>` : ""}</section>`;
  }

  function genres() {
    if (!report.genres.length) return "";
    const max = Math.max(...report.genres.map(g=>g.count));
    return `<section class="mr-genres mr-section"><div class="mr-section-top"><div><span class="mr-eyebrow">YOUR MONTH, IN COLOUR</span><h2>A little of everything.<br>Or a lot of ${esc(report.genres[0].name.toLowerCase())}.</h2></div><span>${report.genres.length} genres</span></div><div class="mr-genre-bars">${report.genres.map((g,i)=>`<button type="button" data-mr-genre="${esc(g.name)}" aria-pressed="${genre === g.name}" style="--genre-size:${Math.max(30,g.count*100/max)}%;--genre-hue:${(i*47 + Number(month.slice(5))*23)%360}"><strong>${esc(g.name)}</strong><span>${count(g.count,"title")}</span><i></i></button>`).join("")}</div><p class="mr-coverage">Genres count unique titles with cached genre data. A title can belong to several genres.</p></section>`;
  }

  function render(keepFocus = false) {
    if (response?.disabled) {
      observer.disconnect();
      host.innerHTML = '<div class="mr-empty mr-section"><h1 tabindex="-1">Month in review is off</h1><p>Enable Month in review reports in this tracker instance’s settings.</p><button type="button" data-mr="back">Back to tracker</button></div>';
      return;
    }
    const optionsOpen = host.querySelector(".mr-options")?.open, holidaysOpen = host.querySelector(".mr-holiday-options")?.open;
    const focused = keepFocus ? document.activeElement : null;
    const focusKey = focused?.dataset.mrFilter ? `[data-mr-filter="${focused.dataset.mrFilter}"]` : null;
    observer.disconnect(); theme();
    host.innerHTML = controls() + (report && response?.error ? `<div class="mr-filter-note" role="status">Showing your saved review. The update could not finish.<button type="button" data-mr="retry">Try again</button></div>` : "") + (report ? report.plays ? `${hero()}${summary()}${activity()}${bookends()}${genre ? `<div class="mr-filter-note">Showing ${esc(genre)}<button type="button" data-mr="clear-genre">Clear ${icon("close")}</button></div>` : ""}<div data-mr-rankings>${ranked("show")}${ranked("movie")}</div>${genres()}<footer class="mr-footer"><span class="mr-eyebrow">${esc(fmtMonth(month))} ${month.slice(0,4)} · ${esc(label)}</span><strong>That was your month<span>.</span></strong><button type="button" data-mr="back">Back to ${esc(returnLabel)} ${icon("arrow")}</button><small>${response?.pending ? "Updating this review in the background…" : response?.error ? "Showing the saved review. The update could not finish." : "Built from your CrossWatch history"}</small></footer>` : `<div class="mr-empty mr-section"><span class="mr-eyebrow">${month.slice(0,4)} · ${esc(label)}</span><h1 tabindex="-1">${esc(fmtMonth(month))}<span>.</span></h1><p>No watches recorded for this month.</p><button type="button" data-mr="previous">${icon("back")} Previous month</button></div>` : `<div class="mr-loading" role="status"><span class="mr-orbit">${icon("spark")}</span><h1 tabindex="-1">${response?.error ? "Your month is taking a little longer" : "Preparing your month"}</h1><p>${response?.error ? "Try again in a moment. Your history is unchanged." : "Building the artwork and activity from your saved history."}</p>${response?.error ? '<button type="button" data-mr="retry">Try again</button>' : ""}</div>`);
    theme();
    host.querySelector(".mr-options").open = !!optionsOpen;
    if (host.querySelector(".mr-holiday-options")) host.querySelector(".mr-holiday-options").open = !!holidaysOpen;
    host.querySelectorAll(".mr-section").forEach(el=>observer.observe(el));
    if (focusKey) host.querySelector(focusKey)?.focus({preventScroll:true});
  }

  async function load(next = month, initial = false) {
    if (initial && response && next !== month && (response.pending || response.stale)) notifications.leave(instance,month,trackerLabel,profile.scope);
    notifications.viewing(instance,next);
    clearTimeout(timer); controller?.abort(); controller = new AbortController(); const id = ++request;
    if (initial) {filter = "all"; genre = ""; selectedDay = 0; shown = 3; attempts = 0; report = cache.get(cacheKey(next))?.report || null; response = null; month = next; render();}
    try {
      const res = await fetch(`/api/tracker/overview/month?provider_instance=${encodeURIComponent(instance)}&month=${encodeURIComponent(next)}`,{credentials:"same-origin",cache:"no-store",signal:controller.signal});
      if (id !== request) return;
      if (!res.ok) {if ([401,403,404].includes(res.status)) {cache.clear(); report = null;} throw new Error("review_unavailable");}
      const data = await res.json(); if (id !== request) return;
      const prior = report, previousGeneration = response?.generated, previousPending = response?.pending, previousError = response?.error;
      if (data.disabled) {
        report = null; response = {...data,month}; cache.clear(); notifications.viewing(instance,month);
        render();
        return;
      }
      response = data; month = data.month;
      notifications.viewing(instance,month);
      if (data.report) {report = data.report; cache.set(cacheKey(month),data); while (cache.size > 6) cache.delete(cache.keys().next().value);}
      if (!prior || !report || prior.fingerprint !== report.fingerprint || previousGeneration !== data.generated || previousPending !== data.pending || previousError !== data.error || initial) render();
      if (initial) {window.scrollTo({top:0,behavior:"instant"}); host.querySelector("h1")?.focus({preventScroll:true});}
      if ((data.pending || data.stale) && !data.error && ++attempts < 30) timer = setTimeout(()=>{if (!document.hidden) load(month); else timer = setTimeout(()=>load(month),10000);},Math.min(10000,1500+attempts*500));
      else if ((data.pending || data.stale || !report) && !data.error) {response = {...response,pending:false,error:"review_timeout"}; render();}
    } catch (error) {
      if (id !== request || error.name === "AbortError") return;
      response = {...response,error:"review_unavailable"}; render();
    }
  }

  host.addEventListener("click",event=>{
    const button = event.target.closest("button"); if (!button || button.disabled) return;
    const title = button.dataset.mrTitle;
    if (title) {const row = report.titles.find(t=>t.key === title); if (row) openMedia(row); return;}
    if (button.dataset.mrFilter) {filter = button.dataset.mrFilter; render(true); return;}
    if (button.dataset.mrDay) {selectedDay = Number(button.dataset.mrDay) === selectedDay ? 0 : Number(button.dataset.mrDay); expandedDay = false; host.querySelectorAll("[data-mr-day]").forEach(el=>{const on = Number(el.dataset.mrDay) === selectedDay; el.classList.toggle("is-selected",on); el.setAttribute("aria-pressed",String(on));}); host.querySelector("[data-mr-day-panel]").innerHTML = dayPanel(); host.querySelector("[data-mr-day-select]").value = String(selectedDay); return;}
    if (button.dataset.mrGenre) {genre = genre === button.dataset.mrGenre ? "" : button.dataset.mrGenre; shown = 3; render(); host.querySelector("[data-mr-rankings]")?.scrollIntoView({behavior:window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "instant" : "smooth",block:"start"}); return;}
    const action = button.dataset.mr;
    if (action === "back") close();
    if (action === "previous" || action === "next") {const date = new Date(`${month}-02T12:00:00Z`); date.setUTCMonth(date.getUTCMonth()+(action === "next" ? 1 : -1)); load(date.toISOString().slice(0,7),true);}
    if (action === "retry") load(month,true);
    if (action === "clear-day") {selectedDay = 0; render(); host.querySelector(".mr-bar")?.focus({preventScroll:true});}
    if (action === "clear-genre") {genre = ""; render();}
    if (action === "more-day") {expandedDay = !expandedDay; host.querySelector("[data-mr-day-panel]").innerHTML = dayPanel();}
    if (action === "more-titles") {shown += 6; render();}
  });
  host.addEventListener("change",event=>{
    if (event.target.matches("[data-mr-day-select]")) {const day = Number(event.target.value); selectedDay = 0; if (day) host.querySelector(`[data-mr-day="${day}"]`)?.click(); else render();}
    if (event.target.type === "month" && /^\d{4}-\d{2}$/.test(event.target.value)) load(event.target.value,true);
    const target = event.target;
    if (target.matches("[data-mr-effects]")) savePreferences(instance,{effects:target.checked});
    if (target.matches("[data-mr-region]")) savePreferences(instance,{hemisphere:target.value});
    if (target.matches("[data-mr-thanksgiving]")) savePreferences(instance,{thanksgiving:target.value});
    if (target.matches("[data-mr-theme]")) {const overrides={...prefs.overrides}; if (target.value === "auto") delete overrides[month]; else overrides[month]=target.value; savePreferences(instance,{overrides});}
    if (target.matches("[data-mr-holiday]")) {const enabled=prefs.enabled.filter(id=>id!==target.dataset.mrHoliday); if (target.checked) enabled.unshift(target.dataset.mrHoliday); savePreferences(instance,{enabled});}
  });
  host.addEventListener("keydown",event=>{
    if (event.key === "Escape") {const options = host.querySelector(".mr-options"); if (options?.open) {options.open = false; options.querySelector("summary").focus();}}
    const day = event.target.closest("[data-mr-day]");
    if (day && ["ArrowLeft","ArrowRight","Home","End"].includes(event.key)) {event.preventDefault(); const index = event.key === "Home" ? 1 : event.key === "End" ? report.days.length : Math.max(1,Math.min(report.days.length,Number(day.dataset.mrDay)+(event.key === "ArrowRight" ? 1 : -1))); host.querySelector(`[data-mr-day="${index}"]`)?.focus();}
  });
  host.addEventListener("error",event=>{if (event.target instanceof HTMLImageElement) {event.target.hidden = true; event.target.parentElement.classList.add("mr-no-art");}},true);
  host.addEventListener("pointermove",event=>{
    if (!effects || event.pointerType !== "mouse" || window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
    const card = event.target.closest(".mr-rank-card,.mr-cover"); if (!card) return;
    cancelAnimationFrame(frame); frame = requestAnimationFrame(()=>{const box = card.getBoundingClientRect(); card.style.setProperty("--tilt-x",`${(event.clientX-box.left)/box.width*6-3}deg`); card.style.setProperty("--tilt-y",`${3-(event.clientY-box.top)/box.height*6}deg`);});
  });
  host.addEventListener("pointerout",event=>{const card = event.target.closest(".mr-rank-card,.mr-cover"); if (card && !card.contains(event.relatedTarget)) {cancelAnimationFrame(frame); card.style.removeProperty("--tilt-x"); card.style.removeProperty("--tilt-y");}});
  load(month,true);
  return api;
}
