/* assets/js/tracker-wrapped.js */
/* CrossWatch - A cinematic yearly watch story and downloadable keepsake */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import {monthNotifications} from "./tracker-month-notifications.js";

const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const number = value => Number(value || 0).toLocaleString();
const hours = value => `${number(Math.round(Number(value || 0) / 60))}h`;
const monthName = value => new Date(`${value}-02T12:00:00`).toLocaleDateString(undefined,{month:"short"});
const chapters = ["The premiere", "Your year in motion", "The movies", "Your leading series", "The streak", "Your signature", "Encore & credits", "The keepsake"];
const imageURL = url => /^https:\/\/image\.tmdb\.org\/t\/p\//.test(url || "") ? url : "";
const art = (row, backdrop = false) => {
  const url = imageURL(row?.[backdrop ? "backdrop" : "poster"] || row?.poster || row?.backdrop);
  return url ? `<img src="${esc(url.replace(/\/t\/p\/[^/]+\//, `/t/p/${backdrop ? "w1280" : "w342"}/`))}" alt="" loading="lazy" decoding="async">` : '<span class="wr-art-empty" aria-hidden="true">CrossWatch</span>';
};
function profile() {
  const auth = window.CW?.AuthState?.read?.() || {}, view = window.CW?.OverviewProfile;
  const user = auth.isAdmin && view?.id ? view.profile || {} : auth.user || {};
  return {name:String(user.display_name || user.label || user.username || ""), scope:JSON.stringify([auth.user?.id || auth.user?.username,auth.profileId,auth.role,auth.permissions,auth.isAdmin ? view?.id || "" : ""])};
}
const notices = monthNotifications(() => profile().scope, {yearly:true});
let active;

export function openWrapped({root, instance, label, year = "", openMedia, onClose}) {
  active?.close(false);
  const identity = profile(), previousFocus = document.activeElement, scroll = window.scrollY;
  const host = document.createElement("section");
  host.className = "wr-page";
  host.setAttribute("aria-label", "CrossWatch Wrapped");
  root.append(host);
  root.classList.add("is-wrapped");
  const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");
  let motionFrame = 0, pointer = null;
  let chapter = 0, response = null, report = null, controller, timer, request = 0, attempts = 0, closed = false, personal = false, touch;
  const api = active = {close};
  const isPending = () => !response || response.pending || response.stale || response.error === "review_timeout";
  function leave() {if (isPending()) notices.leave(instance,year,label,identity.scope);}
  function close(restore = true) {
    if (closed) return;
    closed = true;
    leave(); request++; controller?.abort(); clearTimeout(timer); resetMotion();
    observer.disconnect();
    reducedMotion.removeEventListener("change",resetMotion);
    window.removeEventListener("cw:auth-state-changed",changed);
    window.removeEventListener("cw:overview-profile-changed",changed);
    document.removeEventListener("visibilitychange",visibility);
    document.removeEventListener("config-saved",configured);
    host.remove(); root.classList.remove("is-wrapped");
    if (active === api) active = null;
    if (restore) {window.scrollTo({top:scroll,behavior:"instant"}); previousFocus?.focus({preventScroll:true});}
    onClose?.(restore);
  }
  const observer = new MutationObserver(() => {if (root.hidden || root.classList.contains("hidden")) close(false);});
  observer.observe(root,{attributes:true,attributeFilter:["class","hidden"]});
  const changed = () => {if (profile().scope !== identity.scope) close(false);};
  const visibility = () => {host.classList.toggle("wr-paused",document.hidden); if (document.hidden) resetMotion(); if (!document.hidden && isPending()) {attempts = 0; load();}};
  const configured = () => load();
  window.addEventListener("cw:auth-state-changed",changed);
  window.addEventListener("cw:overview-profile-changed",changed);
  document.addEventListener("visibilitychange",visibility);
  document.addEventListener("config-saved",configured);

  function fireworks() {
    return `<div class="wr-fireworks" aria-hidden="true">${[0,1,2].map(b=>`<div class="wr-burst" style="--burst:${b}">${Array.from({length:16},(_,i)=>`<i style="--angle:${i * 22.5}deg"></i>`).join("")}</div>`).join("")}</div>`;
  }
  function heading(kicker, title, detail = "") {
    return `<p class="wr-kicker">${kicker}</p><h1 tabindex="-1">${title}</h1>${detail ? `<p class="wr-intro">${detail}</p>` : ""}`;
  }
  function cards(rows, limit = 5, offset = 0) {
    return `<div class="wr-cards">${rows.slice(0,limit).map((row,i)=>`<button type="button" data-wr-title="${esc(row.key)}" class="wr-card" style="--rank:${i}"><span class="wr-cover">${art(row)}<b>${String(i+offset+1).padStart(2,"0")}</b></span><strong>${esc(row.title)}</strong><small>${number(row.plays)} plays${row.type === "show" ? ` · ${number(row.episodes)} distinct episodes` : ""}</small></button>`).join("")}</div>`;
  }
  function stats() {
    return `<div class="wr-stats">${[[number(report.plays),"plays"],[hours(report.minutes),"estimated watch time"],[number(report.active_days),"active days"]].map(([value,title])=>`<div><strong>${value}</strong><span>${title}</span></div>`).join("")}</div>`;
  }
  function content() {
    const movies = report.titles.filter(t=>t.type === "movie"), shows = report.titles.filter(t=>t.type === "show");
    if (chapter === 0) return `${fireworks()}<div class="wr-premiere">${heading("CROSSWATCH PRESENTS", `${esc(year)}<span>What a year<br>to press play.</span>`, `${esc(identity.name || label)}, this is ${report.current ? "your year so far" : "your watch story"}.`)}<div class="wr-reel">${report.titles.slice(0,7).map((row,i)=>`<div style="--rank:${i}">${art(row)}</div>`).join("")}</div><button type="button" class="wr-primary" data-wr="next">Unwrap ${esc(year)} <span aria-hidden="true">↗</span></button></div>`;
    if (chapter === 1) {
      const max = Math.max(1,...report.months.map(m=>m.plays));
      return `${heading("TWELVE MONTHS. YOUR STORY.", "A year in motion.", "Every little square is a day you made time for a story.")}${stats()}<div class="wr-months" aria-label="Plays by month">${report.months.map(m=>`<div title="${esc(monthName(m.month))}: ${m.plays} plays"><strong>${number(m.plays)}</strong><span style="--height:${m.plays/max*100}%"></span><small>${esc(monthName(m.month))}</small></div>`).join("")}</div><div class="wr-calendar" aria-label="Daily watch activity">${report.days.map(day=>`<i class="${day.movies+day.episodes ? "is-active" : ""}" title="${day.date}: ${day.movies+day.episodes} plays" style="--activity:${Math.min(1,(day.movies+day.episodes)/8)}"></i>`).join("")}</div>`;
    }
    if (chapter === 2) return `${heading("THE BIG SCREEN ENERGY", movies.length ? `${number(movies.length)} movies.<br>So many worlds.` : "A different kind of year.", movies.length ? "Your most-played movies take the spotlight." : "No movies recorded. Your series have the stage this year.")}${cards(movies)}${movies.length > 5 ? `<details class="wr-gallery"><summary>See all ${number(movies.length)} movies</summary>${cards(movies.slice(5),movies.length,5)}</details>` : ""}`;
    if (chapter === 3) {
      const lead = shows[0];
      return lead ? `${heading("YOUR LEADING SERIES", esc(lead.title), "The series you returned to most, ranked by plays.")}<button type="button" class="wr-feature" data-wr-title="${esc(lead.key)}">${art(lead,true)}<span><strong>${number(lead.plays)} plays</strong><small>${number(lead.episodes)} distinct episodes · ${hours(lead.minutes)} estimated</small><b>Open media card ↗</b></span></button>${cards(shows.slice(1),4,1)}` : heading("YOUR LEADING SERIES", "This year went to the movies.", "No episodes recorded for this year.");
    }
    if (chapter === 4) {
      const busiest = report.months.reduce((a,b)=>b.plays>a.plays?b:a), date = value => value ? new Date(`${value}T12:00:00`).toLocaleDateString(undefined,{month:"short",day:"numeric"}) : "";
      return `${heading("YOU KEPT THE STORIES GOING", "One more day.")}<div class="wr-streak-art" aria-hidden="true"><svg viewBox="0 0 400 400"><circle cx="200" cy="200" r="170"/><circle cx="200" cy="200" r="130"/><circle cx="200" cy="200" r="85"/><path d="M200 30a170 170 0 1 1-162 222"/>${Array.from({length:12},(_,i)=>`<circle class="wr-streak-star" cx="${200+170*Math.cos(i*Math.PI/6)}" cy="${200+170*Math.sin(i*Math.PI/6)}" r="${i%3===0?6:3}"/>`).join("")}<path class="wr-streak-flash" d="m218 110-70 105h48l-14 75 70-110h-48Z"/></svg></div><div class="wr-streak"><strong>${number(report.streak)}</strong><span>consecutive active days</span></div><p class="wr-intro">${esc(date(report.streak_start))} — ${esc(date(report.streak_end))}</p><div class="wr-callout"><span>Your busiest month</span><strong>${esc(monthName(busiest.month))}</strong><span>${number(busiest.plays)} plays · ${hours(busiest.minutes)} estimated</span></div>`;
    }
    if (chapter === 5) {
      const blocks = [report.hours.slice(5,12).reduce((a,b)=>a+b,0),report.hours.slice(12,18).reduce((a,b)=>a+b,0),report.hours.slice(18,22).reduce((a,b)=>a+b,0),[...report.hours.slice(22),...report.hours.slice(0,5)].reduce((a,b)=>a+b,0)];
      const peak = blocks.indexOf(Math.max(...blocks)), weekend = report.weekdays[5]+report.weekdays[6];
      const names = ["Morning stories", "Afternoon adventures", "Evening premieres", "After-dark regular"];
      return `${heading("YOUR WATCHING SIGNATURE", names[peak], "A portrait of your recorded watch times, not a personality test.")}<div class="wr-signature">${blocks.map((value,i)=>`<div><span>${["05–12","12–18","18–22","22–05"][i]}</span><i style="--share:${value/report.plays*100}%"></i><strong>${Math.round(value/report.plays*100)}%</strong></div>`).join("")}</div><div class="wr-callout"><strong>${Math.round(weekend/report.plays*100)}% on weekends</strong><span>${number(report.days.reduce((n,d)=>n+d.movies,0))} movie plays · ${number(report.days.reduce((n,d)=>n+d.episodes,0))} episode plays</span></div><div class="wr-genres">${report.genres.slice(0,4).map(g=>`<span>${esc(g.name)} <b>${g.count} titles</b></span>`).join("")}</div><p class="wr-fine">Watch times use ${esc(report.timezone)}. Genres cover titles with cached metadata.</p>`;
    }
    if (chapter === 6) {
      const replays = report.titles.filter(t=>t.replays>0).sort((a,b)=>b.replays-a.replays);
      return `${heading("ENCORE & CLOSING CREDITS", report.replays ? `${number(report.replays)} return visits.` : "Always another story.", "Replays count additional watches of the same movie or episode within this year.")}${cards(replays,3)}<div class="wr-bookends">${[[report.first,"Opening scene"],[report.last,report.current ? "Latest scene" : "Closing scene"]].map(([event,title])=>{
        const row=report.titles.find(t=>t.key===event?.title_key);
        return row ? `<button type="button" data-wr-title="${esc(row.key)}">${art(row,true)}<span><small>${title} · ${esc(event.date.slice(0,10))}</small><strong>${esc(row.title)}</strong></span></button>` : "";
      }).join("")}</div>`;
    }
    return `${fireworks()}${heading(report.current ? "THE STORY IS STILL UNFOLDING" : "THAT WAS YOUR YEAR", `Your ${esc(year)},<br><em>wrapped.</em>`, report.current ? "Keep watching. There are more chapters to come." : "Same time next year?")}${stats()}<div class="wr-keepsake"><span>CROSSWATCH WRAPPED / ${esc(year)}</span><strong>${esc(shows[0]?.title || movies[0]?.title || "Your year in stories")}</strong><small>${number(report.movies)} movies · ${number(report.shows)} series · ${number(report.streak)}-day streak</small></div><div class="wr-export"><label><input type="checkbox" data-wr-personal${personal ? " checked" : ""}> Include ${esc(identity.name || label)} on my keepsake</label><button type="button" class="wr-primary" data-wr="download">Save your keepsake <span aria-hidden="true">↓</span></button><span role="status" data-wr-export-status></span></div>`;
  }
  function navigation() {
    const glyphs = ["m9 5 11 7-11 7Z", "M4 19V9m5 10V4m6 15v-8m5 8V6", "M4 4h16v16H4ZM4 9h16M8 4v5m8-5v5", "M3 5h18v13H3Zm5 17h8", "m13 2-8 12h6l-1 8 9-13h-7Z", "M12 3a9 9 0 1 0 9 9 7 7 0 0 1-9-9Z", "M4 7h12a5 5 0 0 1 0 10H8m0-14L4 7l4 4", "M4 8h16v12H4ZM2 5h20v4H2Zm10 0v15M12 5C5 5 6-1 9 2Zm0 0c7 0 6-6 3-3Z"];
    const arrow = direction => `<svg viewBox="0 0 32 32" aria-hidden="true"><circle cx="16" cy="16" r="14"/><path d="${direction === "previous" ? "m17 10-6 6 6 6m-6-6h11" : "m15 10 6 6-6 6m6-6H10"}"/></svg>`;
    return `<nav class="wr-navigation" aria-label="Story chapters"><button type="button" class="wr-edge wr-edge-previous" data-wr="previous" aria-label="Previous chapter"${chapter===0 ? " disabled" : ""}>${arrow("previous")}<span>Previous scene</span></button><div class="wr-dock"><p class="wr-chapter-caption"><span>${esc(chapters[chapter])}</span><span>${String(chapter+1).padStart(2,"0")} <i>/ 08</i></span></p><div class="wr-chapters">${chapters.map((name,i)=>`<button type="button" data-wr-chapter="${i}" class="wr-chapter${i<chapter?" is-seen":""}" aria-label="Chapter ${i+1}: ${esc(name)}"${i===chapter?' aria-current="step"':""}><svg viewBox="0 0 24 24" aria-hidden="true"><path d="${glyphs[i]}"/></svg><span class="wr-chapter-preview" aria-hidden="true">${art(report.titles[i%report.titles.length])}<b>${String(i+1).padStart(2,"0")}</b><span>${esc(name)}</span></span><i class="wr-chapter-track" aria-hidden="true"></i></button>`).join("")}</div></div><button type="button" class="wr-edge wr-edge-next" data-wr="next" aria-label="Next chapter"${chapter===7 ? " disabled" : ""}>${arrow("next")}<span>Next scene</span></button></nav>`;
  }
  function resetMotion() {
    cancelAnimationFrame(motionFrame); motionFrame = 0; pointer = null;
    const stage = host.querySelector(".wr-stage");
    for (const name of ["--wr-light-x","--wr-light-y","--wr-drift-x","--wr-drift-y"]) stage?.style.removeProperty(name);
  }
  function render(focus = false) {
    resetMotion();
    host.dataset.chapter = String(chapter);
    const pending = response?.pending || response?.stale, playable = report?.plays && !response?.disabled;
    host.innerHTML = `<nav class="wr-toolbar" aria-label="CrossWatch Wrapped controls"><button type="button" data-wr="close">← Tracker</button><span class="wr-wordmark">CrossWatch <b>WRAPPED</b></span><label>Year <input type="number" min="1900" max="${esc(response?.current_year || new Date().getFullYear())}" value="${esc(year)}" aria-label="Wrapped year" data-wr-year></label></nav><div class="wr-stage${playable ? " wr-story" : ""}"><div class="wr-content">${response?.disabled ? heading("CROSSWATCH WRAPPED", "Wrapped is turned off.", "Enable CrossWatch Wrapped in this tracker's options to prepare your yearly story. Saved reports are kept.") : report ? report.plays ? content() : heading("A BLANK REEL", `Your ${esc(year)} awaits.`, "No watches recorded for this year. Choose another year to explore your history.") : `${heading("CROSSWATCH WRAPPED", response?.error ? "A brief intermission." : "Preparing your premiere.", response?.error ? "Your history is safe. Try again when you're ready." : "Your story is being prepared in the background. You can leave; the bell will let you know when it's ready.")}<span class="wr-loader" aria-hidden="true"></span>`}</div>${playable ? navigation() : ""}</div><div class="wr-status" role="status">${response?.error ? '<span>The latest update could not finish.</span> <button type="button" data-wr="retry">Try again</button>' : pending && report ? 'Updating your story in the background…' : ""}</div>${playable ? `<p class="wr-fine">${report.current ? "Year so far. " : ""}Watch time is estimated from known runtimes (${number(report.timed)} of ${number(report.plays)} plays). Built from your CrossWatch history.</p>` : ""}`;
    if (focus) host.querySelector("h1")?.focus({preventScroll:true});
  }
  function move(next) {
    if (!report?.plays || response?.disabled) return;
    next = Math.max(0,Math.min(7,next));
    if (next === chapter) return;
    host.style.setProperty("--wr-direction",next > chapter ? 1 : -1);
    chapter = next; render(true);
    host.scrollIntoView({behavior:"instant",block:"start"});
  }
  async function load(next = year, initial = false) {
    if (closed) return;
    if (initial && next !== year) {leave(); report = null; response = null; year = next; chapter = 0; attempts = 0;}
    notices.viewing(instance,next);
    clearTimeout(timer); controller?.abort(); controller = new AbortController(); const id=++request;
    if (!response) render();
    const currentController=controller, timeout=setTimeout(()=>currentController.abort(),15000);
    try {
      const res=await fetch(`/api/tracker/overview/wrapped?provider_instance=${encodeURIComponent(instance)}&year=${encodeURIComponent(next)}`,{credentials:"same-origin",cache:"no-store",signal:controller.signal});
      if (!res.ok) {if ([401,403,404].includes(res.status)) report=null; throw new Error("unavailable");}
      const data=await res.json(); if (closed || id!==request) return;
      const prior=response; response=data; year=data.year || year;
      if (data.disabled) report=null;
      else if (data.report) report=data.report;
      notices.viewing(instance,year);
      if (!prior || prior.generated!==data.generated || prior.pending!==data.pending || prior.error!==data.error || prior.disabled!==data.disabled || initial) render(!prior);
      if ((data.pending || data.stale) && !data.error && ++attempts<30) timer=setTimeout(()=>{if (!document.hidden) load();},Math.min(10000,1500+attempts*500));
      else if (!data.disabled && (data.pending || data.stale || !report) && !data.error) {response={...data,pending:false,error:"review_timeout"};render();}
    } catch {
      if (closed || id!==request) return;
      response={...response,error:"review_unavailable"};render();
    } finally {clearTimeout(timeout);}
  }
  async function download() {
    const button=host.querySelector('[data-wr="download"]'), status=host.querySelector('[data-wr-export-status]');
    button.disabled=true;
    try {
      const canvas=document.createElement("canvas"); canvas.width=1200; canvas.height=1600;
      const ctx=canvas.getContext("2d");
      const gradient=ctx.createLinearGradient(0,0,1200,1600);gradient.addColorStop(0,"#10131e");gradient.addColorStop(1,"#382443");ctx.fillStyle=gradient;ctx.fillRect(0,0,1200,1600);
      ctx.strokeStyle="#eac475";ctx.lineWidth=2;
      for (const [x,y] of [[980,240],[180,1110]]) for(let i=0;i<24;i++){const angle=i*Math.PI/12;ctx.beginPath();ctx.moveTo(x+Math.cos(angle)*35,y+Math.sin(angle)*35);ctx.lineTo(x+Math.cos(angle)*150,y+Math.sin(angle)*150);ctx.stroke();}
      const line=(text,y,size,color="#f6e9c6")=>{ctx.font=`700 ${size}px sans-serif`;ctx.fillStyle=color;ctx.fillText(text,90,y,1020);};
      line("CROSSWATCH WRAPPED",130,30);line(year,450,210);line(report.current?"My year so far.":"What a year to press play.",550,45);
      if(personal)line(identity.name || label,640,38);
      line(`${number(report.plays)} plays`,820,80);line(`${hours(report.minutes)} estimated watch time`,920,45);line(`${number(report.movies)} movies · ${number(report.shows)} series`,1000,40);line(`${number(report.streak)}-day streak`,1080,40);
      const top=report.titles[0];if(top){line("MOST PLAYED",1240,24);line(top.title,1310,48);}
      line("Your history. Your story. CrossWatch.",1470,26);line(`${number(report.timed)} of ${number(report.plays)} plays have known runtimes.`,1520,20,"#c4bdc8");
      const blob=await new Promise(resolve=>canvas.toBlob(resolve,"image/png"));if(!blob)throw new Error("export");
      const url=URL.createObjectURL(blob),link=document.createElement("a");link.href=url;link.download=`CrossWatch-Wrapped-${year}.png`;link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
      status.textContent="Your keepsake is ready to save.";
    } catch {status.textContent="Could not create the keepsake. Please try again.";} finally {button.disabled=false;}
  }
  host.addEventListener("click",event=>{
    const button=event.target.closest("button");if(!button || button.disabled)return;
    if(button.hasAttribute("data-wr-chapter")){move(Number(button.dataset.wrChapter));return;}
    if(button.dataset.wrTitle){const row=report?.titles.find(t=>t.key===button.dataset.wrTitle);if(row)openMedia?.(row);return;}
    if(button.dataset.wr==="close")close();
    if(button.dataset.wr==="next")move(chapter+1);
    if(button.dataset.wr==="previous")move(chapter-1);
    if(button.dataset.wr==="retry"){attempts=0;load();}
    if(button.dataset.wr==="download")download();
  });
  host.addEventListener("change",event=>{
    const el=event.target;
    if(el.matches("[data-wr-year]") && el.checkValidity() && /^\d{4}$/.test(el.value))load(el.value,true);
    if(el.matches("[data-wr-personal]"))personal=el.checked;
  });
  host.addEventListener("keydown",event=>{
    if(event.target.closest("input,select,textarea") || event.target.closest('[role="dialog"]'))return;
    if(event.key==="ArrowRight" || event.key==="ArrowLeft"){event.preventDefault();move(chapter+(event.key==="ArrowRight"?1:-1));}
  });
  reducedMotion.addEventListener("change",resetMotion);
  host.addEventListener("pointermove",event=>{
    if (event.pointerType !== "mouse" || reducedMotion.matches || document.hidden) return;
    const stage=event.target.closest(".wr-stage");
    if (!stage) {resetMotion(); return;}
    pointer={stage,x:event.clientX,y:event.clientY};
    if (motionFrame) return;
    motionFrame=requestAnimationFrame(()=>{
      motionFrame=0;
      if (!pointer || closed) return;
      const {stage,x,y}=pointer,rect=stage.getBoundingClientRect(),dx=(x-rect.left)/rect.width,dy=(y-rect.top)/rect.height;
      stage.style.setProperty("--wr-light-x",`${dx*100}%`);stage.style.setProperty("--wr-light-y",`${dy*100}%`);
      stage.style.setProperty("--wr-drift-x",`${(dx-.5)*18}px`);stage.style.setProperty("--wr-drift-y",`${(dy-.5)*18}px`);
    });
  });
  host.addEventListener("pointerleave",resetMotion);
  host.addEventListener("touchstart",event=>{touch=event.target.closest("button,input,select,summary")?null:event.touches[0];},{passive:true});
  host.addEventListener("touchend",event=>{const end=event.changedTouches[0];if(touch && Math.abs(end.clientX-touch.clientX)>70 && Math.abs(end.clientY-touch.clientY)<50)move(chapter+(end.clientX<touch.clientX?1:-1));touch=null;},{passive:true});
  host.addEventListener("error",event=>{if(event.target instanceof HTMLImageElement)event.target.hidden=true;},true);
  render();load();
  return api;
}
