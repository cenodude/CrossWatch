/* assets/helpers/airing-card.js */
/* CrossWatch - Shared airing episode watch status cards */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  const esc = value => String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll('"', "&quot;");
  const icon = name => `<span class="material-symbols-rounded" aria-hidden="true">${name}</span>`;
  const pad = value => String(value).padStart(2, "0");
  const duration = minutes => minutes >= 60 ? `${Math.floor(minutes / 60)}h ${minutes % 60}m` : `${minutes}m`;
  const timers = new WeakMap();
  const views = new WeakMap();

  function render(row, {image = "", dateHtml, label, openAttributes, ariaLabel, title = row.title}) {
    const episodes = row.season_progress?.episodes || [];
    const ready = row.season_progress?.complete === true;
    const watched = episodes.filter(ep => ep.status === "watched").length;
    const left = episodes.filter(ep => ep.status === "unwatched");
    const aired = watched + left.length;
    const upcoming = episodes.filter(ep => ep.status === "upcoming").length;
    const runtime = ready && left.length && left.every(ep => Number.isFinite(ep.runtime) && ep.runtime > 0) ? left.reduce((sum, ep) => sum + ep.runtime, 0) : null;
    const art = image ? `<img src="${esc(image)}" alt="" loading="lazy" decoding="async">` : "";
    const summary = ready ? left.length ? T("library.airing.toCatchUp", {count: left.length}) : T("library.airing.caughtUp") : T("library.airing.statusUnavailable");
    return `<article class="trk-calendar-release cw-airing-card" data-airing-key="${esc(row.key)}" data-airing-episodes="${esc(JSON.stringify(episodes))}" data-airing-season="${esc(row.season)}" data-airing-episode="${esc(row.episode)}">
      <div class="cw-airing-turn"><button type="button" class="cw-airing-face cw-airing-front" data-airing-flip aria-expanded="false" aria-label="${TH("library.airing.showStatus", {label: ariaLabel})}">${art}<span class="trk-calendar-date">${dateHtml}<span class="trk-calendar-label">${esc(label)}</span></span><span class="cw-airing-hint">${icon("flip")}</span><span class="trk-calendar-copy"><strong>${esc(title)}</strong><span>${esc([`S${pad(row.season)} · E${pad(row.episode)}`, row.episode_title].filter(Boolean).join(" · "))}</span></span></button>
      <div class="cw-airing-face cw-airing-back" aria-hidden="true" inert>${art}<button type="button" class="cw-airing-return" data-airing-return aria-label="${TH("library.airing.back")}">${icon("undo")}</button><strong class="cw-airing-title">${esc(title)}</strong>
      <div class="cw-airing-summary"><div class="cw-airing-ring" role="img" aria-label="${ready ? TH("library.airing.watchedOf", {watched, aired}) : TH("library.airing.countUnavailable")}"><svg viewBox="0 0 68 68" aria-hidden="true"><circle class="cw-airing-track" cx="34" cy="34" r="28"/><circle class="cw-airing-value" cx="34" cy="34" r="28" style="--airing-offset:${ready && aired ? 175.93 * (1 - watched / aired) : 175.93}"/></svg><strong>${ready ? `${watched}<span>/${aired}</span>` : "–"}</strong></div><div class="cw-airing-catchup"><strong>${esc(summary)}</strong><span>${runtime ? `${icon("schedule")}${TH("library.airing.durationCatchUp", {duration: duration(runtime)})}` : ready ? TH("library.airing.upcoming", {count: upcoming}) : TH("library.airing.detailsBelow")}</span></div></div>
      <div class="cw-airing-season"><span>${TH("library.airing.seasonCaps", {n: String(row.season)})}</span><span>${ready && runtime ? TH("library.airing.upcoming", {count: upcoming}) : ""}</span></div>
      <div class="cw-airing-episodes" role="group" aria-label="${TH("library.airing.seasonEpisodes", {n: String(row.season)})}"></div><button type="button" class="cw-airing-next" ${openAttributes} aria-label="${TH("library.airing.open", {title})}"><span class="cw-airing-next-state">${TH("library.airing.upcomingCaps")}</span><strong class="cw-airing-next-copy">${TH("profile.media.episodeN", {n: String(row.episode)})}</strong>${icon("arrow_forward")}</button></div></div></article>`;
  }

  function paint(card) {
    const state = views.get(card);
    if (!state) return;
    const {episodes, selected, start, size} = state;
    const step = (direction, disabled) => `<button type="button" data-airing-step="${direction}" aria-label="${TH(direction < 0 ? "library.airing.prevEpisodes" : "library.airing.nextEpisodes")}"${disabled ? " disabled" : ""}>${icon(direction < 0 ? "chevron_left" : "chevron_right")}</button>`;
    card.querySelector(".cw-airing-episodes").innerHTML = (episodes.length > 8 ? step(-1, start === 0) : "") + episodes.slice(start, start + size).map(ep => `<button type="button" data-airing-select="${ep.episode}" data-state="${esc(ep.status)}" aria-label="Episode ${ep.episode}, ${esc(ep.status)}${ep.title ? `: ${esc(ep.title)}` : ""}" aria-pressed="${ep.episode === selected}">${icon(ep.status === "watched" ? "check" : ep.status === "upcoming" ? "schedule" : ep.status === "unknown" ? "help" : "play_arrow")}<span>${pad(ep.episode)}</span></button>`).join("");
    if (episodes.length > 8) card.querySelector(".cw-airing-episodes").insertAdjacentHTML("beforeend", step(1, start + size >= episodes.length));
    const ep = episodes.find(ep => ep.episode === selected);
    const status = ep?.status === "watched" ? "WATCHED" : ep?.status === "unwatched" ? (selected === episodes.find(row => row.status === "unwatched")?.episode ? "NEXT" : "UNWATCHED") : ep?.status === "unknown" ? "EPISODE" : "UPCOMING";
    card.querySelector(".cw-airing-next-state").textContent = status;
    const date = new Date(`${ep?.air_date}T12:00:00`);
    const detail = ep?.status === "upcoming" && Number.isFinite(date.getTime()) ? date.toLocaleDateString((window.CW?.I18n || window.__cwI18n)?.locale, {weekday:"short", month:"short", day:"numeric"}) : ep?.runtime ? T("library.airing.minutes", {n: ep.runtime}) : "";
    card.querySelector(".cw-airing-next-copy").textContent = `${T("profile.media.episodeN", {n: String(selected)})}${detail ? ` · ${detail}` : ""}`;
  }

  function toggle(card, open, keyboard = false) {
    clearTimeout(timers.get(card));
    if (!card.isConnected) return;
    if (open && !views.has(card)) {
      let episodes = [];
      try { episodes = JSON.parse(card.dataset.airingEpisodes || "[]"); } catch {}
      const selected = episodes.find(ep => ep.status === "unwatched")?.episode || Number(card.dataset.airingEpisode);
      const position = Math.max(0, episodes.findIndex(ep => ep.episode === selected));
      const size = episodes.length > 8 ? 6 : 8;
      views.set(card, {episodes, selected, size, start:Math.floor(position / size) * size});
      paint(card);
    }
    const front = card.querySelector(".cw-airing-front"), back = card.querySelector(".cw-airing-back");
    const moveFocus = keyboard || card.contains(document.activeElement);
    card.toggleAttribute("data-airing-open", open);
    front.setAttribute("aria-expanded", String(open));
    front.inert = open;
    back.inert = !open;
    front.setAttribute("aria-hidden", String(open));
    back.setAttribute("aria-hidden", String(!open));
    if (moveFocus) (open ? card.querySelector(".cw-airing-return") : front).focus({preventScroll:true});
  }

  document.addEventListener("pointerover", event => {
    const card = event.target.closest?.(".cw-airing-card");
    if (!card || event.pointerType !== "mouse" || card.contains(event.relatedTarget)) return;
    clearTimeout(timers.get(card));
    timers.set(card, setTimeout(() => toggle(card, true), 450));
  });
  document.addEventListener("pointerout", event => {
    const card = event.target.closest?.(".cw-airing-card");
    if (!card || event.pointerType !== "mouse" || card.contains(event.relatedTarget)) return;
    toggle(card, false);
  });
  document.addEventListener("click", event => {
    const target = event.target.closest?.("[data-airing-flip],[data-airing-return],[data-airing-select],[data-airing-step]");
    const card = target?.closest(".cw-airing-card");
    if (!card) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    if (target.hasAttribute("data-airing-flip")) toggle(card, true, event.detail === 0);
    else if (target.hasAttribute("data-airing-return")) toggle(card, false, event.detail === 0);
    else {
      const state = views.get(card);
      if (!state) return;
      if (target.hasAttribute("data-airing-select")) state.selected = Number(target.dataset.airingSelect);
      else state.start = Math.max(0, Math.min(Math.max(0, Math.ceil(state.episodes.length / state.size) * state.size - state.size), state.start + Number(target.dataset.airingStep) * state.size));
      const selected = target.dataset.airingSelect, direction = target.dataset.airingStep;
      paint(card);
      const focus = selected ? card.querySelector(`[data-airing-select="${selected}"]`) : card.querySelector(`[data-airing-step="${direction}"]:not(:disabled)`) || card.querySelector("[data-airing-select]");
      focus?.focus({preventScroll:true});
    }
  }, true);
  document.addEventListener("keydown", event => {
    const card = event.target.closest?.(".cw-airing-card");
    if (card && event.key === "Escape") { event.preventDefault(); event.stopImmediatePropagation(); toggle(card, false, true); }
  }, true);
  document.addEventListener("focusout", event => {
    const card = event.target.closest?.(".cw-airing-card");
    if (card) queueMicrotask(() => {
      if (!card.contains(document.activeElement) && !card.matches(":hover")) toggle(card, false);
    });
  });
  function capture(host) {
    return [...host.querySelectorAll('.cw-airing-card')].filter(card => card.hasAttribute("data-airing-open") || card.contains(document.activeElement)).map(card => {
      const active = card.contains(document.activeElement) ? document.activeElement : null;
      const selector = active?.hasAttribute("data-airing-select") ? `[data-airing-select="${Number(active.dataset.airingSelect)}"]`
        : active?.hasAttribute("data-airing-step") ? `[data-airing-step="${Number(active.dataset.airingStep)}"]`
        : active?.hasAttribute("data-airing-return") ? "[data-airing-return]" : active?.hasAttribute("data-airing-flip") ? "[data-airing-flip]" : active ? ".cw-airing-next" : "";
      return {node:card, key:card.dataset.airingKey, open:card.hasAttribute("data-airing-open"), selected:views.get(card)?.selected, start:views.get(card)?.start || 0, scroll:card.querySelector(".cw-airing-episodes").scrollLeft, selector};
    });
  }
  function restore(host, saved) {
    const cards = [...host.querySelectorAll(".cw-airing-card")];
    for (const entry of saved) {
      const card = cards.find(node => node.dataset.airingKey === entry.key);
      if (!card || card === entry.node) continue;
      toggle(card, entry.open);
      if (entry.open) {
        const state = views.get(card);
        if (state.episodes.some(ep => ep.episode === entry.selected)) state.selected = entry.selected;
        state.start = Math.min(entry.start, Math.max(0, Math.ceil(state.episodes.length / state.size) * state.size - state.size));
        paint(card);
        card.querySelector(".cw-airing-episodes").scrollLeft = entry.scroll;
      }
      if (entry.selector) (card.querySelector(entry.selector) || card.querySelector(".cw-airing-return"))?.focus({preventScroll:true});
    }
  }
  window.CW = window.CW || {};
  window.CW.AiringCard = {render, capture, restore};
})();
