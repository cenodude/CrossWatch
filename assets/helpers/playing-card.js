/* assets/helpers/playing-card.js */
/* Shared Playing Card renderer: one card, per-variant regions */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const TEMPLATE = `
    <div class="pc-inner">
      <a class="pc-poster-link" target="_blank" rel="noopener noreferrer">
        <img class="pc-poster" src="/assets/img/placeholder_poster.svg" alt="">
      </a>
      <div class="pc-body">
        <div class="pc-title-row">
          <div class="pc-title">Now Playing</div>
          <div class="pc-title-actions">
            <div class="pc-nav" hidden>
              <button class="pc-nav-btn pc-prev" type="button" aria-label="Previous stream"><span class="material-symbols-rounded">chevron_left</span></button>
              <span class="pc-nav-count">1 / 1</span>
              <button class="pc-nav-btn pc-next" type="button" aria-label="Next stream"><span class="material-symbols-rounded">chevron_right</span></button>
            </div>
          </div>
        </div>
        <div class="pc-meta"></div>
        <div class="pc-overview-wrap">
          <div class="pc-overview"></div>
          <button class="pc-overview-more" type="button" aria-expanded="false" hidden>More</button>
        </div>
      </div>
      <div class="pc-stats">
        <div class="pc-progress-wrap">
          <div class="pc-progress-labels">
            <span class="pc-progress-pct"></span>
            <span class="pc-progress-end">
              <span class="pc-progress-time"></span>
              <button class="pc-close" type="button" title="Hide" aria-label="Hide card"><span class="material-symbols-rounded" aria-hidden="true">close</span></button>
            </span>
          </div>
          <div class="pc-progress-bg"><div class="pc-progress"></div></div>
        </div>
        <div class="pc-stats-bottom">
          <div class="pc-info-block pc-information-block">
            <div class="pc-info-label">Information</div>
            <div class="pc-information-rows"></div>
          </div>
          <div class="pc-rating-stack">
            <div class="pc-info-block pc-rating-block">
              <div class="pc-info-label">TMDB Rating</div>
              <div class="pc-info-value"><span class="material-symbols-rounded pc-info-icon" aria-hidden="true">star</span><span class="pc-rating">--</span></div>
              <div class="pc-rating-votes pc-info-note">Rating unavailable</div>
            </div>
            <div class="pc-status-head"><span class="material-symbols-rounded pc-status-icon" aria-hidden="true">play_arrow</span><div class="pc-status">Now Playing</div></div>
            <div class="pc-sources"></div>
          </div>
        </div>
        <div class="pc-actions">
          <div class="pc-links"></div>
          <button class="pc-trailer" type="button">Watch trailer</button>
        </div>
      </div>
    </div>
`;


  const runtimeLabel = (mins) => {
    const m = Number(mins) || 0;
    if (!m) return "";
    const h = Math.floor(m / 60);
    const mm = m % 60;
    return h ? `${h}h ${mm ? `${mm}m` : ""}` : `${mm}m`;
  };

  const formatTime = (ms) => {
    const totalMs = Number(ms) || 0;
    if (!totalMs) return "";
    const totalSec = Math.floor(totalMs / 1000);
    const h = Math.floor(totalSec / 3600);
    const m = Math.floor((totalSec % 3600) / 60);
    const s = totalSec % 60;
    return h > 0 ? `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}` : `${m}:${String(s).padStart(2, "0")}`;
  };

  const formatDateLabel = (raw) => {
    const value = String(raw || "").trim().split("T")[0];
    const match = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!match) return value;
    const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
    return date.toLocaleDateString(undefined, { day: "numeric", month: "short", year: "numeric", timeZone: "UTC" });
  };

  const genreLabel = (meta, det) => {
    const raw = meta?.genres || det?.genres || [];
    return (Array.isArray(raw) ? raw : [])
      .map((genre) => (typeof genre === "string" ? genre : genre?.name))
      .map((genre) => String(genre || "").trim())
      .filter(Boolean)
      .join(", ") || "Genres unavailable";
  };

  const nextEpisodeLabels = (nextEpisode) => {
    if (!nextEpisode || typeof nextEpisode !== "object") return ["No upcoming episode", ""];
    const season = Number(nextEpisode.season_number);
    const episode = Number(nextEpisode.episode_number);
    const code = Number.isInteger(season) && Number.isInteger(episode)
      ? `S${String(season).padStart(2, "0")}E${String(episode).padStart(2, "0")}`
      : "Next episode";
    const airDate = String(nextEpisode.air_date || "").trim();
    let timing = "";
    const match = airDate.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (match) {
      const target = Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
      const now = new Date();
      const today = Date.UTC(now.getFullYear(), now.getMonth(), now.getDate());
      const days = Math.round((target - today) / 86400000);
      timing = days === 0 ? "Airs today" : days === 1 ? "Airs tomorrow" : days > 1 ? `Airs in ${days} days` : "Previously aired";
    }
    return [[code, timing].filter(Boolean).join(" · "), formatDateLabel(airDate)];
  };

  function informationFor(meta, isMovie) {
    const det = meta?.detail || {};
    const rows = [{ icon: "sell", main: genreLabel(meta, det) }];
    if (isMovie) {
      const releaseRaw = meta?.release?.date || det.release_date || "";
      const runtime = meta?.runtime_minutes ?? det.runtime_minutes ?? meta?.runtime ?? det.runtime;
      rows.push({ icon: "calendar_month", main: formatDateLabel(releaseRaw) || "Release date unavailable" });
      rows.push({ icon: "schedule", main: runtimeLabel(runtime) || "Runtime unavailable" });
      return rows;
    }
    rows.push({ icon: "tv", main: String(det.status || "Status unavailable") });
    const seasons = Number(det.number_of_seasons);
    const episodes = Number(det.number_of_episodes);
    const totals = [
      Number.isFinite(seasons) && seasons > 0 ? `${seasons} Season${seasons === 1 ? "" : "s"}` : "",
      Number.isFinite(episodes) && episodes > 0 ? `${episodes} Episode${episodes === 1 ? "" : "s"}` : "",
    ].filter(Boolean).join(" · ") || "Series totals unavailable";
    rows.push({ icon: "layers", main: totals });
    const [nextMain, nextSub] = nextEpisodeLabels(det.next_episode_to_air);
    rows.push({ icon: "arrow_forward", main: nextMain, sub: nextSub });
    return rows;
  }

  function informationRow(icon, main, sub = "") {
    const row = document.createElement("div");
    row.className = "pc-information-row";
    const iconEl = document.createElement("span");
    iconEl.className = "material-symbols-rounded pc-information-row-icon";
    iconEl.setAttribute("aria-hidden", "true");
    iconEl.textContent = icon;
    const copy = document.createElement("span");
    copy.className = "pc-information-copy";
    const mainEl = document.createElement("span");
    mainEl.className = "pc-information-main";
    mainEl.textContent = main;
    mainEl.title = main;
    copy.appendChild(mainEl);
    if (sub) {
      const subEl = document.createElement("span");
      subEl.className = "pc-information-sub";
      subEl.textContent = sub;
      copy.appendChild(subEl);
    }
    row.append(iconEl, copy);
    return row;
  }

  function mount(options = {}) {
    const variant = options.variant === "watchlist" ? "watchlist" : "scrobble";
    const el = document.createElement("div");
    if (options.id) el.id = options.id;
    el.className = `cw-pc cw-pc-${variant}`;
    el.setAttribute("aria-live", "polite");
    if (options.label) el.setAttribute("aria-label", options.label);
    if (options.tabScope) el.dataset.tabScope = options.tabScope;
    if (options.width) el.style.setProperty("--pc-width", options.width);
    el.innerHTML = TEMPLATE;
    document.body.appendChild(el);

    const q = (sel) => el.querySelector(sel);
    const els = {
      poster: q(".pc-poster"),
      posterLink: q(".pc-poster-link"),
      title: q(".pc-title"),
      meta: q(".pc-meta"),
      overview: q(".pc-overview"),
      overviewWrap: q(".pc-overview-wrap"),
      overviewMore: q(".pc-overview-more"),
      progress: q(".pc-progress"),
      progressPct: q(".pc-progress-pct"),
      progressTime: q(".pc-progress-time"),
      informationBlock: q(".pc-information-block"),
      informationRows: q(".pc-information-rows"),
      ratingBlock: q(".pc-rating-block"),
      rating: q(".pc-rating"),
      ratingVotes: q(".pc-rating-votes"),
      status: q(".pc-status"),
      statusIcon: q(".pc-status-icon"),
      nav: q(".pc-nav"),
      navCount: q(".pc-nav-count"),
      prev: q(".pc-prev"),
      next: q(".pc-next"),
      trailer: q(".pc-trailer"),
      sources: q(".pc-sources"),
      links: q(".pc-links"),
    };

    els.poster.onerror = () => {
      els.poster.onerror = null;
      els.poster.src = "/assets/img/placeholder_poster.svg";
    };

    let overviewFrame = 0;
    const updateOverviewMore = () => {
      if (overviewFrame) cancelAnimationFrame(overviewFrame);
      overviewFrame = requestAnimationFrame(() => {
        overviewFrame = 0;
        if (els.overviewWrap.classList.contains("is-expanded")) return;
        const hasOverflow = !!els.overview.textContent.trim() && els.overview.scrollHeight > els.overview.clientHeight + 1;
        els.overviewWrap.classList.toggle("has-overflow", hasOverflow);
        els.overviewMore.hidden = !hasOverflow;
      });
    };

    els.overviewMore.addEventListener("click", () => {
      const expanded = els.overviewWrap.classList.toggle("is-expanded");
      els.overview.scrollTop = 0;
      els.overviewMore.textContent = expanded ? "Less" : "More";
      els.overviewMore.setAttribute("aria-expanded", expanded ? "true" : "false");
      if (!expanded) updateOverviewMore();
    });

    el.querySelectorAll(".pc-close").forEach((btn) => {
      btn.addEventListener("click", () => options.onClose?.(), true);
    });
    els.prev?.addEventListener("click", () => options.onPrev?.(), true);
    els.next?.addEventListener("click", () => options.onNext?.(), true);
    els.trailer?.addEventListener("click", () => options.onTrailer?.(), true);

    const setOverview = (value) => {
      els.overviewWrap.classList.remove("is-expanded");
      els.overview.scrollTop = 0;
      els.overview.textContent = String(value || "");
      els.overviewMore.textContent = "More";
      els.overviewMore.setAttribute("aria-expanded", "false");
      els.overviewMore.hidden = true;
      updateOverviewMore();
    };

    const setChips = (chips) => {
      els.meta.replaceChildren();
      for (const chip of chips || []) {
        const text = String(chip?.text ?? chip ?? "").trim();
        if (!text) continue;
        const span = document.createElement("span");
        span.className = "pc-chip";
        if (chip?.cls) span.classList.add(chip.cls);
        span.textContent = text;
        els.meta.appendChild(span);
      }
    };

    const setRating = (rawRating, rawVotes) => {
      const rating = Number(rawRating);
      const available = Number.isFinite(rating) && rating >= 1 && rating <= 10;
      els.ratingBlock.classList.toggle("rating-low", available && rating < 5);
      els.ratingBlock.classList.toggle("rating-mid", available && rating >= 5 && rating < 7);
      els.ratingBlock.classList.toggle("rating-high", available && rating >= 7);
      els.rating.textContent = available ? rating.toFixed(1) : "--";
      const votes = Number(rawVotes);
      els.ratingVotes.textContent = !available
        ? "Rating unavailable"
        : Number.isFinite(votes) && votes > 0
          ? `${votes.toLocaleString(undefined, { notation: "compact", maximumFractionDigits: 1 })} votes`
          : "0 votes";
    };

    const setPosterLink = (href, title = "") => {
      if (!href) {
        els.posterLink.removeAttribute("href");
        els.posterLink.removeAttribute("aria-label");
        els.posterLink.removeAttribute("title");
        els.posterLink.setAttribute("aria-disabled", "true");
        return;
      }
      els.posterLink.href = href;
      els.posterLink.setAttribute("aria-label", `Open ${title || "title"} on TMDb`);
      els.posterLink.title = `Open ${title || "title"} on TMDb`;
      els.posterLink.removeAttribute("aria-disabled");
    };

    const setInformation = (rows, isMovie) => {
      els.informationBlock.classList.toggle("is-series", isMovie === false);
      if (rows === "loading") {
        els.informationRows.replaceChildren(informationRow("hourglass_empty", "Loading information..."));
        return;
      }
      els.informationRows.replaceChildren(
        ...(Array.isArray(rows) ? rows : []).map((r) => informationRow(r.icon, r.main, r.sub || ""))
      );
    };

    const setSources = (sources) => {
      els.sources.replaceChildren();
      for (const source of sources || []) {
        const span = document.createElement("span");
        span.className = "pc-source";
        if (source?.label) span.title = source.label;
        if (source?.logo) {
          const img = document.createElement("img");
          img.src = source.logo;
          img.alt = `${source.label || ""} logo`;
          span.appendChild(img);
        } else {
          const text = document.createElement("span");
          text.className = "pc-source-text";
          text.textContent = String(source?.short || source?.label || "").slice(0, 4);
          span.appendChild(text);
        }
        els.sources.appendChild(span);
      }
    };

    const setLinks = (links) => {
      els.links.replaceChildren();
      for (const link of links || []) {
        if (!link?.href) continue;
        const a = document.createElement("a");
        a.className = "pc-link";
        a.href = link.href;
        a.target = "_blank";
        a.rel = "noopener noreferrer";
        a.textContent = link.text || "Open";
        els.links.appendChild(a);
      }
    };

    const setProgress = (progress) => {
      const pct = Math.max(0, Math.min(100, Number(progress?.pct) || 0));
      els.progress.style.width = `${pct}%`;
      els.progressPct.textContent = progress ? `${Math.round(pct)}% watched` : "";
      els.progressTime.textContent = progress?.remaining || "";
    };

    const setNav = (nav) => {
      const total = Number(nav?.total) || 0;
      els.nav.hidden = total <= 1;
      els.navCount.textContent = total > 0 ? `${(Number(nav?.index) || 0) + 1} / ${total}` : "0 / 0";
      els.prev.disabled = total <= 1;
      els.next.disabled = total <= 1;
    };

    function render(model) {
      if (!model) return;
      els.title.textContent = model.year ? `${model.title} ${model.year}` : (model.title || "");
      setChips(model.chips);
      setOverview(model.overview || "");
      els.poster.src = model.poster || "/assets/img/placeholder_poster.svg";
      els.poster.alt = model.title || "Poster";
      setPosterLink(model.posterHref || "", model.title);
      el.style.setProperty("--pc-backdrop", model.backdrop ? `url("${model.backdrop}")` : "none");
      setInformation(model.information, model.isMovie);
      setRating(model.rating?.value, model.rating?.votes);
      setProgress(model.progress);
      setNav(model.nav);
      setSources(model.sources);
      setLinks(model.links);
      if (model.status) {
        els.status.textContent = model.status.text || "";
        els.statusIcon.textContent = model.status.icon || "play_arrow";
        els.status.title = model.status.title || "";
      }
      if (els.trailer) els.trailer.textContent = model.trailerLabel || "Watch trailer";
    }

    return {
      el,
      render,
      renderProgress: setProgress,
      show: () => el.classList.add("show"),
      hide: () => el.classList.remove("show"),
      isVisible: () => el.classList.contains("show"),
      destroy: () => el.remove(),
    };
  }

  (window.CW ||= {}).PlayingCard = {
    mount,
    fmt: { runtimeLabel, formatTime, formatDateLabel, genreLabel, nextEpisodeLabels, informationFor },
  };
})();
