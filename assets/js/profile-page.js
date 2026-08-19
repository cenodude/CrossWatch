/* assets/js/profile-page.js */
/* CrossWatch managed user profile page */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
(function () {
  const $ = (sel, root = document) => root.querySelector(sel);
  const esc = (value) => String(value ?? "").replace(/[&<>"]/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;" }[m]));
  const api = async (url, opt = {}) => {
    const res = await fetch(url, { cache: "no-store", credentials: "same-origin", ...opt });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data?.ok === false) throw new Error(data?.error || `HTTP ${res.status}`);
    return data;
  };
  const profileRouteSegment = (value) => {
    try {
      value = decodeURIComponent(String(value || ""));
    } catch {
      value = String(value || "");
    }
    return value.trim().toLowerCase().replace(/-/g, "_");
  };
  const redirectProfileAppHash = () => {
    if (window.location?.pathname !== "/profile") return false;
    const raw = String(window.location?.hash || "");
    if (!raw) return false;
    const tab = profileRouteSegment(raw.replace(/^#\/?/, "").split("?")[0].split("/")[0]);
    const appTabs = new Set(["main", "watchlist", "playback_progress", "snapshots", "playlists", "editor", "settings"]);
    if (!appTabs.has(tab)) return false;
    const doc = document.documentElement;
    const isAdmin = doc?.dataset?.cwRole !== "user";
    const canWrite = doc?.dataset?.cwPermWrite === "on";
    const canReadWatchlist = tab === "watchlist" && doc?.dataset?.cwPermWatchlist !== "off";
    const canReadPlayback = tab === "playback_progress" && doc?.dataset?.cwPermPlayback !== "off";
    if (isAdmin) {
      window.location.replace(`/${raw}`);
      return true;
    }
    if (canWrite) {
      window.location.replace(`/?main=1${raw}`);
      return true;
    }
    if (canReadWatchlist || canReadPlayback) {
      window.location.replace(`/?view=${encodeURIComponent(tab)}${raw}`);
      return true;
    }
    return false;
  };
  if (redirectProfileAppHash()) return;
  window.addEventListener("hashchange", redirectProfileAppHash);
  const post = (url, body) => api(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body || {}) });
  const del = (url) => api(url, { method: "DELETE" });
  const providerLabel = (provider) => window.CW?.ProviderMeta?.label?.(provider) || String(provider || "").toUpperCase();
  const providerKey = (provider) => window.CW?.ProviderMeta?.keyOf?.(provider) || String(provider || "").trim().toUpperCase();
  const providerLogo = (provider) => window.CW?.ProviderMeta?.logoPath?.(provider) || "";
  const visibleProviderLabel = (provider) => {
    const raw = String(provider || "").trim();
    if (!raw || raw === "?" || /^unknown$/i.test(raw) || /^none$/i.test(raw)) return "";
    const label = String(providerLabel(raw) || "").trim();
    return (!label || label === "?" || /^unknown$/i.test(label) || /^none$/i.test(label)) ? "" : label;
  };
  const providerIconHtml = (provider) => {
    const label = visibleProviderLabel(provider);
    if (!label) return "";
    const key = providerKey(provider).toLowerCase().replace(/[^a-z0-9-]+/g, "");
    const logo = providerLogo(provider);
    const icon = logo
      ? `<img class="cw-profile-provider-logo" src="${esc(logo)}" alt="" loading="lazy" onerror="this.onerror=null;this.hidden=true;this.nextElementSibling.hidden=false">`
      : "";
    const fallbackHidden = logo ? " hidden" : "";
    return `<span class="cw-profile-provider-badge cw-profile-provider-badge--icon" data-provider="${esc(key)}" title="${esc(label)}" aria-label="${esc(label)}">${icon}<span class="cw-profile-provider-fallback"${fallbackHidden}>${esc(label.slice(0, 2))}</span></span>`;
  };
  const providerBadgeHtml = (provider) => {
    const label = visibleProviderLabel(provider);
    if (!label) return "";
    const key = providerKey(provider).toLowerCase().replace(/[^a-z0-9-]+/g, "");
    const logo = providerLogo(provider);
    const icon = logo
      ? `<img class="cw-profile-provider-logo" src="${esc(logo)}" alt="" loading="lazy" onerror="this.hidden=true;this.nextElementSibling.hidden=false">`
      : "";
    const fallbackHidden = logo ? " hidden" : "";
    return `<span class="cw-profile-provider-badge" data-provider="${esc(key)}">${icon}<span class="cw-profile-provider-fallback"${fallbackHidden}>${esc(label)}</span><span>${esc(label)}</span></span>`;
  };
  const providerName = (value) => {
    if (!value) return "";
    if (typeof value === "string") return value;
    if (typeof value === "object") return String(value.provider || value.name || value.key || "");
    return String(value);
  };
  const providerOf = (item) => providerName(item?.source) || providerName(item?.provider) || providerName(item?.sources?.[0]) || "";
  const providerRoute = (item) => {
    const source = providerName(item?.source) || providerName(item?.provider);
    const rest = [];
    const push = (value) => {
      const name = providerName(value);
      if (name && name !== source && !rest.includes(name)) rest.push(name);
    };
    for (const row of Array.isArray(item?.targets) ? item.targets : []) push(row);
    for (const row of Array.isArray(item?.sources) ? item.sources : []) push(row);
    return { source: source || rest.shift() || "", sinks: rest, routed: !!source };
  };
  const mediaValue = (item) => String(item?.media_type || item?.type || item?.art_type || "").toLowerCase();
  const mediaType = (item) => /^(tv|show|shows|series|season|episode|anime|anime_episode)$/i.test(mediaValue(item));
  const objectOf = (value) => value && typeof value === "object" && !Array.isArray(value) ? value : {};
  const tmdbId = (item) => {
    const ids = objectOf(item?.ids);
    const meta = objectOf(item?.provider_metadata);
    const showIds = objectOf(meta.show_ids);
    if (mediaValue(item) === "movie") return item?.tmdb || item?.tmdb_id || ids.tmdb || ids.id || "";
    return showIds.tmdb || ids.tmdb_show || item?.tmdb_show || ids.show_tmdb || item?.show_tmdb || item?.tmdb || item?.tmdb_id || ids.tmdb || "";
  };
  const titleOf = (item) => String(item?.series_title || item?.title || item?.name || item?.show_title || item?.label || "Untitled");
  const yearOf = (item) => String(item?.year || item?.release_year || item?.aired_year || "").trim();
  const episodeOf = (item) => {
    const explicit = String(item?.episode_label || item?.episodeLabel || "").trim();
    if (explicit) return explicit;
    const s = Number(item?.season || item?.season_number || item?.episode?.season_number || 0);
    const e = Number(item?.episode || item?.episode_number || item?.episode?.episode_number || 0);
    return s && e ? `S${String(s).padStart(2, "0")}E${String(e).padStart(2, "0")}` : "";
  };
  const poster = (item, size = "w342") => {
    const src = String(item?.poster_url || item?.poster || item?.cover || item?.poster_cover || "");
    if (src) return src;
    const id = tmdbId(item);
    if (!id) return "/assets/img/placeholder_poster.svg";
    const kind = mediaType(item) ? "tv" : "movie";
    const title = item?.series_title || item?.title ? `&title=${encodeURIComponent(String(item?.series_title || item?.title))}` : "";
    const year = !mediaType(item) && item?.year ? `&year=${encodeURIComponent(String(item.year))}` : "";
    return `/art/tmdb/${kind}/${encodeURIComponent(id)}?kind=poster&size=${encodeURIComponent(size)}${title}${year}`;
  };
  const watchlistArtEvidence = (item) => {
    const title = item?.title ? `&title=${encodeURIComponent(String(item.title))}` : "";
    const year = item?.year ? `&year=${encodeURIComponent(String(item.year))}` : "";
    return title + year;
  };
  const tmdbBackdrop = (item) => {
    const id = tmdbId(item);
    if (!id) return "";
    const kind = mediaType(item) ? "tv" : "movie";
    return `/art/tmdb/${kind}/${encodeURIComponent(id)}?kind=backdrop&size=w1280`;
  };
  const backdrop = (item) => {
    const direct = String(item?.backdrop_url || item?.background_url || item?.background || item?.fanart || "").trim();
    if (direct) return direct;
    return tmdbBackdrop(item);
  };
  const watchlistPreviewArt = (item, size = "w300") => {
    const id = tmdbId(item);
    if (!id) return "";
    const season = Number(item?.season_number || item?.episode?.season_number || item?.episode?.season || item?.season || 0);
    const episode = Number(item?.episode_number || item?.episode?.episode_number || item?.episode?.number || item?.episode || 0);
    if (mediaType(item) && season > 0 && episode > 0) {
      return `/art/tmdb/tv/${encodeURIComponent(String(id))}?kind=still&season=${encodeURIComponent(String(season))}&episode=${encodeURIComponent(String(episode))}&size=${encodeURIComponent(size)}${watchlistArtEvidence(item)}`;
    }
    const kind = mediaType(item) ? "tv" : "movie";
    const locale = encodeURIComponent(window.__CW_LOCALE || navigator.language || "en-US");
    return `/art/tmdb/${kind}/${encodeURIComponent(String(id))}?kind=backdrop&size=${encodeURIComponent(size)}&locale=${locale}${watchlistArtEvidence(item)}`;
  };
  const watchlistWidgetArt = (item, size = "w300") => {
    const preview = window.CW?.WatchlistPreview;
    const cover = preview?.artUrl?.(item, "w342") || "/assets/img/placeholder_poster.svg";
    return preview?.gridArtUrl?.(item, size) || cover;
  };
  const heroBackdrop = (item) => tmdbBackdrop(item) || backdrop(item);
  const relTime = (value) => {
    let ts = Number(value || 0);
    if (!Number.isFinite(ts) || ts <= 0) return "";
    if (ts > 100000000000) ts = Math.floor(ts / 1000);
    const delta = Math.max(1, Math.floor(Date.now() / 1000) - ts);
    const units = [["y", 31536000], ["mo", 2592000], ["w", 604800], ["d", 86400], ["h", 3600], ["m", 60]];
    for (const [name, seconds] of units) if (delta >= seconds) return `${Math.floor(delta / seconds)}${name} ago`;
    return `${delta}s ago`;
  };
  const toast = (message, error = false) => {
    const el = $("#profile-toast");
    if (!el) return;
    el.textContent = message;
    el.classList.toggle("error", !!error);
    el.classList.remove("hidden");
    clearTimeout(el.__timer);
    el.__timer = setTimeout(() => el.classList.add("hidden"), 3200);
  };
  const empty = (text) => `<div class="cw-profile-empty">${esc(text)}</div>`;
  const skelLines = `<span class="cw-profile-skel-lines"><span class="cw-skel-line cw-skel-line--title"></span><span class="cw-skel-line cw-skel-line--meta"></span></span>`;
  const skelShapes = {
    progress: `<div class="cw-cw-card cw-dash-skeleton cw-dash-skeleton-row cw-profile-skel" aria-hidden="true"><span class="cw-cw-art cw-skel-block"></span>${skelLines}</div>`,
    watchlist: `<div class="cw-profile-row cw-dash-skeleton cw-dash-skeleton-row cw-profile-skel" aria-hidden="true"><span class="cw-skel-block"></span>${skelLines}<span class="cw-profile-skel-pill cw-skel-dot"></span></div>`,
    stats: `<div class="cw-profile-stat cw-dash-skeleton cw-dash-skeleton-row cw-profile-skel" aria-hidden="true"><span class="cw-profile-skel-icon cw-skel-block"></span>${skelLines}</div>`,
  };
  const skeleton = (kind, count) => Array.from({ length: count }, () => skelShapes[kind]).join("");

  function paintOverviewSkeletons() {
    const hosts = [["#profile-progress", "progress", 3], ["#profile-watchlist", "watchlist", 3], ["#profile-quick-stats", "stats", 6]];
    for (const [sel, kind, count] of hosts) {
      const host = $(sel);
      if (host) host.innerHTML = skeleton(kind, count);
    }
  }
  let profile = null;
  let posterSeq = 0;
  const posterItems = new Map();
  const numberFmt = new Intl.NumberFormat();

  function setAvatar(url) {
    const nodes = [$("#profile-avatar-button"), $("#cw-nav-profile-avatar")].filter(Boolean);
    for (const node of nodes) {
      if (url) node.innerHTML = `<img src="${esc(url)}" alt="">`;
      else node.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">person</span>`;
    }
  }

  function updateSharedProfile(user) {
    if (!window.CW?.AuthState?.user) return;
    window.CW.AuthState.user.avatar_url = String(user?.avatar_url || "");
    window.CW.AuthState.user.preferences = user?.preferences || {};
    try {
      window.dispatchEvent(new CustomEvent("cw:auth-state-changed", { detail: window.CW.AuthState.read?.() || {} }));
    } catch {}
  }

  function bustUrl(url) {
    const text = String(url || "");
    return text ? `${text}${text.includes("?") ? "&" : "?"}v=${Date.now()}` : "";
  }

  function setAvatarUploadState({ visible = false, label = "", percent = 0 } = {}) {
    const host = $("#profile-avatar-upload-status");
    if (!host) return;
    const pct = Math.max(0, Math.min(100, Math.round(Number(percent || 0))));
    host.classList.toggle("hidden", !visible);
    $("#profile-avatar-upload-label").textContent = label || "Uploading picture";
    $("#profile-avatar-upload-percent").textContent = `${pct}%`;
    $("#profile-avatar-upload-bar").style.width = `${pct}%`;
  }

  function uploadAvatar(dataUrl, contentType, onProgress) {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open("POST", "/api/profile/avatar");
      xhr.responseType = "json";
      xhr.timeout = 120000;
      xhr.setRequestHeader("Content-Type", "application/json");
      xhr.upload.onprogress = (event) => {
        if (!event.lengthComputable) return;
        onProgress?.(Math.min(98, Math.round((event.loaded / event.total) * 100)));
      };
      xhr.onload = () => {
        const data = xhr.response || {};
        if (xhr.status < 200 || xhr.status >= 300 || data?.ok === false) {
          reject(new Error(data?.error || `HTTP ${xhr.status}`));
          return;
        }
        resolve(data);
      };
      xhr.onerror = () => reject(new Error("Upload failed"));
      xhr.ontimeout = () => reject(new Error("Upload timed out"));
      xhr.send(JSON.stringify({ data: dataUrl, content_type: contentType }));
    });
  }

  function renderProfile(data) {
    profile = data?.user || profile || {};
    const display = String(profile.display_name || profile.label || profile.username || "Profile");
    $("#profile-display-name").textContent = display;
    $("#profile-username").textContent = `@${profile.username || ""}`;
    $("#profile-role").textContent = profile.is_admin ? "Administrator" : "Managed User";
    $("#profile-display-input").value = display;
    $("#profile-2fa-state").textContent = profile.totp_enabled ? "Enabled" : "Off";
    $("#profile-2fa-state").classList.toggle("is-enabled", !!profile.totp_enabled);
    setAvatar(profile.avatar_url || "");
    renderMemberSince(profile);
    renderPreferences(profile);
    renderSessions(profile);
    updateSharedProfile(profile);
  }

  function renderMemberSince(user) {
    const host = $("#profile-member-since");
    if (!host) return;
    const ts = Number(user?.created_at || 0);
    if (!Number.isFinite(ts) || ts <= 0) {
      host.classList.add("hidden");
      return;
    }
    const when = new Date(ts * 1000);
    host.querySelector("span:last-child").textContent =
      `Member since ${when.toLocaleDateString(undefined, { month: "long", year: "numeric" })}`;
    host.classList.remove("hidden");
  }

  function daysTracked(user) {
    const ts = Number(user?.created_at || 0);
    if (!Number.isFinite(ts) || ts <= 0) return 0;
    return Math.max(0, Math.floor((Date.now() / 1000 - ts) / 86400));
  }

  function renderHeroChips({ itemsWatched, services }) {
    const host = $("#profile-hero-chips");
    if (!host) return;
    const chips = [];
    const days = daysTracked(profile);
    if (days > 0) chips.push(["calendar_month", numberFmt.format(days), "Days tracked"]);
    chips.push(["visibility", numberFmt.format(itemsWatched || 0), "Items watched"]);
    chips.push(["hub", numberFmt.format(services || 0), "Services connected"]);
    host.innerHTML = chips.map(([icon, value, label]) => `
      <div class="cw-profile-chip">
        <span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>
        <div><strong>${esc(value)}</strong><small>${esc(label)}</small></div>
      </div>`).join("");
  }

  function renderPreferences(user) {
    const prefs = user?.preferences || {};
    const card = $("#profile-pref-playing-card");
    const quick = $("#profile-pref-quick-add");
    if (card) card.checked = prefs.playing_card !== false;
    if (quick) quick.checked = prefs.quick_add !== false;
  }

  function renderSessions(user) {
    const host = $("#profile-sessions");
    const current = user?.current_session;
    const others = Array.isArray(user?.other_sessions) ? user.other_sessions : [];
    const rows = [];
    if (current) rows.push({ ...current, current: true });
    rows.push(...others);
    host.innerHTML = rows.length ? rows.map((row) => {
      const ua = String(row.ua || "Browser").split(" ").slice(0, 5).join(" ");
      const when = relTime(row.created_at);
      const action = row.current ? `<span>Current</span>` : `<button class="cw-profile-session-kill" type="button" data-session-id="${esc(row.id)}" title="Revoke session" aria-label="Revoke session"><span class="material-symbols-rounded" aria-hidden="true">logout</span></button>`;
      return `<div class="cw-profile-row"><span class="material-symbols-rounded" aria-hidden="true">devices</span><div><strong>${esc(row.current ? "This session" : ua)}</strong><span>${esc([row.ip, when].filter(Boolean).join(" - "))}</span></div>${action}</div>`;
    }).join("") : empty("No active sessions.");
  }

  function overlayItem(item) {
    const type = mediaType(item) ? "show" : "movie";
    const tmdb = tmdbId(item);
    const ids = { ...(objectOf(item?.ids)) };
    if (tmdb) ids.tmdb = tmdb;
    return {
      ...item,
      ids,
      title: titleOf(item),
      type,
      media_type: type === "show" ? "tv" : "movie",
      tmdb,
      tmdb_id: tmdb,
      year: yearOf(item),
      poster_url: poster(item),
      backdrop_url: backdrop(item),
      episode_label: episodeOf(item),
    };
  }

  function storePosterItem(item) {
    const key = `profile-${++posterSeq}`;
    posterItems.set(key, overlayItem(item || {}));
    return key;
  }

  function progressPct(item) {
    const raw = Number(item?.progress_percent ?? item?.progress ?? item?.percent);
    if (!Number.isFinite(raw) || raw <= 0) return 0;
    return Math.max(0, Math.min(100, raw));
  }

  const progressProviders = (item) => {
    const out = [];
    const push = (value) => {
      const name = providerName(value);
      if (!name || name.toLowerCase() === "combined" || out.includes(name)) return;
      out.push(name);
    };
    for (const row of Array.isArray(item?.providers) ? item.providers : []) push(row);
    for (const row of Array.isArray(item?.sources) ? item.sources : []) push(row);
    if (!out.length) {
      push(item?.provider);
      push(item?.source);
    }
    return out;
  };

  function progressCard(item) {
    const key = storePosterItem(item);
    const pct = progressPct(item);
    const art = watchlistPreviewArt(item) || poster(item, "w780");
    const episode = episodeOf(item);
    const sub = episode || yearOf(item) || "";
    const episodeBadge = episode ? `<span class="cw-cw-episode">${esc(episode)}</span>` : "";
    const providerIcons = progressProviders(item).map(providerIconHtml).filter(Boolean).join("");
    const providerStrip = providerIcons ? `<span class="cw-cw-providers">${providerIcons}</span>` : "";
    return `<button class="cw-cw-card" type="button" data-profile-poster-key="${esc(key)}" aria-label="Show details for ${esc(titleOf(item))}">
      <span class="cw-cw-art">${episodeBadge}${providerStrip}<img src="${esc(art)}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'"></span>
      <span class="cw-cw-title">${esc(titleOf(item))}</span>
      <span class="cw-cw-sub">${esc(sub)}</span>
      <span class="cw-cw-foot">
        <span class="cw-cw-track"><i style="width:${pct}%"></i></span>
        <span class="cw-cw-pct">${pct ? `${Math.round(pct)}%` : ""}</span>
      </span>
    </button>`;
  }

  function mediaKindLabel(item) {
    const raw = String(item?.type || item?.media_type || "").toLowerCase();
    if (/episode/.test(raw) || item?.season || item?.episode) return "Episode";
    if (/tv|show|series|season|anime/.test(raw)) return "Show";
    if (/movie|film/.test(raw)) return "Movie";
    return episodeOf(item) ? "Show" : "Movie";
  }

  function addedEpoch(item) {
    for (const key of ["added_epoch", "added_at", "added", "created_at", "ts"]) {
      const raw = Number(item?.[key]);
      if (Number.isFinite(raw) && raw > 0) return raw > 1e12 ? Math.round(raw / 1000) : raw;
    }
    for (const key of ["added_when", "added_at", "added", "created_at"]) {
      const raw = item?.[key];
      if (typeof raw === "string" && raw.trim()) {
        const parsed = Date.parse(raw);
        if (Number.isFinite(parsed)) return Math.floor(parsed / 1000);
      }
    }
    return 0;
  }

  function syncedEpoch(item, fallbackEpoch = 0) {
    for (const key of ["synced_epoch", "synced_at", "updated_epoch", "updated_at", "last_synced", "last_sync_epoch"]) {
      const raw = Number(item?.[key]);
      if (Number.isFinite(raw) && raw > 0) return raw > 1e12 ? Math.round(raw / 1000) : raw;
    }
    for (const key of ["synced_at", "updated_at", "last_synced"]) {
      const raw = item?.[key];
      if (typeof raw === "string" && raw.trim()) {
        const parsed = Date.parse(raw);
        if (Number.isFinite(parsed)) return Math.floor(parsed / 1000);
      }
    }
    const fallback = Number(fallbackEpoch || 0);
    return Number.isFinite(fallback) && fallback > 0 ? (fallback > 1e12 ? Math.round(fallback / 1000) : fallback) : 0;
  }

  function watchlistRow(item, fallbackSyncEpoch = 0) {
    const key = storePosterItem(item);
    const kind = mediaKindLabel(item);
    const year = yearOf(item);
    const when = addedEpoch(item);
    const syncedWhen = syncedEpoch(item, fallbackSyncEpoch);
    const meta = [kind, year, when ? `updated ${relTime(when)}` : ""].filter(Boolean).join(" - ");
    const art = watchlistWidgetArt(item);
    const synced = item?.synced === false || item?.is_synced === false ? "" : `<span class="cw-profile-watchlist-status">Synced</span>`;
    const syncBadge = syncedWhen ? `<span class="cw-profile-watchlist-sync">${esc(relTime(syncedWhen))}</span>` : "";
    return `<button class="cw-profile-row cw-profile-click-row" type="button" data-profile-poster-key="${esc(key)}" aria-label="Show details for ${esc(titleOf(item))}">
      <span class="cw-profile-watchlist-art">${syncBadge}<img src="${esc(art)}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'"></span>
      <div class="cw-profile-watchlist-copy"><strong>${esc(titleOf(item))}</strong><span>${esc(meta)}</span></div>
      ${synced}
    </button>`;
  }

  function posterCard(item) {
    const meta = [episodeOf(item) || yearOf(item), String(item?.type || item?.media_type || "").replace("_", " ")].filter(Boolean).join(" - ");
    const key = storePosterItem(item);
    return `<button class="cw-profile-poster" type="button" data-profile-poster-key="${esc(key)}" aria-label="Show details for ${esc(titleOf(item))}"><img src="${esc(poster(item))}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'"><span>${esc(titleOf(item))}<small>${esc(meta)}</small></span></button>`;
  }

  function listRow(item, fallbackIcon = "movie") {
    const source = providerOf(item);
    const meta = [visibleProviderLabel(source), episodeOf(item) || yearOf(item), relTime(item?.ts || item?.last_watched_at || item?.watched_at || item?.updated_at)].filter(Boolean).join(" - ");
    const key = storePosterItem(item);
    return `<button class="cw-profile-row cw-profile-click-row" type="button" data-profile-poster-key="${esc(key)}" aria-label="Show details for ${esc(titleOf(item))}"><img src="${esc(poster(item, "w185"))}" alt="" loading="lazy" onerror="this.onerror=null;this.replaceWith(Object.assign(document.createElement('span'),{className:'material-symbols-rounded',textContent:'${fallbackIcon}'}))"><div><strong>${esc(titleOf(item))}</strong><span>${esc(meta)}</span></div><span></span></button>`;
  }

  const watchedEpoch = (item) => {
    const raw = Number(item?.ts || item?.sort_epoch || item?.last_watched_at || item?.watched_at || 0);
    if (!Number.isFinite(raw) || raw <= 0) return 0;
    return raw > 100000000000 ? Math.floor(raw / 1000) : raw;
  };

  const newestItem = (rows) => {
    if (!Array.isArray(rows) || !rows.length) return null;
    return rows.reduce((best, row) => (watchedEpoch(row) > watchedEpoch(best) ? row : best), rows[0]) || null;
  };

  function bindLastWatchedPreview(node) {
    if (!node || node.dataset.previewBound === "1") return;
    node.dataset.previewBound = "1";
    const openLast = (event) => {
      const current = posterItems.get(node.dataset.profilePosterKey || "");
      const open = window.CW?.WatchlistPreview?.openPreviewDrawer || window.openPreviewDrawer;
      if (!current || !open) return;
      event?.preventDefault?.();
      event?.stopPropagation?.();
      void open(current);
    };
    node.addEventListener("click", openLast);
    node.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      openLast(event);
    });
  }

  function renderHero(scrobbleItems, historyItems) {
    const item = newestItem(scrobbleItems) || newestItem(historyItems);
    const hero = $("#profile-hero");
    const last = $(".cw-profile-last");
    const art = item ? heroBackdrop(item) : "";
    const artHost = $("#profile-hero-art");
    artHost.style.backgroundImage = art ? `url("${art.replace(/"/g, "%22")}")` : "";
    hero?.classList.toggle("has-last", !!item);
    hero?.classList.toggle("no-last", !item);
    last?.classList.toggle("hidden", !item);
    if (!item) {
      if (last) {
        delete last.dataset.profilePosterKey;
        last.removeAttribute("aria-label");
      }
      return;
    }
    const key = storePosterItem(item);
    if (last) {
      last.dataset.profilePosterKey = key;
      last.setAttribute("aria-label", `Show details for ${titleOf(item)}`);
      bindLastWatchedPreview(last);
    }
    $("#profile-last-poster").src = poster(item, "w185");
    $("#profile-last-title").textContent = titleOf(item);
    $("#profile-last-meta").textContent = [episodeOf(item) || yearOf(item), relTime(watchedEpoch(item))].filter(Boolean).join(" - ");
    const route = providerRoute(item);
    const sourceHtml = providerIconHtml(route.source);
    const sinkHtml = route.sinks.map(providerIconHtml).filter(Boolean).join("");
    const sep = sourceHtml && sinkHtml && route.routed
      ? `<span class="cw-profile-provider-sep material-symbols-rounded" aria-hidden="true">chevron_right</span>`
      : "";
    const badges = `${sourceHtml}${sep}${sinkHtml}`;
    const providerNode = $("#profile-last-provider");
    if (providerNode) {
      providerNode.innerHTML = badges;
      providerNode.hidden = !badges;
    }
  }

  function nowPlayingItem(payload) {
    const streams = Array.isArray(payload?.streams) ? payload.streams : [];
    const active = streams.find((row) => /play|start|resume|watch/i.test(String(row?.state || "")));
    const item = active || streams[0] || (payload && payload.title ? payload : null);
    if (!item) return null;
    const pct = Number(item.progress ?? item.progress_percent ?? item.percent);
    return { ...item, _pct: Number.isFinite(pct) ? Math.max(0, Math.min(100, pct)) : 0 };
  }

  function renderNowPlaying(payload) {
    const hero = $("#profile-hero");
    const layer = $("#profile-hero-now");
    const panel = $("#profile-now");
    if (!hero || !layer || !panel) return;
    const item = nowPlayingItem(payload);
    if (!item) {
      hero.classList.remove("is-playing");
      layer.style.backgroundImage = "";
      const posterNode = $("#profile-now-poster");
      if (posterNode) posterNode.src = "/assets/img/placeholder_poster.svg";
      panel.classList.add("hidden");
      return;
    }
    const art = heroBackdrop(item);
    layer.style.backgroundImage = art ? `url("${String(art).replace(/"/g, "%22")}")` : "";
    const posterNode = $("#profile-now-poster");
    if (posterNode) {
      posterNode.onerror = () => {
        posterNode.onerror = null;
        posterNode.src = "/assets/img/placeholder_poster.svg";
      };
      posterNode.src = poster(item, "w185");
    }
    $("#profile-now-title").textContent = titleOf(item);
    $("#profile-now-meta").textContent = [
      visibleProviderLabel(providerOf(item)),
      episodeOf(item) || yearOf(item),
    ].filter(Boolean).join(" - ");
    const pct = item._pct;
    $("#profile-now-fill").style.width = `${pct}%`;
    $("#profile-now-pct").textContent = pct ? `${Math.round(pct)}%` : "";
    const runtime = durationMinutes(item);
    const left = runtime > 0 && pct > 0 ? Math.max(0, Math.round(runtime * (1 - pct / 100))) : 0;
    $("#profile-now-left").textContent = left ? `${left} min left` : "";
    panel.classList.remove("hidden");
    hero.classList.add("is-playing");
  }

  async function refreshNowPlaying() {
    try {
      renderNowPlaying(await api("/api/watch/currently_watching"));
    } catch {
      renderNowPlaying(null);
    }
  }

  function statType(item) {
    const raw = mediaValue(item);
    if (/episode|anime_episode/.test(raw) || item?.season || item?.episode || item?.episode_label) return "episode";
    if (/tv|show|shows|series|season|anime/.test(raw)) return "show";
    return "movie";
  }

  function statsFromItems(items) {
    const seen = { movie: new Set(), show: new Set(), episode: new Set() };
    for (const item of Array.isArray(items) ? items : []) {
      const type = statType(item);
      const key = String(item?.key || item?.id || tmdbId(item) || `${type}:${titleOf(item)}:${yearOf(item)}:${episodeOf(item)}`).toLowerCase();
      if (key) seen[type].add(key);
    }
    return { movies: seen.movie.size, shows: seen.show.size, episodes: seen.episode.size };
  }

  function durationMinutes(item) {
    const direct = Number(item?.runtime_minutes || item?.duration_minutes);
    if (Number.isFinite(direct) && direct > 0) return direct;
    const ms = Number(item?.duration_ms);
    if (Number.isFinite(ms) && ms > 0) return ms / 60000;
    const raw = Number(item?.duration || item?.runtime);
    if (!Number.isFinite(raw) || raw <= 0) return 0;
    if (raw > 10000) return raw / 60000;
    if (raw > 300) return raw / 60;
    return raw;
  }

  function renderQuickStats({ wall, widgets, progressItems, insights }) {
    const history = widgets?.recent_history?.items || [];
    const ratings = widgets?.latest_ratings?.items || [];
    const scrobble = widgets?.recent_scrobble?.items || [];
    const sampled = [...(wall?.items || []), ...history, ...ratings, ...scrobble, ...(progressItems || [])];
    const sampleStats = statsFromItems(sampled);
    const breakdown = insights?.features?.history?.breakdown || {};
    const watchtime = insights?.watchtime || {};
    const movies = Number(breakdown.movies ?? watchtime.movies ?? sampleStats.movies) || 0;
    const shows = Number(breakdown.shows ?? watchtime.shows ?? sampleStats.shows) || 0;
    const anime = Number(breakdown.anime) || 0;
    const episodes = Number(breakdown.episodes ?? sampleStats.episodes) || 0;
    const watchlist = Number(wall?.total ?? (wall?.items || []).length) || 0;
    const fallbackHours = scrobble.reduce((sum, item) => sum + durationMinutes(item), 0) / 60;
    const hours = Number(widgets?.recent_scrobble?.scrobble_hours ?? fallbackHours) || 0;
    const values = [
      ["movies", "movie", "theaters", "Movies", numberFmt.format(movies), "Total movies in your syncs"],
      ["tv", "tv", "live_tv", "TV Shows", numberFmt.format(shows), "Total TV shows in your syncs"],
      ["anime", "animation", "auto_awesome", "Anime", numberFmt.format(anime), "Total anime in your syncs"],
      ["episodes", "subscriptions", "stacked_bar_chart", "Episodes", numberFmt.format(episodes), "Total episodes in your syncs"],
      ["watchlist", "bookmark", "format_list_bulleted", "Watchlist Items", numberFmt.format(watchlist), "Items on your watchlist"],
      ["hours", "schedule", "show_chart", "Hours Watched", hours ? `${numberFmt.format(Math.round(hours * 10) / 10)} h` : "0 h", "Total time spent watching"],
    ];
    $("#profile-quick-stats").innerHTML = values.map(([key, icon, backdropIcon, label, value, description]) => `
      <div class="cw-profile-stat cw-profile-stat--${esc(key)}" data-stat="${esc(key)}" data-stat-bg="${esc(backdropIcon)}">
        <span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>
        <div class="cw-profile-stat-copy"><small>${esc(label)}</small><strong>${esc(value)}</strong><span>${esc(description)}</span></div>
      </div>`).join("");
  }

  async function loadOverview() {
    posterSeq = 0;
    posterItems.clear();
    paintOverviewSkeletons();
    const [widgets, wall, progress, insights, status] = await Promise.all([
      api("/api/dashboard/widgets?include=history,ratings,scrobble,progress&history_limit=8&ratings_limit=9&scrobble_limit=8&progress_limit=8"),
      api("/api/state/wall?limit=8"),
      api("/api/playback_progress/items?page=1&page_size=8").catch(() => ({ items: [] })),
      api("/api/insights?limit_samples=0&history=0&runtime=0&include_events=0").catch(() => null),
      api("/api/status").catch(() => null),
    ]);
    const history = widgets?.recent_history?.items || [];
    const ratings = widgets?.latest_ratings?.items || [];
    const scrobble = widgets?.recent_scrobble?.items || [];
    const widgetProgress = widgets?.recent_progress?.items || [];
    const progressItems = progress?.items?.length ? progress.items : widgetProgress;
    const scrobbleTotal = Number(widgets?.recent_scrobble?.scrobble_total ?? widgets?.recent_scrobble?.total ?? scrobble.length) || 0;
    renderHero(scrobble, history);
    renderQuickStats({ wall, widgets, progressItems, insights });
    const watchlistItems = Array.isArray(wall?.items) ? wall.items : [];
    renderHeroChips({
      itemsWatched: scrobbleTotal,
      services: connectedServices(status),
    });
    $("#profile-progress").innerHTML = progressItems.length ? progressItems.slice(0, 9).map(progressCard).join("") : empty("No recent progress yet.");
    $("#profile-watchlist").innerHTML = watchlistItems.length ? watchlistItems.slice(0, 3).map((item) => watchlistRow(item, wall?.last_sync_epoch)).join("") : empty("No watchlist items yet.");
    refreshNowPlaying();
  }

  function connectedServices(status) {
    const providers = status?.providers;
    if (!providers || typeof providers !== "object") return 0;
    let total = 0;
    for (const entry of Object.values(providers)) {
      if (!entry || typeof entry !== "object") continue;
      const instances = entry.instances;
      if (instances && typeof instances === "object") {
        const connected = Object.values(instances).filter((row) => row && row.connected === true).length;
        if (connected) { total += connected; continue; }
      }
      if (entry.connected === true) total += 1;
    }
    return total;
  }

  async function refreshProfile() {
    const data = await api("/api/profile");
    renderProfile(data);
    return data;
  }

  function wireTabs() {
    document.querySelectorAll("[data-profile-tab]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const key = btn.dataset.profileTab;
        document.querySelectorAll("[data-profile-tab]").forEach((tab) => tab.classList.toggle("active", tab === btn));
        document.querySelectorAll(".cw-profile-panel").forEach((panel) => panel.classList.toggle("active", panel.id === `profile-panel-${key}`));
      });
    });
  }

  function wireAvatar() {
    const input = $("#profile-avatar-input");
    const pick = () => input?.click();
    $("#profile-avatar-button")?.addEventListener("click", pick);
    $("#profile-avatar-replace")?.addEventListener("click", pick);
    $("#profile-avatar-remove")?.addEventListener("click", async () => {
      try {
        const data = await del("/api/profile/avatar");
        renderProfile(data);
        toast("Profile picture removed");
      } catch (e) {
        toast(e.message || "Could not remove profile picture", true);
      }
    });
    input?.addEventListener("change", () => {
      const file = input.files?.[0];
      if (!file) return;
      if (file.size > 5 * 1024 * 1024) {
        toast("Profile picture must be 5 MB or smaller", true);
        input.value = "";
        return;
      }
      const allowed = ["image/png", "image/jpeg", "image/webp"];
      if (!allowed.includes(file.type)) {
        toast("Use PNG, JPG or WebP", true);
        input.value = "";
        return;
      }
      const reader = new FileReader();
      setAvatarUploadState({ visible: true, label: "Reading picture", percent: 3 });
      reader.onerror = () => {
        setAvatarUploadState({ visible: false });
        toast("Could not read profile picture", true);
        input.value = "";
      };
      reader.onprogress = (event) => {
        if (!event.lengthComputable) return;
        setAvatarUploadState({ visible: true, label: "Reading picture", percent: Math.min(45, Math.round((event.loaded / event.total) * 45)) });
      };
      reader.onload = async () => {
        const preview = String(reader.result || "");
        try {
          if (preview) setAvatar(preview);
          setAvatarUploadState({ visible: true, label: "Uploading picture", percent: 50 });
          const data = await uploadAvatar(preview, file.type, (percent) => {
            setAvatarUploadState({ visible: true, label: "Uploading picture", percent: Math.max(50, percent) });
          });
          if (data?.user?.avatar_url) data.user.avatar_url = bustUrl(data.user.avatar_url);
          setAvatarUploadState({ visible: true, label: "Saving picture", percent: 100 });
          renderProfile(data);
          toast("Profile picture updated");
        } catch (e) {
          setAvatar(profile?.avatar_url || "");
          toast(e.message || "Could not upload profile picture", true);
        } finally {
          setTimeout(() => setAvatarUploadState({ visible: false }), 500);
          input.value = "";
        }
      };
      reader.readAsDataURL(file);
    });
  }

  function wireForms() {
    $("#profile-name-form")?.addEventListener("submit", async (event) => {
      event.preventDefault();
      try {
        const data = await post("/api/profile", { display_name: $("#profile-display-input")?.value || "" });
        renderProfile(data);
        toast("Profile saved");
      } catch (e) {
        toast(e.message || "Could not save profile", true);
      }
    });
    $("#profile-password-form")?.addEventListener("submit", async (event) => {
      event.preventDefault();
      try {
        const data = await post("/api/profile/password", {
          current_password: $("#profile-current-password")?.value || "",
          new_password: $("#profile-new-password")?.value || "",
        });
        renderProfile(data);
        $("#profile-current-password").value = "";
        $("#profile-new-password").value = "";
        toast("Password changed");
      } catch (e) {
        toast(e.message || "Could not change password", true);
      }
    });
  }

  function recoveryCodesList(codes) {
    const wrap = document.createElement("div");
    wrap.className = "cw-profile-codes";
    const title = document.createElement("strong");
    title.textContent = "Recovery codes";
    wrap.appendChild(title);
    for (const value of codes) {
      const code = document.createElement("code");
      code.textContent = value;
      wrap.appendChild(code);
    }
    return wrap;
  }

  function showRecoveryCodes(codes) {
    const host = $("#profile-2fa-setup");
    if (!host) return;
    const values = (Array.isArray(codes) ? codes : []).map((code) => String(code || "").trim()).filter(Boolean);
    host.replaceChildren();
    const wrap = document.createElement("div");
    wrap.className = "cw-profile-codes cw-profile-codes--locked";
    const title = document.createElement("strong");
    title.textContent = values.length ? "Recovery codes ready" : "No recovery codes available";
    const copy = document.createElement("span");
    copy.textContent = values.length
      ? "These one-time codes are hidden until you reveal them."
      : "No new recovery codes were returned.";
    wrap.append(title, copy);
    if (values.length) {
      const reveal = document.createElement("button");
      reveal.className = "btn";
      reveal.type = "button";
      reveal.textContent = "Show recovery codes";
      reveal.addEventListener("click", () => {
        host.replaceChildren(recoveryCodesList(values));
      }, { once: true });
      wrap.appendChild(reveal);
    }
    host.appendChild(wrap);
  }

  function renderPlexStatus(status) {
    const state = $("#profile-plex-state");
    const summary = $("#profile-plex-summary");
    const link = $("#profile-plex-link");
    const unlink = $("#profile-plex-unlink");
    const linked = !!status?.linked;
    if (state) {
      state.textContent = linked ? "Linked" : "Off";
      state.classList.toggle("is-enabled", linked);
    }
    if (summary) {
      const name = String(status?.linked_username || status?.linked_email || "").trim();
      const email = String(status?.linked_email || "").trim();
      summary.innerHTML = linked ? `<strong>${esc(name || "Plex account")}</strong>${email && email !== name ? esc(email) : ""}` : "No Plex account linked.";
    }
    if (link) link.textContent = linked ? "Replace Plex account" : "Link Plex account";
    if (unlink) unlink.classList.toggle("hidden", !linked);
  }

  async function refreshPlexStatus() {
    try {
      renderPlexStatus(await api("/api/app-auth/plex/status"));
    } catch {
      renderPlexStatus({ linked: false });
    }
  }

  async function pollPlexLink(state) {
    for (let i = 0; i < 90; i += 1) {
      const data = await post("/api/app-auth/plex/link/check", { state });
      if (!data.pending) return data;
      await new Promise((resolve) => setTimeout(resolve, 1800));
    }
    throw new Error("Plex link timed out");
  }

  function wirePlexSso() {
    $("#profile-plex-link")?.addEventListener("click", async () => {
      try {
        const data = await post("/api/app-auth/plex/link/start", {});
        if (data.auth_url) window.open(data.auth_url, "cwPlexLink", "popup,width=720,height=760");
        toast("Finish linking in the Plex window");
        const done = await pollPlexLink(data.state);
        renderPlexStatus(done);
        toast("Plex account linked");
      } catch (e) {
        toast(e.message || "Could not link Plex account", true);
      }
    });
    $("#profile-plex-unlink")?.addEventListener("click", async () => {
      try {
        renderPlexStatus(await post("/api/app-auth/plex/unlink", {}));
        toast("Plex account unlinked");
      } catch (e) {
        toast(e.message || "Could not unlink Plex account", true);
      }
    });
  }

  function renderOidcStatus(status) {
    const state = $("#profile-oidc-state");
    const summary = $("#profile-oidc-summary");
    const link = $("#profile-oidc-link");
    const unlink = $("#profile-oidc-unlink");
    const configured = !!status?.configured;
    const linked = !!status?.linked;
    if (state) {
      state.textContent = linked ? "Linked" : (configured ? "Off" : "Unavailable");
      state.classList.toggle("is-enabled", linked);
    }
    if (summary) {
      const name = String(status?.linked_username || status?.linked_email || "").trim();
      const email = String(status?.linked_email || "").trim();
      summary.innerHTML = !configured
        ? "OIDC is not configured by the administrator."
        : linked
          ? `<strong>${esc(name || "OIDC account")}</strong>${email && email !== name ? esc(email) : ""}`
          : "No OIDC account linked.";
    }
    if (link) {
      link.textContent = linked ? "Replace OIDC account" : "Link OIDC account";
      link.disabled = !configured;
    }
    if (unlink) unlink.classList.toggle("hidden", !linked);
  }

  async function refreshOidcStatus() {
    try {
      renderOidcStatus(await api("/api/app-auth/oidc/status"));
    } catch {
      renderOidcStatus({ configured: false, linked: false });
    }
  }

  async function pollOidcLink(state) {
    for (let i = 0; i < 90; i += 1) {
      const data = await post("/api/app-auth/oidc/link/check", { state });
      if (!data.pending) return data;
      await new Promise((resolve) => setTimeout(resolve, 1800));
    }
    throw new Error("OIDC link timed out");
  }

  function wireOidcSso() {
    $("#profile-oidc-link")?.addEventListener("click", async () => {
      try {
        const data = await post("/api/app-auth/oidc/link/start", {});
        if (data.auth_url) window.open(data.auth_url, "cwOidcLink", "popup,width=720,height=760");
        toast("Finish linking in the OIDC window");
        const done = await pollOidcLink(data.state);
        renderOidcStatus(done);
        toast("OIDC account linked");
      } catch (e) {
        toast(e.message || "Could not link OIDC account", true);
      }
    });
    $("#profile-oidc-unlink")?.addEventListener("click", async () => {
      try {
        renderOidcStatus(await post("/api/app-auth/oidc/unlink", {}));
        toast("OIDC account unlinked");
      } catch (e) {
        toast(e.message || "Could not unlink OIDC account", true);
      }
    });
  }

  function wireSecurity() {
    $("#profile-sessions")?.addEventListener("click", async (event) => {
      const btn = event.target?.closest?.(".cw-profile-session-kill");
      if (!btn) return;
      try {
        const data = await del(`/api/profile/sessions/${encodeURIComponent(btn.dataset.sessionId || "")}`);
        renderProfile(data);
        toast("Session revoked");
      } catch (e) {
        toast(e.message || "Could not revoke session", true);
      }
    });
    $("#profile-2fa-setup-btn")?.addEventListener("click", async () => {
      try {
        const data = await post("/api/profile/totp/setup", {});
        $("#profile-2fa-setup").innerHTML = `<div class="cw-profile-qr"><div class="cw-profile-qr-code">${data.qr_svg || '<span class="material-symbols-rounded" aria-hidden="true">qr_code_2</span>'}</div><div class="cw-profile-qr-copy"><strong>Scan with your authenticator app</strong><span>Or enter this setup key manually.</span><code>${esc(data.secret || "")}</code><div class="cw-profile-qr-verify"><input id="profile-2fa-code" placeholder="123456" inputmode="numeric" autocomplete="one-time-code"><button id="profile-2fa-verify-now" class="btn primary" type="button">Verify</button></div></div></div>`;
        $("#profile-2fa-verify-now")?.addEventListener("click", async () => {
          try {
            const done = await post("/api/profile/totp/verify", { code: $("#profile-2fa-code")?.value || "" });
            renderProfile(done);
            if (Array.isArray(done.recovery_codes)) showRecoveryCodes(done.recovery_codes);
            toast("Two-factor authentication enabled");
          } catch (e) {
            toast(e.message || "Invalid verification code", true);
          }
        });
      } catch (e) {
        toast(e.message || "Could not start 2FA setup", true);
      }
    });
    $("#profile-2fa-disable-btn")?.addEventListener("click", async () => {
      const current = prompt("Current password");
      if (!current) return;
      try {
        const data = await post("/api/profile/totp/disable", { current_password: current });
        renderProfile(data);
        $("#profile-2fa-setup").innerHTML = "";
        toast("Two-factor authentication disabled");
      } catch (e) {
        toast(e.message || "Could not disable 2FA", true);
      }
    });
    $("#profile-recovery-btn")?.addEventListener("click", async () => {
      const current = prompt("Current password");
      if (!current) return;
      try {
        const data = await post("/api/profile/recovery-codes", { current_password: current });
        renderProfile(data);
        showRecoveryCodes(data.recovery_codes || []);
        toast("Recovery codes generated");
      } catch (e) {
        toast(e.message || "Could not generate recovery codes", true);
      }
    });
    $("#profile-revoke-sessions")?.addEventListener("click", async () => {
      try {
        const data = await post("/api/profile/sessions/revoke-others", {});
        renderProfile(data);
        toast("Other sessions revoked");
      } catch (e) {
        toast(e.message || "Could not revoke sessions", true);
      }
    });
  }

  function wireLogout() {
    $("#cw-profile-logout")?.addEventListener("click", async () => {
      try {
        await post("/api/app-auth/logout", {});
      } catch {}
      location.href = "/login";
    });
  }

  function wirePosterOverlay() {
    const openFromTarget = (target, event) => {
      const btn = target?.closest?.("[data-profile-poster-key]");
      if (!btn) return false;
      const item = posterItems.get(btn.dataset.profilePosterKey || "");
      const open = window.CW?.WatchlistPreview?.openPreviewDrawer || window.openPreviewDrawer;
      if (!item || !open) return false;
      event?.preventDefault?.();
      void open(item);
      return true;
    };
    const shell = $(".cw-profile-shell");
    shell?.addEventListener("click", (event) => {
      openFromTarget(event.target, event);
    });
    shell?.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      const btn = event.target?.closest?.("[data-profile-poster-key]");
      if (!btn) return;
      openFromTarget(btn, event);
    });
  }

  function wirePreferences() {
    $("#profile-pref-save")?.addEventListener("click", async (event) => {
      const btn = event.currentTarget;
      btn.disabled = true;
      try {
        const data = await api("/api/profile", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            preferences: {
              playing_card: $("#profile-pref-playing-card")?.checked !== false,
              quick_add: $("#profile-pref-quick-add")?.checked !== false,
            },
          }),
        });
        renderProfile(data);
        toast("Preferences saved");
      } catch (e) {
        toast(e.message || "Preferences could not be saved", true);
      } finally {
        btn.disabled = false;
      }
    });
  }

  let nowTimer = null;

  async function init() {
    paintOverviewSkeletons();
    wireTabs();
    wirePreferences();
    wireAvatar();
    wireForms();
    wireSecurity();
    wirePlexSso();
    wireOidcSso();
    wireLogout();
    wirePosterOverlay();
    try {
      await refreshProfile();
      await refreshPlexStatus();
      await refreshOidcStatus();
      await loadOverview();
      if (nowTimer) clearInterval(nowTimer);
      nowTimer = setInterval(() => {
        if (document.visibilityState === "visible") refreshNowPlaying();
      }, 15000);
    } catch (e) {
      toast(e.message || "Profile could not be loaded", true);
    }
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init, { once: true });
  else init();
})();
