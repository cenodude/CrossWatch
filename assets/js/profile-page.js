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
  const OVERVIEW_CACHE_TTL_MS = 10 * 60 * 1000;
  const profileCacheKey = (name) => {
    const shell = $(".cw-profile-shell");
    const id = shell?.dataset?.profileId || shell?.dataset?.username || document.documentElement?.dataset?.cwProfileId || "self";
    return `cw.profile.${id}.${name}.v1`;
  };
  const readCache = (key, ttl = OVERVIEW_CACHE_TTL_MS) => {
    try {
      const entry = JSON.parse(localStorage.getItem(key) || "null");
      if (!entry || !entry.payload || Date.now() - Number(entry.t || 0) > ttl) return null;
      return entry.payload;
    } catch {
      return null;
    }
  };
  const writeCache = (key, payload) => {
    try { localStorage.setItem(key, JSON.stringify({ t: Date.now(), payload })); } catch {}
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
  const providerLogLogo = (provider) => window.CW?.ProviderMeta?.logLogoPath?.(provider) || providerLogo(provider);
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
  const isEpisodeItem = (item) => /^(episode|anime_episode)$/i.test(mediaValue(item)) || !!(item?.episode_label || item?.episodeLabel || item?.episode_number || item?.season_number || item?.season || objectOf(item?.episode).season_number || objectOf(item?.episode).number);
  const tmdbId = (item) => {
    const ids = objectOf(item?.ids);
    const meta = objectOf(item?.provider_metadata);
    const show = objectOf(item?.show || item?.series || item?.anime);
    const showIds = objectOf(meta.show_ids);
    const nestedShowIds = objectOf(show.ids);
    if (mediaValue(item) === "movie") return item?.tmdb || item?.tmdb_id || ids.tmdb || ids.id || "";
    return showIds.tmdb || nestedShowIds.tmdb || show.tmdb || show.tmdb_id || ids.tmdb_show || item?.tmdb_show || ids.show_tmdb || item?.show_tmdb || item?.tmdb || item?.tmdb_id || ids.tmdb || "";
  };
  const showTmdbId = (item) => {
    const ids = objectOf(item?.ids);
    const meta = objectOf(item?.provider_metadata);
    const show = objectOf(item?.show || item?.series || item?.anime);
    const showIds = objectOf(meta.show_ids);
    const nestedShowIds = objectOf(show.ids);
    const directShowIds = objectOf(item?.show_ids);
    return String(
      showIds.tmdb || nestedShowIds.tmdb || directShowIds.tmdb || show.tmdb || show.tmdb_id
      || ids.tmdb_show || item?.tmdb_show || ids.show_tmdb || item?.show_tmdb || ""
    ).trim();
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
    const show = objectOf(item?.show || item?.series || item?.anime);
    const src = String(isEpisodeItem(item)
      ? (item?.show_cover_url || item?.show_cover || item?.show_poster_url || item?.show_poster || item?.series_poster || item?.series_cover || item?.grandparentThumb || show.poster_url || show.poster || show.cover || show.poster_cover || item?.poster_cover || "")
      : (item?.poster_url || item?.poster || item?.cover || item?.poster_cover || ""));
    if (src) return src;
    const episode = isEpisodeItem(item);
    const id = episode ? showTmdbId(item) : tmdbId(item);
    if (!id) return "/assets/img/placeholder_poster.svg";
    const kind = mediaType(item) ? "tv" : "movie";
    const title = !isEpisodeItem(item) && (item?.series_title || item?.title) ? `&title=${encodeURIComponent(String(item?.series_title || item?.title))}` : "";
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
    collection: `<div class="cw-collection-skel cw-dash-skeleton cw-profile-skel" aria-hidden="true"><span class="cw-collection-skel-art cw-skel-block"></span>${skelLines}</div>`,
    collectionRow: `<div class="cw-collection-skel cw-collection-skel--row cw-dash-skeleton cw-profile-skel" aria-hidden="true"><span class="cw-collection-skel-art cw-skel-block"></span>${skelLines}</div>`,
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

  const readFileAsDataUrl = (file, onProgress) => new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(new Error("Could not read profile picture"));
    reader.onprogress = (event) => {
      if (!event.lengthComputable) return;
      onProgress?.(Math.min(45, Math.round((event.loaded / event.total) * 45)));
    };
    reader.onload = () => resolve(String(reader.result || ""));
    reader.readAsDataURL(file);
  });

  const loadImage = (url) => new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => resolve(img);
    img.onerror = () => reject(new Error("Could not decode profile picture"));
    img.src = url;
  });

  const canvasToBlob = (canvas, type, quality) => new Promise((resolve) => {
    try { canvas.toBlob(resolve, type, quality); } catch { resolve(null); }
  });

  async function prepareAvatarUpload(file) {
    const previewUrl = URL.createObjectURL(file);
    try {
      const img = await loadImage(previewUrl);
      const size = 320;
      const canvas = document.createElement("canvas");
      canvas.width = size;
      canvas.height = size;
      const ctx = canvas.getContext("2d", { alpha: false });
      if (!ctx) throw new Error("Could not prepare profile picture");
      ctx.fillStyle = "#101217";
      ctx.fillRect(0, 0, size, size);
      const source = Math.min(img.naturalWidth || img.width, img.naturalHeight || img.height);
      const sx = Math.max(0, ((img.naturalWidth || img.width) - source) / 2);
      const sy = Math.max(0, ((img.naturalHeight || img.height) - source) / 2);
      ctx.drawImage(img, sx, sy, source, source, 0, 0, size, size);
      const blob = await canvasToBlob(canvas, "image/webp", 0.84) || await canvasToBlob(canvas, "image/jpeg", 0.88);
      if (!blob) throw new Error("Could not compress profile picture");
      const dataUrl = await readFileAsDataUrl(blob);
      return { previewUrl, dataUrl, contentType: blob.type || "image/jpeg" };
    } catch {
      const dataUrl = await readFileAsDataUrl(file);
      return { previewUrl, dataUrl, contentType: file.type };
    }
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

  function prunePosterItems() {
    if (posterItems.size <= 600) return;
    const live = new Set();
    document.querySelectorAll("[data-profile-poster-key]").forEach((node) => {
      const key = node.dataset.profilePosterKey;
      if (key) live.add(key);
    });
    for (const key of [...posterItems.keys()]) {
      if (!live.has(key)) posterItems.delete(key);
    }
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
    const lastPoster = $("#profile-last-poster");
    if (lastPoster) {
      lastPoster.onerror = () => {
        lastPoster.onerror = null;
        lastPoster.src = "/assets/img/placeholder_poster.svg";
      };
      lastPoster.src = poster(item, "w185");
    }
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

  function renderQuickStats({ wall, widgets, progressItems, insights, collection }) {
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
    const collectionCounts = collection?.counts || {};
    const owned = (Number(collectionCounts.movie) || 0) + (Number(collectionCounts.show) || 0);
    const watchlist = Number(wall?.total ?? (wall?.items || []).length) || 0;
    const fallbackHours = scrobble.reduce((sum, item) => sum + durationMinutes(item), 0) / 60;
    const hours = Number(widgets?.recent_scrobble?.scrobble_hours ?? fallbackHours) || 0;
    const values = [
      ["movies", "movie", "theaters", "Movies", numberFmt.format(movies), "Total movies in your syncs"],
      ["tv", "tv", "live_tv", "TV Shows", numberFmt.format(shows), "Total TV shows in your syncs"],
      ["anime", "animation", "auto_awesome", "Anime", numberFmt.format(anime), "Total anime in your syncs"],
      ["collections", "inventory_2", "video_library", "Collections", numberFmt.format(owned), "Movies and shows you own"],
      ["watchlist", "bookmark", "format_list_bulleted", "Watchlist Items", numberFmt.format(watchlist), "Items on your watchlist"],
      ["hours", "schedule", "show_chart", "Hours Watched", hours ? `${numberFmt.format(Math.round(hours * 10) / 10)} h` : "0 h", "Total time spent watching"],
    ];
    $("#profile-quick-stats").innerHTML = values.map(([key, icon, backdropIcon, label, value, description]) => `
      <div class="cw-profile-stat cw-profile-stat--${esc(key)}" data-stat="${esc(key)}" data-stat-bg="${esc(backdropIcon)}">
        <span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>
        <div class="cw-profile-stat-copy"><small>${esc(label)}</small><strong>${esc(value)}</strong><span>${esc(description)}</span></div>
      </div>`).join("");
  }

  function renderProgressItems(progressItems) {
    $("#profile-progress").innerHTML = progressItems.length ? progressItems.slice(0, 9).map(progressCard).join("") : empty("No recent progress yet.");
  }

  function renderOverview(payload = {}) {
    prunePosterItems();
    const widgets = payload.widgets || {};
    const wall = payload.wall || { items: [] };
    const progress = payload.progress || { items: [] };
    const insights = payload.insights || null;
    const status = payload.status || null;
    const collection = payload.collection || null;
    const history = widgets?.recent_history?.items || [];
    const ratings = widgets?.latest_ratings?.items || [];
    const scrobble = widgets?.recent_scrobble?.items || [];
    const widgetProgress = widgets?.recent_progress?.items || [];
    const progressItems = progress?.items?.length ? progress.items : widgetProgress;
    const scrobbleTotal = Number(widgets?.recent_scrobble?.scrobble_total ?? widgets?.recent_scrobble?.total ?? scrobble.length) || 0;
    renderHero(scrobble, history);
    renderQuickStats({ wall, widgets, progressItems, insights, collection });
    const watchlistItems = Array.isArray(wall?.items) ? wall.items : [];
    renderHeroChips({
      itemsWatched: scrobbleTotal,
      services: connectedServices(status),
    });
    renderProgressItems(progressItems);
    $("#profile-watchlist").innerHTML = watchlistItems.length ? watchlistItems.slice(0, 3).map((item) => watchlistRow(item, wall?.last_sync_epoch)).join("") : empty("No watchlist items yet.");
  }

  async function loadOverview() {
    const cacheKey = profileCacheKey("overview");
    const cached = readCache(cacheKey);
    if (cached) renderOverview(cached);
    else paintOverviewSkeletons();

    const [widgetsRes, wallRes, insightsRes, statusRes, collectionRes] = await Promise.allSettled([
      api("/api/dashboard/widgets?include=history,ratings,scrobble,progress&history_limit=8&ratings_limit=9&scrobble_limit=8&progress_limit=8"),
      api("/api/state/wall?limit=8"),
      api("/api/insights?limit_samples=0&history=0&runtime=0&include_events=0"),
      api("/api/status"),
      api("/api/profile/collection?page=1&page_size=1"),
    ]);
    const next = {
      widgets: widgetsRes.status === "fulfilled" ? widgetsRes.value : (cached?.widgets || {}),
      wall: wallRes.status === "fulfilled" ? wallRes.value : (cached?.wall || { items: [] }),
      progress: cached?.progress || { items: [] },
      insights: insightsRes.status === "fulfilled" ? insightsRes.value : (cached?.insights || null),
      status: statusRes.status === "fulfilled" ? statusRes.value : (cached?.status || null),
      collection: collectionRes.status === "fulfilled"
        ? { counts: collectionRes.value?.counts || {}, total: Number(collectionRes.value?.total) || 0 }
        : (cached?.collection || null),
    };
    if (!next.progress?.items?.length && next.widgets?.recent_progress?.items?.length) {
      next.progress = { items: next.widgets.recent_progress.items };
    }
    renderOverview(next);
    writeCache(cacheKey, next);

    api("/api/playback_progress/items?page=1&page_size=8").then((progress) => {
      const fresh = { ...next, progress: progress || { items: [] } };
      const progressItems = fresh.progress?.items?.length ? fresh.progress.items : (fresh.widgets?.recent_progress?.items || []);
      renderProgressItems(progressItems);
      renderQuickStats({ wall: fresh.wall, widgets: fresh.widgets, progressItems, insights: fresh.insights, collection: fresh.collection });
      writeCache(cacheKey, fresh);
    }).catch(() => {});
    refreshNowPlaying();
  }

  const COLLECTION_PREFS_KEY = "cw.profile.collection";
  const COLLECTION_PAGE_SIZES = [24, 48, 72, 96];

  function readCollectionPrefs() {
    try {
      const raw = JSON.parse(window.localStorage?.getItem(COLLECTION_PREFS_KEY) || "{}");
      return raw && typeof raw === "object" ? raw : {};
    } catch {
      return {};
    }
  }

  const collectionPrefs = readCollectionPrefs();

  const collectionState = {
    loaded: false,
    loading: false,
    page: 1,
    pageSize: COLLECTION_PAGE_SIZES.includes(Number(collectionPrefs.pageSize)) ? Number(collectionPrefs.pageSize) : 24,
    view: collectionPrefs.view === "list" ? "list" : "grid",
    type: "all",
    provider: "",
    search: "",
    sort: "collected_at",
    items: [],
    total: 0,
    pageCount: 1,
    cols: (collectionPrefs.cols && typeof collectionPrefs.cols === "object") ? { ...collectionPrefs.cols } : {},
  };

  function saveCollectionPrefs() {
    try {
      window.localStorage?.setItem(COLLECTION_PREFS_KEY, JSON.stringify({ view: collectionState.view, pageSize: collectionState.pageSize, cols: collectionState.cols }));
    } catch {}
  }

  function lockCollectionPager() {
    $("#profile-collection-pages")?.querySelectorAll("button").forEach((btn) => { btn.disabled = true; });
  }

  function epochOf(value) {
    const numeric = Number(value || 0);
    if (Number.isFinite(numeric) && numeric > 0) return numeric > 1e12 ? Math.floor(numeric / 1000) : numeric;
    const parsed = Date.parse(String(value || "").trim());
    return Number.isFinite(parsed) ? Math.floor(parsed / 1000) : 0;
  }

  function collectionKind(item) {
    const raw = String(item?.type || item?.media_type || "").toLowerCase();
    if (raw === "season") return ["stacks", "Season"];
    if (/episode/.test(raw) || item?.episode || item?.episode_number) return ["play_circle", "Episode"];
    if (/show|tv|series|anime/.test(raw)) return ["tv", "Show"];
    return ["movie", "Movie"];
  }

  function collectionSourceProviders(item) {
    const out = [];
    const push = (value) => {
      const name = providerName(value);
      if (name && !out.some((existing) => String(existing).toLowerCase() === name.toLowerCase())) out.push(name);
    };
    for (const row of Array.isArray(item?.providers) ? item.providers : []) push(row);
    for (const row of Array.isArray(item?.sources) ? item.sources : []) push(row);
    const byProvider = item?.sources_by_provider || item?.sourcesByProvider;
    if (byProvider && typeof byProvider === "object") Object.keys(byProvider).forEach(push);
    return out;
  }

  function collectionLibrarySummary(item) {
    const libraries = Array.isArray(item?.libraries) ? item.libraries.filter(Boolean) : [];
    if (!libraries.length) return "";
    if (libraries.length === 1) return libraries[0];
    return `${libraries[0]} +${libraries.length - 1}`;
  }

  function collectionCard(item) {
    const key = storePosterItem(item);
    const [icon, kind] = collectionKind(item);
    const title = titleOf(item);
    const year = yearOf(item);
    const episode = episodeOf(item);
    const added = epochOf(item?.last_collected_at || item?.collected_at || item?.first_collected_at);
    const when = added ? relTime(added) : "";
    const art = poster(item, "w342");
    const providerStrip = collectionSourceProviders(item).map(providerIconHtml).filter(Boolean).join("");
    const meta = [kind, episode || year, when, collectionLibrarySummary(item)].filter(Boolean).join(" · ");
    return `<button class="cw-collection-card" type="button" data-profile-poster-key="${esc(key)}" aria-label="Show details for ${esc(title)}">
      <span class="cw-collection-poster">
        <img src="${esc(art)}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'">
        <span class="cw-collection-badge"><span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>${esc(kind)}</span>
      </span>
      <span class="cw-collection-info">
        <strong class="cw-collection-name">${esc(title)}</strong>
        <span class="cw-collection-meta">${esc(meta)}</span>
        ${providerStrip ? `<span class="cw-collection-sources">${providerStrip}</span>` : ""}
      </span>
    </button>`;
  }

  const collectionDateFmt = (epoch) => {
    if (!epoch) return "";
    try {
      return new Intl.DateTimeFormat(window.__CW_LOCALE || navigator.language || undefined, { day: "2-digit", month: "2-digit", year: "numeric" }).format(new Date(epoch * 1000));
    } catch {
      return "";
    }
  };

  const collectionIsoFmt = (iso) => {
    const raw = String(iso || "").trim();
    if (!raw) return "";
    const parsed = Date.parse(raw.length <= 10 ? raw + "T00:00:00Z" : raw);
    if (!Number.isFinite(parsed)) return "";
    try {
      return new Intl.DateTimeFormat(window.__CW_LOCALE || navigator.language || undefined, { day: "2-digit", month: "2-digit", year: "numeric", timeZone: "UTC" }).format(new Date(parsed));
    } catch {
      return "";
    }
  };

  const COLLECTION_COLUMNS = [
    { key: "poster", label: "Poster", width: 78, min: 62, max: 130 },
    { key: "title", label: "Title", width: 300, min: 170, max: 900, flex: 1.7 },
    { key: "rel", label: "Release", width: 130, min: 96, max: 240 },
    { key: "genre", label: "Genre", width: 180, min: 110, max: 420, flex: 1 },
    { key: "added", label: "Added", width: 140, min: 108, max: 260 },
    { key: "type", label: "Type", width: 112, min: 84, max: 200 },
    { key: "providers", label: "Providers", width: 130, min: 84, max: 280 },
  ];

  const collectionColumnWidth = (col) => {
    const stored = Number(collectionState.cols?.[col.key]);
    const value = Number.isFinite(stored) && stored > 0 ? stored : col.width;
    return Math.max(col.min, Math.min(col.max, Math.round(value)));
  };

  function applyCollectionColumns() {
    const grid = $("#profile-collection-grid");
    if (!grid) return;
    grid.style.setProperty("--cch-cols", COLLECTION_COLUMNS.map((col) => {
      const width = collectionColumnWidth(col);
      return col.flex ? `minmax(${width}px,${col.flex}fr)` : `${width}px`;
    }).join(" "));
  }

  function collectionListHead() {
    const cells = COLLECTION_COLUMNS.map((col, index) => {
      const handle = index < COLLECTION_COLUMNS.length - 1
        ? `<span class="cw-collection-resize" role="separator" aria-orientation="vertical" title="Drag to resize, double-click to reset" data-collection-resize="${esc(col.key)}"></span>`
        : "";
      return `<span class="cw-collection-cell cw-collection-cell--${esc(col.key)}">${esc(col.label)}${handle}</span>`;
    }).join("");
    return `<div class="cw-collection-row cw-collection-row--head" role="presentation">${cells}</div>`;
  }

  const collectionReleaseIso = (item, meta) => {
    const movie = /^movie$/i.test(String(item?.type || item?.media_type || ""));
    const fromMeta = meta ? (movie ? (meta.detail?.release_date || meta.release?.date || "") : (meta.detail?.first_air_date || meta.release?.date || "")) : "";
    return String(fromMeta || item?.release_date || item?.first_air_date || item?.released || item?.premiered || "").trim();
  };

  const collectionGenreText = (item, meta) => {
    const raw = (meta && (meta.genres || meta.detail?.genres)) || item?.genres || item?.genre || [];
    const list = Array.isArray(raw) ? raw : String(raw || "").split(",");
    return list.map((g) => (typeof g === "string" ? g : (g?.name || g?.title || ""))).map((g) => String(g).trim()).filter(Boolean).slice(0, 3).join(", ");
  };

  function collectionListRow(item, index) {
    const key = storePosterItem(item);
    const [icon, kind] = collectionKind(item);
    const title = titleOf(item);
    const year = yearOf(item);
    const episode = episodeOf(item);
    const added = epochOf(item?.last_collected_at || item?.collected_at || item?.first_collected_at);
    const art = poster(item, "w342");
    const providerStrip = collectionSourceProviders(item).map(providerIconHtml).filter(Boolean).join("");
    const stamp = episode || year;
    const stored = recallCollectionMeta(item);
    const release = collectionIsoFmt(collectionReleaseIso(item, null)) || stored?.r || "";
    const genres = collectionGenreText(item, null) || stored?.g || "";
    return `<button class="cw-collection-row" type="button" data-collection-index="${index}" data-profile-poster-key="${esc(key)}" aria-label="Show details for ${esc(title)}">
      <span class="cw-collection-cell cw-collection-cell--poster"><img src="${esc(art)}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'"></span>
      <span class="cw-collection-cell cw-collection-cell--title">
        <strong>${esc(title)}</strong>
        ${stamp ? `<span class="cw-collection-pill">${esc(stamp)}</span>` : ""}
      </span>
      <span class="cw-collection-cell cw-collection-cell--rel">${esc(release || "—")}</span>
      <span class="cw-collection-cell cw-collection-cell--genre">${esc(genres || "—")}</span>
      <span class="cw-collection-cell cw-collection-cell--added">
        <strong>${esc(collectionDateFmt(added) || "—")}</strong>
        ${added ? `<small>${esc(relTime(added))}</small>` : ""}
      </span>
      <span class="cw-collection-cell cw-collection-cell--type"><span class="cw-collection-pill cw-collection-pill--type"><span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>${esc(kind)}</span></span>
      <span class="cw-collection-cell cw-collection-cell--providers">${providerStrip}</span>
    </button>`;
  }

  function applyCollectionMeta() {
    const grid = $("#profile-collection-grid");
    if (!grid || collectionState.view !== "list") return;
    grid.querySelectorAll("[data-collection-index]").forEach((row) => {
      const item = collectionState.items[Number(row.dataset.collectionIndex)];
      if (!item) return;
      const meta = window.CW?.Meta?.peek?.(item) || null;
      const relCell = row.querySelector(".cw-collection-cell--rel");
      const genreCell = row.querySelector(".cw-collection-cell--genre");
      const stored = meta ? null : recallCollectionMeta(item);
      const release = meta ? collectionIsoFmt(collectionReleaseIso(item, meta)) : (stored?.r || "");
      const genres = meta ? collectionGenreText(item, meta) : (stored?.g || "");
      if (meta) rememberCollectionMeta(item, release, genres);
      if (relCell) relCell.textContent = release || "—";
      if (genreCell) {
        genreCell.textContent = genres || "—";
        genreCell.title = genres;
      }
    });
    saveCollectionMetaCache();
  }

  let collectionMetaSeq = 0;
  const COLLECTION_META_TTL_MS = 7 * 24 * 60 * 60 * 1000;
  const COLLECTION_META_MAX = 4000;
  let collectionMetaStore = null;
  let collectionMetaDirty = false;

  const collectionMetaKey = (item) => {
    try {
      return String(window.CW?.Meta?.key?.(item) || "");
    } catch {
      return "";
    }
  };

  function collectionMetaCache() {
    if (collectionMetaStore) return collectionMetaStore;
    const stored = readCache(profileCacheKey("collection_meta"), COLLECTION_META_TTL_MS);
    collectionMetaStore = new Map(Object.entries(stored && typeof stored === "object" ? stored : {}));
    return collectionMetaStore;
  }

  function saveCollectionMetaCache() {
    if (!collectionMetaDirty || !collectionMetaStore) return;
    collectionMetaDirty = false;
    let entries = [...collectionMetaStore.entries()];
    if (entries.length > COLLECTION_META_MAX) entries = entries.slice(-COLLECTION_META_MAX);
    writeCache(profileCacheKey("collection_meta"), Object.fromEntries(entries));
  }

  function rememberCollectionMeta(item, release, genres) {
    const key = collectionMetaKey(item);
    if (!key || (!release && !genres)) return;
    const cache = collectionMetaCache();
    const prev = cache.get(key);
    if (prev && prev.r === release && prev.g === genres) return;
    cache.delete(key);
    cache.set(key, { r: release, g: genres });
    collectionMetaDirty = true;
  }

  function recallCollectionMeta(item) {
    const key = collectionMetaKey(item);
    return key ? collectionMetaCache().get(key) || null : null;
  }

  async function hydrateCollectionMeta() {
    if (collectionState.view !== "list" || !collectionState.items.length) return;
    const meta = window.CW?.Meta;
    if (typeof meta?.batch !== "function") return;
    const token = ++collectionMetaSeq;
    try {
      await meta.batch(collectionState.items, "row");
    } catch {
      return;
    }
    if (token === collectionMetaSeq) applyCollectionMeta();
  }

  function startCollectionResize(event, key) {
    const col = COLLECTION_COLUMNS.find((entry) => entry.key === key);
    if (!col || (event.button != null && event.button !== 0)) return;
    event.preventDefault();
    event.stopPropagation();
    const startX = event.clientX;
    const startWidth = collectionColumnWidth(col);
    document.body.classList.add("cw-column-resizing");
    const onMove = (moveEvent) => {
      collectionState.cols[col.key] = Math.max(col.min, Math.min(col.max, startWidth + moveEvent.clientX - startX));
      applyCollectionColumns();
    };
    const finish = () => {
      document.removeEventListener("pointermove", onMove);
      document.removeEventListener("pointerup", finish);
      document.removeEventListener("pointercancel", finish);
      document.body.classList.remove("cw-column-resizing");
      saveCollectionPrefs();
    };
    document.addEventListener("pointermove", onMove);
    document.addEventListener("pointerup", finish);
    document.addEventListener("pointercancel", finish);
  }

  function renderCollectionTypeChips(counts = {}) {
    const host = $("#profile-collection-types");
    if (!host) return;
    const rows = [
      ["all", "apps", "All"],
      ["movie", "movie", "Movies"],
      ["show", "tv", "Shows"],
      ["season", "stacks", "Seasons"],
      ["episode", "play_circle", "Episodes"],
    ];
    host.innerHTML = rows.map(([key, icon, label]) => {
      const count = Number(counts[key] || 0);
      return `<button class="${collectionState.type === key ? "active" : ""}" type="button" data-collection-type="${esc(key)}" aria-pressed="${collectionState.type === key}">
        <span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>
        <span>${esc(label)}</span>
        <strong>${esc(numberFmt.format(count))}</strong>
      </button>`;
    }).join("");
  }

  function renderCollectionProviders(providers = []) {
    const select = $("#profile-collection-provider");
    if (!select) return;
    const current = collectionState.provider;
    const options = [`<option value="">All providers</option>`].concat((providers || []).map((row) => {
      const key = String(row?.provider || "").toLowerCase();
      const label = visibleProviderLabel(key) || key.toUpperCase();
      const count = Number(row?.count || 0);
      return `<option value="${esc(key)}" data-provider="${esc(key)}" data-label="${esc(label)}">${esc(label)} (${esc(numberFmt.format(count))})</option>`;
    }));
    select.innerHTML = options.join("");
    select.value = current;
    enhanceCollectionProviderSelect(select);
  }

  function enhanceCollectionProviderSelect(select = $("#profile-collection-provider")) {
    if (!select || typeof window.CW?.IconSelect?.enhance !== "function") return;
    window.CW.IconSelect.enhance(select, {
      className: "cw-profile-collection-select",
      menuClassName: "cw-profile-collection-menu",
      menuMinWidth: 260,
      getOptionData: (value, option) => {
        if (!value) return { label: "All providers", icons: [{ symbol: "hub" }] };
        const provider = option?.dataset?.provider || value;
        const label = option?.dataset?.label || visibleProviderLabel(provider) || String(provider || "").toUpperCase();
        const logo = providerLogLogo(provider);
        return {
          label: option?.textContent?.trim() || label,
          icons: logo ? [{ src: logo, alt: label }] : [{ text: label.slice(0, 2) || "?" }],
        };
      },
    });
  }

  function enhanceCollectionSortSelect(select = $("#profile-collection-sort")) {
    if (!select || typeof window.CW?.IconSelect?.enhance !== "function") return;
    const sortIcons = {
      collected_at: "schedule",
      collected_at_asc: "history",
      title: "sort_by_alpha",
      title_desc: "sort_by_alpha",
      year_desc: "calendar_month",
      year_asc: "calendar_month",
    };
    window.CW.IconSelect.enhance(select, {
      className: "cw-profile-collection-select",
      menuClassName: "cw-profile-collection-menu",
      menuMinWidth: 230,
      getOptionData: (value, option) => ({
        label: option?.textContent?.trim() || "Sort",
        icons: [{ symbol: sortIcons[value] || "sort" }],
      }),
    });
  }

  function renderCollectionMetrics(data = {}) {
    const host = $("#profile-collection-metrics");
    if (!host) return;
    const counts = data.counts || {};
    const providers = Array.isArray(data.providers) ? data.providers.length : 0;
    const values = [
      ["violet", "movie", "Movies", "Owned", counts.movie || 0],
      ["blue", "tv", "Shows", "Owned", counts.show || 0],
      ["green", "hub", "Providers", "Connected", providers],
      ["amber", "inventory_2", "Items", "Total", counts.all ?? data.total ?? 0],
    ];
    host.innerHTML = values.map(([tone, icon, label, note, value]) => `<span class="cw-collection-tile" data-tone="${esc(tone)}">
      <span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>
      <span class="cw-collection-tile-body">
        <strong>${esc(numberFmt.format(Number(value) || 0))}</strong>
        <span>${esc(label)}</span>
        <small>${esc(note)}</small>
      </span>
    </span>`).join("");
  }

  function paintCollectionItems() {
    const grid = $("#profile-collection-grid");
    if (!grid) return;
    const list = collectionState.view === "list";
    grid.dataset.view = list ? "list" : "grid";
    if (!collectionState.items.length) {
      grid.innerHTML = empty("No collection items match this view.");
      return;
    }
    if (list) {
      grid.innerHTML = collectionListHead() + collectionState.items.map(collectionListRow).join("");
      applyCollectionColumns();
      applyCollectionMeta();
      void hydrateCollectionMeta();
      return;
    }
    grid.innerHTML = collectionState.items.map(collectionCard).join("");
  }

  function setCollectionView(view) {
    const next = view === "list" ? "list" : "grid";
    if (next === collectionState.view) return;
    collectionState.view = next;
    saveCollectionPrefs();
    syncCollectionViewButtons();
    paintCollectionItems();
  }

  function syncCollectionViewButtons() {
    document.querySelectorAll("[data-collection-view]").forEach((btn) => {
      const active = btn.dataset.collectionView === collectionState.view;
      btn.classList.toggle("active", active);
      btn.setAttribute("aria-pressed", String(active));
    });
  }

  function renderCollection(data = {}) {
    const grid = $("#profile-collection-grid");
    if (!grid) return;
    collectionState.total = Number(data.total || 0);
    renderCollectionMetrics(data);
    renderCollectionTypeChips(data.counts || {});
    renderCollectionProviders(data.providers || []);
    paintCollectionItems();
    renderCollectionPager(data);
  }

  function collectionPageNumbers(page, pageCount) {
    if (pageCount <= 7) return Array.from({ length: pageCount }, (_, i) => i + 1);
    let from = Math.max(2, page - 1);
    let to = Math.min(pageCount - 1, page + 1);
    if (page <= 3) { from = 2; to = 3; }
    else if (page >= pageCount - 2) { from = pageCount - 2; to = pageCount - 1; }
    const out = [1];
    if (from > 2) out.push("gap");
    for (let i = from; i <= to; i += 1) out.push(i);
    if (to < pageCount - 1) out.push("gap");
    out.push(pageCount);
    return out;
  }

  function renderCollectionPager(data = {}) {
    const footer = $("#profile-collection-footer");
    if (!footer) return;
    const total = Number(data.total || 0);
    const pageSize = Number(data.page_size || collectionState.pageSize) || collectionState.pageSize;
    const pageCount = total ? Math.ceil(total / pageSize) : 1;
    const page = Math.min(Math.max(1, Number(data.page || collectionState.page) || 1), pageCount);
    collectionState.page = page;
    collectionState.pageCount = pageCount;
    footer.hidden = !total;
    const start = total ? (page - 1) * pageSize : 0;
    const end = total ? Math.min(start + pageSize, total) : 0;
    const label = $("#profile-collection-page-label");
    if (label) label.textContent = total ? `Showing ${numberFmt.format(start + 1)}–${numberFmt.format(end)} of ${numberFmt.format(total)}` : "";
    const pages = $("#profile-collection-pages");
    if (pages) {
      const step = (target, icon, title, disabled) => `<button type="button" data-collection-page="${target}" title="${title}" aria-label="${title}"${disabled ? " disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">${icon}</span></button>`;
      const numbers = collectionPageNumbers(page, pageCount).map((entry) => {
        if (entry === "gap") return `<span class="cw-collection-gap" aria-hidden="true">…</span>`;
        const active = entry === page;
        return `<button type="button" class="${active ? "active" : ""}" data-collection-page="${entry}"${active ? ' aria-current="page" disabled' : ""}>${numberFmt.format(entry)}</button>`;
      }).join("");
      pages.innerHTML = step(page - 1, "chevron_left", "Previous page", page <= 1) + numbers + step(page + 1, "chevron_right", "Next page", page >= pageCount);
    }
  }

  function goToCollectionPage(page) {
    const target = Math.max(1, Number(page) || 1);
    if (collectionState.loading || target === collectionState.page) return;
    if (collectionState.pageCount && target > collectionState.pageCount) return;
    collectionState.page = target;
    void loadCollection();
    $("#profile-panel-collection")?.scrollIntoView({ block: "start", behavior: "smooth" });
  }

  async function loadCollection({ reset = false } = {}) {
    if (collectionState.loading) return;
    if (reset) collectionState.page = 1;
    collectionState.items = [];
    const skeletonGrid = $("#profile-collection-grid");
    if (skeletonGrid) {
      skeletonGrid.dataset.view = collectionState.view === "list" ? "list" : "grid";
      skeletonGrid.innerHTML = collectionState.view === "list" ? skeleton("collectionRow", 6) : skeleton("collection", 7);
    }
    collectionState.loading = true;
    lockCollectionPager();
    const params = new URLSearchParams({
      type: collectionState.type,
      provider: collectionState.provider,
      search: collectionState.search,
      sort: collectionState.sort,
      page: String(collectionState.page),
      page_size: String(collectionState.pageSize),
    });
    try {
      const data = await api(`/api/profile/collection?${params.toString()}`);
      collectionState.items = Array.isArray(data.items) ? data.items : [];
      collectionState.loaded = true;
      renderCollection(data);
    } catch (e) {
      const grid = $("#profile-collection-grid");
      if (grid) grid.innerHTML = empty("Collections could not be loaded.");
      renderCollectionPager({ total: collectionState.total, page: collectionState.page, page_size: collectionState.pageSize });
      toast(e.message || "Collections could not be loaded", true);
    } finally {
      collectionState.loading = false;
    }
  }

  function wireCollection() {
    let searchTimer = null;
    document.addEventListener("click", (event) => {
      const btn = event.target?.closest?.("[data-collection-view]");
      if (!btn) return;
      setCollectionView(btn.dataset.collectionView);
    });
    syncCollectionViewButtons();
    document.addEventListener("pointerdown", (event) => {
      const handle = event.target?.closest?.("[data-collection-resize]");
      if (!handle) return;
      startCollectionResize(event, handle.dataset.collectionResize);
    });
    document.addEventListener("dblclick", (event) => {
      const handle = event.target?.closest?.("[data-collection-resize]");
      if (!handle) return;
      event.preventDefault();
      delete collectionState.cols[handle.dataset.collectionResize];
      applyCollectionColumns();
      saveCollectionPrefs();
    });
    enhanceCollectionProviderSelect();
    enhanceCollectionSortSelect();
    $("#profile-collection-types")?.addEventListener("click", (event) => {
      const btn = event.target?.closest?.("[data-collection-type]");
      if (!btn) return;
      collectionState.type = btn.dataset.collectionType || "all";
      void loadCollection({ reset: true });
    });
    $("#profile-collection-provider")?.addEventListener("change", (event) => {
      collectionState.provider = event.target?.value || "";
      void loadCollection({ reset: true });
    });
    $("#profile-collection-sort")?.addEventListener("change", (event) => {
      collectionState.sort = event.target?.value || "collected_at";
      void loadCollection({ reset: true });
    });
    $("#profile-collection-search")?.addEventListener("input", (event) => {
      collectionState.search = event.target?.value || "";
      clearTimeout(searchTimer);
      searchTimer = setTimeout(() => loadCollection({ reset: true }), 220);
    });
    document.addEventListener("keydown", (event) => {
      if (!(event.ctrlKey || event.metaKey) || String(event.key || "").toLowerCase() !== "k") return;
      if (!$("#profile-panel-collection")?.classList?.contains("active")) return;
      const input = $("#profile-collection-search");
      if (!input) return;
      event.preventDefault();
      input.focus();
      input.select?.();
    });
    $("#profile-collection-pages")?.addEventListener("click", (event) => {
      const btn = event.target?.closest?.("[data-collection-page]");
      if (!btn || btn.disabled) return;
      goToCollectionPage(Number(btn.dataset.collectionPage));
    });
    const pageSizeSelect = $("#profile-collection-page-size");
    if (pageSizeSelect) {
      pageSizeSelect.value = String(collectionState.pageSize);
      pageSizeSelect.addEventListener("change", (event) => {
        const next = Number(event.target?.value) || 24;
        if (next === collectionState.pageSize) return;
        collectionState.pageSize = next;
        saveCollectionPrefs();
        void loadCollection({ reset: true });
      });
    }
    syncCollectionViewButtons();
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
        if (key === "collection" && !collectionState.loaded) void loadCollection({ reset: true });
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
      setAvatarUploadState({ visible: true, label: "Reading picture", percent: 3 });
      let previewUrl = "";
      (async () => {
        try {
          const prepared = await prepareAvatarUpload(file);
          previewUrl = prepared.previewUrl;
          setAvatar(previewUrl);
          setAvatarUploadState({ visible: true, label: "Uploading picture", percent: 50 });
          const data = await uploadAvatar(prepared.dataUrl, prepared.contentType, (percent) => {
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
          if (previewUrl) setTimeout(() => URL.revokeObjectURL(previewUrl), 1000);
          setTimeout(() => setAvatarUploadState({ visible: false }), 500);
          input.value = "";
        }
      })();
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
      if (target?.closest?.("[data-collection-resize]")) return false;
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
    wireTabs();
    wireCollection();
    wirePreferences();
    wireAvatar();
    wireForms();
    wireSecurity();
    wirePlexSso();
    wireOidcSso();
    wireLogout();
    wirePosterOverlay();
    const [profileResult, overviewResult] = await Promise.allSettled([
      refreshProfile(),
      loadOverview(),
    ]);
    void refreshPlexStatus();
    void refreshOidcStatus();
    if (profileResult.status === "rejected") toast(profileResult.reason?.message || "Profile could not be loaded", true);
    if (overviewResult.status === "rejected") toast(overviewResult.reason?.message || "Profile overview could not be loaded", true);
    if (nowTimer) clearInterval(nowTimer);
    nowTimer = setInterval(() => {
      if (document.visibilityState === "visible") refreshNowPlaying();
    }, 15000);
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init, { once: true });
  else init();
})();
