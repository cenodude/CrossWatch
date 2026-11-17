/* assets/helpers/media-meta.js */
/* Shared TMDB metadata fetcher and cache for the playing card and its watchlist variants */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const authSetupPending = () => window.cwIsAuthSetupPending?.() === true;

  const NEED = Object.freeze({
    row: Object.freeze({ genres: 1, release: 1 }),
    detail: Object.freeze({
      overview: 1, runtime_minutes: 1, ids: 1, videos: 1, genres: 1,
      certification: 1, score: 1, vote_count: 1, release: 1, backdrop: 1,
    }),
    media: Object.freeze({
      overview: 1, tagline: 1, runtime_minutes: 1, ids: 1, videos: 1, genres: 1,
      certification: 1, score: 1, vote_count: 1, release: 1, backdrop: 1,
      credits: 1, recommendations: 1, collection: 1,
    }),
  });
  const profileOf = (profile) => (profile === "detail" || profile === "media" ? profile : "row");

  const cache = new Map();
  const inflight = new Map();

  const isEpisode = (item) => {
    const raw = String(item?.media_type || item?.type || item?.entity || "").toLowerCase();
    return raw === "episode" || item?.season != null || item?.episode != null;
  };

  const kindOf = (item) => {
    const raw = String(item?.media_type || item?.type || item?.entity || "").toLowerCase();
    return raw === "movie" ? "movie" : "show";
  };

  const tmdbFromCanonical = (item) => {
    const match = String(item?.canonical_key || item?.key || "").trim().match(/^tmdb:(\d+)(?:#|$)/i);
    return match ? match[1] : "";
  };

  const tmdbOf = (item) => {
    const ids = item?.ids || {};
    const id = isEpisode(item)
      ? (ids.tmdb_show || item?.tmdb_show || tmdbFromCanonical(item) || item?.tmdb || item?.tmdb_id || ids.tmdb || ids.id)
      : (item?.tmdb || item?.tmdb_id || ids.tmdb || ids.tmdb_show || ids.id);
    return String(id || "").trim();
  };

  const keyOf = (item) => {
    const tmdb = tmdbOf(item);
    return tmdb ? `${kindOf(item)}:${tmdb}` : "";
  };

  const merge = (base, extra, profile, kind, wantsSeries) => {
    const prev = base && typeof base === "object" ? base : {};
    const next = extra && typeof extra === "object" ? extra : {};
    const merged = { ...prev, ...next };
    merged.ids = { ...(prev.ids || {}), ...(next.ids || {}) };
    merged.detail = { ...(prev.detail || {}), ...(next.detail || {}) };
    merged.images = { ...(prev.images || {}), ...(next.images || {}) };
    merged.__kind = kind || prev.__kind || "";
    merged.__rowLoaded = true;
    merged.__detailLoaded = !!(prev.__detailLoaded || profile === "detail" || profile === "media");
    merged.__mediaLoaded = !!(prev.__mediaLoaded || profile === "media");
    merged.__videosLoaded = !!(
      prev.__videosLoaded
      || (profile !== "row" && Object.prototype.hasOwnProperty.call(next, "videos"))
    );
    merged.__seriesLoaded = !!(prev.__seriesLoaded || (wantsSeries && profile !== "row"));
    return merged;
  };

  const satisfies = (meta, profile, wantsSeries) => {
    if (!meta || typeof meta !== "object") return false;
    if (profile === "row") return !!meta.__rowLoaded;
    if (wantsSeries && meta.__kind === "show" && !meta.__seriesLoaded) return false;
    if (profile === "media") return !!meta.__mediaLoaded && !!meta.__videosLoaded;
    return !!meta.__detailLoaded
      && !!meta.__videosLoaded
      && !!(meta.overview || meta.detail?.overview || meta.detail?.tagline);
  };

  const descriptorFor = (item) => {
    const episode = isEpisode(item);
    const ids = item?.ids || {};
    return {
      tmdb: tmdbOf(item),
      type: kindOf(item),
      title: episode ? "" : String(item?.title || ""),
      year: episode ? "" : String(item?.year || ""),
      ids: { imdb: String(ids.imdb || ids.imdb_id || ids.imdb_show || ""), tvdb: String(ids.tvdb || "") },
    };
  };

  async function fetchGroup(descriptors, profile, wantsSeries) {
    const kind = descriptors[0].type;
    const need = { ...NEED[profileOf(profile)] };
    if (wantsSeries && kind === "show" && profile !== "row") need.series_info = 1;
    if (kind === "show" && profile === "media") need.seasons = 1;
    const overview = profile === "row" ? "none" : "full";

    try {
      const res = await fetch(`/api/metadata/bulk?overview=${encodeURIComponent(overview)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          items: descriptors.map((d) => ({ type: d.type, tmdb: d.tmdb, title: d.title, year: d.year, ids: d.ids })),
          need,
          concurrency: Math.min(Math.max(descriptors.length, 1), 6),
        }),
      });
      if (!res.ok) return false;
      const data = await res.json();
      const results = data?.results || {};
      let hit = false;
      for (const d of descriptors) {
        const entry = results[`${d.type}:${d.tmdb}`];
        const meta = entry?.ok ? (entry.meta || null) : null;
        if (!meta) continue;
        const cacheKey = `${d.type}:${d.tmdb}`;
        cache.set(cacheKey, merge(cache.get(cacheKey), meta, profile, d.type, wantsSeries));
        hit = true;
      }
      return hit;
    } catch {
      return false;
    }
  }

  async function batch(items, profile = "row", options = {}) {
    if (authSetupPending()) return false;
    const wantsSeries = !!options.seriesInfo;
    const profileKey = profileOf(profile);
    const groups = new Map();
    const pending = [];
    const seen = new Set();

    for (const item of items || []) {
      const cacheKey = keyOf(item);
      if (!cacheKey || seen.has(cacheKey)) continue;
      seen.add(cacheKey);
      if (satisfies(cache.get(cacheKey), profileKey, wantsSeries)) continue;
      const inflightKey = `${profileKey}:${wantsSeries ? "s" : "-"}:${cacheKey}`;
      const running = inflight.get(inflightKey);
      if (running) {
        pending.push(running);
        continue;
      }
      const descriptor = descriptorFor(item);
      if (!descriptor.tmdb) continue;
      const bucket = groups.get(descriptor.type) || [];
      bucket.push({ descriptor, inflightKey });
      groups.set(descriptor.type, bucket);
    }

    for (const bucket of groups.values()) {
      const descriptors = bucket.map((b) => b.descriptor);
      const request = fetchGroup(descriptors, profileKey, wantsSeries)
        .finally(() => bucket.forEach((b) => inflight.delete(b.inflightKey)));
      bucket.forEach((b) => inflight.set(b.inflightKey, request));
      pending.push(request);
    }

    if (!pending.length) return false;
    return (await Promise.all(pending)).some(Boolean);
  }

  async function get(item, profile = "detail", options = {}) {
    const cacheKey = keyOf(item);
    if (!cacheKey) return null;
    const cached = cache.get(cacheKey);
    if (satisfies(cached, profileOf(profile), !!options.seriesInfo)) return cached;
    await batch([item], profile, options);
    return cache.get(cacheKey) || null;
  }

  function peek(item) {
    const cacheKey = keyOf(item);
    return cacheKey ? cache.get(cacheKey) || null : null;
  }

  function has(item, profile = "row", options = {}) {
    return satisfies(peek(item), profileOf(profile), !!options.seriesInfo);
  }

  function invalidate(item) {
    const cacheKey = keyOf(item);
    if (!cacheKey) return;
    const cached = cache.get(cacheKey);
    if (!cached || typeof cached !== "object") return;
    cache.set(cacheKey, { ...cached, __detailLoaded: false, __videosLoaded: false, __seriesLoaded: false });
  }

  (window.CW ||= {}).Meta = { key: keyOf, kind: kindOf, tmdbId: tmdbOf, isEpisode, get, peek, has, batch, invalidate };

  const chosenArt = new Map(String(document.documentElement.dataset.cwArtOv || "").split(" ").filter(Boolean).map((part) => part.split("=")).filter((part) => part.length === 2 && part[1]));
  const artKey = (type, id) => `${String(type || "").toLowerCase() === "movie" ? "movie" : "tv"}:${String(id || "").trim()}`;
  const ART_URL_RE = /\/art\/(?:tmdb|fanart)\/(movie|tv|show)\/(\d+)\?[^"')\s]*/g;

  function artParam(type, id) {
    const rev = chosenArt.get(artKey(type, id));
    return rev ? `&ov=${encodeURIComponent(rev)}` : "";
  }

  function rewriteArt(text, key, rev) {
    return String(text || "").replace(ART_URL_RE, (url, type, id) => {
      if (artKey(type, id) !== key || /[?&]kind=(still|episode_still)\b/.test(url)) return url;
      const clean = url.replace(/&ov=[^&]*/g, "");
      return rev ? `${clean}&ov=${encodeURIComponent(rev)}` : clean;
    });
  }

  function setArtChoice(type, id, rev) {
    const key = artKey(type, id);
    if (rev) chosenArt.set(key, String(rev));
    else chosenArt.delete(key);
    document.querySelectorAll('img[src*="/art/tmdb/"], img[data-src*="/art/tmdb/"], [style*="/art/tmdb/"], img[src*="/art/fanart/"]').forEach((node) => {
      ["src", "data-src", "style"].forEach((attr) => {
        const value = node.getAttribute(attr);
        if (!value || !(value.includes("/art/tmdb/") || value.includes("/art/fanart/"))) return;
        const next = rewriteArt(value, key, rev);
        if (next !== value) node.setAttribute(attr, next);
      });
    });
    window.dispatchEvent(new CustomEvent("cw:art-choice", { detail: { type, id: String(id), rev: rev || "" } }));
  }

  window.CW.ArtChoice = { param: artParam, set: setArtChoice };

  const SHELF_FORMATS = { "4k_bluray": "4K UHD", bluray: "Blu-ray", hd_dvd: "HD DVD", dvd: "DVD", digital: "Digital", vcd: "VCD", vhs: "VHS", betamax: "Betamax", laserdisc: "LaserDisc" };
  const SHELF_STATUSES = { owned: "Owned", ordered: "Ordered", wishlist: "Wishlist" };
  const SHELF_DISCS = { "4k_bluray": "bluray", bluray: "bluray", dvd: "dvd" };
  const shelfIndex = { data: null, promise: null, currency: "" };
  const shelfEsc = (value) => String(value ?? "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));

  function shelfLoad(force = false) {
    if (shelfIndex.promise && !force) return shelfIndex.promise;
    shelfIndex.promise = fetch("/api/profile/shelf", { cache: "no-store", credentials: "same-origin" })
      .then((response) => (response.ok ? response.json() : null))
      .then((data) => {
        shelfIndex.data = data?.items && typeof data.items === "object" ? data.items : {};
        shelfIndex.currency = typeof data?.currency === "string" ? data.currency : "";
        window.dispatchEvent(new CustomEvent("cw:shelf-changed", { detail: { loaded: true } }));
        return shelfIndex.data;
      })
      .catch(() => {
        shelfIndex.data ||= {};
        return shelfIndex.data;
      });
    return shelfIndex.promise;
  }

  function shelfPeek(type, id) {
    return shelfIndex.data?.[artKey(type, id)] || null;
  }

  function shelfSet(type, id, info) {
    shelfIndex.data ||= {};
    const key = artKey(type, id);
    if (info) shelfIndex.data[key] = info;
    else delete shelfIndex.data[key];
    window.dispatchEvent(new CustomEvent("cw:shelf-changed", { detail: { type, id: String(id), info: info || null } }));
  }

  function shelfMoney(value, currency = shelfIndex.currency) {
    const amount = Number(value);
    if (!Number.isFinite(amount)) return "";
    try {
      return currency
        ? new Intl.NumberFormat(window.__CW_LOCALE || undefined, { style: "currency", currency }).format(amount)
        : new Intl.NumberFormat(window.__CW_LOCALE || undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(amount);
    } catch {
      return amount.toFixed(2);
    }
  }

  function shelfLabel(info) {
    if (!info) return "";
    const formats = Array.isArray(info.formats) ? info.formats : [];
    const first = SHELF_FORMATS[formats[0]] || "";
    if (first) return formats.length > 1 ? `${first} +${formats.length - 1}` : first;
    return SHELF_STATUSES[info.status] || "On shelf";
  }

  function shelfDiscUrl(type, id, format) {
    const disc = SHELF_DISCS[format];
    if (!disc || type !== "movie" || !/^\d+$/.test(String(id || "")) || !String(document.documentElement.dataset.cwArt || "").startsWith("f")) return "";
    return `/art/fanart/movie/${encodeURIComponent(String(id))}?kind=disc&disc=${disc}${artParam("movie", id)}`;
  }

  function shelfLogoUrl(path, size = "w92") {
    return /^\/[A-Za-z0-9_-]{4,64}\.(jpg|jpeg|png)$/i.test(String(path || "")) ? `/art/tmdb/image?path=${encodeURIComponent(path)}&size=${size}` : "";
  }

  const PILL_FORMATS = { "4k_bluray": "uhd-bluray.svg", bluray: "bluray.svg", dvd: "dvd.svg" };
  const PILL_HDR = { dolby_vision: ["dolby-vision.svg", "Dolby Vision"], hdr10_plus: ["hdr10-plus.svg", "HDR10+"], hdr10: ["hdr10.svg", "HDR10"] };
  const PILL_ATMOS = new Set(["dolby_atmos", "dolby_digital_plus_atmos"]);

  function shelfPill({ format = "", media = {}, copies = 0, service = null } = {}) {
    const info = media && typeof media === "object" ? media : {};
    const logo = (file, alt) => `<img class="cw-media-pill-logo is-${file.replace(".svg", "")}" src="/assets/img/quality/${file}" alt="${shelfEsc(alt)}" loading="lazy" decoding="async">`;
    const parts = [];
    if (PILL_FORMATS[format]) parts.push(logo(PILL_FORMATS[format], SHELF_FORMATS[format]));
    else if (format === "digital" && service?.name) {
      const src = shelfLogoUrl(service.logo, "w45");
      parts.push(`${src ? `<img class="cw-media-pill-service" src="${shelfEsc(src)}" alt="" loading="lazy" decoding="async">` : ""}<span class="cw-media-pill-text">${shelfEsc(service.name)}</span>`);
    } else if (format) parts.push(`<span class="cw-media-pill-text">${shelfEsc(SHELF_FORMATS[format] || format)}</span>`);
    else if (info.resolution === "uhd_4k") parts.push(logo("4k-uhd.svg", "4K"));
    const hdr = PILL_HDR[info.hdr];
    if (hdr) parts.push(logo(hdr[0], hdr[1]));
    if (PILL_ATMOS.has(info.audio)) parts.push(logo("dolby-atmos.svg", "Dolby Atmos"));
    if (Number(copies) > 1) parts.push(`<span class="cw-media-pill-text">×${Number(copies)}</span>`);
    return parts.length ? `<span class="cw-media-pill">${parts.join("")}</span>` : "";
  }

  const CASE_FORMATS = { "4k_bluray": "uhd", bluray: "bluray", dvd: "dvd", hd_dvd: "dvd", vcd: "dvd", vhs: "dvd", betamax: "dvd", laserdisc: "dvd" };
  const CASE_LOGOS = { bluray: "bluray.svg", uhd: "uhd-bluray.svg" };

  const SHELF_CASES_KEY = "cw.shelf.cases";

  function shelfCasesOn() {
    try { return window.localStorage?.getItem(SHELF_CASES_KEY) !== "off"; } catch { return true; }
  }

  function shelfSetCases(on) {
    try { window.localStorage?.setItem(SHELF_CASES_KEY, on ? "on" : "off"); } catch {}
    window.dispatchEvent(new CustomEvent("cw:shelf-cases", { detail: { on: !!on } }));
  }

  function shelfCasesSwitch() {
    return `<button type="button" class="cw-shelfv-cases" role="switch" aria-checked="${shelfCasesOn()}" title="Show disc cases around the posters" data-shelf-cases><span>Cases</span><span class="cw-shelfv-cases-ui" aria-hidden="true"></span></button>`;
  }

  document.addEventListener("click", (event) => {
    const button = event.target?.closest?.("[data-shelf-cases]");
    if (button) shelfSetCases(button.getAttribute("aria-checked") !== "true");
  });

  function shelfCaseKind(format = "", packaging = "") {
    if (!shelfCasesOn()) return "plain";
    if (packaging === "steelbook" && format !== "digital") return "steelbook";
    return CASE_FORMATS[format] || "plain";
  }

  function shelfCase({ format = "", packaging = "", poster = "" } = {}) {
    const kind = shelfCaseKind(format, packaging);
    const art = `<img class="cw-case-art" src="${shelfEsc(poster)}" alt="" loading="lazy" decoding="async" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg?v=2'">`;
    if (kind === "plain") return `<span class="cw-case" data-case="plain">${art}</span>`;
    const logo = CASE_LOGOS[kind];
    return `<span class="cw-case" data-case="${kind}">${art}<img class="cw-case-frame" src="/assets/img/cases/${kind}.svg" alt="" decoding="async">${logo ? `<img class="cw-case-logo" src="/assets/img/quality/${logo}" alt="" decoding="async">` : ""}<span class="cw-case-sheen" aria-hidden="true"></span></span>`;
  }

  function shelfScene({ cover = "", disc = "", reflection = false } = {}) {
    const scene = `<span class="cw-shelf-scene">${disc ? `<span class="cw-shelf-disc"><img src="${shelfEsc(disc)}" alt="" loading="lazy" decoding="async" onerror="this.closest('.cw-shelfv-item,.cw-mm-coll-art')?.classList.remove('has-disc');this.parentElement.remove()"></span>` : ""}${cover}</span>`;
    return `${scene}${reflection ? `<span class="cw-shelf-reflection" aria-hidden="true">${scene}</span>` : ""}`;
  }

  const shelfPopState = { node: null, anchor: null };

  function shelfDate(value) {
    const parts = String(value || "").split("-").map(Number);
    if (parts.length !== 3 || parts.some((n) => !Number.isFinite(n))) return "";
    try {
      return new Date(parts[0], parts[1] - 1, parts[2]).toLocaleDateString(window.__CW_LOCALE || undefined, { day: "numeric", month: "short", year: "numeric" });
    } catch {
      return "";
    }
  }

  function shelfPopHide() {
    shelfPopState.anchor = null;
    shelfPopState.node?.classList.remove("is-open");
  }

  function shelfPopHtml({ title = "", sub = "", info = {}, media = {} } = {}) {
    const perFormat = Object.entries(info.per_format || {}).map(([key, count]) => `${count > 1 ? `${count}× ` : ""}${SHELF_FORMATS[key] || key}`).join(", ");
    const copies = Number(info.copies) || 0;
    const edition = [info.edition, info.packaging === "steelbook" && String(info.edition || "").toLowerCase() !== "steelbook" ? "Steelbook" : ""].filter(Boolean).join(" · ");
    const bought = [shelfDate(info.purchased), Number(info.spent) > 0 ? shelfMoney(info.spent) : ""].filter(Boolean).join(" · ");
    const rows = [
      ["Copies", copies ? `${new Intl.NumberFormat(window.__CW_LOCALE || undefined).format(copies)} · ${perFormat}` : ""],
      ["Edition", edition],
      ["Bought", bought],
      ["Shelf", (info.shelves || []).join(", ")],
      ["Status", info.status && info.status !== "owned" ? SHELF_STATUSES[info.status] || info.status : ""],
      ["Lent out", info.lent ? "Yes" : ""],
    ].filter(([, value]) => value);
    const pill = shelfPill({ format: (info.formats || [])[0] || "", media, service: info.service });
    return `<h4>${shelfEsc(title)}</h4><span class="cw-shelfv-pop-sub">${shelfEsc(sub)}</span>${pill}${rows.length ? `<dl>${rows.map(([label, value]) => `<dt>${shelfEsc(label)}</dt><dd>${shelfEsc(value)}</dd>`).join("")}</dl>` : ""}<span class="cw-shelfv-pop-hint">Click for all details</span>`;
  }

  function shelfPopShow(anchor, html, target = anchor) {
    if (!anchor || !html || shelfPopState.anchor === anchor) return;
    if (!shelfPopState.node) {
      shelfPopState.node = document.createElement("div");
      shelfPopState.node.className = "cw-shelfv-pop";
      shelfPopState.node.setAttribute("aria-hidden", "true");
      document.body.appendChild(shelfPopState.node);
      window.addEventListener("scroll", shelfPopHide, { passive: true, capture: true });
      window.addEventListener("resize", shelfPopHide, { passive: true });
    }
    shelfPopState.anchor = anchor;
    const pop = shelfPopState.node;
    const art = (target || anchor).querySelector?.(".cw-case-art");
    const poster = art?.currentSrc || art?.src || "";
    pop.innerHTML = `${poster ? `<span class="cw-shelfv-pop-bg" aria-hidden="true"><img src="${shelfEsc(poster)}" alt="" decoding="async"></span>` : ""}${html}`;
    const rect = (target || anchor).getBoundingClientRect();
    const width = pop.offsetWidth || 220;
    const height = pop.offsetHeight || 200;
    const base = anchor.querySelector?.(".cw-shelfv-base")?.getBoundingClientRect();
    const left = Math.max(12, Math.min(rect.left + (rect.width - width) / 2, window.innerWidth - width - 12));
    const above = rect.top - height - 12;
    const below = Math.max(rect.bottom, base?.bottom || 0) + 12;
    const top = Math.max(12, Math.min(above >= 12 ? above : below, window.innerHeight - height - 12));
    pop.style.left = `${Math.round(left)}px`;
    pop.style.top = `${Math.round(top)}px`;
    pop.classList.add("is-open");
  }

  window.CW.Shelf = {
    load: shelfLoad, peek: shelfPeek, set: shelfSet, pill: shelfPill, caseHtml: shelfCase, sceneHtml: shelfScene, caseKind: shelfCaseKind, casesOn: shelfCasesOn, setCases: shelfSetCases, casesSwitch: shelfCasesSwitch, date: shelfDate, popHtml: shelfPopHtml, popShow: shelfPopShow, popHide: shelfPopHide, label: shelfLabel, money: shelfMoney, discUrl: shelfDiscUrl, logoUrl: shelfLogoUrl, formats: SHELF_FORMATS, statuses: SHELF_STATUSES,
    currency: () => shelfIndex.currency,
    setCurrency: (value) => { shelfIndex.currency = String(value || ""); window.dispatchEvent(new CustomEvent("cw:shelf-changed", { detail: { currency: shelfIndex.currency } })); },
  };
})();
