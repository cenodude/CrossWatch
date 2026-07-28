/* assets/helpers/trailer.js */
/* Shared trailer modal used by the playing card variants */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const MODAL_ID = "cw-trailer";

  let modal = null;
  let titleEl = null;
  let stageEl = null;
  let closeBtn = null;

  function ensureModal() {
    if (modal?.isConnected) return modal;
    modal = document.createElement("div");
    modal.id = MODAL_ID;
    modal.setAttribute("role", "dialog");
    modal.setAttribute("aria-modal", "true");
    modal.setAttribute("aria-label", "Trailer");
    modal.innerHTML = `
      <div class="cw-trailer-box">
        <div class="cw-trailer-shell">
          <div class="cw-trailer-head">
            <div class="cw-trailer-meta">
              <div class="cw-trailer-kicker">Trailer</div>
              <div class="cw-trailer-title">Loading trailer</div>
            </div>
            <button type="button" class="cw-trailer-close" title="Close trailer" aria-label="Close trailer"><span class="material-symbol">close</span></button>
          </div>
          <div class="cw-trailer-stage"></div>
        </div>
      </div>`;
    document.body.appendChild(modal);

    titleEl = modal.querySelector(".cw-trailer-title");
    stageEl = modal.querySelector(".cw-trailer-stage");
    closeBtn = modal.querySelector(".cw-trailer-close");

    closeBtn.addEventListener("click", (e) => { e.preventDefault(); close(); }, true);
    modal.addEventListener("click", (e) => { if (e.target === modal) close(); }, true);
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && modal?.classList.contains("show")) close();
    }, true);
    return modal;
  }

  function pick(meta) {
    const flat = [meta?.videos, meta?.videos?.results, meta?.detail?.videos, meta?.detail?.videos?.results]
      .flatMap((v) => (Array.isArray(v) ? v : []));
    const scored = flat.map((v) => {
      const site0 = String(v.site || v.host || "").toLowerCase();
      const site = /youtube/.test(site0) ? "youtube" : /vimeo/.test(site0) ? "vimeo" : site0;
      const type = String(v.type || "").toLowerCase();
      const rank = (type.includes("trailer") ? 100 : type.includes("teaser") ? 60 : type.includes("clip") ? 40 : 10)
        + (v.official ? 30 : 0) + (site === "youtube" ? 5 : 0) + (v.published_at || v.created_at ? 1 : 0);
      return { site, key: v.key || v.id || "", name: v.name || "Trailer", rank };
    }).filter((v) => v.site && v.key);
    const best = scored.sort((a, b) => b.rank - a.rank)[0];
    if (!best) return null;
    if (best.site === "youtube") return { url: `https://www.youtube-nocookie.com/embed/${encodeURIComponent(best.key)}?autoplay=1&rel=0&modestbranding=1&playsinline=1`, title: best.name };
    if (best.site === "vimeo") return { url: `https://player.vimeo.com/video/${encodeURIComponent(best.key)}?autoplay=1`, title: best.name };
    return null;
  }

  function open(url, title = "Trailer") {
    if (!url) return;
    ensureModal();
    titleEl.textContent = title || "Trailer";
    stageEl.querySelector("iframe")?.remove();
    const frame = document.createElement("iframe");
    Object.assign(frame, { title, src: url, loading: "lazy" });
    frame.setAttribute("allow", "autoplay; fullscreen; encrypted-media; picture-in-picture");
    frame.setAttribute("referrerpolicy", "strict-origin-when-cross-origin");
    stageEl.appendChild(frame);
    modal.classList.add("show");
    closeBtn.focus();
  }

  function close() {
    if (!modal) return;
    modal.classList.remove("show");
    if (titleEl) titleEl.textContent = "Loading trailer";
    const frame = modal.querySelector("iframe");
    if (frame) {
      try { frame.src = "about:blank"; } catch {}
      frame.remove();
    }
  }

  function searchFallback(item, meta) {
    const title = item?.title || meta?.title || "";
    const year = item?.year || meta?.year || "";
    const query = `${title} ${year} trailer`.trim();
    window.open(`https://www.youtube.com/results?search_query=${encodeURIComponent(query)}`, "_blank", "noopener,noreferrer");
  }

  async function openFor(item, meta) {
    let current = meta;
    let found = pick(current);
    if (!found) {
      const shared = window.CW?.Meta;
      shared?.invalidate(item);
      current = (await shared?.get(item, "detail")) || current;
      found = pick(current);
    }
    if (found) open(found.url, found.title);
    else searchFallback(item, current);
    return !!found;
  }

  (window.CW ||= {}).Trailer = { open, openFor, close, pick, has: (meta) => !!pick(meta) };
})();
