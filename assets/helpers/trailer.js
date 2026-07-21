/* assets/helpers/trailer.js */
/* Shared trailer modal used by the playing card variants */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const STYLE_ID = "cw-trailer-styles";
  const MODAL_ID = "cw-trailer";

  const css = `
  #cw-trailer{position:fixed;inset:0;display:none;align-items:center;justify-content:center;z-index:10050;background:radial-gradient(120% 120% at 12% 0%,rgba(96,88,214,.22),transparent 40%),radial-gradient(120% 120% at 100% 100%,rgba(34,118,215,.18),transparent 44%),rgba(3,5,10,.78);backdrop-filter:blur(8px) saturate(120%);-webkit-backdrop-filter:blur(8px) saturate(120%)}
  #cw-trailer.show{display:flex}
  #cw-trailer .cw-trailer-box{position:relative;width:min(94vw,1120px);border-radius:24px;overflow:hidden;background:linear-gradient(180deg,rgba(10,12,20,.985),rgba(4,6,12,.99));box-shadow:0 32px 80px rgba(0,0,0,.58),inset 0 1px 0 rgba(255,255,255,.05)}
  #cw-trailer .cw-trailer-shell{position:relative;display:grid;grid-template-rows:auto minmax(0,1fr);min-height:min(82vh,760px)}
  #cw-trailer .cw-trailer-shell::before{content:"";position:absolute;inset:0;pointer-events:none;background:radial-gradient(90% 120% at 0% 0%,rgba(98,92,182,.16),transparent 42%),linear-gradient(180deg,rgba(255,255,255,.04),transparent 28%)}
  #cw-trailer .cw-trailer-head{position:relative;z-index:1;display:flex;align-items:flex-start;justify-content:space-between;gap:16px;padding:16px 18px 14px;border-bottom:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.01))}
  #cw-trailer .cw-trailer-meta{min-width:0;display:grid;gap:4px}
  #cw-trailer .cw-trailer-kicker{font-size:11px;font-weight:800;letter-spacing:.16em;text-transform:uppercase;color:rgba(206,216,236,.66)}
  #cw-trailer .cw-trailer-title{font-size:20px;font-weight:800;line-height:1.15;color:#f7f9ff;letter-spacing:-.02em}
  #cw-trailer .cw-trailer-close{display:inline-flex;align-items:center;justify-content:center;min-width:42px;min-height:42px;padding:0 12px;border-radius:999px;border:1px solid rgba(255,255,255,.12);background:linear-gradient(180deg,rgba(255,255,255,.08),rgba(255,255,255,.03));color:#f7f9ff;cursor:pointer;line-height:1;transition:background .16s ease,border-color .16s ease,transform .16s ease}
  #cw-trailer .cw-trailer-close:hover{transform:translateY(-1px);border-color:rgba(255,255,255,.20);background:linear-gradient(180deg,rgba(255,255,255,.12),rgba(255,255,255,.05))}
  #cw-trailer .cw-trailer-close .material-symbol,#cw-trailer .cw-trailer-close .material-symbols-rounded{font-size:20px;line-height:1;color:currentColor}
  #cw-trailer .cw-trailer-stage{position:relative;z-index:1;margin:14px;border-radius:20px;overflow:hidden;min-height:min(68vh,640px);background:linear-gradient(180deg,rgba(7,9,15,.98),rgba(2,3,7,.99));border:1px solid rgba(255,255,255,.08);box-shadow:inset 0 1px 0 rgba(255,255,255,.04)}
  #cw-trailer .cw-trailer-stage::before{content:"";position:absolute;inset:0;pointer-events:none;background:radial-gradient(80% 80% at 50% 0%,rgba(98,92,182,.12),transparent 38%)}
  #cw-trailer .cw-trailer-stage iframe{width:100%;height:100%;min-height:min(68vh,640px);display:block;border:0;position:relative;z-index:1}
  html[data-cw-theme="flat-light"] #cw-trailer{background:rgba(15,23,42,.52)}
  html[data-cw-theme="flat-light"] #cw-trailer .cw-trailer-box{background:#f8fafc;box-shadow:0 24px 60px rgba(15,23,42,.22)}
  html[data-cw-theme="flat-light"] #cw-trailer .cw-trailer-shell::before{background:none}
  html[data-cw-theme="flat-light"] #cw-trailer .cw-trailer-head{border-bottom-color:rgba(16,24,40,.14);background:#ffffff}
  html[data-cw-theme="flat-light"] #cw-trailer .cw-trailer-kicker{color:#475467}
  html[data-cw-theme="flat-light"] #cw-trailer .cw-trailer-title{color:#111827}
  html[data-cw-theme="flat-light"] #cw-trailer .cw-trailer-close{border-color:rgba(16,24,40,.16);background:#ffffff;color:#172033}
  html[data-cw-theme="flat-light"] #cw-trailer .cw-trailer-close:hover{background:#eef2f7}
  html[data-cw-theme="flat-light"] #cw-trailer .cw-trailer-stage{border-color:rgba(16,24,40,.14);background:#0b0f17}
  html[data-cw-theme="flat-light"] #cw-trailer .cw-trailer-stage::before{background:none}
  @media (max-width:760px){
    #cw-trailer .cw-trailer-head{padding:14px 14px 12px}
    #cw-trailer .cw-trailer-title{font-size:17px}
    #cw-trailer .cw-trailer-stage{margin:10px;min-height:min(60vh,420px)}
    #cw-trailer .cw-trailer-stage iframe{min-height:min(60vh,420px)}
  }
  `;

  let modal = null;
  let titleEl = null;
  let stageEl = null;
  let closeBtn = null;

  function ensureStyles() {
    if (document.getElementById(STYLE_ID)) return;
    const style = document.createElement("style");
    style.id = STYLE_ID;
    style.textContent = css;
    document.head.appendChild(style);
  }

  function ensureModal() {
    if (modal?.isConnected) return modal;
    ensureStyles();
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
