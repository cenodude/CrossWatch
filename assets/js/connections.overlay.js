// connections.overlay.js — Providers connection UI 

(function () {
  // ────────────────────────────────────────────────────────────────────────────
  // Globals (drag state)
  // ────────────────────────────────────────────────────────────────────────────
  let dragSrc = null;
  let isDragging = false;

  // ────────────────────────────────────────────────────────────────────────────
  // Branding helper
  // ────────────────────────────────────────────────────────────────────────────
  /**
   * Brand details (class + icon) for a provider name.
   * @param {string} name
   * @returns {{cls:string, icon:string}}
   */
  function _brandInfo(name) {
    const key = String(name || "").trim().toUpperCase();
    if (key === "PLEX") return { cls: "brand-plex", icon: "/assets/img/PLEX.svg" };
    if (key === "SIMKL") return { cls: "brand-simkl", icon: "/assets/img/SIMKL.svg" };
    if (key === "TRAKT") return { cls: "brand-trakt", icon: "/assets/img/TRAKT.svg" };
    if (key === "JELLYFIN") return { cls: "brand-jellyfin", icon: "/assets/img/JELLYFIN.svg" };
    if (key === "EMBY")     return { cls: "brand-emby",     icon: "/assets/img/EMBY.svg" };
    if (key === "MDBLIST")     return { cls: "brand-mdblist",     icon: "/assets/img/MDBLIST.svg" };
    if (key === "CROSSWATCH") return { cls: "brand-crosswatch", icon: "/assets/img/CROSSWATCH.svg" };
    return { cls: "", icon: "" };
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Styles (injected once)
  // ────────────────────────────────────────────────────────────────────────────
  function ensureStyles() {
    if (document.getElementById("cx-overlay-style")) return;
    const css = `
      :root{ --plex:#e5a00d; --simkl:#00b7eb; --trakt:#ed1c24; --jellyfin:#9654f4; --emby:#52b54b; --mdblist:#00a3ff; --crosswatch:#7c5cff; }

      .cx-grid{
        display:grid;
        grid-template-columns:repeat(auto-fill,minmax(200px,1fr));
        gap:16px;
        margin-top:6px
      }

      /* Subtle glass look (non-invasive) */
      .prov-card{
        position:relative;
        overflow:hidden;
        border:1px solid rgba(255,255,255,.10);
        border-radius:16px;
        padding:14px;
        background:
          linear-gradient(180deg, rgba(255,255,255,.06), rgba(255,255,255,.02)),
          rgba(13,15,20,.86);
        backdrop-filter: blur(6px);
        box-shadow:
          inset 0 1px 0 rgba(255,255,255,.06),
          0 4px 18px rgba(0,0,0,.35);
        transition:transform .12s ease, box-shadow .18s ease, filter .18s ease, opacity .18s ease;
        user-select:none;
      }
      .prov-card:focus-visible{ outline:2px solid rgba(124,92,255,.7); }
      .prov-card.selected{ outline:2px solid rgba(124,92,255,.6); box-shadow:0 0 22px rgba(124,92,255,.25) }

      /* Uppercase */
      .prov-title{
        font-family: inherit;
        font-weight: 800;
        font-size: .9rem;
        letter-spacing: .02em;
        color: #fff;
        margin-bottom: 8px;
        text-transform: uppercase;
      }

      /* Brand accents */
      .prov-card.brand-plex{border-color:rgba(229,160,13,.55); box-shadow:inset 0 0 0 1px rgba(229,160,13,.20), 0 0 24px rgba(229,160,13,.18)}
      .prov-card.brand-simkl{border-color:rgba(0,183,235,.55); box-shadow:inset 0 0 0 1px rgba(0,183,235,.20), 0 0 24px rgba(0,183,235,.18)}
      .prov-card.brand-trakt{border-color:rgba(237,28,36,.55); box-shadow:inset 0 0 0 1px rgba(237,28,36,.20), 0 0 24px rgba(237,28,36,.18)}
      .prov-card.brand-jellyfin{border-color:rgba(150,84,244,.55);box-shadow:inset 0 0 0 1px rgba(150,84,244,.2),0 0 24px rgba(150,84,244,.18);}
      .prov-card.brand-emby{border-color:rgba(82,181,75,.55); box-shadow:inset 0 0 0 1px rgba(82,181,75,.20), 0 0 24px rgba(82,181,75,.18)}
      .prov-card.brand-mdblist{border-color:rgba(0,163,255,.55); box-shadow:inset 0 0 0 1px rgba(0,163,255,.20), 0 0 24px rgba(0,163,255,.18)}
      .prov-card.brand-crosswatch{border-color: rgba(124,92,255,.55); box-shadow:inset 0 0 0 1px rgba(124,92,255,.25), 0 0 24px rgba(124,92,255,.20);}
      }

      .prov-caps{display:flex;gap:6px;margin:8px 0}
      .prov-caps .dot{width:8px;height:8px;border-radius:50%;background:#444}
      .prov-caps .dot.on{background:#5ad27a}
      .prov-caps .dot.off{background:#555}

      .btn.neon{
        display:inline-block;padding:8px 14px;border-radius:12px;
        border:1px solid rgba(255,255,255,.18);background:#121224;color:#fff;
        font-weight:700;cursor:pointer
      }
      .prov-action{ position:relative; z-index:2; }

      /* blended watermark (unchanged) */
      .prov-watermark { position:absolute; inset:0; pointer-events:none; z-index:0; opacity:.4; }
      .brand-plex  .prov-watermark{ background:
        radial-gradient(80% 60% at 35% 40%, rgba(229,160,13,.18), transparent 60%),
        radial-gradient(80% 60% at 50% 70%, rgba(229,160,13,.10), transparent 70%); }
      .brand-simkl .prov-watermark{ background:
        radial-gradient(80% 60% at 35% 40%, rgba(0,183,235,.20), transparent 60%),
        radial-gradient(80% 60% at 50% 70%, rgba(0,183,235,.10), transparent 70%); }
      .brand-trakt .prov-watermark{ background:
        radial-gradient(80% 60% at 35% 40%, rgba(237,28,36,.18), transparent 60%),
        radial-gradient(80% 60% at 50% 70%, rgba(237,28,36,.10), transparent 70%); }
      .brand-jellyfin .prov-watermark{ background:
        radial-gradient(80% 60% at 35% 40%, rgba(150,84,244,.18), transparent 60%),
        radial-gradient(80% 60% at 50% 70%, rgba(150,84,244,.10), transparent 70%); }
      .brand-emby .prov-watermark{ background:
        radial-gradient(80% 60% at 35% 40%, rgba(82,181,75,.18), transparent 60%),
        radial-gradient(80% 60% at 50% 70%, rgba(82,181,75,.10), transparent 70%); }
      .brand-mdblist .prov-watermark{ background:
        radial-gradient(80% 60% at 35% 40%, rgba(0,163,255,.18), transparent 60%),
        radial-gradient(80% 60% at 50% 70%, rgba(0,163,255,.10), transparent 70%); }
      .brand-crosswatch .prov-watermark{ background:
        radial-gradient(80% 60% at 35% 40%, rgba(124,92,255,.18), transparent 60%),
        radial-gradient(80% 60% at 50% 70%, rgba(124,92,255,.10), transparent 70%); } 
      .prov-watermark::after{
        content:""; position:absolute; top:50%; right:8%;
        width:120%; aspect-ratio:1/1; transform:translateY(-50%);
        background-repeat:no-repeat; background-position:center; background-size:contain;
        background-image: var(--wm);
        filter:grayscale(1) brightness(1.15);
        opacity:.14; mix-blend-mode:screen;
      }

      /* DnD feedback */
      .prov-card[draggable="true"]{ cursor:grab; }
      .prov-card.dragging{
        cursor:grabbing;
        opacity:.87;
        transform:scale(.985);
        animation: prov-wiggle .35s ease-in-out infinite;
        z-index: 2;
      }
      @keyframes prov-wiggle{
        0%{ transform:scale(.985) rotate(-.6deg); }
        50%{ transform:scale(.985) rotate(.6deg); }
        100%{ transform:scale(.985) rotate(-.6deg); }
      }
      .prov-card.drop-ok{
        outline:2px dashed rgba(255,255,255,.35);
        outline-offset:-3px;
      }
      .prov-card.drop-ok::before{
        content:"Drop for Target";
        position:absolute; bottom:10px; right:12px; padding:4px 8px; font-size:11px; border-radius:8px;
        background:rgba(0,0,0,.45); border:1px solid rgba(255,255,255,.22);
      }
      .prov-card.pulse{ animation: prov-pulse .6s ease-out 1; }
      @keyframes prov-pulse{
        0%{ box-shadow:0 0 0 0 rgba(124,92,255,.45); }
        100%{ box-shadow:0 0 0 14px rgba(124,92,255,0); }
      }
    `;
    const s = document.createElement("style");
    s.id = "cx-overlay-style";
    s.textContent = css;
    document.head.appendChild(s);
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Capability helper
  // ────────────────────────────────────────────────────────────────────────────
  /**
   * capability check for a provider object.
   * @param {any} obj provider manifest-like object
   * @param {string} key capability key (e.g., "watchlist")
   * @returns {boolean}
   */
  function cap(obj, key) {
    try { return !!(obj && obj.features && obj.features[key]); } catch (_) { return false; }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // UI: (Re)build providers list
  // ────────────────────────────────────────────────────────────────────────────
  /** (Re)build the providers list UI based on current state in window.cx. */
  function rebuildProviders() {
    ensureStyles();
    const host = document.getElementById("providers_list");
    if (!host) return;
    const provs = (window.cx && window.cx.providers) || [];
    if (!provs.length) return;

    const sel = (window.cx && window.cx.connect) || {};
    const selSrc = sel.source || null;

    const html = provs.map((p) => {
      const rawName = p.label || p.name;
      const displayName = String(rawName || "").toUpperCase(); // force uppercase display
      const brand = _brandInfo(p.name);
      const isSrc = !!(selSrc && String(selSrc).toUpperCase() === String(p.name).toUpperCase());
      const btnLab = !selSrc ? "Set as Source" : isSrc ? "Cancel" : "Set as Target";
      const btnOn = !selSrc
        ? `cxToggleConnect('${p.name}')`
        : isSrc
          ? `cxToggleConnect('${p.name}')`
          : `cxPickTarget('${p.name}')`;

      const wl = cap(p, "watchlist"),
            rat = cap(p, "ratings"),
            hist = cap(p, "history"),
            pl = cap(p, "playlists");

      const caps = `<div class="prov-caps">
        <span class="dot ${wl ? "on" : "off"}"   title="Watchlist"></span>
        <span class="dot ${rat ? "on" : "off"}"  title="Ratings"></span>
        <span class="dot ${hist ? "on" : "off"}" title="History"></span>
        <span class="dot ${pl ? "on" : "off"}"   title="Playlists"></span>
      </div>`;

      const wmStyle = brand.icon ? ` style="--wm:url('${brand.icon}')" ` : "";

      return `
        <div class="prov-card ${brand.cls}${isSrc ? " selected" : ""}" data-prov="${p.name}" draggable="true" tabindex="0">
          <div class="prov-watermark"${wmStyle}></div>
          <div class="prov-head">
            <div class="prov-title">${displayName}</div>
          </div>
          ${caps}
          <button type="button" class="btn neon prov-action" onclick="${btnOn}">${btnLab}</button>
        </div>`;
    }).join("");

    const wrap =
      host.querySelector(".cx-grid") ||
      (() => {
        const d = document.createElement("div");
        d.className = "cx-grid";
        host.innerHTML = "";
        host.appendChild(d);
        return d;
      })();
    wrap.innerHTML = html;
  }

  // refresh on state change
  document.addEventListener("cx-state-change", function () {
    try { rebuildProviders(); } catch (_) {}
  });

  // ────────────────────────────────────────────────────────────────────────────
  // Glue: keep original render
  // ────────────────────────────────────────────────────────────────────────────
  const _origRender = window.renderConnections;
  window.renderConnections = function () {
    try { if (typeof _origRender === "function") _origRender(); } catch {}
    rebuildProviders();
  };

  // ────────────────────────────────────────────────────────────────────────────
  // Glue: keep original start + update selection
  // ────────────────────────────────────────────────────────────────────────────
  const _origStart = window.cxStartConnect;
  window.cxStartConnect = function (name) {
    try { if (typeof _origStart === "function") _origStart(name); } catch {}
    window.cx = window.cx || {};
    window.cx.connect = { source: String(name), target: null };
    try { window.renderConnections(); } catch (_) {}
  };

  // ────────────────────────────────────────────────────────────────────────────
  // Actions: pick target / toggle connect
  // ────────────────────────────────────────────────────────────────────────────
  /**
   * Set the target provider for an in-progress connect action. If a modal
   * opener is available, it is used; otherwise, a custom event is dispatched.
   */
  window.cxPickTarget = window.cxPickTarget || function (name) {
    if (!window.cx || !window.cx.connect || !window.cx.connect.source) return;
    window.cx.connect.target = String(name);
    const detail = { source: window.cx.connect.source, target: window.cx.connect.target };
    try {
      const srcCard = document.querySelector(`.prov-card[data-prov="\${detail.source}"]`);
      const tgtCard = document.querySelector(`.prov-card[data-prov="\${detail.target}"]`);
      srcCard && srcCard.classList.add('pulse');
      tgtCard && tgtCard.classList.add('pulse');
    } catch(_) {}
    if (typeof window.cxOpenModalFor === "function") {
      try { window.cxOpenModalFor(detail); } catch (e) { console.warn("cxOpenModalFor failed", e); }
    } else {
      window.dispatchEvent(new CustomEvent("cx:open-modal", { detail }));
    }
  };

  /**
   * Toggle connect state: no source → set source; other provider → set target;
   * same source → cancel selection.
   */
  window.cxToggleConnect = function (name) {
    name = String(name || "");
    window.cx = window.cx || { providers: [], pairs: [], connect: { source: null, target: null } };
    const sel = window.cx.connect || (window.cx.connect = { source: null, target: null });
    if (!sel.source) { window.cxStartConnect(name); return; }
    if (sel.source && sel.source !== name) { window.cxPickTarget(name); return; }
    window.cx.connect = { source: null, target: null };
    try { window.renderConnections(); } catch (_) {}
  };

  // ────────────────────────────────────────────────────────────────────────────
  // Drag & Drop (button-safe)
  // ────────────────────────────────────────────────────────────────────────────
  // Disable button clicks during drag so a drop can be performed cleanly.
  document.addEventListener("click", (e) => {
    if (!isDragging) return;
    if (e.target.closest && e.target.closest(".prov-action")) {
      e.preventDefault(); e.stopPropagation(); e.stopImmediatePropagation();
    }
  }, true);

  document.addEventListener("dragstart", (e) => {
    const card = e.target.closest && e.target.closest(".prov-card");
    if (!card) return;
    if (e.target.closest && e.target.closest(".prov-action")) {
      e.preventDefault(); return;
    }
    const name = card.getAttribute("data-prov");
    if (!name) return;

    dragSrc = name;
    isDragging = true;

    try { e.dataTransfer.setData("text/plain", name); e.dataTransfer.effectAllowed = "move"; } catch (_) {}
    card.classList.add("dragging");

    document.querySelectorAll('.prov-card').forEach(c=>{
      if (c !== card) c.classList.add('drop-ok');
    });
  });

  document.addEventListener("dragend", (e) => {
    const card = e.target.closest && e.target.closest(".prov-card");
    if (card) card.classList.remove("dragging");
    isDragging = false;
    dragSrc = null;
    document.querySelectorAll('.prov-card').forEach(c=>c.classList.remove('drop-ok'));
  });

  document.addEventListener("dragover", (e) => {
    const card = e.target.closest && e.target.closest(".prov-card");
    if (card) { e.preventDefault(); e.dataTransfer && (e.dataTransfer.dropEffect = "move"); }
  });

  document.addEventListener("drop", (e) => {
    const card = e.target.closest && e.target.closest(".prov-card");
    if (!card) return; e.preventDefault();
    if (!dragSrc) return;
    const target = card.getAttribute("data-prov");
    if (target && dragSrc && target !== dragSrc) {
      try { window.cxToggleConnect(dragSrc); } catch (_) {}
      try { window.cxPickTarget(target); } catch (_) {}
    }
    isDragging = false;
    dragSrc = null;
    document.querySelectorAll('.prov-card').forEach(c=>c.classList.remove('drop-ok','dragging'));
  });

  // ────────────────────────────────────────────────────────────────────────────
  // Keyboard helpers
  // ────────────────────────────────────────────────────────────────────────────
  document.addEventListener("keydown", (e)=>{
    const card = e.target.closest && e.target.closest(".prov-card");
    if (!card) return;
    if (e.key === "Enter" && !e.shiftKey){
      e.preventDefault();
      const name = card.getAttribute("data-prov");
      window.cxToggleConnect(name);
    }
    if (e.key === "Enter" && e.shiftKey){
      e.preventDefault();
      const name = card.getAttribute("data-prov");
      window.cxPickTarget(name);
    }
  });

  // ────────────────────────────────────────────────────────────────────────────
  // Init
  // ────────────────────────────────────────────────────────────────────────────
  document.addEventListener("DOMContentLoaded", () => {
    try { window.renderConnections && window.renderConnections(); } catch (_) {}
  });
})();
