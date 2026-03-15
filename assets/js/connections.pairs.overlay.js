// connections.pairs.overlay.js - Pairs board UI

(function () {
  let _renderBusy = false;
  let _lastHost = null;
  let _lastBoard = null;
  let _resizeTimer = 0;
  let _limitTimer = 0;
  let _limitTries = 0;

  const key = (s) => String(s || "").trim().toUpperCase();
  const brandKey = (k) => ({ PLEX: "plex", SIMKL: "simkl", TRAKT: "trakt", JELLYFIN: "jellyfin", CROSSWATCH: "crosswatch", EMBY: "emby" }[key(k)] || "x");
  const esc = (s) => String(s == null ? "" : s).replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
  const providerMeta = () => window.CW?.ProviderMeta || null;
  const providerLabel = (provider) => providerMeta()?.label?.(provider) || key(provider) || "Provider";
  const providerLogo = (provider) => providerMeta()?.logoPath?.(provider) || "";
  const brandTone = (provider) => providerMeta()?.tone?.(provider) || { solid: "#7c5cff", rgb: "124,92,255" };
  const truthy = (v) => {
    if (v && typeof v === "object") v = v.enable;
    if (typeof v === "string") v = v.toLowerCase().trim();
    return v === true || v === 1 || v === "1" || v === "true" || v === "on" || v === "yes";
  };

  function ensureStyles() {
    const css = `
#pairs_list{scrollbar-gutter:stable;overscroll-behavior:contain}
#pairs_list .pairs-board{display:grid!important;grid-template-columns:1fr!important;gap:14px!important;align-items:start!important;padding:8px 2px 14px!important;overflow:visible!important}
#pairs_list .pair-card{width:100%!important;margin:0!important;display:block!important}
#pairs_list .pair-card{--accent:#7c5cff;--accent-rgb:124,92,255;--src-solid:#7c5cff;--src-rgb:124,92,255;--dst-solid:#7c5cff;--dst-rgb:124,92,255}
#pairs_list .pair-card{
  --chip-w:184px;--btn:34px;--btn-gap:10px;
  position:relative;overflow:hidden;isolation:isolate;
  border-radius:18px;padding:12px 14px;
  background:
    radial-gradient(120% 140% at 0% 50%, rgba(var(--src-rgb),.14) 0%, rgba(var(--src-rgb),0) 48%),
    radial-gradient(110% 140% at 100% 50%, rgba(var(--dst-rgb),.12) 0%, rgba(var(--dst-rgb),0) 44%),
    linear-gradient(180deg, rgba(10,12,20,.98), rgba(5,7,14,.96));
  border:1px solid rgba(255,255,255,.08);
  box-shadow:0 14px 34px rgba(0,0,0,.34);
  backdrop-filter:blur(16px);
  transition:transform .18s ease, box-shadow .18s ease, border-color .18s ease;
  cursor:default!important; user-select:none
}
#pairs_list .pair-card::before{
  content:"";position:absolute;inset:1px;border-radius:17px;pointer-events:none;
  background:
    linear-gradient(180deg, rgba(255,255,255,.06), rgba(255,255,255,0) 30%),
    radial-gradient(70% 130% at 100% 50%, rgba(255,255,255,.05) 0%, rgba(255,255,255,0) 60%);
  opacity:.95;z-index:-1
}
#pairs_list .pair-card::after{content:none}
#pairs_list .pair-card:hover{
  transform:translateY(-2px);
  border-color:rgba(255,255,255,.12);
  box-shadow:0 18px 40px rgba(0,0,0,.44)
}
#pairs_list .pair-row{display:flex;align-items:center;justify-content:space-between;gap:14px;flex-wrap:wrap}
#pairs_list .pair-left{display:flex;align-items:center;gap:10px;min-width:0;flex:1 1 580px}
#pairs_list .ord-badge{
  min-width:34px;height:34px;border-radius:999px;background:#7c5cff;color:#fff;font-size:15px;font-weight:900;
  display:flex;align-items:center;justify-content:center;border:1px solid rgba(255,255,255,.18);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.18);letter-spacing:.01em
}
#pairs_list .pair-pill{
  position:relative;display:inline-flex;align-items:center;justify-content:flex-start;gap:10px;
  width:var(--chip-w);min-height:40px;padding:8px 18px;border-radius:999px;
  font-weight:850;font-size:.93rem;letter-spacing:.02em;color:#f5f7ff;text-align:center;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;border:1px solid rgba(255,255,255,.12);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.10)
}
#pairs_list .pair-pill.src,#pairs_list .pair-pill.dst{color:#fff}
#pairs_list .pair-pill.src{
  background:
    radial-gradient(120% 170% at 0% 50%, rgba(var(--src-rgb),.34) 0%, rgba(var(--src-rgb),.14) 42%, rgba(var(--src-rgb),0) 72%),
    linear-gradient(180deg, rgba(8,10,18,.96), rgba(5,7,14,.94));
  border-color:rgba(var(--src-rgb),.54)
}
#pairs_list .pair-pill.dst{
  background:
    radial-gradient(120% 170% at 0% 50%, rgba(var(--dst-rgb),.34) 0%, rgba(var(--dst-rgb),.14) 42%, rgba(var(--dst-rgb),0) 72%),
    linear-gradient(180deg, rgba(8,10,18,.96), rgba(5,7,14,.94));
  border-color:rgba(var(--dst-rgb),.54)
}
#pairs_list .pair-pill.mode{
  width:auto;min-width:126px;justify-content:center;color:rgba(255,255,255,.9);background:rgba(255,255,255,.05)
}
#pairs_list .pair-pill::before{
  content:"";position:absolute;inset:1px;border-radius:999px;pointer-events:none;
  background:linear-gradient(180deg, rgba(255,255,255,.08), rgba(255,255,255,0) 36%);
  opacity:.8
}
#pairs_list .pair-pill .prov-watermark{
  position:absolute;inset:0;pointer-events:none;opacity:.28
}
#pairs_list .pair-pill .prov-watermark::after{
  content:"";position:absolute;right:0;top:50%;width:72px;height:72px;transform:translateY(-50%);
  background-image:var(--wm);background-repeat:no-repeat;background-position:100% 50%;background-size:contain;
  filter:grayscale(.08) brightness(1.05)
}
#pairs_list .pair-pill-text{position:relative;z-index:1;min-width:0;overflow:hidden;text-overflow:ellipsis}
#pairs_list .arrow{
  color:rgba(235,240,255,.86);font-size:18px;line-height:1;width:26px;height:26px;border-radius:999px;text-align:center;
  display:inline-flex;align-items:center;justify-content:center;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.10)
}
#pairs_list .pair-actions{
  display:flex;align-items:center;gap:var(--btn-gap);justify-content:flex-end;margin-left:auto;
  padding:6px;border-radius:16px;background:linear-gradient(180deg, rgba(255,255,255,.05), rgba(255,255,255,.025));
  border:1px solid rgba(255,255,255,.10);box-shadow:inset 0 1px 0 rgba(255,255,255,.05)
}
#pairs_list .feat-beads{
  display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:999px;
  background:rgba(6,10,18,.58);border:1px solid rgba(255,255,255,.10);box-shadow:inset 0 1px 0 rgba(255,255,255,.05)
}
#pairs_list .feat-beads .bead{
  width:13px;height:13px;border-radius:50%;border:2px solid rgba(255,255,255,.24);
  background:transparent;display:inline-block;transition:transform .12s ease, box-shadow .12s ease, border-color .12s ease
}
#pairs_list .feat-beads .bead:hover{transform:translateY(-1px) scale(1.05)}
#pairs_list .bead.on{border-color:transparent!important}
#pairs_list .bead.wl.on{background:#00ffa3!important;box-shadow:0 0 8px #00ffa3,0 0 18px #00ffa3aa}
#pairs_list .bead.rt.on{background:#ffc400!important;box-shadow:0 0 8px #ffc400,0 0 18px #ffc40099}
#pairs_list .bead.hi.on{background:#2de2ff!important;box-shadow:0 0 8px #2de2ff,0 0 18px #2de2ffaa}
#pairs_list .bead.pr.on{background:#a78bfa!important;box-shadow:0 0 8px #a78bfa,0 0 18px #a78bfaaa}
#pairs_list .bead.pl.on{background:#ff00e5!important;box-shadow:0 0 8px #ff00e5,0 0 18px #ff00e599}
#pairs_list .icon-btn{
  width:var(--btn);height:var(--btn);border-radius:12px;background:rgba(255,255,255,.04);
  border:1px solid rgba(255,255,255,.12);color:#e9edf7;display:inline-flex;align-items:center;justify-content:center;cursor:pointer;
  transition:transform .12s, box-shadow .12s, background .12s, border-color .12s, color .12s
}
#pairs_list .icon-btn:hover{
  background:rgba(255,255,255,.08);transform:translateY(-1px);border-color:rgba(var(--accent-rgb),.28);
  box-shadow:0 10px 24px rgba(0,0,0,.28), 0 0 18px rgba(var(--accent-rgb),.10)
}
#pairs_list .icon-btn .ico{width:18px;height:18px;fill:none;stroke:currentColor;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
#pairs_list .icon-btn.danger:hover{color:#ff6b72;border-color:rgba(255,107,114,.30);box-shadow:0 10px 24px rgba(0,0,0,.28),0 0 18px rgba(255,107,114,.16)}
#pairs_list .icon-btn.power:not(.off){
  color:#12d68c;background:rgba(18,214,140,.12);border-color:rgba(18,214,140,.35);
  box-shadow:0 6px 16px rgba(18,214,140,.16), inset 0 1px 0 rgba(255,255,255,.06)
}
#pairs_list .icon-btn.power:not(.off):hover{background:rgba(18,214,140,.18)}
#pairs_list .icon-btn.power.off{
  color:#ff6b72;background:rgba(255,90,94,.12);border-color:rgba(255,90,94,.35);
  box-shadow:0 6px 16px rgba(255,90,94,.16), inset 0 1px 0 rgba(255,255,255,.06)
}
#pairs_list .sr-only{
  position:absolute!important;width:1px!important;height:1px!important;padding:0!important;margin:-1px!important;overflow:hidden!important;clip:rect(0,0,0,0)!important;clip-path:inset(50%)!important;border:0!important;white-space:nowrap!important
}
#pairs_list [draggable]{user-drag:none;-webkit-user-drag:none}
#pairs_list .pair-card.dragging,#pairs_list .drag-placeholder{display:none!important}
#pairs_list .pair-card.removing{opacity:0;transform:translateY(8px) scale(.985);transition:opacity .18s ease,transform .18s ease}
#pairs_list .cx-tip{
  position:fixed;z-index:99999;pointer-events:none;background:rgba(10,12,18,.96);color:#fff;border:1px solid rgba(255,255,255,.12);
  padding:7px 9px;border-radius:10px;font-size:12px;line-height:1.2;white-space:nowrap;
  box-shadow:0 12px 26px rgba(0,0,0,.34);opacity:0;transform:translateY(6px);transition:opacity .10s ease, transform .10s ease
}
#pairs_list .cx-tip.on{opacity:1;transform:none}
@media (max-width:1280px){
  #pairs_list .pair-left{flex-basis:100%}
  #pairs_list .pair-actions{width:100%;justify-content:flex-end}
}
@media (max-width:920px){
  #pairs_list .pair-card{--chip-w:160px}
  #pairs_list .pair-left{gap:8px}
}
@media (max-width:720px){
  #pairs_list .pair-card{padding:12px}
  #pairs_list .pair-left{flex-basis:100%}
  #pairs_list .pair-actions{width:100%;justify-content:space-between;flex-wrap:wrap;gap:8px;padding:8px 10px}
  #pairs_list .feat-beads{order:1}
  #pairs_list .icon-btn{order:2}
  #pairs_list .pair-pill.src,#pairs_list .pair-pill.dst{width:calc(50% - 22px);min-width:136px}
  #pairs_list .pair-pill.mode{min-width:110px}
}`;
    let s = document.getElementById("cx-pairs-style");
    if (!s) { s = document.createElement("style"); s.id = "cx-pairs-style"; document.head.appendChild(s); }
    s.textContent = css;
  }

  function scheduleViewportLimit(delay = 0) {
    clearTimeout(_limitTimer);
    _limitTimer = setTimeout(() => applyPairsViewportLimit(5), delay);
  }

  function applyPairsViewportLimit(visibleCount = 5) {
    const host = _lastHost;
    const board = _lastBoard;
    if (!host || !board) return;

    if (!host.offsetParent) {
      if (_limitTries++ < 10) scheduleViewportLimit(80);
      return;
    }
    _limitTries = 0;

    const cards = [...board.querySelectorAll(".pair-card")];
    if (cards.length <= visibleCount) {
      host.style.maxHeight = "";
      host.style.overflowY = "";
      host.style.paddingRight = "";
      return;
    }

    const nth = cards[visibleCount - 1];
    if (!nth || nth.offsetHeight < 12) { scheduleViewportLimit(60); return; }

    const pb = parseFloat(getComputedStyle(board).paddingBottom || "0") || 0;
    const max = nth.offsetTop + nth.offsetHeight + pb;

    host.style.maxHeight = Math.ceil(max) + "px";
    host.style.overflowY = "auto";
    host.style.paddingRight = "6px";
  }

  function ensureHost() {
    const host = document.getElementById("pairs_list");
    if (!host) return null;
    let board = host.querySelector(".pairs-board");
    if (!board) { board = document.createElement("div"); board.className = "pairs-board"; host.innerHTML = ""; host.appendChild(board); }
    return { host, board };
  }

  async function loadPairsIfNeeded(force = false) {
    if (!force && Array.isArray(window.cx?.pairs) && window.cx.pairs.length) return;
    if (typeof window.loadPairs === "function") {
      try {
        await window.loadPairs(!!force);
        if (Array.isArray(window.cx?.pairs)) return;
      } catch {}
    }
    try {
      const arr = await fetch("/api/pairs", { cache: "no-store" }).then((r) => r.ok ? r.json() : []);
      window.cx = window.cx || {};
      window.cx.pairs = Array.isArray(arr) ? arr : [];
    } catch (e) {
      window.cx = window.cx || {};
      if (!Array.isArray(window.cx.pairs)) window.cx.pairs = [];
      console.warn("[pairs.overlay] fetch failed", e);
    }
  }

  function installTooltip(host) {
    let tip = host.querySelector(".cx-tip");
    if (!tip) { tip = document.createElement("div"); tip.className = "cx-tip"; host.appendChild(tip); }

    let showTimer = 0;

    const show = (el, ev) => {
      clearTimeout(showTimer);
      const msg = el.getAttribute("data-tip") || el.getAttribute("aria-label") || el.getAttribute("title") || "";
      if (!msg) return;
      showTimer = setTimeout(() => {
        tip.textContent = msg;
        tip.style.left = (ev.clientX + 10) + "px";
        tip.style.top = (ev.clientY + 10) + "px";
        tip.classList.add("on");
      }, 120);
    };

    const move = (ev) => {
      if (!tip.classList.contains("on")) return;
      tip.style.left = (ev.clientX + 10) + "px";
      tip.style.top = (ev.clientY + 10) + "px";
    };

    const hide = () => {
      clearTimeout(showTimer);
      tip.classList.remove("on");
    };

    host.addEventListener("pointerover", (e) => {
      const el = e.target.closest?.("[data-tip]");
      if (!el || !host.contains(el)) return;
      show(el, e);
    }, { passive: true });

    host.addEventListener("pointermove", move, { passive: true });
    host.addEventListener("pointerout", hide, { passive: true });
    host.addEventListener("pointerdown", hide, { passive: true });
    window.addEventListener("scroll", hide, { passive: true });
  }

  window.cxPairsEditClick = function (btn) {
    try {
      const id = btn.closest(".pair-card")?.dataset?.id; if (!id) return;
      if (typeof window.cxEditPair === "function") return window.cxEditPair(id);
      const pairs = Array.isArray(window.cx?.pairs) ? window.cx.pairs : [];
      const pair = pairs.find((p) => String(p.id) === String(id));
      if (pair) {
        if (typeof window.openPairModal === "function") return window.openPairModal(pair);
        if (typeof window.cxOpenModalFor === "function") return window.cxOpenModalFor(pair);
      }
      alert("Edit is not available.");
    } catch (e) { console.warn("[cxPairsEditClick] failed", e); }
  };

  if (typeof window.cxToggleEnable !== "function") {
    window.cxToggleEnable = async function (id, on, inputEl) {
      try {
        const card = (inputEl && inputEl.closest(".pair-card")) || document.querySelector(`#pairs_list .pair-card[data-id="${id}"]`);
        const btn = card?.querySelector(".icon-btn.power");
        if (btn) btn.classList.toggle("off", !on);
        const list = Array.isArray(window.cx?.pairs) ? window.cx.pairs : [];
        const it = list.find((p) => String(p.id) === String(id)); if (it) it.enabled = !!on;
        await fetch(`/api/pairs/${id}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ enabled: !!on })
        }).then(() => { try { document.dispatchEvent(new Event("cx-state-change")); } catch {} });
      } catch (e) { console.warn("[cxToggleEnable] failed", e); }
    };
  }

  async function deletePairCard(id) {
    const board = document.querySelector("#pairs_list .pairs-board");
    const el = board?.querySelector(`.pair-card[data-id="${id}"]`); if (!el) return;
    el.classList.add("removing"); setTimeout(() => el.remove(), 200);
    try { await fetch(`/api/pairs/${id}`, { method: "DELETE" }); } catch (e) { console.warn("delete api failed", e); }
    if (Array.isArray(window.cx?.pairs)) window.cx.pairs = window.cx.pairs.filter((p) => String(p.id) !== String(id));
    setTimeout(() => refreshBadges(board), 220);
  }
  window.deletePairCard = deletePairCard;

  function renderPairsOverlay() {
    ensureStyles();
    const containers = ensureHost(); if (!containers) return;
    const { host, board } = containers;

    _lastHost = host;
    _lastBoard = board;

    const pairs = Array.isArray(window.cx?.pairs) ? window.cx.pairs : [];
    if (!pairs.length) { host.style.display = "none"; board.innerHTML = ""; return; }
    host.style.display = "block";

    const bead = (cls, tip, val) => `<span class="bead ${cls} ${truthy(val) ? "on" : ""}" data-tip="${tip}"></span>`;
    const inst = (v) => (String(v || "default").trim() || "default");
    const pill = (provider, instance, role) => {
      const name = providerLabel(provider), full = String(instance || "default").toLowerCase() !== "default" ? `${name}:${instance}` : name, logo = providerLogo(provider), tip = `${role === "src" ? "Source" : "Target"}: ${full}`;
      return `<span class="pair-pill ${role}" data-tip="${esc(tip)}"><span class="pair-pill-text">${esc(full)}</span><span class="prov-watermark" aria-hidden="true" style="--wm:url('${esc(logo)}')"></span></span>`;
    };

    const html = pairs.map((pr, i) => {
      const src = key(pr.source);
      const dst = key(pr.target);
      const srcInst = inst(pr.source_instance);
      const dstInst = inst(pr.target_instance);
      const isTwo = (pr.mode || "one-way").toLowerCase().includes("two");
      const modeLabel = isTwo ? "Two-way" : "One-way";
      const arrow = isTwo ? "↔" : "→";
      const enabled = pr.enabled !== false;
      const f = pr.features || {};
      const srcTone = brandTone(src);
      const dstTone = brandTone(dst);

      return `
        <div class="pair-card brand-${brandKey(src)} dst-${brandKey(dst)}" data-id="${pr.id || ""}" data-source="${src}" data-target="${dst}" data-mode="${modeLabel}" style="--src-solid:${srcTone.solid};--src-rgb:${srcTone.rgb};--dst-solid:${dstTone.solid};--dst-rgb:${dstTone.rgb};--accent:${srcTone.solid};--accent-rgb:${srcTone.rgb}">
          <div class="pair-row">
            <div class="pair-left">
              <span class="ord-badge" data-tip="Order position">${i + 1}</span>
              ${pill(src, srcInst, "src")}
              <span class="arrow" data-tip="${modeLabel}">${arrow}</span>
              ${pill(dst, dstInst, "dst")}
              <span class="pair-pill mode" data-tip="${modeLabel}">${modeLabel}</span>
            </div>
            <div class="pair-actions">
              <div class="feat-beads" role="group" aria-label="Enabled features">
                ${bead("wl", "Watchlist", f.watchlist)}
                ${bead("rt", "Ratings", f.ratings)}
                ${bead("hi", "History", f.history)}
                ${bead("pr", "Progress", f.progress)}
                ${bead("pl", "Playlists", f.playlists)}
              </div>

              <label class="icon-btn power ${enabled ? "" : "off"}" data-tip="Enable / disable" role="switch" aria-checked="${enabled}">
                <input class="sr-only" type="checkbox" name="pair-enabled" ${enabled ? "checked" : ""}
                  onchange="this.closest('.icon-btn.power')?.setAttribute('aria-checked', this.checked); window.cxToggleEnable && window.cxToggleEnable('${pr.id}', this.checked, this)">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M12 3v6"></path><path d="M5.6 7a8 8 0 1 0 12.8 0"></path></svg>
              </label>

              <button class="icon-btn" data-tip="Move left" onclick="window.movePair && window.movePair('${pr.id}','prev')" aria-label="Move left">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M15 18l-6-6 6-6"></path></svg>
              </button>
              <button class="icon-btn" data-tip="Move right" onclick="window.movePair && window.movePair('${pr.id}','next')" aria-label="Move right">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M9 6l6 6-6 6"></path></svg>
              </button>
              <button class="icon-btn" data-tip="Edit" onclick="window.cxPairsEditClick(this)" aria-label="Edit">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25z"></path><path d="M14.06 4.94l3.75 3.75"></path></svg>
              </button>
              <button class="icon-btn danger" data-tip="Delete" onclick="window.deletePairCard('${pr.id}')" aria-label="Delete">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M3 6h18"></path><path d="M8 6V4h8v2"></path><path d="M6 6l1 14h10l1-14"></path></svg>
              </button>
            </div>
          </div>
        </div>`;
    }).join("");

    board.innerHTML = html;
    scheduleViewportLimit(0);
    installTooltip(host);
    refreshBadges(board);
  }

  function refreshBadges(board) {
    [...board.querySelectorAll(".pair-card")].forEach((el, i) => {
      const b = el.querySelector(".ord-badge"); if (b) b.textContent = String(i + 1);
    });
  }

  if (typeof window.movePair !== "function") {
    window.movePair = async function (id, dir) {
      try {
        const list = Array.isArray(window.cx?.pairs) ? window.cx.pairs : [];
        const idx = list.findIndex((p) => String(p.id) === String(id)); if (idx < 0) return;
        const newIdx = dir === "prev" ? Math.max(0, idx - 1) : Math.min(list.length - 1, idx + 1); if (newIdx === idx) return;

        const [item] = list.splice(idx, 1); list.splice(newIdx, 0, item);

        const board = document.querySelector("#pairs_list .pairs-board");
        const el = board?.querySelector(`.pair-card[data-id="${id}"]`);
        if (el) {
          if (dir === "prev") {
            const prev = el.previousElementSibling;
            if (prev) board.insertBefore(el, prev);
          } else {
            const next = el.nextElementSibling;
            if (next) board.insertBefore(el, next.nextSibling);
          }
          refreshBadges(board);
        }

        try {
          await fetch("/api/pairs/reorder", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(list.map((p) => p.id)) });
        } catch (_) {}
      } catch (e) { console.warn("[movePair] failed", e); }
    };
  }

  function watchSyncSection() {
    const sec = document.getElementById("sec-sync");
    if (!sec || sec.dataset.cxPairsWatch) return;
    sec.dataset.cxPairsWatch = "1";

    const obs = new MutationObserver(() => {
      if (sec.classList.contains("open")) scheduleViewportLimit(120);
    });
    obs.observe(sec, { attributes: true, attributeFilter: ["class"] });
  }

  async function renderOrEnhance(force = false) {
    if (_renderBusy) return;
    _renderBusy = true;
    try { await loadPairsIfNeeded(!!force); renderPairsOverlay(); }
    finally { _renderBusy = false; }
  }

  document.addEventListener("DOMContentLoaded", () => {
    watchSyncSection();
    renderOrEnhance();
  });
  document.addEventListener("cx-state-change", renderOrEnhance);
  window.addEventListener("cx:pairs:changed", () => { renderOrEnhance(true); });

  window.addEventListener("resize", () => {
    clearTimeout(_resizeTimer);
    _resizeTimer = setTimeout(() => scheduleViewportLimit(0), 120);
  }, { passive: true });

  const _origRender = window.renderConnections;
  window.renderConnections = function () { try { if (typeof _origRender === "function") _origRender(); } catch {} renderOrEnhance(); };

  window.cxRenderPairsOverlay = renderOrEnhance;
})();
