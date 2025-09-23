// connections.pairs.overlay.js
(function () {
  // Render guard
  let _renderBusy = false;

  // Helpers
  const key = (s) => String(s || "").trim().toUpperCase();
  const brandKey = (k) => ({ PLEX: "plex", SIMKL: "simkl", TRAKT: "trakt" }[key(k)] || "x");
  const truthy = (v) => {
    if (v && typeof v === "object") v = v.enable;
    if (typeof v === "string") v = v.toLowerCase().trim();
    return v === true || v === 1 || v === "1" || v === "true" || v === "on" || v === "yes";
  };

  // Styles
  function ensureStyles() {
    const css = `
/* ===== Pairs board (scoped) ===== */
#pairs_list .pairs-board{display:flex!important;flex-wrap:wrap!important;gap:12px!important;align-items:flex-start!important;padding:6px 0 12px!important;overflow:visible!important}
#pairs_list .pair-card{flex:0 0 auto!important;width:auto!important;margin:0!important}

/* ===== Card ===== */
#pairs_list .pair-card{
  --chip-w:128px; --btn:30px; --btn-gap:8px; --beads-w:96px;
  position:relative;border-radius:16px;padding:8px 12px;background:rgba(13,15,20,.92);
  border:1px solid rgba(255,255,255,.12);box-shadow:0 8px 24px rgba(0,0,0,.32);
  transition:box-shadow .18s ease,transform .15s ease;display:inline-block;cursor:default!important;user-select:none
}
#pairs_list .pair-card:hover{transform:translateY(-1px);box-shadow:0 12px 36px rgba(0,0,0,.50)}
#pairs_list .pair-row{display:flex;align-items:center;gap:16px}
#pairs_list .pair-left{display:flex;align-items:center;gap:12px;min-width:0}

/* Index badge */
#pairs_list .ord-badge{
  min-width:24px;height:24px;border-radius:999px;background:linear-gradient(135deg,#7b68ee,#a78bfa);
  color:#fff;font-size:13px;font-weight:850;display:flex;align-items:center;justify-content:center;
  box-shadow:0 0 10px rgba(124,92,255,.45)
}

/* Pills */
#pairs_list .pair-pill{display:inline-block;width:var(--chip-w);padding:6px 12px;border-radius:999px;font-weight:800;font-size:.9rem;letter-spacing:.02em;color:#f4f6ff;text-align:center;white-space:nowrap;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.14)}
#pairs_list .pair-pill.mode{width:var(--chip-w)}
#pairs_list .arrow{color:#cfd3e1;opacity:.8;width:18px;text-align:center}

/* Actions rail */
#pairs_list .pair-actions{display:flex;align-items:center;gap:var(--btn-gap);justify-content:flex-end;margin-left:8px}

/* Feature beads (neon) */
#pairs_list .feat-beads{display:inline-flex;align-items:center;gap:8px;padding:6px 10px;border-radius:999px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.12)}
#pairs_list .feat-beads .bead{width:12px;height:12px;border-radius:50%;border:2px solid rgba(255,255,255,.28);background:transparent;display:inline-block;transition:transform .12s ease, box-shadow .12s ease}
#pairs_list .feat-beads .bead:hover{transform:translateY(-1px)}
#pairs_list .bead.on{border-color:transparent!important}
#pairs_list .bead.wl.on{background:#00ffa3 !important; box-shadow:0 0 8px #00ffa3,0 0 18px #00ffa3aa}
#pairs_list .bead.rt.on{background:#ffc400 !important; box-shadow:0 0 8px #ffc400,0 0 18px #ffc40099}
#pairs_list .bead.hi.on{background:#2de2ff !important; box-shadow:0 0 8px #2de2ff,0 0 18px #2de2ffaa}
#pairs_list .bead.pl.on{background:#ff00e5 !important; box-shadow:0 0 8px #ff00e5,0 0 18px #ff00e599}

/* Icon buttons */
#pairs_list .icon-btn{width:var(--btn);height:var(--btn);border-radius:10px;background:transparent;border:1px solid rgba(255,255,255,.14);color:#e5e8f2;display:inline-flex;align-items:center;justify-content:center;cursor:pointer;transition:transform .12s,box-shadow .12s,background .12s,opacity .12s}
#pairs_list .icon-btn:hover{background:rgba(255,255,255,.06);transform:translateY(-1px);box-shadow:0 10px 24px rgba(0,0,0,.26)}
#pairs_list .icon-btn .ico{width:18px;height:18px;fill:none;stroke:currentColor;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
#pairs_list .icon-btn.danger:hover{color:#ff5a5e}
#pairs_list .icon-btn.power:not(.off){color:#12d68c;background:rgba(18,214,140,.12);border-color:rgba(18,214,140,.35);box-shadow:0 4px 14px rgba(18,214,140,.18)}
#pairs_list .icon-btn.power:not(.off):hover{background:rgba(18,214,140,.18)}
#pairs_list .icon-btn.power.off{color:#ff5a5e;background:rgba(255,90,94,.12);border-color:rgba(255,90,94,.35);box-shadow:0 4px 14px rgba(255,90,94,.18)}

/* Visually hidden checkbox for clean click/tip behavior */
#pairs_list .sr-only{
  position:absolute!important;width:1px!important;height:1px!important;padding:0!important;margin:-1px!important;overflow:hidden!important;clip:rect(0,0,0,0)!important;clip-path:inset(50%)!important;border:0!important;white-space:nowrap!important
}

/* Kill any DnD leftovers */
#pairs_list [draggable]{user-drag:none;-webkit-user-drag:none}
#pairs_list .pair-card.dragging, #pairs_list .drag-placeholder{display:none!important}

/* Tooltip bubble (simple, passive) */
#pairs_list .cx-tip{position:fixed;z-index:99999;pointer-events:none;background:rgba(16,18,24,.96);color:#fff;border:1px solid rgba(255,255,255,.12);padding:6px 8px;border-radius:8px;font-size:12px;line-height:1.2;white-space:nowrap;box-shadow:0 8px 20px rgba(0,0,0,.34);opacity:0;transform:translateY(6px);transition:opacity .10s ease, transform .10s ease}
#pairs_list .cx-tip.on{opacity:1;transform:none}
`;
    let s = document.getElementById("cx-pairs-style");
    if (!s) { s = document.createElement("style"); s.id = "cx-pairs-style"; document.head.appendChild(s); }
    s.textContent = css;
  }

  // Host/board
  function ensureHost() {
    const host = document.getElementById("pairs_list");
    if (!host) return null;
    let board = host.querySelector(".pairs-board");
    if (!board) { board = document.createElement("div"); board.className = "pairs-board"; host.innerHTML = ""; host.appendChild(board); }
    return { host, board };
  }

  // Data loader (robust)
  async function loadPairsIfNeeded() {
    if (Array.isArray(window.cx?.pairs) && window.cx.pairs.length) return;
    if (typeof window.loadPairs === "function") { try { await window.loadPairs(); if (Array.isArray(window.cx?.pairs) && window.cx.pairs.length) return; } catch {} }
    try {
      const arr = await fetch("/api/pairs", { cache: "no-store" }).then(r => r.ok ? r.json() : []);
      window.cx = window.cx || {}; window.cx.pairs = Array.isArray(arr) ? arr : [];
    } catch (e) { window.cx = window.cx || {}; if (!Array.isArray(window.cx.pairs)) window.cx.pairs = []; console.warn("[pairs.overlay] fetch failed", e); }
  }

  // Tooltip (passive; 120ms delay; hides on press; never stops events)
  function installTooltip(host) {
    let tip = host.querySelector(".cx-tip");
    if (!tip) { tip = document.createElement("div"); tip.className = "cx-tip"; host.appendChild(tip); }

    let showTimer = 0, active = null;

    const show = (el, ev) => {
      clearTimeout(showTimer);
      const msg = el.getAttribute("data-tip") || el.getAttribute("aria-label") || el.getAttribute("title") || "";
      if (!msg) return;
      showTimer = setTimeout(() => {
        active = el;
        tip.textContent = msg;
        tip.style.left = (ev.clientX + 10) + "px";
        tip.style.top  = (ev.clientY + 10) + "px";
        tip.classList.add("on");
      }, 120);
    };

    const move = (ev) => {
      if (!tip.classList.contains("on")) return;
      tip.style.left = (ev.clientX + 10) + "px";
      tip.style.top  = (ev.clientY + 10) + "px";
    };

    const hide = () => {
      clearTimeout(showTimer);
      tip.classList.remove("on");
      active = null;
    };

    host.addEventListener("pointerover", (e) => {
      const el = e.target.closest?.("[data-tip]");
      if (!el || !host.contains(el)) return;
      show(el, e);
    }, { passive:true });

    host.addEventListener("pointermove", move, { passive:true });
    host.addEventListener("pointerout", hide, { passive:true });
    host.addEventListener("pointerdown", hide, { passive:true });
    window.addEventListener("scroll", hide, { passive:true });
  }

  // Actions
  window.cxPairsEditClick = function (btn) {
    try {
      const id = btn.closest(".pair-card")?.dataset?.id; if (!id) return;
      if (typeof window.cxEditPair === "function") return window.cxEditPair(id);
      const pairs = Array.isArray(window.cx?.pairs) ? window.cx.pairs : [];
      const pair = pairs.find(p => String(p.id) === String(id));
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
        // Update UI state
        const card = (inputEl && inputEl.closest(".pair-card")) || document.querySelector(`#pairs_list .pair-card[data-id="${id}"]`);
        const btn = card?.querySelector(".icon-btn.power");
        if (btn) btn.classList.toggle("off", !on);
        // Update model
        const list = Array.isArray(window.cx?.pairs) ? window.cx.pairs : [];
        const it = list.find(p => String(p.id) === String(id)); if (it) it.enabled = !!on;
        // Persist (best-effort)
        await fetch(`/api/pairs/${id}`, {
          method:"PUT", headers:{ "Content-Type":"application/json" }, body:JSON.stringify({ enabled: !!on })
        }).then(() => { try { document.dispatchEvent(new Event("cx-state-change")); } catch(_){} });
      } catch (e) { console.warn("[cxToggleEnable] failed", e); }
    };
  }

  async function deletePairCard(id) {
    const board = document.querySelector("#pairs_list .pairs-board");
    const el = board?.querySelector(`.pair-card[data-id="${id}"]`); if (!el) return;
    el.classList.add("removing"); setTimeout(() => el.remove(), 200);
    try { await fetch(`/api/pairs/${id}`, { method:"DELETE" }); } catch (e) { console.warn("delete api failed", e); }
    if (Array.isArray(window.cx?.pairs)) window.cx.pairs = window.cx.pairs.filter(p => String(p.id) !== String(id));
    setTimeout(() => refreshBadges(board), 220);
  }
  window.deletePairCard = deletePairCard;

  // Render
  function renderPairsOverlay() {
    ensureStyles();
    const containers = ensureHost(); if (!containers) return;
    const { host, board } = containers;

    const pairs = Array.isArray(window.cx?.pairs) ? window.cx.pairs : [];
    if (!pairs.length) { host.style.display = "none"; board.innerHTML = ""; return; }
    host.style.display = "block";

    const bead = (cls, tip, val) => `<span class="bead ${cls} ${truthy(val) ? "on" : ""}" data-tip="${tip}"></span>`;

    const html = pairs.map((pr, i) => {
      const src = key(pr.source), dst = key(pr.target);
      const isTwo = (pr.mode || "one-way").toLowerCase().includes("two");
      const modeLabel = isTwo ? "Two-way" : "One-way";
      const arrow = isTwo ? "↔" : "→";
      const enabled = pr.enabled !== false;
      const f = pr.features || {};

      return `
        <div class="pair-card brand-${brandKey(src)} dst-${brandKey(dst)}" data-id="${pr.id || ""}" data-source="${src}" data-target="${dst}" data-mode="${modeLabel}">
          <div class="pair-row">
            <div class="pair-left">
              <span class="ord-badge" data-tip="Order position">${i + 1}</span>
              <span class="pair-pill src"  data-tip="Source: ${src}">${src}</span>
              <span class="arrow"          data-tip="${modeLabel}">${arrow}</span>
              <span class="pair-pill dst"  data-tip="Target: ${dst}">${dst}</span>
              <span class="pair-pill mode" data-tip="${modeLabel}">${modeLabel}</span>
            </div>
            <div class="pair-actions">
              <div class="feat-beads" role="group" aria-label="Enabled features">
                ${bead("wl","Watchlist", f.watchlist)}
                ${bead("rt","Ratings",   f.ratings)}
                ${bead("hi","History",   f.history)}
                ${bead("pl","Playlists", f.playlists)}
              </div>

              <!-- Power: label is the click target; checkbox is visually hidden -->
              <label class="icon-btn power ${enabled ? "" : "off"}" data-tip="Enable / disable" role="switch" aria-checked="${enabled}">
                <input class="sr-only" type="checkbox" ${enabled ? "checked" : ""}
                  onchange="this.closest('.icon-btn.power')?.setAttribute('aria-checked', this.checked); window.cxToggleEnable && window.cxToggleEnable('${pr.id}', this.checked, this)">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M12 3v6"></path><path d="M5.6 7a8 8 0 1 0 12.8 0"></path></svg>
              </label>

              <button class="icon-btn" data-tip="Move left"  onclick="window.movePair && window.movePair('${pr.id}','prev')"  aria-label="Move left">
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

    // Tooltips (passive) and badges
    installTooltip(host);
    refreshBadges(board);
  }

  function refreshBadges(board) {
    [...board.querySelectorAll(".pair-card")].forEach((el, i) => {
      const b = el.querySelector(".ord-badge"); if (b) b.textContent = String(i + 1);
    });
  }

  // Button-based reorder only (no drag)
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
          if (dir === "prev") { const prev = el.previousElementSibling; if (prev) board.insertBefore(el, prev); }
          else { const next = el.nextElementSibling; if (next) board.insertBefore(el, next.nextSibling); }
          refreshBadges(board);
        }

        try {
          await fetch("/api/pairs/reorder", { method:"POST", headers:{ "Content-Type":"application/json" }, body:JSON.stringify(list.map(p => p.id)) });
        } catch (_){ /* non-fatal */ }
      } catch (e) { console.warn("[movePair] failed", e); }
    };
  }

  // Orchestrate
  async function renderOrEnhance() {
    if (_renderBusy) return; _renderBusy = true;
    try { await loadPairsIfNeeded(); renderPairsOverlay(); }
    finally { _renderBusy = false; }
  }

  document.addEventListener("DOMContentLoaded", renderOrEnhance);
  document.addEventListener("cx-state-change", renderOrEnhance);

  const _origRender = window.renderConnections;
  window.renderConnections = function () { try { if (typeof _origRender === "function") _origRender(); } catch {} renderOrEnhance(); };

  window.cxRenderPairsOverlay = renderOrEnhance;
})();
