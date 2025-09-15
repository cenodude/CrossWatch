// connections.pairs.overlay.js
(function () {
  // Render lock
  let _pairsRenderBusy = false;

  // Provider icons and helpers
  const ICONS = { PLEX: "/assets/PLEX.svg", SIMKL: "/assets/SIMKL.svg", TRAKT: "/assets/TRAKT.svg" };
  const key = (s) => String(s || "").trim().toUpperCase();
  const brandKey = (k) => ({ PLEX: "plex", SIMKL: "simkl", TRAKT: "trakt" }[key(k)] || "x");

  // Inject component-scoped styles (prefixed with #pairs_list)
  function ensureStyles() {
    const css = `
/* ===== Pairs board (scoped) ===== */
#pairs_list .pairs-board{
  display:flex!important;flex-direction:row!important;flex-wrap:wrap!important;
  align-items:flex-start!important;gap:12px!important;padding:6px 0 12px!important;overflow:visible!important;
}
/* Kill legacy widths */
#pairs_list .pair-card{flex:0 0 auto!important;width:auto!important;margin:0!important}

/* ===== Card: one-line layout ===== */
#pairs_list .pair-card{
  --chip-w:128px;            /* fixed chip width for alignment */
  --btn:30px;                /* icon button size */
  --btn-gap:8px;             /* gap between buttons */
  --actions-w:calc((var(--btn)*4)+(var(--btn-gap)*3)); /* power + left + right + edit + delete (delete shares group) */
  position:relative;border-radius:16px;padding:8px 12px;background:rgba(13,15,20,.92);
  border:1px solid rgba(255,255,255,.12);box-shadow:0 8px 24px rgba(0,0,0,.32);
  transition:box-shadow .18s ease,transform .15s ease;overflow:hidden;display:inline-block;
}
#pairs_list .pair-card:hover{transform:translateY(-1px);box-shadow:0 12px 36px rgba(0,0,0,.50)}

#pairs_list .pair-row{display:flex;align-items:center;gap:16px;position:relative;z-index:3}
#pairs_list .pair-left{display:flex;align-items:center;gap:12px;min-width:0}

/* Index badge */
#pairs_list .ord-badge{
  min-width:24px;height:24px;border-radius:999px;background:linear-gradient(135deg,#7b68ee,#a78bfa);
  color:#fff;font-size:13px;font-weight:850;display:flex;align-items:center;justify-content:center;
  box-shadow:0 0 10px rgba(124,92,255,.45)
}

/* Chips (renamed to avoid global .pill collisions) */
#pairs_list .pair-pill{
  display:inline-block;width:var(--chip-w);padding:6px 12px;border-radius:999px;
  font-weight:800;font-size:.9rem;letter-spacing:.02em;color:#f4f6ff;text-align:center;white-space:nowrap;
  background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.14)
}
#pairs_list .pair-pill.mode{width:var(--chip-w)}
#pairs_list .arrow{color:#cfd3e1;opacity:.75;width:18px;text-align:center}

/* Action rail (fixed width so rows align) */
#pairs_list .pair-actions{
  display:flex;align-items:center;gap:var(--btn-gap);
  width:var(--actions-w);justify-content:flex-end;margin-left:8px;
}
#pairs_list .icon-btn{
  width:var(--btn);height:var(--btn);border-radius:10px;background:transparent;border:1px solid rgba(255,255,255,.14);color:#e5e8f2;
  display:inline-flex;align-items:center;justify-content:center;cursor:pointer;
  transition:transform .12s,box-shadow .12s,background .12s,opacity .12s
}
#pairs_list .icon-btn:hover{background:rgba(255,255,255,.06);transform:translateY(-1px);box-shadow:0 10px 24px rgba(0,0,0,.26)}
#pairs_list .icon-btn:active{transform:none}
#pairs_list .icon-btn .ico{width:18px;height:18px;fill:none;stroke:currentColor;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
#pairs_list .icon-btn.danger:hover{color:#ff5a5e}
#pairs_list .icon-btn.power:not(.off){color:#12d68c;background:rgba(18,214,140,.12);border-color:rgba(18,214,140,.35);box-shadow:0 4px 14px rgba(18,214,140,.18)}
#pairs_list .icon-btn.power:not(.off):hover{background:rgba(18,214,140,.18)}

#pairs_list .icon-btn.power.off{opacity:1;color:#ff5a5e;background:rgba(255,90,94,.12);border-color:rgba(255,90,94,.35);box-shadow:0 4px 14px rgba(255,90,94,.18)}

/* Subtle watermarks */
#pairs_list .cx-conn{position:relative}
#pairs_list .cx-conn::before,#pairs_list .cx-conn::after{
  content:"";position:absolute;top:0;bottom:0;pointer-events:none;z-index:0;opacity:.05;
  background-repeat:no-repeat;background-position:center;background-size:140% auto
}
#pairs_list .cx-conn::before{left:-6%;width:54%;background-image:var(--src-logo)}
#pairs_list .cx-conn::after{right:-6%;width:54%;background-image:var(--dst-logo)}
#pairs_list .cx-conn .wm-mask{
  position:absolute;inset:0;z-index:1;pointer-events:none;
  background:linear-gradient(90deg, rgba(13,15,20,1) 0%, rgba(13,15,20,0) 18%, rgba(13,15,20,0) 82%, rgba(13,15,20,1) 100%);
}

/* Drag & drop */
#pairs_list .pair-card[draggable=true]{cursor:grab}
#pairs_list .pair-card.dragging{opacity:.85;transform:scale(.985) rotate(.6deg);animation:cx-wiggle .35s ease-in-out infinite;z-index:10}
#pairs_list .drag-placeholder{border-radius:16px;outline:2px dashed rgba(255,255,255,.24);outline-offset:-2px;min-height:44px;margin:6px 0;background:rgba(255,255,255,.04)}
@keyframes cx-wiggle{0%{transform:scale(.985) rotate(-.6deg)}50%{transform:scale(.985) rotate(.6deg)}100%{transform:scale(.985) rotate(-.6deg)}

/* Remove transition */
#pairs_list .pair-card.removing{
  opacity:0;transform:translateY(-10px) scale(.98);height:0!important;margin:0!important;padding:0!important;overflow:hidden!important;
  transition:opacity .25s ease,transform .25s ease,height .25s ease,margin .25s ease,padding .25s ease
}
`;
    let s = document.getElementById("cx-pairs-style");
    if (!s) {
      s = document.createElement("style");
      s.id = "cx-pairs-style";
      document.head.appendChild(s);
    }
    s.textContent = css;
  }

  // Ensure host container exists
  function ensureHost() {
    const host = document.getElementById("pairs_list");
    if (!host) return null;
    let board = host.querySelector(".pairs-board");
    if (!board) {
      board = document.createElement("div");
      board.className = "pairs-board";
      host.innerHTML = "";
      host.appendChild(board);
    }
    return { host, board };
  }

  // Lazy-load pairs if not present
  async function loadPairsIfNeeded() {
    if (Array.isArray(window.cx?.pairs) && window.cx.pairs.length) return;
    if (typeof window.loadPairs === "function") {
      try { await window.loadPairs(); return; } catch {}
    }
    try {
      const arr = await fetch("/api/pairs", { cache: "no-store" }).then((r) => r.json());
      window.cx = window.cx || {};
      window.cx.pairs = Array.isArray(arr) ? arr : [];
    } catch (e) {
      console.warn("[pairs.overlay] fetch /api/pairs failed", e);
    }
  }

  // Edit callback (respects your existing edit handlers)
  window.cxPairsEditClick = function (btn) {
    try {
      const id = btn.closest(".pair-card")?.dataset?.id;
      if (!id) return;
      if (typeof window.cxEditPair === "function") return window.cxEditPair(id);

      const pairs = (window.cx && Array.isArray(window.cx.pairs)) ? window.cx.pairs : [];
      const pair = pairs.find((p) => String(p.id) === String(id));
      if (pair) {
        if (typeof window.openPairModal === "function") return window.openPairModal(pair);
        if (typeof window.cxOpenModalFor === "function") return window.cxOpenModalFor(pair);
      }
      alert("Edit is not available.");
    } catch (e) {
      console.warn("[cxPairsEditClick] failed", e);
    }
  };

  // Enable/disable toggle
  if (typeof window.cxToggleEnable !== "function") {
    window.cxToggleEnable = async function (id, on, inputEl) {
            try {
        const card = (inputEl && inputEl.closest(".pair-card")) || document.querySelector(`#pairs_list .pair-card[data-id="${id}"]`);
        const btn = card?.querySelector(".icon-btn.power");
        if (btn) btn.classList.toggle("off", !on);

        // Update client cache if present
        const list = Array.isArray(window.cx?.pairs) ? window.cx.pairs : [];
        const it = list.find((p) => String(p.id) === String(id));
        if (it) it.enabled = !!on;

        // Build payload that satisfies backend PairIn (source/target required)
        const src = card?.dataset?.source || (it && it.source) || "";
        const tgt = card?.dataset?.target || (it && it.target) || "";
        let mode = (card?.dataset?.mode || (it && it.mode) || "").toString().toLowerCase().replace(/\s+/g, "-");
        if (mode !== "one-way" && mode !== "two-way") mode = undefined;

        const payload = { enabled: !!on };
        if (src) payload.source = src;
        if (tgt) payload.target = tgt;
        if (mode) payload.mode = mode;

        await fetch(`/api/pairs/${id}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        })
        .then(() => { try { document.dispatchEvent(new Event("cx-state-change")); } catch (_) {} })
        .catch(() => {});
      } catch (e) {
        console.warn("toggle failed", e);
      }};
  }

  // Delete card (optimistic UI)
  async function deletePairCard(id) {
    const board = document.querySelector("#pairs_list .pairs-board");
    const el = board?.querySelector(`.pair-card[data-id="${id}"]`);
    if (!el) return;
    el.classList.add("removing");
    setTimeout(() => el.remove(), 280);

    try { await fetch(`/api/pairs/${id}`, { method: "DELETE" }); } catch (e) { console.warn("delete api failed", e); }
    if (Array.isArray(window.cx?.pairs)) {
      window.cx.pairs = window.cx.pairs.filter((p) => String(p.id) !== String(id));
    }
    setTimeout(() => refreshBadges(board), 300);
  }
  window.deletePairCard = deletePairCard;

  // Render all pairs
  function renderPairsOverlay() {
    ensureStyles();
    const containers = ensureHost();
    if (!containers) return;
    const { host, board } = containers;

    const pairs = (window.cx && Array.isArray(window.cx.pairs)) ? window.cx.pairs : [];
    if (!pairs.length) { host.style.display = "none"; board.innerHTML = ""; return; }
    host.style.display = "block";

    const html = pairs.map((pr, i) => {
      const src = key(pr.source);
      const dst = key(pr.target);
      const isTwo = (pr.mode || "one-way").toLowerCase().includes("two");
      const modeLabel = isTwo ? "Two-way" : "One-way";
      const arrow = isTwo ? "↔" : "→";
      const enabled = pr.enabled !== false;
      const srcUrl = ICONS[src] ? `url('${ICONS[src]}')` : "none";
      const dstUrl = ICONS[dst] ? `url('${ICONS[dst]}')` : "none";

      return `
        <div class="pair-card cx-conn brand-${brandKey(src)} dst-${brandKey(dst)}"
             data-id="${pr.id || ""}" data-source="${src}" data-target="${dst}" data-mode="${modeLabel}"
             draggable="true"
             style="--src-logo:${srcUrl}; --dst-logo:${dstUrl};">
          <div class="wm-mask" aria-hidden="true"></div>
          <div class="pair-row">
            <div class="pair-left">
              <span class="ord-badge">${i + 1}</span>
              <span class="pair-pill src">${src}</span>
              <span class="arrow">${arrow}</span>
              <span class="pair-pill dst">${dst}</span>
              <span class="pair-pill mode">${modeLabel}</span>
            </div>
            <div class="pair-actions">
              <label class="icon-btn power ${enabled ? "" : "off"}" title="Enable/disable">
                <input type="checkbox" ${enabled ? "checked" : ""}
                  onchange="window.cxToggleEnable && window.cxToggleEnable('${pr.id}', this.checked, this)"
                  style="position:absolute; inset:0; opacity:0; cursor:pointer">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true">
                  <path d="M12 3v6"></path>
                  <path d="M5.6 7a8 8 0 1 0 12.8 0"></path>
                </svg>
              </label>

              <button class="icon-btn" title="Move left" onclick="window.movePair && window.movePair('${pr.id}','prev')">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M15 18l-6-6 6-6"></path></svg>
              </button>

              <button class="icon-btn" title="Move right" onclick="window.movePair && window.movePair('${pr.id}','next')">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M9 6l6 6-6 6"></path></svg>
              </button>

              <button class="icon-btn" title="Edit" onclick="window.cxPairsEditClick(this)">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true">
                  <path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25z"></path>
                  <path d="M14.06 4.94l3.75 3.75"></path>
                </svg>
              </button>

              <button class="icon-btn danger" title="Delete" onclick="window.deletePairCard('${pr.id}')">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true">
                  <path d="M3 6h18"></path>
                  <path d="M8 6V4h8v2"></path>
                  <path d="M6 6l1 14h10l1-14"></path>
                </svg>
              </button>
            </div>
          </div>
        </div>`;
    }).join("");

    board.innerHTML = html;
    enableReorder(board);
    refreshBadges(board);
  }

  // Re-number badges
  function refreshBadges(board) {
    [...board.querySelectorAll(".pair-card")].forEach((el, i) => {
      const b = el.querySelector(".ord-badge");
      if (b) b.textContent = String(i + 1);
    });
  }

  // DnD reordering with persistence
  function enableReorder(board) {
    let dragging = null;
    const placeholder = document.createElement("div");
    placeholder.className = "drag-placeholder";

    board.querySelectorAll(".pair-card").forEach((el) => {
      el.addEventListener("dragstart", () => {
        dragging = el;
        el.classList.add("dragging");
        placeholder.style.height = `${el.getBoundingClientRect().height}px`;
        board.insertBefore(placeholder, el.nextSibling);
        setTimeout(() => { el.style.display = "none"; }, 0);
      });

      el.addEventListener("dragend", async () => {
        el.style.display = "";
        el.classList.remove("dragging");
        if (placeholder.parentNode) {
          board.insertBefore(el, placeholder);
          placeholder.remove();
        }
        dragging = null;

        const order = [...board.querySelectorAll(".pair-card")].map((n) => n.dataset.id);
        try {
          await fetch("/api/pairs/reorder", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(order),
          });
          if (Array.isArray(window.cx?.pairs)) {
            const map = new Map(window.cx.pairs.map((p) => [String(p.id), p]));
            window.cx.pairs = order.map((id) => map.get(String(id))).filter(Boolean);
          }
        } catch (err) {
          console.warn("reorder failed", err);
        }
        refreshBadges(board);
      });
    });

    board.addEventListener("dragover", (e) => {
      if (!dragging) return;
      e.preventDefault();
      const after = getAfterElement(board, e.clientY);
      if (after == null) board.appendChild(placeholder);
      else board.insertBefore(placeholder, after);
    });

    function getAfterElement(container, y) {
      const els = [...container.querySelectorAll(".pair-card:not(.dragging)")];
      let closest = { offset: Number.NEGATIVE_INFINITY, element: null };
      for (const child of els) {
        const box = child.getBoundingClientRect();
        const offset = y - box.top - box.height / 2;
        if (offset < 0 && offset > closest.offset) closest = { offset, element: child };
      }
      return closest.element;
    }
  }

  // Move one step left/right + persist (no full re-render)
  if (typeof window.movePair !== "function") {
    window.movePair = async function (id, dir) {
      try {
        const list = Array.isArray(window.cx?.pairs) ? window.cx.pairs : [];
        const idx = list.findIndex((p) => String(p.id) === String(id));
        if (idx < 0) return;

        const newIdx = dir === "prev" ? Math.max(0, idx - 1) : Math.min(list.length - 1, idx + 1);
        if (newIdx === idx) return;

        const [item] = list.splice(idx, 1);
        list.splice(newIdx, 0, item);

        // Instant DOM move
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

        // Persist order
        try {
          await fetch("/api/pairs/reorder", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(list.map((p) => p.id)),
          });
        } catch (_) { /* non-fatal */ }
      } catch (e) {
        console.warn("[movePair] failed", e);
      }
    };
  }

  // Render once and on state changes
  async function renderOrEnhance() {
    if (_pairsRenderBusy) return;
    _pairsRenderBusy = true;
    try { await loadPairsIfNeeded(); renderPairsOverlay(); }
    finally { _pairsRenderBusy = false; }
  }

  document.addEventListener("DOMContentLoaded", renderOrEnhance);
  document.addEventListener("cx-state-change", renderOrEnhance);

  // Chain original renderer, keep this overlay in sync
  const _origRender = window.renderConnections;
  window.renderConnections = function () {
    try { if (typeof _origRender === "function") _origRender(); } catch {}
    renderOrEnhance();
  };

  // Expose manual refresh
  window.cxRenderPairsOverlay = renderOrEnhance;
})();
