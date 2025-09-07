// connections.pairs.overlay.js
(function () {
  let _pairsRenderBusy = false;

  const ICONS = { PLEX:"/assets/PLEX.svg", SIMKL:"/assets/SIMKL.svg", TRAKT:"/assets/TRAKT.svg" };
  const key = (s) => String(s || "").trim().toUpperCase();
  const brandKey = (k) => ({ PLEX:"plex", SIMKL:"simkl", TRAKT:"trakt" }[key(k)] || "x");

  function ensureStyles() {
    if (document.getElementById("cx-pairs-style")) return;
    const css = `
/* --- Title styled like "Providers" --- */
#cx-pairs-title{
  margin: 6px 0 2px 0;
  font-family: inherit;
  font-weight: 600;
  font-size: 16px;
  letter-spacing: .02em;
  color: var(--muted);
  opacity: 1;
}

#cx-pairs .cx-conn{ margin-top:8px !important; }
#pairs_list{ display:block; }
.pairs-board{ padding-bottom:12px; position:relative; }

/* ===== Card base (brighter glassy) ===== */
.pair-card{
  position:relative;
  border:none !important;
  border-radius:16px;
  padding:12px 14px 10px;
  background:
    linear-gradient(180deg, rgba(255,255,255,.06), rgba(255,255,255,.02)),
    rgba(20,21,28,.92);
  box-shadow: inset 0 1px 0 rgba(255,255,255,.06), 0 8px 28px rgba(0,0,0,.28);
  overflow:hidden;
  transition: box-shadow .18s ease, transform .12s ease, filter .2s ease, opacity .22s ease;
}
.pair-card:hover{ filter:saturate(1.02) brightness(1.02); }

/* Header */
.pair-head{
  display:flex; align-items:center; justify-content:center;
  gap:12px; margin-bottom:10px; line-height:1; position:relative; z-index:5;
}
/* Same font vibe as provider titles */
.pair-card .pill{
  display:inline-block; padding:6px 12px; border-radius:999px;
  font-family: inherit;
  font-weight: 800;
  font-size:.9rem; letter-spacing:.02em; color:#fff;
  background: rgba(255,255,255,.08);
  border:1px solid rgba(255,255,255,.14);
  box-shadow: inset 0 0 0 1px rgba(255,255,255,.05), 0 3px 10px rgba(0,0,0,.25);
  backdrop-filter: blur(6px);
}
.pair-card .arrow{ color:#d7deee; opacity:.8; margin:0 2px; }

/* Mode chip → short + nowrap */
.pair-card .mode{
  font-family: inherit;
  font-size:.84rem;            /* iets compacter */
  font-weight:800;
  letter-spacing:.02em;
  color:#f0f3ff;
  margin-left:6px;
  padding:4px 10px;            /* compacter */
  border-radius:999px;
  background:rgba(255,255,255,.06);
  border:1px solid rgba(255,255,255,.12);
  box-shadow: inset 0 0 0 1px rgba(255,255,255,.04);
  backdrop-filter: blur(6px);
  white-space: nowrap;         /* nooit afbreken */
}

/* Index badge */
.ord-badge{
  position:absolute; top:6px; left:12px;
  min-width:18px; height:18px; padding:0 6px; border-radius:999px;
  background:linear-gradient(135deg,#7b68ee,#a78bfa);
  color:#fff; font-size:11px; font-weight:850; line-height:18px; text-align:center;
  box-shadow:0 0 10px rgba(124,92,255,.45);
  z-index:6;
}

/* Actions */
.actions3{ display:flex; justify-content:space-between; align-items:center; gap:12px; margin-top:10px; position:relative; z-index:5; }
.act-group{
  display:flex; gap:10px; padding:8px 10px; border-radius:14px;
  background:rgba(255,255,255,.06); border:1px solid rgba(255,255,255,.14);
  box-shadow: inset 0 0 0 1px rgba(255,255,255,.05), 0 6px 18px rgba(0,0,0,.22);
  backdrop-filter: blur(6px);
}
.icon-only{
  position:relative; display:inline-flex; align-items:center; justify-content:center;
  width:34px; height:34px; border:0; background:rgba(255,255,255,.08); border-radius:12px;
  cursor:pointer; color:#e5e8f2;
  transition:transform .12s, background .12s, box-shadow .12s, color .12s;
}
.icon-only .ico{ width:18px; height:18px; fill:none; stroke:currentColor; stroke-width:2; stroke-linecap:round; stroke-linejoin:round; }
.icon-only:hover{ background:rgba(255,255,255,.14); transform:translateY(-1px) scale(1.05); box-shadow:0 10px 24px rgba(0,0,0,.26); }
.icon-only:active{ transform:none; }
.icon-only input{ position:absolute; inset:0; opacity:0; margin:0; cursor:pointer; }
.icon-only input:checked + .ico{
  color:#39ff14;
  filter: drop-shadow(0 0 6px rgba(57,255,20,.7)) drop-shadow(0 0 14px rgba(57,255,20,.45));
  transform: scale(1.08);
}
.icon-only.danger:hover{ color:#ff5a5e; }

@media (prefers-reduced-motion: reduce){ .icon-only{ transition:none !important; } }

.cx-conn{ position:relative; overflow:hidden; }
.cx-conn::before,
.cx-conn::after{
  content:""; position:absolute; top:0; bottom:0; pointer-events:none; z-index:0;
  opacity:.2; mix-blend-mode:screen;
  filter: saturate(.95) brightness(1) contrast(1);
  background-repeat:no-repeat; background-position:center;
  background-size:170% auto;
}
.cx-conn::before{
  left:0; width:52%; background-image:var(--src-logo);
  -webkit-mask-image: linear-gradient(90deg, #fff 0 44%, rgba(255,255,255,0) 66%);
          mask-image: linear-gradient(90deg, #fff 0 44%, rgba(255,255,255,0) 66%);
}
.cx-conn::after{
  right:0; width:52%; background-image:var(--dst-logo);
  -webkit-mask-image: linear-gradient(-90deg, #fff 0 44%, rgba(255,255,255,0) 66%);
          mask-image: linear-gradient(-90deg, #fff 0 44%, rgba(255,255,255,0) 66%);
}
.cx-conn .fusion{
  position:absolute; inset:0; pointer-events:none; z-index:2; border-radius:16px;
  background:
    linear-gradient(90deg, transparent 46%, rgba(255,255,255,.08) 50%, transparent 54%),
    radial-gradient(36% 120% at 46% 50%, rgba(255,255,255,.08), transparent 60%),
    radial-gradient(36% 120% at 54% 50%, rgba(255,255,255,.08), transparent 60%);
  mix-blend-mode: screen;
  filter: blur(12px);
  opacity:.5;
}

.cx-conn.brand-plex::before  { box-shadow: inset -260px 0 300px rgba(229,160,13,.28), inset 0 0 120px rgba(229,160,13,.16); }
.cx-conn.brand-simkl::before { box-shadow: inset -260px 0 300px rgba(0,183,235,.3),  inset 0 0 120px rgba(0,183,235,.16); }
.cx-conn.brand-trakt::before { box-shadow: inset -260px 0 300px rgba(237,28,36,.28),  inset 0 0 120px rgba(237,28,36,.16); }
.cx-conn.dst-plex::after  { box-shadow: inset 260px 0 300px rgba(229,160,13,.28), inset 0 0 120px rgba(229,160,13,.16); }
.cx-conn.dst-simkl::after { box-shadow: inset 260px 0 300px rgba(0,183,235,.3),  inset 0 0 120px rgba(0,183,235,.16); }
.cx-conn.dst-trakt::after { box-shadow: inset 260px 0 300px rgba(237,28,36,.28),  inset 0 0 120px rgba(237,28,36,.16); }

.cx-conn .wm-dim{
  position:absolute; inset:0; pointer-events:none; z-index:1; border-radius:16px;
  background:
    radial-gradient(120% 140% at 10% 20%, rgba(0,0,0,.18), transparent 60%),
    radial-gradient(120% 140% at 90% 20%, rgba(0,0,0,.18), transparent 60%);
  opacity:.85;
}

.cx-conn > .split-border{ display:none !important; }

/* DnD cards */
.pair-card[draggable="true"]{ cursor:grab; }
.pair-card.dragging{
  opacity:.82;
  transform: scale(.985) rotate(.6deg);
  animation: cx-wiggle .35s ease-in-out infinite;
  z-index: 10;
}
.drag-placeholder{
  border-radius:16px;
  outline:2px dashed rgba(255,255,255,.24);
  outline-offset: -2px;
  min-height:68px;
  margin:6px 0;
  background:rgba(255,255,255,.04);
}
@keyframes cx-wiggle{
  0%{ transform: scale(.985) rotate(-.6deg); }
  50%{ transform: scale(.985) rotate(.6deg); }
  100%{ transform: scale(.985) rotate(-.6deg); }
}

/* Delete animation: fade + slide up + collapse */
.pair-card.removing{
  opacity:0;
  transform: translateY(-10px) scale(.98);
  height:0 !important;
  margin:0 !important;
  padding:0 !important;
  overflow:hidden !important;
  transition: opacity .25s ease, transform .25s ease, height .25s ease, margin .25s ease, padding .25s ease;
}
`;
    const s = document.createElement("style");
    s.id = "cx-pairs-style";
    s.textContent = css;
    document.head.appendChild(s);
  }

  function ensureTitle() {
    const host = document.getElementById("pairs_list");
    if (!host) return;
    if (document.getElementById("cx-pairs-title")) return;
    const title = document.createElement("div");
    title.id = "cx-pairs-title";
    title.textContent = "Configured Connections";
    host.parentElement?.insertBefore(title, host);
  }

  function ensureHost() {
    const host = document.getElementById("pairs_list");
    if (!host) return null;
    ensureTitle();
    let board = host.querySelector(".pairs-board");
    if (!board) {
      board = document.createElement("div");
      board.className = "pairs-board";
      host.innerHTML = "";
      host.appendChild(board);
    }
    return { host, board };
  }

  async function loadPairsIfNeeded() {
    if (Array.isArray(window.cx?.pairs) && window.cx.pairs.length) return;
    if (typeof window.loadPairs === "function") {
      try { await window.loadPairs(); return; } catch {}
    }
    try {
      const arr = await fetch("/api/pairs", { cache:"no-store" }).then(r => r.json());
      window.cx = window.cx || {};
      window.cx.pairs = Array.isArray(arr) ? arr : [];
    } catch (e) {
      console.warn("[pairs.overlay] fetch /api/pairs failed", e);
    }
  }

  window.cxPairsEditClick = function (btn) {
    try {
      const id = btn.closest('.pair-card')?.dataset?.id;
      if (!id) return;
      if (typeof window.cxEditPair === 'function') return window.cxEditPair(id);
      const pairs = (window.cx && Array.isArray(window.cx.pairs)) ? window.cx.pairs : [];
      const pair = pairs.find(p => String(p.id) === String(id));
      if (pair) {
        if (typeof window.openPairModal === 'function') return window.openPairModal(pair);
        if (typeof window.cxOpenModalFor === 'function') return window.cxOpenModalFor(pair);
      }
      alert('Edit is not available.');
    } catch (e) { console.warn('[cxPairsEditClick] failed', e); }
  };

  if (typeof window.cxToggleEnable !== "function") {
    window.cxToggleEnable = async function (id, on) {
      try {
        const list = Array.isArray(window.cx?.pairs) ? window.cx.pairs : [];
        const it = list.find(p => String(p.id) === String(id));
        if (!it) return;
        it.enabled = !!on;
      } catch (e) { console.warn("toggle failed", e); }
    };
  }

  async function deletePairCard(id) {
    const board = document.querySelector("#pairs_list .pairs-board");
    const el = board?.querySelector(`.pair-card[data-id="${id}"]`);
    if (!el) return;
    el.classList.add("removing");
    setTimeout(() => el.remove(), 280);
    try { await fetch(`/api/pairs/${id}`, { method:"DELETE" }); } catch(e){ console.warn("delete api failed", e); }
    if (Array.isArray(window.cx?.pairs)) {
      window.cx.pairs = window.cx.pairs.filter(p => String(p.id) !== String(id));
    }
    setTimeout(() => { refreshBadges(board); }, 300);
  }
  window.deletePairCard = deletePairCard;

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
      const rawMode = (pr.mode || "one-way").toLowerCase();
      const modeShort = rawMode.includes("two") ? "2-WAY" : "1-WAY";
      const enabled = pr.enabled === true;
      const srcUrl = ICONS[src] ? `url('${ICONS[src]}')` : "none";
      const dstUrl = ICONS[dst] ? `url('${ICONS[dst]}')` : "none";
      return `
        <div class="pair-card cx-conn brand-${brandKey(src)} dst-${brandKey(dst)}"
             data-id="${pr.id || ""}" data-source="${src}" data-target="${dst}" data-mode="${modeShort}"
             draggable="true"
             style="--src-logo:${srcUrl}; --dst-logo:${dstUrl};">
          <div class="wm-dim"></div>
          <div class="fusion"></div>
          <span class="ord-badge">${i + 1}</span>
          <div class="pair-head">
            <span class="pill src">${src}</span>
            <span class="arrow">↔</span>
            <span class="pill dst">${dst}</span>
            <span class="mode">${modeShort}</span>
          </div>
          <div class="actions actions3">
            <div class="act-group">
              <label class="icon-only" title="Enable/disable">
                <input type="checkbox" ${enabled ? "checked" : ""}
                       onchange="window.cxToggleEnable && window.cxToggleEnable('${pr.id}', this.checked, this)">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true">
                  <path d="M12 3v6"></path>
                  <path d="M5.6 7a8 8 0 1 0 12.8 0"></path>
                </svg>
              </label>
            </div>
            <div class="act-group">
              <button class="icon-only" title="Edit" onclick="window.cxPairsEditClick(this)">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true">
                  <path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25z"></path>
                  <path d="M14.06 4.94l3.75 3.75"></path>
                </svg>
              </button>
              <button class="icon-only danger" title="Delete" onclick="window.deletePairCard('${pr.id}')">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true">
                  <path d="M3 6h18"></path>
                  <path d="M8 6V4h8v2"></path>
                  <path d="M6 6l1 14h10l1-14"></path>
                </svg>
              </button>
            </div>
            <div class="act-group">
              <button class="icon-only" title="Move first" onclick="window.movePair && window.movePair('${pr.id}','first')">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M14 7l-5 5 5 5"></path></svg>
              </button>
              <button class="icon-only" title="Move last" onclick="window.movePair && window.movePair('${pr.id}','last')">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M10 7l5 5-5 5"></path></svg>
              </button>
            </div>
          </div>
        </div>`;
    }).join("");

    board.innerHTML = html;
    enableReorder(board);
    refreshBadges(board);
  }

  function refreshBadges(board) {
    [...board.querySelectorAll(".pair-card")].forEach((el, i) => {
      const b = el.querySelector(".ord-badge");
      if (b) b.textContent = String(i + 1);
    });
  }

  function enableReorder(board) {
    let dragging = null;
    const placeholder = document.createElement("div");
    placeholder.className = "drag-placeholder";

    board.querySelectorAll(".pair-card").forEach(el => {
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
        const order = [...board.querySelectorAll(".pair-card")].map(n => n.dataset.id);
        try {
          await fetch("/api/pairs/reorder", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(order)
          });
          if (Array.isArray(window.cx?.pairs)) {
            const map = new Map(window.cx.pairs.map(p => [String(p.id), p]));
            window.cx.pairs = order.map(id => map.get(String(id))).filter(Boolean);
          }
        } catch (err) { console.warn("reorder failed", err); }
        refreshBadges(board);
      });
    });

    board.addEventListener("dragover", e => {
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

  if (typeof window.movePair !== "function") {
    window.movePair = async function (id, where) {
      try {
        const list = Array.isArray(window.cx?.pairs) ? window.cx.pairs : [];
        const idx = list.findIndex(p => String(p.id) === String(id));
        if (idx < 0) return;
        const item = list.splice(idx, 1)[0];
        if (where === "first") list.unshift(item); else list.push(item);
        try {
          await fetch("/api/pairs/reorder", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(list.map(p => p.id))
          });
        } catch (_) {}
        renderPairsOverlay();
      } catch (e) { console.warn("movePair failed", e); }
    };
  }

  async function renderOrEnhance() {
    if (_pairsRenderBusy) return;
    _pairsRenderBusy = true;
    try { await loadPairsIfNeeded(); renderPairsOverlay(); }
    finally { _pairsRenderBusy = false; }
  }

  document.addEventListener("DOMContentLoaded", renderOrEnhance);
  document.addEventListener("cx-state-change", renderOrEnhance);

  const _origRender = window.renderConnections;
  window.renderConnections = function () {
    try { if (typeof _origRender === "function") _origRender(); } catch {}
    renderOrEnhance();
  };

  window.cxRenderPairsOverlay = renderOrEnhance;
})();
