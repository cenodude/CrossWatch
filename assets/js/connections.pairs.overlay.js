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
  const playlistMappingIds = (v) => Array.isArray(v?.mappings) ? v.mappings.map((x) => String(x || "").trim()).filter(Boolean) : [];
  const hasPlaylistMappings = (v) => truthy(v) && playlistMappingIds(v).length > 0;


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
        if (card) card.classList.toggle("pair-disabled", !on);
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
    try { window.dispatchEvent(new CustomEvent("cx:pairs:changed", { detail: { action: "delete", id } })); } catch {}
    setTimeout(() => refreshBadges(board), 220);
  }
  window.deletePairCard = deletePairCard;

  async function openPlaylistMappingsForPair(pairId, trigger) {
    const id = String(pairId || "").trim();
    if (!id) return;
    try {
      if (typeof window.showTab === "function") await window.showTab("playlists");
      const api = window.Playlists;
      if (api?.openMappingForPair) {
        await api.openMappingForPair(id, trigger || null, { returnToSyncPairs: true });
      }
    } catch (e) {
      console.warn("[pairs.overlay] playlist mappings open failed", e);
    }
  }
  window.cxOpenPlaylistMappingsForPair = openPlaylistMappingsForPair;

  function renderPairsOverlay() {
    const containers = ensureHost(); if (!containers) return;
    const { host, board } = containers;

    _lastHost = host;
    _lastBoard = board;

    const pairs = Array.isArray(window.cx?.pairs) ? window.cx.pairs : [];
    if (!pairs.length) { host.style.display = "none"; board.innerHTML = ""; return; }
    host.style.display = "block";

    const bead = (cls, tip, val) => `<span class="bead ${cls} ${truthy(val) ? "on" : ""}" data-tip="${tip}"></span>`;
    const inst = (v) => (String(v || "default").trim() || "default");
    // TEMP Disabled due to playlists validation
    const showPlaylistMappingButton = false;
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
        <div class="pair-card brand-${brandKey(src)} dst-${brandKey(dst)} ${enabled ? "" : "pair-disabled"}" data-id="${pr.id || ""}" data-source="${src}" data-target="${dst}" data-mode="${modeLabel}" style="--src-solid:${srcTone.solid};--src-rgb:${srcTone.rgb};--dst-solid:${dstTone.solid};--dst-rgb:${dstTone.rgb};--accent:${srcTone.solid};--accent-rgb:${srcTone.rgb}">
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

              ${showPlaylistMappingButton && hasPlaylistMappings(f.playlists) ? `<button type="button" class="icon-btn" data-tip="Manage playlist mappings" onclick="window.cxOpenPlaylistMappingsForPair && window.cxOpenPlaylistMappingsForPair(this.closest('.pair-card')?.dataset?.id, this)" aria-label="Manage playlist mappings">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M4 6h11"></path><path d="M4 12h11"></path><path d="M4 18h7"></path><path d="M17 15l4 3-4 3"></path></svg>
              </button>` : ""}

              <label class="icon-btn power ${enabled ? "" : "off"}" data-tip="Enable / disable" role="switch" aria-checked="${enabled}">
                <input class="sr-only" type="checkbox" name="pair-enabled" ${enabled ? "checked" : ""}
                  onchange="this.closest('.icon-btn.power')?.setAttribute('aria-checked', this.checked); window.cxToggleEnable && window.cxToggleEnable('${pr.id}', this.checked, this)">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M12 3v6"></path><path d="M5.6 7a8 8 0 1 0 12.8 0"></path></svg>
              </label>

              <button type="button" class="icon-btn" data-tip="Move left" onclick="window.movePair && window.movePair('${pr.id}','prev')" aria-label="Move left">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M15 18l-6-6 6-6"></path></svg>
              </button>
              <button type="button" class="icon-btn" data-tip="Move right" onclick="window.movePair && window.movePair('${pr.id}','next')" aria-label="Move right">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M9 6l6 6-6 6"></path></svg>
              </button>
              <button type="button" class="icon-btn" data-tip="Edit" onclick="window.cxPairsEditClick(this)" aria-label="Edit">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25z"></path><path d="M14.06 4.94l3.75 3.75"></path></svg>
              </button>
              <button type="button" class="icon-btn danger" data-tip="Delete" onclick="window.deletePairCard('${pr.id}')" aria-label="Delete">
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
  document.addEventListener("cw-settings-pane-changed", (ev) => {
    if (String(ev?.detail?.pane || "").toLowerCase() === "sync") {
      renderOrEnhance(true);
      scheduleViewportLimit(120);
    }
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
