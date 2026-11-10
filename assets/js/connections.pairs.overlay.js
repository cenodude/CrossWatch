// connections.pairs.overlay.js - Pairs board UI

(function () {
  let _renderBusy = false;
  const _renderedPairs = new WeakMap();
  let _pairDrag = null;
  let _reorderBusy = false;
  const _resettingPairs = new Set();
  const _pairMenuHosts = new WeakSet();

  const key = (s) => String(s || "").trim().toUpperCase();
  const brandKey = (k) => key(k).toLowerCase().replace(/[^a-z0-9]/g, "") || "x";
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
  const playlistBlock = (pair) => {
    const block = pair?.features?.playlists;
    return block && typeof block === "object" ? block : {};
  };
  const isPlaylistManagedPair = (pair) => String(playlistBlock(pair).managed_by || "").trim().toLowerCase() === "playlists";


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
    if (host.dataset.tooltipBound) return;
    host.dataset.tooltipBound = "1";
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

    host.addEventListener("focusin", event => {
      const el = event.target.closest?.("[data-tip]");
      if (!el) return;
      const rect = el.getBoundingClientRect();
      show(el, { clientX: rect.left, clientY: rect.bottom });
    });
    host.addEventListener("focusout", hide);
    host.addEventListener("keydown", event => { if (event.key === "Escape") hide(); });
    host.addEventListener("pointermove", move, { passive: true });
    host.addEventListener("pointerout", hide, { passive: true });
    host.addEventListener("pointerdown", hide, { passive: true });
    window.addEventListener("scroll", hide, { passive: true });
  }

  window.cxPairsEditClick = function (btn) {
    try {
      const id = btn.closest(".pair-card")?.dataset?.id; if (!id) return;
      const pairs = Array.isArray(window.cx?.pairs) ? window.cx.pairs : [];
      const pair = pairs.find((p) => String(p.id) === String(id));
      if (isPlaylistManagedPair(pair)) return openPlaylistMappingsForPair(id, btn);
      if (typeof window.cxEditPair === "function") return window.cxEditPair(id);
      if (pair) {
        if (typeof window.openPairModal === "function") return window.openPairModal(pair);
        if (typeof window.cxOpenModalFor === "function") return window.cxOpenModalFor(pair);
      }
      alert("Edit is not available.");
    } catch (e) { console.warn("[cxPairsEditClick] failed", e); }
  };

  if (typeof window.cxToggleEnable !== "function") {
    window.cxToggleEnable = async function (id, on, inputEl) {
      const card = (inputEl && inputEl.closest(".pair-card")) || document.querySelector(`#pairs_list .pair-card[data-id="${id}"]`);
      const btn = card?.querySelector(".icon-btn.power");
      const list = Array.isArray(window.cx?.pairs) ? window.cx.pairs : [];
      const it = list.find((p) => String(p.id) === String(id));
      const prev = it ? it.enabled !== false : !on;
      try {
        if (btn) btn.classList.toggle("off", !on);
        if (btn) btn.setAttribute("aria-checked", on ? "true" : "false");
        if (card) card.classList.toggle("pair-disabled", !on);
        if (inputEl) inputEl.disabled = true;
        if (isPlaylistManagedPair(it)) {
          await setPlaylistManagedPairEnabled(id, on, it);
        } else {
          await fetch(`/api/pairs/${id}`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ enabled: !!on })
          }).then((r) => r.ok ? r.json() : Promise.reject(new Error("pair update failed")));
        }
        if (it) it.enabled = !!on;
        try { document.dispatchEvent(new Event("cx-state-change")); } catch {}
      } catch (e) {
        if (btn) btn.classList.toggle("off", !prev);
        if (btn) btn.setAttribute("aria-checked", prev ? "true" : "false");
        if (card) card.classList.toggle("pair-disabled", !prev);
        if (inputEl) inputEl.checked = !!prev;
        console.warn("[cxToggleEnable] failed", e);
      } finally {
        if (inputEl) inputEl.disabled = false;
      }
    };
  }

  async function playlistMappingsForPair(pairId, pair) {
    const id = String(pairId || "").trim();
    if (!id) return [];
    try {
      const data = await fetch(`/api/playlists/pairs/${encodeURIComponent(id)}/mappings`, { cache: "no-store" }).then((r) => r.ok ? r.json() : null);
      if (Array.isArray(data?.mappings) && data.mappings.length) return data.mappings;
    } catch {}
    const ids = new Set(playlistMappingIds(playlistBlock(pair)));
    if (!ids.size) return [];
    try {
      const data = await fetch("/api/playlists/mappings", { cache: "no-store" }).then((r) => r.ok ? r.json() : null);
      const mappings = Array.isArray(data?.mappings) ? data.mappings : [];
      return mappings.filter((m) => ids.has(String(m.id || "")));
    } catch {
      return [];
    }
  }

  function playlistMappingPayload(mapping, enabled) {
    return {
      id: mapping.id,
      name: mapping.name,
      source_endpoint: mapping.source_endpoint,
      target_endpoints: mapping.target_endpoints || [],
      ruleset_id: mapping.ruleset_id || "",
      membership: mapping.membership || "managed_only",
      order: mapping.order || "ignore",
      enabled: !!enabled,
    };
  }

  async function setPlaylistManagedPairEnabled(pairId, enabled, pair) {
    const mappings = await playlistMappingsForPair(pairId, pair);
    if (!mappings.length) throw new Error("playlist mapping not found");
    await Promise.all(mappings.map((mapping) => fetch("/api/playlists/mappings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(playlistMappingPayload(mapping, enabled)),
    }).then((r) => r.ok ? r.json() : Promise.reject(new Error("playlist mapping update failed")))));
  }

  async function deletePlaylistManagedPair(pairId, pair) {
    const mappings = await playlistMappingsForPair(pairId, pair);
    if (!mappings.length) {
      await openPlaylistMappingsForPair(pairId, null);
      return false;
    }
    await Promise.all(mappings.map((mapping) => fetch(`/api/playlists/mappings/${encodeURIComponent(mapping.id)}`, {
      method: "DELETE",
    }).then((r) => r.ok ? r.json() : Promise.reject(new Error("playlist mapping delete failed")))));
    return true;
  }

  async function deletePairCard(id) {
    const pairs = Array.isArray(window.cx?.pairs) ? window.cx.pairs : [];
    const pair = pairs.find((p) => String(p.id) === String(id));
    if (isPlaylistManagedPair(pair)) {
      try {
        const deleted = await deletePlaylistManagedPair(id, pair);
        if (!deleted) return;
      } catch (e) {
        console.warn("delete playlist mapping failed", e);
        await openPlaylistMappingsForPair(id, null);
        return;
      }
      if (Array.isArray(window.cx?.pairs)) window.cx.pairs = window.cx.pairs.filter((p) => String(p.id) !== String(id));
      try { window.dispatchEvent(new CustomEvent("cx:pairs:changed", { detail: { action: "delete", id } })); } catch {}
      return;
    }
    const board = document.querySelector("#pairs_list .pairs-board");
    const el = board?.querySelector(`.pair-card[data-id="${id}"]`); if (!el) return;
    try {
      const res = await fetch(`/api/pairs/${id}`, { method: "DELETE" });
      if (res.status === 409) window.alert("A sync or database maintenance task is running. Wait for it to finish, then delete the pair again.");
      let data = null;
      try { data = await res.json(); } catch {}
      if (!res.ok || data?.ok === false || Number(data?.deleted || 0) < 1) throw new Error(data?.error || "pair delete failed");
      if (Number(data?.inventory_removed || 0) > 0) {
        try {
          localStorage.removeItem("cw.dashboardWidgets.data.v1");
          localStorage.removeItem("cw.profileWidgets.data.v1");
        } catch {}
        try { window.CW?.DashboardWidgets?.clear?.(); } catch {}
        try { window.CW?.DashboardWidgets?.refresh?.({ force: true, forceConfig: true, preserve: false }); } catch {}
      }
    } catch (e) {
      console.warn("delete api failed", e);
      el.classList.remove("removing");
      return;
    }
    el.classList.add("removing"); setTimeout(() => el.remove(), 200);
    if (Array.isArray(window.cx?.pairs)) window.cx.pairs = window.cx.pairs.filter((p) => String(p.id) !== String(id));
    try { window.dispatchEvent(new CustomEvent("cx:pairs:changed", { detail: { action: "delete", id } })); } catch {}
    setTimeout(() => refreshBadges(board), 220);
  }
  window.deletePairCard = deletePairCard;

  const PAIR_DELETE_ICON = '<path d="M3 6h18"></path><path d="M8 6V4h8v2"></path><path d="M6 6l1 14h10l1-14"></path>';
  const PAIR_DELETE_WARN_ICON = '<path d="M12 3 2 20h20L12 3z"></path><path d="M12 10v4"></path><path d="M12 17h.01"></path>';
  let _pairDelBtn = null;
  let _pairDelTimer = 0;

  function resetPairDelete() {
    if (_pairDelTimer) clearTimeout(_pairDelTimer);
    _pairDelTimer = 0;
    const btn = _pairDelBtn;
    _pairDelBtn = null;
    if (btn && btn.isConnected) {
      btn.classList.remove("is-confirming");
      btn.setAttribute("data-tip", "Delete");
      const ico = btn.querySelector("svg.ico");
      if (ico) ico.innerHTML = PAIR_DELETE_ICON;
    }
  }

  function pairDeleteClick(btn, id) {
    if (!btn) return;
    if (_pairDelBtn !== btn) {
      resetPairDelete();
      _pairDelBtn = btn;
      btn.classList.add("is-confirming");
      btn.setAttribute("data-tip", "Click again to delete");
      const ico = btn.querySelector("svg.ico");
      if (ico) ico.innerHTML = PAIR_DELETE_WARN_ICON;
      _pairDelTimer = setTimeout(resetPairDelete, 4200);
      return;
    }
    resetPairDelete();
    deletePairCard(id);
  }
  window.cxPairsDeleteClick = pairDeleteClick;

  function pairResetFeatures(pair) {
    return ["watchlist", "ratings", "history", "progress", "playlists", "collection"].filter(name => truthy(pair.features?.[name]));
  }

  async function resetPairCard(btn) {
    const id = btn.dataset.pairReset;
    const pair = window.cx?.pairs?.find(pair => String(pair.id) === id);
    if (!pair || _resettingPairs.has(id) || window.CW?.AuthState?.read?.().isManaged) return;
    const features = pairResetFeatures(pair);
    if (!features.length) return;
    const endpoint = side => `${providerLabel(pair[side])} (${providerMeta()?.instanceLabel?.(pair[side], pair[`${side}_instance`] || "default") || pair[`${side}_instance`] || "Default"})`;
    const label = `${endpoint("source")} → ${endpoint("target")}`;
    const message = `Give ${label} a fresh start?\n\nThis resets the sync memory for ${features.join(", ")} and clears retry and blocked items for this pair. Your pair settings stay as they are.\n\nThe next sync reads fresh provider data. Recent deletions won't carry across, and a two-way pair can add missing items back. Other pairs keep their sync state.\n\nReady to reset this pair?`;
    if (!window.confirm(message)) return;
    _resettingPairs.add(id);
    btn.disabled = true;
    btn.setAttribute("aria-busy", "true");
    try {
      const response = await fetch("/api/maintenance/clear-pair-state", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ pair_id: id, features, clear_retry: true }),
      });
      if (response.status === 409) throw new Error("A sync or database maintenance task is running. Wait for it to finish, then reset the pair again.");
      if (response.status === 403) throw new Error("You don't have permission to reset this pair.");
      const data = await response.json();
      if (!response.ok || data?.ok !== true) throw new Error("The reset couldn't finish. Please try again.");
      window.alert("Pair reset! Its next sync will start fresh.");
    } catch (error) {
      window.alert(error.message || "The reset couldn't finish. Please try again.");
    } finally {
      _resettingPairs.delete(id);
      document.querySelectorAll("[data-pair-reset]").forEach(button => {
        if (button.dataset.pairReset !== id) return;
        const current = window.cx?.pairs?.find(pair => String(pair.id) === id);
        button.disabled = !current || !pairResetFeatures(current).length || !!window.CW?.AuthState?.read?.().isManaged;
        button.removeAttribute("aria-busy");
      });
    }
  }

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

  const PERSON_ICON = '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="8" r="4"></circle><path d="M4 21a8 8 0 0 1 16 0"></path></svg>';
  const PEOPLE_ICON = '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="9" cy="8" r="3.5"></circle><path d="M2.5 20a6.5 6.5 0 0 1 13 0"></path><path d="M16 4.5a3.5 3.5 0 0 1 0 7"></path><path d="M18.5 14a6 6 0 0 1 3 6"></path></svg>';
  let _identityLabels = {};
  let _identityLabelsAt = 0;

  function pairProfileLabel(pr) {
    return String(pr?.profile_label || pr?.profile_id || "").trim();
  }

  function identityName(provider, instance, key) {
    const labels = _identityLabels[String(provider || "").toUpperCase()] || {};
    return labels[`${String(instance || "default").trim() || "default"}~${key}`] || key;
  }

  function pairModeTip(pr, modeLabel) {
    const lines = [`${modeLabel}: ${providerLabel(pr.source)} ${modeLabel === "Two-way" ? "and" : "to"} ${providerLabel(pr.target)}`];
    const profile = pairProfileLabel(pr);
    if (profile) lines.push(`Belongs to profile: ${profile}`);
    if (pr?.match_users) {
      const links = Object.entries(pr.user_matches && typeof pr.user_matches === "object" ? pr.user_matches : {});
      if (!links.length) lines.push("Identities on, but nobody linked yet");
      else {
        lines.push(`Syncs ${links.length} ${links.length === 1 ? "person" : "people"}, each with their own history:`);
        links.forEach(([src, dst]) => lines.push(`  ${identityName(pr.source, pr.source_instance, src)} → ${identityName(pr.target, pr.target_instance, dst)}`));
      }
    }
    return lines.join("\n");
  }

  function pairWhoBadge(pr) {
    const icons = (pairProfileLabel(pr) ? PERSON_ICON : "") + (pr?.match_users ? PEOPLE_ICON : "");
    return icons ? `<span class="pair-people" aria-hidden="true">${icons}</span>` : "";
  }

  function refreshIdentityTips(board, pairs) {
    if (!pairs.some((pr) => pr?.match_users) || Date.now() - _identityLabelsAt < 30000) return;
    _identityLabelsAt = Date.now();
    fetch("/api/sync-user-labels", { cache: "no-store", credentials: "same-origin" })
      .then((r) => (r.ok ? r.json() : {}))
      .then((data) => {
        _identityLabels = data && typeof data === "object" ? data : {};
        board.querySelectorAll(".pair-card[data-id]").forEach((el) => {
          const pr = pairs.find((p) => String(p.id) === el.dataset.id);
          if (pr) el.querySelectorAll("[data-pair-who]").forEach(indicator => {
            const tip = pairModeTip(pr, el.dataset.mode || "");
            indicator.setAttribute("data-tip", tip);
            if (indicator.getAttribute("role") === "img") indicator.setAttribute("aria-label", tip);
          });
        });
      })
      .catch(() => {});
  }

  function renderPairsOverlay() {
    if (_pairDrag || _reorderBusy) return;
    const containers = ensureHost(); if (!containers) return;
    const { host, board } = containers;


    const pairs = Array.isArray(window.cx?.pairs) ? window.cx.pairs : [];
    if (!pairs.length) { host.style.display = "none"; board.innerHTML = ""; _renderedPairs.delete(board); return; }
    host.style.display = "block";

    const bead = (cls, tip, val) => `<span class="bead ${cls} ${truthy(val) ? "on" : ""}" data-tip="${tip}"></span>`;
    const inst = (v) => (String(v || "default").trim() || "default");
    const pill = (provider, instance, role) => {
      const name = providerLabel(provider), logo = providerLogo(provider);
      const detail = providerMeta()?.instanceLabel?.(provider, instance) || (instance === "default" ? "Default instance" : instance);
      const named = detail !== "Default instance";
      const tip = `${role === "src" ? "Source" : "Target"}: ${name}${named ? ` (${detail})` : ""}`;
      return `<span class="pair-pill ${role}" data-tip="${esc(tip)}"><span class="pair-pill-text"><span>${esc(name)}</span>${named ? `<small class="pair-instance-name">${esc(detail)}</small>` : ""}</span><span class="prov-watermark" aria-hidden="true" style="--wm:url('${esc(logo)}')"></span></span>`;
    };

    const renderPairCard = (pr, displayIndex) => {
      const src = key(pr.source);
      const dst = key(pr.target);
      const srcInst = inst(pr.source_instance);
      const dstInst = inst(pr.target_instance);
      const isTwo = (pr.mode || "one-way").toLowerCase().includes("two");
      const modeLabel = isTwo ? "Two-way" : "One-way";
      const directionPath = isTwo ? "M7 7h26m-4-4 4 4-4 4M33 17H7m4-4-4 4 4 4" : "M7 12h26m-5-5 5 5-5 5";
      const directionTip = esc(pairModeTip(pr, modeLabel));
      const people = pairWhoBadge(pr);
      const enabled = pr.enabled !== false;
      const f = pr.features || {};
      const srcTone = brandTone(src);
      const dstTone = brandTone(dst);

      return `
        <div class="pair-card brand-${brandKey(src)} dst-${brandKey(dst)} ${enabled ? "" : "pair-disabled"}" data-id="${pr.id || ""}" data-source="${src}" data-target="${dst}" data-mode="${modeLabel}" style="--src-solid:${srcTone.solid};--src-rgb:${srcTone.rgb};--dst-solid:${dstTone.solid};--dst-rgb:${dstTone.rgb};--accent:${srcTone.solid};--accent-rgb:${srcTone.rgb}">
          <div class="pair-row">
            <div class="pair-left">
              <button type="button" class="ord-badge cw-page-pair-handle" data-tip="Drag to reorder · or use arrow keys" aria-label="Reorder pair ${displayIndex}: ${esc(providerLabel(src))} to ${esc(providerLabel(dst))}. Use Up or Down arrow keys."><span class="pair-order">${displayIndex}</span></button>
              ${pill(src, srcInst, "src")}
              <span class="cw-page-pair-direction" role="img" tabindex="0" aria-label="${directionTip}" data-pair-who data-tip="${directionTip}"><svg viewBox="0 0 40 24" aria-hidden="true"><path d="${directionPath}"/></svg></span>
              ${pill(dst, dstInst, "dst")}
            </div>
            <div class="pair-actions">
              <div class="pair-feature-summary">
              ${people ? `<span data-pair-who data-tip="${directionTip}">${people}</span>` : ""}
              <div class="feat-beads cw-page-pair-features" role="group" aria-label="Enabled features">
                ${bead("wl", "Watchlist", f.watchlist)}
                ${bead("rt", "Ratings", f.ratings)}
                ${bead("hi", "History", f.history)}
                ${bead("pr", "Progress", f.progress)}
                ${bead("pl", "Playlists", f.playlists)}
                ${bead("co", "Collections", f.collection)}
              </div>
              </div>

              <div class="pair-action-buttons cw-page-pair-actions" role="group" aria-label="Sync pair actions">
              <button type="button" class="icon-btn cw-page-pair-run" data-tip="Interactive sync" aria-label="Interactive sync" ${enabled ? "" : "disabled"} data-interactive-pair="${esc(pr.id)}">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M8 5l11 7-11 7V5z"></path></svg><span class="pair-action-label">Sync</span>
              </button>

              <button type="button" class="icon-btn" data-tip="Latest logs" aria-label="Latest logs" data-pair-logs="${esc(pr.id)}">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><rect x="3" y="4" width="18" height="16" rx="2"></rect><path d="m7 9 3 3-3 3M13 15h4"></path></svg><span class="pair-action-label">Logs</span>
              </button>

              <label class="icon-btn power ${enabled ? "" : "off"}" data-tip="Enable / disable" role="switch" aria-checked="${enabled}">
                <input class="sr-only" type="checkbox" name="pair-enabled" ${enabled ? "checked" : ""}
                  onchange="this.closest('.icon-btn.power')?.setAttribute('aria-checked', this.checked); window.cxToggleEnable && window.cxToggleEnable('${pr.id}', this.checked, this)">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M12 3v6"></path><path d="M5.6 7a8 8 0 1 0 12.8 0"></path></svg><span class="pair-action-label pair-power-label"><span class="pair-power-on">Enabled</span><span class="pair-power-off">Disabled</span></span>
              </label>

              <button type="button" class="icon-btn" data-tip="Reset pair" data-pair-reset="${esc(pr.id)}" aria-label="Reset pair" ${_resettingPairs.has(String(pr.id)) || window.CW?.AuthState?.read?.().isManaged || !pairResetFeatures(pr).length ? "disabled" : ""}>
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M3 11a9 9 0 1 1 2.6 7M3 4v7h7"></path></svg><span class="pair-action-label">Reset</span>
              </button>
              <button type="button" class="icon-btn" data-tip="Edit" onclick="window.cxPairsEditClick(this)" aria-label="Edit">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25z"></path><path d="M14.06 4.94l3.75 3.75"></path></svg><span class="pair-action-label">Edit</span>
              </button>
              <button type="button" class="icon-btn danger cw-danger-confirm" data-tip="Delete" onclick="window.cxPairsDeleteClick(this, '${pr.id}')" aria-label="Delete">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true">${PAIR_DELETE_ICON}</svg><span class="pair-action-label">Delete</span>
              </button>
              <button type="button" class="icon-btn pair-more" popovertarget="pair-overflow-${displayIndex}" aria-label="More pair actions" aria-expanded="false">
                <svg viewBox="0 0 24 24" class="ico" aria-hidden="true"><circle cx="5" cy="12" r="1"></circle><circle cx="12" cy="12" r="1"></circle><circle cx="19" cy="12" r="1"></circle></svg><span class="pair-action-label">More</span>
              </button>
              </div>
              <div id="pair-overflow-${displayIndex}" class="pair-overflow cw-page-pair-actions" popover="auto" role="group" aria-label="More pair actions"></div>
            </div>
          </div>
        </div>`;
    };

    const html = pairs.map((pair, index) => renderPairCard(pair, index + 1)).join("");
    if (_renderedPairs.get(board) !== html) {
      board.innerHTML = html;
      _renderedPairs.set(board, html);
    }
    refreshIdentityTips(board, pairs);
    installTooltip(host);
    installPairReorder(host, board);
    installPairMenus(host);
    refreshBadges(board);
  }

  function syncPairMenus(host) {
    const style = getComputedStyle(host);
    const compact = host.clientWidth - parseFloat(style.paddingLeft || 0) - parseFloat(style.paddingRight || 0) <= 380;
    for (const card of host.querySelectorAll(".pair-card")) {
      const actions = card.querySelector(".pair-action-buttons");
      const more = actions.querySelector(".pair-more");
      const menu = card.querySelector(".pair-overflow");
      const secondary = [...card.querySelectorAll('[data-pair-logs],[data-pair-reset],button[aria-label="Edit"],button[aria-label="Delete"]')];
      if (compact) secondary.filter(button => button.parentElement !== menu).forEach(button => menu.appendChild(button));
      else {
        if (menu.matches(":popover-open")) menu.hidePopover();
        for (const button of secondary.filter(button => button.parentElement !== actions)) actions.insertBefore(button, button.hasAttribute("data-pair-logs") ? actions.querySelector(".power") : more);
      }
    }
  }

  function positionPairMenu(menu) {
    const anchor = menu.closest(".pair-card").querySelector(".pair-more").getBoundingClientRect();
    const bounds = menu.getBoundingClientRect();
    if (anchor.bottom < 0 || anchor.top > innerHeight) { menu.hidePopover(); return; }
    menu.style.left = `${Math.max(12, Math.min(anchor.right - bounds.width, innerWidth - bounds.width - 12))}px`;
    menu.style.top = `${Math.max(12, Math.min(anchor.bottom + 8 + bounds.height <= innerHeight - 12 ? anchor.bottom + 8 : anchor.top - bounds.height - 8, innerHeight - bounds.height - 12))}px`;
  }

  function installPairMenus(host) {
    syncPairMenus(host);
    if (_pairMenuHosts.has(host)) return;
    _pairMenuHosts.add(host);
    new ResizeObserver(() => syncPairMenus(host)).observe(host);
    host.addEventListener("beforetoggle", event => {
      const menu = event.target;
      if (!menu.matches(".pair-overflow")) return;
      const more = menu.closest(".pair-card").querySelector(".pair-more");
      if (event.newState === "open") {
        syncPairMenus(host);
        more.setAttribute("aria-expanded", "true");
        requestAnimationFrame(() => {
          if (!menu.matches(":popover-open")) return;
          positionPairMenu(menu);
          menu.querySelector("button:not(:disabled)")?.focus({preventScroll:true});
        });
        return;
      }
      more.setAttribute("aria-expanded", "false");
      if (menu.contains(document.activeElement)) more.focus({preventScroll:true});
      resetPairDelete();
    }, true);
  }

  function refreshBadges(board) {
    [...board.querySelectorAll(".pair-card")].forEach((el, i) => {
      const b = el.querySelector(".pair-order"); if (b) b.textContent = String(i + 1);
      const handle = el.querySelector(".cw-page-pair-handle");
      if (handle) handle.setAttribute("aria-label", `Reorder pair ${i + 1}: ${providerLabel(el.dataset.source)} to ${providerLabel(el.dataset.target)}. Use Up or Down arrow keys.`);
    });
  }

  function animatePairOrder(board, change) {
    const cards = [...board.querySelectorAll(".pair-card")];
    const before = new Map(cards.map(card => [card, card.getBoundingClientRect().top]));
    cards.forEach(card => card.getAnimations().forEach(animation => animation.cancel()));
    change();
    refreshBadges(board);
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
    cards.forEach(card => {
      const dy = before.get(card) - card.getBoundingClientRect().top;
      if (Math.abs(dy) > 1) card.animate([{ translate: `0 ${dy}px` }, { translate: "0 0" }], { duration: 180, easing: "ease-out" });
    });
  }

  function pairOrderNotice(host, message) {
    host.querySelector(".pair-order-status").textContent = message;
  }

  async function savePairOrder(host, board, original) {
    const cards = [...board.querySelectorAll(".pair-card")];
    const order = cards.map(card => card.dataset.id);
    if (cards.every((card, index) => card === original[index])) return;
    _reorderBusy = true;
    board.inert = true;
    board.setAttribute("aria-busy", "true");
    const focused = document.activeElement;
    pairOrderNotice(host, "Saving pair order…");
    try {
      const response = await fetch("/api/pairs/reorder", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(order) });
      const data = await response.json();
      if (!response.ok || data?.ok !== true) throw new Error("reorder_failed");
      const pairs = window.cx?.pairs || [];
      const savedOrder = Array.isArray(data.final_order) ? data.final_order.map(String) : order;
      window.cx.pairs = [...pairs].sort((a, b) => savedOrder.indexOf(String(a.id)) - savedOrder.indexOf(String(b.id)));
      animatePairOrder(board, () => {
        for (const id of savedOrder) {
          const card = cards.find(card => card.dataset.id === id);
          if (card) board.appendChild(card);
        }
      });
      pairOrderNotice(host, "Pair order saved.");
      window._invalidatePairsCache?.();
      document.dispatchEvent(new Event("cw:sync-data-changed"));
    } catch {
      animatePairOrder(board, () => original.forEach(card => board.appendChild(card)));
      pairOrderNotice(host, "Couldn't save the order. Your previous order is restored.");
      window.alert("Couldn't save the pair order. Your previous order is restored. Please try again.");
    } finally {
      board.inert = false;
      board.removeAttribute("aria-busy");
      _reorderBusy = false;
      _renderedPairs.delete(board);
      if (focused?.isConnected) focused.focus({ preventScroll: true });
    }
  }

  function installPairReorder(host, board) {
    if (board.dataset.reorderBound) return;
    board.dataset.reorderBound = "1";
    const status = document.createElement("div");
    status.className = "pair-order-status sr-only";
    status.setAttribute("role", "status");
    host.appendChild(status);

    const move = () => {
      const drag = _pairDrag;
      if (!drag?.moving) return;
      const others = [...board.querySelectorAll(".pair-card")].filter(card => card !== drag.card);
      const top = board.getBoundingClientRect().top;
      const next = others.find(card => drag.y < top + card.offsetTop + card.offsetHeight / 2) || null;
      if (drag.card.nextElementSibling === next) return;
      animatePairOrder(board, () => board.insertBefore(drag.card, next));
      pairOrderNotice(host, `Pair moved to position ${[...board.children].indexOf(drag.card) + 1}. Release to save, or press Escape to cancel.`);
    };
    const scroll = () => {
      const drag = _pairDrag;
      if (!drag) return;
      if (drag.moving) {
        const bounds = host.getBoundingClientRect();
        const top = Math.max(0, bounds.top);
        const bottom = Math.min(window.innerHeight, bounds.bottom);
        const dy = drag.y < top + 48 ? -12 : drag.y > bottom - 48 ? 12 : 0;
        if (dy) {
          window.scrollBy(0, dy);
          move();
        }
      }
      drag.frame = requestAnimationFrame(scroll);
    };
    const finish = (cancel = false) => {
      const drag = _pairDrag;
      if (!drag) return;
      _pairDrag = null;
      cancelAnimationFrame(drag.frame);
      drag.card.classList.remove("pair-reordering");
      board.classList.remove("pair-ordering");
      if (board.hasPointerCapture(drag.pointerId)) board.releasePointerCapture(drag.pointerId);
      drag.handle.focus({ preventScroll: true });
      if (cancel) {
        animatePairOrder(board, () => drag.original.forEach(card => board.appendChild(card)));
        pairOrderNotice(host, "Reordering cancelled.");
      } else {
        savePairOrder(host, board, drag.original);
      }
    };
    board.addEventListener("pointerdown", event => {
      const handle = event.target.closest(".cw-page-pair-handle");
      if (!handle || event.button !== 0 || event.isPrimary === false || _pairDrag || _reorderBusy || board.children.length < 2) return;
      event.preventDefault();
      handle.focus({ preventScroll: true });
      _pairDrag = { handle, card: handle.closest(".pair-card"), pointerId: event.pointerId, startY: event.clientY, y: event.clientY, original: [...board.children], moving: false };
      board.setPointerCapture(event.pointerId);
      _pairDrag.frame = requestAnimationFrame(scroll);
    });
    board.addEventListener("pointermove", event => {
      const drag = _pairDrag;
      if (!drag || event.pointerId !== drag.pointerId) return;
      drag.y = event.clientY;
      if (!drag.moving && Math.abs(drag.y - drag.startY) < 6) return;
      drag.moving = true;
      drag.card.classList.add("pair-reordering");
      board.classList.add("pair-ordering");
      move();
    });
    board.addEventListener("pointerup", event => { if (event.pointerId === _pairDrag?.pointerId) finish(); });
    board.addEventListener("pointercancel", () => finish(true));
    board.addEventListener("lostpointercapture", () => finish(true));
    window.addEventListener("blur", () => finish(true));
    document.addEventListener("keydown", event => {
      if (event.key !== "Escape" || !_pairDrag) return;
      event.preventDefault();
      event.stopPropagation();
      finish(true);
    }, true);
    board.addEventListener("keydown", event => {
      const handle = event.target.closest(".cw-page-pair-handle");
      if (!handle || _pairDrag || _reorderBusy || !["ArrowUp", "ArrowDown", "Home", "End"].includes(event.key)) return;
      event.preventDefault();
      const card = handle.closest(".pair-card");
      const original = [...board.children];
      const target = event.key === "ArrowUp" ? card.previousElementSibling : event.key === "ArrowDown" ? card.nextElementSibling?.nextElementSibling : event.key === "Home" ? board.firstElementChild : null;
      if (event.key === "ArrowUp" && !target) return;
      animatePairOrder(board, () => board.insertBefore(card, target || null));
      handle.focus({ preventScroll: true });
      card.scrollIntoView({ block: "nearest" });
      savePairOrder(host, board, original);
    });
  }

  async function renderOrEnhance(force = false) {
    if (_renderBusy || _pairDrag || _reorderBusy) return;
    _renderBusy = true;
    try { await loadPairsIfNeeded(!!force); renderPairsOverlay(); }
    finally { _renderBusy = false; }
  }

  document.addEventListener("DOMContentLoaded", () => {
    renderOrEnhance();
  });
  document.addEventListener("cw-settings-pane-changed", (ev) => {
    if (String(ev?.detail?.pane || "").toLowerCase() === "sync") {
      renderOrEnhance(true);
    }
  });
  document.addEventListener("cx-state-change", renderOrEnhance);
  window.addEventListener("auth-changed", () => renderOrEnhance());
  document.addEventListener("click", event => {
    const menu = event.target.closest?.(".pair-overflow:popover-open");
    if (menu && event.target.closest("button:not(.danger)")) menu.hidePopover();
    const reset = event.target.closest?.("[data-pair-reset]");
    if (reset && !reset.disabled) resetPairCard(reset);
    const logs = event.target.closest?.('[data-pair-logs]');
    if (logs) window.openLogs?.({pairId:logs.dataset.pairLogs, latest:true});
    const button = event.target.closest?.("[data-interactive-pair]");
    if (button && !button.disabled) location.hash = `interactive_sync?pair=${encodeURIComponent(button.dataset.interactivePair)}`;
  });
  window.addEventListener("cx:pairs:changed", () => { renderOrEnhance(true); });

  window.addEventListener("resize", () => {
    document.querySelectorAll("#pairs_list .pair-overflow:popover-open").forEach(menu => menu.hidePopover());
    const host = document.getElementById("pairs_list");
    if (host?.querySelector(".pair-more")) syncPairMenus(host);
  }, { passive: true });

  window.addEventListener("scroll", event => {
    const menu = document.querySelector("#pairs_list .pair-overflow:popover-open");
    if (menu && !(event.target instanceof Node && menu.contains(event.target))) positionPairMenu(menu);
  }, {capture:true, passive:true});

  const _origRender = window.renderConnections;
  window.renderConnections = function () { try { if (typeof _origRender === "function") _origRender(); } catch {} renderOrEnhance(); };

  window.cxRenderPairsOverlay = renderOrEnhance;
})();
