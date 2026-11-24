/* assets/js/editor/send-modal.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;

  function escFactory(ctx) {
    if (typeof ctx?.escapeHtml === "function") return ctx.escapeHtml;
    return (value) => String(value ?? "").replace(/[&<>"']/g, ch => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    }[ch]));
  }

  function providerCapabilityKey(kind) {
    if (kind === "ratings") return "ratings_enabled";
    return `${kind}_enabled`;
  }

  function providerDisplayName(provider) {
    const PM = window.CW?.ProviderMeta;
    const p = String(provider?.provider || "").toUpperCase();
    const label = provider?.display || provider?.label || PM?.label?.(p) || p;
    return String(label || p);
  }

  function providerLogoHtml(provider, esc) {
    const PM = window.CW?.ProviderMeta;
    const p = String(provider?.provider || "").toUpperCase();
    if (typeof PM?.logoHtml === "function") return PM.logoHtml(p, "cw-editor-send-logo");
    const src = PM?.logoPath?.(p) || "";
    return src
      ? `<img class="cw-editor-send-logo" src="${esc(src)}" alt="${TH("editor.sendModal.logo", {name: p})}" width="36" height="36" loading="lazy">`
      : `<span class="material-symbols-rounded" aria-hidden="true">hub</span>`;
  }

  function providerToneStyle(provider, esc) {
    const PM = window.CW?.ProviderMeta;
    const p = String(provider?.provider || "").toUpperCase();
    const rgb = PM?.tone?.(p)?.rgb || "124,92,255";
    return `--send-rgb:${esc(String(rgb))}`;
  }

  function sendKindLabel(kind) {
    const k = String(kind || "").toLowerCase();
    return (["watchlist", "history", "ratings", "progress", "collection"].includes(k) ? T(`editor.sendModal.kind.${k}`) : (k ? k[0].toUpperCase() + k.slice(1) : T("editor.sendModal.kind.data")));
  }

  function sendKindIcon(kind) {
    const k = String(kind || "").toLowerCase();
    return ({ watchlist: "bookmark_add", history: "history", ratings: "star", progress: "resume", collection: "inventory_2" }[k] || "send");
  }

  function formatSendResult(data, esc) {
    const removing = data?.operation === "remove";
    const verb = T(removing ? "editor.sendModal.removed" : "editor.sendModal.sent");
    const confirmed = Number(data?.confirmed || 0);
    const attempted = Number(data?.attempted || data?.sent || 0);
    const skipped = Number(data?.skipped || 0);
    const unresolved = Number(data?.unresolved || 0);
    const errors = Number(data?.errors || 0);
    const invalid = Number(data?.invalid || 0);
    const ok = data?.ok !== false && errors === 0 && unresolved === 0;
    const done = confirmed + skipped;
    const pct = attempted ? Math.max(0, Math.min(100, Math.round((done / attempted) * 100))) : (ok ? 100 : 0);
    const providers = Array.isArray(data?.results) ? data.results : [];
    const rows = providers.map(r => {
      const result = r?.result || {};
      const name = providerDisplayName(r || {});
      const parts = [
        `${Number(result.confirmed || result.count || 0)} ${verb}`,
        Number(result.skipped || 0) ? T("editor.sendModal.nSkipped", {n: Number(result.skipped || 0)}) : "",
        Number(result.unresolved || 0) ? T("editor.sendModal.nUnresolved", {n: Number(result.unresolved || 0)}) : "",
        Number(result.errors || 0) ? T("editor.sendModal.nErrors", {n: Number(result.errors || 0)}) : "",
        removing && Number(result.still_watched || 0) ? T("editor.sendModal.stillWatched", {n: Number(result.still_watched)}) : "",
        r?.error === "unsupported_rating" ? T("editor.sendModal.unsupportedScores") : r?.error ? String(r.error) : "",
      ].filter(Boolean).join(" - ");
      return `<div class="cw-editor-send-result-row ${r?.ok ? "ok" : "bad"}" style="${providerToneStyle(r, esc)}">
        <span class="cw-editor-send-result-icon">${providerLogoHtml(r, esc)}</span>
        <strong>${esc(name)}</strong>
        <small>${esc(parts || (r?.ok ? T("editor.sendModal.done") : T("editor.sendModal.failedShort")))}</small>
        ${(r.rejected || []).map(item => `<small>${esc(item.title || T("editor.table.untitled"))}: ${TH("editor.sendModal.ratingUnsupported", {rating: String(item.rating)})}</small>`).join("")}
        ${(result.cloud_results || []).map(row => `<small>${esc(row.title || row.key)}: ${esc(["dated", "present", "cached", "plex_discover_date_pending"].includes(row.outcome) ? T(`editor.sendModal.cloud.${row.outcome}`) : row.reason || row.outcome)}</small>`).join("")}
      </div>`;
    }).join("");
    return `<div class="cw-editor-send-progress ${ok ? "done" : "error"}">
      <div class="cw-editor-send-progress-head">
        <div>
          <strong>${removing ? (ok ? T("editor.sendModal.removalComplete") : T("editor.sendModal.removalIssues")) : (ok ? T("editor.sendModal.sendComplete") : T("editor.sendModal.sendIssues"))}</strong>
          <span>${esc(providers.length ? T("editor.sendModal.processed", {count: providers.length}) : T("editor.sendModal.processedOne"))}</span>
        </div>
        <span class="material-symbols-rounded" aria-hidden="true">${ok ? "check_circle" : "error"}</span>
      </div>
      <div class="cw-editor-send-progress-bar" aria-hidden="true"><span style="width:${pct}%"></span></div>
      <div class="cw-editor-send-progress-grid">
        <div><span>${verb}</span><b>${confirmed}</b></div>
        <div><span>${TH("editor.sendModal.attempted")}</span><b>${attempted}</b></div>
        <div><span>${TH("editor.sendModal.skipped")}</span><b>${skipped}</b></div>
        <div><span>${TH("editor.sendModal.unresolved")}</span><b>${unresolved}</b></div>
        <div><span>${TH("editor.sendModal.errors")}</span><b>${errors}</b></div>
        ${invalid ? `<div><span>${TH("editor.sendModal.invalid")}</span><b>${invalid}</b></div>` : ""}
      </div>
      ${rows ? `<div class="cw-editor-send-result-list">${rows}</div>` : ""}
    </div>`;
  }

  function formatSendRunning(targetCount, operation = "add") {
    if (operation === "remove") return '<div class="cw-editor-send-progress active"><strong>' + TH("editor.sendModal.removingRecords") + '</strong><p>' + TH("editor.sendModal.checking") + '</p><div class="cw-editor-send-progress-bar"><span style="width:24%"></span></div></div>';
    return `<div class="cw-editor-send-progress active">
      <div class="cw-editor-send-progress-head">
        <div><strong>${TH("editor.sendModal.sendingRows")}</strong><span>${TH("editor.sendModal.queued", {count: Number(targetCount || 0)})}</span></div>
        <span class="material-symbols-rounded" aria-hidden="true">sync</span>
      </div>
      <div class="cw-editor-send-progress-bar" aria-hidden="true"><span style="width:24%"></span></div>
      <div class="cw-editor-send-progress-message">${TH("editor.sendModal.sendingTo", {count: Number(targetCount || 0)})}</div>
    </div>`;
  }

  let returnFocus = null;

  function close() {
    const shell = document.getElementById("cw-editor-send-modal");
    window.CW?.DialogFocus?.deactivate(shell);
    shell?.remove();
    if (returnFocus?.isConnected) returnFocus.focus?.({ preventScroll: true });
    returnFocus = null;
  }

  function notify(message, ok = false) {
    const msg = String(message || "");
    try {
      if (window.CW?.DOM?.showToast) {
        window.CW.DOM.showToast(msg, ok);
        return;
      }
      if (window.cxToast) {
        window.cxToast(msg);
        return;
      }
    } catch (_) {}
    const el = document.createElement("div");
    el.className = `save-toast ${ok ? "ok" : "error"}`;
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => el.remove(), 3800);
  }

  async function open(ctx = {}) {
    const esc = escFactory(ctx);
    const state = ctx.state || {};
    let rows = ctx.selectedRowsForSend();
    const removableRows = ctx.selectedRowsForSend("remove");
    let operation = rows.length ? "add" : "remove";
    if (!rows.length) rows = removableRows;
    if (!rows.length) {
      notify(T("editor.sendModal.selectActive"));
      return;
    }
    const kind = String(state.kind || "watchlist").toLowerCase();
    let providers = [];
    const rememberedKey = `cw.editorSend.${kind}.providers`;
    let selected = new Set();
    let previewId = "";
    let removalEmptyReason = "";
    let busy = false;
    let loading = false;
    let generation = 0;
    let confirmUntil = 0;
    let confirmTimer;
    const providerKey = p => `${String(p.provider || "").toUpperCase()}:${String(p.instance || "default")}`;
    const scoped = url => ctx.sendQuery ? `${url}${url.includes("?") ? "&" : "?"}${ctx.sendQuery}` : url;
    const inScope = p => !ctx.sendScope || ctx.sendScope.has(providerKey(p));
    const currentSourceProviderKey = () => {
      if (ctx.isProviderPickerSource() && state.snapshot) {
        return providerKey({ provider: state.snapshot, instance: state.instance || "default" });
      }
      if (state.source === "playlist") {
        const ep = ctx.currentPlaylistEndpoint();
        if (ep && ep.provider) return providerKey({ provider: ep.provider, instance: ep.instance || "default" });
      }
      return "";
    };

    close();
    returnFocus = document.activeElement;
    const shell = document.createElement("div");
    shell.id = "cw-editor-send-modal";
    shell.className = "cw-editor-send-overlay cw-page-send";
    const kindLabel = sendKindLabel(kind);
    const kindIcon = sendKindIcon(kind);
    shell.innerHTML = `
      <div class="modal-backdrop"></div>
      <div id="cx-modal" class="cx-card cw-editor-send-card" role="dialog" aria-modal="true" aria-label="${TH("editor.sendModal.sendRows")}">
        <div class="cx-head">
          <div class="cw-editor-send-head-left">
            <div class="cw-editor-send-head-icon"><span class="material-symbols-rounded" aria-hidden="true">send</span></div>
            <div>
              <div class="cw-editor-send-title">${TH("editor.sendModal.sendRows")}</div>
              <div class="cw-editor-send-sub">${TH("editor.sendModal.selectedRows", {count: rows.length, kind: kindLabel})}</div>
            </div>
          </div>
          <button type="button" class="cx-btn cw-editor-send-close" data-send-close aria-label="${TH("editor.raw.close")}"><span class="material-symbols-rounded" aria-hidden="true">close</span></button>
        </div>
        <div class="body">
          <div class="cw-editor-send-operation" role="group" aria-label="${TH("editor.sendModal.operation")}">
            <button type="button" class="cx-btn" data-operation="add">${TH("editor.sendModal.addTo")}</button>
            <button type="button" class="cx-btn" data-operation="remove">${TH("editor.sendModal.removeFrom")}</button>
          </div>
          <section class="cw-editor-send-section cw-editor-send-target-card">
            <div class="cw-editor-send-list">
              <div class="cw-editor-send-empty">${TH("editor.sendModal.loading")}</div>
            </div>
          </section>
          <section class="cw-editor-send-section cw-editor-send-data-card">
            <div class="cw-editor-send-section-head compact">
              <span class="material-symbols-rounded" aria-hidden="true">${esc(kindIcon)}</span>
              <div>
                <h3>${TH("editor.sendModal.dataToSend")}</h3>
                <p>${TH("editor.sendModal.willSend", {count: rows.length, kind: kindLabel})}</p>
              </div>
            </div>
            <div class="cw-editor-send-data-grid">
              <div><span>${TH("editor.sendModal.feature")}</span><b>${esc(kindLabel)}</b></div>
              <div><span>${TH("editor.sendModal.rows")}</span><b>${rows.length}</b></div>
              <div><span>${TH("editor.sendModal.mode")}</span><b>${TH("editor.sendModal.selective")}</b></div>
            </div>
          </section>
          ${kind === "history" ? `<section data-discover-option hidden><label><input type="checkbox" data-discover-fallback> ${TH("editor.sendModal.discover")}</label><p>${TH("editor.sendModal.discoverHelp")}</p></section>` : ""}
          <section class="cw-editor-send-run-card">
            <div class="cw-editor-send-warning">
              <span class="material-symbols-rounded" aria-hidden="true">warning</span>
              <div>${TH("editor.sendModal.warning", {kind: kindLabel})}</div>
            </div>
            <div class="cw-editor-send-run-actions">
              <button type="button" class="cx-btn primary" data-send-submit disabled><span class="material-symbols-rounded" aria-hidden="true">send</span><span>${TH("editor.sendModal.sendData")}</span></button>
            </div>
            <div class="cw-editor-send-status" data-send-status aria-live="polite"></div>
          </section>
        </div>
      </div>`;
    document.body.appendChild(shell);
    window.CW?.DialogFocus?.activate(shell, () => {
      if (!busy) {
        clearTimeout(confirmTimer);
        close();
      }
    });
    const dialog = shell.querySelector('[role="dialog"]');
    dialog.tabIndex = -1;
    dialog.focus?.({ preventScroll: true });
    shell.addEventListener("keydown", event => {
      if (event.key !== "Escape") return;
      event.preventDefault();
      event.stopPropagation();
      if (!busy) {
        clearTimeout(confirmTimer);
        close();
      }
    });
    const list = shell.querySelector(".cw-editor-send-list");
    const submit = shell.querySelector("[data-send-submit]");
    const status = shell.querySelector("[data-send-status]");
    const body = shell.querySelector(".body");
    const baseWarning = shell.querySelector(".cw-editor-send-warning div").textContent;
    const resetConfirm = () => {
      confirmUntil = 0;
      clearTimeout(confirmTimer);
      submit.classList.remove("is-confirming");
      submit.innerHTML = operation === "remove"
        ? '<span class="material-symbols-rounded">delete</span><span>' + TH("editor.sendModal.removeData") + '</span>'
        : '<span class="material-symbols-rounded">send</span><span>' + TH("editor.sendModal.sendData") + '</span>';
    };
    const updateMode = () => {
      const removing = operation === "remove";
      shell.classList.toggle("is-removing", removing);
      shell.querySelector(".cw-editor-send-title").textContent = removing ? T("editor.sendModal.removeRecords") : T("editor.sendModal.sendRows");
      shell.querySelector(".cw-editor-send-head-icon .material-symbols-rounded").textContent = removing ? "delete" : "send";
      shell.querySelector('[role="dialog"]').setAttribute("aria-label", removing ? T("editor.sendModal.removeRecords") : T("editor.sendModal.sendRows"));
      shell.querySelector(".cw-editor-send-sub").textContent = T("editor.sendModal.selectedRows", {count: rows.length, kind: kindLabel});
      shell.querySelector(".cw-editor-send-data-card h3").textContent = removing ? T("editor.sendModal.dataToRemove") : T("editor.sendModal.dataToSend");
      shell.querySelector(".cw-editor-send-data-card p").textContent = removing
        ? T("editor.sendModal.onlyMatching")
        : T("editor.sendModal.willSend", {count: rows.length, kind: kindLabel});
      const summary = shell.querySelectorAll(".cw-editor-send-data-grid b");
      summary[1].textContent = String(rows.length);
      summary[2].textContent = removing ? T("editor.sendModal.selectiveRemoval") : T("editor.sendModal.selectiveSend");
      shell.querySelector(".cw-editor-send-warning div").textContent = removing
        ? (kind === "history"
          ? T("editor.sendModal.removeHistory")
          : T("editor.sendModal.removeOther"))
        : baseWarning;
      shell.querySelectorAll("[data-operation]").forEach(button => {
        button.classList.toggle("active", button.dataset.operation === operation);
        button.setAttribute("aria-pressed", String(button.dataset.operation === operation));
      });
      resetConfirm();
    };
    const scrollSendStatusIntoView = () => {
      window.requestAnimationFrame(() => {
        body?.scrollTo({ top: body.scrollHeight, behavior: "smooth" });
      });
    };

    const unsupportedRatings = provider => kind === "ratings" && operation === "add"
      ? rows.filter(row => !(provider.rating_scale?.values || []).includes(Number(ctx.rowToSendItem(row)?.rating)))
      : [];
    const syncButtons = () => {
      for (const provider of providers) {
        if (rows.length && unsupportedRatings(provider).length === rows.length) selected.delete(providerKey(provider));
      }
      shell.querySelectorAll("[data-provider-key]").forEach(btn => {
        const key = btn.getAttribute("data-provider-key") || "";
        const on = selected.has(key);
        btn.classList.toggle("active", on);
        btn.setAttribute("aria-pressed", on ? "true" : "false");
      });
      const discoverOption = shell.querySelector("[data-discover-option]");
      const discoverInput = shell.querySelector("[data-discover-fallback]");
      const discoverAvailable = kind === "history" && operation === "add" && [...selected].some(key => key.startsWith("PLEX:"));
      if (discoverOption) discoverOption.hidden = !discoverAvailable;
      if (discoverInput) {
        discoverInput.disabled = busy || !discoverAvailable;
        if (!discoverAvailable) discoverInput.checked = false;
      }
      if (submit) submit.disabled = busy || loading || !selected.size || !rows.length;
      shell.querySelectorAll("[data-operation], [data-provider-key], [data-send-close]").forEach(button => {
        const provider = providers.find(p => providerKey(p) === button.getAttribute("data-provider-key"));
        button.disabled = busy || (button.dataset.operation === "add" && !ctx.selectedRowsForSend().length)
          || !!(provider && rows.length && unsupportedRatings(provider).length === rows.length);
      });
    };

    const renderProviderList = () => {
      if (!list) return;
      list.innerHTML = providers.length ? providers.map(p => {
        const key = providerKey(p);
        const unsupported = unsupportedRatings(p).length;
        const ratingNote = unsupported ? T("editor.sendModal.ratingNote", {supported: rows.length - unsupported, count: unsupported}) : "";
        const providerClass = `provider-${String(p.provider || "").toLowerCase().replace(/[^a-z0-9_-]+/g, "")}`;
        return `<button type="button" class="cw-editor-send-provider ${providerClass} ${selected.has(key) ? "active" : ""}" data-provider-key="${esc(key)}" aria-pressed="${selected.has(key) ? "true" : "false"}" style="${providerToneStyle(p, esc)}">
          <span class="cw-editor-send-provider-icon">${providerLogoHtml(p, esc)}</span>
          <span><strong>${esc(providerDisplayName(p))}</strong>${ratingNote ? `<small>${esc(ratingNote)}</small>` : ""}<small>${esc(p.instance_label || String(p.instance || "default"))}${operation === "remove" ? ` · ${TH("editor.sendModal.matched", {matched: p.matched, total: rows.length})}` : ""}${ctx.sendNotes?.[key] ? ` · ${esc(ctx.sendNotes[key])}` : ""}</small></span>
          <span class="cw-editor-send-check"><span class="material-symbols-rounded" aria-hidden="true">check</span></span>
        </button>`;
      }).join("") : `<div class="cw-editor-send-empty">${operation === "remove"
        ? (removalEmptyReason === "pair_baseline_unavailable"
          ? T("editor.sendModal.noBaseline")
          : T("editor.sendModal.noRemoval"))
        : TH("editor.sendModal.noOther", {kind})}</div>`;
      syncButtons();
    };

    shell.addEventListener("click", async ev => {
      const target = ev.target instanceof Element ? ev.target : ev.target?.parentElement;
      if (!target) return;
      if (busy) return;
      if (target.closest("[data-send-close]")) {
        generation++;
        clearTimeout(confirmTimer);
        close();
        return;
      }
      const modeButton = target.closest("[data-operation]");
      if (modeButton) {
        operation = modeButton.dataset.operation;
        rows = ctx.selectedRowsForSend(operation);
        selected = new Set();
        previewId = "";
        updateMode();
        await loadProviders();
        return;
      }
      const providerBtn = target.closest("[data-provider-key]");
      if (providerBtn) {
        const key = providerBtn.getAttribute("data-provider-key") || "";
        if (selected.has(key)) selected.delete(key);
        else selected.add(key);
        resetConfirm();
        syncButtons();
        return;
      }
      if (!target.closest("[data-send-submit]")) return;
      if (!selected.size || loading) return;
      if (operation === "remove" && Date.now() > confirmUntil) {
        confirmUntil = Date.now() + 6000;
        submit.classList.add("is-confirming");
        const count = providers.filter(p => selected.has(providerKey(p))).reduce((sum, p) => sum + p.count, 0);
        submit.innerHTML = `<span class="material-symbols-rounded">warning</span><span>${TH("editor.sendModal.confirmRemoval", {count})}</span>`;
        confirmTimer = setTimeout(resetConfirm, 6000);
        return;
      }
      const targets = providers
        .filter(p => selected.has(providerKey(p)))
        .map(p => ({ provider: p.provider, instance: p.instance || "default" }));
      busy = true;
      dialog.focus?.({ preventScroll: true });
      clearTimeout(confirmTimer);
      syncButtons();
      if (submit) {
        submit.disabled = true;
        submit.classList.add("busy");
      }
      if (status) {
        status.innerHTML = formatSendRunning(targets.length, operation);
        scrollSendStatusIntoView();
      }
      try {
        const data = await ctx.fetchJSON(scoped("/api/editor/send"), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ kind, operation, providers: targets, items: rows.map(ctx.rowToSendItem),
            plex_discover_fallback: kind === "history" && operation === "add" && shell.querySelector("[data-discover-fallback]")?.checked === true,
            ...(operation === "remove" ? { confirmed: true, preview_id: previewId, ...removalScope() } : {}) }),
        });
        if (operation === "add" && !ctx.preselect) {
          try { localStorage.setItem(rememberedKey, JSON.stringify([...selected])); } catch (_) {}
        }
        const confirmed = Number(data.confirmed || 0);
        const unresolved = Number(data.unresolved || 0);
        const skipped = Number(data.skipped || 0);
        const parts = [`${confirmed} ${T(operation === "remove" ? "editor.sendModal.removed" : "editor.sendModal.sent")}`];
        if (skipped) parts.push(T("editor.sendModal.nSkipped", {n: skipped}));
        if (unresolved) parts.push(T("editor.sendModal.nUnresolved", {n: unresolved}));
        ctx.setStatusSticky(parts.join(", "), 5000);
        window.dispatchEvent(new CustomEvent("cw:editor-send-complete", { detail: data }));
        if (data.ok !== false) ctx.clearSelection();
        await ctx.loadState();
        if (status) {
          status.innerHTML = formatSendResult(data, esc);
          scrollSendStatusIntoView();
        }
        if (submit) {
          submit.classList.remove("busy");
          resetConfirm();
        }
      } catch (err) {
        if (status) {
          status.innerHTML = `<div class="cw-editor-send-progress error">
          <div class="cw-editor-send-progress-head">
            <div><strong>${operation === "remove" ? T("editor.sendModal.removalFailed") : T("editor.sendModal.sendFailed")}</strong><span>${esc(String(err?.message || T("editor.sendModal.processFailed")))}</span></div>
            <span class="material-symbols-rounded" aria-hidden="true">error</span>
          </div>
          <div class="cw-editor-send-progress-bar" aria-hidden="true"><span style="width:100%"></span></div>
          <div class="cw-editor-send-progress-message">${esc(String(err?.message || T("editor.sendModal.sendFailed")))}</div>
        </div>`;
          scrollSendStatusIntoView();
        }
        if (submit) {
          submit.classList.remove("busy");
          resetConfirm();
        }
      } finally {
        const discoverInput = shell.querySelector("[data-discover-fallback]");
        if (discoverInput) discoverInput.checked = false;
        busy = false;
        // Require a fresh preview and confirmation before another removal attempt.
        if (operation === "remove") {
          selected = new Set();
          previewId = "";
          await loadProviders(true);
        }
        syncButtons();
      }
    });

    function removalScope() {
      return { pair_id: state.mappingPair || "", source_provider: ctx.isProviderPickerSource() ? state.snapshot : "",
        source_instance: state.instance || "default" };
    }

    async function loadProviders(preserveStatus = false) {
      const revision = ++generation;
      loading = true;
      providers = [];
      list.innerHTML = '<div class="cw-editor-send-empty">' + TH("editor.sendModal.loading") + '</div>';
      if (!preserveStatus) status.innerHTML = "";
      syncButtons();
      try {
        const data = operation === "remove"
          ? await ctx.fetchJSON(scoped("/api/editor/send/preview"), { method: "POST", headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ kind, items: rows.map(ctx.rowToSendItem), ...removalScope() }) })
          : await ctx.fetchJSON(scoped(`/api/editor/send/providers?kind=${encodeURIComponent(kind)}`));
        if (revision !== generation || !shell.isConnected) return;
        const capKey = providerCapabilityKey(kind);
        const sourceKey = currentSourceProviderKey();
        if (operation === "remove") {
          providers = (Array.isArray(data.providers) ? data.providers : []).filter(inScope);
          previewId = data.preview_id;
          removalEmptyReason = data.empty_reason || "";
        } else {
          providers = (Array.isArray(data.providers) ? data.providers : [])
            .filter(p => !!p?.[capKey])
            .filter(inScope)
            .filter(p => providerKey(p) !== sourceKey);
          try {
            const saved = JSON.parse(localStorage.getItem(rememberedKey) || "[]");
            if (Array.isArray(saved)) selected = new Set(saved.map(String));
          } catch (_) {}
        }
        if (operation === "add" && ctx.preselect) selected = new Set([ctx.preselect]);
        selected = new Set([...selected].filter(k => providers.some(p => providerKey(p) === k)));
        if (operation === "add" && !selected.size && providers.length === 1) selected.add(providerKey(providers[0]));
        if (status && !preserveStatus && operation === "add") status.innerHTML = providers.length
          ? ""
          : `<div class="cw-editor-send-status-line">${TH("editor.sendModal.noTarget")}</div>`;
        renderProviderList();
      } catch (err) {
        if (revision !== generation || !shell.isConnected) return;
        const msg = String(err?.message || T("editor.sendModal.loadFailed"));
        if (list) list.innerHTML = `<div class="cw-editor-send-empty">${TH("editor.sendModal.loadFailedDot")}</div>`;
        if (status) status.innerHTML = `<div class="cw-editor-send-status-line err">${esc(msg)}</div>`;
        if (submit) submit.disabled = true;
      } finally {
        if (revision === generation) {
          loading = false;
          syncButtons();
        }
      }
    }
    updateMode();
    await loadProviders();
  }

  Editor.SendModal = { open, close };
  window.CrossWatchEditorSendModal = Editor.SendModal;
})();
