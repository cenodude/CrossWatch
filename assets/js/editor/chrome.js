/* assets/js/editor/chrome.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, c => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    }[c]));
  }

  function wireStaticLabels(root) {
    if (!root) return;

    const bindPrevLabel = (fieldId) => {
      const field = root.querySelector(`#${fieldId}`);
      const label = field?.previousElementSibling;
      if (label?.tagName === "LABEL") label.htmlFor = fieldId;
    };

    bindPrevLabel("cw-source");
    bindPrevLabel("cw-kind");
    bindPrevLabel("cw-pair");
    bindPrevLabel("cw-snapshot");
    bindPrevLabel("cw-instance");

    const convertGroupLabel = (cardId) => {
      const field = root.querySelector(`#${cardId} .ins-kv`);
      const label = field?.firstElementChild;
      if (!field || label?.tagName !== "LABEL") return;
      const title = document.createElement("div");
      title.className = "field-label";
      title.textContent = label.textContent || "";
      label.replaceWith(title);
    };

    convertGroupLabel("cw-backup-card");
    convertGroupLabel("cw-state-backup-card");
  }

  function prepareSourceOptions(root) {
    const sourceSelect = document.getElementById("cw-source");
    if (sourceSelect) {
      sourceSelect.querySelector('option[value="pair"]')?.remove();
      sourceSelect.querySelector('option[value="tracker"]')?.remove();
      if (!sourceSelect.querySelector('option[value="manual"]')) {
        const manualOpt = document.createElement("option");
        manualOpt.value = "manual";
        manualOpt.textContent = "Manual Overrides";
        sourceSelect.appendChild(manualOpt);
      } else {
        sourceSelect.querySelector('option[value="manual"]').textContent = "Manual Overrides";
      }
      sourceSelect.querySelector('option[value="playlist"]')?.remove();
    }

    const sub = root?.querySelector(".cw-sub");
    if (sub) sub.textContent = "Edit your current state or playlist endpoints";
  }

  function addTrackerNotice(root) {
    root?.querySelector("#cw-tracker-notice")?.remove();
  }

  function ensureFieldNames(root) {
    root?.querySelectorAll("input,select,textarea").forEach((field, idx) => {
      if (!field.name) field.name = field.id || `cw-field-${idx + 1}`;
    });
  }

  function decorateImportPanel(ctx = {}) {
    const details = document.getElementById("cw-import-details");
    if (!details || details.dataset.decorated === "1") return;
    details.dataset.decorated = "1";
    details.classList.add("cw-import-panel");

    const summary = details.querySelector("summary");
    if (summary) {
      summary.classList.add("cw-import-summary");
      summary.innerHTML =
        '<span class="cw-import-title">Import provider state</span>' +
        '<span class="cw-import-help material-symbol" title="Imports selected Watchlist, History, Ratings, or Progress data from a configured provider profile into Current State. Replace baseline refreshes those datasets from the provider; Merge keeps existing baseline rows and adds or updates provider rows. Provider accounts are not changed." aria-label="Import provider state help">help</span>';
    }

    const body = summary ? summary.nextElementSibling : null;
    if (body instanceof HTMLElement) {
      body.classList.add("cw-import-body");
      body.style.cssText = "";
      const rows = Array.from(body.children).filter(el => el instanceof HTMLElement);
      const fields = rows[0];
      const actions = rows[1];
      if (fields instanceof HTMLElement) {
        fields.classList.add("cw-import-fields");
        fields.style.cssText = "";
      }
      if (actions instanceof HTMLElement) {
        actions.classList.add("cw-import-actions");
        actions.style.cssText = "";
      }
    }

    const wrapField = (el, label) => {
      if (!(el instanceof HTMLElement) || el.parentElement?.classList.contains("cw-import-field")) return;
      const field = document.createElement("label");
      field.className = "cw-import-field";
      const text = document.createElement("span");
      text.className = "cw-import-field-label";
      text.textContent = label;
      el.parentNode.insertBefore(field, el);
      field.append(text, el);
    };

    wrapField(ctx.importProviderSel, "Provider");
    wrapField(ctx.importInstanceSel, "Profile");
    wrapField(ctx.importModeSel, "Mode");

    const featureWrap = document.createElement("div");
    featureWrap.className = "cw-import-features";
    [ctx.importWatchlistWrap, ctx.importHistoryWrap, ctx.importRatingsWrap, ctx.importProgressFeatWrap].forEach(wrap => {
      if (!(wrap instanceof HTMLElement)) return;
      wrap.classList.add("cw-import-feature");
      wrap.style.cssText = "";
      featureWrap.append(wrap);
    });

    const runRow = document.createElement("div");
    runRow.className = "cw-import-run-row";
    if (ctx.importRunBtn) runRow.append(ctx.importRunBtn);

    const actions = details.querySelector(".cw-import-actions");
    if (actions) {
      actions.textContent = "";
      actions.append(featureWrap, runRow);
    }

    if (ctx.importProgressWrap) ctx.importProgressWrap.classList.add("cw-import-progress-row");
  }

  function decoratePolicyBackupPanel(ctx = {}) {
    const { stateBackupCard, importRow, stateDownloadBtn, stateUploadBtn, stateUploadInput } = ctx;
    if (!stateBackupCard || stateBackupCard.dataset.decorated === "1") return;
    stateBackupCard.dataset.decorated = "1";
    stateBackupCard.className = "ins-row cw-policy-row";
    if (importRow && importRow.parentNode && stateBackupCard.parentNode !== importRow.parentNode) {
      importRow.insertAdjacentElement("afterend", stateBackupCard);
    }

    const details = document.createElement("details");
    details.id = "cw-policy-details";
    details.className = "cw-collapse cw-policy-panel";
    details.style.width = "100%";

    const summary = document.createElement("summary");
    summary.className = "cw-import-summary";
    summary.innerHTML =
      '<span class="cw-import-title">Policy backup</span>' +
      '<span class="cw-import-help material-symbol" title="Exports or imports the local Current State policy JSON used by manual baseline edits and block rules. It does not change provider accounts." aria-label="Policy backup help">help</span>';

    const body = document.createElement("div");
    body.className = "cw-policy-body";

    const copy = document.createElement("div");
    copy.className = "cw-policy-copy";
    copy.textContent = "Export or import the local Current State policy as JSON.";

    const actions = document.createElement("div");
    actions.className = "cw-policy-actions";

    const exportAction = document.createElement("div");
    exportAction.className = "cw-policy-action";
    exportAction.innerHTML = "<span>Export</span>";
    if (stateDownloadBtn) exportAction.append(stateDownloadBtn);

    const importAction = document.createElement("div");
    importAction.className = "cw-policy-action";
    importAction.innerHTML = "<span>Import</span>";
    if (stateUploadBtn) importAction.append(stateUploadBtn);
    if (stateUploadInput) importAction.append(stateUploadInput);

    actions.append(exportAction, importAction);
    body.append(copy, actions);
    details.append(summary, body);
    stateBackupCard.textContent = "";
    stateBackupCard.append(details);
  }

  function decorateEditorChrome(ctx = {}) {
    const typeIcons = {
      movie: "movie",
      show: "tv",
      anime: "theater_comedy",
      season: "layers",
      episode: "play_circle",
      blocked: "block",
    };
    const setButtonIcon = (btn, icon, label) => {
      if (!btn) return;
      btn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">${icon}</span><span>${label}</span>`;
      btn.setAttribute("aria-label", label);
    };

    setButtonIcon(ctx.addBtn, "add", "Add row");
    setButtonIcon(ctx.saveBtn, "check", "Save changes");

    const typeFilterWrap = ctx.typeFilterWrap;
    if (typeFilterWrap && typeFilterWrap.dataset.decorated !== "1") {
      typeFilterWrap.dataset.decorated = "1";
      const field = typeFilterWrap.closest(".ins-kv");
      const label = field?.querySelector(".field-label");
      if (field && label) {
        const shell = document.createElement("div");
        shell.className = "cw-type-filter-shell";
        const head = document.createElement("div");
        head.className = "cw-type-filter-head";
        const title = document.createElement("div");
        title.className = "field-label";
        title.textContent = label.textContent || "Types";
        const clear = document.createElement("button");
        clear.type = "button";
        clear.className = "cw-type-filter-clear";
        clear.innerHTML = 'Clear <span class="material-symbol" aria-hidden="true">filter_alt</span>';
        clear.setAttribute("aria-label", "Clear type filters");
        clear.addEventListener("click", () => {
          ["movie", "show", "anime", "season", "episode"].forEach(t => {
            ctx.state.typeFilter[t] = true;
          });
          ctx.state.blockedOnly = false;
          ctx.syncTypeFilterUI?.();
          ctx.state.page = 0;
          ctx.persistUIState?.();
          ctx.renderRows?.();
        });
        head.append(title, clear);
        label.replaceWith(shell);
        shell.append(head, typeFilterWrap);
      }

      typeFilterWrap.querySelectorAll("button").forEach(btn => {
        const type = btn.dataset.type || (btn.id === "cw-blocked-only" ? "blocked" : "");
        const text = (btn.textContent || "").trim();
        btn.innerHTML =
          `<span class="material-symbol cw-type-icon" aria-hidden="true">${typeIcons[type] || "category"}</span>` +
          `<span>${esc(text)}</span>`;
      });
    }
  }

  Editor.Chrome = {
    wireStaticLabels,
    prepareSourceOptions,
    addTrackerNotice,
    ensureFieldNames,
    decorateImportPanel,
    decoratePolicyBackupPanel,
    decorateEditorChrome,
  };
  window.CrossWatchEditorChrome = Editor.Chrome;
})();
