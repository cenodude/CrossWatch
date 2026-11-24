/* assets/js/editor/chrome.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;

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
      sourceSelect.querySelector('option[value="manual"]')?.remove();
      sourceSelect.querySelector('option[value="playlist"]')?.remove();
    }

    const sub = root?.querySelector(".cw-sub");
    if (sub) sub.textContent = T("editor.sources.sub");
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
        '<span class="cw-import-title">' + TH("editor.chrome.debug") + '</span>' +
        '<span class="cw-import-help material-symbol" title="' + TH("editor.chrome.debugHelp") + '" aria-label="' + TH("editor.chrome.debugHelpLabel") + '">help</span>';
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

    wrapField(ctx.importProviderSel, T("editor.sources.provider"));
    wrapField(ctx.importInstanceSel, T("editor.chrome.profile"));
    wrapField(ctx.importModeSel, T("editor.chrome.mode"));

    const featureWrap = document.createElement("div");
    featureWrap.className = "cw-import-features";
    [ctx.importWatchlistWrap, ctx.importHistoryWrap, ctx.importRatingsWrap, ctx.importProgressFeatWrap, ctx.importCollectionWrap].forEach(wrap => {
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
      '<span class="cw-import-title">' + TH("editor.chrome.policy") + '</span>' +
      '<span class="cw-import-help material-symbol" title="' + TH("editor.chrome.policyHelp") + '" aria-label="' + TH("editor.chrome.policyHelpLabel") + '">help</span>';

    const body = document.createElement("div");
    body.className = "cw-policy-body";

    const copy = document.createElement("div");
    copy.className = "cw-policy-copy";
    copy.textContent = T("editor.chrome.policyCopy");

    const actions = document.createElement("div");
    actions.className = "cw-policy-actions";

    const exportAction = document.createElement("div");
    exportAction.className = "cw-policy-action";
    exportAction.innerHTML = "<span>" + TH("editor.chrome.export") + "</span>";
    if (stateDownloadBtn) exportAction.append(stateDownloadBtn);

    const importAction = document.createElement("div");
    importAction.className = "cw-policy-action";
    importAction.innerHTML = "<span>" + TH("editor.chrome.import") + "</span>";
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
      btn.title = label;
    };

    setButtonIcon(ctx.addBtn, "add", T("editor.chrome.addRow"));
    setButtonIcon(ctx.saveBtn, "check", T("editor.chrome.save"));
    if (ctx.addBtn && !document.getElementById("cw-saved-mappings")) {
      const button = document.createElement("button");
      button.id = "cw-saved-mappings";
      button.type = "button";
      button.className = "cw-btn";
      setButtonIcon(button, "compare_arrows", T("editor.chrome.mapping"));
      ctx.addBtn.before(button);
      button.addEventListener("click", async () => {
        button.disabled = true;
        try {
          const view = await import(`/assets/js/editor/saved-mappings.js?v=${encodeURIComponent(window.APP_VERSION || "1")}`);
          view.open(button, ctx.mappingEditor);
        } catch { ctx.setStatus?.(T("editor.chrome.openFailed")); }
        finally { button.disabled = false; }
      });
    }

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
        title.textContent = label.textContent || T("editor.chrome.types");
        const clear = document.createElement("button");
        clear.type = "button";
        clear.className = "cw-type-filter-clear";
        clear.innerHTML = TH("editor.rowType.clear") + ' <span class="material-symbol" aria-hidden="true">filter_alt</span>';
        clear.setAttribute("aria-label", T("editor.chrome.clearTypes"));
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

  function prepareMobileTools(root) {
    const toolbar = root.querySelector('.cw-controls');
    if (!toolbar || toolbar.querySelector('.cw-editor-tools-toggle')) return;
    const trigger = document.createElement('button');
    trigger.type = 'button';
    trigger.className = 'cw-btn cw-editor-tools-toggle';
    trigger.setAttribute('aria-label', T("editor.chrome.more"));
    trigger.setAttribute('aria-haspopup', 'dialog');
    trigger.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">more_horiz</span>';
    const panel = document.createElement('div');
    panel.className = 'cw-editor-tools-panel';
    panel.hidden = true;
    toolbar.append(trigger, panel);
    trigger.addEventListener('click', () => {
      panel.replaceChildren();
      const pageSize = root.querySelector('#cw-page-size');
      const label = document.createElement('label');
      label.textContent = T("editor.chrome.rowsPerPage");
      const select = document.createElement('select');
      select.className = 'cw-select';
      select.dataset.cwNativeSelect = 'true';
      select.replaceChildren(...Array.from(pageSize.options, option => new Option(option.text, option.value)));
      select.value = pageSize.value;
      select.addEventListener('change', () => {
        pageSize.value = select.value;
        pageSize.dispatchEvent(new Event('change', {bubbles:true}));
      });
      label.append(select);
      panel.append(label);
      let action;
      for (const [id, title, glyph] of [['cw-advanced-toggle', root.classList.contains('cw-editor-simple') ? T("editor.chrome.advanced") : T("editor.chrome.simple"), 'tune'], ['cw-columns-btn', T("editor.chrome.columns"), 'view_column'], ['cw-saved-mappings', T("editor.chrome.savedMappings"), 'compare_arrows'], ['cw-add', T("editor.chrome.addRow"), 'add']]) {
        const original = root.querySelector(`#${id}`);
        if (!original || original.classList.contains('cw-advanced-only') && root.classList.contains('cw-editor-simple')) continue;
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'cw-btn';
        button.disabled = original.disabled;
        button.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">${glyph}</span><span>${title}</span>`;
        button.addEventListener('click', () => { action = original; panel.closest('dialog')?.close(); });
        panel.append(button);
      }
      window.CW.MobileUI.sheet(panel, {title:T("editor.chrome.options"), trigger, onClose:() => {
        if (action) window.CW.MobileUI.beforeNavigate().then(() => { if (action.isConnected && !action.disabled) action.click(); });
      }});
    });
  }

  function prepareMobileFilters(root) {
    const side = root.querySelector('.cw-side');
    if (!side || side.parentElement.classList.contains('cw-editor-filters')) return;
    const details = document.createElement('details');
    details.className = 'cw-editor-filters';
    const summary = document.createElement('summary');
    summary.textContent = T("editor.chrome.sourceFilters");
    const context = document.createElement('small');
    context.className = 'cw-editor-source-summary';
    summary.append(context);
    details.appendChild(summary);
    side.before(details);
    details.appendChild(side);
    const narrow = window.matchMedia('(max-width: 1320px)');
    const update = () => { details.open = !narrow.matches; };
    update();
    narrow.addEventListener('change', update);
    const updateContext = () => {
      context.textContent = ['#cw-source', '#cw-kind', '#cw-snapshot', '#cw-instance'].map(selector => {
        const select = side.querySelector(selector);
        return select && (selector === '#cw-snapshot' || select.style.display !== 'none') ? select.selectedOptions[0]?.textContent : '';
      }).filter(Boolean).join(' · ');
    };
    side.addEventListener('change', updateContext);
    new MutationObserver(updateContext).observe(side, {childList:true, subtree:true, attributes:true, attributeFilter:['style', 'selected']});
    updateContext();
  }

  Editor.Chrome = {
    prepareMobileTools,
    prepareMobileFilters,
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
