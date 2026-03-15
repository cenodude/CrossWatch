(function (w, d) {
  w.CW ||= {};

  const FILTER_LABEL_STYLE = "font-size:11px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.68)";
  const FILTER_ACTION_COLS = "minmax(0,1fr) 84px 84px";

  function helpBtnNode(tipId) {
    const btn = d.createElement("button");
    btn.type = "button";
    btn.className = "cx-help material-symbols-rounded";
    btn.setAttribute("data-tip-id", tipId);
    btn.setAttribute("aria-label", "Help");
    btn.textContent = "help";
    return btn;
  }

  function ensureInlineHelp(labelEl, tipId) {
    if (!labelEl || !tipId) return;
    let head = labelEl.closest(".sc-inline-head");
    if (!head) {
      head = d.createElement("div");
      head.className = "sc-inline-head";
      labelEl.parentNode?.insertBefore(head, labelEl);
      head.appendChild(labelEl);
    }
    if (!head.querySelector(`.cx-help[data-tip-id="${tipId}"]`)) {
      head.appendChild(helpBtnNode(tipId));
    }
  }

  const styleFilterLabel = (label, tipId) => {
    if (label) label.style.cssText += `;${FILTER_LABEL_STYLE}`;
    ensureInlineHelp(label, tipId);
  };

  const styleFilterCard = (card, rows) => {
    if (!card) return;
    card.style.display = "grid";
    card.style.gridTemplateRows = rows;
    card.style.alignContent = "stretch";
    card.style.minHeight = "100%";
  };

  const styleFilterActionRow = (row, input) => {
    if (!row || !input) return;
    row.style.display = "grid";
    row.style.gridTemplateColumns = FILTER_ACTION_COLS;
    row.style.alignItems = "center";
    row.style.gap = "8px";
    row.style.alignSelf = "end";
    input.style.width = "100%";
    input.style.flex = "";
  };

  function ensureRowSpacer(row) {
    if (!row) return;
    let spacer = row.querySelector(".sc-filter-input-spacer");
    if (!spacer) {
      spacer = d.createElement("span");
      spacer.className = "sc-filter-input-spacer";
      spacer.setAttribute("aria-hidden", "true");
      row.appendChild(spacer);
    }
    return spacer;
  }

  function ensureCardSpacer(card, cls, height, beforeNode) {
    if (!card) return null;
    let spacer = card.querySelector(`.${cls}`);
    if (!spacer) {
      spacer = d.createElement("div");
      spacer.className = cls;
      spacer.setAttribute("aria-hidden", "true");
      spacer.style.minHeight = height;
      if (beforeNode && beforeNode.parentElement === card) card.insertBefore(spacer, beforeNode);
      else card.appendChild(spacer);
    }
    return spacer;
  }

  function alignRowsToSameTrack(anchorRow, targetRow) {
    if (!anchorRow || !targetRow) return;
    const apply = () => {
      targetRow.style.marginTop = "0";
      const delta = Math.round(anchorRow.getBoundingClientRect().top - targetRow.getBoundingClientRect().top);
      targetRow.style.marginTop = `${Math.max(0, delta)}px`;
      targetRow.style.alignSelf = "end";
    };
    apply();
    w.requestAnimationFrame(apply);
  }

  function enhanceWatcherFiltersUI(root) {
    if (!root) return;
    const filtersBox = root.querySelector("#sc-filters");
    if (!filtersBox) return;
    filtersBox.classList.add("sc-filters-enhanced");

    const topHelpWrap = filtersBox.firstElementChild;
    const routeLabel = root.querySelector("#sc-route-filter-wrap .muted");
    const topHelpBtn = topHelpWrap?.querySelector('.cx-help[data-tip-id="sc-help-watch-filters"]');
    if (routeLabel) ensureInlineHelp(routeLabel, "sc-help-watch-filters");
    if (topHelpBtn && routeLabel) topHelpBtn.remove();
    if (topHelpWrap && !topHelpWrap.querySelector(".cx-help")) topHelpWrap.remove();

    const whitelistLabel = root.querySelector("#sc-whitelist")?.previousElementSibling;
    const uuidLabel = root.querySelector("#sc-uuid-label");
    styleFilterLabel(whitelistLabel, "sc-help-watch-username-whitelist");
    styleFilterLabel(uuidLabel, "sc-help-watch-server-uuid");

    const userInput = root.querySelector("#sc-user-input");
    const userAdd = root.querySelector("#sc-add-user");
    const userPick = root.querySelector("#sc-load-users");
    const userRow = userInput?.parentElement;
    const whitelistCard = userRow?.parentElement || whitelistLabel?.parentElement;
    styleFilterCard(whitelistCard, "auto auto auto 1fr auto");
    if (userRow && userInput && userAdd && userPick) {
      userRow.classList.add("sc-filter-input-row", "sc-filter-input-row--actions");
      styleFilterActionRow(userRow, userInput);
    }

    const uuidInput = root.querySelector("#sc-server-uuid");
    const uuidFetch = root.querySelector("#sc-fetch-uuid");
    const uuidRow = uuidInput?.parentElement;
    const uuidCard = uuidRow?.parentElement || uuidLabel?.parentElement;
    styleFilterCard(uuidCard, "auto auto 1fr auto");
    if (uuidRow && uuidInput && uuidFetch) {
      uuidRow.classList.add("sc-filter-input-row", "sc-filter-input-row--actions", "sc-filter-input-row--fetch");
      styleFilterActionRow(uuidRow, uuidInput);
      ensureRowSpacer(uuidRow);
      ensureCardSpacer(uuidCard, "sc-filter-grow", "1px", uuidRow);
    }
  }

  function enhanceWatcherAdvancedUI(root) {
    if (!root) return;
    const advancedBox = root.querySelector("#sc-advanced");
    if (!advancedBox) return;

    const body = advancedBox.querySelector(".body");
    if (!body) return;
    Array.from(advancedBox.children).forEach((child) => {
      if (child !== body && child.querySelector?.('.cx-help[data-tip-id="sc-help-watch-advanced"]')) {
        child.remove();
      }
    });
    body.style.display = "block";

    let header = body.querySelector(".sc-advanced-header");
    if (!header) {
      header = d.createElement("div");
      header.className = "sc-advanced-header";
      body.insertBefore(header, body.firstChild);
    }
    header.style.display = "flex";
    header.style.alignItems = "center";
    header.style.margin = "0 0 16px";

    let titleRow = header.querySelector(".sc-advanced-title");
    if (!titleRow) {
      titleRow = d.createElement("div");
      titleRow.className = "sc-advanced-title muted";
      titleRow.textContent = "Advanced";
      header.appendChild(titleRow);
    }
    ensureInlineHelp(titleRow, "sc-help-watch-advanced");
    const inlineHead = titleRow.closest(".sc-inline-head");
    if (inlineHead) {
      inlineHead.style.display = "inline-flex";
      inlineHead.style.alignItems = "center";
      inlineHead.style.gap = "8px";
      inlineHead.style.flexWrap = "nowrap";
    }

    const grids = Array.from(body.querySelectorAll(".sc-adv-grid"));
    let fieldsWrap = body.querySelector(".sc-advanced-fields");
    if (!fieldsWrap) {
      fieldsWrap = d.createElement("div");
      fieldsWrap.className = "sc-advanced-fields";
      body.insertBefore(fieldsWrap, header.nextSibling);
    }
    fieldsWrap.style.display = "grid";
    fieldsWrap.style.gap = "16px";
    if (grids.length >= 2) {
      const mainGrid = grids[0];
      const progressGrid = grids[1];
      const progressField = progressGrid.querySelector(".field");
      if (mainGrid && progressField) mainGrid.appendChild(progressField);
      if (progressGrid && !progressGrid.children.length) progressGrid.remove();
    }
    Array.from(body.querySelectorAll(".sc-adv-grid")).forEach((grid) => {
      grid.style.display = "grid";
      grid.style.gridTemplateColumns = "repeat(3,minmax(0,1fr))";
      grid.style.gap = "16px";
      if (grid.parentElement !== fieldsWrap) fieldsWrap.appendChild(grid);
    });
    fieldsWrap.querySelectorAll(".field").forEach((field) => {
      field.style.display = "grid";
      field.style.gridTemplateColumns = "minmax(0,1fr) 36px 112px";
      field.style.alignItems = "center";
      field.style.minHeight = "88px";
    });
    fieldsWrap.querySelectorAll(".field .cx-help").forEach((btn) => {
      btn.style.justifySelf = "center";
      btn.style.alignSelf = "center";
      btn.style.transform = "none";
      btn.style.margin = "0";
    });
    fieldsWrap.querySelectorAll(".field input").forEach((input) => {
      input.style.width = "112px";
    });
    const note = body.querySelector(".micro-note");
    if (note) {
      note.classList.add("sc-advanced-note");
      note.style.display = "block";
      note.style.marginTop = "12px";
      if (note.parentElement !== body || note.previousElementSibling !== fieldsWrap) body.appendChild(note);
    }
  }

  function enhanceWebhookFiltersUI(root) {
    if (!root) return;

    const endpointNote = root.querySelector("#sc-endpoint-note");
    if (endpointNote && !String(endpointNote.textContent || "").trim()) {
      endpointNote.hidden = true;
      endpointNote.style.display = "none";
    }

    const whitelistLabel = root.querySelector("#sc-whitelist-webhook")?.previousElementSibling;
    const uuidLabel = root.querySelector("#sc-server-uuid-webhook")?.closest("div")?.previousElementSibling?.previousElementSibling;
    styleFilterLabel(whitelistLabel, "sc-help-watch-username-whitelist");
    styleFilterLabel(uuidLabel, "sc-help-watch-server-uuid");

    const userInput = root.querySelector("#sc-user-input-webhook");
    const userAdd = root.querySelector("#sc-add-user-webhook");
    const userPick = root.querySelector("#sc-load-users-webhook");
    const userRow = userInput?.parentElement;
    const whitelistCard = userRow?.parentElement || whitelistLabel?.parentElement;
    styleFilterCard(whitelistCard, "auto auto auto 1fr auto");
    if (userRow && userInput && userAdd && userPick) {
      styleFilterActionRow(userRow, userInput);
    }

    const uuidInput = root.querySelector("#sc-server-uuid-webhook");
    const uuidFetch = root.querySelector("#sc-fetch-uuid-webhook");
    const uuidRow = uuidInput?.parentElement;
    const uuidNote = root.querySelector("#sc-uuid-note-webhook");
    const uuidCard = uuidRow?.parentElement || uuidLabel?.parentElement;
    styleFilterCard(uuidCard, "auto auto auto 1fr auto");
    ensureCardSpacer(uuidCard, "sc-filter-chips-spacer", "40px", uuidNote || uuidRow);
    if (uuidRow && uuidInput && uuidFetch) {
      styleFilterActionRow(uuidRow, uuidInput);
      ensureRowSpacer(uuidRow);
      ensureCardSpacer(uuidCard, "sc-filter-grow", "1px", uuidRow);
      alignRowsToSameTrack(userRow, uuidRow);
    }
  }

  w.CW.ScrobblerUI = {
    helpBtnNode,
    ensureInlineHelp,
    enhanceWatcherFiltersUI,
    enhanceWatcherAdvancedUI,
    enhanceWebhookFiltersUI,
  };
})(window, document);
