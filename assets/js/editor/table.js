/* assets/js/editor/table.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});

  function call(ctx, name, ...args) {
    const fn = ctx && ctx[name];
    return typeof fn === "function" ? fn(...args) : undefined;
  }

  function cell(inner, className = "") {
    const td = document.createElement("td");
    if (className) td.className = className;
    td.appendChild(inner);
    return td;
  }

  function dataColumnOrder(ctx = {}) {
    const order = Array.isArray(ctx.state?.columnOrder) ? ctx.state.columnOrder : [];
    const valid = ["key", "type", "title", "year", "id", "imdb", "tvdb", "trakt", "simkl", "anilist", "extra"];
    const seen = new Set();
    const out = [];
    order.forEach(column => {
      if (!valid.includes(column) || seen.has(column)) return;
      seen.add(column);
      out.push(column);
    });
    valid.forEach(column => {
      if (!seen.has(column)) out.push(column);
    });
    return out;
  }

  function createRowElement(row, ctx = {}) {
    const state = ctx.state || {};
    const anilistMode = !!ctx.anilistMode;
    const locked = !!call(ctx, "isRowLocked", row);
    const blockMode = !!call(ctx, "isPolicySource");
    const tr = document.createElement("tr");
    const fieldName = suffix => `cw-row-${row._rid || "new"}-${suffix}`;

    if (row.episode) tr.classList.add("cw-row-episode");
    if (row.deleted) tr.classList.add("cw-row-deleted");

    const selCb = document.createElement("input");
    selCb.type = "checkbox";
    selCb.name = fieldName("selected");
    selCb.className = "cw-checkbox";
    selCb.checked = (state.selected || new Set()).has(row._rid);
    selCb.onchange = () => {
      if (!state.selected) state.selected = new Set();
      if (selCb.checked) state.selected.add(row._rid);
      else state.selected.delete(row._rid);
      call(ctx, "syncBulkBar");
      call(ctx, "syncSelectPageCheckbox");
    };
    tr.appendChild(cell(selCb));

    const baselineRow = blockMode && row._origin === "baseline";
    const delBtn = document.createElement("button");
    delBtn.type = "button";
    delBtn.className = "cw-btn cw-btn-del danger";
    delBtn.innerHTML = `<span class="material-symbol">${blockMode ? "block" : "delete"}</span>`;
    delBtn.title = blockMode
      ? (baselineRow
          ? (row.deleted ? "Restore for future syncs" : "Block from future syncs")
          : (row.deleted ? "Restore row" : "Remove manual correction"))
      : (row.deleted ? "Restore row" : "Delete row");
    delBtn.onclick = () => {
      row.deleted = !row.deleted;
      call(ctx, "markChanged");
      call(ctx, "renderRows");
    };
    const actionWrap = document.createElement("div");
    actionWrap.className = "cw-action-buttons";
    actionWrap.appendChild(delBtn);
    const delTd = cell(actionWrap);
    delTd.className = "cw-action-cell";
    if (ctx.wideActions) delTd.classList.add("cw-action-wide");

    if (blockMode) {
      const rawBtn = document.createElement("button");
      rawBtn.type = "button";
      rawBtn.className = "cw-btn cw-btn-del";
      rawBtn.innerHTML = '<span class="material-symbol">database</span>';
      rawBtn.title = "Record details";
      rawBtn.setAttribute("aria-label", "Record details");
      rawBtn.onclick = () => call(ctx, "openRawFieldsModal", row);
      actionWrap.appendChild(rawBtn);
    }
    tr.appendChild(delTd);

    const keyIn = document.createElement("input");
    keyIn.name = fieldName("key");
    keyIn.value = row.key || "";
    keyIn.className = "cw-key";
    keyIn.disabled = locked;
    keyIn.oninput = e => {
      row.key = e.target.value;
      call(ctx, "markChanged");
    };
    const dataCells = {};
    dataCells.key = cell(keyIn, "cw-col-key");

    const typeBtn = document.createElement("button");
    typeBtn.type = "button";
    typeBtn.className = "cw-extra-display cw-type-display";
    typeBtn.disabled = locked;
    if (locked) {
      typeBtn.style.opacity = "0.6";
      typeBtn.style.cursor = "not-allowed";
    }
    call(ctx, "updateTypeDisplay", row, typeBtn);
    typeBtn.onclick = () => {
      if (typeBtn.disabled) return;
      call(ctx, "openTypeEditor", row, typeBtn);
    };
    dataCells.type = cell(typeBtn, "cw-col-type");

    const titleCell = document.createElement("div");
    titleCell.className = "cw-title-cell";

    const titleRow = document.createElement("div");
    titleRow.className = "cw-title-row";
    titleCell.appendChild(titleRow);

    const titleIn = document.createElement("input");
    titleIn.name = fieldName("title");
    titleIn.value = call(ctx, "formatEpisodeVisualTitle", row) || row.title || "";
    titleIn.disabled = locked;
    titleIn.onfocus = () => {
      const visual = call(ctx, "formatEpisodeVisualTitle", row);
      if (visual) titleIn.value = row.title || "";
    };
    titleIn.oninput = e => {
      row.title = e.target.value;
      row.raw.title = e.target.value || null;
      call(ctx, "markChanged");
    };
    titleIn.onblur = () => {
      const visual = call(ctx, "formatEpisodeVisualTitle", row);
      if (visual) titleIn.value = visual;
    };
    titleRow.appendChild(titleIn);
    if (row._mappingScope) {
      const indicator = document.createElement("span");
      indicator.className = "material-symbols-rounded cw-mapping-indicator";
      indicator.textContent = "link";
      indicator.title = row._mappingScope === "pair" ? "Mapping saved for this pair"
        : state.mappingPair ? "Shared mapping inherited by this pair" : "Shared mapping for all pairs using this provider instance";
      indicator.setAttribute("role", "img");
      indicator.setAttribute("aria-label", indicator.title);
      titleRow.appendChild(indicator);
    }

    const yearIn = document.createElement("input");
    yearIn.name = fieldName("year");
    yearIn.value = row.year || "";
    yearIn.disabled = locked;
    yearIn.oninput = e => {
      row.year = e.target.value;
      const v = e.target.value.trim();
      const n = v ? parseInt(v, 10) : NaN;
      row.raw.year = Number.isFinite(n) ? n : null;
      call(ctx, "markChanged");
    };

    const setIdValue = (idKey, value) => {
      row[idKey] = value;
      row.raw = row.raw || {};
      row.raw.ids = row.raw.ids || {};
      if (value) row.raw.ids[idKey] = value;
      else delete row.raw.ids[idKey];
      call(ctx, "markChanged");
    };

    const makeIdInput = (idKey, label) => {
      const input = document.createElement("input");
      input.name = fieldName(idKey);
      input.value = row[idKey] || "";
      input.placeholder = `${label}...`;
      input.disabled = locked;
      input.oninput = e => setIdValue(idKey, e.target.value);
      return input;
    };

    const imdbIn = makeIdInput("imdb", "IMDb");
    const idAIn = makeIdInput(anilistMode ? "mal" : "tmdb", anilistMode ? "MAL" : "TMDB");
    const tvdbIn = makeIdInput("tvdb", "TVDB");
    const traktIn = makeIdInput("trakt", "Trakt");
    const simklIn = makeIdInput("simkl", "SIMKL");
    const anilistIn = makeIdInput("anilist", "AniList");

    const searchBtn = document.createElement("button");
    searchBtn.type = "button";
    searchBtn.className = "cw-title-search-btn";
    searchBtn.innerHTML = '<span class="material-symbol">search</span>';
    const searchUsesCorrection = !!call(ctx, "canReplaceRow", row);
    searchBtn.title = searchUsesCorrection ? "Edit mapping" : "Search and fill IDs";
    searchBtn.setAttribute("aria-label", searchBtn.title);
    searchBtn.disabled = locked && !searchUsesCorrection;
    if (searchBtn.disabled) {
      searchBtn.style.opacity = "0.6";
      searchBtn.style.cursor = "not-allowed";
    }
    searchBtn.onclick = () => {
      if (searchBtn.disabled) return;
      if (searchUsesCorrection) {
        call(ctx, "openItemReplacer", row, searchBtn);
        return;
      }
      call(ctx, "openTitleSearchEditor", row, searchBtn, {
        keyIn,
        titleIn,
        yearIn,
        imdbIn,
        tmdbIn: anilistMode ? null : idAIn,
        tvdbIn,
        traktIn,
        simklIn,
        typeBtn,
      });
    };
    titleRow.appendChild(searchBtn);

    const subType = (((row.raw && row.raw.type) || row.type || "") + "").toLowerCase();
    if (subType === "season" && row.raw && row.raw.series_title) {
      const sub = document.createElement("div");
      sub.className = "cw-title-sub";
      let label = row.raw.series_title;
      const code = subType === "episode"
        ? call(ctx, "formatSxxEyy", row.raw.season, row.raw.episode)
        : call(ctx, "formatSxxEyy", row.raw.season, null);
      if (code) label += " - " + code;
      sub.textContent = label;
      titleCell.appendChild(sub);
    }
    dataCells.title = cell(titleCell, "cw-col-title");

    const yearTd = cell(yearIn);
    yearTd.className = "cw-col-year";
    dataCells.year = yearTd;
    dataCells.id = cell(idAIn, "cw-col-id");
    dataCells.imdb = cell(imdbIn, "cw-col-imdb");
    dataCells.tvdb = cell(tvdbIn, "cw-col-tvdb");
    dataCells.trakt = cell(traktIn, "cw-col-trakt");
    dataCells.simkl = cell(simklIn, "cw-col-simkl");
    dataCells.anilist = cell(anilistIn, "cw-col-anilist");

    const extraBtn = document.createElement("button");
    extraBtn.type = "button";
    extraBtn.className = "cw-extra-display";
    call(ctx, "updateExtraDisplay", row, extraBtn);

    if (!call(ctx, "isExtraKindEditable")) {
      extraBtn.disabled = true;
      extraBtn.style.opacity = "0.6";
      extraBtn.style.cursor = "default";
    } else if (state.kind === "ratings") {
      extraBtn.onclick = () => call(ctx, "openRatingEditor", row, extraBtn, extraBtn);
    } else if (state.kind === "history") {
      extraBtn.onclick = () => call(ctx, "openHistoryEditor", row, extraBtn, extraBtn);
    } else if (state.kind === "collection") {
      extraBtn.onclick = () => call(ctx, "openCollectionEditor", row, extraBtn, extraBtn);
    } else if (state.kind === "progress") {
      extraBtn.onclick = () => call(ctx, "openProgressEditor", row, extraBtn, extraBtn);
    }
    dataCells.extra = cell(extraBtn, "cw-col-extra");

    dataColumnOrder(ctx).forEach(column => {
      const td = dataCells[column];
      if (!td) return;
      td.dataset.column = column;
      td.dataset.label = ({key: 'Key', type: 'Type', title: 'Title', year: 'Year',
        id: anilistMode ? 'AniList' : 'TMDB', imdb: 'IMDb', tvdb: 'TVDB', trakt: 'Trakt',
        simkl: 'SIMKL', anilist: 'AniList', extra: 'Extra'})[column];
      tr.appendChild(td);
    });

    return tr;
  }

  function createViewingGroupElement(unit, ctx = {}, expanded = false) {
    const state = ctx.state || {};
    const rows = (unit && unit.rows) || [];
    const latest = rows[0];
    if (!latest) return null;
    const tr = document.createElement("tr");
    tr.className = "cw-row-viewing-group";
    if (latest.episode) tr.classList.add("cw-row-episode");

    const selected = state.selected || new Set();
    const picked = rows.filter(row => selected.has(row._rid)).length;
    const selCb = document.createElement("input");
    selCb.type = "checkbox";
    selCb.name = `cw-group-${latest._rid || "new"}-selected`;
    selCb.className = "cw-checkbox";
    selCb.checked = picked === rows.length;
    selCb.indeterminate = picked > 0 && picked < rows.length;
    selCb.title = "Select every watch of this title";
    selCb.setAttribute("aria-label", selCb.title);
    selCb.onchange = () => {
      if (!state.selected) state.selected = new Set();
      rows.forEach(row => {
        if (selCb.checked) state.selected.add(row._rid);
        else state.selected.delete(row._rid);
      });
      call(ctx, "syncBulkBar");
      call(ctx, "syncSelectPageCheckbox");
      call(ctx, "renderRows");
    };
    tr.appendChild(cell(selCb));

    const toggle = document.createElement("button");
    toggle.type = "button";
    toggle.className = "cw-btn cw-btn-del cw-viewing-toggle";
    toggle.innerHTML = `<span class="material-symbol">${expanded ? "expand_less" : "expand_more"}</span>`;
    toggle.title = expanded ? "Hide watches" : "Show watches";
    toggle.setAttribute("aria-label", toggle.title);
    toggle.setAttribute("aria-expanded", expanded ? "true" : "false");
    toggle.onclick = () => {
      if (!state.expandedViewings) state.expandedViewings = new Set();
      if (state.expandedViewings.has(unit.base)) state.expandedViewings.delete(unit.base);
      else state.expandedViewings.add(unit.base);
      call(ctx, "renderRows");
    };
    const actionWrap = document.createElement("div");
    actionWrap.className = "cw-action-buttons";
    actionWrap.appendChild(toggle);
    const actionTd = cell(actionWrap, "cw-action-cell");
    if (ctx.wideActions) actionTd.classList.add("cw-action-wide");
    tr.appendChild(actionTd);

    const summary = document.createElement("div");
    summary.className = "cw-viewing-summary";
    const title = document.createElement("strong");
    title.textContent = call(ctx, "formatEpisodeVisualTitle", latest) || latest.title || latest.key || "";
    summary.appendChild(title);
    if (latest.year) {
      const year = document.createElement("span");
      year.className = "cw-viewing-year";
      year.textContent = latest.year;
      summary.appendChild(year);
    }
    const count = document.createElement("span");
    count.className = "cw-viewing-count";
    count.textContent = `${rows.length} watches`;
    summary.appendChild(count);
    const last = document.createElement("span");
    last.className = "cw-viewing-latest";
    call(ctx, "updateExtraDisplay", latest, last);
    summary.appendChild(last);

    const summaryTd = cell(summary, "cw-viewing-summary-cell");
    summaryTd.colSpan = dataColumnOrder(ctx).length;
    tr.appendChild(summaryTd);
    return tr;
  }

  Editor.Table = {
    createRowElement,
    createViewingGroupElement,
  };
  window.CrossWatchEditorTable = Editor.Table;
})();
