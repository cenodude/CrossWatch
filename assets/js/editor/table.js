/* assets/js/editor/table.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});

  function call(ctx, name, ...args) {
    const fn = ctx && ctx[name];
    return typeof fn === "function" ? fn(...args) : undefined;
  }

  function cell(inner) {
    const td = document.createElement("td");
    td.appendChild(inner);
    return td;
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
    const delTd = cell(delBtn);
    delTd.className = "cw-action-cell";
    if (ctx.wideActions) delTd.classList.add("cw-action-wide");

    if (call(ctx, "canReplaceRow", row)) {
      const t = String(call(ctx, "rowType", row) || "").toLowerCase();
      const repBtn = document.createElement("button");
      repBtn.type = "button";
      repBtn.className = "cw-btn cw-btn-del";
      repBtn.innerHTML = '<span class="material-symbol">published_with_changes</span>';
      repBtn.title = t === "episode" ? "Replace episode" : t === "season" ? "Replace season" : "Replace item";
      repBtn.style.marginLeft = "4px";
      repBtn.onclick = () => call(ctx, "openItemReplacer", row, repBtn);
      delTd.appendChild(repBtn);
    }

    if (blockMode) {
      const rawBtn = document.createElement("button");
      rawBtn.type = "button";
      rawBtn.className = "cw-btn cw-btn-del";
      rawBtn.innerHTML = '<span class="material-symbol">data_object</span>';
      rawBtn.title = "Advanced fields";
      rawBtn.setAttribute("aria-label", "Advanced fields");
      rawBtn.style.marginLeft = "4px";
      rawBtn.onclick = () => call(ctx, "openRawFieldsModal", row);
      delTd.appendChild(rawBtn);
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
    tr.appendChild(cell(keyIn));

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
    tr.appendChild(cell(typeBtn));

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

    const imdbIn = document.createElement("input");
    imdbIn.name = fieldName("imdb");
    imdbIn.value = row.imdb || "";
    imdbIn.disabled = locked;
    imdbIn.oninput = e => {
      row.imdb = e.target.value;
      row.raw.ids = row.raw.ids || {};
      if (e.target.value) row.raw.ids.imdb = e.target.value;
      else delete row.raw.ids.imdb;
      call(ctx, "markChanged");
    };

    const idAIn = document.createElement("input");
    idAIn.name = fieldName(anilistMode ? "mal" : "tmdb");
    idAIn.value = anilistMode ? (row.mal || "") : (row.tmdb || "");
    idAIn.placeholder = anilistMode ? "MAL..." : "TMDB...";
    idAIn.disabled = locked;
    idAIn.oninput = e => {
      const v = e.target.value;
      row.raw.ids = row.raw.ids || {};
      if (anilistMode) {
        row.mal = v;
        if (v) row.raw.ids.mal = v;
        else delete row.raw.ids.mal;
      } else {
        row.tmdb = v;
        if (v) row.raw.ids.tmdb = v;
        else delete row.raw.ids.tmdb;
      }
      call(ctx, "markChanged");
    };

    const idBIn = document.createElement("input");
    idBIn.name = fieldName(anilistMode ? "anilist" : "trakt");
    idBIn.value = anilistMode ? (row.anilist || "") : (row.trakt || "");
    idBIn.placeholder = anilistMode ? "AniList..." : "Trakt...";
    idBIn.disabled = locked;
    idBIn.oninput = e => {
      const v = e.target.value;
      row.raw.ids = row.raw.ids || {};
      if (anilistMode) {
        row.anilist = v;
        if (v) row.raw.ids.anilist = v;
        else delete row.raw.ids.anilist;
      } else {
        row.trakt = v;
        if (v) row.raw.ids.trakt = v;
        else delete row.raw.ids.trakt;
      }
      call(ctx, "markChanged");
    };

    const searchBtn = document.createElement("button");
    searchBtn.type = "button";
    searchBtn.className = "cw-title-search-btn";
    searchBtn.innerHTML = '<span class="material-symbol">search</span>';
    const searchUsesCorrection = locked && call(ctx, "canReplaceRow", row);
    searchBtn.title = searchUsesCorrection
      ? (call(ctx, "usesCoordinateReplacer", row) ? "Replace episode" : "Search and add correction")
      : "Search and fill IDs";
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
        traktIn: null,
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
    if (call(ctx, "isTrackerSource") && row._origin !== "baseline") {
      const origin = document.createElement("div");
      origin.className = "cw-title-sub";
      origin.textContent = "Manual correction";
      titleCell.appendChild(origin);
    }
    tr.appendChild(cell(titleCell));

    const yearTd = cell(yearIn);
    yearTd.className = "cw-col-year";
    tr.appendChild(yearTd);
    tr.appendChild(cell(idAIn));

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
    } else if (state.kind === "progress") {
      extraBtn.onclick = () => call(ctx, "openProgressEditor", row, extraBtn, extraBtn);
    }
    tr.appendChild(cell(extraBtn));

    return tr;
  }

  Editor.Table = {
    createRowElement,
  };
  window.CrossWatchEditorTable = Editor.Table;
})();
