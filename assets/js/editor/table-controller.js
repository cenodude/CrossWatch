/* assets/js/editor/table-controller.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});

  function compareValues(aVal, bVal) {
    if (typeof aVal === "number" && typeof bVal === "number") {
      if (aVal < bVal) return -1;
      if (aVal > bVal) return 1;
      return 0;
    }
    const aStr = aVal == null ? "" : String(aVal).toLowerCase();
    const bStr = bVal == null ? "" : String(bVal).toLowerCase();
    if (aStr < bStr) return -1;
    if (aStr > bStr) return 1;
    return 0;
  }

  function applyFilter(rows, ctx = {}) {
    const state = ctx.state;
    const rawQ = (state.filter || "").trim();
    const q = ctx.normalizeSearchText(rawQ);
    const queryCoord = ctx.parseSeasonEpisodeText(rawQ);
    const queryTokens = q ? q.split(" ").filter(Boolean) : [];
    const filters = state.typeFilter || {};
    const hasTypeFilter = filters.movie || filters.show || filters.anime || filters.season || filters.episode;

    return (rows || []).filter(r => {
      if (hasTypeFilter) {
        const t = (r.type || "").toLowerCase();
        const known = t === "movie" || t === "show" || t === "anime" || t === "season" || t === "episode";
        let allowed = true;
        if (known) {
          if (t === "movie") allowed = !!filters.movie;
          else if (t === "show") allowed = !!filters.show;
          else if (t === "anime") allowed = !!filters.anime;
          else if (t === "season") allowed = !!filters.season;
          else if (t === "episode") allowed = !!filters.episode;
        }
        if (!allowed) return false;
      }

      if (state.blockedOnly && ctx.isPolicySource()) {
        if (!(r.deleted && r._origin === "baseline")) return false;
      }

      if (!q) return true;

      const rowCoord = ctx.rowSeasonEpisode(r);
      if (queryCoord.season != null && rowCoord.season !== queryCoord.season) return false;
      if (queryCoord.episode != null && rowCoord.episode !== queryCoord.episode) return false;

      const haystack = ctx.rowSearchHaystack(r);
      return haystack.includes(q) || queryTokens.every(token => haystack.includes(token));
    });
  }

  function sortRows(rows, ctx = {}) {
    const state = ctx.state;
    const key = state.sortKey;
    const dir = state.sortDir === "desc" ? -1 : 1;
    if (!key) return rows;
    return (rows || []).slice().sort((a, b) => {
      let av;
      let bv;
      if (key === "title") {
        av = a.title || "";
        bv = b.title || "";
      } else if (key === "type") {
        av = a.type || "";
        bv = b.type || "";
      } else if (key === "key") {
        av = a.key || "";
        bv = b.key || "";
      } else if (key === "year") {
        av = a.year ? Number(a.year) || String(a.year) : "";
        bv = b.year ? Number(b.year) || String(b.year) : "";
      } else if (key === "id") {
        av = ctx.isAnilistMode?.() ? a.mal || "" : a.tmdb || "";
        bv = ctx.isAnilistMode?.() ? b.mal || "" : b.tmdb || "";
      } else if (["imdb", "tvdb", "trakt", "simkl", "anilist"].includes(key)) {
        av = a[key] || "";
        bv = b[key] || "";
      } else if (key === "extra") {
        if (state.kind === "ratings") {
          av = a.raw && a.raw.rating != null ? Number(a.raw.rating) : -Infinity;
          bv = b.raw && b.raw.rating != null ? Number(b.raw.rating) : -Infinity;
        } else if (state.kind === "history") {
          const aw = a.raw && a.raw.watched_at;
          const bw = b.raw && b.raw.watched_at;
          av = aw ? Date.parse(aw) || 0 : 0;
          bv = bw ? Date.parse(bw) || 0 : 0;
        } else {
          av = "";
          bv = "";
        }
      } else {
        av = "";
        bv = "";
      }
      return compareValues(av, bv) * dir;
    });
  }

  function updateSortUI(ctx = {}) {
    const state = ctx.state;
    ctx.sortHeaders.forEach(th => {
      const k = th.dataset.sort;
      th.classList.remove("sort-asc", "sort-desc");
      if (k === state.sortKey) th.classList.add(state.sortDir === "desc" ? "sort-desc" : "sort-asc");
    });
  }

  function tableRowContext(ctx = {}, anilistMode, wideActions) {
    return {
      state: ctx.state,
      anilistMode,
      wideActions,
      isPolicySource: ctx.isPolicySource,
      isTrackerSource: ctx.isTrackerSource,
      isRowLocked: ctx.isRowLocked,
      isExtraKindEditable: ctx.isExtraKindEditable,
      canReplaceRow: ctx.canReplaceRow,
      usesCoordinateReplacer: ctx.usesCoordinateReplacer,
      rowType: ctx.rowType,
      markChanged: ctx.markChanged,
      renderRows: ctx.renderRows,
      syncBulkBar: ctx.syncBulkBar,
      syncSelectPageCheckbox: ctx.syncSelectPageCheckbox,
      updateTypeDisplay: ctx.updateTypeDisplay,
      updateExtraDisplay: ctx.updateExtraDisplay,
      formatEpisodeVisualTitle: ctx.formatEpisodeVisualTitle,
      formatSxxEyy: ctx.formatSxxEyy,
      openTypeEditor: ctx.openTypeEditor,
      openTitleSearchEditor: ctx.openTitleSearchEditor,
      openItemReplacer: ctx.openItemReplacer,
      openRawFieldsModal: ctx.openRawFieldsModal,
      openRatingEditor: ctx.openRatingEditor,
      openHistoryEditor: ctx.openHistoryEditor,
      openProgressEditor: ctx.openProgressEditor,
    };
  }

  function renderRows(ctx = {}) {
    const state = ctx.state;
    ctx.closePopup();
    ctx.syncColumnVisibilityUI?.();
    updateSortUI(ctx);
    ctx.syncIdColumnHeaders();

    let filtered = applyFilter(state.rows || [], ctx);
    const totalFiltered = filtered.length;
    const totalAll = (state.rows || []).length;
    ctx.syncHeaderPills(totalFiltered, totalAll);

    filtered = sortRows(filtered, ctx);

    let movies = 0;
    let shows = 0;
    let seasons = 0;
    let episodes = 0;
    for (const row of state.rows || []) {
      const t = (row.type || "").toLowerCase();
      if (t === "movie") movies += 1;
      else if (t === "show") shows += 1;
      else if (t === "season") seasons += 1;
      else if (t === "episode") episodes += 1;
    }
    if (ctx.summaryMovies) ctx.summaryMovies.textContent = String(movies);
    if (ctx.summaryShows) ctx.summaryShows.textContent = String(shows);
    if (ctx.summarySeasons) ctx.summarySeasons.textContent = String(seasons);
    if (ctx.summaryEpisodes) ctx.summaryEpisodes.textContent = String(episodes);

    if (ctx.tbody) ctx.tbody.innerHTML = "";

    const actionHead = ctx.host.querySelector(".cw-action-head");
    const wideActions = !!ctx.isPolicySource();
    if (actionHead) {
      actionHead.classList.toggle("cw-action-wide", wideActions);
      actionHead.style.width = wideActions ? "84px" : "46px";
      actionHead.style.minWidth = actionHead.style.width;
    }

    if (!totalFiltered) {
      if (ctx.empty) ctx.empty.style.display = "block";
      if (ctx.empty) {
        const main = ctx.empty.closest(".cw-main");
        if (main) main.classList.add("cw-main-empty");
      }
      if (ctx.pager) ctx.pager.style.display = "none";
      if (ctx.summaryVisible) ctx.summaryVisible.textContent = "0";
      if (ctx.summaryTotal) ctx.summaryTotal.textContent = String(totalAll || 0);
      ctx.setStatus("0 rows visible");
      state.pageRids = [];
      ctx.syncSelectPageCheckbox();
      ctx.syncBulkBar();
      if (ctx.pageInfo) ctx.pageInfo.textContent = "";
      return;
    }

    if (ctx.empty) ctx.empty.style.display = "none";
    if (ctx.empty) {
      const main = ctx.empty.closest(".cw-main");
      if (main) main.classList.remove("cw-main-empty");
    }

    const pageSize = ctx.pageSize || 50;
    const pageCount = Math.max(1, Math.ceil(totalFiltered / pageSize));
    if (state.page >= pageCount) state.page = pageCount - 1;
    if (state.page < 0) state.page = 0;

    const start = state.page * pageSize;
    const end = start + pageSize;
    const rows = filtered.slice(start, end);

    state.pageRids = rows.map(r => r._rid);
    ctx.syncSelectPageCheckbox();
    ctx.syncBulkBar();

    const frag = document.createDocumentFragment();
    const rowCtx = tableRowContext(ctx, ctx.isAnilistMode(), wideActions);
    rows.forEach(row => {
      const tr = ctx.editorTable.createRowElement(row, rowCtx);
      if (tr) frag.appendChild(tr);
    });
    if (ctx.tbody) ctx.tbody.appendChild(frag);
    ctx.applyColumnWidths?.();

    const vis = rows.length;
    const first = start + 1;
    const last = start + vis;

    if (ctx.summaryVisible) ctx.summaryVisible.textContent = String(vis);
    if (ctx.summaryTotal) ctx.summaryTotal.textContent = String(totalAll);

    if (ctx.pageInfo) ctx.pageInfo.textContent = `Page ${state.page + 1} of ${pageCount} • Rows ${first}-${last} of ${totalFiltered}`;
    if (ctx.pager) ctx.pager.style.display = pageCount > 1 ? "flex" : "none";
    if (ctx.prevBtn) ctx.prevBtn.disabled = state.page <= 0;
    if (ctx.nextBtn) ctx.nextBtn.disabled = state.page >= pageCount - 1;

    if (totalFiltered > vis) {
      ctx.setRowsStatus(`${vis} rows visible (rows ${first}-${last} of ${totalFiltered} filtered, ${totalAll} total)`);
    } else {
      ctx.setRowsStatus(`${vis} rows visible, ${totalAll} total`);
    }
  }

  Editor.TableController = { applyFilter, sortRows, updateSortUI, renderRows };
  window.CrossWatchEditorTableController = Editor.TableController;
})();
