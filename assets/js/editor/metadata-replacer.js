/* assets/js/editor/metadata-replacer.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});

  const REPLACEABLE_TYPES = ["movie", "show", "anime", "season", "episode"];
  const EPISODE_ID_FIELDS = [
    { key: "tvdb", label: "TVDB episode ID" },
    { key: "tmdb", label: "TMDB episode ID" },
    { key: "simkl", label: "SIMKL episode ID" },
  ];
  const PROVIDER_HISTORY_ID_FIELDS = [
    "_trakt_history_id",
    "history_id",
    "_simkl_history_id",
    "_plex_history_id",
    "watched_id",
    "play_id",
  ];

  function rowType(row) {
    return String((row && row.type) || "").toLowerCase();
  }

  function canReplaceRow(row, ctx = {}) {
    if (typeof ctx.isPolicySource === "function" && !ctx.isPolicySource()) return false;
    return REPLACEABLE_TYPES.includes(rowType(row));
  }

  function usesCoordinateReplacer(row) {
    const t = rowType(row);
    return t === "episode" || t === "season";
  }

  function coordinateKeyFor(row, season, episode) {
    const base = String((row && row.key) || "").split("#")[0].trim();
    if (!base) return "";
    if (rowType(row) === "season") return `${base}#season:${season}`;
    const s = String(Math.max(0, season)).padStart(2, "0");
    const e = String(Math.max(0, episode)).padStart(2, "0");
    return `${base}#s${s}e${e}`;
  }

  function carryOverFields(row, drop) {
    const src = (row && row.raw) || {};
    const out = {};
    for (const [k, v] of Object.entries(src)) {
      if (drop.includes(k) || PROVIDER_HISTORY_ID_FIELDS.includes(k)) continue;
      out[k] = JSON.parse(JSON.stringify(v === undefined ? null : v));
    }
    return out;
  }

  function correctedEpisodeItem(row, season, episode, watchedAt, extraIds) {
    const isSeason = rowType(row) === "season";
    const out = carryOverFields(row, ["ids", "season", "episode", "title"]);
    out.type = isSeason ? "season" : "episode";
    out.season = season;
    if (isSeason) {
      out.title = `Season ${season}`;
    } else {
      out.episode = episode;
      out.title = `S${String(season).padStart(2, "0")}E${String(episode).padStart(2, "0")}`;
      out.watched_at = watchedAt || null;
    }
    const ids = {};
    for (const [k, v] of Object.entries(extraIds || {})) {
      const val = String(v || "").trim();
      if (val) ids[k] = val;
    }
    out.ids = ids;
    return out;
  }

  function correctedMetadataItem(row, draft) {
    const out = carryOverFields(row, [
      "ids", "title", "year", "type",
      "season", "episode", "series_title", "series_year", "show_ids",
    ]);
    const picked = (draft && draft.raw) || {};
    out.type = picked.type || draft.type || "movie";
    out.title = picked.title || draft.title || null;
    out.year = picked.year != null ? picked.year : null;
    out.ids = { ...(picked.ids || {}) };
    return out;
  }

  function openItemReplacer(row, anchor, ctx = {}) {
    if (usesCoordinateReplacer(row)) return openEpisodeReplacer(row, anchor, ctx);
    return openMetadataReplacer(row, anchor, ctx);
  }

  function openMetadataReplacer(row, anchor, ctx = {}) {
    const draft = {
      _rid: -1,
      key: String(row.key || ""),
      type: rowType(row),
      title: String(row.title || ""),
      year: String(row.year || ""),
      imdb: "",
      tmdb: "",
      tvdb: "",
      trakt: "",
      simkl: "",
      mal: "",
      anilist: "",
      raw: JSON.parse(JSON.stringify(row.raw || {})),
      deleted: false,
      episode: false,
    };
    const stub = () => document.createElement("input");
    const refs = {
      keyIn: stub(),
      titleIn: stub(),
      yearIn: stub(),
      imdbIn: stub(),
      tmdbIn: stub(),
      tvdbIn: stub(),
      traktIn: stub(),
      simklIn: stub(),
      typeBtn: document.createElement("button"),
      onApplied: applied => {
        const key = String(applied.key || "").trim();
        if (!key) {
          ctx.setStatusSticky?.("That result has no usable identifier.", 5000);
          return;
        }
        if (key.toLowerCase() === String(row.key || "").trim().toLowerCase()) {
          ctx.setStatusSticky?.("The corrected item is the same as the current one.", 5000);
          return;
        }
        const corrected = correctedMetadataItem(row, applied);
        const err = ctx.commitReplacement?.(row, corrected, key, "The corrected local item was updated.");
        if (err) ctx.setStatusSticky?.(err, 5000);
      },
    };
    openTitleSearchEditor(draft, anchor, refs, ctx);
  }

  function openEpisodeReplacer(row, anchor, ctx = {}) {
    const isSeason = rowType(row) === "season";
    const dt = window.CW && window.CW.Editor && window.CW.Editor.DateTime;
    if (!dt) throw new Error("Editor module missing: DateTime");
    ctx.openPopup(anchor, (pop, close) => {
      ctx.appendPopupTitle(pop, isSeason ? "Replace season" : "Replace episode");

      const raw = (row && row.raw) || {};
      const curSeason = Number(raw.season || 0);
      const curEpisode = Number(raw.episode || 0);
      const seriesTitle = String(raw.series_title || row.title || "").trim() || "Unknown series";

      const info = document.createElement("div");
      info.className = "cw-search-meta";
      info.textContent = isSeason
        ? `${seriesTitle} - currently season ${curSeason}`
        : `${seriesTitle} - currently S${String(curSeason).padStart(2, "0")}E${String(curEpisode).padStart(2, "0")}`;
      pop.appendChild(info);

      ctx.appendPopupTitle(pop, isSeason ? "Corrected season" : "Corrected episode", "10px");

      const grid = document.createElement("div");
      grid.className = "cw-datetime-grid";
      grid.style.gridTemplateColumns = isSeason ? "minmax(0,1fr)" : "minmax(0,1fr) minmax(0,1fr)";

      const seasonIn = document.createElement("input");
      seasonIn.type = "number";
      seasonIn.min = "0";
      seasonIn.placeholder = "Season";
      seasonIn.value = String(curSeason);

      const episodeIn = document.createElement("input");
      episodeIn.type = "number";
      episodeIn.min = "0";
      episodeIn.placeholder = "Episode";
      episodeIn.value = String(curEpisode);

      grid.appendChild(seasonIn);
      if (!isSeason) grid.appendChild(episodeIn);
      pop.appendChild(grid);

      const dateInput = document.createElement("input");
      dateInput.type = "date";
      const timeInput = document.createElement("input");
      timeInput.type = "time";
      timeInput.step = 60;

      if (!isSeason) {
        ctx.appendPopupTitle(pop, "Watched at", "10px");
        const whenGrid = document.createElement("div");
        whenGrid.className = "cw-datetime-grid";
        dt.fillDateTimeInputs?.(raw.watched_at, dateInput, timeInput);
        whenGrid.appendChild(dateInput);
        whenGrid.appendChild(timeInput);
        pop.appendChild(whenGrid);
      }

      const advanced = document.createElement("details");
      advanced.className = "cw-collapse";
      advanced.style.marginTop = "10px";
      const summary = document.createElement("summary");
      summary.style.cursor = "pointer";
      summary.style.fontWeight = "700";
      summary.style.userSelect = "none";
      summary.textContent = "Advanced";
      advanced.appendChild(summary);

      const advGrid = document.createElement("div");
      advGrid.className = "cw-datetime-grid";
      advGrid.style.marginTop = "8px";
      const idInputs = {};
      EPISODE_ID_FIELDS.forEach(field => {
        const input = document.createElement("input");
        input.type = "text";
        input.placeholder = field.label;
        idInputs[field.key] = input;
        advGrid.appendChild(input);
      });
      advanced.appendChild(advGrid);
      pop.appendChild(advanced);

      const status = document.createElement("div");
      status.className = "cw-search-status";
      status.style.marginTop = "8px";
      pop.appendChild(status);

      const apply = () => {
        const season = parseInt(seasonIn.value, 10);
        const episode = isSeason ? 0 : parseInt(episodeIn.value, 10);
        const seasonOk = Number.isFinite(season) && season >= 0;
        const episodeOk = isSeason || (Number.isFinite(episode) && episode > 0);
        if (!seasonOk || !episodeOk) {
          status.textContent = isSeason
            ? "Enter a valid corrected season."
            : "Enter a valid corrected season and episode.";
          return;
        }
        if (season === curSeason && (isSeason || episode === curEpisode)) {
          status.textContent = isSeason
            ? "The corrected season is the same as the current one."
            : "The corrected episode is the same as the current one.";
          return;
        }
        const key = coordinateKeyFor(row, season, episode);
        if (!key) {
          status.textContent = "This row has no usable series identifier.";
          return;
        }

        const watchedAt = dt.dateTimeInputsToIso?.(dateInput.value, timeInput.value) || raw.watched_at || null;
        const extraIds = {};
        EPISODE_ID_FIELDS.forEach(field => {
          extraIds[field.key] = idInputs[field.key].value;
        });
        const corrected = correctedEpisodeItem(row, season, episode, watchedAt, extraIds);
        const err = ctx.commitReplacement?.(
          row,
          corrected,
          key,
          isSeason ? "The corrected local season was updated." : "The corrected local episode was updated."
        );
        if (err) {
          status.textContent = err;
          return;
        }
        close();
      };

      ctx.appendPopupActions(pop, [
        { label: "Close", kind: "ghost", onClick: close },
        { label: "Replace episode", kind: "primary", onClick: apply },
      ]);

      seasonIn.focus();
    });
  }

  function openTitleSearchEditor(row, anchor, refs, ctx = {}) {
    ctx.openPopup(anchor, (pop, close) => {
      const title = document.createElement("div");
      title.className = "cw-pop-title";
      title.textContent = "Search metadata";
      pop.appendChild(title);

      const bar = document.createElement("div");
      bar.className = "cw-search-bar";

      const qInput = document.createElement("input");
      qInput.type = "text";
      qInput.id = "cw_meta_search_title";
      qInput.name = qInput.id;
      qInput.placeholder = "Title...";
      qInput.value = row.title || "";
      bar.appendChild(qInput);

      const yearInput = document.createElement("input");
      yearInput.type = "number";
      yearInput.id = "cw_meta_search_year";
      yearInput.name = yearInput.id;
      yearInput.placeholder = "Year";
      if (row.year) yearInput.value = row.year;
      bar.appendChild(yearInput);

      const typeSelect = document.createElement("select");
      typeSelect.id = "cw_meta_search_type";
      typeSelect.name = typeSelect.id;
      [["movie", "Movie"], ["show", "Show"], ["anime", "Anime"]].forEach(([val, label]) => {
        const opt = document.createElement("option");
        opt.value = val;
        opt.textContent = label;
        typeSelect.appendChild(opt);
      });
      typeSelect.value = row.type === "anime" ? "anime" : row.type === "show" || row.type === "episode" ? "show" : "movie";
      bar.appendChild(typeSelect);

      pop.appendChild(bar);

      const actions = document.createElement("div");
      actions.className = "cw-pop-actions";

      const searchBtn = document.createElement("button");
      searchBtn.type = "button";
      searchBtn.className = "cw-pop-btn primary";
      searchBtn.textContent = "Search";
      actions.appendChild(searchBtn);

      const closeBtn = document.createElement("button");
      closeBtn.type = "button";
      closeBtn.className = "cw-pop-btn ghost";
      closeBtn.textContent = "Close";
      closeBtn.onclick = close;
      actions.appendChild(closeBtn);

      pop.appendChild(actions);

      const status = document.createElement("div");
      status.className = "cw-search-status";
      pop.appendChild(status);

      const resultsBox = document.createElement("div");
      resultsBox.className = "cw-search-results";
      pop.appendChild(resultsBox);

      async function doSearch() {
        const q = (qInput.value || "").trim();
        const yearVal = parseInt(yearInput.value || "", 10);
        if (q.length < 2) {
          status.textContent = "Type at least 2 characters.";
          resultsBox.innerHTML = "";
          return;
        }
        const typ = String(typeSelect.value || "").toLowerCase();
        const makeUrl = t => {
          let u = `/api/metadata/search?q=${encodeURIComponent(q)}&typ=${encodeURIComponent(t)}`;
          if (!Number.isNaN(yearVal)) u += `&year=${yearVal}`;
          return u;
        };

        status.textContent = "Searching...";
        resultsBox.innerHTML = "";
        try {
          let items = [];
          if (typ === "anime") {
            const [showRes, movieRes] = await Promise.all([ctx.fetchJSON(makeUrl("show")), ctx.fetchJSON(makeUrl("movie"))]);

            const showOk = !!(showRes && showRes.ok !== false);
            const movieOk = !!(movieRes && movieRes.ok !== false);

            if (!showOk && !movieOk) {
              const msg = (showRes && showRes.error) || (movieRes && movieRes.error) || "Search failed.";
              status.textContent = msg;
              return;
            }

            const a = showOk && Array.isArray(showRes.results) ? showRes.results : [];
            const b = movieOk && Array.isArray(movieRes.results) ? movieRes.results : [];

            items = [...a.map(x => ({ ...x, _resolve_entity: "show" })), ...b.map(x => ({ ...x, _resolve_entity: "movie" }))];

            const seen = new Set();
            items = items.filter(it => {
              const k = `${String(it.tmdb || "")}:${String(it.type || "")}`;
              if (!k || seen.has(k)) return false;
              seen.add(k);
              return true;
            });
          } else {
            const data = await ctx.fetchJSON(makeUrl(typ));
            if (!data || data.ok === false) {
              status.textContent = data && data.error ? data.error : "Search failed.";
              return;
            }
            items = Array.isArray(data.results) ? data.results : [];
          }
          if (!items.length) {
            resultsBox.innerHTML = '<div class="cw-search-empty">No results.</div>';
            status.textContent = "";
            return;
          }

          resultsBox.innerHTML = "";
          items.forEach(item => {
            const btn = document.createElement("button");
            btn.type = "button";
            btn.className = "cw-search-item";

            const posterWrap = document.createElement("div");
            posterWrap.className = "cw-search-poster";

            if (item.poster_path) {
              const img = document.createElement("img");
              img.src = `/art/tmdb/${item.type === "show" ? "tv" : "movie"}/${encodeURIComponent(String(item.tmdb))}?size=w92`;
              img.alt = "";
              img.onerror = () => {
                img.remove();
                const ph = document.createElement("div");
                ph.className = "cw-search-poster-placeholder";
                ph.textContent = item.type === "show" ? "TV" : "MOV";
                posterWrap.appendChild(ph);
              };
              posterWrap.appendChild(img);
            } else {
              const ph = document.createElement("div");
              ph.className = "cw-search-poster-placeholder";
              ph.textContent = item.type === "show" ? "TV" : "MOV";
              posterWrap.appendChild(ph);
            }

            btn.appendChild(posterWrap);

            const content = document.createElement("div");
            content.className = "cw-search-content";

            const titleLine = document.createElement("div");
            titleLine.className = "cw-search-title-line";

            const t = document.createElement("div");
            t.className = "cw-search-title";
            const yearTxt = item.year ? ` (${item.year})` : "";
            t.textContent = (item.title || "") + yearTxt;
            titleLine.appendChild(t);

            const tag2 = document.createElement("span");
            tag2.className = "cw-search-tag";
            tag2.textContent = item.type === "show" ? "Show" : "Movie";
            titleLine.appendChild(tag2);

            content.appendChild(titleLine);

            const meta = document.createElement("div");
            meta.className = "cw-search-meta";
            const bits = [];
            if (item.year) bits.push(String(item.year));
            bits.push(item.type === "show" ? "TV" : "Movie");
            if (item.tmdb) bits.push(`TMDb ${item.tmdb}`);
            meta.textContent = bits.join(" - ");
            content.appendChild(meta);

            if (item.overview) {
              const ov = document.createElement("div");
              ov.className = "cw-search-overview";
              ov.textContent = item.overview;
              content.appendChild(ov);
            }

            btn.appendChild(content);

            btn.onclick = async () => {
              const picked = item;
              const newTitle = picked.title || row.title || "";
              row.title = newTitle;
              row.raw.title = newTitle || null;
              refs.titleIn.value = ctx.formatEpisodeVisualTitle(row) || newTitle;

              if (picked.year) {
                row.year = String(picked.year);
                row.raw.year = picked.year;
                refs.yearIn.value = row.year;
              }

              const wantsAnime = String(typeSelect.value || "").toLowerCase() === "anime";
              const pickedType = String(picked.type || "movie").toLowerCase();

              const newType = wantsAnime ? "anime" : pickedType;
              const resolveEntity = wantsAnime ? picked._resolve_entity || pickedType || "movie" : newType;

              row.type = newType;
              row.raw.type = newType;
              row.episode = false;
              ctx.updateTypeDisplay(row, refs.typeBtn);

              const tmdbId = picked.tmdb;
              if (tmdbId != null) {
                const tmdbStr = String(tmdbId);
                row.tmdb = tmdbStr;
                row.raw.ids = row.raw.ids || {};
                row.raw.ids.tmdb = tmdbId;
                if (refs.tmdbIn) refs.tmdbIn.value = tmdbStr;
                const prevKey = (row.key || "").trim();
                if (!prevKey || /^(tmdb|imdb|trakt|tvdb|slug):/i.test(prevKey)) {
                  row.key = `tmdb:${tmdbStr}`;
                  if (refs.keyIn) refs.keyIn.value = row.key;
                }
              }

              if (tmdbId != null) {
                try {
                  const metaRes = await ctx.fetchJSON("/api/metadata/resolve", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ entity: resolveEntity, ids: { tmdb: tmdbId } }),
                  });

                  if (metaRes && metaRes.ok && metaRes.result && metaRes.result.ids) {
                    const ids = metaRes.result.ids || {};
                    row.raw.ids = row.raw.ids || {};

                    if (ids.imdb) {
                      row.imdb = ids.imdb;
                      row.raw.ids.imdb = ids.imdb;
                      refs.imdbIn.value = ids.imdb;
                    }
                    if (ids.tmdb) {
                      const tVal = String(ids.tmdb);
                      row.tmdb = tVal;
                      row.raw.ids.tmdb = ids.tmdb;
                      if (refs.tmdbIn) refs.tmdbIn.value = tVal;
                      const prevKey = (row.key || "").trim();
                      if (!prevKey || /^(tmdb|imdb|trakt|tvdb|slug):/i.test(prevKey)) {
                        row.key = `tmdb:${tVal}`;
                        if (refs.keyIn) refs.keyIn.value = row.key;
                      }
                    }
                    if (ids.trakt) {
                      const trVal = String(ids.trakt);
                      row.trakt = trVal;
                      row.raw.ids.trakt = ids.trakt;
                      if (refs.traktIn) refs.traktIn.value = trVal;
                    }
                    if (ids.tvdb) {
                      const tvdbVal = String(ids.tvdb);
                      row.tvdb = tvdbVal;
                      row.raw.ids.tvdb = ids.tvdb;
                      if (refs.tvdbIn) refs.tvdbIn.value = tvdbVal;
                    }
                    if (ids.simkl) {
                      const simklVal = String(ids.simkl);
                      row.simkl = simklVal;
                      row.raw.ids.simkl = ids.simkl;
                      if (refs.simklIn) refs.simklIn.value = simklVal;
                    }
                  }
                } catch (err) {
                  console.error("metadata resolve failed", err);
                }
              }

              if (typeof refs.onApplied === "function") {
                close();
                refs.onApplied(row);
                return;
              }

              ctx.markChanged?.();
              ctx.setStatusSticky?.("Row updated from metadata", 2500);
              close();
              ctx.renderRows?.();
            };

            resultsBox.appendChild(btn);
          });

          status.textContent = `${items.length} result${items.length === 1 ? "" : "s"} found.`;
        } catch (err) {
          console.error("search failed", err);
          status.textContent = "Search failed.";
        }
      }

      searchBtn.onclick = () => doSearch();

      qInput.addEventListener("keydown", ev => {
        if (ev.key === "Enter") {
          ev.preventDefault();
          doSearch();
        }
      });

      if ((row.title || "").trim().length >= 3) doSearch();
      else status.textContent = "Enter a title and press Enter or Search.";
    });
  }

  Editor.MetadataReplacer = {
    canReplaceRow,
    usesCoordinateReplacer,
    openItemReplacer,
    openTitleSearchEditor,
  };
  window.CrossWatchEditorMetadataReplacer = Editor.MetadataReplacer;
})();
