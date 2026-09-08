/* assets/js/editor/metadata-replacer.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});

  const REPLACEABLE_TYPES = ["movie", "show", "anime", "season", "episode"];
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

  function openItemReplacer(row, anchor, ctx = {}) {
    if (canReplaceRow(row, ctx)) return ctx.openMapping?.(row);
  }

  function openTitleSearchEditor(row, anchor, refs, ctx = {}) {
    if (ctx.openMapping && canReplaceRow(row, ctx)) return ctx.openMapping(row);
    ctx.openPopup(anchor, (pop, close) => {
      pop.classList.add("cw-metadata-search-pop");

      const head = document.createElement("div");
      head.className = "cw-meta-search-head";
      head.innerHTML = `
        <span class="material-symbols-rounded cw-meta-search-head-icon" aria-hidden="true">search</span>
        <div class="cw-meta-search-title">Search metadata</div>
      `;
      const headClose = document.createElement("button");
      headClose.type = "button";
      headClose.className = "cw-meta-search-close";
      headClose.setAttribute("aria-label", "Close search metadata");
      headClose.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">close</span>';
      headClose.onclick = close;
      head.appendChild(headClose);
      pop.appendChild(head);

      const bar = document.createElement("div");
      bar.className = "cw-search-bar";

      const titleField = document.createElement("label");
      titleField.className = "cw-meta-search-field cw-meta-search-field-title";
      titleField.htmlFor = "cw_meta_search_title";
      titleField.appendChild(document.createTextNode("Title"));

      const qInput = document.createElement("input");
      qInput.type = "text";
      qInput.id = "cw_meta_search_title";
      qInput.name = qInput.id;
      qInput.placeholder = "Title...";
      qInput.value = row.title || "";
      const qWrap = document.createElement("div");
      qWrap.className = "cw-meta-search-input-wrap";
      qWrap.appendChild(qInput);
      const qIcon = document.createElement("span");
      qIcon.className = "material-symbols-rounded cw-meta-search-inline-icon";
      qIcon.setAttribute("aria-hidden", "true");
      qIcon.textContent = "search";
      qWrap.appendChild(qIcon);
      titleField.appendChild(qWrap);
      bar.appendChild(titleField);

      const yearField = document.createElement("label");
      yearField.className = "cw-meta-search-field";
      yearField.htmlFor = "cw_meta_search_year";
      yearField.appendChild(document.createTextNode("Year"));

      const yearInput = document.createElement("input");
      yearInput.type = "number";
      yearInput.id = "cw_meta_search_year";
      yearInput.name = yearInput.id;
      yearInput.placeholder = "Year";
      if (row.year) yearInput.value = row.year;
      const yearWrap = document.createElement("div");
      yearWrap.className = "cw-meta-search-input-wrap";
      yearWrap.appendChild(yearInput);
      const yearIcon = document.createElement("span");
      yearIcon.className = "material-symbols-rounded cw-meta-search-inline-icon";
      yearIcon.setAttribute("aria-hidden", "true");
      yearIcon.textContent = "calendar_month";
      yearWrap.appendChild(yearIcon);
      yearField.appendChild(yearWrap);
      bar.appendChild(yearField);

      const typeOptions = [["all", "All"], ["movie", "Movie"], ["show", "TV Show"]];
      let typeValue = row.type === "show" || row.type === "episode" ? "show" : row.type === "anime" ? "all" : "movie";

      const typeField = document.createElement("div");
      typeField.className = "cw-meta-search-field cw-meta-search-type-field";
      const typeLabel = document.createElement("div");
      typeLabel.className = "cw-meta-search-label";
      typeLabel.textContent = "Type";
      const typeSegment = document.createElement("div");
      typeSegment.className = "cw-meta-type-segment";
      typeSegment.setAttribute("role", "group");
      typeSegment.setAttribute("aria-label", "Metadata type");
      typeOptions.forEach(([val, label]) => {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "cw-meta-type-option";
        btn.dataset.value = val;
        btn.textContent = label;
        btn.addEventListener("click", () => {
          typeValue = val;
          syncTypeSegment();
        });
        typeSegment.appendChild(btn);
      });
      typeField.append(typeLabel, typeSegment);
      bar.appendChild(typeField);

      pop.appendChild(bar);

      const footer = document.createElement("div");
      footer.className = "cw-meta-search-footer";

      const status = document.createElement("div");
      status.className = "cw-search-status";
      footer.appendChild(status);

      const actions = document.createElement("div");
      actions.className = "cw-pop-actions";

      const searchBtn = document.createElement("button");
      searchBtn.type = "button";
      searchBtn.className = "cw-pop-btn primary";
      searchBtn.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">search</span><span>Search</span>';
      actions.appendChild(searchBtn);

      const closeBtn = document.createElement("button");
      closeBtn.type = "button";
      closeBtn.className = "cw-pop-btn ghost";
      closeBtn.textContent = "Close";
      closeBtn.onclick = close;
      actions.appendChild(closeBtn);

      footer.appendChild(actions);
      pop.appendChild(footer);

      const resultsBox = document.createElement("div");
      resultsBox.className = "cw-search-results";
      pop.appendChild(resultsBox);

      function syncTypeSegment() {
        const selected = String(typeValue || "all");
        typeSegment.querySelectorAll(".cw-meta-type-option").forEach(btn => {
          const active = btn.dataset.value === selected;
          btn.classList.toggle("active", active);
          btn.setAttribute("aria-pressed", active ? "true" : "false");
        });
      }
      syncTypeSegment();

      async function doSearch() {
        const q = (qInput.value || "").trim();
        const yearVal = parseInt(yearInput.value || "", 10);
        if (q.length < 2) {
          status.textContent = "Type at least 2 characters.";
          resultsBox.innerHTML = "";
          return;
        }
        const typ = String(typeValue || "").toLowerCase();
        const makeUrl = t => {
          let u = `/api/metadata/search?q=${encodeURIComponent(q)}&typ=${encodeURIComponent(t)}`;
          if (!Number.isNaN(yearVal)) u += `&year=${yearVal}`;
          return u;
        };

        status.textContent = "Searching...";
        resultsBox.innerHTML = "";
        try {
          let items = [];
          if (typ === "all") {
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
              if (seen.has(k)) return false;
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
            meta.textContent = bits.join(" • ");
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

              const selectedType = String(typeValue || "").toLowerCase();
              const preserveAnime = String(row.type || "").toLowerCase() === "anime" && selectedType === "all";
              const pickedType = String(picked.type || "movie").toLowerCase();

              const newType = preserveAnime ? "anime" : pickedType;
              const resolveEntity = picked._resolve_entity || newType;

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

          status.textContent = `${items.length} result${items.length === 1 ? "" : "s"} found`;
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
    }, { closeOnOutside: false, trackAnchorOnScroll: true });
  }

  Editor.MetadataReplacer = {
    canReplaceRow,
    usesCoordinateReplacer,
    openItemReplacer,
    openTitleSearchEditor,
  };
  window.CrossWatchEditorMetadataReplacer = Editor.MetadataReplacer;
})();
