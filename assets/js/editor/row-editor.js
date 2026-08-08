/* assets/js/editor/row-editor.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});

  function updateTypeDisplay(row, el) {
    if (!el) return;
    let label = "";
    let icon = "category";
    const t = (row?.type || "").toLowerCase();
    if (t === "movie") {
      label = "Movie";
      icon = "movie";
    } else if (t === "show") {
      label = "Show";
      icon = "monitoring";
    } else if (t === "anime") {
      label = "Anime";
      icon = "auto_awesome";
    } else if (t === "season") {
      label = "Season";
      icon = "layers";
    } else if (t === "episode") {
      label = "Episode";
      icon = "open_in_new";
    }

    el.innerHTML = "";
    ["type-movie", "type-show", "type-anime", "type-season", "type-episode"].forEach(cls => el.classList.remove(cls));
    if (t) el.classList.add(`type-${t}`);
    const text = document.createElement("span");
    text.className = "cw-extra-display-label";
    if (label) {
      text.textContent = label;
      text.classList.add("cw-extra-display-value");
    } else {
      text.textContent = "Set type";
      text.classList.add("cw-extra-display-placeholder");
    }
    el.appendChild(text);

    const iconEl = document.createElement("span");
    iconEl.className = "material-symbol cw-extra-display-icon";
    iconEl.textContent = icon;
    el.appendChild(iconEl);
  }

  function openTypeEditor(row, anchor, ctx = {}) {
    const locked = !!ctx.isRowLocked(row);

    ctx.openPopup(anchor, (pop, close) => {
      ctx.appendPopupTitle(pop, "Type");
      if (locked) return ctx.renderLockedPopup(pop, close);

      const grid = document.createElement("div");
      grid.className = "cw-type-grid";
      const current = (row.type || "").toLowerCase();
      const allowed = ctx.allowedTypesForKind(ctx.state.kind);
      const options = [
        { key: "movie", label: "Movie" },
        { key: "show", label: "Show" },
        { key: "anime", label: "Anime" },
        { key: "season", label: "Season" },
        { key: "episode", label: "Episode" },
      ].filter(o => allowed.includes(o.key));

      options.forEach(opt => {
        const pill = document.createElement("button");
        pill.type = "button";
        pill.className = "cw-type-pill" + (current === opt.key ? " active" : "");
        pill.textContent = opt.label;
        pill.onclick = () => {
          row.type = opt.key;
          row.raw.type = opt.key;
          row.episode = opt.key === "episode";
          ctx.finishPopupChange(close, true);
        };
        grid.appendChild(pill);
      });

      pop.appendChild(grid);
      ctx.appendPopupActions(pop, [
        {
          label: "Clear",
          kind: "ghost",
          onClick: () => {
            row.type = "";
            row.raw.type = null;
            row.episode = false;
            ctx.finishPopupChange(close, true);
          }
        },
        { label: "Close", kind: "ghost", onClick: close },
      ]);
    });
  }

  function addRow(ctx = {}) {
    const state = ctx.state;
    const raw = { ids: {}, type: "movie", title: "", year: null };
    if (!Array.isArray(state.rows)) state.rows = [];
    state.rows.unshift({
      _rid: state.ridSeq++,
      key: "",
      type: raw.type,
      title: "",
      year: "",
      imdb: "",
      tmdb: "",
      tvdb: "",
      trakt: "",
      simkl: "",
      mal: "",
      anilist: "",
      raw,
      deleted: false,
      episode: false,
      _origin: ctx.isPolicySource() ? "manual" : "playlist",
    });
    state.page = 0;
    ctx.markChanged();
    ctx.renderRows();
  }

  Editor.RowEditor = { updateTypeDisplay, openTypeEditor, addRow };
  window.CrossWatchEditorRowEditor = Editor.RowEditor;
})();
