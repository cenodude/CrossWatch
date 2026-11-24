/* assets/js/editor/row-editor.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;

  function updateTypeDisplay(row, el) {
    if (!el) return;
    let label = "";
    let icon = "category";
    const t = (row?.type || "").toLowerCase();
    if (t === "movie") {
      label = T("editor.rowType.movie");
      icon = "movie";
    } else if (t === "show") {
      label = T("editor.rowType.show");
      icon = "monitoring";
    } else if (t === "anime") {
      label = T("editor.rowType.anime");
      icon = "auto_awesome";
    } else if (t === "season") {
      label = T("editor.rowType.season");
      icon = "layers";
    } else if (t === "episode") {
      label = T("editor.rowType.episode");
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
      text.textContent = T("editor.rowType.set");
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
      ctx.appendPopupTitle(pop, T("editor.rowType.type"));
      if (locked) return ctx.renderLockedPopup(pop, close);

      const grid = document.createElement("div");
      grid.className = "cw-type-grid";
      const current = (row.type || "").toLowerCase();
      const allowed = ctx.allowedTypesForKind(ctx.state.kind);
      const options = [
        { key: "movie", label: T("editor.rowType.movie") },
        { key: "show", label: T("editor.rowType.show") },
        { key: "anime", label: T("editor.rowType.anime") },
        { key: "season", label: T("editor.rowType.season") },
        { key: "episode", label: T("editor.rowType.episode") },
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
          label: T("editor.rowType.clear"),
          kind: "ghost",
          onClick: () => {
            row.type = "";
            row.raw.type = null;
            row.episode = false;
            ctx.finishPopupChange(close, true);
          }
        },
        { label: T("editor.raw.close"), kind: "ghost", onClick: close },
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
    if (window.CW?.MobileUI?.mobile()) (state.mobileExpandedRows ||= new Set()).add(state.rows[0]._rid);
    state.page = 0;
    ctx.markChanged();
    ctx.renderRows();
  }

  Editor.RowEditor = { updateTypeDisplay, openTypeEditor, addRow };
  window.CrossWatchEditorRowEditor = Editor.RowEditor;
})();
