/* assets/js/editor/extra-editors.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});

  const DT = () => {
    const dt = window.CW && window.CW.Editor && window.CW.Editor.DateTime;
    if (!dt) throw new Error("Editor module missing: DateTime");
    return dt;
  };

  function createDateTimeEditor(row, ctx = {}) {
    const dt = DT();
    const field = ctx.state.kind === "history" ? "watched_at" : "collected_at";
    const input = document.createElement("input");
    input.type = "datetime-local";
    input.step = 1;
    input.className = "cw-input cw-timestamp-input";
    input.value = dt.localInput(row.raw && row.raw[field]);
    input.title = "Local time; saved in UTC";
    input.onclick = () => {
      try { input.showPicker?.(); } catch {}
    };
    input.onchange = () => {
      if (!input.checkValidity()) return;
      const value = dt.dateTimeInputToIso(input.value, row.raw && row.raw[field]);
      if (value === (row.raw[field] || null)) return;
      row.raw[field] = value;
      ctx.finishExtraChange(row, input, () => {});
    };
    return input;
  }

  function updateExtraDisplay(row, el, ctx = {}) {
    const state = ctx.state || {};
    const dt = DT();
    if (el.type === "datetime-local") {
      const field = state.kind === "history" ? "watched_at" : "collected_at";
      el.value = dt.localInput(row.raw && row.raw[field]);
      return;
    }
    let label = "";
    let placeholder = "";
    let icon = "";
    if (state.kind === "ratings") {
      icon = "star";
      const r = row.raw && row.raw.rating;
      if (r == null || r === "") placeholder = "Set rating";
      else label = String(r) + "/10";
    } else if (state.kind === "history") {
      icon = "schedule";
      const w = row.raw && row.raw.watched_at;
      if (!w) placeholder = "Set time";
      else label = dt.formatHistoryLabel ? dt.formatHistoryLabel(w) : String(w || "");
    } else if (state.kind === "collection") {
      icon = "inventory_2";
      const c = row.raw && row.raw.collected_at;
      if (!c) placeholder = "Set date";
      else label = dt.formatHistoryLabel ? dt.formatHistoryLabel(c) : String(c || "");
    } else if (state.kind === "progress") {
      icon = "play_circle";
      const p = row.raw && row.raw.progress_ms;
      const d = row.raw && row.raw.duration_ms;
      const percent = dt.progressPercentValue ? dt.progressPercentValue(row.raw) : null;
      const pm = p == null ? NaN : Number(p);
      const dm = d == null ? NaN : Number(d);
      if (!Number.isFinite(pm) || pm <= 0) {
        if (percent != null) label = dt.formatProgressPercent ? dt.formatProgressPercent(percent) : `${percent}%`;
        else placeholder = "Set progress";
      }
      else {
        const left = dt.formatMs ? dt.formatMs(pm) : "";
        const right = Number.isFinite(dm) && dm > 0 && dt.formatMs ? dt.formatMs(dm) : "";
        const pct = percent != null && dt.formatProgressPercent ? ` (${dt.formatProgressPercent(percent)})` : "";
        label = right ? `${left} / ${right}${pct}` : left;
      }
    } else {
      placeholder = "";
    }

    el.innerHTML = "";
    const text = document.createElement("span");
    text.className = "cw-extra-display-label";
    if (label) {
      text.textContent = label;
      text.classList.add("cw-extra-display-value");
    } else {
      text.textContent = placeholder || "";
      text.classList.add("cw-extra-display-placeholder");
    }
    el.appendChild(text);

    if (icon) {
      const iconEl = document.createElement("span");
      iconEl.className = "material-symbol cw-extra-display-icon";
      iconEl.textContent = icon;
      el.appendChild(iconEl);
    }
  }

  function openHistoryEditor(row, anchor, displayEl, ctx = {}) {
    const dt = DT();
    ctx.openPopup(anchor, (pop, close) => {
      ctx.appendPopupTitle(pop, "Watched at");
      const dateInput = dt.appendDateTimeInput(pop, row.raw && row.raw.watched_at, "Watched at");

      ctx.appendPopupActions(pop, [
        { label: "Clear", kind: "ghost", onClick: () => { row.raw.watched_at = null; ctx.finishExtraChange(row, displayEl, close); } },
        { label: "Close", kind: "ghost", onClick: close },
        { label: "Save", kind: "primary", onClick: () => { row.raw.watched_at = dt.dateTimeInputToIso(dateInput.value, row.raw.watched_at) || null; ctx.finishExtraChange(row, displayEl, close); } },
      ]);

      dateInput.focus();
    });
  }

  function openCollectionEditor(row, anchor, displayEl, ctx = {}) {
    const dt = DT();
    ctx.openPopup(anchor, (pop, close) => {
      ctx.appendPopupTitle(pop, "Collected at");
      const dateInput = dt.appendDateTimeInput(pop, row.raw && row.raw.collected_at, "Collected at");

      ctx.appendPopupActions(pop, [
        { label: "Clear", kind: "ghost", onClick: () => { row.raw.collected_at = null; ctx.finishExtraChange(row, displayEl, close); } },
        { label: "Close", kind: "ghost", onClick: close },
        { label: "Save", kind: "primary", onClick: () => { row.raw.collected_at = dt.dateTimeInputToIso(dateInput.value, row.raw.collected_at) || null; ctx.finishExtraChange(row, displayEl, close); } },
      ]);

      dateInput.focus();
    });
  }

  function openProgressEditor(row, anchor, displayEl, ctx = {}) {
    const dt = DT();
    ctx.openPopup(anchor, (pop, close) => {
      ctx.appendPopupTitle(pop, "Progress");

      const makeInput = (type, props = {}) => Object.assign(document.createElement("input"), { type, ...props });
      const makeField = (label, child, extraClass = "") => {
        const field = document.createElement("label");
        field.className = `cw-progress-edit-field ${extraClass}`.trim();
        const labelEl = Object.assign(document.createElement("span"), { className: "cw-progress-edit-label", textContent: label });
        field.appendChild(labelEl);
        field.appendChild(child);
        return field;
      };

      const grid = document.createElement("div");
      grid.className = "cw-progress-edit-grid";

      const curPos = row.raw && row.raw.progress_ms;
      const curDur = row.raw && row.raw.duration_ms;
      const curPercent = dt.progressPercentValue ? dt.progressPercentValue(row.raw) : null;
      const posInput = makeInput("text", { placeholder: "mm:ss", value: curPos != null && dt.formatMs ? dt.formatMs(curPos) : "" });
      const durInput = makeInput("text", { placeholder: "mm:ss", value: curDur != null && dt.formatMs ? dt.formatMs(curDur) : "" });

      grid.appendChild(makeField("Position", posInput));
      grid.appendChild(makeField("Duration", durInput));
      pop.appendChild(grid);

      const percentInput = makeInput("number", {
        min: "0",
        max: "100",
        step: "0.1",
        placeholder: "0-100",
        value: curPercent != null ? String(Math.round(curPercent * 10) / 10) : "",
      });
      const percentWrap = document.createElement("div");
      percentWrap.className = "cw-progress-percent-wrap";
      percentWrap.append(percentInput, Object.assign(document.createElement("span"), { className: "cw-progress-percent-suffix", textContent: "%" }));
      pop.appendChild(makeField("Percent", percentWrap, "cw-progress-percent-field"));

      ctx.appendPopupTitle(pop, "Updated at", "10px");

      const dateInput = dt.appendDateTimeInput(pop, row.raw && row.raw.progress_at, "Updated at");

      const saveProgress = () => {
        const posMs = dt.parseTimeToMs ? dt.parseTimeToMs(posInput.value) : null;
        const durMs = dt.parseTimeToMs ? dt.parseTimeToMs(durInput.value) : null;
        const percent = dt.parseProgressPercent ? dt.parseProgressPercent(percentInput.value) : null;

        row.raw.progress_ms = posMs != null && posMs > 0 ? posMs : null;
        row.raw.duration_ms = durMs != null && durMs > 0 ? durMs : null;
        row.raw.progress_percent = row.raw.progress_ms != null && row.raw.duration_ms != null
          ? Math.round((row.raw.progress_ms / row.raw.duration_ms) * 1000) / 10
          : percent;
        row.raw.progress_at = dt.dateTimeInputToIso(dateInput.value, row.raw.progress_at) || null;
        if (!row.raw.progress_at && (row.raw.progress_ms != null || row.raw.progress_percent != null)) row.raw.progress_at = new Date().toISOString().replace(/\.\d{3}Z$/, ".000Z");
        ctx.finishExtraChange(row, displayEl, close);
      };

      ctx.appendPopupActions(pop, [
        {
          label: "Clear",
          kind: "ghost",
          onClick: () => {
            for (const key of ["progress_ms", "duration_ms", "progress_percent", "progress_at"]) row.raw[key] = null;
            ctx.finishExtraChange(row, displayEl, close);
          }
        },
        { label: "Close", kind: "ghost", onClick: close },
        { label: "Save", kind: "primary", onClick: saveProgress },
      ]);

      posInput.focus();
    });
  }

  function openRatingEditor(row, anchor, displayEl, ctx = {}) {
    const dt = DT();
    ctx.openPopup(anchor, (pop, close) => {
      ctx.appendPopupTitle(pop, "Rating");

      const grid = document.createElement("div");
      grid.className = "cw-rating-grid";
      const current = row.raw && row.raw.rating != null ? Number(row.raw.rating) : null;

      for (let i = 1; i <= 10; i += 1) {
        const pill = document.createElement("button");
        pill.type = "button";
        pill.className = "cw-rating-pill" + (current === i ? " active" : "");
        pill.textContent = String(i);
        pill.onclick = () => {
          row.raw.rating = i;
          if (dateInput.value !== initialDate) {
            row.raw.rated_at = dt.dateTimeInputToIso(dateInput.value, row.raw.rated_at);
          }
          ctx.finishExtraChange(row, displayEl, close);
        };
        grid.appendChild(pill);
      }

      pop.appendChild(grid);
      const dates = document.createElement("details");
      const summary = document.createElement("summary");
      summary.textContent = "Rated at";
      dates.appendChild(summary);
      const dateInput = dt.appendDateTimeInput(dates, row.raw && row.raw.rated_at, "Rated at");
      const initialDate = dateInput.value;
      ctx.appendPopupActions(dates, [
        { label: "Save date", kind: "primary", onClick: () => {
          row.raw.rated_at = dt.dateTimeInputToIso(dateInput.value, row.raw.rated_at);
          ctx.finishExtraChange(row, displayEl, close);
        } },
      ]);
      pop.appendChild(dates);
      ctx.appendPopupActions(pop, [
        { label: "Clear", kind: "ghost", onClick: () => { row.raw.rating = null; ctx.finishExtraChange(row, displayEl, close); } },
        { label: "Close", kind: "ghost", onClick: close },
      ]);
    });
  }

  Editor.ExtraEditors = {
    createDateTimeEditor,
    updateExtraDisplay,
    openHistoryEditor,
    openCollectionEditor,
    openProgressEditor,
    openRatingEditor,
  };
  window.CrossWatchEditorExtraEditors = Editor.ExtraEditors;
})();
