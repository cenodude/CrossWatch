/* assets/js/editor/datetime.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});

  function pad2(n) {
    return String(n).padStart(2, "0");
  }

  function formatHistoryLabel(iso) {
    if (!iso) return "";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    return (
      d.getUTCFullYear() +
      "-" +
      pad2(d.getUTCMonth() + 1) +
      "-" +
      pad2(d.getUTCDate()) +
      " " +
      pad2(d.getUTCHours()) +
      ":" +
      pad2(d.getUTCMinutes()) +
      " UTC"
    );
  }

  function formatSxxEyy(season, episode) {
    const s = season == null ? NaN : parseInt(String(season), 10);
    if (!Number.isFinite(s)) return "";
    const e = episode == null ? NaN : parseInt(String(episode), 10);
    if (Number.isFinite(e)) return `S${pad2(s)}E${pad2(e)}`;
    return `S${pad2(s)}`;
  }

  function formatMs(ms) {
    const n = ms == null ? NaN : Number(ms);
    if (!Number.isFinite(n) || n <= 0) return "";
    const total = Math.floor(n / 1000);
    const h = Math.floor(total / 3600);
    const m = Math.floor((total % 3600) / 60);
    const s = total % 60;
    if (h > 0) return `${h}:${pad2(m)}:${pad2(s)}`;
    return `${m}:${pad2(s)}`;
  }

  const PROGRESS_PERCENT_KEYS = ["progress_percent", "progressPercent", "percent", "position_percent", "resume_percent"];

  function clampProgressPercent(value) {
    const n = value == null || value === "" ? NaN : Number(value);
    return Number.isFinite(n) ? Math.max(0, Math.min(100, n)) : null;
  }

  function progressPercentValue(raw) {
    if (!raw) return null;
    const pm = Number(raw.progress_ms);
    const dm = Number(raw.duration_ms);
    if (Number.isFinite(pm) && pm > 0 && Number.isFinite(dm) && dm > 0) return clampProgressPercent((pm / dm) * 100);
    for (const key of PROGRESS_PERCENT_KEYS) {
      const n = clampProgressPercent(raw[key]);
      if (n != null) return n;
    }
    return null;
  }

  function formatProgressPercent(value) {
    const n = clampProgressPercent(value);
    if (n == null) return "";
    const rounded = Math.round(n * 10) / 10;
    return `${Number.isInteger(rounded) ? Math.trunc(rounded) : rounded}%`;
  }

  function parseProgressPercent(v) {
    const s = (v == null ? "" : String(v)).trim().replace(/%$/, "").trim();
    return s ? clampProgressPercent(s) : null;
  }

  function parseTimeToMs(v) {
    const s = (v == null ? "" : String(v)).trim();
    if (!s) return null;

    const lower = s.toLowerCase();
    if (lower.endsWith("ms")) {
      const num = parseFloat(lower.slice(0, -2));
      return Number.isFinite(num) ? Math.max(0, Math.floor(num)) : null;
    }

    if (s.includes(":")) {
      const parts = s.split(":").map(p => p.trim()).filter(Boolean);
      if (!parts.length) return null;
      const nums = parts.map(x => parseInt(x, 10));
      if (nums.some(n => !Number.isFinite(n))) return null;

      let sec = 0;
      if (nums.length === 3) sec = nums[0] * 3600 + nums[1] * 60 + nums[2];
      else if (nums.length === 2) sec = nums[0] * 60 + nums[1];
      else sec = nums[0];
      return Math.max(0, sec * 1000);
    }

    const num = parseFloat(s);
    if (!Number.isFinite(num)) return null;
    if (num >= 100000) return Math.max(0, Math.floor(num));
    return Math.max(0, Math.floor(num * 1000));
  }

  function fillDateTimeInputs(iso, dateInput, timeInput) {
    if (!iso) return;
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return;
    dateInput.value = `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
    timeInput.value = `${pad2(d.getUTCHours())}:${pad2(d.getUTCMinutes())}`;
  }

  function dateTimeInputsToIso(dateValue, timeValue) {
    if (!dateValue) return null;
    const parts = dateValue.split("-");
    const y = parseInt(parts[0], 10);
    const m = parseInt(parts[1], 10);
    const dDay = parseInt(parts[2], 10);
    const [hhRaw, mmRaw] = (timeValue || "").split(":");
    const hh = parseInt(hhRaw, 10) || 0;
    const mm = parseInt(mmRaw, 10) || 0;
    return new Date(Date.UTC(y, m - 1, dDay, hh, mm, 0)).toISOString().replace(/\.\d{3}Z$/, ".000Z");
  }

  function appendUtcHint(pop) {
    const hint = document.createElement("div");
    hint.className = "cw-search-status";
    hint.textContent = "Times are shown and saved in UTC.";
    pop.appendChild(hint);
  }

  Editor.DateTime = {
    formatHistoryLabel,
    formatSxxEyy,
    formatMs,
    clampProgressPercent,
    progressPercentValue,
    formatProgressPercent,
    parseProgressPercent,
    parseTimeToMs,
    fillDateTimeInputs,
    dateTimeInputsToIso,
    appendUtcHint,
  };
  window.CrossWatchEditorDateTime = Editor.DateTime;
})();
