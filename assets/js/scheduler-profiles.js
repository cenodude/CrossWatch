/* assets/js/scheduler-profiles.js */
/* CrossWatch - Per-profile sync schedules */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function (w, d) {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  if (w.cwSchedulerProfiles) return;

  const LIST_ID = "cw-psch-list";
  const OWN_ID = "cw-psch-own";
  const MODES = [
    ["hourly", T("scheduler.profiles.everyHour")],
    ["every_n_hours", T("scheduler.profiles.everyFew")],
    ["daily_time", T("scheduler.profiles.onceDay")],
    ["custom_interval", T("scheduler.profiles.custom")],
  ];
  let rows = [];
  let openId = "";
  let loadSeq = 0;

  const $ = (sel, root = d) => root.querySelector(sel);
  const txt = (v) => String(v ?? "").trim();
  const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m]));

  function toast(message, ok = false) {
    try { w._cwShowToast?.(String(message || ""), !!ok); } catch {}
  }

  function scope() {
    const s = w.CW?.SettingsScope;
    return s && typeof s.active === "function" && s.active() ? s : null;
  }

  async function json(url, opts) {
    const res = await fetch(url, { cache: "no-store", credentials: "same-origin", ...(opts || {}) });
    const data = await res.json().catch(() => null);
    if (!res.ok || data?.ok === false) throw new Error(String(data?.error || `HTTP ${res.status}`));
    return data;
  }

  function summary(schedule) {
    if (!schedule?.enabled) return T("scheduler.profiles.off");
    if (schedule.mode === "hourly") return T("scheduler.profiles.everyHour");
    if (schedule.mode === "daily_time") return T("scheduler.profiles.dailyAt", {time: schedule.daily_time});
    if (schedule.mode === "custom_interval") {
      const minutes = Number(schedule.custom_interval_minutes || 0);
      return minutes % 60 === 0 ? T("scheduler.profiles.everyHours", {count: minutes / 60}) : T("scheduler.profiles.everyMinutes", {count: minutes});
    }
    const hours = Number(schedule.every_n_hours || 0);
    return T("scheduler.profiles.everyHours", {count: hours});
  }

  function runLine(row) {
    const parts = [];
    if (row.schedule?.enabled && row.next_run_iso) parts.push(T("scheduler.profiles.nextRun", {when: row.next_run_iso}));
    if (row.last_run_at) parts.push(`${T("scheduler.profiles.lastRun", {when: new Date(row.last_run_at * 1000).toLocaleString((w.CW?.I18n || w.__cwI18n)?.locale)})}${row.last_run_ok === false ? ` (${T("scheduler.profiles.failed")})` : ""}`);
    return parts.join(" · ");
  }

  function pairsLine(row) {
    const count = Number(row.pair_count || 0);
    return count ? T("scheduler.profiles.pairs", {count}) : T("scheduler.profiles.noPairs");
  }

  function editor(row) {
    const s = row.schedule || {};
    const show = (mode) => (s.mode === mode ? "" : "hidden");
    return `
      <div class="cw-psch-editor" data-psch-editor="${esc(row.id)}">
        <label class="cw-psch-toggle">
          <input type="checkbox" data-psch-field="enabled" ${s.enabled ? "checked" : ""}>
          <span class="cw-psch-toggle-track" aria-hidden="true"></span>
          <span>${TH("scheduler.profiles.runOwn", {name: row.label})}</span>
        </label>
        <div class="cw-psch-fields">
          <label class="cw-psch-field"><span>${TH("scheduler.profiles.frequency")}</span>
            <select data-psch-field="mode">${MODES.map(([value, label]) => `<option value="${value}" ${s.mode === value ? "selected" : ""}>${label}</option>`).join("")}</select>
          </label>
          <label class="cw-psch-field" data-psch-for="every_n_hours" ${show("every_n_hours")}><span>${TH("scheduler.profiles.everyHoursLabel")}</span>
            <input type="number" min="1" max="168" step="1" data-psch-field="every_n_hours" value="${esc(s.every_n_hours)}">
          </label>
          <label class="cw-psch-field" data-psch-for="daily_time" ${show("daily_time")}><span>${TH("scheduler.profiles.time")}</span>
            <input type="time" data-psch-field="daily_time" value="${esc(s.daily_time)}">
          </label>
          <label class="cw-psch-field" data-psch-for="custom_interval" ${show("custom_interval")}><span>${TH("scheduler.profiles.everyMinutesLabel")}</span>
            <input type="number" min="60" max="10080" step="15" data-psch-field="custom_interval_minutes" value="${esc(s.custom_interval_minutes)}">
          </label>
        </div>
        <p class="cw-psch-note">${TH("scheduler.profiles.runsOnly", {name: row.label, pairs: pairsLine(row).toLowerCase()})}</p>
        <div class="cw-psch-actions">
          <span class="cw-psch-run">${esc(runLine(row))}</span>
          <button type="button" class="cw-psch-save" data-psch-save="${esc(row.id)}">${TH("scheduler.profiles.save")}</button>
        </div>
      </div>`;
  }

  function renderOwn(row) {
    return `
      <section class="cw-psch-card" id="${OWN_ID}">
        <div class="cw-psch-head">
          <span class="material-symbols-rounded cw-psch-icon" aria-hidden="true">event_repeat</span>
          <div class="cw-psch-head-copy">
            <strong>${esc(row.label)}'s schedule</strong>
            <small>${esc(summary(row.schedule))} · ${esc(pairsLine(row))}</small>
          </div>
        </div>
        ${editor(row)}
      </section>`;
  }

  function renderList() {
    const items = rows.map((row) => `
      <div class="cw-psch-row${openId === row.id ? " is-open" : ""}">
        <button type="button" class="cw-psch-row-head" data-psch-toggle="${esc(row.id)}" aria-expanded="${openId === row.id}">
          <span class="cw-psch-row-name"><strong>${esc(row.label)}</strong><small>${esc(pairsLine(row))}</small></span>
          <span class="cw-psch-badge${row.schedule?.enabled ? " is-on" : ""}">${esc(summary(row.schedule))}</span>
          <span class="cw-psch-row-next">${esc(row.schedule?.enabled ? row.next_run_iso || T("scheduler.profiles.waiting") : T("scheduler.profiles.global"))}</span>
          <span class="material-symbols-rounded cw-psch-chev" aria-hidden="true">expand_more</span>
        </button>
        ${openId === row.id ? editor(row) : ""}
      </div>`).join("");
    return `
      <section class="cw-psch-card" id="${LIST_ID}">
        <div class="cw-psch-head">
          <span class="material-symbols-rounded cw-psch-icon" aria-hidden="true">groups</span>
          <div class="cw-psch-head-copy">
            <strong>${TH("scheduler.profiles.title")}</strong>
            <small>${TH("scheduler.profiles.sub")}</small>
          </div>
        </div>
        <div class="cw-psch-rows">${items || '<p class="cw-psch-note">' + TH("scheduler.profiles.none") + '</p>'}</div>
      </section>`;
  }

  function mountPoint() {
    if (scope()) return $("#sec-scheduling > .body");
    return $("#sec-scheduling .cw-subpanel[data-sub='basic']");
  }

  function render() {
    const s = scope();
    $("#" + LIST_ID)?.remove();
    $("#" + OWN_ID)?.remove();
    const host = mountPoint();
    if (!host) return false;
    if (s) {
      const row = rows.find((item) => item.id === s.profileId());
      if (!row) return true;
      host.insertAdjacentHTML("afterbegin", renderOwn(row));
      return true;
    }
    if (rows.length) host.insertAdjacentHTML("beforeend", renderList());
    return true;
  }

  async function load() {
    const seq = ++loadSeq;
    let next = [];
    try {
      const data = await json(`/api/scheduling/profiles?ts=${Date.now()}`);
      next = Array.isArray(data?.items) ? data.items : [];
    } catch {
      next = [];
    }
    if (seq !== loadSeq) return;
    rows = next;
    mountWhenReady();
  }

  function mountWhenReady(tries = 0) {
    if (render() || tries > 40) return;
    setTimeout(() => mountWhenReady(tries + 1), 250);
  }

  function readEditor(node) {
    const field = (name) => node.querySelector(`[data-psch-field="${name}"]`);
    return {
      enabled: !!field("enabled")?.checked,
      mode: txt(field("mode")?.value) || "every_n_hours",
      every_n_hours: Number(field("every_n_hours")?.value || 12),
      daily_time: txt(field("daily_time")?.value) || "03:30",
      custom_interval_minutes: Number(field("custom_interval_minutes")?.value || 120),
    };
  }

  async function save(id, button) {
    const node = d.querySelector(`[data-psch-editor="${CSS.escape(id)}"]`);
    if (!node) return;
    button.disabled = true;
    try {
      const saved = await json(`/api/scheduling/profiles/${encodeURIComponent(id)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(readEditor(node)),
      });
      rows = rows.map((row) => (row.id === id ? { ...row, ...saved } : row));
      render();
      toast(saved?.schedule?.enabled ? T("scheduler.profiles.saved", {summary: summary(saved.schedule)}) : T("scheduler.profiles.turnedOff"), true);
    } catch (e) {
      toast(T("scheduler.profiles.saveFailed", {error: String(e?.message || e)}), false);
      button.disabled = false;
    }
  }

  d.addEventListener("click", (event) => {
    const toggle = event.target?.closest?.("[data-psch-toggle]");
    if (toggle) {
      const id = txt(toggle.dataset.pschToggle);
      openId = openId === id ? "" : id;
      render();
      return;
    }
    const saveBtn = event.target?.closest?.("[data-psch-save]");
    if (saveBtn) save(txt(saveBtn.dataset.pschSave), saveBtn);
  });

  d.addEventListener("change", (event) => {
    const select = event.target?.closest?.('[data-psch-field="mode"]');
    if (!select) return;
    const editorNode = select.closest("[data-psch-editor]");
    editorNode?.querySelectorAll("[data-psch-for]").forEach((node) => {
      node.hidden = node.dataset.pschFor !== select.value;
    });
  });

  d.addEventListener("cw-settings-pane-changed", (event) => {
    if (txt(event?.detail?.pane).toLowerCase() === "scheduling") load();
  });
  w.addEventListener("cw:settings-scope-changed", load);
  w.addEventListener("cw-user-profiles-changed", load);

  w.cwSchedulerProfiles = { load };
  Promise.resolve(w.__cwAuthBootstrapPromise).then(load, load);
})(window, document);
