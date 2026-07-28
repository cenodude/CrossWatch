/* assets/helpers/backups.js */
/* CrossWatch Backup & Restore UI */
(function(){
  const DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
  const SPLIT_STORAGE_KEY = "cw.backups.controlsHeight.v4";
  const SPLIT_DEFAULTS = { manual: 215, scheduled: 315 };
  const SPLIT_MINS = { manual: 145, scheduled: 285 };
  const SCOPES = [
    ["config_only", "Config"],
    ["app_state", "Normal"],
    ["full", "Full"]
  ];
  const state = { backups: [], schedule: {}, selected: "", controlsHeight: {}, rowStatus: {}, rowFlash: {}, message: "", mode: "manual", refreshing: false };
  const flashTimers = {};

  function $(id){ return document.getElementById(id); }

  function api(url, init){
    return fetch(url, Object.assign({ cache: "no-store" }, init || {})).then(async (r) => {
      const j = await r.json().catch(() => ({}));
      if (!r.ok || j?.ok === false) throw new Error(j?.error || "Request failed");
      return j;
    });
  }

  function postJSON(url, body){
    return api(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {})
    });
  }

  function el(tag, attrs, children){
    const node = document.createElement(tag);
    const a = attrs || {};
    for (const [k, v] of Object.entries(a)) {
      if (v === false || v === null || v === undefined) continue;
      if (k === "class") node.className = String(v);
      else if (k === "text") node.textContent = String(v);
      else if (k === "title" || k === "aria-label" || k === "type" || k === "value" || k === "name" || k === "id" || k === "for" || k === "accept") node.setAttribute(k, String(v));
      else if (k === "checked") node.checked = !!v;
      else if (k === "disabled") node.disabled = !!v;
      else if (k === "on") {
        for (const [ev, fn] of Object.entries(v || {})) node.addEventListener(ev, fn);
      }
      else node.setAttribute(k, String(v));
    }
    for (const child of [].concat(children || [])) {
      if (child === null || child === undefined) continue;
      node.appendChild(typeof child === "string" ? document.createTextNode(child) : child);
    }
    return node;
  }

  function icon(name){ return el("span", { class: "material-symbols-rounded br-icon", text: name }); }

  function flashRowIcon(path, action, ok){
    const key = `${path}::${action}`;
    state.rowFlash[key] = ok ? "ok" : "fail";
    renderBody();
    clearTimeout(flashTimers[key]);
    flashTimers[key] = setTimeout(() => {
      delete state.rowFlash[key];
      delete flashTimers[key];
      renderBody();
    }, 1500);
  }

  function rowAction(path, action, title, glyph, handler, extraClass){
    const flash = state.rowFlash[`${path}::${action}`];
    const shown = flash === "ok" ? "check" : flash === "fail" ? "close" : glyph;
    const cls = ["br-iconbtn", extraClass, flash ? `flash is-${flash}` : ""].filter(Boolean).join(" ");
    return el("button", {
      class: cls,
      type: "button",
      title,
      "aria-label": title,
      on: { click: (e) => { e.stopPropagation(); handler(path); } }
    }, [icon(shown)]);
  }

  function fmtBytes(n){
    const v = Number(n || 0);
    if (v < 1024) return `${v} B`;
    const units = ["KB", "MB", "GB", "TB"];
    let x = v / 1024;
    let i = 0;
    while (x >= 1024 && i < units.length - 1) { x /= 1024; i++; }
    return `${x.toFixed(x >= 10 ? 1 : 2)} ${units[i]}`;
  }

  function fmtDate(value, fallback){
    const raw = value || (fallback ? Number(fallback) * 1000 : 0);
    if (!raw) return "Never";
    const d = new Date(raw);
    if (Number.isNaN(d.getTime())) return "Unknown";
    return d.toLocaleString();
  }

  function scopeLabel(scope){
    const found = SCOPES.find((x) => x[0] === scope);
    return found ? found[1] : (scope || "Unknown");
  }

  function toast(text, delay){
    state.message = String(text || "");
    clearTimeout(toast._lt);
    const local = $("br-msg");
    if (local) {
      local.textContent = state.message;
      local.classList.remove("hidden");
    }
    toast._lt = setTimeout(() => {
      state.message = "";
      const cur = $("br-msg");
      if (cur) cur.classList.add("hidden");
    }, delay || 4200);
  }


  function scopeSelect(id, value){
    const sel = el("select", { id });
    for (const [v, label] of SCOPES) sel.appendChild(el("option", { value: v, text: label }));
    sel.value = value || "app_state";
    return sel;
  }

  function check(id, label, checked){
    const input = el("input", { id, type: "checkbox", checked: !!checked });
    return el("label", { class: "br-check", for: id }, [input, el("span", { text: label })]);
  }

  function switcher(id, label, checked){
    const input = el("input", { id, type: "checkbox", checked: !!checked });
    return el("label", { class: "br-switch", for: id }, [
      input,
      el("span", { class: "br-switch-ui", "aria-hidden": "true" }),
      el("span", { class: "br-switch-text", text: label }),
      el("span", { class: "br-switch-state", "aria-hidden": "true" })
    ]);
  }

  function buildShell(){
    if ($("cw-backups-modal")) return;
    const modal = el("div", { id: "cw-backups-modal", class: "hidden", "aria-hidden": "true" });
    const dialog = el("div", { class: "br-dialog", role: "dialog", "aria-modal": "true", "aria-label": "Backup and Restore" });
    const head = el("div", { class: "br-head" }, [
      el("div", {}, [
        el("div", { class: "br-title", text: "Backup & Restore" }),
        el("div", { class: "br-sub", text: "Back up CrossWatch config, Normal state, or a Full archive." })
      ]),
      el("button", { class: "br-close", type: "button", title: "Close", "aria-label": "Close", on: { click: close } }, [icon("close")])
    ]);
    const body = el("div", { class: "br-body cw-scrollbars", id: "br-body" });
    dialog.appendChild(head);
    dialog.appendChild(body);
    modal.appendChild(dialog);
    modal.addEventListener("click", (e) => { if (e.target === modal) close(); });
    ["input", "change"].forEach((name) => {
      modal.addEventListener(name, (e) => e.stopPropagation());
    });
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && !modal.classList.contains("hidden")) close();
    });
    document.body.appendChild(modal);
    renderBody();
  }

  function renderBody(){
    const body = $("br-body");
    if (!body) return;
    const listTop = $("br-list")?.scrollTop || 0;
    body.replaceChildren();
    applySplitHeight(body);
    body.appendChild(renderControls());
    body.appendChild(renderSplitter(body));
    body.appendChild(renderListPanel());
    const list = $("br-list");
    if (list && listTop) list.scrollTop = listTop;
  }

  function splitMode(){
    return state.mode === "scheduled" ? "scheduled" : "manual";
  }

  function splitStorageKey(mode){
    return `${SPLIT_STORAGE_KEY}.${mode || splitMode()}`;
  }

  function splitDefault(mode){
    return SPLIT_DEFAULTS[mode || splitMode()] || SPLIT_DEFAULTS.manual;
  }

  function splitMin(mode){
    return SPLIT_MINS[mode || splitMode()] || SPLIT_MINS.manual;
  }

  function clampSplitHeight(body, value){
    const mode = splitMode();
    const min = splitMin(mode);
    const max = Math.max(min, body.clientHeight - 220);
    return Math.min(Math.max(Number(value) || splitDefault(mode), min), max);
  }

  function storedSplitHeight(){
    const mode = splitMode();
    if (state.controlsHeight[mode]) return state.controlsHeight[mode];
    try {
      const raw = Number(window.localStorage?.getItem(splitStorageKey(mode)) || 0);
      return Number.isFinite(raw) && raw > 0 ? raw : splitDefault(mode);
    } catch {
      return splitDefault(mode);
    }
  }

  function applySplitHeight(body, value){
    const wanted = value || storedSplitHeight();
    if (body.clientHeight <= 0) {
      body.style.setProperty("--br-controls-height", `${Number(wanted) || splitDefault()}px`);
      return;
    }
    const height = clampSplitHeight(body, wanted);
    const mode = splitMode();
    state.controlsHeight[mode] = height;
    body.style.setProperty("--br-controls-height", `${height}px`);
    try { window.localStorage?.setItem(splitStorageKey(mode), String(height)); } catch {}
  }

  function renderSplitter(body){
    const splitter = el("button", {
      class: "br-splitter",
      type: "button",
      title: "Resize panels",
      "aria-label": "Resize backup panels",
      "aria-orientation": "horizontal"
    });
    splitter.addEventListener("pointerdown", (e) => {
      if (e.button !== 0) return;
      e.preventDefault();
      const modal = $("cw-backups-modal");
      const startY = e.clientY;
      const startHeight = state.controlsHeight[splitMode()] || body.getBoundingClientRect().height * 0.42;
      splitter.setPointerCapture(e.pointerId);
      modal?.classList.add("br-resizing");
      const move = (ev) => applySplitHeight(body, startHeight + ev.clientY - startY);
      const stop = () => {
        modal?.classList.remove("br-resizing");
        splitter.removeEventListener("pointermove", move);
        splitter.removeEventListener("pointerup", stop);
        splitter.removeEventListener("pointercancel", stop);
      };
      splitter.addEventListener("pointermove", move);
      splitter.addEventListener("pointerup", stop);
      splitter.addEventListener("pointercancel", stop);
    });
    splitter.addEventListener("keydown", (e) => {
      if (e.key !== "ArrowUp" && e.key !== "ArrowDown") return;
      e.preventDefault();
      applySplitHeight(body, (state.controlsHeight[splitMode()] || storedSplitHeight()) + (e.key === "ArrowDown" ? 20 : -20));
    });
    return splitter;
  }

  function renderControls(){
    const schedule = state.schedule || {};
    const panel = el("div", { class: "br-panel br-controls" });
    const activeMode = state.mode === "scheduled" ? "scheduled" : "manual";
    const setMode = (mode) => {
      state.mode = mode === "scheduled" ? "scheduled" : "manual";
      renderBody();
    };
    const tab = (mode, label) => el("button", {
      class: `br-mode-tab ${activeMode === mode ? "active" : ""}`,
      type: "button",
      "aria-pressed": activeMode === mode ? "true" : "false",
      on: { click: () => setMode(mode) }
    }, [label]);
    panel.appendChild(el("div", { class: "br-mode-top" }, [
      el("div", { class: "br-mode-tabs", role: "group", "aria-label": "Backup mode" }, [
        tab("manual", "Manual Backup"),
        tab("scheduled", "Scheduled Backups")
      ]),
      el("div", { id: "br-msg", class: `br-msg ${state.message ? "" : "hidden"}`, text: state.message })
    ]));

    const create = el("div", { class: "br-mode-pane br-section" });
    create.appendChild(el("div", { class: "br-manual-grid" }, [
      el("div", { class: "br-field" }, [el("label", { for: "br-scope", text: "Scope" }), scopeSelect("br-scope", "app_state")]),
      el("div", { class: "br-field" }, [el("label", { for: "br-label", text: "Label" }), el("input", { id: "br-label", type: "text", value: "manual" })]),
      el("div", { class: "br-actions" }, [
        el("button", { class: "br-btn primary", type: "button", on: { click: createNow } }, ["Create Backup"]),
        el("button", { class: "br-btn", type: "button", on: { click: () => $("br-upload")?.click() } }, ["Import"]),
        el("input", { id: "br-upload", class: "br-upload", type: "file", accept: ".zip", on: { change: uploadBackup } })
      ])
    ]));

    const scheduled = el("div", { class: "br-mode-pane br-section br-scheduled" });
    scheduled.appendChild(el("div", { class: "br-section-head" }, [
      el("h3", { text: "Scheduled Backups" }),
      switcher("br-sch-enabled", "Enable", !!schedule.active),
      el("div", { class: "br-actions" }, [
        el("button", { class: "br-btn primary", type: "button", on: { click: saveSchedule } }, ["Save Schedule"])
      ])
    ]));
    scheduled.appendChild(el("div", { class: "br-schedule-grid" }, [
      el("div", { class: "br-field" }, [el("label", { for: "br-sch-scope", text: "Scope" }), scopeSelect("br-sch-scope", schedule.scope || "app_state")]),
      el("div", { class: "br-field" }, [el("label", { for: "br-sch-at", text: "Time" }), el("input", { id: "br-sch-at", type: "time", value: schedule.at || "03:00" })]),
      el("div", { class: "br-field" }, [el("label", { for: "br-ret-days", text: "Retention days" }), el("input", { id: "br-ret-days", type: "number", value: String(schedule.retention_days ?? 30), min: "0" })]),
      el("div", { class: "br-field" }, [el("label", { for: "br-max-backups", text: "Max backups" }), el("input", { id: "br-max-backups", type: "number", value: String(schedule.max_backups ?? 10), min: "0" })])
    ]));
    const days = Array.isArray(schedule.days) ? schedule.days.map(Number) : [];
    const daysBox = el("div", { class: "br-days" });
    DAY_NAMES.forEach((name, i) => {
      const n = i + 1;
      const cb = el("input", { type: "checkbox", value: String(n), checked: days.includes(n) });
      daysBox.appendChild(el("label", { class: "br-day" }, [cb, el("span", { text: name })]));
    });
    scheduled.appendChild(el("div", { class: "br-field br-days-field" }, [el("label", { text: "Days" }), daysBox]));
    panel.appendChild(el("div", { class: "br-mode-window" }, [
      el("div", { class: `br-mode-track ${activeMode}` }, [create, scheduled])
    ]));
    return panel;
  }

  function renderStatus(panel){
    const latest = state.backups[0] || {};
    const total = state.backups.reduce((n, b) => n + Number(b.size || 0), 0);
    const ext = latest.external_key_required ? "External key" : (latest.master_key_included ? "Key included" : "No key needed");
    const next = (state.schedule || {}).active ? `${state.schedule.at || "03:00"}` : "Disabled";
    panel.appendChild(el("div", { class: "br-status" }, [
      el("div", { class: "br-stat" }, [el("b", { text: fmtDate(latest.created_at, latest.mtime) }), el("span", { text: "Last backup" })]),
      el("div", { class: "br-stat" }, [el("b", { text: next }), el("span", { text: "Schedule" })]),
      el("div", { class: "br-stat" }, [el("b", { text: fmtBytes(total) }), el("span", { text: "Stored" })]),
      el("div", { class: "br-stat" }, [el("b", { text: ext }), el("span", { text: "Key status" })])
    ]));
  }

  function renderListPanel(){
    const panel = el("div", { class: "br-panel br-list-panel" });
    panel.appendChild(el("div", { class: "br-list-head" }, [
      el("h3", { text: "Backup List" }),
      el("button", {
        class: `br-iconbtn ${state.refreshing ? "spin" : ""}`,
        type: "button",
        title: "Refresh",
        "aria-label": "Refresh",
        disabled: state.refreshing,
        on: { click: () => refresh({ busy: true }) }
      }, [icon("refresh")])
    ]));
    renderStatus(panel);
    const list = el("div", { class: "br-list cw-scrollbars", id: "br-list" });
    if (!state.backups.length) {
      list.appendChild(el("div", { class: "br-row" }, [el("div", { class: "br-main" }, [el("div", { class: "br-name", text: "No backups yet" }), el("div", { class: "br-meta" }, [el("span", { text: "Create one manually or enable the schedule." })])])]));
    } else {
      state.backups.forEach((b) => list.appendChild(renderBackupRow(b)));
    }
    panel.appendChild(list);
    return panel;
  }

  function renderBackupRow(b){
    const path = String(b.path || "");
    const row = el("div", { class: `br-row ${state.selected === path ? "active" : ""}` });
    const main = el("div", { class: "br-main" }, [
      el("div", { class: "br-name", text: b.label || PathName(path) }),
      el("div", { class: "br-meta" }, [
        el("span", { class: "br-pill", text: scopeLabel(b.scope) }),
        el("span", { class: "br-pill", text: fmtBytes(b.size) }),
        el("span", { class: "br-pill", text: fmtDate(b.created_at, b.mtime) }),
        el("span", { class: "br-pill", text: b.external_key_required ? "External key required" : (b.master_key_included ? "Key included" : "No key") })
      ])
    ]);
    const note = state.rowStatus[path];
    if (note?.text) {
      main.appendChild(el("div", { class: `br-row-note ${note.kind || ""}`, text: note.text }));
    }
    const actions = el("div", { class: "br-row-actions" }, [
      rowAction(path, "download", "Download", "download", downloadBackup),
      rowAction(path, "validate", "Validate", "verified", validateBackup),
      rowAction(path, "restore", "Restore", "settings_backup_restore", restoreBackup),
      rowAction(path, "delete", "Delete", "delete", deleteBackup, "danger")
    ]);
    row.appendChild(main);
    row.appendChild(actions);
    row.addEventListener("click", () => { state.selected = path; renderBody(); });
    return row;
  }

  function PathName(path){
    const parts = String(path || "").split("/");
    return parts[parts.length - 1] || "Backup";
  }

  function currentCreateOptions(){
    return {
      scope: $("br-scope")?.value || "app_state",
      label: $("br-label")?.value || "manual",
      include_snapshots: false,
      include_reports: false,
      include_cache: false
    };
  }

  async function createNow(){
    try {
      toast("Creating backup...");
      await postJSON("/api/backups/create", currentCreateOptions());
      toast("Backup created");
      await refresh();
    } catch (e) {
      toast(`Backup failed: ${e.message || e}`, 3200);
    }
  }

  async function uploadBackup(e){
    const input = e.currentTarget;
    const file = input?.files?.[0];
    if (!file) return;
    try {
      const form = new FormData();
      form.append("file", file);
      toast("Importing backup...");
      await api("/api/backups/upload", { method: "POST", body: form });
      toast("Backup imported");
      input.value = "";
      await refresh();
    } catch (err) {
      toast(`Import failed: ${err.message || err}`, 3200);
    }
  }

  function downloadBackup(path){
    if (!path) return;
    window.location.href = `/api/backups/download?path=${encodeURIComponent(path)}`;
    flashRowIcon(path, "download", true);
  }

  async function validateBackup(path){
    try {
      state.rowStatus[path] = { kind: "", text: "Validating backup..." };
      renderBody();
      const res = await postJSON("/api/backups/validate", { path });
      const errors = res?.validation?.errors || [];
      state.rowStatus[path] = errors.length
        ? { kind: "warn", text: `Validation found ${errors.length} issue(s).` }
        : { kind: "ok", text: "Successfully validated." };
      flashRowIcon(path, "validate", !errors.length);
    } catch (e) {
      state.rowStatus[path] = { kind: "warn", text: `Validation failed: ${e.message || e}` };
      flashRowIcon(path, "validate", false);
    }
  }

  async function restoreBackup(path){
    if (!path) return;
    const ok = window.confirm("Restore this CrossWatch backup?\n\nA pre-restore backup will be created first and CrossWatch will restart after restore.");
    if (!ok) return;
    try {
      toast("Restoring backup...");
      await postJSON("/api/backups/restore", { path, restart: true });
      toast("Restore applied. Restarting...");
      flashRowIcon(path, "restore", true);
      setTimeout(() => { try { window.location.reload(); } catch {} }, 2400);
    } catch (e) {
      toast(`Restore failed: ${e.message || e}`, 4200);
      flashRowIcon(path, "restore", false);
    }
  }

  async function deleteBackup(path){
    if (!path) return;
    if (!window.confirm("Delete this backup?")) return;
    try {
      await postJSON("/api/backups/delete", { path });
      toast("Backup deleted");
      flashRowIcon(path, "delete", true);
      setTimeout(() => { refresh().catch(() => {}); }, 800);
    } catch (e) {
      toast(`Delete failed: ${e.message || e}`, 3200);
      flashRowIcon(path, "delete", false);
    }
  }

  function readScheduleForm(){
    const dayChecks = Array.from(document.querySelectorAll(".br-days input[type=checkbox]"));
    const days = dayChecks.filter((x) => x.checked).map((x) => Number(x.value)).filter((n) => n >= 1 && n <= 7);
    return {
      enabled: !!$("br-sch-enabled")?.checked,
      scope: $("br-sch-scope")?.value || "app_state",
      at: $("br-sch-at")?.value || "03:00",
      days,
      retention_days: Number($("br-ret-days")?.value || 30),
      max_backups: Number($("br-max-backups")?.value || 10),
      auto_delete_old: true,
      include_snapshots: false,
      include_reports: false,
      include_cache: false
    };
  }

  async function saveSchedule(){
    try {
      const res = await postJSON("/api/backups/schedule", readScheduleForm());
      state.schedule = res.schedule || {};
      toast("Backup schedule saved");
      renderBody();
    } catch (e) {
      toast(`Schedule failed: ${e.message || e}`, 3200);
    }
  }

  async function refresh(opts){
    const busy = !!(opts && opts.busy);
    if (busy) {
      state.refreshing = true;
      renderBody();
    }
    try {
      const [list, sched] = await Promise.all([
        api("/api/backups/list"),
        api("/api/backups/schedule")
      ]);
      state.backups = Array.isArray(list.backups) ? list.backups : [];
      state.schedule = sched.schedule || {};
    } finally {
      if (busy) state.refreshing = false;
      renderBody();
    }
  }

  function open(){
    buildShell();
    const modal = $("cw-backups-modal");
    if (!modal) return;
    document.body.classList.add("br-backups-open", "cx-modal-open");
    modal.classList.remove("hidden");
    modal.setAttribute("aria-hidden", "false");
    refresh().catch((e) => toast(`Refresh failed: ${e.message || e}`, 3200));
  }

  function close(){
    const modal = $("cw-backups-modal");
    if (!modal) return;
    modal.classList.add("hidden");
    modal.setAttribute("aria-hidden", "true");
    document.body.classList.remove("br-backups-open", "cx-modal-open");
  }

  window.openBackupRestore = open;
  (window.CW ||= {});
  window.CW.Backups = { open, close, refresh };
})();
