/* assets/helpers/backups.js */
/* CrossWatch Backup & Restore UI */
(function(){
  const DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
  const SCOPES = [
    ["config_only", "Settings only", "Your settings and connections. Small, but no fixes, blocks or sync state."],
    ["app_state", "Settings and data", "Everything CrossWatch needs to carry on: settings, sync state, fixes, blocks and published lists."],
    ["full", "Everything", "Also snapshots, sync reports and caches. The largest backup."]
  ];
  const state = { backups: [], schedule: {}, rowStatus: {}, rowFlash: {}, selected: new Set(), message: "", messageKind: "", scope: "app_state", refreshing: false, busy: false };
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

  function icon(name){ return el("span", { class: "material-symbols-rounded br-icon", "aria-hidden": "true", text: name }); }

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
      on: { click: () => handler(path) }
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

  function toast(text, kind, delay){
    state.message = String(text || "");
    state.messageKind = kind || "";
    clearTimeout(toast._lt);
    showMessage();
    toast._lt = setTimeout(() => {
      state.message = "";
      showMessage();
    }, delay || 4200);
  }

  function showMessage(){
    const box = $("br-msg");
    if (!box) return;
    box.textContent = state.message;
    box.className = `br-msg ${state.messageKind} ${state.message ? "" : "hidden"}`.trim();
  }

  function buildShell(){
    if ($("cw-backups-modal")) return;
    const modal = el("div", { id: "cw-backups-modal", class: "hidden", "aria-hidden": "true" });
    const dialog = el("div", { class: "br-dialog", role: "dialog", "aria-modal": "true", "aria-label": "Backups" });
    const head = el("div", { class: "br-head" }, [
      el("div", {}, [
        el("div", { class: "br-title", text: "Backups" }),
        el("div", { class: "br-sub", text: "Save a copy of CrossWatch and bring it back when you need it." })
      ]),
      el("button", { class: "br-close", type: "button", title: "Close", "aria-label": "Close", on: { click: close } }, [icon("close")])
    ]);
    const body = el("div", { class: "br-body cw-scrollbars", id: "br-body" });
    dialog.appendChild(head);
    dialog.appendChild(body);
    modal.appendChild(dialog);
    ["input", "change"].forEach((name) => {
      modal.addEventListener(name, (e) => e.stopPropagation());
    });
    modal.addEventListener("mousedown", (e) => { if (e.target === modal) close(); });
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && !modal.classList.contains("hidden")) close();
    });
    document.body.appendChild(modal);
    renderBody();
  }

  function renderBody(){
    const body = $("br-body");
    if (!body) return;
    const top = body.scrollTop;
    const label = $("br-label")?.value;
    const draft = readScheduleDraft();
    body.replaceChildren(renderCreate(label), renderSchedule(draft), renderList());
    body.scrollTop = top;
    showMessage();
  }

  function sectionHead(glyph, title, sub, extra){
    return el("div", { class: "br-card-head" }, [
      el("span", { class: "br-card-icon" }, [icon(glyph)]),
      el("div", { class: "br-card-copy" }, [el("h3", { text: title }), el("p", { text: sub })]),
      extra || null
    ]);
  }

  function renderCreate(label){
    const card = el("section", { class: "br-card" });
    card.appendChild(sectionHead("backup", "Create a backup", "Choose what to save."));
    const choices = el("div", { class: "br-choices", role: "radiogroup", "aria-label": "What to back up" });
    SCOPES.forEach(([value, title, hint]) => {
      const input = el("input", { type: "radio", name: "br-scope", value, checked: state.scope === value, on: { change: () => { state.scope = value; renderBody(); } } });
      choices.appendChild(el("label", { class: `br-choice ${state.scope === value ? "active" : ""}` }, [
        input,
        el("span", {}, [
          el("b", {}, [title, value === "app_state" ? el("em", { text: "Recommended" }) : null]),
          el("small", { text: hint })
        ])
      ]));
    });
    card.appendChild(choices);
    card.appendChild(el("div", { class: "br-create-row" }, [
      el("input", { id: "br-label", class: "br-input", type: "text", maxlength: "60", placeholder: "Name (optional)", "aria-label": "Backup name", value: label || "" }),
      el("button", { class: "br-btn primary", type: "button", disabled: state.busy, on: { click: createNow } }, [icon("add"), "Create backup"]),
      el("button", { class: "br-btn", type: "button", disabled: state.busy, on: { click: () => $("br-upload")?.click() } }, [icon("upload"), "Import a file"]),
      el("input", { id: "br-upload", class: "br-upload", type: "file", accept: ".zip", on: { change: uploadBackup } })
    ]));
    card.appendChild(el("div", { id: "br-msg", class: "br-msg hidden", role: "status" }));
    return card;
  }

  function scheduleSummary(schedule){
    if (!schedule.active) return "Off";
    const days = (Array.isArray(schedule.days) ? schedule.days.map(Number) : []).filter((n) => n >= 1 && n <= 7);
    const when = !days.length || days.length === 7 ? "Every day" : days.map((n) => DAY_NAMES[n - 1]).join(", ");
    return `${when} at ${schedule.at || "03:00"} · ${scopeLabel(schedule.scope || "app_state")}`;
  }

  function readScheduleDraft(){
    if (!$("br-sch-at")) return null;
    return readScheduleForm();
  }

  function renderSchedule(draft){
    const saved = state.schedule || {};
    const schedule = draft
      ? { active: draft.enabled, scope: draft.scope, at: draft.at, days: draft.days, retention_days: draft.retention_days, max_backups: draft.max_backups }
      : saved;
    const stored = Array.isArray(schedule.days) ? schedule.days.map(Number) : [];
    const days = draft || stored.length ? stored : [1, 2, 3, 4, 5, 6, 7];
    const card = el("section", { class: "br-card" });
    const toggle = el("label", { class: "br-switch" }, [
      el("input", { id: "br-sch-enabled", type: "checkbox", checked: !!schedule.active, on: { change: () => renderBody() } }),
      el("span", { class: "br-switch-ui", "aria-hidden": "true" }),
      el("span", { class: "br-switch-text", text: schedule.active ? "On" : "Off" })
    ]);
    card.appendChild(sectionHead("schedule", "Automatic backups", `Now: ${scheduleSummary(saved)}${draft && draft.enabled !== !!saved.active ? " · not saved yet" : ""}`, toggle));
    const scopeSel = el("select", { id: "br-sch-scope", class: "br-input" });
    SCOPES.forEach(([value, title]) => scopeSel.appendChild(el("option", { value, text: title })));
    scopeSel.value = schedule.scope || "app_state";
    const daysBox = el("div", { class: "br-days" });
    DAY_NAMES.forEach((name, i) => {
      const n = i + 1;
      daysBox.appendChild(el("label", { class: "br-day" }, [
        el("input", { type: "checkbox", value: String(n), checked: days.includes(n) }),
        el("span", { text: name })
      ]));
    });
    const field = (labelText, control, hint) => el("div", { class: "br-field" }, [
      el("label", { for: control.id || null, text: labelText }),
      hint ? el("div", { class: "br-affix" }, [control, el("small", { text: hint })]) : control
    ]);
    const fields = el("div", { class: `br-schedule ${schedule.active ? "" : "is-off"}` }, [
      el("div", { class: "br-schedule-grid" }, [
        field("What", scopeSel),
        field("Time", el("input", { id: "br-sch-at", class: "br-input", type: "time", value: schedule.at || "03:00" })),
        field("Keep for", el("input", { id: "br-ret-days", class: "br-input", type: "number", min: "0", value: String(schedule.retention_days ?? 30) }), "days"),
        field("Keep at most", el("input", { id: "br-max-backups", class: "br-input", type: "number", min: "0", value: String(schedule.max_backups ?? 10) }), "backups")
      ]),
      el("div", { class: "br-field" }, [el("label", { text: "Days" }), daysBox]),
      el("div", { class: "br-schedule-foot" }, [
        el("small", { text: "Older automatic backups are removed when a limit is reached." }),
        el("button", { class: "br-btn primary", type: "button", on: { click: saveSchedule } }, ["Save schedule"])
      ])
    ]);
    card.appendChild(fields);
    return card;
  }

  function renderList(){
    const card = el("section", { class: "br-card" });
    const total = state.backups.reduce((n, b) => n + Number(b.size || 0), 0);
    const count = state.backups.length;
    const sub = count ? `${count} backup${count === 1 ? "" : "s"} · ${fmtBytes(total)} in total` : "Nothing saved yet.";
    const paths = state.backups.map((b) => String(b.path || ""));
    state.selected = new Set([...state.selected].filter((path) => paths.includes(path)));
    const picked = state.selected.size;
    card.appendChild(sectionHead("inventory_2", "Your backups", sub, el("button", {
      class: `br-iconbtn ${state.refreshing ? "spin" : ""}`,
      type: "button",
      title: "Refresh",
      "aria-label": "Refresh",
      disabled: state.refreshing,
      on: { click: () => refresh({ busy: true }) }
    }, [icon("refresh")])));
    if (count) {
      const all = el("input", { type: "checkbox", checked: picked === count, "aria-label": "Select all backups", on: { change: (e) => {
        state.selected = new Set(e.currentTarget.checked ? paths : []);
        renderBody();
      } } });
      all.indeterminate = picked > 0 && picked < count;
      card.appendChild(el("div", { class: "br-select-bar" }, [
        el("label", { class: "br-select-all" }, [all, el("span", { text: picked ? `${picked} selected` : "Select all" })]),
        picked ? el("button", { class: "br-btn small danger", type: "button", disabled: state.busy, on: { click: deleteSelected } }, [icon("delete"), `Delete ${picked}`]) : null
      ]));
    }
    const list = el("div", { class: "br-list", id: "br-list" });
    if (!count) {
      list.appendChild(el("div", { class: "br-empty" }, [icon("cloud_off"), el("span", { text: "Create your first backup above, or switch on automatic backups." })]));
    } else {
      state.backups.forEach((b) => list.appendChild(renderBackupRow(b)));
    }
    card.appendChild(list);
    return card;
  }

  function renderBackupRow(b){
    const path = String(b.path || "");
    const row = el("div", { class: `br-row ${state.selected.has(path) ? "selected" : ""}` });
    row.appendChild(el("input", { type: "checkbox", class: "br-row-check", checked: state.selected.has(path), "aria-label": `Select ${b.label || pathName(path)}`, on: { change: (e) => {
      if (e.currentTarget.checked) state.selected.add(path); else state.selected.delete(path);
      renderBody();
    } } }));
    const meta = [scopeLabel(b.scope), fmtBytes(b.size), fmtDate(b.created_at, b.mtime)].join(" · ");
    const main = el("div", { class: "br-main" }, [
      el("div", { class: "br-name", text: b.label || pathName(path) }),
      el("div", { class: "br-meta" }, [
        el("span", { text: meta }),
        b.external_key_required ? el("span", { class: "br-pill warn", title: "This backup can only be restored with your own key file.", text: "Needs your key" }) : null
      ])
    ]);
    const note = state.rowStatus[path];
    if (note?.text) main.appendChild(el("div", { class: `br-row-note ${note.kind || ""}`, text: note.text }));
    row.appendChild(main);
    row.appendChild(el("div", { class: "br-row-actions" }, [
      el("button", { class: "br-btn small", type: "button", title: "Put CrossWatch back to this backup", on: { click: () => restoreBackup(path) } }, [icon("settings_backup_restore"), "Restore"]),
      rowAction(path, "download", "Download", "download", downloadBackup),
      rowAction(path, "validate", "Check this backup", "verified", validateBackup),
      rowAction(path, "delete", "Delete", "delete", deleteBackup, "danger")
    ]));
    return row;
  }

  function pathName(path){
    const parts = String(path || "").split("/");
    return parts[parts.length - 1] || "Backup";
  }

  async function createNow(){
    const label = String($("br-label")?.value || "").trim() || "manual";
    state.busy = true;
    renderBody();
    try {
      toast("Creating backup...", "", 60000);
      await postJSON("/api/backups/create", { scope: state.scope, label, include_snapshots: false, include_reports: false, include_cache: false });
      const input = $("br-label");
      if (input) input.value = "";
      toast("Backup created.", "ok");
    } catch (e) {
      toast(`Backup failed: ${e.message || e}`, "warn");
    } finally {
      state.busy = false;
      await refresh().catch(() => renderBody());
    }
  }

  async function uploadBackup(e){
    const input = e.currentTarget;
    const file = input?.files?.[0];
    if (!file) return;
    try {
      const form = new FormData();
      form.append("file", file);
      toast("Importing backup...", "", 60000);
      await api("/api/backups/upload", { method: "POST", body: form });
      toast("Backup imported. It is in the list below.", "ok");
      input.value = "";
      await refresh();
    } catch (err) {
      toast(`Import failed: ${err.message || err}`, "warn");
    }
  }

  function downloadBackup(path){
    if (!path) return;
    window.location.href = `/api/backups/download?path=${encodeURIComponent(path)}`;
    flashRowIcon(path, "download", true);
  }

  async function validateBackup(path){
    try {
      state.rowStatus[path] = { kind: "", text: "Checking backup..." };
      renderBody();
      const res = await postJSON("/api/backups/validate", { path });
      const errors = res?.validation?.errors || [];
      state.rowStatus[path] = errors.length
        ? { kind: "warn", text: `Found ${errors.length} problem${errors.length === 1 ? "" : "s"}. Do not rely on this backup.` }
        : { kind: "ok", text: "This backup is complete and can be restored." };
      flashRowIcon(path, "validate", !errors.length);
    } catch (e) {
      state.rowStatus[path] = { kind: "warn", text: `Check failed: ${e.message || e}` };
      flashRowIcon(path, "validate", false);
    }
  }

  async function restoreBackup(path){
    if (!path) return;
    const backup = state.backups.find((b) => String(b.path || "") === path) || {};
    const ok = window.confirm(`Restore "${backup.label || pathName(path)}" (${scopeLabel(backup.scope)})?\n\nCrossWatch first saves a backup of how things are now, then restores and restarts.`);
    if (!ok) return;
    try {
      state.rowStatus[path] = { kind: "", text: "Restoring..." };
      renderBody();
      await postJSON("/api/backups/restore", { path, restart: true });
      state.rowStatus[path] = { kind: "ok", text: "Restored. CrossWatch is restarting..." };
      renderBody();
      setTimeout(() => { try { window.location.reload(); } catch {} }, 2400);
    } catch (e) {
      state.rowStatus[path] = { kind: "warn", text: `Restore failed: ${e.message || e}` };
      renderBody();
    }
  }

  async function deleteBackup(path){
    if (!path) return;
    if (!window.confirm("Delete this backup? This cannot be undone.")) return;
    try {
      await postJSON("/api/backups/delete", { path });
      flashRowIcon(path, "delete", true);
      setTimeout(() => { refresh().catch(() => {}); }, 800);
    } catch (e) {
      state.rowStatus[path] = { kind: "warn", text: `Delete failed: ${e.message || e}` };
      flashRowIcon(path, "delete", false);
    }
  }

  async function deleteSelected(){
    const paths = [...state.selected];
    if (!paths.length) return;
    if (!window.confirm(`Delete ${paths.length} backup${paths.length === 1 ? "" : "s"}? This cannot be undone.`)) return;
    state.busy = true;
    renderBody();
    let failed = 0;
    for (const path of paths) {
      try {
        await postJSON("/api/backups/delete", { path });
        state.selected.delete(path);
      } catch (e) {
        failed += 1;
        state.rowStatus[path] = { kind: "warn", text: `Delete failed: ${e.message || e}` };
      }
    }
    state.busy = false;
    await refresh().catch(() => renderBody());
    toast(failed ? `${paths.length - failed} deleted, ${failed} could not be deleted.` : `${paths.length} backup${paths.length === 1 ? "" : "s"} deleted.`, failed ? "warn" : "ok");
  }

  function readScheduleForm(){
    const dayChecks = Array.from(document.querySelectorAll("#cw-backups-modal .br-days input[type=checkbox]"));
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
    const form = readScheduleForm();
    if (form.enabled && !form.days.length) {
      toast("Pick at least one day for automatic backups.", "warn");
      return;
    }
    try {
      const res = await postJSON("/api/backups/schedule", form);
      state.schedule = res.schedule || {};
      toast(form.enabled ? "Automatic backups saved." : "Automatic backups are off.", "ok");
      renderBody();
    } catch (e) {
      toast(`Could not save the schedule: ${e.message || e}`, "warn");
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
      const body = $("br-body");
      if (body) body.replaceChildren();
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
    refresh().catch((e) => toast(`Could not load backups: ${e.message || e}`, "warn"));
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
