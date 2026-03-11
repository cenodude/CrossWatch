/* assets/js/scheduler.js */
/* CrossWatch - Advanced Scheduling UI */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(() => {
  "use strict";
  if (window.__SCHED_UI_INIT__) return; window.__SCHED_UI_INIT__ = true;

  // tiny helpers
  const $ = (s, r = document) => r.querySelector(s);
  const el = (t, c) => Object.assign(document.createElement(t), c ? { className: c } : {});

  const fieldKey = (value, fallback = "field") => String(value || fallback).replace(/[^a-z0-9_-]+/gi, "_");

  // stable id (UUID when possible)
  const genId = (() => {
    const withCrypto = () => {
      try {
        const b = new Uint8Array(16); crypto.getRandomValues(b);
        b[6] = (b[6] & 0x0f) | 0x40; b[8] = (b[8] & 0x3f) | 0x80;
        const h = [...b].map(x => x.toString(16).padStart(2, "0"));
        return `${h.slice(0,4).join("")}-${h.slice(4,6).join("")}-${h.slice(6,8).join("")}-${h.slice(8,10).join("")}-${h.slice(10).join("")}`;
      } catch { return null; }
    };
    return () => crypto?.randomUUID?.() || withCrypto() || `id_${Date.now().toString(36)}_${Math.random().toString(36).slice(2,10)}`;
  })();

  // styles (once)
  document.head.appendChild(Object.assign(el("style"), { id: "sch-css", textContent: `
#sec-scheduling{--sch-shell-bg:radial-gradient(120% 140% at 0% 0%,rgba(90,74,201,.11) 0%,rgba(90,74,201,0) 42%),radial-gradient(110% 130% at 100% 100%,rgba(41,110,214,.08) 0%,rgba(41,110,214,0) 44%),linear-gradient(180deg,rgba(8,11,18,.985),rgba(3,5,10,.99));--sch-card-bg:linear-gradient(180deg,rgba(10,13,21,.92),rgba(4,6,11,.96));--sch-card-bg-soft:linear-gradient(180deg,rgba(14,18,30,.84),rgba(5,7,13,.9));--sch-border:rgba(255,255,255,.08);--sch-border-soft:rgba(255,255,255,.055);--sch-shadow:0 22px 56px rgba(0,0,0,.4),inset 0 1px 0 rgba(255,255,255,.03);--sch-fg:#f3f7ff;--sch-fg-soft:rgba(208,217,233,.7)}
#sec-scheduling .cw-subpanel[data-sub]{padding-top:6px}
#sec-scheduling .cw-subpanel[data-sub="basic"] .auth-card,#schAdv{position:relative;border:1px solid var(--sch-border);border-radius:22px;background:var(--sch-shell-bg);box-shadow:var(--sch-shadow);overflow:hidden}
#sec-scheduling .cw-subpanel[data-sub="basic"] .auth-card::before,#schAdv::before{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(135deg,rgba(255,255,255,.045),transparent 38%),radial-gradient(90% 120% at 0% 0%,rgba(92,76,204,.1),transparent 52%)}
#sec-scheduling .cw-subpanel[data-sub="basic"] .auth-card-fields{position:relative;z-index:1;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;padding:16px}
#sec-scheduling .cw-subpanel[data-sub="basic"] .field{display:grid;gap:8px;padding:14px 16px;border:1px solid var(--sch-border-soft);border-radius:18px;background:var(--sch-card-bg);box-shadow:inset 0 1px 0 rgba(255,255,255,.025)}
#sec-scheduling .cw-subpanel[data-sub="basic"] .field:first-child{grid-column:1 / -1}
#sec-scheduling .cw-subpanel[data-sub="basic"] .field>.muted{margin:0!important;font-size:11px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:rgba(214,223,238,.58)}
#sec-scheduling .cw-subpanel[data-sub="basic"] .auth-card-notes{margin-top:0;color:var(--sch-fg-soft);font-size:12px;line-height:1.45}
#sec-scheduling .cw-subpanel[data-sub="basic"] select,#sec-scheduling .cw-subpanel[data-sub="basic"] input[type=time],#sec-scheduling .cw-subpanel[data-sub="basic"] input[type=number],.sch-adv select,.sch-adv input[type=time]{width:100%;min-height:46px;padding:0 14px;border:1px solid rgba(255,255,255,.08);border-radius:16px;background:linear-gradient(180deg,rgba(4,6,11,.94),rgba(2,4,8,.98));color:var(--sch-fg);box-shadow:inset 0 1px 0 rgba(255,255,255,.02);transition:border-color .18s ease,background .18s ease,box-shadow .18s ease,transform .18s ease}
#sec-scheduling .cw-subpanel[data-sub="basic"] select:hover,#sec-scheduling .cw-subpanel[data-sub="basic"] input[type=time]:hover,#sec-scheduling .cw-subpanel[data-sub="basic"] input[type=number]:hover,.sch-adv select:hover,.sch-adv input[type=time]:hover{border-color:rgba(255,255,255,.13);background:linear-gradient(180deg,rgba(7,10,18,.96),rgba(3,5,10,.985))}
#sec-scheduling .cw-subpanel[data-sub="basic"] select:focus,#sec-scheduling .cw-subpanel[data-sub="basic"] input[type=time]:focus,#sec-scheduling .cw-subpanel[data-sub="basic"] input[type=number]:focus,.sch-adv select:focus,.sch-adv input[type=time]:focus{outline:none;border-color:rgba(122,120,255,.42);box-shadow:0 0 0 3px rgba(101,107,255,.12),inset 0 1px 0 rgba(255,255,255,.025)}
.sch-adv{padding:16px}
.sch-adv .cw-panel-head{position:relative;z-index:1;display:flex;align-items:center;min-height:104px;margin:0 0 14px;padding:14px 16px;border:1px solid var(--sch-border-soft);border-radius:18px;background:var(--sch-card-bg);box-shadow:inset 0 1px 0 rgba(255,255,255,.025)}
.sch-adv .cw-panel-head::before{content:"Enable";position:absolute;top:14px;left:16px;font-size:11px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:rgba(214,223,238,.58)}
.sch-adv .cw-panel-head .cx-toggle{margin-top:20px}
.sch-adv .mini,.sch-adv .status{position:relative;z-index:1}
.sch-adv .mini{font-size:12px;color:var(--sch-fg-soft)}
.sch-adv .status{display:flex;align-items:center;min-height:22px;font-size:12px;font-weight:700;color:rgba(255,214,128,.88)}
.sch-adv .status:empty{display:none}
.sch-adv table{position:relative;z-index:1;width:100%;border-collapse:separate;border-spacing:0 10px;margin-top:0}
.sch-adv thead th{padding:0 10px 6px;text-align:left;font-size:11px;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:rgba(214,223,238,.56);border-bottom:none}
.sch-adv tbody tr{background:var(--sch-card-bg);box-shadow:inset 0 1px 0 rgba(255,255,255,.02)}
.sch-adv tbody td{padding:12px 10px;vertical-align:middle;border-top:1px solid var(--sch-border-soft);border-bottom:1px solid var(--sch-border-soft)}
.sch-adv tbody td:first-child{border-left:1px solid var(--sch-border-soft);border-radius:18px 0 0 18px}
.sch-adv tbody td:last-child{border-right:1px solid var(--sch-border-soft);border-radius:0 18px 18px 0}
.sch-adv .chipdays{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;align-items:center}
.sch-adv .chipdays label{display:inline-flex;align-items:center;justify-content:center;gap:7px;min-height:38px;padding:0 10px;border:1px solid var(--sch-border-soft);border-radius:999px;cursor:pointer;width:100%;background:var(--sch-card-bg-soft);color:rgba(236,241,251,.78);font-size:12px;font-weight:700;transition:border-color .18s ease,background .18s ease,transform .18s ease,color .18s ease}
.sch-adv .chipdays label:hover{transform:translateY(-1px);border-color:rgba(255,255,255,.13);background:linear-gradient(180deg,rgba(16,20,33,.92),rgba(7,9,15,.96))}
.sch-adv .chipdays input{accent-color:#7c76ff;transform:translateY(1px)}
.sch-adv .chipdays label:has(input:checked){color:#f7f9ff;border-color:rgba(122,120,255,.34);background:linear-gradient(180deg,rgba(89,86,196,.22),rgba(16,18,34,.96))}
.sch-adv .chipdays .chipspacer{width:100%;height:1px;visibility:hidden;pointer-events:none}
.sch-adv .row-disabled{opacity:.5;filter:grayscale(.24)}
.sch-adv option[disabled]{color:#666}
.sch-adv-actions{position:relative;z-index:1;display:flex;gap:10px;flex-wrap:wrap;margin-top:12px}
.sch-adv .btn,.sch-adv .btn.ghost{min-height:40px;padding:0 14px;border-radius:999px;border:1px solid rgba(255,255,255,.09);background:linear-gradient(180deg,rgba(255,255,255,.065),rgba(255,255,255,.03));color:var(--sch-fg);box-shadow:inset 0 1px 0 rgba(255,255,255,.03);transition:transform .18s ease,background .18s ease,border-color .18s ease}
.sch-adv .btn:hover,.sch-adv .btn.ghost:hover{transform:translateY(-1px);border-color:rgba(255,255,255,.14);background:linear-gradient(180deg,rgba(110,112,255,.16),rgba(255,255,255,.04))}
.sch-adv tbody .btn.ghost{min-width:38px;padding:0 12px}
.sch-adv.adv-disabled{opacity:.55;filter:saturate(.75)}
.sch-std-toggle{margin-top:0}
@media (max-width:980px){#sec-scheduling .cw-subpanel[data-sub="basic"] .auth-card-fields{grid-template-columns:1fr}.sch-adv .chipdays{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media (max-width:760px){.sch-adv{padding:14px}.sch-adv .cw-panel-head{min-height:0;padding:14px}.sch-adv .cw-panel-head .cx-toggle{margin-top:18px}.sch-adv table,.sch-adv thead,.sch-adv tbody,.sch-adv tr,.sch-adv td,.sch-adv th{display:block}.sch-adv thead{display:none}.sch-adv tbody{display:grid;gap:10px}.sch-adv tbody tr{border:1px solid var(--sch-border-soft);border-radius:18px;overflow:hidden}.sch-adv tbody td{display:grid;gap:6px;border:none!important;border-radius:0!important;padding:10px 12px}.sch-adv tbody td::before{font-size:10px;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:rgba(214,223,238,.56)}.sch-adv tbody td:nth-child(1)::before{content:"Pair"}.sch-adv tbody td:nth-child(2)::before{content:"Time"}.sch-adv tbody td:nth-child(3)::before{content:"Days"}.sch-adv tbody td:nth-child(4)::before{content:"After"}.sch-adv tbody td:nth-child(5)::before{content:"Active"}.sch-adv tbody td:nth-child(6)::before{content:"Remove"}.sch-adv .chipdays{grid-template-columns:repeat(2,minmax(0,1fr))}}
` }));

  // state
  let _pairs = [], _jobs = [], _advEnabled = false, _loading = false;
  const DAY = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"];
  const setBooleanSelect = (sel, v) => {
    if (!sel) return;
    const want = v ? "true" : "false";
    const opts = [...(sel.options || [])];
    let hit = opts.find(o => String(o.value).trim().toLowerCase() === want);
    if (!hit) {
      const labels = v ? ["enabled","enable","on","yes","true","1"] : ["disabled","disable","off","no","false","0"];
      hit = opts.find(o => labels.includes(String(o.textContent).trim().toLowerCase()));
    }
    if (hit) sel.value = hit.value;
  };

const ensureStdEnabledToggle = () => {
  const sel = $("#schEnabled");
  if (!sel || sel.__toggleEnhanced) return;
  const box = sel.parentElement;
  if (!box) return;

  const lab = box.querySelector("label");
  if (lab) lab.remove();
  sel.style.display = "none";

  const t = el("label", "cx-toggle sch-std-toggle");
  t.innerHTML = `<input type="checkbox" id="schEnabledToggle"><span class="cx-toggle-ui" aria-hidden="true"></span><span class="cx-toggle-text"></span><span class="cx-toggle-state" aria-hidden="true"></span>`;
  box.appendChild(t);

  const cb = $("#schEnabledToggle", box);
  const syncFromSel = () => { cb.checked = String(sel.value || "").trim().toLowerCase() === "true"; };
  sel.__toggleSync = syncFromSel;
  syncFromSel();

  cb.onchange = () => {
    // Standard on -> force advanced off
    if (cb.checked) {
      const advCb = $("#schAdvEnabled");
      if (advCb && advCb.checked) advCb.checked = false;
      _advEnabled = !!$("#schAdvEnabled")?.checked;
    }

    setBooleanSelect(sel, cb.checked);
    try { sel.dispatchEvent(new Event("change", { bubbles: true })); } catch {}
    try { sel.dispatchEvent(new Event("input", { bubbles: true })); } catch {}

    applyModeLocks();
  };

  sel.addEventListener("change", syncFromSel);
  sel.__toggleEnhanced = true;
};

  const setAdvDisabled = (disabled) => {
    const adv = $("#schAdv");
    if (!adv) return;
    adv.classList.toggle("adv-disabled", !!disabled);
    [...adv.querySelectorAll("select,input,button")].forEach((n) => {
      if (n && n.id === "schAdvEnabled") return;
      n.disabled = !!disabled;
    });
  };

  const decorateStandardPanel = () => {
    const basic = $("#sec-scheduling .cw-subpanel[data-sub='basic'] .auth-card");
    if (!basic) return;
  };

  const applyModeLocks = () => {
    const sel = $("#schEnabled");
    const stdToggle = $("#schEnabledToggle");
    const advCb = $("#schAdvEnabled");

    const stdEnabled = String(sel?.value || "").trim().toLowerCase() === "true";
    const advEnabled = !!advCb?.checked;

    if (advEnabled) {
      // Advanced on -> force standard off
      if (stdEnabled) {
        setBooleanSelect(sel, false);
        try { sel.dispatchEvent(new Event("change", { bubbles: true })); } catch {}
        try { sel.dispatchEvent(new Event("input", { bubbles: true })); } catch {}
      }
      if (stdToggle) stdToggle.checked = false;
    } else if (stdEnabled) {
      // Standard on -> force advanced off
      if (advCb && advCb.checked) advCb.checked = false;
      _advEnabled = !!advCb?.checked;
    }

    const advOn = !!advCb?.checked;
    const stdOn = String(sel?.value || "").trim().toLowerCase() === "true";

    // Lock standard fields when advanced is on
    const lockStdFields = advOn;
    ["schMode", "schN", "schTime"].forEach((id) => {
      const n = $("#" + id);
      if (n) n.disabled = lockStdFields;
    });

    // Lock advanced fields when standard is on 
    setAdvDisabled(stdOn);

    try { window.refreshSchedulingBanner?.(); } catch {}
    try { window.cwSchedSettingsHubUpdate?.(); } catch {}
  };

  // data
  const fetchPairs = async () => {
    try {
      const r = await fetch("/api/pairs", { cache: "no-store" });
      const arr = await r.json();
      _pairs = Array.isArray(arr) ? arr.map(p => ({
        id: String(p.id),
        label: `${String(p.source || "").toUpperCase()} -> ${String(p.target || "").toUpperCase()} ${String(p.mode || "")}`.trim(),
        enabled: !!p.enabled
      })) : [];
    } catch (e) { console.warn("[scheduler] /api/pairs failed", e); _pairs = []; }
  };
  const isEnabled = pid => !!_pairs.find(p => String(p.id) === String(pid) && p.enabled);

  // row builder
  const jobRow = j => {
    const tr = el("tr"); if (j.active !== false && j.pair_id && !isEnabled(j.pair_id)) tr.classList.add("row-disabled");
    const rowKey = fieldKey(j?.id, `job_${_jobs.indexOf(j) + 1}`);

    // pair
    const tdPair = el("td"), sel = el("select");
    sel.id = `sched_pair_${rowKey}`;
    sel.name = sel.id;
    sel.appendChild(Object.assign(el("option"), { value: "", textContent: "Select pair" }));
    _pairs.forEach(p => {
      const o = Object.assign(el("option"), { value: p.id, textContent: p.label + (p.enabled ? "" : " (disabled)"), disabled: !p.enabled, selected: String(j.pair_id||"") === p.id });
      sel.appendChild(o);
    });
    sel.onchange = () => j.pair_id = sel.value || null; tdPair.appendChild(sel);

    // time
    const tdTime = el("td"), t = Object.assign(el("input"), { type: "time", value: j.at || "" });
    t.id = `sched_time_${rowKey}`;
    t.name = t.id;
    t.onchange = () => j.at = (t.value || "").trim() || null; tdTime.appendChild(t);

    // days
    const tdDays = el("td"), wrap = el("div","chipdays"), cur = new Set(Array.isArray(j.days) ? j.days : []);
    DAY.forEach((d,i) => {
      const lab = el("label"), chk = Object.assign(el("input"), { type: "checkbox", checked: cur.has(i+1) }), txt = el("span");
      chk.id = `sched_days_${rowKey}_${i+1}`;
      chk.name = `sched_days_${rowKey}[]`;
      chk.onchange = () => { const S = new Set(Array.isArray(j.days) ? j.days : []); chk.checked ? S.add(i+1) : S.delete(i+1); j.days = [...S].sort((a,b)=>a-b); };
      txt.textContent = d;
      lab.append(chk, txt); wrap.appendChild(lab);
    if(i===2){ wrap.appendChild(el("span","chipspacer")); }
    });
    tdDays.appendChild(wrap);

    // after
    const tdAfter = el("td"), sa = el("select");
    sa.id = `sched_after_${rowKey}`;
    sa.name = sa.id;
    sa.appendChild(Object.assign(el("option"), { value: "", textContent: "None" }));
    _jobs.filter(x => x !== j).forEach((x,i) => sa.appendChild(Object.assign(el("option"), { value: String(x.id), textContent: `Step ${i+1}`, selected: String(j.after||"") === String(x.id) })));
    sa.onchange = () => { j.after = sa.value || null; renderJobs(); }; tdAfter.appendChild(sa);

    // active
    const tdOn = el("td"), c = Object.assign(el("input"), { type: "checkbox", checked: j.active !== false });
    c.id = `sched_active_${rowKey}`;
    c.name = c.id;
    c.onchange = () => { j.active = !!c.checked; renderJobs(); }; tdOn.appendChild(c);

    // delete
    const tdDel = el("td"), del = Object.assign(el("button"), { className: "btn ghost", textContent: "x" });
    del.onclick = () => { _jobs = _jobs.filter(x => x !== j); renderJobs(); }; tdDel.appendChild(del);

    tr.append(tdPair, tdTime, tdDays, tdAfter, tdOn, tdDel);
    return tr;
  };

  // mount UI
  const ensureUI = () => {
    const host = $("#sched_advanced_mount") || $("#sec-scheduling .body");
    if (!host || $("#schAdv")) return;

    const adv = Object.assign(el("div", "sch-adv"), { id: "schAdv" });
    adv.innerHTML = `
<div class="cw-panel-head">
  <label class="cx-toggle">
    <input type="checkbox" id="schAdvEnabled">
    <span class="cx-toggle-ui" aria-hidden="true"></span>
    <span class="cx-toggle-text">Use advanced plan</span>
    <span class="cx-toggle-state" aria-hidden="true"></span>
  </label>
</div>

<table>
  <thead><tr>
    <th style="width:32%">Pair</th>
    <th style="width:14%">Time</th>
    <th style="width:30%">Days</th>
    <th style="width:14%">After</th>
    <th style="width:6%">Active</th>
    <th style="width:4%"></th>
  </tr></thead>
  <tbody id="schJobsBody"></tbody>
</table>

<div class="mini">Only enabled pairs are selectable; disabled pairs are greyed out.</div>
<div class="status" id="schAdvStatus"></div>
<div class="sch-adv-actions">
  <button class="btn" id="btnAddStep">Add step</button>
  <button class="btn" id="btnAutoFromPairs">Auto-create from enabled pairs</button>
</div>`;
    host.appendChild(adv);

    $("#btnAddStep").onclick = () => { _jobs.push({ id: genId(), pair_id: null, at: null, days: [], after: null, active: true }); renderJobs(); };
    $("#btnAutoFromPairs").onclick = () => {
      const eps = _pairs.filter(p => p.enabled);
      _jobs = eps.map(p => ({ id: genId(), pair_id: p.id, at: null, days: [], after: null, active: true }));
      if (!_jobs.length) _jobs.push({ id: genId(), pair_id: null, at: null, days: [], after: null, active: true });
      renderJobs();
    };
    $("#schAdvEnabled").onchange = () => {
      _advEnabled = !!$("#schAdvEnabled").checked;

      // Advanced on -> force standard off
      if (_advEnabled) {
        const sel = $("#schEnabled");
        setBooleanSelect(sel, false);
        try { sel.dispatchEvent(new Event("change", { bubbles: true })); } catch {}
        try { sel.dispatchEvent(new Event("input", { bubbles: true })); } catch {}
        try { sel.__toggleSync?.(); } catch {}
      }

      applyModeLocks();
    };
  };

  // render
  const renderJobs = () => {
    const tbody = $("#schJobsBody"); if (!tbody) return;
    tbody.innerHTML = "";
    if (!_jobs.length) _jobs.push({ id: genId(), pair_id: null, at: null, days: [], after: null, active: true });
    _jobs.forEach(j => j._blocked = j.active !== false && j.pair_id && !isEnabled(j.pair_id));
    _jobs.forEach(j => tbody.appendChild(jobRow(j)));
    const st = $("#schAdvStatus");
    st.textContent = !_pairs.length ? "No pairs from /api/pairs." : (_jobs.some(j => j._blocked) ? "Some steps reference disabled pairs." : "");
    try { window.cwSchedSettingsHubUpdate?.(); } catch {}
  };

  // load
  const loadScheduling = async () => {
    if (_loading) return; _loading = true;
    try {
      ensureUI();
      decorateStandardPanel();
      await fetchPairs();

      let saved = {};
      try { saved = await fetch(`/api/scheduling?t=${Date.now()}`, { cache: "no-store" }).then(r => r.json()); } catch {}

      setBooleanSelect($("#schEnabled"), !!saved.enabled);
      ensureStdEnabledToggle();
      try { $("#schEnabled")?.__toggleSync?.(); } catch {}
      $("#schMode") && ($("#schMode").value = saved.mode || "hourly");
      $("#schN")    && ($("#schN").value = String(saved.every_n_hours || 2));
      $("#schTime") && ($("#schTime").value = saved.daily_time || "03:30");

      const adv = saved?.advanced || {};
      _advEnabled = !!adv.enabled;
      $("#schAdvEnabled") && ($("#schAdvEnabled").checked = _advEnabled);
      _jobs = Array.isArray(adv.jobs) ? adv.jobs.map(j => ({
        id: j.id || genId(),
        pair_id: j.pair_id || null,
        at: j.at || null,
        days: Array.isArray(j.days) ? j.days.filter(n => n >= 1 && n <= 7) : [],
        after: j.after || null,
        active: j.active !== false
      })) : [];
      renderJobs();

      applyModeLocks();

      try { window.cwSchedSettingsHubInit?.(); } catch {}
      try { window.cwSchedSettingsHubUpdate?.(); } catch {}

      try { typeof window.refreshSchedulingBanner === "function" && window.refreshSchedulingBanner(); } catch {}
    } finally { _loading = false; }
  };
  window.loadScheduling = loadScheduling;

  // serialize advanced
  const serializeAdvanced = () => ({
    enabled: !!_advEnabled,
    jobs: _jobs.map(j => ({
      id: j.id,
      pair_id: j.pair_id || null,
      at: j.at || null,
      days: Array.isArray(j.days) ? j.days.slice() : [],
      after: j.after || null,
      active: j.active !== false
    }))
  });

  // public getter for current scheduling patch
  window.getSchedulingPatch = () => {
    const mode = $("#schMode")?.value || "hourly";
    const every_n_hours = parseInt($("#schN")?.value || "2", 10);
    const daily_time = $("#schTime")?.value || "03:30";
    const advanced = serializeAdvanced();

    // Advanced plan disables standard scheduling
    const stdEnabled = ($("#schEnabled")?.value || "").trim() === "true";
    const enabled = advanced.enabled ? false : stdEnabled;

    return { enabled, mode, every_n_hours, daily_time, advanced };
  };

  // boot
  document.addEventListener("DOMContentLoaded", () => {
    loadScheduling().catch(e => console.warn("scheduler load failed", e));
    try { window.dispatchEvent(new Event("sched-banner-ready")); } catch {}
  });

  document.addEventListener("config-saved", (e) => {
    const section = e?.detail?.section;
    if (section && section !== "scheduling") return;
    loadScheduling().catch(err => console.warn("scheduler reload failed", err));
  });
})();
