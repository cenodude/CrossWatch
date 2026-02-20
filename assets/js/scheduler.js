/* assets/js/scheduler.js */
/* CrossWatch - Advanced Scheduling UI */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(() => {
  "use strict";
  if (window.__SCHED_UI_INIT__) return; window.__SCHED_UI_INIT__ = true;

  // tiny helpers
  const $ = (s, r = document) => r.querySelector(s);
  const el = (t, c) => Object.assign(document.createElement(t), c ? { className: c } : {});

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
.sch-adv{padding:0;margin-top:0}
.sch-adv .mini{font-size:12px;color:var(--muted)}
.sch-adv table{width:100%;border-collapse:collapse;margin-top:10px}
.sch-adv th,.sch-adv td{text-align:left;padding:10px 8px;border-bottom:1px solid var(--border);vertical-align:middle}
.sch-adv th{font-weight:600;color:var(--muted)}
.sch-adv select,.sch-adv input[type=time]{width:100%}
.sch-adv .chipdays{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:6px;align-items:center}
.sch-adv .chipdays label{display:inline-flex;align-items:center;justify-content:center;gap:6px;padding:6px 8px;border:1px solid var(--border);border-radius:10px;cursor:pointer;width:100%}
.sch-adv .chipdays input{transform:translateY(1px)}
.sch-adv .chipdays .chipspacer{width:100%;height:1px;visibility:hidden;pointer-events:none}
.sch-adv .row-disabled{opacity:.55;filter:grayscale(.25)}
.sch-adv option[disabled]{color:#666}
.sch-adv .status{margin-top:10px;min-height:20px}

/* Shared toggle  */
.cx-toggle{position:relative;display:inline-flex;align-items:center;gap:10px;cursor:pointer;user-select:none}
.cx-toggle input{position:absolute;opacity:0;width:1px;height:1px;pointer-events:none}
.cx-toggle-ui{display:inline-block;width:46px;height:26px;border-radius:999px;background:rgba(255,255,255,.10);border:1px solid rgba(255,255,255,.14);position:relative;box-shadow:inset 0 0 0 1px rgba(0,0,0,.18);transition:background .15s ease,border-color .15s ease,box-shadow .15s ease}
.cx-toggle-ui:after{content:"";position:absolute;top:3px;left:3px;width:20px;height:20px;border-radius:999px;background:rgba(255,255,255,.92);box-shadow:0 8px 18px rgba(0,0,0,.35);transition:transform .15s ease,background .15s ease}
.cx-toggle-text{display:inline-block;font-size:12px;opacity:.9;white-space:nowrap}
.cx-toggle-state{display:inline-flex;align-items:center;font-size:11px;padding:2px 8px;border-radius:999px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.08);opacity:.85}
.cx-toggle-state:before{content:"Off"}
.cx-toggle:hover .cx-toggle-ui{border-color:rgba(255,255,255,.22)}
.cx-toggle input:checked + .cx-toggle-ui{background:rgba(34,197,94,.28);border-color:rgba(34,197,94,.45)}
.cx-toggle input:checked + .cx-toggle-ui:after{transform:translateX(20px)}
.cx-toggle input:checked ~ .cx-toggle-state:before{content:"On"}
.cx-toggle input:focus-visible + .cx-toggle-ui{box-shadow:0 0 0 2px rgba(255,255,255,.14),0 0 0 6px rgba(34,197,94,.15),inset 0 0 0 1px rgba(0,0,0,.18)}
.sch-std-toggle{margin-top:0}
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
        label: `${String(p.source||"").toUpperCase()} → ${String(p.target||"").toUpperCase()} ${String(p.mode||"")}`.trim(),
        enabled: !!p.enabled
      })) : [];
    } catch (e) { console.warn("[scheduler] /api/pairs failed", e); _pairs = []; }
  };
  const isEnabled = pid => !!_pairs.find(p => String(p.id) === String(pid) && p.enabled);

  // row builder
  const jobRow = j => {
    const tr = el("tr"); if (j.active !== false && j.pair_id && !isEnabled(j.pair_id)) tr.classList.add("row-disabled");

    // pair
    const tdPair = el("td"), sel = el("select");
    sel.appendChild(Object.assign(el("option"), { value: "", textContent: "— select pair —" }));
    _pairs.forEach(p => {
      const o = Object.assign(el("option"), { value: p.id, textContent: p.label + (p.enabled ? "" : " (disabled)"), disabled: !p.enabled, selected: String(j.pair_id||"") === p.id });
      sel.appendChild(o);
    });
    sel.onchange = () => j.pair_id = sel.value || null; tdPair.appendChild(sel);

    // time
    const tdTime = el("td"), t = Object.assign(el("input"), { type: "time", value: j.at || "" });
    t.onchange = () => j.at = (t.value || "").trim() || null; tdTime.appendChild(t);

    // days
    const tdDays = el("td"), wrap = el("div","chipdays"), cur = new Set(Array.isArray(j.days) ? j.days : []);
    DAY.forEach((d,i) => {
      const lab = el("label"), chk = Object.assign(el("input"), { type: "checkbox", checked: cur.has(i+1) });
      chk.onchange = () => { const S = new Set(Array.isArray(j.days) ? j.days : []); chk.checked ? S.add(i+1) : S.delete(i+1); j.days = [...S].sort((a,b)=>a-b); };
      lab.append(chk, document.createTextNode(d)); wrap.appendChild(lab);
    if(i===2){ wrap.appendChild(el("span","chipspacer")); }
    });
    tdDays.appendChild(wrap);

    // after
    const tdAfter = el("td"), sa = el("select");
    sa.appendChild(Object.assign(el("option"), { value: "", textContent: "— none —" }));
    _jobs.filter(x => x !== j).forEach((x,i) => sa.appendChild(Object.assign(el("option"), { value: String(x.id), textContent: `Step ${i+1}`, selected: String(j.after||"") === String(x.id) })));
    sa.onchange = () => { j.after = sa.value || null; renderJobs(); }; tdAfter.appendChild(sa);

    // active
    const tdOn = el("td"), c = Object.assign(el("input"), { type: "checkbox", checked: j.active !== false });
    c.onchange = () => { j.active = !!c.checked; renderJobs(); }; tdOn.appendChild(c);

    // delete
    const tdDel = el("td"), del = Object.assign(el("button"), { className: "btn ghost", textContent: "✕" });
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

<div class="mini" style="margin-top:8px">Only enabled pairs are selectable; disabled pairs are greyed-out.</div>
<div class="status" id="schAdvStatus"></div>
<div style="display:flex;gap:8px;margin-top:10px">
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
})();
