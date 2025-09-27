// /assets/js/scheduler.js 
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
.sch-adv{margin-top:12px;padding:16px;border:1px solid var(--border);border-radius:12px;background:var(--panel2)}
.sch-adv summary{font-weight:700;letter-spacing:.02em;cursor:pointer;list-style:none;display:flex;align-items:center;justify-content:space-between}
.sch-adv summary::-webkit-details-marker{display:none}
.sch-adv .mini{font-size:12px;color:var(--muted)}
.sch-adv table{width:100%;border-collapse:collapse;margin-top:8px}
.sch-adv th,.sch-adv td{text-align:left;padding:10px 8px;border-bottom:1px solid var(--border);vertical-align:middle}
.sch-adv th{font-weight:600;color:var(--muted)}
.sch-adv select,.sch-adv input[type=time]{width:100%}
.sch-adv .chipdays{display:flex;gap:6px;flex-wrap:wrap}
.sch-adv .chipdays label{display:inline-flex;align-items:center;gap:6px;padding:6px 8px;border:1px solid var(--border);border-radius:10px;cursor:pointer}
.sch-adv .chipdays input{transform:translateY(1px)}
.sch-adv .row-disabled{opacity:.55;filter:grayscale(.25)}
.sch-adv option[disabled]{color:#666}
.sch-adv .status{margin-top:10px;min-height:20px}
` }));

  // state
  let _pairs = [], _jobs = [], _advEnabled = false, _loading = false;
  const DAY = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"];

  // select true/false in vendor dropdowns
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

  // mount UI (idempotent)
  const ensureUI = () => {
    const host = $("#sec-scheduling .body"); if (!host || $("#schAdv")) return;
    const adv = Object.assign(el("details","sch-adv"), { id: "schAdv" });
    adv.innerHTML = `
<summary><span>Advanced plan (sequential)</span><span class="mini">Click to expand</span></summary>
<div style="margin-top:12px">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
    <label class="mini"><input type="checkbox" id="schAdvEnabled"> Use advanced plan</label>
    <span class="mini">Only enabled pairs are selectable; disabled pairs are greyed-out.</span>
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
  <div class="mini" style="margin-top:8px">Always sequential. Times are user-defined.</div>
  <div class="status" id="schAdvStatus"></div>
  <div style="display:flex;gap:8px;margin-top:10px">
    <button class="btn" id="btnAddStep">Add step</button>
    <button class="btn" id="btnAutoFromPairs">Auto-create from enabled pairs</button>
  </div>
</div>`;
    host.appendChild(adv);

    $("#btnAddStep").onclick = () => { _jobs.push({ id: genId(), pair_id: null, at: null, days: [], after: null, active: true }); renderJobs(); };
    $("#btnAutoFromPairs").onclick = () => {
      const eps = _pairs.filter(p => p.enabled);
      _jobs = eps.map(p => ({ id: genId(), pair_id: p.id, at: null, days: [], after: null, active: true }));
      if (!_jobs.length) _jobs.push({ id: genId(), pair_id: null, at: null, days: [], after: null, active: true });
      renderJobs();
    };
    $("#schAdvEnabled").onchange = () => { _advEnabled = !!$("#schAdvEnabled").checked; };
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
  };

  // load (guarded)
  const loadScheduling = async () => {
    if (_loading) return; _loading = true;
    try {
      ensureUI();
      await fetchPairs();

      let saved = {};
      try { saved = await fetch(`/api/scheduling?t=${Date.now()}`, { cache: "no-store" }).then(r => r.json()); } catch {}

      setBooleanSelect($("#schEnabled"), !!saved.enabled);
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

      try { typeof window.refreshSchedulingBanner === "function" && window.refreshSchedulingBanner(); } catch {}
    } finally { _loading = false; }
  };
  window.loadScheduling = loadScheduling; // exposed for global flows

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

  // public patch accessor (used by Save)
  window.getSchedulingPatch = () => {
    const enabled = ($("#schEnabled")?.value || "").trim() === "true";
    const mode = $("#schMode")?.value || "hourly";
    const every_n_hours = parseInt($("#schN")?.value || "2", 10);
    const daily_time = $("#schTime")?.value || "03:30";
    const advanced = serializeAdvanced();
    return { enabled, mode, every_n_hours, daily_time, advanced };
  };

  // boot
  document.addEventListener("DOMContentLoaded", () => {
    loadScheduling().catch(e => console.warn("scheduler load failed", e));
    try { window.dispatchEvent(new Event("sched-banner-ready")); } catch {}
  });
})();
