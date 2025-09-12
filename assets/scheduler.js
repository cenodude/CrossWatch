// /assets/scheduler.js
// Scheduler UI: simple "run all pairs" + optional Advanced (sequential plan).
// No local save buttons; global Save uses window.getSchedulingPatch().

(() => {
  const $  = (sel, root = document) => root.querySelector(sel);
  const el = (tag, cls) => { const n = document.createElement(tag); if (cls) n.className = cls; return n; };

  // --- Stable ID generator
  const genId = (() => {
    function withCrypto() {
      try {
        if (typeof crypto !== "undefined" && typeof crypto.getRandomValues === "function") {
          const b = new Uint8Array(16); crypto.getRandomValues(b);
          b[6] = (b[6] & 0x0f) | 0x40; b[8] = (b[8] & 0x3f) | 0x80;
          const h = Array.from(b, x => x.toString(16).padStart(2, "0"));
          return `${h.slice(0,4).join("")}-${h.slice(4,6).join("")}-${h.slice(6,8).join("")}-${h.slice(8,10).join("")}-${h.slice(10).join("")}`;
        }
      } catch {}
      return null;
    }
    return () => (crypto?.randomUUID?.() || withCrypto() || `id_${Date.now().toString(36)}_${Math.random().toString(36).slice(2,10)}`);
  })();

  // --- Minimal CSS
  const CSS = `
  .sch-adv{margin-top:12px;padding:16px;border:1px solid var(--border);border-radius:12px;background:var(--panel2)}
  .sch-adv summary{font-weight:700;letter-spacing:.02em;cursor:pointer;list-style:none;display:flex;align-items:center;justify-content:space-between}
  .sch-adv summary::-webkit-details-marker{display:none}
  .sch-adv .mini{font-size:12px;color:var(--muted)}
  .sch-adv table{width:100%;border-collapse:collapse;margin-top:8px}
  .sch-adv th,.sch-adv td{text-align:left;padding:10px 8px;border-bottom:1px solid var(--border);vertical-align:middle}
  .sch-adv th{font-weight:600;color:var(--muted)}
  .sch-adv select,.sch-adv input[type="time"]{width:100%}
  .sch-adv .chipdays{display:flex;gap:6px;flex-wrap:wrap}
  .sch-adv .chipdays label{display:inline-flex;align-items:center;gap:6px;padding:6px 8px;border:1px solid var(--border);border-radius:10px;cursor:pointer}
  .sch-adv .chipdays input{transform:translateY(1px)}
  .sch-adv .row-disabled{opacity:.55;filter:grayscale(.25)}
  .sch-adv option[disabled]{color:#666}
  .sch-adv .status{margin-top:10px;min-height:20px}
  `;
  document.head.appendChild(Object.assign(el('style'), { textContent: CSS }));

  // --- State
  let _pairs = [];
  let _jobs  = [];
  let _advEnabled = false;
  const DAY = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"];

  function setBooleanSelect(selectEl, boolVal) {
    if (!selectEl) return;
    const wantVal = boolVal ? "true" : "false";
    const opts = Array.from(selectEl.options || []);
    let hit = opts.find(o => String(o.value).trim().toLowerCase() === wantVal);
    if (!hit) {
      const labels = boolVal ? ["enabled","enable","on","yes","true","1"] : ["disabled","disable","off","no","false","0"];
      hit = opts.find(o => labels.includes(String(o.textContent).trim().toLowerCase()));
    }
    if (hit) selectEl.value = hit.value;
  }

  // --- Data
  async function fetchPairs() {
    try {
      const r = await fetch('/api/pairs', { cache: 'no-store' });
      const arr = await r.json();
      _pairs = Array.isArray(arr) ? arr.map(p => ({
        id: String(p.id),
        label: `${String(p.source||'').toUpperCase()} → ${String(p.target||'').toUpperCase()} ${String(p.mode||'')}`.trim(),
        enabled: !!p.enabled
      })) : [];
    } catch (e) {
      console.warn('[scheduler] /api/pairs failed', e);
      _pairs = [];
    }
  }
  const isEnabled = (pid) => !!_pairs.find(p => String(p.id) === String(pid) && p.enabled);

  // --- UI
  function jobRow(j) {
    const tr = el('tr');
    if (j.active !== false && j.pair_id && !isEnabled(j.pair_id)) tr.classList.add('row-disabled');

    // Pair
    const tdPair = el('td');
    const sel = el('select');
    const ph = el('option'); ph.value=''; ph.textContent='— select pair —'; sel.appendChild(ph);
    _pairs.forEach(p => {
      const o = el('option');
      o.value = p.id;
      o.textContent = p.label + (p.enabled ? '' : ' (disabled)');
      if (!p.enabled) o.disabled = true;
      if (String(j.pair_id||'') === p.id) o.selected = true;
      sel.appendChild(o);
    });
    sel.onchange = () => { j.pair_id = sel.value || null; };
    tdPair.appendChild(sel);

    // Time
    const tdTime = el('td');
    const t = el('input'); t.type = 'time'; if (j.at) t.value = j.at;
    t.onchange = () => { j.at = (t.value || '').trim() || null; };
    tdTime.appendChild(t);

    // Days
    const tdDays = el('td');
    const wrap = el('div','chipdays');
    const cur = new Set(Array.isArray(j.days) ? j.days : []);
    DAY.forEach((d,i) => {
      const lab = el('label');
      const chk = el('input'); chk.type='checkbox'; chk.checked = cur.has(i+1);
      chk.onchange = () => {
        const set = new Set(Array.isArray(j.days) ? j.days : []);
        if (chk.checked) set.add(i+1); else set.delete(i+1);
        j.days = [...set].sort((a,b)=>a-b);
      };
      lab.appendChild(chk); lab.appendChild(document.createTextNode(d));
      wrap.appendChild(lab);
    });
    tdDays.appendChild(wrap);

    // After
    const tdAfter = el('td');
    const sa = el('select');
    const none = el('option'); none.value=''; none.textContent='— none —'; sa.appendChild(none);
    _jobs.filter(x => x !== j).forEach((x,i) => {
      const o = el('option'); o.value = String(x.id);
      o.textContent = `Step ${i+1}`;
      if (String(j.after||'') === String(x.id)) o.selected = true;
      sa.appendChild(o);
    });
    sa.onchange = () => { j.after = sa.value || null; renderJobs(); };
    tdAfter.appendChild(sa);

    // Active
    const tdOn = el('td');
    const c = el('input'); c.type='checkbox'; c.checked = (j.active !== false);
    c.onchange = () => { j.active = !!c.checked; renderJobs(); };
    tdOn.appendChild(c);

    // Delete
    const tdDel = el('td');
    const del = el('button'); del.className = 'btn ghost'; del.textContent = '✕';
    del.onclick = () => { _jobs = _jobs.filter(x => x !== j); renderJobs(); };
    tdDel.appendChild(del);

    tr.append(tdPair, tdTime, tdDays, tdAfter, tdOn, tdDel);
    return tr;
  }

  function ensureUI() {
    const host = $('#sec-scheduling .body');
    if (!host || $('#schAdv')) return;

    // Collapsed by default (no "open" attribute)
    const adv = el('details','sch-adv'); adv.id = 'schAdv';
    adv.innerHTML = `
      <summary>
        <span>Advanced plan (sequential)</span>
        <span class="mini">Click to expand</span>
      </summary>
      <div style="margin-top:12px">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
          <label class="mini"><input type="checkbox" id="schAdvEnabled"> Use advanced plan</label>
          <span class="mini">Only enabled pairs are selectable; disabled pairs are greyed-out.</span>
        </div>

        <table>
          <thead>
            <tr>
              <th style="width:32%">Pair</th>
              <th style="width:14%">Time</th>
              <th style="width:30%">Days</th>
              <th style="width:14%">After</th>
              <th style="width:6%">Active</th>
              <th style="width:4%"></th>
            </tr>
          </thead>
          <tbody id="schJobsBody"></tbody>
        </table>

        <div class="mini" style="margin-top:8px">Always sequential. Times are user-defined.</div>
        <div class="status" id="schAdvStatus"></div>

        <div style="display:flex;gap:8px;margin-top:10px">
          <button class="btn" id="btnAddStep">Add step</button>
          <button class="btn" id="btnAutoFromPairs">Auto-create from enabled pairs</button>
        </div>
      </div>
    `;
    host.appendChild(adv);

    $('#btnAddStep').onclick = () => {
      _jobs.push({ id: genId(), pair_id: null, at: null, days: [], after: null, active: true });
      renderJobs();
    };
    $('#btnAutoFromPairs').onclick = () => {
      const eps = _pairs.filter(p => p.enabled);
      _jobs = eps.map(p => ({ id: genId(), pair_id: p.id, at: null, days: [], after: null, active: true }));
      if (!_jobs.length) _jobs.push({ id: genId(), pair_id: null, at: null, days: [], after: null, active: true });
      renderJobs();
    };
    $('#schAdvEnabled').onchange = () => { _advEnabled = !!$('#schAdvEnabled').checked; };
  }

  function renderJobs() {
    const tbody = $('#schJobsBody'); if (!tbody) return;
    tbody.innerHTML = '';

    if (!_jobs.length) _jobs.push({ id: genId(), pair_id: null, at: null, days: [], after: null, active: true });

    _jobs.forEach(j => j._blocked = j.active !== false && j.pair_id && !isEnabled(j.pair_id));
    _jobs.forEach(j => tbody.appendChild(jobRow(j)));

    const st = $('#schAdvStatus');
    if (!_pairs.length) st.textContent = 'No pairs from /api/pairs.';
    else if (_jobs.some(j => j._blocked)) st.textContent = 'Some steps reference disabled pairs.';
    else st.textContent = '';
  }

  // --- Load
  async function loadScheduling() {
    ensureUI();
    await fetchPairs();

    let saved = {};
    try { saved = await fetch('/api/scheduling', { cache:'no-store' }).then(r => r.json()); } catch {}

    setBooleanSelect($('#schEnabled'), !!saved.enabled);
    if ($('#schMode')) $('#schMode').value = saved.mode || 'hourly';
    if ($('#schN'))    $('#schN').value = String(saved.every_n_hours || 2);
    if ($('#schTime')) $('#schTime').value = saved.daily_time || '03:30';

    // Advanced payload
    const adv = saved?.advanced || {};
    _advEnabled = !!adv.enabled;
    if ($('#schAdvEnabled')) $('#schAdvEnabled').checked = _advEnabled;
    _jobs = Array.isArray(adv.jobs)
      ? adv.jobs.map(j => ({
          id: j.id || genId(),
          pair_id: j.pair_id || null,
          at: j.at || null,
          days: Array.isArray(j.days) ? j.days.filter(n => n>=1 && n<=7) : [],
          after: j.after || null,
          active: j.active !== false
        }))
      : [];
    renderJobs();

    // Nudge the banner (safe if it exists)
    try { window.refreshSchedulingBanner?.(); } catch {}
  }

  // --- Serialize (for global Save)
  function serializeAdvanced() {
    return {
      enabled: !!_advEnabled,
      jobs: _jobs.map(j => ({
        id: j.id,
        pair_id: j.pair_id || null,
        at: j.at || null,
        days: Array.isArray(j.days) ? j.days.slice() : [],
        after: j.after || null,
        active: j.active !== false
      }))
    };
  }

  // Public patch
  window.getSchedulingPatch = function() {
    const enabled = ($('#schEnabled')?.value || '').trim() === 'true';
    const mode = $('#schMode')?.value || 'hourly';
    const every_n_hours = parseInt($('#schN')?.value || '2', 10);
    const daily_time = $('#schTime')?.value || '03:30';
    const advanced = serializeAdvanced();
    return { enabled, mode, every_n_hours, daily_time, advanced };
  };

  // Bootstrap
  document.addEventListener('DOMContentLoaded', () => {
    loadScheduling().catch(e => console.warn('scheduler load failed', e));
  });
})();
