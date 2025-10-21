// assets/js/modals/exporter/index.js

const fjson = async (u, o) => {
  const r = await fetch(u, o);
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
  return r.json();
};
const $  = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => Array.from(r.querySelectorAll(s));

const LS = {
  get(k, d) { try { return JSON.parse(localStorage.getItem(k)) ?? d; } catch { return d; } },
  set(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch {} },
};

function injectCSS() {
  if (document.getElementById("ex-css")) return;
  const el = document.createElement("style");
  el.id = "ex-css";
  el.textContent = `
  .modal-root{position:relative;display:flex;flex-direction:column;height:100%}
  .cx-head{display:flex;align-items:center;justify-content:space-between;padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.12)}
  .cx-left{display:flex;align-items:center;gap:10px;flex:1;min-width:0}
  .cx-title{font-weight:800}
  .badge{opacity:.85;font-size:12px}
  .close-btn{border:1px solid rgba(255,255,255,.2);background:#171b2a;color:#fff;border-radius:10px;padding:6px 10px}
  .ex-body{flex:1;min-height:0;display:grid;grid-template-rows:auto 1fr;overflow:hidden}
  .row{display:flex;flex-wrap:wrap;gap:12px;padding:10px;border-bottom:1px solid rgba(255,255,255,.06);align-items:flex-end}
  .row .field{display:flex;flex-direction:column;gap:6px;min-width:160px}
  .input{background:#0b0f19;border:1px solid rgba(255,255,255,.12);color:#dbe8ff;border-radius:12px;padding:8px 10px;height:36px}
  .search{min-width:260px;flex:1}
  .row-right{margin-left:auto;display:flex;gap:8px;align-items:center}
  .btn{border:1px solid rgba(255,255,255,.16);background:#111524;color:#dfe7ff;border-radius:12px;padding:8px 12px;cursor:pointer}
  .btn.primary{background:rgba(122,107,255,.14);box-shadow:0 0 10px #7a6bff44 inset}
  .btn:disabled{opacity:.6;cursor:not-allowed}
  .ex-grid{overflow:auto}
  table{width:100%;border-collapse:separate;border-spacing:0;table-layout:fixed}
  th,td{padding:8px 10px;border-bottom:1px solid rgba(255,255,255,.06);font-size:12px;vertical-align:top;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  th{position:relative;text-align:left;opacity:.85;user-select:none}
  th .resizer{position:absolute;right:-2px;top:0;width:6px;height:100%;cursor:col-resize}
  th:hover .resizer{background:linear-gradient(90deg,transparent 0,rgba(122,107,255,.35) 50%,transparent 100%)}
  .td-wrap{white-space:normal;overflow:visible;text-overflow:clip}
  .ids span{margin-right:8px}
  .mono{font-family:ui-monospace,Menlo,Consolas,monospace}
  .hint{opacity:.75;font-size:12px}
  .glow-check{appearance:none;width:14px;height:14px;border-radius:4px;border:1px solid rgba(255,255,255,.28);background:#0b0f19;box-shadow:inset 0 0 0 2px rgba(255,255,255,.06);display:inline-block}
  .glow-check:checked{background:#7a6bff;box-shadow:0 0 8px #7a6bffbb, inset 0 0 0 2px rgba(0,0,0,.25)}
  .neon-switch{display:inline-flex;align-items:center;gap:8px;cursor:pointer;user-select:none}
  .neon-switch input{display:none}
  .neon-pill{width:44px;height:24px;border-radius:999px;background:#0b0f19;border:1px solid rgba(122,107,255,.4);position:relative;box-shadow:0 0 12px #7a6bff33 inset}
  .neon-knob{position:absolute;top:2px;left:2px;width:20px;height:20px;border-radius:50%;background:#7a6bff;box-shadow:0 0 12px #7a6bffaa;transition:transform .18s ease}
  .neon-switch input:checked + .neon-pill .neon-knob{transform:translateX(20px)}
  .neon-label{font-size:12px;opacity:.9}
  .wait-overlay{position:fixed;inset:0;display:flex;align-items:center;justify-content:center;background:rgba(5,8,20,.72);backdrop-filter:blur(6px);z-index:9999;opacity:1;transition:opacity .18s ease;}
  .wait-overlay.hidden{opacity:0;pointer-events:none}
  .wait-card{display:flex;flex-direction:column;align-items:center;gap:14px;padding:22px 28px;border-radius:18px;background:linear-gradient(180deg,#0b0f19,#0e1325);box-shadow:0 0 40px #7a6bff55, inset 0 0 1px rgba(255,255,255,.08)}
  .wait-ring{width:56px;height:56px;border-radius:50%;position:relative;filter:drop-shadow(0 0 12px #7a6bff88)}
  .wait-ring::before{content:"";position:absolute;inset:0;border-radius:50%;padding:4px;background:conic-gradient(#7a6bff,#23a8ff,#7a6bff);-webkit-mask:linear-gradient(#000 0 0) content-box,linear-gradient(#000 0 0);-webkit-mask-composite:xor;mask-composite:exclude;animation:wait-spin 1.1s linear infinite}
  .wait-text{font-weight:800;color:#dbe8ff;text-shadow:0 0 12px #7a6bff88}
  @keyframes wait-spin{to{transform:rotate(360deg)}}
  `;
  document.head.appendChild(el);
}

function closeModal() {
  // Prefer host hook; fallback to bubbling event
  if (window.cxCloseModal) { window.cxCloseModal(); return; }
  document.querySelector(".cx-modal-shell")?.dispatchEvent(new CustomEvent("cw-modal-close", { bubbles: true }));
}

async function downloadFile(u) {
  const r = await fetch(u);
  if (!r.ok) throw new Error("Download failed: " + r.status);
  const blob = await r.blob();
  const cd = r.headers.get("Content-Disposition") || "";
  const m = /filename="([^"]+)"/i.exec(cd);
  const name = m ? m[1] : "export.csv";
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = name;
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 4000);
}

/* column resizing via <colgroup> + dblclick autofit (safe if colgroup is empty) */
function enableColumnResize(table, lsKey = "cw.exporter.cols.v2") {
  try {
    if (!table || !table.isConnected) return;
    let colgroup = table.querySelector("colgroup");
    const ths = $$("thead th", table);
    if (!ths.length) return;

    // Ensure a colgroup exists and has the same number of <col> as headers
    if (!colgroup) {
      colgroup = document.createElement("colgroup");
      table.insertBefore(colgroup, table.firstChild);
    }
    while (colgroup.children.length < ths.length) {
      colgroup.appendChild(document.createElement("col"));
    }
    while (colgroup.children.length > ths.length) {
      colgroup.removeChild(colgroup.lastElementChild);
    }

    const cols = Array.from(colgroup.children);
    const saved = LS.get(lsKey, {});

    ths.forEach((th, i) => {
      const key = th.getAttribute("data-col") || `c${i}`;
      const col = cols[i];
      if (!col) return; // hard guard
      const initW = saved[key] || Math.max(90, Math.round(th.getBoundingClientRect().width));
      col.style.width = initW + "px";
      th.style.width  = initW + "px";
    });

    // Measure text with canvas
    const _canvas = document.createElement("canvas");
    const ctx = _canvas.getContext("2d");
    function textWidth(text, ref) {
      const cs = getComputedStyle(ref);
      ctx.font = `${cs.fontWeight} ${cs.fontSize} ${cs.fontFamily}`.replace(/\s{2,}/g, " ");
      return Math.ceil(ctx.measureText(text || "").width);
    }
    function autoFit(colIndex) {
      const th = ths[colIndex]; const col = cols[colIndex];
      if (!th || !col) return;
      const tds = $$(`tbody tr td:nth-child(${colIndex + 1})`, table);
      const pad = 24;
      let maxW = textWidth(th.innerText.trim(), th) + pad;
      for (let i = 0; i < Math.min(250, tds.length); i++) {
        const td = tds[i];
        const txt = td.innerText?.trim?.() || td.textContent || "";
        const w = textWidth(txt, td) + pad;
        if (w > maxW) maxW = w;
      }
      const w = Math.max(90, Math.min(1000, maxW));
      col.style.width = w + "px";
      th.style.width  = w + "px";
      const key = th.getAttribute("data-col") || `c${colIndex}`;
      saved[key] = Math.round(w);
      LS.set(lsKey, saved);
    }

    // Drag handlers
    let drag = null;
    function onDown(e, idx) {
      const col = cols[idx]; const th = ths[idx];
      if (!col || !th) return;
      drag = {
        idx,
        startX: e.clientX,
        startW: parseInt(col.style.width || th.offsetWidth, 10) || 120
      };
      document.body.style.userSelect = "none";
      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
      e.preventDefault(); e.stopPropagation();
    }
    function onMove(e) {
      if (!drag) return;
      const col = cols[drag.idx]; const th = ths[drag.idx];
      if (!col || !th) return;
      const w = Math.max(80, drag.startW + (e.clientX - drag.startX));
      col.style.width = w + "px";
      th.style.width  = w + "px";
    }
    function onUp() {
      if (!drag) return;
      const i = drag.idx; const col = cols[i]; const th = ths[i];
      if (col && th) {
        const key = th.getAttribute("data-col") || `c${i}`;
        const w = parseInt(col.style.width, 10) || th.offsetWidth;
        saved[key] = Math.round(w);
        LS.set(lsKey, saved);
      }
      drag = null;
      document.body.style.userSelect = "";
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
    }

    // Attach handles
    ths.forEach((th, i) => {
      let handle = th.querySelector(".resizer");
      if (!handle) {
        handle = document.createElement("div");
        handle.className = "resizer";
        th.appendChild(handle);
      }
      handle.onmousedown = (e) => onDown(e, i);
      handle.ondblclick  = (e) => { e.stopPropagation(); autoFit(i); };
    });
  } catch (err) {
    console.warn("Column resize init failed:", err);
  }
}

/* selected counter */
function selectedSummary({ mode, selected, filteredTotal }) {
  return mode === "all" ? `Selected: ${filteredTotal} of ${filteredTotal}` : `Selected: ${selected.size} of ${filteredTotal}`;
}

export default {
  async mount(root) {
    injectCSS();
    root.classList.add("modal-root");
    root.innerHTML = `
      <div class="cx-head">
        <div class="cx-left">
          <div class="cx-title">Exporter</div>
          <div class="badge" id="ex-badge">—</div>
        </div>
        <div class="ex-actions">
          <button class="close-btn" id="ex-close">Close</button>
        </div>
      </div>

      <div class="ex-body">
        <div class="row">
          <div class="field">
            <label>Provider</label>
            <select id="ex-prov" class="input"></select>
          </div>
          <div class="field">
            <label>Feature</label>
            <select id="ex-feat" class="input">
              <option value="watchlist">Watchlist</option>
              <option value="history">History</option>
              <option value="ratings">Ratings</option>
            </select>
          </div>
          <div class="field">
            <label>Format</label>
            <select id="ex-fmt" class="input"></select>
          </div>

          <div class="field search">
            <label>Search (title/id/year)</label>
            <input id="ex-q" class="input" type="text" placeholder="e.g. imdb:tt0468569, 2025, Twisted Metal">
          </div>

          <div class="row-right">
            <label class="neon-switch" title="Export all filtered results (live)">
              <input id="ex-all" type="checkbox" checked>
              <span class="neon-pill"><span class="neon-knob"></span></span>
              <span class="neon-label">Select all (filtered)</span>
            </label>
            <div class="hint" id="ex-count">—</div>
            <button class="btn" id="ex-preview">Preview</button>
            <button class="btn primary" id="ex-export">Export</button>
          </div>
        </div>

        <div class="ex-grid">
          <table id="ex-table">
            <colgroup></colgroup>
            <thead>
              <tr>
                <th data-col="sel"   style="width:26px"></th>
                <th data-col="title">Title</th>
                <th data-col="year">Year</th>
                <th data-col="type">Type</th>
                <th data-col="ids">IDs</th>
                <th data-col="extra">Watched/Rating</th>
              </tr>
            </thead>
            <tbody id="ex-tbody">
              <tr><td colspan="6" class="hint">Loading…</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    `;

    // wait overlay
    const wait = document.createElement("div");
    wait.className = "wait-overlay hidden";
    wait.innerHTML = `
      <div class="wait-card" role="status" aria-live="assertive">
        <div class="wait-ring"></div>
        <div class="wait-text" id="ex-wait-text">Loading…</div>
      </div>`;
    root.appendChild(wait);
    const setWait = (t) => { $("#ex-wait-text", root).textContent = t; };
    let waitTimer = null, shownAt = 0;
    const showWait = (t = "Loading…") => {
      setWait(t); wait.classList.remove("hidden");
      shownAt = performance.now();
      clearTimeout(waitTimer);
      waitTimer = setTimeout(() => setWait(`${t} (still working…)`), 3000);
    };
    const hideWait = () => {
      clearTimeout(waitTimer);
      const min = 250, elapsed = performance.now() - shownAt;
      const doHide = () => wait.classList.add("hidden");
      if (elapsed < min) setTimeout(doHide, min - elapsed); else doHide();
    };

    // refs
    const badge   = $("#ex-badge", root);
    const countEl = $("#ex-count", root);
    const provSel = $("#ex-prov", root);
    const featSel = $("#ex-feat", root);
    const fmtSel  = $("#ex-fmt", root);
    const qInput  = $("#ex-q", root);
    const allChk  = $("#ex-all", root);
    const btnPrev = $("#ex-preview", root);
    const btnExp  = $("#ex-export", root);
    const btnClose= $("#ex-close", root);
    const tbody   = $("#ex-tbody", root);
    const table   = $("#ex-table", root);

    // state
    let OPTS = null;
    let filteredTotal = 0;
    const selected = new Set();
    let mode = "all";
    let lastQuery = "";

    // prefs
    const PREFS_KEY = "cw.exporter.prefs";
    const prefs = LS.get(PREFS_KEY, {});
    const savePrefs = () => LS.set(PREFS_KEY, {
      provider: provSel.value, feature: featSel.value, format: fmtSel.value,
      q: qInput.value, all: allChk.checked
    });

    const fmtBadge = (optCounts) => {
      if (!optCounts) return "—";
      const seg = (p) => {
        const c = optCounts[p] || {};
        return `${p}: W${c.watchlist||0}/H${c.history||0}/R${c.ratings||0}`;
      };
      return Object.keys(optCounts).map(seg).join(" • ");
    };

    function syncFormats() {
      const f = featSel.value;
      const list = (OPTS.formats && OPTS.formats[f]) || [];
      const labels = OPTS.labels || {};
      fmtSel.innerHTML = list.map(x => `<option value="${x}">${labels[x] || x.toUpperCase()}</option>`).join("");
      if (prefs.format && list.includes(prefs.format)) fmtSel.value = prefs.format;
    }

    function rowHTML(it) {
      const idsHTML = Object.entries(it.ids || {}).map(([k,v]) => `<span class="mono">${k}:${v}</span>`).join(" ");
      const extra = (it.rating || "") || (it.watched_at || "");
      const isChecked = (mode === "all") ? true : selected.has(it.key);
      return `<tr data-key="${it.key}">
        <td><input type="checkbox" class="glow-check row-check" ${isChecked ? "checked" : ""}></td>
        <td class="td-wrap">${it.title || ""}</td>
        <td>${it.year || ""}</td>
        <td>${it.type || ""}</td>
        <td class="ids">${idsHTML}</td>
        <td>${extra || ""}</td>
      </tr>`;
    }

    function refreshCounts() {
      countEl.textContent = mode === "all"
        ? `Selected: ${filteredTotal} of ${filteredTotal}`
        : `Selected: ${selected.size} of ${filteredTotal}`;
    }

    async function renderPreview({ auto = false } = {}) {
      if (!OPTS?.providers?.length) {
        tbody.innerHTML = `<tr><td colspan="6" class="hint">No state loaded. Nothing to show.</td></tr>`;
        filteredTotal = 0; selected.clear(); refreshCounts();
        btnExp.disabled = true;
        return;
      }

      tbody.innerHTML = `<tr><td colspan="6" class="hint">Loading…</td></tr>`;
      showWait(auto ? "Refreshing…" : "Generating preview…");
      try {
        const limit = 50;
        lastQuery = qInput.value || "";
        const u = `/api/export/sample?provider=${encodeURIComponent(provSel.value)}&feature=${encodeURIComponent(featSel.value)}&limit=${limit}&q=${encodeURIComponent(lastQuery)}`;
        const data = await fjson(u);
        filteredTotal = data.total || 0;

        if (mode === "all") selected.clear();

        const rows = (data.items || []).map(rowHTML);
        tbody.innerHTML = rows.length ? rows.join("") : `<tr><td colspan="6" class="hint">No items.</td></tr>`;

        $$(".row-check", tbody).forEach(cb => {
          cb.addEventListener("change", () => {
            const tr = cb.closest("tr"); const key = tr?.getAttribute("data-key");
            if (!key) return;
            if (mode === "all") { mode = "manual"; allChk.checked = false; }
            if (cb.checked) selected.add(key); else selected.delete(key);
            refreshCounts();
          });
        });

        $$("tbody tr", table).forEach(tr => {
          tr.addEventListener("click", (e) => {
            if (e.target.closest("input,button,select,.resizer")) return;
            const cb = tr.querySelector(".row-check"); if (!cb) return;
            cb.checked = !cb.checked;
            cb.dispatchEvent(new Event("change"));
          });
        });

        btnExp.disabled = filteredTotal === 0 && selected.size === 0;
        refreshCounts();
      } catch (e) {
        tbody.innerHTML = `<tr><td colspan="6" class="hint">No data.</td></tr>`;
        filteredTotal = 0; selected.clear(); refreshCounts();
        btnExp.disabled = true;
      } finally {
        hideWait();
      }
    }

    async function doExport() {
      btnExp.disabled = true;
      const label = btnExp.textContent;
      btnExp.textContent = "Preparing…";
      showWait("Preparing file…");
      try {
        const base = `/api/export/file?provider=${encodeURIComponent(provSel.value)}&feature=${encodeURIComponent(featSel.value)}&format=${encodeURIComponent(fmtSel.value)}`;
        const q = `&q=${encodeURIComponent(lastQuery)}`;
        let extra = "";
        if (mode === "manual" && selected.size > 0) extra = "&ids=" + encodeURIComponent(Array.from(selected).join(","));
        await downloadFile(base + q + extra);
      } finally {
        btnExp.disabled = false;
        btnExp.textContent = label;
        hideWait();
      }
    }

    // init
    showWait("Loading options…");
    try {
      try {
        OPTS = await fjson("/api/export/options");
      } catch {
        OPTS = {
          providers: [],
          counts: {},
          formats: {
            watchlist: ["letterboxd","imdb","justwatch","yamtrack","tmdb"],
            history:   ["letterboxd","justwatch","yamtrack"],
            ratings:   ["letterboxd","yamtrack","tmdb"],
          },
          labels: {
            letterboxd: "Letterboxd",
            imdb: "IMDb (list)",
            justwatch: "JustWatch",
            yamtrack: "Yamtrack",
            tmdb: "TMDB (Auto: IMDb/Trakt/SIMKL)",
          }
        };
      }

      badge.textContent = OPTS.providers?.length ? fmtBadge(OPTS.counts) : "No state.json detected";

      if (OPTS.providers?.length) {
        provSel.innerHTML = OPTS.providers.map(p => `<option value="${p}">${p}</option>`).join("");
      } else {
        provSel.innerHTML = `<option value="" disabled>(no providers)</option>`;
        provSel.disabled = true; featSel.disabled = false; fmtSel.disabled = false;
      }

      if (OPTS.providers?.includes(prefs.provider)) provSel.value = prefs.provider;
      if (["watchlist","history","ratings"].includes(prefs.feature)) featSel.value = prefs.feature;
      qInput.value = prefs.q || "";
      allChk.checked = prefs.all !== false;

      syncFormats();
      enableColumnResize($("#ex-table", root));
    } finally {
      hideWait();
    }

    // events
    const debounced = (fn, ms=250) => { let t=null; return (...a)=>{ clearTimeout(t); t=setTimeout(()=>fn(...a),ms); }; };
    const autoRefresh = debounced(() => renderPreview({ auto:true }), 200);

    provSel.addEventListener("change", () => { selected.clear(); mode="all"; allChk.checked=true; savePrefs(); autoRefresh(); });
    featSel.addEventListener("change", () => { selected.clear(); mode="all"; allChk.checked=true; syncFormats(); savePrefs(); autoRefresh(); });
    fmtSel .addEventListener("change", savePrefs);
    qInput.addEventListener("input", () => { savePrefs(); autoRefresh(); });

    allChk.addEventListener("change", () => {
      mode = allChk.checked ? "all" : "manual";
      if (mode === "all") selected.clear();
      savePrefs();
      autoRefresh();
    });

    btnPrev.addEventListener("click", () => renderPreview({ auto:false }));
    btnExp .addEventListener("click", doExport);
    btnClose.addEventListener("click", closeModal);

    await renderPreview({ auto:false });
  },
  unmount() {}
};
