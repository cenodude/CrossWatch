// assets/js/modals/capture-compare/index.js
// reused analyzer modal

const fjson = async (u, o = {}) => {
  const r = await fetch(u, { cache: "no-store", ...o });
  if (!r.ok) throw new Error(String(r.status));
  return r.json();
};
const Q = (s, r = document) => r.querySelector(s);
const QA = (s, r = document) => Array.from(r.querySelectorAll(s));
const esc = (s) =>
  String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])
  );
const clamp = (n, a, b) => Math.max(a, Math.min(b, n));

function css() {
  const existing = Q("#cc-css");
  const el = existing || document.createElement("style");
  el.id = "cc-css";
  el.textContent = `
  .cx-modal-shell.cc-modal{background:#05060c!important;width:min(var(--cxModalMaxW,1700px),calc(100vw - 160px))!important}
  .cc-modal{position:relative;display:flex;flex-direction:column;height:100%;background:#05060c}
  .cc-modal .cx-head{display:flex;align-items:center;gap:10px;justify-content:space-between;background:linear-gradient(90deg,#05070d,#05040b);padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.08);box-shadow:0 0 24px rgba(0,0,0,.85)}
  .cc-modal .cc-left{display:flex;align-items:center;gap:12px;min-width:0;flex:1}
  .cc-modal .cc-title{font-weight:950;letter-spacing:.02em;white-space:nowrap}
  .cc-modal .cc-meta{font-size:12px;opacity:.76;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .cc-modal .cc-actions{display:flex;gap:8px;align-items:center}
  .cc-modal .pill{border:1px solid rgba(255,255,255,.14);background:#080a12;color:#e5ecff;border-radius:16px;padding:6px 12px;font-size:13px;display:inline-flex;align-items:center;gap:6px;white-space:nowrap;flex:0 0 auto}
  .cc-modal .pill.ghost{background:transparent}
  .cc-modal .pill[disabled]{opacity:.55;pointer-events:none}
  .cc-modal .pill.cc-copied{border-color:rgba(35,213,255,.65);box-shadow:0 0 12px rgba(35,213,255,.22)}
  .cc-modal .pill.cc-fail{border-color:rgba(255,59,127,.65);box-shadow:0 0 12px rgba(255,59,127,.18)}
  .cc-modal .close-btn{border:1px solid rgba(255,255,255,.16);background:#11131e;color:#fff;border-radius:10px;padding:6px 10px}

  .cc-modal .cc-toolbar{display:flex;flex-wrap:nowrap;gap:6px;padding:6px 12px;border-bottom:1px solid rgba(255,255,255,.08);background:#05060c;align-items:center;overflow-x:auto;scrollbar-width:none}
  .cc-modal .cc-toolbar::-webkit-scrollbar{display:none}
  .cc-modal input[type=search]{background:#05060c;border:1px solid rgba(255,255,255,.12);color:#dbe8ff;border-radius:12px;padding:6px 10px;min-width:220px;flex:1 1 320px;max-width:520px}
  .cc-modal select{background:#05060c;border:1px solid rgba(255,255,255,.12);color:#dbe8ff;border-radius:12px;padding:6px 10px}
  .cc-modal .cc-chip{cursor:pointer;user-select:none;font-size:11.5px;display:inline-flex;align-items:center;gap:6px;padding:5px 9px;border-radius:999px;border:1px solid rgba(255,255,255,.12);background:radial-gradient(circle at top,#12131f,#05060c);opacity:.86;color:#f5f6ff;font-weight:850;letter-spacing:.03em;text-transform:uppercase;box-shadow:0 0 10px rgba(0,0,0,.9);transition:opacity .16s ease,transform .12s ease,border-color .12s ease,box-shadow .12s ease;white-space:nowrap}
  .cc-modal .cc-chip:hover{opacity:1;transform:translateY(-1px);border-color:rgba(122,107,255,.55)}
  .cc-modal .cc-chip.on{opacity:1;border-color:rgba(122,107,255,.85);box-shadow:0 0 18px rgba(122,107,255,.35)}
  .cc-modal .cc-chip.add.on{border-color:rgba(35,213,255,.8);box-shadow:0 0 18px rgba(35,213,255,.28)}
  .cc-modal .cc-chip.del.on{border-color:rgba(255,59,127,.85);box-shadow:0 0 18px rgba(255,59,127,.28)}
  .cc-modal .cc-chip.unc.on{border-color:rgba(255,255,255,.35)}
  .cc-modal .cc-chip.small{padding:5px 9px;font-weight:750}

  .cc-modal .cc-wrap{flex:1;min-height:0;display:flex;flex-direction:column;overflow:hidden}
  .cc-modal .cc-top{flex:0 0 var(--ccTopH,44%);min-height:250px;display:flex;overflow:hidden;border-top:1px solid rgba(255,255,255,.04)}
  .cc-modal .cc-hsplit,.cc-modal .cc-vsplit{position:relative;flex:0 0 var(--ccSplitW,10px);background:transparent;touch-action:none}
.cc-modal .cc-hsplit{height:var(--ccSplitW,10px);cursor:row-resize}
.cc-modal .cc-vsplit{width:var(--ccSplitW,10px);cursor:col-resize}
.cc-modal .cc-hsplit::after{content:"";position:absolute;left:0;right:0;top:50%;height:var(--ccSplitLine,2px);transform:translateY(-50%);border-radius:999px;background:linear-gradient(90deg,rgba(122,107,255,.12),rgba(122,107,255,.85),rgba(122,107,255,.12));box-shadow:0 0 10px rgba(122,107,255,.28)}
.cc-modal .cc-vsplit::after{content:"";position:absolute;top:0;bottom:0;left:50%;width:var(--ccSplitLine,2px);transform:translateX(-50%);border-radius:999px;background:linear-gradient(180deg,rgba(122,107,255,.12),rgba(122,107,255,.85),rgba(122,107,255,.12));box-shadow:0 0 10px rgba(122,107,255,.28)}
.cc-modal .cc-hsplit:hover::after,.cc-modal .cc-vsplit:hover::after{box-shadow:0 0 14px rgba(122,107,255,.42)}
  .cc-modal #cc-pane-a{flex:1 1 0;min-width:360px}
  .cc-modal #cc-pane-b{flex:1 1 0;min-width:360px}

  .cc-modal .cc-pane{min-width:0;display:flex;flex-direction:column;overflow:hidden;background:#05060c}
  .cc-modal .cc-pane-head{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.08);background:radial-gradient(circle at top left,#151624,#05060c)}
  .cc-modal .cc-pane-head .h{min-width:0}
  .cc-modal .cc-pane-head .t{font-weight:950;letter-spacing:.02em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .cc-modal .cc-pane-head .s{font-size:12px;opacity:.76;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .cc-modal .cc-pane-head .tag{font-size:11px;padding:4px 8px;border-radius:999px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.06);font-weight:900;letter-spacing:.05em;text-transform:uppercase}

  .cc-modal .cc-pane-list{flex:1;min-height:0;overflow:auto}
  .cc-modal .cc-row{display:grid;grid-template-columns:auto 1fr auto;gap:10px;padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.06);align-items:center;cursor:pointer}
  .cc-modal .cc-row:hover{background:rgba(122,107,255,.06)}
  .cc-modal .cc-row.sel{outline:1px solid rgba(122,163,255,.9);background:rgba(122,107,255,.09)}
  .cc-modal .cc-row.miss{opacity:.55}
  .cc-modal .cc-st{font-size:11px;font-weight:950;letter-spacing:.05em;border-radius:999px;padding:4px 8px;border:1px solid rgba(255,255,255,.12);text-transform:uppercase}
  .cc-modal .cc-st.add{background:rgba(35,213,255,.12);border-color:rgba(35,213,255,.35)}
  .cc-modal .cc-st.del{background:rgba(255,59,127,.12);border-color:rgba(255,59,127,.35)}
  .cc-modal .cc-st.upd{background:rgba(122,107,255,.14);border-color:rgba(122,107,255,.35)}
  .cc-modal .cc-st.unc{background:rgba(255,255,255,.07)}
  .cc-modal .cc-main{min-width:0}
  .cc-modal .cc-title2{font-weight:850;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .cc-modal .cc-sub{font-size:12px;opacity:.75;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .cc-modal .cc-mini{font-size:12px;opacity:.85;white-space:nowrap}
  .cc-modal .mono{font-family:ui-monospace,SFMono-Regular,Consolas,monospace}

  .cc-modal .cc-pane-foot{padding:10px 12px;border-top:1px solid rgba(255,255,255,.08);display:flex;flex-wrap:wrap;gap:8px;align-items:center;justify-content:space-between;background:#05060c}
  .cc-modal .cc-pills{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
  .cc-modal .stat-pill{font-size:12px;font-weight:900;letter-spacing:.02em;border-radius:999px;padding:6px 10px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.05);white-space:nowrap}
  .cc-modal .stat-pill.movie{border-color:rgba(35,213,255,.35);background:rgba(35,213,255,.10)}
  .cc-modal .stat-pill.show{border-color:rgba(122,107,255,.35);background:rgba(122,107,255,.10)}
  .cc-modal .stat-pill.season{border-color:rgba(255,255,255,.18);background:rgba(255,255,255,.06)}
  .cc-modal .stat-pill.episode{border-color:rgba(255,59,127,.25);background:rgba(255,59,127,.08)}
  .cc-modal .cc-foot-mini{font-size:12px;opacity:.75;white-space:nowrap}

  .cc-modal .cc-bottom{flex:1;min-height:320px;display:flex;flex-direction:column;overflow:hidden;background:#05060c;border-top:1px solid rgba(255,255,255,.06)}
  .cc-modal .cc-detail-head{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:8px 12px;border-bottom:1px solid rgba(255,255,255,.08);background:radial-gradient(circle at top left,#151624,#05060c)}
  .cc-modal .cc-detail-head .h{display:flex;min-width:0}
  .cc-modal .cc-detail-head .t{display:flex;align-items:baseline;gap:10px;font-weight:950;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0}
  .cc-modal .cc-detail-head .tt{min-width:0;overflow:hidden;text-overflow:ellipsis}
  .cc-modal .cc-detail-head .k{font-size:12px;opacity:.72;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex:0 0 auto}
  .cc-modal .cc-detail-actions{display:flex;gap:8px;align-items:center}

  .cc-modal .cc-bottom-wrap{flex:1;min-height:0;display:flex;flex-direction:column;overflow:hidden}
  .cc-modal .cc-changes{flex:0 0 auto;padding:8px 12px;border-bottom:1px solid rgba(255,255,255,.06);overflow:auto;max-height:160px}
  .cc-modal .cc-changes.hidden{display:none}
  .cc-modal #cc-rec-b{scrollbar-width:none}
  .cc-modal #cc-rec-b::-webkit-scrollbar{display:none}

  .cc-modal .chg{display:grid;grid-template-columns:minmax(160px,260px) 1fr 1fr;gap:10px;padding:8px 10px;border:1px solid rgba(255,255,255,.08);border-radius:12px;background:rgba(255,255,255,.03);margin-bottom:8px}
  .cc-modal .chg .p{font-weight:900;opacity:.95}
  .cc-modal .chg .v{font-size:12px;opacity:.88;word-break:break-word;white-space:pre-wrap}
  .cc-modal .chg .lab{font-size:11px;opacity:.7;margin-bottom:4px;text-transform:uppercase;letter-spacing:.04em}

  .cc-modal .cc-detail-split{flex:1;min-height:0;display:flex;overflow:hidden}
  .cc-modal #cc-rec-a{flex:1 1 0;min-width:320px}
  .cc-modal #cc-rec-b{flex:1 1 0;min-width:320px}
  .cc-modal .cc-rec{min-width:0;overflow:auto;padding:10px 12px}
  .cc-modal .cc-rec .card{border-radius:16px;padding:12px 12px;background:radial-gradient(circle at top left,#151624,#05060c);border:1px solid rgba(255,255,255,.10);box-shadow:0 0 18px rgba(0,0,0,.85)}
  .cc-modal .cc-rec .card + .card{margin-top:10px}
  .cc-modal .cc-rec .card .ttl{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:8px}
  .cc-modal .cc-rec .card .ttl .name{font-weight:950}
  .cc-modal .cc-rec .card .ttl .mini{font-size:12px;opacity:.75}
  .cc-modal .kv{display:grid;grid-template-columns:minmax(120px,160px) 1fr;gap:6px 10px;font-size:12.5px}
  .cc-modal .kv .k{opacity:.75}
  .cc-modal .kv .v{opacity:.95;word-break:break-word}
  .cc-modal .chips{display:flex;flex-wrap:wrap;gap:6px;margin-top:10px}
  .cc-modal .chip{display:inline-flex;align-items:center;gap:6px;padding:5px 9px;border-radius:999px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.04);font-size:12px;cursor:pointer;transition:transform .12s ease,border-color .12s ease,background .12s ease;user-select:none}
  .cc-modal .chip:hover{transform:translateY(-1px);border-color:rgba(122,107,255,.55);background:rgba(122,107,255,.08)}
  .cc-modal .chip.copied{border-color:rgba(35,213,255,.55);background:rgba(35,213,255,.10)}
  .cc-modal .chip .k{opacity:.75;text-transform:uppercase;letter-spacing:.03em;font-weight:850}
  .cc-modal details{border-radius:14px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.03);padding:10px 11px;margin:10px 0 0}
  .cc-modal details>summary{cursor:pointer;font-weight:900;opacity:.95}
  .cc-modal pre{margin:10px 0 0;white-space:pre-wrap;word-break:break-word;background:#04050a;border:1px solid rgba(255,255,255,.10);border-radius:12px;padding:10px 12px;max-height:420px;overflow:auto}

  .cc-modal .empty{padding:18px 12px;opacity:.75}

  .wait-overlay{position:fixed;inset:0;display:flex;align-items:center;justify-content:center;background:rgba(3,4,10,.8);backdrop-filter:blur(6px);z-index:9999;opacity:1;transition:opacity .18s ease}
  .wait-overlay.hidden{opacity:0;pointer-events:none}
  .wait-card{display:flex;flex-direction:column;align-items:center;gap:14px;padding:22px 28px;border-radius:18px;background:linear-gradient(180deg,#05060c,#101124);box-shadow:0 0 40px rgba(122,107,255,.45),inset 0 0 1px rgba(255,255,255,.08)}
  .wait-ring{width:64px;height:64px;border-radius:50%;position:relative;filter:drop-shadow(0 0 12px rgba(122,107,255,.55))}
  .wait-ring::before{content:"";position:absolute;inset:0;border-radius:50%;padding:4px;background:conic-gradient(#7a6bff,#23d5ff,#7a6bff);-webkit-mask:linear-gradient(#000 0 0) content-box,linear-gradient(#000 0 0);-webkit-mask-composite:xor;mask-composite:exclude;animation:wait-spin 1.1s linear infinite}
  .wait-text{font-weight:950;color:#dbe8ff;text-shadow:0 0 12px rgba(122,107,255,.6)}
  @keyframes wait-spin{to{transform:rotate(360deg)}}
  `;
  document.head.appendChild(el);
}

function kindOf(r) {
  if (!r || typeof r !== "object") return "unknown";
  const t = String(r.type || r.media_type || r.entity || "").toLowerCase();
  const s = r.season;
  const e = r.episode;
  if (t === "episode" || e != null) return "episode";
  if (t === "season" || s != null) return "season";
  if (["tv", "show", "shows", "series", "anime"].includes(t)) return "show";
  if (["movie", "movies", "film", "films"].includes(t)) return "movie";
  return t || "unknown";
}

function displayTitle(r) {
  if (!r || typeof r !== "object") return "Item";
  const t = String(r.type || "").toLowerCase();
  const series = String(r.series_title || r.show_title || r.series || "");
  const title = String(r.title || "");
  const year = r.year ? ` (${r.year})` : "";
  const season = r.season != null ? String(r.season).padStart(2, "0") : "";
  const episode = r.episode != null ? String(r.episode).padStart(2, "0") : "";
  if (t === "episode" && series && season && episode) return `${series} - S${season}E${episode}`;
  if (t === "season" && series && season) return `${series} - S${season}`;
  return `${title || series || kindOf(r)}${year}`;
}

function displaySub(r) {
  if (!r || typeof r !== "object") return "";
  const parts = [];
  const k = kindOf(r);
  if (k && k !== "unknown") parts.push(k);
  if (r.year) parts.push(String(r.year));
  if (k === "episode" && r.season != null && r.episode != null) {
    parts.push(`S${String(r.season).padStart(2, "0")}E${String(r.episode).padStart(2, "0")}`);
  } else if (k === "season" && r.season != null) {
    parts.push(`S${String(r.season).padStart(2, "0")}`);
  }
  if (r.watched_at) parts.push("watched");
  return parts.join(" • ");
}

function stringify(v) {
  if (v === null) return "null";
  if (v === undefined) return "—";
  if (typeof v === "string") return v;
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}

function pretty(v) {
  try {
    return JSON.stringify(v, null, 2);
  } catch {
    return String(v);
  }
}

async function copyText(t) {
  const s = String(t ?? "");
  if (!s) return false;
  try {
    await navigator.clipboard.writeText(s);
    return true;
  } catch {
    try {
      const ta = document.createElement("textarea");
      ta.value = s;
      ta.style.position = "fixed";
      ta.style.left = "-9999px";
      document.body.appendChild(ta);
      // Ensure focus and selection for browsers that require it
      ta.focus({ preventScroll: true });
      ta.select();
      ta.setSelectionRange(0, ta.value.length);
      document.execCommand("copy");
      ta.remove();
      return true;
    } catch {
      return false;
    }
  }
}

function flashCopy(btn, ok, okText = "Copied", failText = "Copy blocked") {
  if (!btn) return;
  const orig = btn.dataset.ccOrig || btn.textContent || "";
  btn.dataset.ccOrig = orig;
  btn.classList.remove("cc-copied", "cc-fail");
  btn.classList.add(ok ? "cc-copied" : "cc-fail");
  btn.textContent = ok ? okText : failText;
  window.setTimeout(() => {
    btn.textContent = btn.dataset.ccOrig || orig;
    btn.classList.remove("cc-copied", "cc-fail");
  }, 850);
}


function countsFor(records) {
  const out = { movie: 0, show: 0, season: 0, episode: 0, unknown: 0, total: 0 };
  for (const r of records) {
    if (!r) continue;
    const k = kindOf(r);
    out[k] = (out[k] || 0) + 1;
    out.total++;
  }
  return out;
}

function renderCountsPills(c) {
  return `
    <div class="cc-pills">
      <span class="stat-pill movie">Movies ${c.movie || 0}</span>
      <span class="stat-pill show">Shows ${c.show || 0}</span>
      <span class="stat-pill season">Seasons ${c.season || 0}</span>
      <span class="stat-pill episode">Episodes ${c.episode || 0}</span>
    </div>`;
}

function renderRecordCard(label, rec, missingText = "Missing") {
  if (!rec) {
    return `
      <div class="card">
        <div class="ttl"><div class="name">${esc(label)}</div><div class="mini">${esc(missingText)}</div></div>
        <div class="empty">No record in this file.</div>
      </div>`;
  }

  const ids = rec.ids && typeof rec.ids === "object" ? rec.ids : null;
  const showIds = rec.show_ids && typeof rec.show_ids === "object" ? rec.show_ids : null;

  const kv = [];
  const k = kindOf(rec);
  kv.push(["Type", k]);
  if (rec.title) kv.push(["Title", rec.title]);
  if (rec.series_title || rec.show_title) kv.push(["Series", rec.series_title || rec.show_title]);
  if (rec.year != null) kv.push(["Year", rec.year]);
  if (rec.season != null) kv.push(["Season", rec.season]);
  if (rec.episode != null) kv.push(["Episode", rec.episode]);
  if (rec.watched_at) kv.push(["Watched", rec.watched_at]);
  if (rec.added_at) kv.push(["Added", rec.added_at]);
  if (rec.updated_at) kv.push(["Updated", rec.updated_at]);

  const chips = [];
  const mkChips = (obj, prefix) => {
    if (!obj) return;
    for (const [kk, vv] of Object.entries(obj)) {
      if (vv == null || vv === "" || vv === 0 || vv === false) continue;
      chips.push(
        `<span class="chip" data-copy="${esc(vv)}" title="Click to copy"><span class="k">${esc(
          prefix + kk
        )}</span><span class="v mono">${esc(vv)}</span></span>`
      );
    }
  };
  mkChips(ids, "");
  mkChips(showIds, "show.");

  return `
    <div class="card">
      <div class="ttl">
        <div class="name">${esc(label)}</div>
        <div class="mini">${esc(displayTitle(rec))}</div>
      </div>
      <div class="kv">
        ${kv
          .map(
            ([kk, vv]) =>
              `<div class="k">${esc(kk)}</div><div class="v">${esc(stringify(vv))}</div>`
          )
          .join("")}
      </div>
      ${chips.length ? `<div class="chips">${chips.join("")}</div>` : ""}
      <details>
        <summary>Raw JSON</summary>
        <pre class="mono">${esc(pretty(rec))}</pre>
      </details>
    </div>`;
}

function renderChanges(changes) {
  if (!Array.isArray(changes) || !changes.length) {
    return `<div class="empty">No field-level changes for this item.</div>`;
  }
  const rows = changes.slice(0, 200).map((c) => {
    const p = esc(String(c.path || ""));
    const o = esc(stringify(c.old));
    const n = esc(stringify(c.new));
    return `
      <div class="chg">
        <div class="p mono">${p}</div>
        <div>
          <div class="lab">A</div>
          <div class="v mono">${o}</div>
        </div>
        <div>
          <div class="lab">B</div>
          <div class="v mono">${n}</div>
        </div>
      </div>`;
  });
  return rows.join("");
}

function initSplit({
  handle,
  container,
  axis,
  getMin,
  getMax,
  onSet,
}) {
  let dragging = false;
  let start = 0;

  const onDown = (e) => {
    dragging = true;
    start = axis === "x" ? e.clientX : e.clientY;
    handle.setPointerCapture?.(e.pointerId);
    document.body.style.userSelect = "none";
    e.preventDefault();
  };

  const onMove = (e) => {
    if (!dragging) return;
    const pos = axis === "x" ? e.clientX : e.clientY;
    const delta = pos - start;
    start = pos;

    const rect = container.getBoundingClientRect();
    const min = getMin(rect);
    const max = getMax(rect);

    onSet(delta, min, max, rect);
  };

  const onUp = () => {
    if (!dragging) return;
    dragging = false;
    document.body.style.userSelect = "";
  };

  handle.addEventListener("pointerdown", onDown);
  window.addEventListener("pointermove", onMove);
  window.addEventListener("pointerup", onUp);

  return () => {
    handle.removeEventListener("pointerdown", onDown);
    window.removeEventListener("pointermove", onMove);
    window.removeEventListener("pointerup", onUp);
  };
}

export default {
  async mount(root, props = {}) {
    css();

    root.style.setProperty("--cxModalMaxW", "1700px");
    root.style.setProperty("--cxModalMaxH", "94vh");

    root.style.setProperty("--ccSplitW", "10px");
    root.style.setProperty("--ccSplitLine", "2px");

    root.classList.add("modal-root", "cc-modal");
    root.innerHTML = `
      <div class="cx-head">
        <div class="cc-left">
          <div class="cc-title">Capture Compare</div>
          <div class="cc-meta" id="cc-meta">Loading…</div>
        </div>
        <div class="cc-actions">
          <button class="pill" id="cc-refresh" type="button">Refresh</button>
          <button class="close-btn" id="cc-close" type="button">Close</button>
        </div>
      </div>

      <div class="cc-toolbar">
        <input id="cc-search" type="search" placeholder="Search title, ids, key…">
        <div class="cc-chip add on" data-st="added">Added</div>
        <div class="cc-chip del on" data-st="removed">Deleted</div>
        <div class="cc-chip on" data-st="updated">Updated</div>
        <div class="cc-chip unc" data-st="unchanged">Unchanged</div>
        <select id="cc-type">
          <option value="">All types</option>
          <option value="movie">Movies</option>
          <option value="show">Shows</option>
          <option value="season">Seasons</option>
          <option value="episode">Episodes</option>
        </select>
        <select id="cc-sort">
          <option value="status">Sort: status</option>
          <option value="title">Sort: title</option>
          <option value="key">Sort: key</option>
        </select>
        <div class="cc-chip small on" id="cc-changed">Changed only</div>
      </div>

      <div class="cc-wrap" id="cc-wrap">
        <div class="cc-top" id="cc-top">
          <div class="cc-pane" id="cc-pane-a">
            <div class="cc-pane-head">
              <div class="h">
                <div class="t" id="cc-a-title">File A - Capture</div>
                <div class="s" id="cc-a-sub">—</div>
              </div>
              <span class="tag" id="cc-a-tag">A</span>
            </div>
            <div class="cc-pane-list" id="cc-list-a"></div>
            <div class="cc-pane-foot">
              <div id="cc-a-pills"></div>
              <div class="cc-foot-mini" id="cc-a-total">—</div>
            </div>
          </div>

          <div class="cc-vsplit" id="cc-vsplit-top" title="drag to resize"></div>

          <div class="cc-pane" id="cc-pane-b">
            <div class="cc-pane-head">
              <div class="h">
                <div class="t" id="cc-b-title">File B - Capture</div>
                <div class="s" id="cc-b-sub">—</div>
              </div>
              <span class="tag" id="cc-b-tag">B</span>
            </div>
            <div class="cc-pane-list" id="cc-list-b"></div>
            <div class="cc-pane-foot">
              <div id="cc-b-pills"></div>
              <div class="cc-foot-mini" id="cc-b-total">—</div>
            </div>
          </div>
        </div>

        <div class="cc-hsplit" id="cc-hsplit" title="drag to resize"></div>

        <div class="cc-bottom" id="cc-bottom">
          <div class="cc-detail-head">
            <div class="h">
              <div class="t"><span class="tt" id="cc-d-title">Select an item</span><span class="k mono" id="cc-d-key">—</span></div>
            </div>
            <div class="cc-detail-actions">
              <button class="pill ghost" id="cc-copy-key" type="button">Copy key</button>
              <button class="pill" id="cc-copy-a" type="button">Copy JSON A</button>
              <button class="pill" id="cc-copy-b" type="button">Copy JSON B</button>
            </div>
          </div>

          <div class="cc-bottom-wrap">
            <div class="cc-changes" id="cc-changes"></div>
            <div class="cc-detail-split" id="cc-detail-split">
              <div class="cc-rec" id="cc-rec-a"></div>
              <div class="cc-vsplit" id="cc-vsplit-detail" title="drag to resize"></div>
              <div class="cc-rec" id="cc-rec-b"></div>
            </div>
          </div>
        </div>
      </div>
    `;

    const wait = document.createElement("div");
    wait.id = "cc-wait";
    wait.className = "wait-overlay hidden";
    wait.innerHTML = `
      <div class="wait-card" role="status" aria-live="assertive">
        <div class="wait-ring"></div>
        <div class="wait-text" id="cc-wait-text">Loading…</div>
      </div>`;
    root.appendChild(wait);

    const state = {
      aPath: String(props.aPath || props.a || ""),
      bPath: String(props.bPath || props.b || ""),
      diff: null,
      rows: [],
      filtered: [],
      selectedKey: "",
      search: "",
      st: new Set(["added", "removed", "updated"]),
      type: "",
      sort: "status",
      countsA: { movie: 0, show: 0, season: 0, episode: 0, unknown: 0, total: 0 },
      countsB: { movie: 0, show: 0, season: 0, episode: 0, unknown: 0, total: 0 },
      layout: {
        topH: null,
        aW: null,
        detailAW: null,
      },
    };

    const setWait = (on, text = "Loading…") => {
      const w = Q("#cc-wait", root);
      const t = Q("#cc-wait-text", root);
      if (t) t.textContent = text;
      if (!w) return;
      if (on) w.classList.remove("hidden");
      else w.classList.add("hidden");
    };

    const stOrder = (s) => ({ added: 0, removed: 1, updated: 2, unchanged: 3 }[s] ?? 99);
    const stCls = (s) => ({ added: "add", removed: "del", updated: "upd", unchanged: "unc" }[s] ?? "unc");

    const normalizeRow = (r) => {
      const st = String(r.status || "unchanged");
      const recA = st === "added" ? null : r.old || r.item || null;
      const recB = st === "removed" ? null : r.new || r.item || null;
      const brief = r.brief && typeof r.brief === "object" ? r.brief : recB || recA || {};
      const changes = Array.isArray(r.changes) ? r.changes : [];
      return { key: String(r.key || ""), status: st, brief, recA, recB, changes };
    };

    const computeCounts = () => {
      const a = [];
      const b = [];
      for (const r of state.rows) {
        if (r.recA) a.push(r.recA);
        if (r.recB) b.push(r.recB);
      }
      state.countsA = countsFor(a);
      state.countsB = countsFor(b);
    };

    const renderMeta = () => {
      const meta = Q("#cc-meta", root);
      const d = state.diff;
      if (!meta) return;
      if (!d) {
        meta.textContent = "No data";
        return;
      }
      const a = d.a || {};
      const b = d.b || {};
      const s = d.summary || {};
      meta.textContent = `${a.provider || ""} • ${(a.feature || "").toLowerCase()} • +${s.added ?? 0} -${s.removed ?? 0} ~${s.updated ?? 0} =${s.unchanged ?? 0}`;

      const at = Q("#cc-a-title", root);
      const bt = Q("#cc-b-title", root);
      const as = Q("#cc-a-sub", root);
      const bs = Q("#cc-b-sub", root);
      if (at) at.textContent = "File A - Capture";
      if (bt) bt.textContent = "File B - Capture";
      if (as) as.textContent = `${(a.created_at || "").replace("T", " ").replace("Z", "")} • ${a.count ?? "?"} items`;
      if (bs) bs.textContent = `${(b.created_at || "").replace("T", " ").replace("Z", "")} • ${b.count ?? "?"} items`;

      const ap = Q("#cc-a-pills", root);
      const bp = Q("#cc-b-pills", root);
      if (ap) ap.innerHTML = renderCountsPills(state.countsA);
      if (bp) bp.innerHTML = renderCountsPills(state.countsB);

      const atot = Q("#cc-a-total", root);
      const btot = Q("#cc-b-total", root);
      if (atot) atot.textContent = `Total: ${state.countsA.total}`;
      if (btot) btot.textContent = `Total: ${state.countsB.total}`;
    };

    const sortRows = (rows) => {
      const sort = String(state.sort || "status");
      if (sort === "key") {
        rows.sort((a, b) => a.key.localeCompare(b.key));
        return;
      }
      if (sort === "title") {
        rows.sort((a, b) => displayTitle(a.brief).localeCompare(displayTitle(b.brief)));
        return;
      }
      rows.sort(
        (a, b) =>
          stOrder(a.status) - stOrder(b.status) ||
          displayTitle(a.brief).localeCompare(displayTitle(b.brief)) ||
          a.key.localeCompare(b.key)
      );
    };

    const applyFilters = () => {
      const q = String(state.search || "").trim().toLowerCase();
      const wantType = String(state.type || "").toLowerCase();
      const st = state.st;

      let rows = state.rows.filter((r) => st.has(r.status));
      if (wantType) {
        rows = rows.filter((r) => {
          const ref = r.recB || r.recA || r.brief;
          return kindOf(ref) === wantType;
        });
      }
      if (q) {
        rows = rows.filter((r) => {
          const ref = r.recB || r.recA || r.brief;
          const ids = ref && typeof ref === "object" ? ref.ids : null;
          const showIds = ref && typeof ref === "object" ? ref.show_ids : null;
          const bits = [r.key, displayTitle(ref), displaySub(ref)];
          if (ids && typeof ids === "object") bits.push(JSON.stringify(ids));
          if (showIds && typeof showIds === "object") bits.push(JSON.stringify(showIds));
          return bits.join(" ").toLowerCase().includes(q);
        });
      }

      sortRows(rows);
      state.filtered = rows;

      if (state.selectedKey && !rows.some((x) => x.key === state.selectedKey)) state.selectedKey = "";
      if (!state.selectedKey && rows.length) state.selectedKey = rows[0].key;

      renderLists();
      renderDetail();
    };

    const renderList = (hostSel, side) => {
      const host = Q(hostSel, root);
      if (!host) return;

      const rows = state.filtered.filter((r) => (side === "A" ? !!r.recA : !!r.recB));
      if (!rows.length) {
        host.innerHTML = `<div class="empty">No matches.</div>`;
        return;
      }

      host.innerHTML = rows
        .map((r) => {
          const ref = side === "A" ? r.recA : r.recB;
          const title = esc(displayTitle(ref || r.brief));
          const sub = esc(displaySub(ref || r.brief));
          const sel = r.key === state.selectedKey ? "sel" : "";
          const st = esc(r.status);
          const cls = stCls(r.status);
          const changeCount = r.status === "updated" ? r.changes.length : 0;
          const mini = changeCount ? `<span class="cc-mini mono">Δ${changeCount}</span>` : "";
          return `
            <div class="cc-row ${sel}" data-key="${esc(r.key)}" data-side="${side}">
              <span class="cc-st ${cls}">${st}</span>
              <div class="cc-main">
                <div class="cc-title2">${title}</div>
                <div class="cc-sub">${sub}</div>
              </div>
              ${mini}
            </div>`;
        })
        .join("");
    };

    const renderLists = () => {
      renderList("#cc-list-a", "A");
      renderList("#cc-list-b", "B");

      for (const sel of ["#cc-list-a", "#cc-list-b"]) {
        const host = Q(sel, root);
        const row = host?.querySelector?.(`.cc-row.sel`);
        if (row && typeof row.scrollIntoView === "function") {
          row.scrollIntoView({ block: "nearest" });
        }
      }
    };

    const rowByKey = (k) => state.rows.find((x) => x.key === k) || null;

    const renderDetail = () => {
      const row = rowByKey(state.selectedKey);
      const dt = Q("#cc-d-title", root);
      const dk = Q("#cc-d-key", root);
      const ch = Q("#cc-changes", root);
      const ra = Q("#cc-rec-a", root);
      const rb = Q("#cc-rec-b", root);

      const btnKey = Q("#cc-copy-key", root);
      const btnA = Q("#cc-copy-a", root);
      const btnB = Q("#cc-copy-b", root);

      if (!row) {
        if (dt) dt.textContent = "Select an item";
        if (dk) dk.textContent = "—";
        if (ch) { ch.classList.add("hidden"); ch.innerHTML = ""; }
        if (ra) ra.innerHTML = renderRecordCard("File A", null);
        if (rb) rb.innerHTML = renderRecordCard("File B", null);
        if (btnA) btnA.disabled = true;
        if (btnB) btnB.disabled = true;
        if (btnKey) btnKey.disabled = true;
        return;
      }

      const best = row.recB || row.recA || row.brief;
      if (dt) {
        const st = row.status;
        const cls = stCls(st);
        const delta = st === "updated" ? `<span class="mono" style="opacity:.75;font-size:12px">Δ${row.changes.length}</span>` : "";
        dt.innerHTML = `<span class="cc-st ${cls}">${esc(st)}</span><span class="tt">${esc(
          displayTitle(best)
        )}</span>${delta}`;
      }
      if (dk) dk.textContent = row.key;

      if (ch) {
        if (row.status === "updated") {
          ch.classList.remove("hidden");
          ch.innerHTML = renderChanges(row.changes);
        } else {
          ch.classList.add("hidden");
          ch.innerHTML = "";
        }
      }

      if (ra) ra.innerHTML = renderRecordCard("File A", row.recA, row.status === "added" ? "(missing)" : "");
      if (rb) rb.innerHTML = renderRecordCard("File B", row.recB, row.status === "removed" ? "(missing)" : "");

      if (btnKey) btnKey.disabled = !row.key;
      if (btnA) btnA.disabled = !row.recA;
      if (btnB) btnB.disabled = !row.recB;
    };

    const load = async () => {
      if (!state.aPath || !state.bPath) return;
      setWait(true, "Loading diff…");
      try {
        const url = `/api/snapshots/diff/extended?a=${encodeURIComponent(state.aPath)}&b=${encodeURIComponent(
          state.bPath
        )}&kind=all&q=&offset=0&limit=20000&max_changes=250&max_depth=6`;
        const res = await fjson(url);
        const d = res?.diff;
        if (!d || d.ok === false) throw new Error(d?.error || "Invalid response");

        state.diff = d;
        const items = Array.isArray(d.items) ? d.items : [];
        state.rows = items.map(normalizeRow).filter((r) => r.key);

        computeCounts();
        renderMeta();
        applyFilters();
      } catch (e) {
        console.error("Capture Compare load failed:", e);
        const meta = Q("#cc-meta", root);
        if (meta) meta.textContent = `Error: ${String(e?.message || e)}`;
        Q("#cc-list-a", root).innerHTML = `<div class="empty">Failed to load diff.</div>`;
        Q("#cc-list-b", root).innerHTML = `<div class="empty">Failed to load diff.</div>`;
        Q("#cc-changes", root).innerHTML = `<div class="empty">—</div>`;
      } finally {
        setWait(false);
      }
    };

    // Events
    const onClickRow = (e) => {
      const row = e.target.closest?.(".cc-row");
      if (!row) return;
      const key = String(row.dataset.key || "");
      if (!key) return;
      state.selectedKey = key;
      renderLists();
      renderDetail();
    };

    const listA = Q("#cc-list-a", root);
    const listB = Q("#cc-list-b", root);
    listA?.addEventListener("click", onClickRow);
    listB?.addEventListener("click", onClickRow);

    Q("#cc-search", root)?.addEventListener("input", (e) => {
      state.search = e.target.value || "";
      applyFilters();
    });

    Q("#cc-type", root)?.addEventListener("change", (e) => {
      state.type = e.target.value || "";
      applyFilters();
    });

    Q("#cc-sort", root)?.addEventListener("change", (e) => {
      state.sort = e.target.value || "status";
      applyFilters();
    });

    const changedBtn = Q("#cc-changed", root);
    const syncChangedBtn = () => {
      if (!changedBtn) return;
      changedBtn.classList.toggle("on", !state.st.has("unchanged"));
    };

    QA(".cc-chip[data-st]", root).forEach((chip) => {
      chip.addEventListener("click", () => {
        const st = String(chip.dataset.st || "");
        if (!st) return;
        if (state.st.has(st)) {
          state.st.delete(st);
          chip.classList.remove("on");
        } else {
          state.st.add(st);
          chip.classList.add("on");
        }
        syncChangedBtn();
        applyFilters();
      });
    });

    syncChangedBtn();
    if (changedBtn) {
      changedBtn.addEventListener("click", () => {
        const want = ["added", "removed", "updated"];
        const isChangedOnly = want.every((x) => state.st.has(x)) && !state.st.has("unchanged");
        state.st = new Set(isChangedOnly ? ["added", "removed", "updated", "unchanged"] : want);
        QA(".cc-chip[data-st]", root).forEach((chip) => {
          const st = String(chip.dataset.st || "");
          chip.classList.toggle("on", state.st.has(st));
        });
        syncChangedBtn();
        applyFilters();
      });
    }

    Q("#cc-refresh", root)?.addEventListener("click", () => load());
    Q("#cc-close", root)?.addEventListener("click", () => window.cxCloseModal?.());

    Q("#cc-copy-key", root)?.addEventListener("click", async (e) => {
      const btn = e.currentTarget;
      const ok = await copyText(state.selectedKey);
      flashCopy(btn, ok, "Copied", "Copy blocked");
    });

    Q("#cc-copy-a", root)?.addEventListener("click", async (e) => {
      const btn = e.currentTarget;
      const row = rowByKey(state.selectedKey);
      const ok = row?.recA ? await copyText(pretty(row.recA)) : false;
      flashCopy(btn, ok, "Copied A", "Copy blocked");
    });

    Q("#cc-copy-b", root)?.addEventListener("click", async (e) => {
      const btn = e.currentTarget;
      const row = rowByKey(state.selectedKey);
      const ok = row?.recB ? await copyText(pretty(row.recB)) : false;
      flashCopy(btn, ok, "Copied B", "Copy blocked");
    });

    const onChipCopy = async (e) => {
      const chip = e.target.closest?.('.chip[data-copy]');
      if (!chip) return;
      const val = String(chip.dataset.copy || "");
      if (!val) return;
      const ok = await copyText(val);
      if (!ok) return;
      chip.classList.add("copied");
      window.setTimeout(() => chip.classList.remove("copied"), 450);
    };
    root.addEventListener("click", onChipCopy);

    // Splitters
    const cleanup = [];
    const top = Q("#cc-top", root);
    const wrap = Q("#cc-wrap", root);
    const detailSplit = Q("#cc-detail-split", root);
    const paneAEl = Q("#cc-pane-a", root);
    const paneBEl = Q("#cc-pane-b", root);
    const recAEl = Q("#cc-rec-a", root);
    const recBEl = Q("#cc-rec-b", root);

    // One scrollbar
    let ccSyncing = false;
    const onSync = (src, dst) => () => {
      if (!src || !dst || ccSyncing) return;
      ccSyncing = true;
      dst.scrollTop = src.scrollTop;
      ccSyncing = false;
    };
    const onScrollA = onSync(recAEl, recBEl);
    const onScrollB = onSync(recBEl, recAEl);
    recAEl?.addEventListener("scroll", onScrollA, { passive: true });
    recBEl?.addEventListener("scroll", onScrollB, { passive: true });
    cleanup.push(() => {
      try { recAEl?.removeEventListener("scroll", onScrollA); } catch {}
      try { recBEl?.removeEventListener("scroll", onScrollB); } catch {}
    });


    const applyDefaultLayout = () => {
      const h = wrap?.getBoundingClientRect?.().height || 800;
      const topH = Math.max(240, Math.round(h * 0.44));
      if (!state.layout.topH) root.style.setProperty("--ccTopH", `${topH}px`);
      if (!state.layout.aW) {
        const r = top?.getBoundingClientRect?.();
        const splitW = parseInt(getComputedStyle(root).getPropertyValue("--ccSplitW")) || 10;
        const half = r ? Math.floor((r.width - splitW) * 0.5) : null;
        if (paneAEl) paneAEl.style.flex = half ? `0 0 ${Math.max(360, half)}px` : "1 1 0";
        if (paneBEl) paneBEl.style.flex = "1 1 0";
      }
      if (!state.layout.detailAW) {
        const r = detailSplit?.getBoundingClientRect?.();
        const splitW = parseInt(getComputedStyle(root).getPropertyValue("--ccSplitW")) || 10;
        const half = r ? Math.floor((r.width - splitW) * 0.5) : null;
        if (recAEl) recAEl.style.flex = half ? `0 0 ${Math.max(320, half)}px` : "1 1 0";
        if (recBEl) recBEl.style.flex = "1 1 0";
      }
    };

    applyDefaultLayout();
    window.setTimeout(applyDefaultLayout, 50);

    const onResize = () => {
      if (!state.layout.aW || !state.layout.detailAW || !state.layout.topH) applyDefaultLayout();
    };
    window.addEventListener("resize", onResize);
    cleanup.push(() => window.removeEventListener("resize", onResize));

    const vTop = Q("#cc-vsplit-top", root);
    if (vTop && top) {
      cleanup.push(
        initSplit({
          handle: vTop,
          container: top,
          axis: "x",
          getMin: () => 360,
          getMax: (rect) => rect.width - 360,
          onSet: (delta, min, max) => {
            const cur = paneAEl?.getBoundingClientRect?.().width || rectHalf(top);
            const nxt = clamp(cur + delta, min, max);
            if (paneAEl) paneAEl.style.flex = `0 0 ${nxt}px`;
            if (paneBEl) paneBEl.style.flex = "1 1 0";
            state.layout.aW = nxt;
          },
        })
      );
    }

    const hSplit = Q("#cc-hsplit", root);
    if (hSplit && wrap) {
      cleanup.push(
        initSplit({
          handle: hSplit,
          container: wrap,
          axis: "y",
          getMin: () => 260,
          getMax: (rect) => rect.height - 220,
          onSet: (delta, min, max) => {
            const cur = parseInt(getComputedStyle(root).getPropertyValue("--ccTopH")) || rectHalfY(wrap);
            const nxt = clamp(cur + delta, min, max);
            root.style.setProperty("--ccTopH", `${nxt}px`);
            state.layout.topH = nxt;
          },
        })
      );
    }

    const vDetail = Q("#cc-vsplit-detail", root);
    if (vDetail && detailSplit) {
      cleanup.push(
        initSplit({
          handle: vDetail,
          container: detailSplit,
          axis: "x",
          getMin: () => 340,
          getMax: (rect) => rect.width - 340,
          onSet: (delta, min, max) => {
            const cur = recAEl?.getBoundingClientRect?.().width || rectHalf(detailSplit);
            const nxt = clamp(cur + delta, min, max);
            if (recAEl) recAEl.style.flex = `0 0 ${nxt}px`;
            if (recBEl) recBEl.style.flex = "1 1 0";
            state.layout.detailAW = nxt;
          },
        })
      );
    }

    function rectHalf(el) {
      const r = el.getBoundingClientRect();
      return Math.round(r.width * 0.5);
    }

    function rectHalfY(el) {
      const r = el.getBoundingClientRect();
      return Math.round(r.height * 0.58);
    }

    // Bootstrap
    await load();

    root._ccCleanup = () => {
      try {
        listA?.removeEventListener("click", onClickRow);
        listB?.removeEventListener("click", onClickRow);
        root.removeEventListener("click", onChipCopy);
      } catch {}
      cleanup.forEach((fn) => {
        try {
          fn();
        } catch {}
      });
    };
  },

  unmount() {
    try {
      if (this._root && this._root._ccCleanup) this._root._ccCleanup();
    } catch {}
  },
};
