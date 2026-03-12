/* assets/js/modals/capture-compare/index.js */
/* refactored */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
const fjson = async (u, o = {}) => {
  const r = await fetch(u, { cache: "no-store", ...o });
  if (!r.ok) throw new Error(String(r.status));
  return r.json();
};
const Q = (s, r = document) => r.querySelector(s);
const QA = (s, r = document) => [...r.querySelectorAll(s)];
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const clamp = (n, a, b) => Math.max(a, Math.min(b, n));
const emptyCounts = () => ({ movie: 0, show: 0, season: 0, episode: 0, unknown: 0, total: 0 });
const STATUS_ORDER = { added: 0, removed: 1, updated: 2, unchanged: 3 };
const STATUS_CLASS = { added: "add", removed: "del", updated: "upd", unchanged: "unc" };
const STATUS_LABEL = { added: "Added", removed: "Deleted", updated: "Updated", unchanged: "Unchanged" };
const SPLIT = "10px";
const ROOT_HTML = `
<div class="cx-head"><div class="cc-left"><div class="cc-mark" aria-hidden="true"><span class="material-symbols-rounded">difference</span></div><div class="cc-title">Capture Compare</div><div class="cc-meta" id="cc-meta" hidden></div></div><div class="cc-actions"><button class="pill" id="cc-refresh" type="button">Refresh</button><button class="close-btn" id="cc-close" type="button">Close</button></div></div>
<div class="cc-toolbar"><input id="cc-search" type="search" placeholder="Search title, ids, key…"><select id="cc-feature" title="Feature"><option value="">Feature</option></select><div class="cc-chip add on" data-st="added">Added</div><div class="cc-chip del on" data-st="removed">Deleted</div><div class="cc-chip on" data-st="updated">Updated</div><div class="cc-chip unc" data-st="unchanged">Unchanged</div><select id="cc-type"><option value="">All types</option><option value="movie">Movies</option><option value="show">Shows</option><option value="season">Seasons</option><option value="episode">Episodes</option></select><select id="cc-sort"><option value="status">Sort: status</option><option value="title">Sort: title</option><option value="key">Sort: key</option></select><div class="cc-chip on" id="cc-changed">Changed only</div></div>
<div class="cc-wrap" id="cc-wrap"><div class="cc-top" id="cc-top"><div class="cc-pane" id="cc-pane-a"><div class="cc-pane-head"><div class="h"><div class="t" id="cc-a-title">File A - Capture</div><div class="s" id="cc-a-sub">—</div></div><span class="tag">A</span></div><div class="cc-pane-list" id="cc-list-a"></div><div class="cc-pane-foot"><div id="cc-a-pills"></div><div class="cc-foot-mini" id="cc-a-total">—</div></div></div><div class="cc-vsplit" id="cc-vsplit-top" title="drag to resize"></div><div class="cc-pane" id="cc-pane-b"><div class="cc-pane-head"><div class="h"><div class="t" id="cc-b-title">File B - Capture</div><div class="s" id="cc-b-sub">—</div></div><span class="tag">B</span></div><div class="cc-pane-list" id="cc-list-b"></div><div class="cc-pane-foot"><div id="cc-b-pills"></div><div class="cc-foot-mini" id="cc-b-total">—</div></div></div></div><div class="cc-hsplit" id="cc-hsplit" title="drag to resize"></div><div class="cc-bottom"><div class="cc-detail-head"><div class="h"><div class="t"><span class="tt" id="cc-d-title">Select an item</span><span class="k mono" id="cc-d-key">—</span></div></div><div class="cc-detail-actions"><button class="pill ghost" id="cc-copy-key" type="button">Copy key</button><button class="pill" id="cc-copy-a" type="button">Copy JSON A</button><button class="pill" id="cc-copy-b" type="button">Copy JSON B</button></div></div><div class="cc-bottom-wrap"><div class="cc-changes" id="cc-changes"></div><div class="cc-detail-split" id="cc-detail-split"><div class="cc-rec" id="cc-rec-a"></div><div class="cc-vsplit" id="cc-vsplit-detail" title="drag to resize"></div><div class="cc-rec" id="cc-rec-b"></div></div></div></div></div>
<div id="cc-wait" class="wait-overlay hidden"><div class="wait-card" role="status" aria-live="assertive"><div class="wait-ring"></div><div class="wait-text" id="cc-wait-text">Loading…</div></div></div>`;
const CSS = `.cx-modal-shell.cc-modal{position:relative;background:linear-gradient(180deg,rgba(7,9,14,.985),rgba(4,6,10,.985))!important;width:min(var(--cxModalMaxW,1620px),calc(100vw - 120px))!important;border-radius:28px!important;border:1px solid rgba(255,255,255,.08)!important;box-shadow:0 28px 80px rgba(0,0,0,.56)!important;overflow:hidden;isolation:isolate}.cx-modal-shell.cc-modal::before{content:"";position:absolute;inset:0;pointer-events:none;background:radial-gradient(120% 100% at 0 0,rgba(88,72,168,.08),transparent 40%),radial-gradient(80% 90% at 100% 100%,rgba(30,48,98,.06),transparent 44%),linear-gradient(180deg,rgba(255,255,255,.03),transparent 26%);z-index:0}.cc-modal{position:relative;display:flex;flex-direction:column;height:100%;background:transparent;color:#edf2ff}.cc-modal::before{content:none}.cc-modal>*{position:relative;z-index:1}.cc-modal .cx-head,.cc-modal .cc-pane-head,.cc-modal .cc-detail-head{display:flex;align-items:center;justify-content:space-between;gap:10px}.cc-modal .cx-head{padding:12px 14px;border-bottom:1px solid rgba(255,255,255,.07);background:linear-gradient(180deg,rgba(13,16,24,.92),rgba(7,10,16,.88));backdrop-filter:blur(14px)}.cc-modal .cc-left{display:flex;align-items:center;gap:12px;flex-wrap:wrap;min-width:0;flex:1}.cc-modal .cc-mark{width:36px;height:36px;border-radius:12px;display:grid;place-items:center;background:linear-gradient(135deg,rgba(94,226,172,.18),rgba(56,189,248,.12));border:1px solid rgba(79,209,156,.22);box-shadow:inset 0 0 0 1px rgba(255,255,255,.03);flex:0 0 auto}.cc-modal .cc-mark .material-symbols-rounded{font-variation-settings:"FILL" 0,"wght" 500,"GRAD" 0,"opsz" 24;font-size:18px;line-height:1;color:#f3f6ff}.cc-modal .cc-title{display:inline-flex;align-items:center;gap:10px;font-weight:900;font-size:18px;letter-spacing:.08em;text-transform:uppercase;color:#f3f6ff;text-shadow:0 0 18px rgba(104,122,255,.16);white-space:nowrap}.cc-modal .cc-meta,.cc-modal .cc-sub,.cc-modal .cc-mini,.cc-modal .cc-foot-mini,.cc-modal .kv .k,.cc-modal .empty,.cc-modal .cc-pane-head .s,.cc-modal .cc-detail-head .k,.cc-modal .cc-rec .card .ttl .mini{font-size:12px;color:rgba(205,214,230,.72);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.cc-modal .cc-actions,.cc-modal .cc-pills,.cc-modal .chips,.cc-modal .cc-detail-actions{display:flex;flex-wrap:wrap;gap:8px;align-items:center}.cc-modal .pill,.cc-modal .close-btn,.cc-modal .stat-pill,.cc-modal .chip{display:inline-flex;align-items:center;justify-content:center;gap:6px;padding:0 12px;border-radius:999px;border:1px solid rgba(255,255,255,.1);background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.03));color:#eef3ff;font-size:12px}.cc-modal .pill,.cc-modal .close-btn{min-height:34px;font-weight:800}.cc-modal .pill.ghost{background:rgba(255,255,255,.03)}.cc-modal .pill[disabled]{opacity:.55;pointer-events:none}.cc-modal .pill.cc-copied{border-color:rgba(35,213,255,.5);box-shadow:0 0 0 1px rgba(35,213,255,.26),0 10px 24px rgba(35,213,255,.12)}.cc-modal .pill.cc-fail{border-color:rgba(255,59,127,.55);box-shadow:0 0 0 1px rgba(255,59,127,.24),0 10px 24px rgba(255,59,127,.1)}.cc-modal .cc-toolbar{display:flex;flex-wrap:nowrap;gap:8px;padding:10px 14px;border-bottom:1px solid rgba(255,255,255,.06);background:rgba(6,8,13,.9);align-items:center;overflow-x:auto;scrollbar-width:none}.cc-modal .cc-toolbar::-webkit-scrollbar{display:none}.cc-modal input[type=search],.cc-modal select{min-height:36px;background:rgba(4,6,10,.92);border:1px solid rgba(255,255,255,.1);color:#dbe8ff;border-radius:12px;padding:0 10px}.cc-modal input[type=search]{min-width:220px;flex:1 1 320px;max-width:520px}.cc-modal .cc-chip{cursor:pointer;user-select:none;font-size:11px;display:inline-flex;align-items:center;gap:6px;padding:0 10px;min-height:32px;border-radius:999px;border:1px solid rgba(255,255,255,.1);background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.03));color:#f5f6ff;font-weight:850;letter-spacing:.05em;text-transform:uppercase;transition:transform .12s ease,border-color .12s ease,box-shadow .12s ease;white-space:nowrap}.cc-modal .cc-chip:hover,.cc-modal .chip:hover{transform:translateY(-1px);border-color:rgba(122,107,255,.55)}.cc-modal .cc-chip.on{border-color:rgba(122,107,255,.72);box-shadow:0 0 0 1px rgba(122,107,255,.24)}.cc-modal .cc-chip.add.on{border-color:rgba(35,213,255,.66);box-shadow:0 0 0 1px rgba(35,213,255,.22)}.cc-modal .cc-chip.del.on{border-color:rgba(255,59,127,.68);box-shadow:0 0 0 1px rgba(255,59,127,.22)}.cc-modal .cc-chip.unc.on{border-color:rgba(255,255,255,.28)}.cc-modal .cc-wrap,.cc-modal .cc-bottom-wrap{flex:1;min-height:0;display:flex;flex-direction:column;overflow:hidden}.cc-modal .cc-top{flex:0 0 var(--ccTopH,43%);min-height:250px;display:flex;overflow:hidden;border-top:1px solid rgba(255,255,255,.04)}.cc-modal .cc-hsplit,.cc-modal .cc-vsplit{position:relative;flex:0 0 var(--ccSplitW,10px);background:transparent;touch-action:none}.cc-modal .cc-hsplit{height:var(--ccSplitW,10px);cursor:row-resize}.cc-modal .cc-vsplit{width:var(--ccSplitW,10px);cursor:col-resize}.cc-modal .cc-hsplit::after,.cc-modal .cc-vsplit::after{content:"";position:absolute;border-radius:999px}.cc-modal .cc-hsplit::after{left:0;right:0;top:50%;height:2px;transform:translateY(-50%);background:linear-gradient(90deg,rgba(122,107,255,.1),rgba(122,107,255,.8),rgba(122,107,255,.1))}.cc-modal .cc-vsplit::after{top:0;bottom:0;left:50%;width:2px;transform:translateX(-50%);background:linear-gradient(180deg,rgba(122,107,255,.1),rgba(122,107,255,.8),rgba(122,107,255,.1))}.cc-modal #cc-pane-a,.cc-modal #cc-pane-b,.cc-modal #cc-rec-a,.cc-modal #cc-rec-b{flex:1 1 0;min-width:320px}.cc-modal .cc-pane,.cc-modal .cc-bottom{min-width:0;display:flex;flex-direction:column;overflow:hidden;background:transparent}.cc-modal .cc-pane-head,.cc-modal .cc-detail-head{padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.07);background:linear-gradient(180deg,rgba(12,15,23,.9),rgba(6,8,13,.82))}.cc-modal .cc-pane-head .h,.cc-modal .cc-detail-head .h{min-width:0}.cc-modal .cc-pane-head .t,.cc-modal .cc-detail-head .tt{font-weight:900;letter-spacing:-.01em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.cc-modal .cc-pane-head .tag,.cc-modal .cc-st{font-size:11px;font-weight:900;letter-spacing:.05em;border-radius:999px;text-transform:uppercase}.cc-modal .cc-pane-head .tag{padding:0 8px;min-height:24px;border:1px solid rgba(255,255,255,.11);background:rgba(255,255,255,.05);display:inline-flex;align-items:center}.cc-modal .cc-pane-list{flex:1;min-height:0;overflow:auto}.cc-modal .cc-row{display:grid;grid-template-columns:auto minmax(0,1fr) auto;gap:10px;padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.05);align-items:center;cursor:pointer}.cc-modal .cc-row:hover{background:rgba(255,255,255,.03)}.cc-modal .cc-row.sel{background:rgba(122,107,255,.08);box-shadow:inset 0 0 0 1px rgba(122,107,255,.34)}.cc-modal .cc-st{padding:5px 8px;border:1px solid rgba(255,255,255,.12)}.cc-modal .cc-st.add{background:rgba(35,213,255,.12);border-color:rgba(35,213,255,.35)}.cc-modal .cc-st.del{background:rgba(255,59,127,.12);border-color:rgba(255,59,127,.35)}.cc-modal .cc-st.upd{background:rgba(122,107,255,.14);border-color:rgba(122,107,255,.35)}.cc-modal .cc-st.unc{background:rgba(255,255,255,.07)}.cc-modal .cc-title2{font-weight:850;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.cc-modal .cc-pane-foot{padding:10px 12px;border-top:1px solid rgba(255,255,255,.07);display:flex;flex-wrap:wrap;gap:8px;align-items:center;justify-content:space-between;background:rgba(6,8,13,.84)}.cc-modal .stat-pill,.cc-modal .chip{min-height:30px;border-color:rgba(255,255,255,.12);background:rgba(255,255,255,.05)}.cc-modal .stat-pill{font-weight:900;letter-spacing:.02em}.cc-modal .stat-pill.movie{border-color:rgba(35,213,255,.35);background:rgba(35,213,255,.1)}.cc-modal .stat-pill.show{border-color:rgba(122,107,255,.35);background:rgba(122,107,255,.1)}.cc-modal .stat-pill.season{border-color:rgba(255,255,255,.18);background:rgba(255,255,255,.06)}.cc-modal .stat-pill.episode{border-color:rgba(255,59,127,.25);background:rgba(255,59,127,.08)}.cc-modal .cc-bottom{flex:1;min-height:320px;border-top:1px solid rgba(255,255,255,.06)}.cc-modal .cc-changes{flex:0 0 auto;padding:8px 12px;border-bottom:1px solid rgba(255,255,255,.06);overflow:auto;max-height:180px}.cc-modal .cc-changes.hidden{display:none}.cc-modal .chg{display:grid;grid-template-columns:minmax(140px,220px) 1fr 1fr;gap:10px;padding:9px 10px;border:1px solid rgba(255,255,255,.08);border-radius:14px;background:rgba(255,255,255,.03);margin-bottom:8px}.cc-modal .chg .p{font-weight:900;opacity:.95}.cc-modal .chg .v,.cc-modal .kv{font-size:12.5px;opacity:.95;word-break:break-word}.cc-modal .chg .v{font-size:12px;opacity:.88;white-space:pre-wrap}.cc-modal .chg .lab{font-size:11px;color:rgba(205,214,230,.68);margin-bottom:4px;text-transform:uppercase;letter-spacing:.04em}.cc-modal .cc-detail-split{flex:1;min-height:0;display:flex;overflow:hidden}.cc-modal .cc-rec{min-width:0;overflow:auto;padding:10px 12px}.cc-modal .cc-rec .card{border-radius:18px;padding:12px;background:linear-gradient(180deg,rgba(12,15,23,.9),rgba(5,7,11,.94));border:1px solid rgba(255,255,255,.09);box-shadow:0 12px 30px rgba(0,0,0,.22)}.cc-modal .cc-rec .card+.card{margin-top:10px}.cc-modal .cc-rec .card .ttl{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:8px}.cc-modal .cc-rec .card .ttl .name{font-weight:950}.cc-modal .kv{display:grid;grid-template-columns:minmax(120px,160px) 1fr;gap:6px 10px}.cc-modal .chip:hover{background:rgba(122,107,255,.08)}.cc-modal .chip.copied{border-color:rgba(35,213,255,.55);background:rgba(35,213,255,.1)}.cc-modal .chip .k{opacity:.75;text-transform:uppercase;letter-spacing:.03em;font-weight:850}.cc-modal .mono{font-family:ui-monospace,SFMono-Regular,Consolas,monospace}.cc-modal details{border-radius:14px;border:1px solid rgba(255,255,255,.1);background:rgba(255,255,255,.03);padding:10px 11px;margin-top:10px}.cc-modal details>summary{cursor:pointer;font-weight:900;opacity:.95}.cc-modal pre{margin:10px 0 0;white-space:pre-wrap;word-break:break-word;background:#04050a;border:1px solid rgba(255,255,255,.1);border-radius:12px;padding:10px 12px}.wait-overlay{position:fixed;inset:0;display:flex;align-items:center;justify-content:center;background:rgba(3,4,10,.74);backdrop-filter:blur(7px);z-index:9999;opacity:1;transition:opacity .18s ease}.wait-overlay.hidden{opacity:0;pointer-events:none}.wait-card{display:flex;flex-direction:column;align-items:center;gap:14px;padding:22px 28px;border-radius:20px;background:linear-gradient(180deg,rgba(8,10,15,.97),rgba(5,7,11,.97));border:1px solid rgba(255,255,255,.08);box-shadow:0 24px 60px rgba(0,0,0,.44)}.wait-ring{width:64px;height:64px;border-radius:50%;position:relative;filter:drop-shadow(0 0 12px rgba(122,107,255,.35))}.wait-ring::before{content:"";position:absolute;inset:0;border-radius:50%;padding:4px;background:conic-gradient(#7a6bff,#23d5ff,#7a6bff);-webkit-mask:linear-gradient(#000 0 0) content-box,linear-gradient(#000 0 0);-webkit-mask-composite:xor;mask-composite:exclude;animation:wait-spin 1.1s linear infinite}.wait-text{font-weight:950;color:#dbe8ff}@keyframes wait-spin{to{transform:rotate(360deg)}}@media (max-width:980px){.cx-modal-shell.cc-modal{width:min(100vw - 28px,1620px)!important}.cc-modal .cc-top,.cc-modal .cc-detail-split{flex-direction:column}.cc-modal #cc-pane-a,.cc-modal #cc-pane-b,.cc-modal #cc-rec-a,.cc-modal #cc-rec-b{min-width:0}.cc-modal .cc-vsplit{width:100%;height:10px;cursor:row-resize}.cc-modal .cc-vsplit::after{top:50%;bottom:auto;left:0;right:0;width:auto;height:2px;transform:translateY(-50%)}}`;

const css = () => {
  let el = Q("#cc-css");
  if (!el) {
    el = document.createElement("style");
    el.id = "cc-css";
    document.head.appendChild(el);
  }
  el.textContent = CSS;
};

function kindOf(r) {
  if (!r || typeof r !== "object") return "unknown";
  const t = String(r.type || r.media_type || r.entity || "").toLowerCase();
  if (t === "episode" || r.episode != null) return "episode";
  if (t === "season" || r.season != null) return "season";
  if (["tv", "show", "shows", "series", "anime"].includes(t)) return "show";
  if (["movie", "movies", "film", "films"].includes(t)) return "movie";
  return t || "unknown";
}

function displayTitle(r) {
  if (!r || typeof r !== "object") return "Item";
  const t = String(r.type || "").toLowerCase();
  const series = String(r.series_title || r.show_title || r.series || "");
  const season = r.season != null ? String(r.season).padStart(2, "0") : "";
  const episode = r.episode != null ? String(r.episode).padStart(2, "0") : "";
  if (t === "episode" && series && season && episode) return `${series} - S${season}E${episode}`;
  if (t === "season" && series && season) return `${series} - S${season}`;
  return `${String(r.title || series || kindOf(r))}${r.year ? ` (${r.year})` : ""}`;
}

function displaySub(r) {
  if (!r || typeof r !== "object") return "";
  const out = [];
  const k = kindOf(r);
  if (k !== "unknown") out.push(k);
  if (r.year) out.push(String(r.year));
  if (k === "episode" && r.season != null && r.episode != null) out.push(`S${String(r.season).padStart(2, "0")}E${String(r.episode).padStart(2, "0")}`);
  else if (k === "season" && r.season != null) out.push(`S${String(r.season).padStart(2, "0")}`);
  if (r.watched_at) out.push("watched");
  return out.join(" • ");
}

const stringify = (v) => {
  if (v === null) return "null";
  if (v === undefined) return "—";
  if (["string", "number", "boolean"].includes(typeof v)) return String(v);
  try { return JSON.stringify(v); } catch { return String(v); }
};
const pretty = (v) => { try { return JSON.stringify(v, null, 2); } catch { return String(v); } };

async function copyText(t) {
  const s = String(t ?? "");
  if (!s) return false;
  try {
    await navigator.clipboard.writeText(s);
    return true;
  } catch {
    try {
      const ta = Object.assign(document.createElement("textarea"), { value: s });
      ta.style.cssText = "position:fixed;left:-9999px";
      document.body.appendChild(ta);
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
  btn.dataset.ccOrig ||= btn.textContent || "";
  btn.classList.remove("cc-copied", "cc-fail");
  btn.classList.add(ok ? "cc-copied" : "cc-fail");
  btn.textContent = ok ? okText : failText;
  window.setTimeout(() => {
    btn.textContent = btn.dataset.ccOrig || "";
    btn.classList.remove("cc-copied", "cc-fail");
  }, 850);
}

function countsFor(records) {
  return records.reduce((out, r) => {
    if (!r) return out;
    out[kindOf(r)] = (out[kindOf(r)] || 0) + 1;
    out.total++;
    return out;
  }, emptyCounts());
}

function countsFromSnapshotMeta(meta, fallback) {
  const base = fallback && typeof fallback === "object" ? { ...fallback } : emptyCounts();
  const by = meta?.by_type && typeof meta.by_type === "object" ? meta.by_type : null;
  if (!by) return base;
  const out = emptyCounts();
  const feat = String(meta.feature || "").toLowerCase();
  for (const [rawKey, rawVal] of Object.entries(by)) {
    const n = Number(rawVal || 0), k = String(rawKey || "").toLowerCase();
    if (!Number.isFinite(n) || n <= 0) continue;
    if (["movie", "movies", "film", "films"].includes(k)) out.movie += n;
    else if (["episode", "episodes"].includes(k)) out.episode += n;
    else if (["season", "seasons"].includes(k)) out.season += n;
    else if (["show", "shows", "series", "anime"].includes(k)) out.show += n;
    else if (k === "tv") feat === "history" ? (out.episode += n) : (out.show += n);
    else out.unknown += n;
  }
  const total = Number(meta.count || 0);
  out.total = Number.isFinite(total) && total > 0 ? total : out.movie + out.show + out.season + out.episode + out.unknown;
  return out;
}

const renderCountsPills = (c) => `<div class="cc-pills"><span class="stat-pill movie">Movies ${c.movie || 0}</span><span class="stat-pill show">Shows ${c.show || 0}</span><span class="stat-pill season">Seasons ${c.season || 0}</span><span class="stat-pill episode">Episodes ${c.episode || 0}</span></div>`;

function renderRecordCard(label, rec, missingText = "Missing") {
  if (!rec) return `<div class="card"><div class="ttl"><div class="name">${esc(label)}</div><div class="mini">${esc(missingText)}</div></div><div class="empty">No record in this file.</div></div>`;
  const kv = [["Type", kindOf(rec)], ["Title", rec.title], ["Series", rec.series_title || rec.show_title], ["Year", rec.year], ["Season", rec.season], ["Episode", rec.episode], ["Watched", rec.watched_at], ["Added", rec.added_at], ["Updated", rec.updated_at]].filter(([, v]) => v != null && v !== "");
  const chips = [];
  for (const [prefix, obj] of [["", rec.ids], ["show.", rec.show_ids]]) {
    if (!obj || typeof obj !== "object") continue;
    for (const [k, v] of Object.entries(obj)) {
      if (v == null || v === "" || v === 0 || v === false) continue;
      chips.push(`<span class="chip" data-copy="${esc(v)}" title="Click to copy"><span class="k">${esc(prefix + k)}</span><span class="v mono">${esc(v)}</span></span>`);
    }
  }
  return `<div class="card"><div class="ttl"><div class="name">${esc(label)}</div><div class="mini">${esc(displayTitle(rec))}</div></div><div class="kv">${kv.map(([k, v]) => `<div class="k">${esc(k)}</div><div class="v">${esc(stringify(v))}</div>`).join("")}</div>${chips.length ? `<div class="chips">${chips.join("")}</div>` : ""}<details data-cc-raw="1"><summary>Raw JSON</summary><pre class="mono">${esc(pretty(rec))}</pre></details></div>`;
}

const renderChanges = (changes) => !Array.isArray(changes) || !changes.length ? `<div class="empty">No field-level changes for this item.</div>` : changes.slice(0, 200).map((c) => `<div class="chg"><div class="p mono">${esc(String(c.path || ""))}</div><div><div class="lab">A</div><div class="v mono">${esc(stringify(c.old))}</div></div><div><div class="lab">B</div><div class="v mono">${esc(stringify(c.new))}</div></div></div>`).join("");

function initSplit({ handle, container, axis, getMin, getMax, onSet }) {
  let dragging = false, start = 0;
  const pos = (e) => axis === "x" ? e.clientX : e.clientY;
  const onDown = (e) => {
    dragging = true;
    start = pos(e);
    handle.setPointerCapture?.(e.pointerId);
    document.body.style.userSelect = "none";
    e.preventDefault();
  };
  const onMove = (e) => {
    if (!dragging) return;
    const next = pos(e), delta = next - start, rect = container.getBoundingClientRect();
    start = next;
    onSet(delta, getMin(rect), getMax(rect), rect);
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
    this._root = root;
    css();
    Object.entries({ "--cxModalMaxW": "1700px", "--cxModalMaxH": "94vh", "--ccSplitW": SPLIT }).forEach(([k, v]) => root.style.setProperty(k, v));
    root.classList.add("modal-root", "cc-modal");
    root.innerHTML = ROOT_HTML;

    const el = Object.fromEntries(["meta", "aTitle", "aSub", "aPills", "aTotal", "bTitle", "bSub", "bPills", "bTotal", "listA", "listB", "changes", "dTitle", "dKey", "recA", "recB", "wait", "waitText", "search", "feature", "type", "sort", "changed", "top", "wrap", "detailSplit", "paneA", "paneB", "copyKey", "copyA", "copyB", "refresh", "close"].map((k) => [k, Q({ meta: "#cc-meta", aTitle: "#cc-a-title", aSub: "#cc-a-sub", aPills: "#cc-a-pills", aTotal: "#cc-a-total", bTitle: "#cc-b-title", bSub: "#cc-b-sub", bPills: "#cc-b-pills", bTotal: "#cc-b-total", listA: "#cc-list-a", listB: "#cc-list-b", changes: "#cc-changes", dTitle: "#cc-d-title", dKey: "#cc-d-key", recA: "#cc-rec-a", recB: "#cc-rec-b", wait: "#cc-wait", waitText: "#cc-wait-text", search: "#cc-search", feature: "#cc-feature", type: "#cc-type", sort: "#cc-sort", changed: "#cc-changed", top: "#cc-top", wrap: "#cc-wrap", detailSplit: "#cc-detail-split", paneA: "#cc-pane-a", paneB: "#cc-pane-b", copyKey: "#cc-copy-key", copyA: "#cc-copy-a", copyB: "#cc-copy-b", refresh: "#cc-refresh", close: "#cc-close" }[k], root)]));
    const state = { aPath: String(props.aPath || props.a || ""), bPath: String(props.bPath || props.b || ""), diff: null, rows: [], filtered: [], selectedKey: "", search: "", st: new Set(["added", "removed", "updated"]), feature: String(props.feature || props.compareFeature || "").toLowerCase(), featureOptions: [], type: "", sort: "status", countsA: emptyCounts(), countsB: emptyCounts(), layout: { topH: null, aW: null, detailAW: null } };
    const cleanup = [];
    let syncing = false;

    const setWait = (on, text = "Loading…") => {
      if (el.waitText) el.waitText.textContent = text;
      el.wait?.classList.toggle("hidden", !on);
    };
    const rowByKey = (k) => state.rows.find((r) => r.key === k) || null;
    const normalizeRow = (r) => {
      const status = String(r.status || "unchanged"), recA = status === "added" ? null : r.old || r.item || null, recB = status === "removed" ? null : r.new || r.item || null;
      return { key: String(r.key || ""), status, brief: r.brief && typeof r.brief === "object" ? r.brief : recB || recA || {}, recA, recB, changes: Array.isArray(r.changes) ? r.changes : [] };
    };
    const renderSideList = (host, side) => {
      if (!host) return;
      const rows = state.filtered.filter((r) => side === "A" ? r.recA : r.recB);
      host.innerHTML = rows.length ? rows.map((r) => {
        const ref = side === "A" ? r.recA : r.recB, st = r.status, delta = st === "updated" ? `<span class="cc-mini mono">Δ${r.changes.length}</span>` : "";
        return `<div class="cc-row ${r.key === state.selectedKey ? "sel" : ""}" data-key="${esc(r.key)}"><span class="cc-st ${STATUS_CLASS[st] || "unc"}">${esc(st)}</span><div class="cc-main"><div class="cc-title2">${esc(displayTitle(ref || r.brief))}</div><div class="cc-sub">${esc(displaySub(ref || r.brief))}</div></div>${delta}</div>`;
      }).join("") : `<div class="empty">No matches.</div>`;
      host.querySelector(".cc-row.sel")?.scrollIntoView?.({ block: "nearest" });
    };
    const renderLists = () => {
      renderSideList(el.listA, "A");
      renderSideList(el.listB, "B");
    };
    const renderDetail = () => {
      const row = rowByKey(state.selectedKey);
      if (!row) {
        if (el.dTitle) el.dTitle.textContent = "Select an item";
        if (el.dKey) el.dKey.textContent = "—";
        el.changes?.classList.add("hidden");
        if (el.changes) el.changes.innerHTML = "";
        if (el.recA) el.recA.innerHTML = renderRecordCard("File A", null);
        if (el.recB) el.recB.innerHTML = renderRecordCard("File B", null);
        [el.copyKey, el.copyA, el.copyB].forEach((b) => b && (b.disabled = true));
        return;
      }
      const best = row.recB || row.recA || row.brief, status = row.status, delta = status === "updated" ? `<span class="mono" style="opacity:.75;font-size:12px">Δ${row.changes.length}</span>` : "";
      if (el.dTitle) el.dTitle.innerHTML = `<span class="cc-st ${STATUS_CLASS[status] || "unc"}">${esc(status)}</span><span class="tt">${esc(displayTitle(best))}</span>${delta}`;
      if (el.dKey) el.dKey.textContent = row.key;
      if (el.changes) {
        const show = status === "updated";
        el.changes.classList.toggle("hidden", !show);
        el.changes.innerHTML = show ? renderChanges(row.changes) : "";
      }
      if (el.recA) el.recA.innerHTML = renderRecordCard("File A", row.recA, status === "added" ? "(missing)" : "");
      if (el.recB) el.recB.innerHTML = renderRecordCard("File B", row.recB, status === "removed" ? "(missing)" : "");
      if (el.copyKey) el.copyKey.disabled = !row.key;
      if (el.copyA) el.copyA.disabled = !row.recA;
      if (el.copyB) el.copyB.disabled = !row.recB;
    };
    const renderMeta = () => {
      const d = state.diff, a = d?.a || {}, b = d?.b || {}, s = d?.summary || {}, fmt = (createdAt, count) => `${String(createdAt || "").replace("T", " ").replace("Z", "")} • ${count ?? "?"} items`;
      if (!el.meta) return;
      if (!d) return;
      const selectedFeature = String(d.selected_feature || a.feature || "").toLowerCase();
      if (el.aTitle) el.aTitle.textContent = "File A - Capture";
      if (el.bTitle) el.bTitle.textContent = "File B - Capture";
      if (el.aSub) el.aSub.textContent = fmt(a.created_at, a.count);
      if (el.bSub) el.bSub.textContent = fmt(b.created_at, b.count);
      state.featureOptions = Array.isArray(d.available_features) ? d.available_features.map((x) => String(x || "").toLowerCase()).filter(Boolean) : [];
      if (!state.feature && selectedFeature) state.feature = selectedFeature;
      if (el.feature) {
        const opts = state.featureOptions.length ? state.featureOptions : [selectedFeature].filter(Boolean);
        el.feature.innerHTML = opts.map((feat) => `<option value="${esc(feat)}">${esc(feat)}</option>`).join("");
        if (!opts.length) el.feature.innerHTML = `<option value="">Feature</option>`;
        el.feature.value = state.feature || selectedFeature || "";
        el.feature.disabled = opts.length <= 1;
      }
      const ca = countsFromSnapshotMeta(a, state.countsA), cb = countsFromSnapshotMeta(b, state.countsB);
      if (el.aPills) el.aPills.innerHTML = renderCountsPills(ca);
      if (el.bPills) el.bPills.innerHTML = renderCountsPills(cb);
      if (el.aTotal) el.aTotal.textContent = `Total: ${ca.total}`;
      if (el.bTotal) el.bTotal.textContent = `Total: ${cb.total}`;
    };
    const applyFilters = () => {
      const q = state.search.trim().toLowerCase(), wantType = state.type.toLowerCase();
      const rows = state.rows.filter((r) => state.st.has(r.status)).filter((r) => !wantType || kindOf(r.recB || r.recA || r.brief) === wantType).filter((r) => {
        if (!q) return true;
        const ref = r.recB || r.recA || r.brief, bits = [r.key, displayTitle(ref), displaySub(ref)];
        if (ref?.ids && typeof ref.ids === "object") bits.push(JSON.stringify(ref.ids));
        if (ref?.show_ids && typeof ref.show_ids === "object") bits.push(JSON.stringify(ref.show_ids));
        return bits.join(" ").toLowerCase().includes(q);
      });
      rows.sort((a, b) => state.sort === "key" ? a.key.localeCompare(b.key) : state.sort === "title" ? displayTitle(a.brief).localeCompare(displayTitle(b.brief)) : (STATUS_ORDER[a.status] ?? 99) - (STATUS_ORDER[b.status] ?? 99) || displayTitle(a.brief).localeCompare(displayTitle(b.brief)) || a.key.localeCompare(b.key));
      state.filtered = rows;
      if (state.selectedKey && !rows.some((r) => r.key === state.selectedKey)) state.selectedKey = "";
      if (!state.selectedKey && rows.length) state.selectedKey = rows[0].key;
      renderLists();
      renderDetail();
    };
    const computeCounts = () => {
      state.countsA = countsFor(state.rows.map((r) => r.recA).filter(Boolean));
      state.countsB = countsFor(state.rows.map((r) => r.recB).filter(Boolean));
    };
    const syncChanged = () => el.changed?.classList.toggle("on", !state.st.has("unchanged"));
    const load = async () => {
      if (!state.aPath || !state.bPath) return;
      setWait(true, "Loading diff…");
      try {
        const featureQ = state.feature ? `&feature=${encodeURIComponent(state.feature)}` : "";
        const res = await fjson(`/api/snapshots/diff/extended?a=${encodeURIComponent(state.aPath)}&b=${encodeURIComponent(state.bPath)}${featureQ}&kind=all&q=&offset=0&limit=20000&max_changes=250&max_depth=6`);
        const d = res?.diff;
        if (!d || d.ok === false) throw new Error(d?.error || "Invalid response");
        state.diff = d;
        state.feature = String(d.selected_feature || state.feature || "").toLowerCase();
        state.rows = (Array.isArray(d.items) ? d.items : []).map(normalizeRow).filter((r) => r.key);
        computeCounts();
        renderMeta();
        applyFilters();
      } catch (e) {
        console.error("Capture Compare load failed:", e);

        [el.listA, el.listB].forEach((x) => x && (x.innerHTML = `<div class="empty">Failed to load diff.</div>`));
        if (el.changes) el.changes.innerHTML = `<div class="empty">—</div>`;
      } finally {
        setWait(false);
      }
    };
    const syncScroll = (src, dst) => () => {
      if (!src || !dst || syncing) return;
      syncing = true;
      dst.scrollTop = src.scrollTop;
      syncing = false;
    };
    const syncRaw = (fromA, details) => {
      const other = (fromA ? el.recB : el.recA)?.querySelector?.('details[data-cc-raw="1"]');
      if (other) other.open = details.open;
      if (!el.recA || !el.recB) return;
      syncing = true;
      (fromA ? el.recB : el.recA).scrollTop = (fromA ? el.recA : el.recB).scrollTop;
      syncing = false;
    };
    const half = (node, axis = "width", ratio = .5) => Math.round((node?.getBoundingClientRect?.()[axis] || 0) * ratio);
    const applyDefaultLayout = () => {
      if (!state.layout.topH) root.style.setProperty("--ccTopH", `${Math.max(240, Math.round((el.wrap?.getBoundingClientRect?.().height || 800) * .44))}px`);
      const splitW = parseInt(getComputedStyle(root).getPropertyValue("--ccSplitW")) || 10;
      if (!state.layout.aW) {
        const width = Math.max(360, Math.floor(((el.top?.getBoundingClientRect?.().width || 0) - splitW) * .5));
        if (el.paneA) el.paneA.style.flex = width ? `0 0 ${width}px` : "1 1 0";
        if (el.paneB) el.paneB.style.flex = "1 1 0";
      }
      if (!state.layout.detailAW) {
        const width = Math.max(320, Math.floor(((el.detailSplit?.getBoundingClientRect?.().width || 0) - splitW) * .5));
        if (el.recA) el.recA.style.flex = width ? `0 0 ${width}px` : "1 1 0";
        if (el.recB) el.recB.style.flex = "1 1 0";
      }
    };

    const onInput = ({ target }) => {
      if (target === el.search) state.search = target.value || "";
      else if (target === el.feature) {
        const next = String(target.value || "").toLowerCase();
        if (next !== state.feature) {
          state.feature = next;
          return void load();
        }
      }
      else if (target === el.type) state.type = target.value || "";
      else if (target === el.sort) state.sort = target.value || "status";
      else return;
      applyFilters();
    };
    const onClick = async (e) => {
      const row = e.target.closest?.(".cc-row"), chip = e.target.closest?.(".chip[data-copy]"), summary = e.target.closest?.('details[data-cc-raw="1"]>summary'), stChip = e.target.closest?.(".cc-chip[data-st]");
      if (row) {
        state.selectedKey = String(row.dataset.key || "");
        renderLists();
        return void renderDetail();
      }
      if (chip) {
        const ok = await copyText(chip.dataset.copy || "");
        if (ok) {
          chip.classList.add("copied");
          return void window.setTimeout(() => chip.classList.remove("copied"), 450);
        }
      }
      if (summary) {
        const details = summary.parentElement, inA = !!summary.closest("#cc-rec-a"), inB = !!summary.closest("#cc-rec-b");
        if (inA || inB) {
          e.preventDefault();
          details.open = !details.open;
          return void syncRaw(inA, details);
        }
      }
      if (stChip) {
        const st = String(stChip.dataset.st || "");
        if (!st) return;
        state.st.has(st) ? state.st.delete(st) : state.st.add(st);
        stChip.classList.toggle("on", state.st.has(st));
        syncChanged();
        return void applyFilters();
      }
      const id = e.target.closest?.("button,[id]")?.id;
      if (id === "cc-refresh") return void load();
      if (id === "cc-close") return void window.cxCloseModal?.();
      if (id === "cc-changed") {
        const changedOnly = ["added", "removed", "updated"].every((x) => state.st.has(x)) && !state.st.has("unchanged");
        state.st = new Set(changedOnly ? Object.keys(STATUS_LABEL) : ["added", "removed", "updated"]);
        QA(".cc-chip[data-st]", root).forEach((x) => x.classList.toggle("on", state.st.has(x.dataset.st)));
        syncChanged();
        return void applyFilters();
      }
      const btn = e.target.closest?.("#cc-copy-key,#cc-copy-a,#cc-copy-b");
      if (!btn) return;
      const rowData = rowByKey(state.selectedKey), payload = btn.id === "cc-copy-key" ? state.selectedKey : btn.id === "cc-copy-a" ? pretty(rowData?.recA) : pretty(rowData?.recB), ok = btn.id === "cc-copy-a" ? !!rowData?.recA && await copyText(payload) : btn.id === "cc-copy-b" ? !!rowData?.recB && await copyText(payload) : await copyText(payload);
      flashCopy(btn, ok, btn.id === "cc-copy-a" ? "Copied A" : btn.id === "cc-copy-b" ? "Copied B" : "Copied", "Copy blocked");
    };

    root.addEventListener("click", onClick);
    root.addEventListener("input", onInput);
    root.addEventListener("change", onInput);
    cleanup.push(() => {
      root.removeEventListener("click", onClick);
      root.removeEventListener("input", onInput);
      root.removeEventListener("change", onInput);
    });

    const onScrollA = syncScroll(el.recA, el.recB), onScrollB = syncScroll(el.recB, el.recA);
    el.recA?.addEventListener("scroll", onScrollA, { passive: true });
    el.recB?.addEventListener("scroll", onScrollB, { passive: true });
    cleanup.push(() => {
      el.recA?.removeEventListener("scroll", onScrollA);
      el.recB?.removeEventListener("scroll", onScrollB);
    });

    applyDefaultLayout();
    window.setTimeout(applyDefaultLayout, 50);
    const onResize = () => (!state.layout.aW || !state.layout.detailAW || !state.layout.topH) && applyDefaultLayout();
    window.addEventListener("resize", onResize);
    cleanup.push(() => window.removeEventListener("resize", onResize));

    [["#cc-vsplit-top", el.top, "x", () => 360, (r) => r.width - 360, (d, min, max) => { const n = clamp((el.paneA?.getBoundingClientRect?.().width || half(el.top)) + d, min, max); if (el.paneA) el.paneA.style.flex = `0 0 ${n}px`; if (el.paneB) el.paneB.style.flex = "1 1 0"; state.layout.aW = n; }], ["#cc-hsplit", el.wrap, "y", () => 260, (r) => r.height - 220, (d, min, max) => { const n = clamp((parseInt(getComputedStyle(root).getPropertyValue("--ccTopH")) || half(el.wrap, "height", .58)) + d, min, max); root.style.setProperty("--ccTopH", `${n}px`); state.layout.topH = n; }], ["#cc-vsplit-detail", el.detailSplit, "x", () => 340, (r) => r.width - 340, (d, min, max) => { const n = clamp((el.recA?.getBoundingClientRect?.().width || half(el.detailSplit)) + d, min, max); if (el.recA) el.recA.style.flex = `0 0 ${n}px`; if (el.recB) el.recB.style.flex = "1 1 0"; state.layout.detailAW = n; }]].forEach(([sel, container, axis, getMin, getMax, onSet]) => {
      const handle = Q(sel, root);
      if (handle && container) cleanup.push(initSplit({ handle, container, axis, getMin, getMax, onSet }));
    });

    syncChanged();
    await load();
    root._ccCleanup = () => {
      cleanup.forEach((fn) => {
        try { fn(); } catch {}
      });
    };
  },

  unmount() {
    try {
      this._root?._ccCleanup?.();
    } catch {}
  },
};




