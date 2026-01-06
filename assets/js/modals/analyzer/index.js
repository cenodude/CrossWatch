// assets/js/modals/analyzer/index.js
const fjson = async (u, o) => {
  const r = await fetch(u, o);
  if (!r.ok) throw new Error(r.status);
  return r.json();
};
const Q = (s, r = document) => r.querySelector(s);
const QA = (s, r = document) => Array.from(r.querySelectorAll(s));
const esc = s =>
  (window.CSS?.escape ? CSS.escape(s) : String(s).replace(/[^\w-]/g, "\\$&"));
const tagOf = (p, f, k) => `${p}::${f}::${k}`;
const chips = ids =>
  Object.entries(ids || {})
    .map(([k, v]) => `<span class="chip mono">${k}:${v}</span>`)
    .join("");

const displayTitle = r => {
  const type = String(r.type || "").toLowerCase();
  const series = r.series_title || "";
  const season = r.season;
  const episode = r.episode;

  if (series && type === "episode" && season != null && episode != null) {
    const s = String(season).padStart(2, "0");
    const e = String(episode).padStart(2, "0");
    return `${series} - S${s}E${e}`;
  }
  if (series && type === "season" && season != null) {
    const s = String(season).padStart(2, "0");
    return `${series} - S${s}`;
  }
  return r.title || "Untitled";
};

const fmtCounts = c => {
  const entries = Object.entries(c || {});
  if (!entries.length) return "";
  const shortName = p => {
    const up = String(p || "").toUpperCase();
    if (up === "JELLYFIN") return "JF";
    if (up === "MDBLIST") return "MDB";
    if (up === "CROSSWATCH") return "CW";
    return up;
  };
  return entries
    .map(([p, v]) => {
      const total =
        v.total || (v.history || 0) + (v.watchlist || 0) + (v.ratings || 0);
      const label = shortName(p);
      const shortTotal =
        total > 999
          ? `${(total / 1000).toFixed(1).replace(/\.0$/, "")}k`
          : total;
      return `${label} ${shortTotal}`;
    })
    .join(" | ");
};

const ID_FIELDS = [
  "imdb",
  "tmdb",
  "tvdb",
  "trakt",
  "plex",
  "simkl",
  "emby",
  "mdblist"
];

function css() {
  if (Q("#an-css")) return;
  const el = document.createElement("style");
  el.id = "an-css";
  el.textContent = `
  .an-modal{position:relative;display:flex;flex-direction:column;height:100%}
  .an-modal .cx-head{display:flex;align-items:center;gap:10px;justify-content:space-between;background:linear-gradient(90deg,#05070d,#05040b);padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.08);box-shadow:0 0 24px rgba(0,0,0,.85)}
  .an-modal .cx-left{display:flex;align-items:center;gap:12px;flex:1}
  .an-modal .cx-title{font-weight:800}
  .an-modal .an-pairs{display:flex;flex-wrap:wrap;gap:6px;padding:4px 12px;border-bottom:1px solid rgba(255,255,255,.08);background:#05060c}
  .an-modal .an-pair-chip{font-size:12px;cursor:pointer;display:inline-flex;align-items:center;gap:6px;padding:4px 10px;border-radius:999px;border:1px solid rgba(255,255,255,.1);background:radial-gradient(circle at top,#12131f,#05060c);opacity:.85;color:#f5f6ff;font-weight:600;letter-spacing:.03em;text-transform:uppercase;box-shadow:0 0 10px rgba(0,0,0,.9);transition:background .16s ease,box-shadow .16s ease,opacity .16s ease,transform .12s ease}
  .an-modal .an-pair-chip:hover{opacity:1;box-shadow:0 0 18px rgba(122,107,255,.45);transform:translateY(-1px)}
  .an-modal .an-pair-chip.on{background:linear-gradient(120deg,#7a6bff,#23d5ff);box-shadow:0 0 18px rgba(122,107,255,.7);border-color:rgba(122,107,255,.9);opacity:1}
  .an-modal .an-pair-chip span.dir{opacity:.9}
  .an-modal .an-actions{display:flex;gap:8px;align-items:center}
  .an-modal .pill{border:1px solid rgba(255,255,255,.14);background:#080a12;color:#e5ecff;border-radius:16px;padding:6px 12px;font-size:13px;display:inline-flex;align-items:center;gap:6px;white-space:nowrap;flex:0 0 auto}
  .an-modal .pill.ghost{background:transparent}
  .an-modal .pill[disabled]{opacity:.55;pointer-events:none}
  .an-modal .close-btn{border:1px solid rgba(255,255,255,.16);background:#11131e;color:#fff;border-radius:10px;padding:6px 10px}
  .an-modal #an-toggle-ids{white-space:nowrap;min-width:110px;padding:6px 16px}
  .an-modal .badge{padding:3px 8px;border-radius:999px;border:1px solid rgba(255,255,255,.12);background:#10121e}
  .an-modal .an-wrap{flex:1;min-height:0;display:grid;grid-template-rows:minmax(220px,1fr) 8px minmax(180px,1fr);overflow:hidden}
  .an-modal .an-split{height:8px;background:linear-gradient(90deg,#141624,#3b1f5f);box-shadow:0 0 10px rgba(122,107,255,.55) inset;cursor:row-resize}
  .an-modal .an-grid{overflow:auto;border-bottom:1px solid rgba(255,255,255,.08);background:#05060c}
  .an-modal .an-issues{overflow:auto;padding:8px;background:#05060c}
  .an-modal .row{display:grid;gap:8px;padding:8px 10px;border-bottom:1px solid rgba(255,255,255,.06);align-items:center}
  .an-modal .head{position:sticky;top:0;background:#05060c;z-index:2}
  .an-modal .row.sel{outline:1px solid rgba(122,163,255,.9);background:rgba(122,107,255,.08)}
  .an-modal .chip{display:inline-block;border:1px solid rgba(255,255,255,.16);border-radius:999px;padding:2px 6px;margin:2px}
  .an-modal .mono{font-family:ui-monospace,SFMono-Regular,Consolas,monospace}
  .an-modal .ids{opacity:.9}
  .an-modal .an-grid.show-ids .ids{display:block}
  .an-modal .an-grid .ids{display:none}
  .an-modal .row .title{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .an-modal .row .title small{opacity:.7;margin-left:6px}
  .an-modal .row .prov{font-weight:600}
  .an-modal .row .feat{opacity:.8}
  .an-modal .row .counts{font-size:12px;opacity:.8}
  .an-modal .sort{cursor:pointer;user-select:none}
  .an-modal .sort span.label{margin-right:4px}
  .an-modal .sort span.dir{opacity:.7;font-size:11px}
  .an-modal .issue{border-radius:12px;padding:10px 11px;margin-bottom:8px;background:radial-gradient(circle at top left,#151624,#05060c);border:1px solid rgba(255,255,255,.1);box-shadow:0 0 18px rgba(0,0,0,.85)}
  .an-modal .issue .h{font-weight:700;margin-bottom:4px}
  .an-modal .issue .badge{margin-top:4px}
  .an-modal .issue.manual-ids{margin-top:4px}
  .an-modal .ids-edit{display:flex;flex-direction:column;gap:8px;margin-top:6px}
  .an-modal .ids-edit-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:6px}
  .an-modal .ids-edit-row label{display:flex;align-items:center;gap:6px;font-size:12px;opacity:.9}
  .an-modal .ids-edit-row label span{min-width:52px;text-transform:uppercase;letter-spacing:.03em;color:#9fb4ff}
  .an-modal .ids-edit-row input{flex:1 1 auto;background:#05060c;border:1px solid rgba(255,255,255,.14);border-radius:8px;padding:4px 6px;font-size:12px;color:#dbe8ff}
  .an-modal .ids-edit-actions{display:flex;gap:8px;justify-content:flex-end;margin-top:8px}
  .an-modal .an-footer{padding:8px 12px;border-top:1px solid rgba(255,255,255,.12);display:grid;grid-template-columns:auto 1fr;align-items:center;font-size:12px;background:#05060c;gap:12px}
  .an-modal .an-footer .count-stack{display:inline-flex;align-items:baseline;gap:8px;line-height:1.15;white-space:nowrap}
  .an-modal .an-footer .stats{justify-self:end;text-align:right;white-space:nowrap}
  .an-modal .an-footer .stats.empty{opacity:.45}
  .an-modal input[type=search]{background:#05060c;border:1px solid rgba(255,255,255,.12);color:#dbe8ff;border-radius:12px;padding:6px 10px}
  .an-modal #an-search{flex:1 1 420px;min-width:220px;max-width:460px;width:auto}
  .an-modal .an-grid,.an-modal .an-issues{scrollbar-width:thin;scrollbar-color:#7a6bff #05060c}
  .an-modal .an-grid::-webkit-scrollbar,.an-modal .an-issues::-webkit-scrollbar{height:12px;width:12px}
  .an-modal .an-grid::-webkit-scrollbar-track,.an-modal .an-issues::-webkit-scrollbar-track{background:#05060c}
  .an-modal .an-grid::-webkit-scrollbar-thumb,.an-modal .an-issues::-webkit-scrollbar-thumb{background:linear-gradient(180deg,#7a6bff,#23d5ff);border-radius:10px;border:2px solid #05060c;box-shadow:0 0 12px rgba(122,107,255,.55) inset}
  .unsync-dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px;background:radial-gradient(circle,#ffb0d0,#ff3b7f);box-shadow:0 0 8px rgba(255,59,127,.8);vertical-align:middle}
  .blocked-ico{display:inline-block;margin-right:6px;vertical-align:middle;font-size:13px;line-height:1;filter:drop-shadow(0 0 10px rgba(255,90,120,.7))}
  .wait-overlay{position:fixed;inset:0;display:flex;align-items:center;justify-content:center;background:rgba(3,4,10,.8);backdrop-filter:blur(6px);z-index:9999;opacity:1;transition:opacity .18s ease}
  .wait-overlay.hidden{opacity:0;pointer-events:none}
  .wait-card{display:flex;flex-direction:column;align-items:center;gap:14px;padding:22px 28px;border-radius:18px;background:linear-gradient(180deg,#05060c,#101124);box-shadow:0 0 40px rgba(122,107,255,.45),inset 0 0 1px rgba(255,255,255,.08)}
  .wait-ring{width:64px;height:64px;border-radius:50%;position:relative;filter:drop-shadow(0 0 12px rgba(122,107,255,.55))}
  .wait-ring::before{content:"";position:absolute;inset:0;border-radius:50%;padding:4px;background:conic-gradient(#7a6bff,#23d5ff,#7a6bff);-webkit-mask:linear-gradient(#000 0 0) content-box,linear-gradient(#000 0 0);-webkit-mask-composite:xor;mask-composite:exclude;animation:wait-spin 1.1s linear infinite}
  .wait-text{font-weight:800;color:#dbe8ff;text-shadow:0 0 12px rgba(122,107,255,.6)}
  @keyframes wait-spin{to{transform:rotate(360deg)}}
  `;
  document.head.appendChild(el);
}

function gridTemplateFrom(widths) {
  return widths.map(w => `${w}px`).join(" ");
}

export default {
  async mount(root) {
    css();
    root.classList.add("modal-root","an-modal");
    root.innerHTML = `
      <div class="cx-head">
        <div class="cx-left">
          <div class="cx-title">Analyzer</div>
          <button class="pill ghost" id="an-toggle-ids">IDs: hidden</button>
          <button class="pill ghost" id="an-scope">Scope: issues</button>
          <input id="an-search" type="search" placeholder="title, year, provider, feature…">
        </div>
        <div class="an-actions">
          <button class="pill" id="an-run" type="button">Analyze</button>
          <button class="close-btn" id="an-close">Close</button>
        </div>
      </div>
      <div class="an-pairs" id="an-pairs"></div>
      <div class="an-wrap" id="an-wrap">
        <div class="an-grid" id="an-grid"></div>
        <div class="an-split" id="an-split" title="drag to resize"></div>
        <div class="an-issues" id="an-issues"></div>
      </div>
      <div class="an-footer">
        <div class="count-stack">
          <span class="mono" id="an-issues-count">Issues: 0</span>
          <span class="mono" id="an-blocked-count">Blocked: 0</span>
        </div>
        <div class="stats empty" id="an-stats"></div>
      </div>
    `;

    const wait = document.createElement("div");
    wait.id = "an-wait";
    wait.className = "wait-overlay hidden";
    wait.innerHTML = `
      <div class="wait-card" role="status" aria-live="assertive">
        <div class="wait-ring"></div>
        <div class="wait-text" id="an-wait-text">Loading…</div>
      </div>`;
    root.appendChild(wait);

    let waitSlowTimer = null;
    let waitShownAt = 0;
    const setWaitText = t => {
      const el = Q("#an-wait-text", root);
      if (el) el.textContent = t;
    };
    function showWait(text) {
      waitShownAt = performance.now();
      const el = Q("#an-wait", root);
      if (el) el.classList.remove("hidden");
      setWaitText(text || "Working…");
      clearTimeout(waitSlowTimer);
      waitSlowTimer = setTimeout(
        () => setWaitText(`${text} (still working…)`),
        3000
      );
    }
    function hideWait() {
      clearTimeout(waitSlowTimer);
      waitSlowTimer = null;
      const minVisible = 250;
      const elapsed = performance.now() - waitShownAt;
      const doHide = () => Q("#an-wait", root).classList.add("hidden");
      if (elapsed < minVisible) setTimeout(doHide, minVisible - elapsed);
      else doHide();
    }

    const wrap = Q("#an-wrap", root);
    const grid = Q("#an-grid", root);
    const issues = Q("#an-issues", root);
    const pairBar = Q("#an-pairs", root);
    const stats = Q("#an-stats", root);
    const issuesCount = Q("#an-issues-count", root);
    const blockedCount = Q("#an-blocked-count", root);
    const search = Q("#an-search", root);
    const btnRun = Q("#an-run", root);
    const btnToggleIDs = Q("#an-toggle-ids", root);
    const btnClose = Q("#an-close", root);
    const btnScope = Q("#an-scope", root);
    const split = Q("#an-split", root);

    let COLS = JSON.parse(localStorage.getItem("an.cols") || "null");
    if (!Array.isArray(COLS) || COLS.length !== 4) COLS = [110, 110, 360, 90];
    let ITEMS = [];
    let VIEW = [];
    let SORT_KEY = "title";
    let SORT_DIR = "asc";
    let SHOW_IDS = false;
    let SELECTED = null;
    let PAIRS = [];
    let PAIR_FILTER = new Set();
    let PAIR_STATS = [];
    let PAIR_EXCLUSIONS = [];
    let PAIR_SCOPE_KEYS = new Set();
    let UNSYNCED = new Set();
    let UNSYNCED_META = new Map();
    let UNSYNCED_REASON = new Map();
    let SCOPE = "issues";
    let NORMALIZATION = [];
    let LIMIT_INFO = {};
    let LIMIT_AFFECTED = new Map();
    let BLOCKS_BY_PF = new Map();

    function applySplit(top, total) {
      const bar = 8;
      const min = 140;
      const clamped = Math.max(
        min,
        Math.min(total - min - bar, top)
      );
      wrap.style.gridTemplateRows = `${clamped}px 8px 1fr`;
      localStorage.setItem("an.split.r", (clamped / total).toFixed(4));
    }
    function restoreSplit() {
      const r = parseFloat(localStorage.getItem("an.split.r") || "0.5") || 0.5;
      const rect = wrap.getBoundingClientRect();
      const tot = rect.height || 420;
      applySplit(Math.round(r * tot), tot);
    }
    function dragY() {
      const rect = wrap.getBoundingClientRect();
      const tot = rect.height || 420;
      let startY = 0;
      let startTop = 0;
      const mv = e => {
        const clientY = e.touches ? e.touches[0].clientY : e.clientY;
        const y = clientY - rect.top;
        applySplit(startTop + y - startY, tot);
        e.preventDefault();
      };
      const up = () => {
        document.removeEventListener("mousemove", mv);
        document.removeEventListener("mouseup", up);
        document.removeEventListener("touchmove", mv);
        document.removeEventListener("touchend", up);
      };
      const dn = e => {
        const clientY = e.touches ? e.touches[0].clientY : e.clientY;
        startY = clientY - rect.top;
        const firstRow = (wrap.style.gridTemplateRows || "").split(" ")[0];
        startTop = parseFloat(firstRow) || rect.height * 0.6;
        document.addEventListener("mousemove", mv);
        document.addEventListener("mouseup", up);
        document.addEventListener("touchmove", mv, { passive: false });
        document.addEventListener("touchend", up);
        e.preventDefault();
      };
      split.addEventListener("mousedown", dn);
      split.addEventListener(
        "touchstart",
        e => {
          dn(e);
          e.preventDefault();
        },
        { passive: false }
      );
    }

    function setCols() {
      grid.style.setProperty("--col-template", gridTemplateFrom(COLS));
      grid
        .querySelectorAll(".row")
        .forEach(r => (r.style.gridTemplateColumns = gridTemplateFrom(COLS)));
    }

    function sortRows(rows) {
      const k = SORT_KEY;
      const dir = SORT_DIR === "asc" ? 1 : -1;
      const val = r => {
        if (k === "title") return displayTitle(r).toLowerCase();
        if (k === "provider") return String(r.provider || "").toUpperCase();
        if (k === "feature") return String(r.feature || "").toUpperCase();
        if (k === "type") return String(r.type || "").toUpperCase();
        return displayTitle(r).toLowerCase();
      };
      return rows.sort((a, b) => {
        const va = val(a);
        const vb = val(b);
        if (va < vb) return -1 * dir;
        if (va > vb) return 1 * dir;
        return 0;
      });
    }

    function renderHeader() {
      const dirMark = k =>
        SORT_KEY === k ? (SORT_DIR === "asc" ? "▲" : "▼") : "";
      return `
        <div class="row head" style="grid-template-columns:${gridTemplateFrom(
          COLS
        )}">
          <div class="cell sort" data-k="provider"><span class="label">Provider</span><span class="dir">${dirMark(
            "provider"
          )}</span></div>
          <div class="cell sort" data-k="feature"><span class="label">Feature</span><span class="dir">${dirMark(
            "feature"
          )}</span></div>
          <div class="cell sort" data-k="title"><span class="label">Title</span><span class="dir">${dirMark(
            "title"
          )}</span></div>
          <div class="cell sort" data-k="type"><span class="label">Type</span><span class="dir">${dirMark(
            "type"
          )}</span></div>
        </div>`;
    }


    function _pfKey(provider, feature) {
      return `${String(provider || "").toUpperCase()}::${String(feature || "").toLowerCase()}`;
    }
    function _normKey(v) {
      return String(v || "").trim().toLowerCase();
    }
    function isBlocked(provider, feature, key) {
      const set = BLOCKS_BY_PF.get(_pfKey(provider, feature));
      if (!set) return false;
      return set.has(_normKey(key));
    }
    async function refreshBlocked() {
      const pairs = new Map();
      for (const r of ITEMS || []) {
        const k = _pfKey(r.provider, r.feature);
        if (!pairs.has(k)) pairs.set(k, { provider: r.provider, feature: r.feature });
      }
      if (!pairs.size) {
        BLOCKS_BY_PF = new Map();
        return;
      }
      const next = new Map();
      await Promise.all(
        Array.from(pairs.values()).map(async ({ provider, feature }) => {
          try {
            const u = `/api/editor?source=state&kind=${encodeURIComponent(
              String(feature || "")
            )}&provider=${encodeURIComponent(String(provider || ""))}`;
            const res = await fjson(u, { cache: "no-store" });
            const blocks = Array.isArray(res && res.manual_blocks)
              ? res.manual_blocks
              : [];
            const set = new Set(blocks.map(_normKey).filter(Boolean));
            next.set(_pfKey(provider, feature), set);
          } catch {
            next.set(_pfKey(provider, feature), new Set());
          }
        })
      );
      BLOCKS_BY_PF = next;
    }

    function renderBody(rows) {
      return rows
        .map(r => {
          const tag = tagOf(r.provider, r.feature, r.key);
          const blk = isBlocked(r.provider, r.feature, r.key);
          const uns = UNSYNCED.has(tag);
          const label = displayTitle(r);
          return `<div class="row${SELECTED === tag ? " sel" : ""}" data-tag="${tag}">
            <div class="prov">${r.provider}</div>
            <div class="feat">${r.feature}</div>
            <div>
              <div class="title">${
                blk
                  ? `<span class="blocked-ico" title="Blocked (manual)">⛔</span>`
                  : ""
              }${
                uns
                  ? (() => {
                      const miss = UNSYNCED_META.get(tag) || [];
                      const text = miss.length
                        ? `Missing at ${miss.join(" & ")}`
                        : "Missing at other provider";
                      const rs = UNSYNCED_REASON.get(tag) || [];
                      const reason = rs.length ? rs[0] : "";
                      const tip = reason ? `${text} — ${reason}` : text;
                      return `<span class="unsync-dot" title="${escHtml(tip)}"></span>`;
                    })()
                  : ""
              }${label}</div>
              <div class="ids mono">${chips(r.ids)}</div>
            </div>
            <div>${r.type || ""}</div>
          </div>`;
        })
        .join("");
    }

    function bindHeader() {
      QA(".head .sort", grid).forEach(h =>
        h.addEventListener("click", () => {
          const k = h.dataset.k;
          SORT_DIR =
            SORT_KEY === k && SORT_DIR === "asc" ? "desc" : "asc";
          SORT_KEY = k;
          draw();
        })
      );
    }

    function inPairScope(r) {
      if (!PAIR_SCOPE_KEYS || !PAIR_SCOPE_KEYS.size) return true;
      const key = `${String(r.provider || "").toUpperCase()}::${String(
        r.feature || ""
      ).toLowerCase()}`;
      return PAIR_SCOPE_KEYS.has(key);
    }

    function baseItems() {
      const scoped = ITEMS.filter(inPairScope);
      if (SCOPE === "issues") {
        if (!UNSYNCED || !UNSYNCED.size) return [];
        return scoped.filter(r =>
          UNSYNCED.has(tagOf(r.provider, r.feature, r.key))
        );
      }
      return scoped;
    }

    function draw() {
      grid.innerHTML = renderHeader() + renderBody(sortRows(VIEW.slice()));
      bindHeader();
      setCols();
      QA(".row:not(.head)", grid).forEach(r =>
        r.addEventListener("click", () =>
          select(r.getAttribute("data-tag"))
        )
      );
    }

    function filter(q) {
      q = (q || "").toLowerCase().trim();
      const base = baseItems();
      if (!q) {
        VIEW = base.slice();
        draw();
        return;
      }
      const W = q.split(/\s+/g);
      VIEW = base.filter(r => {
        const label = displayTitle(r);
        const hay = [r.provider, r.feature, r.title, r.year, r.type, label]
          .map(x => String(x || "").toLowerCase())
          .join(" ");
        return W.every(w => hay.includes(w));
      });
      draw();
    }

    function escHtml(s) {
      return String(s)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function renderHistoryNormalizationBlocks(list) {
      if (!Array.isArray(list) || !list.length) return "";
      return list
        .map(p => {
          const src = p && p.source ? String(p.source) : "?";
          const dst = p && p.target ? String(p.target) : "?";
          const delta = (p && p.show_delta) || {};
          const srcCount =
            typeof delta.source === "number" ? delta.source : 0;
          const dstCount =
            typeof delta.target === "number" ? delta.target : 0;

          const extraSource =
            (Array.isArray(p.extra_source_titles) &&
            p.extra_source_titles.length
              ? p.extra_source_titles
              : p.extra_source) || [];
          const extraTarget =
            (Array.isArray(p.extra_target_titles) &&
            p.extra_target_titles.length
              ? p.extra_target_titles
              : p.extra_target) || [];

          const renderList = (items, label) => {
            if (!items.length) {
              return `<div>
                <div class="h" style="font-size:12px">${escHtml(
                  label
                )}</div>
                <div class="mono" style="opacity:.7">—</div>
              </div>`;
            }
            const lis = items
              .slice(0, 50)
              .map(x => `<li>${escHtml(String(x))}</li>`)
              .join("");
            return `<div>
              <div class="h" style="font-size:12px">${escHtml(
                label
              )}</div>
              <ul class="mono">${lis}</ul>
            </div>`;
          };

          return `
            <div class="issue">
              <div class="h">History normalization: ${escHtml(
                src
              )} ↔ ${escHtml(dst)}</div>
              <div class="mono" style="margin-bottom:6px">
                ${escHtml(src)} has ${srcCount} shows, ${escHtml(
            dst
          )} has ${dstCount} shows.
              </div>
              <div style="font-size:12px;opacity:.8;margin-bottom:6px">
                These counts can differ because some shows are split or merged differently between providers.
              </div>
              <div class="ids-edit-row">
                ${renderList(extraSource, `Only in ${src}`)}
                ${renderList(extraTarget, `Only in ${dst}`)}
              </div>
            </div>`;
        })
        .join("");
    }

    function manualIdsBlock(it) {
      const ids = it.ids || {};
      const inputs = ID_FIELDS.map(name => {
        const val = ids[name] || "";
        return `<label><span>${name}</span><input type="text" data-idfield="${name}" value="${String(
          val
        )}"></label>`;
      }).join("");
      return `
        <div class="issue manual-ids">
          <div class="h">Manual IDs</div>
          <div class="ids-edit">
            <div class="ids-edit-row">
              ${inputs}
            </div>
            <div class="ids-edit-actions">
              <button type="button" class="pill" data-act="patch-ids">Save IDs</button>
              <button type="button" class="pill ghost" data-act="reset-ids">Reset</button>
            </div>
          </div>
        </div>`;
    }

    function renderNormalizationPanel(list) {
      if (!list || !list.length) return "";

      const renderList = arr => {
        if (!arr || !arr.length) return `<span class="mono">none</span>`;
        return `<ul>${arr.map(x => `<li>${x}</li>`).join("")}</ul>`;
      };

      return list
        .map(p => {
          const src = String(p.source || "").toUpperCase();
          const dst = String(p.target || "").toUpperCase();
          const delta = p.show_delta || {};
          const srcCount = delta.source ?? "?";
          const dstCount = delta.target ?? "?";

          const srcTitles = p.extra_source_titles || [];
          const dstTitles = p.extra_target_titles || [];
          const srcIds = p.extra_source || [];
          const dstIds = p.extra_target || [];

          const listSrc = srcTitles.length ? srcTitles : srcIds;
          const listDst = dstTitles.length ? dstTitles : dstIds;

          return `
          <div class="issue">
            <div class="h">History normalization: ${src} ↔ ${dst}</div>
            <div>${src} has ${srcCount} shows, ${dst} has ${dstCount} shows.</div>
            <div>These counts differ because some series are split or merged differently between providers.</div>
            <div style="margin-top:6px">
              <div><strong>Only in ${src}:</strong> ${renderList(listSrc)}</div>
              <div><strong>Only in ${dst}:</strong> ${renderList(listDst)}</div>
            </div>
          </div>`;
        })
        .join("");
    }

    function bindManualIds(provider, feature, key, it) {
      const box = Q(".issue.manual-ids", issues);
      if (!box) return;
      const inputs = QA("input[data-idfield]", box);
      const btnSave = Q("button[data-act='patch-ids']", box);
      const btnReset = Q("button[data-act='reset-ids']", box);
      const original = Object.assign({}, it.ids || {});

      if (btnReset) {
        btnReset.addEventListener("click", () => {
          inputs.forEach(inp => {
            const f = inp.getAttribute("data-idfield") || "";
            inp.value = original[f] || "";
          });
        });
      }

      if (!btnSave) return;
      btnSave.addEventListener("click", async () => {
        if (btnSave.disabled) return;
        const idsPayload = {};
        inputs.forEach(inp => {
          const f = inp.getAttribute("data-idfield") || "";
          const v = inp.value.trim();
          idsPayload[f] = v || null;
        });
        const prev = btnSave.textContent;
        btnSave.disabled = true;
        btnSave.textContent = "Saving…";
        try {
          const body = {
            provider,
            feature,
            key,
            ids: idsPayload,
            rekey: true,
            merge_peer_ids: false
          };
          const res = await fjson("/api/analyzer/patch", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body)
          });
          const newKey =
            res && res.new_key ? String(res.new_key) : key;
          const tagOld = tagOf(provider, feature, key);
          const idx = ITEMS.findIndex(
            r => tagOf(r.provider, r.feature, r.key) === tagOld
          );
          if (idx >= 0) {
            const cleanIds = {};
            Object.entries(idsPayload).forEach(([k, v]) => {
              if (v && String(v).trim())
                cleanIds[k] = String(v).trim();
            });
            ITEMS[idx].ids = cleanIds;
            ITEMS[idx].key = newKey;
          }
          await analyze(true);
          const newTag = tagOf(provider, feature, newKey);
          SELECTED = newTag;
          await select(newTag);
        } catch (err) {
          console.error(err);
          alert("Failed to save IDs. Check console for details.");
        } finally {
          btnSave.disabled = false;
          btnSave.textContent = prev;
        }
      });
    }


    function renderLimitPanel(tag) {
      const hit = LIMIT_AFFECTED.get(tag);
      if (!hit) return "";
      const key = hit.key;
      const info = (LIMIT_INFO && LIMIT_INFO[key]) || {};
      const prov = String(hit.provider || "").toUpperCase() || "PROVIDER";
      const what = String(hit.limit_feature || "").toLowerCase() || "watchlist";
      const cap = typeof info.cap === "number" ? info.cap : null;
      const used = typeof info.used === "number" ? info.used : null;
      const affected =
        typeof info.affected === "number" && info.affected > 0
          ? info.affected
          : null;
      const last =
        typeof info.last_error === "string" && info.last_error
          ? info.last_error
          : "";
      const title =
        prov === "TRAKT" && info.plan === "free"
          ? "TRAKT free account limit reached"
          : `${prov} limit reached`;
      const targetLabel = what === "collection" ? "collection" : "watchlist";
      const capLine =
        cap != null && used != null
          ? `${targetLabel} is at ${used}/${cap}.`
          : `${targetLabel} limit reached.`;
      const fix =
        prov === "TRAKT" && info.plan === "free"
          ? "Fix: remove items from Trakt, or upgrade to Trakt VIP."
          : "Fix: remove items, or upgrade the account.";
      const countLine = affected
        ? `<div class="mono" style="opacity:.78;margin-top:6px">Affected items: ${affected}</div>`
        : "";
      const lastLine = last
        ? `<div class="mono" style="opacity:.75;margin-top:6px">Last limit error: ${escHtml(last)}</div>`
        : "";
      return `<div class="issue">
        <div class="h">${escHtml(title)}</div>
        <div>${escHtml(capLine)} CrossWatch can't add more items there.</div>
        <div style="opacity:.85;margin-top:6px">${escHtml(fix)}</div>
        ${countLine}
        ${lastLine}
      </div>`;
    }

    async function select(tag) {
      SELECTED = tag;
      draw();

      const [provider, feature, key] = tag.split("::");
      const it = ITEMS.find(r => tagOf(r.provider, r.feature, r.key) === tag);
      if (!it) {
        issues.innerHTML =
          "<div class='issue'><div class='h'>No selection</div></div>";
        return;
      }

      const label = displayTitle(it);
      const heading = it.year ? `${label} (${it.year})` : label;
      const unsynced = UNSYNCED.has(tag);
      const blocked = isBlocked(provider, feature, key);
      const missingTargets = UNSYNCED_META.get(tag) || [];
      const missingLabel = missingTargets.length
        ? `Missing at ${missingTargets.join(" & ")}`
        : "Missing at other provider";

      const reasons = UNSYNCED_REASON.get(tag) || [];
      const reasonBadge =
        unsynced && reasons.length
          ? ` <span class="badge mono">${escHtml(reasons[0])}</span>`
          : "";

      const blockedBadge = blocked
        ? ` <span class="badge mono">Blocked</span>`
        : "";

      const status = unsynced
        ? `<span class="badge">${missingLabel}</span>${reasonBadge}${blockedBadge}`
        : `<span class="badge">No analyzer issues</span>${blockedBadge}`;

      const header = `<div class="issue">
        <div class="h">${heading}</div>
        <div>${status}</div>
      </div>`;

      const manual = manualIdsBlock(it);
      const normalizationBlock = renderNormalizationPanel(NORMALIZATION);
      const limitBlock = renderLimitPanel(tag);
      issues.innerHTML = limitBlock + header + normalizationBlock + manual;


      issues.scrollTop = 0;

      bindManualIds(provider, feature, key, it);
    }

    

function _isTwoWayMode(mode) {
  const m = String(mode || "one-way").toLowerCase();
  return m === "two-way" || m === "bi" || m === "both" || m === "mirror";
}

function renderScopeExclusions() {
  const dirs = new Set();
  const list = (PAIRS || []).filter(
    p =>
      p &&
      p.enabled &&
      (!PAIR_FILTER.size || PAIR_FILTER.has(String(p.id)))
  );
  for (const p of list) {
    const src = String(p.source || "").toUpperCase();
    const dst = String(p.target || "").toUpperCase();
    dirs.add(`${src}::${dst}`);
    if (_isTwoWayMode(p.mode)) dirs.add(`${dst}::${src}`);
  }

  const scoped = (PAIR_EXCLUSIONS || []).filter(e =>
    dirs.has(
      `${String(e.source || "").toUpperCase()}::${String(
        e.target || ""
      ).toUpperCase()}`
    )
  );
  if (!scoped.length) return "";

  const typeLabel = t => {
    const x = String(t || "").toLowerCase();
    if (x === "episode") return "episodes";
    if (x === "season") return "seasons";
    if (x === "movie") return "movies";
    if (x === "show") return "shows";
    return x ? `${x}s` : "items";
  };

  const typeOrder = { season: 0, episode: 1, show: 2, movie: 3, anime: 4 };

  const lines = scoped
    .map(e => {
      const src = String(e.source || "").toUpperCase();
      const dst = String(e.target || "").toUpperCase();
      const feat = String(e.feature || "").toLowerCase();
      const types = e.excluded_types || {};
      const entries = Object.entries(types)
        .filter(([, c]) => typeof c === "number" && c > 0)
        .sort((a, b) => {
          const oa = typeOrder[String(a[0] || "").toLowerCase()] ?? 99;
          const ob = typeOrder[String(b[0] || "").toLowerCase()] ?? 99;
          if (oa !== ob) return oa - ob;
          return (b[1] || 0) - (a[1] || 0);
        });

      if (!entries.length) return "";

      const countStr = entries
        .map(([t, c]) => `${c} ${typeLabel(t)}`)
        .join(", ");

      const allowed = Array.isArray(e.allowed_types) && e.allowed_types.length
        ? ` (allowed: ${e.allowed_types.join(", ")})`
        : "";

      return `<div class="mono" style="opacity:.78;margin-top:6px">${escHtml(
        `${src} → ${dst} • ${feat}: ${countStr}${allowed}`
      )}</div>`;
    })
    .filter(Boolean)
    .join("");

  if (!lines) return "";

  return `<div class="issue">
    <div class="h">Out of scope (pair setup)</div>
    <div style="opacity:.85">These won't sync because the selected pair config excludes them.</div>
    ${lines}
  </div>`;
}

function renderPairs() {
      if (!pairBar) return;
      const list = (PAIRS || []).filter(p => p && p.enabled);
      if (!PAIR_FILTER.size && list.length) {
        try {
          const raw = localStorage.getItem("an.pairs");
          if (raw) {
            const ids = JSON.parse(raw);
            if (Array.isArray(ids))
              ids.forEach(id => PAIR_FILTER.add(String(id)));
          }
        } catch {}
        if (!PAIR_FILTER.size) {
          for (const p of list) PAIR_FILTER.add(String(p.id));
        }
      }
      if (!list.length) {
        pairBar.innerHTML = "";
        return;
      }
      const statsByKey = {};
      for (const st of PAIR_STATS || []) {
        const key = `${String(st.source || "").toUpperCase()}::${String(
          st.target || ""
        ).toUpperCase()}`;
        if (!statsByKey[key])
          statsByKey[key] = { total: 0, synced: 0, unsynced: 0 };
        statsByKey[key].total += st.total || 0;
        statsByKey[key].synced += st.synced || 0;
        statsByKey[key].unsynced += st.unsynced || 0;
      }
      const html = list
        .map(p => {
          const src = String(p.source || "").toUpperCase();
          const dst = String(p.target || "").toUpperCase();
          const keyAB = `${src}::${dst}`;
          const keyBA = `${dst}::${src}`;
          const stAB = statsByKey[keyAB] || {
            total: 0,
            synced: 0,
            unsynced: 0
          };
          const stBA = statsByKey[keyBA] || {
            total: 0,
            synced: 0,
            unsynced: 0
          };
          const total = stAB.total + stBA.total;
          const unsynced = stAB.unsynced + stBA.unsynced;
          const on =
            !PAIR_FILTER.size || PAIR_FILTER.has(String(p.id));
          const mode = String(p.mode || "one-way").toLowerCase();
          const dir =
            mode === "two-way" ||
            mode === "bi" ||
            mode === "both" ||
            mode === "mirror"
              ? "⇄"
              : "→";
          const badge = total
            ? `<span class="mono">${unsynced || 0}/${total}</span>`
            : "";
          const cls = `an-pair-chip${on ? " on" : ""}`;
          return `<button type="button" class="${cls}" data-id="${esc(
            String(p.id || "")
          )}"><span class="mono">${src}</span><span class="dir">${dir}</span><span class="mono">${dst}</span>${badge}</button>`;
        })
        .join("");
      pairBar.innerHTML = html;

      const allIds = list.map(p => String(p.id));
      QA(".an-pair-chip", pairBar).forEach(btn => {
        btn.addEventListener("click", () => {
          const id = btn.getAttribute("data-id") || "";
          if (!id) return;
          const allSelected =
            allIds.length > 0 &&
            allIds.every(x => PAIR_FILTER.has(x)) &&
            PAIR_FILTER.size === allIds.length;
          if (allSelected) {
            PAIR_FILTER = new Set([id]);
          } else if (PAIR_FILTER.size === 1 && PAIR_FILTER.has(id)) {
            PAIR_FILTER = new Set(allIds);
          } else {
            if (PAIR_FILTER.has(id)) PAIR_FILTER.delete(id);
            else PAIR_FILTER.add(id);
            if (!PAIR_FILTER.size) {
              PAIR_FILTER = new Set(allIds);
            }
          }
          try {
            localStorage.setItem(
              "an.pairs",
              JSON.stringify(Array.from(PAIR_FILTER))
            );
          } catch {}
          renderPairs();
          analyze(true);
        });
      });
    }

    async function getActivePairMap() {
      try {
        const arr = await fjson("/api/pairs", { cache: "no-store" });
        const map = new Map();
        const on = feat =>
          feat && (typeof feat.enable === "boolean" ? feat.enable : !!feat);
        const add = (src, feat, dst) => {
          const k = `${String(src || "").toUpperCase()}::${feat}`;
          if (!map.has(k)) map.set(k, new Set());
          map.get(k).add(String(dst || "").toUpperCase());
        };
        PAIRS = (arr || [])
          .filter(p => p && p.source && p.target)
          .map(p => {
            const src = String(p.source || "").toUpperCase();
            const dst = String(p.target || "").toUpperCase();
            const id = String(p.id || `${src}->${dst}`);
            return Object.assign({}, p, { source: src, target: dst, id });
          });
        renderPairs();
        for (const p of PAIRS) {
          if (!p.enabled) continue;
          if (PAIR_FILTER.size && !PAIR_FILTER.has(String(p.id)))
            continue;
          const src = p.source;
          const dst = p.target;
          const mode = String(p.mode || "one-way").toLowerCase();
          const F = p.features || {};
          for (const feat of ["history", "watchlist", "ratings"]) {
            if (!on(F[feat])) continue;
            add(src, feat, dst);
            if (
              mode === "two-way" ||
              mode === "bi" ||
              mode === "both" ||
              mode === "mirror"
            )
              add(dst, feat, src);
          }
        }
        return map;
      } catch {
        return new Map();
      }
    }

    async function load() {
      restoreSplit();
      dragY();
      showWait("Reading state.json…");
      let s;
      try {
        s = await fjson("/api/analyzer/state");
      } catch {
        s = { counts: {}, items: [] };
        issues.innerHTML = `
          <div class="issue">
            <div class="h">No state.json yet</div>
            <div>Run a sync to generate a baseline, then reopen Analyzer.</div>
          </div>`;
      }
      ITEMS = s.items || [];
      VIEW = ITEMS.slice();
      const countsText = fmtCounts(s.counts);
      stats.textContent = countsText;
      if (!countsText) stats.classList.add("empty");
      else stats.classList.remove("empty");
      issuesCount.textContent = "Issues: 0";
      if (blockedCount) blockedCount.textContent = "Blocked: 0";
      draw();
      setWaitText("Analyzing…");
      try {
        await analyze(true);
      } finally {
        hideWait();
      }
    }

    async function analyze(silent = false) {
      if (!silent) showWait("Analyzing…");
      const [pairMap, meta, status] = await Promise.all([
        getActivePairMap(),
        fjson("/api/analyzer/problems").catch(() => ({ problems: [] })),
        fjson("/api/status").catch(() => null),
        refreshBlocked().catch(() => null)
      ]);

      PAIR_STATS = meta.pair_stats || [];
      PAIR_EXCLUSIONS = meta.pair_exclusions || [];
      PAIR_SCOPE_KEYS = new Set(pairMap ? Array.from(pairMap.keys()) : []);
      renderPairs();

      const all = meta.problems || [];
      const normalization = all.filter(
        p => p && p.type === "history_show_normalization"
      );
      NORMALIZATION = normalization;

      LIMIT_INFO = {};
      LIMIT_AFFECTED = new Map();
      try {
        const provs = (status && status.providers) || {};
        const pickProvider = want => {
          const w = String(want || "").toUpperCase();
          for (const [k, v] of Object.entries(provs)) {
            if (String(k).toUpperCase() === w) return v || null;
          }
          return null;
        };
        const trakt = pickProvider("TRAKT");
        if (trakt && trakt.connected) {
          const vip = trakt.vip;
          const plan = vip === false ? "free" : "vip";
          const limits = trakt.limits || {};
          const last = trakt.last_limit_error || {};
          const pushLimit = (name, lf) => {
            const node = limits[name] || {};
            const cap = Number(node.item_count || 0) || 0;
            const used = Number(node.used || 0) || 0;
            if (!cap) return;
            LIMIT_INFO[`TRAKT::${lf}`] = {
              provider: "TRAKT",
              limit_feature: lf,
              plan,
              cap,
              used,
              reached: used >= cap,
              last_error:
                String(last.feature || "").toLowerCase() === String(lf || "").toLowerCase()
                  ? String(last.ts || "")
                  : ""
            };
          };
          pushLimit("watchlist", "watchlist");
          pushLimit("collection", "collection");
        }
      } catch {}

      const hasPairFilter = pairMap && pairMap.size > 0;
      const seen = new Set();
      const per = { history: 0, watchlist: 0, ratings: 0 };
      const keep = [];

      for (const p of all) {
        if (p.type !== "missing_peer") continue;

        if (hasPairFilter) {
          const key = `${String(p.provider || "").toUpperCase()}::${String(
            p.feature || ""
          ).toLowerCase()}`;
          const allowed = pairMap.get(key);
          if (!allowed) continue;
          const tgts = (p.targets || []).map(t =>
            String(t || "").toUpperCase()
          );
          if (!tgts.some(t => allowed.has(t))) continue;
        }

        if (isBlocked(p.provider, p.feature, p.key)) continue;

        const sig = `${p.provider}::${p.feature}::${p.key}`;
        if (seen.has(sig)) continue;
        seen.add(sig);
        per[p.feature] = (per[p.feature] || 0) + 1;
        keep.push(p);
      }

      UNSYNCED = new Set(
        keep.map(p => tagOf(p.provider, p.feature, p.key))
      );

      UNSYNCED_META = new Map(
        keep.map(p => [
          tagOf(p.provider, p.feature, p.key),
          (p.targets || []).map(t => String(t || "").toUpperCase())
        ])
      );

      UNSYNCED_REASON = new Map();
      const limitKeyFor = (provUpper, featLower) => {
        const p = String(provUpper || "").toUpperCase();
        const f = String(featLower || "").toLowerCase();
        if (p !== "TRAKT") return null;
        if (f.includes("watchlist")) return "TRAKT::watchlist";
        if (f.includes("collect")) return "TRAKT::collection";
        return null;
      };
      for (const p of keep) {
        const tag = tagOf(p.provider, p.feature, p.key);
        const details = Array.isArray(p.target_show_info)
          ? p.target_show_info
          : [];
        const msgs = details
          .map(d => String((d && d.message) || "").trim())
          .filter(Boolean);
        const reasons = msgs.slice();
        try {
          const featLower = String(p.feature || "").toLowerCase();
          const targets = (p.targets || []).map(t => String(t || "").toUpperCase());
          for (const t of targets) {
            const lk = limitKeyFor(t, featLower);
            if (!lk) continue;
            const info = LIMIT_INFO && LIMIT_INFO[lk];
            if (info && info.reached) {
              const cap = typeof info.cap === "number" ? info.cap : null;
              const used = typeof info.used === "number" ? info.used : null;
              const short =
                cap != null && used != null
                  ? `${t} limit reached (${used}/${cap})`
                  : `${t} limit reached`;
              reasons.unshift(short);
              LIMIT_AFFECTED.set(tag, {
                key: lk,
                provider: t,
                limit_feature: info.limit_feature || "watchlist"
              });
              break;
            }
          }
        } catch {}
        if (reasons.length) {
          UNSYNCED_REASON.set(tag, reasons);
        }
      }

      try {
        const by = {};
        for (const v of LIMIT_AFFECTED.values()) {
          const k = v && v.key ? String(v.key) : "";
          if (!k) continue;
          by[k] = (by[k] || 0) + 1;
        }
        for (const [k, c] of Object.entries(by)) {
          const info = LIMIT_INFO && LIMIT_INFO[k];
          if (info) info.affected = c;
        }
      } catch {}

      const parts = [`Issues: ${keep.length}`];
      if (per.history) parts.push(`H:${per.history}`);
      if (per.watchlist) parts.push(`W:${per.watchlist}`);
      if (per.ratings) parts.push(`R:${per.ratings}`);
      issuesCount.textContent = parts.join(" • ");
      if (blockedCount) {
        const scoped = ITEMS.filter(inPairScope);
        const n = scoped.reduce(
          (acc, r) => acc + (isBlocked(r.provider, r.feature, r.key) ? 1 : 0),
          0
        );
        blockedCount.textContent = `Blocked: ${n}`;
      }

      filter(search.value || "");

      if (!keep.length) {
        const notes = renderScopeExclusions();
        if (NORMALIZATION && NORMALIZATION.length) {
          issues.innerHTML = renderNormalizationPanel(NORMALIZATION) + notes;
        } else {
          const ok = `<div class="issue"><div class="h">No issues detected</div><div>All good.</div></div>`;
          issues.innerHTML = notes + ok;
        }
        if (!silent) hideWait();
        return;
      }

      const first = keep[0];
      const tag = tagOf(first.provider, first.feature, first.key);
      await select(tag);
      SELECTED = tag;
      if (!silent) hideWait();
    }

    btnRun.addEventListener("click", async e => {
      e.preventDefault();
      e.stopPropagation();
      if (btnRun.disabled) return;
      const prev = btnRun.textContent;
      btnRun.disabled = true;
      btnRun.textContent = "Analyzing…";
      try {
        await analyze(false);
      } finally {
        btnRun.disabled = false;
        btnRun.textContent = prev;
      }
    });

    btnToggleIDs.onclick = () => {
      SHOW_IDS = !SHOW_IDS;
      btnToggleIDs.textContent = `IDs: ${
        SHOW_IDS ? "shown" : "hidden"
      }`;
      grid.classList.toggle("show-ids", SHOW_IDS);
    };
    btnScope.onclick = () => {
      SCOPE = SCOPE === "issues" ? "all" : "issues";
      btnScope.textContent = `Scope: ${SCOPE}`;
      filter(search.value || "");
    };
    search.addEventListener("input", e => filter(e.target.value));
    btnClose.addEventListener("click", () => {
      if (window.cxCloseModal) window.cxCloseModal();
    });

    await load();
  },
  unmount() {}
};
