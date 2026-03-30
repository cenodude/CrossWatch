/* assets/js/modals/analyzer/index.js */
/* Modal for analyzing sync issues and provider feature coverage. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

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

const fmtCountNumber = total =>
  total > 999 ? `${(total / 1000).toFixed(1).replace(/\.0$/, "")}k` : total;

const renderCounts = c => {
  const entries = Object.entries(c || {});
  if (!entries.length) return "";
  const meta = window.CW?.ProviderMeta || {};
  return entries
    .map(([p, v]) => {
      const key = meta.keyOf?.(p) || String(p || "").toUpperCase();
      const total =
        v.total || (v.history || 0) + (v.watchlist || 0) + (v.ratings || 0);
      const label = meta.label?.(key) || key;
      const logo = meta.logLogoPath?.(key) || meta.logoPath?.(key) || "";
      const count = fmtCountNumber(total);
      return `<span class="prov-stat" title="${label} ${count}">
        <span class="prov-stat-brand">${logo ? `<img src="${logo}" alt="${label} logo" loading="lazy">` : `<span class="prov-stat-text">${label}</span>`}</span>
        <span class="prov-stat-count">${count}</span>
      </span>`;
    })
    .join("");
};

const ID_FIELDS = [
  "imdb",
  "tmdb",
  "tvdb",
  "mal",
  "anilist",
  "trakt",
  "plex",
  "simkl",
  "emby",
  "mdblist"
];

function buildPairScopeKeys(pairMap) {
  const out = new Set();
  if (!pairMap || !pairMap.size) return out;
  for (const [k, targets] of pairMap.entries()) {
    const key = String(k || "");
    if (!key) continue;
    out.add(key);

    const parts = key.split("::");
    const feat = String(parts[1] || "").toLowerCase();
    if (!feat) continue;

    if (targets && typeof targets.forEach === "function") {
      targets.forEach(t => {
        const prov = String(t || "").toUpperCase();
        if (prov) out.add(`${prov}::${feat}`);
      });
    }
  }
  return out;
}

function providerInstanceLabel(provider, instance) {
  const prov = String(provider || "").toUpperCase();
  const inst = String(instance || "").trim();
  if (!prov) return "";
  if (!inst || inst.toLowerCase() === "default") return prov;
  return `${prov}@${inst}`;
}

function css() {
  if (Q("#an-css")) return;
  const el = document.createElement("style");
  el.id = "an-css";
  el.textContent = `
  .cx-modal-shell.analyzer-modal-shell{width:min(var(--cxModalMaxW,960px),calc(100vw - 64px))!important;max-width:min(var(--cxModalMaxW,960px),calc(100vw - 64px))!important;height:min(var(--cxModalMaxH,86vh),calc(100vh - 56px))!important;background:linear-gradient(180deg,rgba(7,10,18,.96),rgba(5,8,15,.94))!important;border:1px solid rgba(103,128,255,.16)!important;box-shadow:0 34px 90px rgba(0,0,0,.58),0 0 0 1px rgba(255,255,255,.03) inset!important}
  .an-modal{position:relative;display:flex;flex-direction:column;height:100%;background:radial-gradient(120% 120% at 0% 0%,rgba(102,88,255,.08),transparent 34%),radial-gradient(110% 140% at 100% 100%,rgba(0,208,255,.06),transparent 30%),linear-gradient(180deg,rgba(6,9,16,.985),rgba(4,6,12,.985));color:#eaf1ff}
  .an-modal::before{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(90deg,rgba(255,255,255,.025),transparent 28%,transparent 72%,rgba(255,255,255,.02));opacity:.55}
  .an-modal .cx-head{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:12px;padding:12px 14px 10px;border-bottom:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.01));backdrop-filter:blur(10px)}
  .an-modal .cx-left{display:flex;align-items:center;gap:12px;flex-wrap:wrap;min-width:0}
  .an-modal .cx-mark{width:36px;height:36px;border-radius:12px;display:grid;place-items:center;background:linear-gradient(135deg,rgba(94,226,172,.18),rgba(56,189,248,.12));border:1px solid rgba(79,209,156,.22);box-shadow:inset 0 0 0 1px rgba(255,255,255,.03);flex:0 0 auto}
  .an-modal .cx-mark .material-symbols-rounded{font-variation-settings:"FILL" 0,"wght" 500,"GRAD" 0,"opsz" 24;font-size:18px;line-height:1;color:#f3f6ff}
  .an-modal .cx-title{display:inline-flex;align-items:center;gap:10px;font-weight:900;font-size:18px;letter-spacing:.08em;text-transform:uppercase;color:#f3f6ff;text-shadow:0 0 18px rgba(104,122,255,.16)}
  .an-modal .an-actions{display:flex;gap:8px;align-items:center;justify-content:flex-end;flex-wrap:wrap}
  .an-modal .an-intro{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:12px;align-items:center;padding:10px 14px 12px;border-bottom:1px solid rgba(255,255,255,.06);background:linear-gradient(180deg,rgba(255,255,255,.02),rgba(255,255,255,.006))}
  .an-modal .an-intro-copy{min-width:0}
  .an-modal .an-intro-title{font-size:14px;font-weight:800;letter-spacing:.01em;color:#f4f7ff}
  .an-modal .an-intro-sub{margin-top:4px;font-size:12px;line-height:1.45;color:rgba(205,215,235,.74)}
  .an-modal .an-intro-meta{display:flex;align-items:center;justify-content:flex-end;gap:8px;flex-wrap:wrap}
  .an-modal .an-intro-meta .mini{display:inline-flex;align-items:center;min-height:28px;padding:0 10px;border-radius:999px;border:1px solid rgba(255,255,255,.09);background:rgba(255,255,255,.035);font-size:11px;font-weight:800;letter-spacing:.04em;text-transform:uppercase;color:rgba(230,237,250,.84)}
  .an-modal .pill,.an-modal .close-btn{appearance:none;border:1px solid rgba(255,255,255,.12);background:linear-gradient(180deg,rgba(255,255,255,.055),rgba(255,255,255,.02));color:#edf3ff;border-radius:14px;padding:8px 12px;font-size:12px;font-weight:800;letter-spacing:.05em;text-transform:uppercase;display:inline-flex;align-items:center;justify-content:center;gap:6px;white-space:nowrap;flex:0 0 auto;box-shadow:0 10px 24px rgba(0,0,0,.16),inset 0 1px 0 rgba(255,255,255,.04);transition:transform .14s ease,box-shadow .14s ease,border-color .14s ease,background .14s ease}
  .an-modal .pill:hover,.an-modal .close-btn:hover{transform:translateY(-1px);border-color:rgba(123,112,255,.4);box-shadow:0 14px 30px rgba(0,0,0,.24),0 0 0 1px rgba(123,112,255,.14) inset}
  .an-modal .pill:active,.an-modal .close-btn:active{transform:none}
  .an-modal .pill.ghost,.an-modal .close-btn{background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.012))}
  .an-modal #an-run{background:linear-gradient(135deg,rgba(112,92,255,.92),rgba(72,144,255,.88));border-color:rgba(143,165,255,.38);box-shadow:0 16px 34px rgba(45,96,255,.26),0 0 18px rgba(116,97,255,.18)}
  .an-modal .pill[disabled],.an-modal .close-btn[disabled]{opacity:.55;pointer-events:none}
  .an-modal #an-toggle-ids{min-width:112px}
  .an-modal .badge{display:inline-flex;align-items:center;gap:6px;padding:4px 9px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.045);box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
  .an-modal input[type=search]{flex:1 1 320px;min-width:240px;max-width:440px;width:auto;height:38px;background:rgba(6,10,19,.82);border:1px solid rgba(255,255,255,.1);color:#e6eeff;border-radius:14px;padding:0 13px;box-shadow:inset 0 0 0 1px rgba(255,255,255,.02),0 8px 24px rgba(0,0,0,.12)}
  .an-modal input[type=search]:focus{outline:none;border-color:rgba(120,136,255,.52);box-shadow:0 0 0 3px rgba(115,97,255,.14),inset 0 0 0 1px rgba(255,255,255,.02)}
  .an-modal .an-pairs{display:flex;flex-wrap:wrap;gap:8px;padding:8px 14px 10px;border-bottom:1px solid rgba(255,255,255,.06);background:linear-gradient(180deg,rgba(255,255,255,.02),rgba(255,255,255,.008))}
  .an-modal .an-pair-chip{font-size:11px;cursor:pointer;display:inline-flex;align-items:center;gap:7px;padding:7px 11px;border-radius:999px;border:1px solid rgba(255,255,255,.1);background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.02));color:#dbe6ff;font-weight:800;letter-spacing:.06em;text-transform:uppercase;box-shadow:0 10px 22px rgba(0,0,0,.16);transition:transform .14s ease,box-shadow .14s ease,border-color .14s ease,background .14s ease;opacity:.92}
  .an-modal .an-pair-chip:hover{transform:translateY(-1px);border-color:rgba(139,92,246,.4);box-shadow:0 14px 28px rgba(0,0,0,.22),0 0 0 1px rgba(139,92,246,.12) inset}
  .an-modal .an-pair-chip.on{background:linear-gradient(180deg,rgba(107,92,255,.28),rgba(58,130,246,.12));border-color:rgba(118,110,255,.5);box-shadow:0 16px 30px rgba(0,0,0,.24),0 0 18px rgba(110,94,255,.14)}
  .an-modal .an-pair-chip span.dir{display:inline-flex;align-items:center;justify-content:center;opacity:.82}
  .an-modal .an-pair-chip span.dir .material-symbols-rounded{font-size:15px;line-height:1;font-variation-settings:"FILL" 0,"wght" 500,"GRAD" 0,"opsz" 20}
  .an-modal .an-wrap{flex:1;min-height:0;display:grid;grid-template-rows:minmax(230px,1fr) 10px minmax(180px,.8fr);overflow:hidden;padding:10px 14px 0;gap:0}
  .an-modal .an-grid,.an-modal .an-issues{overflow:auto;min-height:0;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(11,16,29,.92),rgba(6,9,16,.96));box-shadow:inset 0 1px 0 rgba(255,255,255,.03),0 18px 36px rgba(0,0,0,.18)}
  .an-modal .an-grid{border-radius:20px 20px 14px 14px}
  .an-modal .an-issues{border-radius:14px 14px 20px 20px;padding:12px}
  .an-modal .an-split{height:10px;margin:0 14px;border-radius:999px;background:linear-gradient(90deg,rgba(255,255,255,.05),rgba(123,100,255,.24),rgba(80,155,255,.24),rgba(255,255,255,.05));box-shadow:inset 0 0 0 1px rgba(255,255,255,.05),0 0 0 1px rgba(0,0,0,.2);cursor:row-resize;position:relative}
  .an-modal .an-split::after{content:"";position:absolute;left:50%;top:50%;width:72px;height:3px;border-radius:999px;background:rgba(226,236,255,.72);transform:translate(-50%,-50%);box-shadow:0 0 14px rgba(128,115,255,.28)}
  .an-modal .row{display:grid;gap:10px;padding:11px 12px;border-bottom:1px solid rgba(255,255,255,.05);align-items:center;transition:background .14s ease,box-shadow .14s ease}
  .an-modal .row:not(.head):hover{background:rgba(255,255,255,.025)}
  .an-modal .head{position:sticky;top:0;background:linear-gradient(180deg,rgba(17,24,40,.96),rgba(10,14,24,.92));z-index:2;backdrop-filter:blur(8px);border-bottom:1px solid rgba(255,255,255,.08)}
  .an-modal .row.sel{outline:1px solid rgba(123,147,255,.44);background:linear-gradient(90deg,rgba(111,93,255,.12),rgba(76,145,255,.08));box-shadow:inset 0 0 0 1px rgba(255,255,255,.03)}
  .an-modal .cell,.an-modal .prov,.an-modal .feat,.an-modal .title{min-width:0}
  .an-modal .title-stack{display:flex;flex-direction:column;justify-content:center;align-self:center;gap:6px}
  .an-modal .an-grid:not(.show-ids) .title-stack{gap:0;justify-content:center}
  .an-modal .chip{display:inline-flex;align-items:center;border:1px solid rgba(255,255,255,.10);border-radius:999px;padding:3px 7px;margin:2px;background:rgba(255,255,255,.038);color:#dbe8ff}
  .an-modal .mono{font-family:ui-monospace,SFMono-Regular,Consolas,monospace}
  .an-modal .ids{opacity:.84;padding-top:0}
  .an-modal .an-grid.show-ids .ids{display:block;margin-top:6px}
  .an-modal .an-grid .ids{display:none}
  .an-modal .row .title{display:flex;align-items:center;gap:6px;min-height:20px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-weight:700;color:#f3f6ff;line-height:1.2}
  .an-modal .row .title small{opacity:.62;margin-left:6px}
  .an-modal .row .prov{font-weight:800;letter-spacing:.05em;color:#dce7ff;text-transform:uppercase}
  .an-modal .row .feat{opacity:.82;text-transform:capitalize}
  .an-modal .row .counts{font-size:12px;opacity:.8}
  .an-modal .sort{cursor:pointer;user-select:none;font-weight:800;letter-spacing:.05em;text-transform:uppercase;font-size:11px;color:#adbbdb}
  .an-modal .sort span.label{margin-right:4px}
  .an-modal .sort span.dir{opacity:.72;font-size:10px}
  .an-modal .issue{border-radius:18px;padding:14px 15px;margin-bottom:10px;background:linear-gradient(180deg,rgba(255,255,255,.055),rgba(255,255,255,.02));border:1px solid rgba(255,255,255,.09);box-shadow:0 16px 30px rgba(0,0,0,.16),inset 0 1px 0 rgba(255,255,255,.03)}
  .an-modal .issue .h{font-weight:800;margin-bottom:6px;letter-spacing:.02em;color:#f4f7ff}
  .an-modal .issue .badge{margin-top:6px}
  .an-modal .an-collapse{margin-top:6px;border:1px solid rgba(255,255,255,.08);border-radius:14px;background:rgba(255,255,255,.025);padding:8px 10px}
  .an-modal .an-collapse summary{cursor:pointer;list-style:none;font-weight:800;color:#dbe8ff}
  .an-modal .an-collapse summary::-webkit-details-marker{display:none}
  .an-modal .an-collapse ul{margin:8px 0 0 18px;padding:0}
  .an-modal .issue.manual-ids{margin-top:6px}
  .an-modal .ids-edit{display:flex;flex-direction:column;gap:10px;margin-top:8px}
  .an-modal .ids-edit-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:8px}
  .an-modal .ids-edit-row label{display:flex;align-items:center;gap:8px;font-size:12px;opacity:.94;padding:8px 10px;border-radius:14px;background:rgba(255,255,255,.025);border:1px solid rgba(255,255,255,.06)}
  .an-modal .ids-edit-row label span{min-width:52px;text-transform:uppercase;letter-spacing:.05em;color:#a7b9ff;font-weight:800}
  .an-modal .ids-edit-row input{flex:1 1 auto;background:rgba(4,8,16,.82);border:1px solid rgba(255,255,255,.12);border-radius:10px;padding:7px 8px;font-size:12px;color:#dbe8ff}
  .an-modal .ids-edit-row input:focus{outline:none;border-color:rgba(118,135,255,.5);box-shadow:0 0 0 3px rgba(115,97,255,.12)}
  .an-modal .ids-edit-actions{display:flex;gap:8px;justify-content:flex-end;margin-top:8px}
  .an-modal .an-footer{padding:9px 14px 12px;border-top:1px solid rgba(255,255,255,.08);display:grid;grid-template-columns:auto 1fr;align-items:center;font-size:12px;background:linear-gradient(180deg,rgba(255,255,255,.012),rgba(255,255,255,.03));gap:12px}
  .an-modal .an-footer .count-stack{display:inline-flex;align-items:center;flex-wrap:wrap;gap:8px;line-height:1.15;white-space:nowrap}
  .an-modal .an-footer .count-stack > span{display:inline-flex;align-items:center;min-height:30px;padding:0 10px;border-radius:999px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08)}
  .an-modal .an-footer .stats{justify-self:end;display:flex;align-items:center;justify-content:flex-end;gap:8px;flex-wrap:wrap;opacity:.88}
  .an-modal .an-footer .stats.empty{opacity:.4}
  .an-modal .an-footer .prov-stat{display:inline-flex;align-items:center;gap:8px;min-height:30px;padding:0 10px;border-radius:999px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.035)}
  .an-modal .an-footer .prov-stat-brand{display:inline-flex;align-items:center;justify-content:center;min-width:18px;height:18px}
  .an-modal .an-footer .prov-stat-brand img{display:block;width:auto;height:13px;max-width:42px;object-fit:contain;filter:brightness(1.03)}
  .an-modal .an-footer .prov-stat-text{font-size:10px;font-weight:800;letter-spacing:.05em;text-transform:uppercase;color:#e7eeff}
  .an-modal .an-footer .prov-stat-count{font-size:11px;font-weight:800;color:rgba(231,238,255,.84)}
  .an-modal .an-grid,.an-modal .an-issues{scrollbar-width:thin;scrollbar-color:#8b5cf6 #10131a}
  .an-modal .an-grid::-webkit-scrollbar,.an-modal .an-issues::-webkit-scrollbar{height:10px;width:10px}
  .an-modal .an-grid::-webkit-scrollbar-track,.an-modal .an-issues::-webkit-scrollbar-track{background:rgba(255,255,255,.03);border-radius:12px}
  .an-modal .an-grid::-webkit-scrollbar-thumb,.an-modal .an-issues::-webkit-scrollbar-thumb{background:linear-gradient(180deg,#8b5cf6 0%,#3b82f6 100%);border-radius:12px;border:2px solid #11141c;box-shadow:inset 0 0 0 1px rgba(139,92,246,.35),0 0 10px rgba(139,92,246,.4)}
  .an-modal .an-grid::-webkit-scrollbar-thumb:hover,.an-modal .an-issues::-webkit-scrollbar-thumb:hover{background:linear-gradient(180deg,#a78bfa 0%,#60a5fa 100%)}
  .unsync-dot{display:inline-block;flex:0 0 8px;width:8px;height:8px;border-radius:50%;margin:0;background:radial-gradient(circle,#ffb0d0,#ff3b7f);box-shadow:0 0 8px rgba(255,59,127,.8);align-self:center}
  .blocked-ico{display:inline-block;margin-right:6px;vertical-align:middle;font-size:13px;line-height:1;filter:drop-shadow(0 0 10px rgba(255,90,120,.7))}
  .wait-overlay{position:fixed;inset:0;display:flex;align-items:center;justify-content:center;background:rgba(3,4,10,.74);backdrop-filter:blur(6px);z-index:9999;opacity:1;transition:opacity .18s ease}
  .wait-overlay.hidden{opacity:0;pointer-events:none}
  .wait-card{display:flex;flex-direction:column;align-items:center;gap:14px;padding:24px 30px;border-radius:22px;background:linear-gradient(180deg,rgba(10,14,24,.96),rgba(7,10,18,.96));border:1px solid rgba(255,255,255,.08);box-shadow:0 24px 60px rgba(0,0,0,.35),0 0 0 1px rgba(255,255,255,.03) inset}
  .wait-ring{width:64px;height:64px;border-radius:50%;position:relative;filter:drop-shadow(0 0 12px rgba(122,107,255,.55))}
  .wait-ring::before{content:"";position:absolute;inset:0;border-radius:50%;padding:4px;background:conic-gradient(#7a6bff,#23d5ff,#7a6bff);-webkit-mask:linear-gradient(#000 0 0) content-box,linear-gradient(#000 0 0);-webkit-mask-composite:xor;mask-composite:exclude;animation:wait-spin 1.1s linear infinite}
  .wait-text{font-weight:800;color:#dbe8ff;text-shadow:0 0 12px rgba(122,107,255,.28)}
  @keyframes wait-spin{to{transform:rotate(360deg)}}
  @media (max-width:980px){
    .cx-modal-shell.analyzer-modal-shell{width:min(var(--cxModalMaxW,960px),calc(100vw - 24px))!important;max-width:min(var(--cxModalMaxW,960px),calc(100vw - 24px))!important;height:min(var(--cxModalMaxH,86vh),calc(100vh - 24px))!important}
    .an-modal .cx-head{grid-template-columns:1fr}
    .an-modal .an-actions{justify-content:flex-start}
    .an-modal .an-intro{grid-template-columns:1fr}
    .an-modal .an-intro-meta{justify-content:flex-start}
    .an-modal .an-wrap{padding:10px 12px 0}
  }
  @media (max-width:720px){
    .an-modal .cx-left{gap:10px}
    .an-modal input[type=search]{min-width:100%;max-width:none}
    .an-modal .an-pairs{padding:8px 12px 10px}
    .an-modal .an-footer{grid-template-columns:1fr}
    .an-modal .an-footer .stats{justify-self:start;justify-content:flex-start}
  }
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
    const shell = root.closest(".cx-modal-shell");
    if (shell) {
      shell.classList.add("analyzer-modal-shell");
      shell.style.setProperty("--cxModalMaxW", "960px");
      shell.style.setProperty("--cxModalMaxH", "86vh");
    }
    root.innerHTML = `
      <div class="cx-head">
        <div class="cx-left">
          <div class="cx-mark"><span class="material-symbols-rounded" aria-hidden="true">troubleshoot</span></div>
          <div class="cx-title">Analyzer</div>
          <button class="pill ghost" id="an-toggle-ids">IDs: hidden</button>
          <button class="pill ghost" id="an-scope">Scope: issues</button>
          <input id="an-search" type="search" placeholder="title, year, provider, feature...">
        </div>
        <div class="an-actions">
          <button class="pill" id="an-run" type="button">Analyze</button>
          <button class="close-btn" id="an-close">Close</button>
        </div>
      </div>
      <div class="an-intro">
        <div class="an-intro-copy">
          <div class="an-intro-title">Find missing, blocked, and out-of-scope deltas</div>
          <div class="an-intro-sub" id="an-summary-copy">Analyzer compares the selected source and destination pairs so you can see why a title is not lining up between providers.</div>
        </div>
        <div class="an-intro-meta" id="an-summary-meta">
          <span class="mini">Scoped 0</span>
          <span class="mini">Visible 0</span>
          <span class="mini">Issues 0</span>
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
          <span class="mono" id="an-issues-count" title="Issues are sync delta problems in the selected pairs, like missing peers or blocked items.">Issues: 0</span>
          <span class="mono" id="an-system-count" title="System findings are analyzer diagnostics about state files, providers, metadata, or other background integrity checks.">System: 0</span>
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
        <div class="wait-text" id="an-wait-text">Loading...</div>
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
      setWaitText(text || "Working...");
      clearTimeout(waitSlowTimer);
      waitSlowTimer = setTimeout(
        () => setWaitText(`${text} (still working...)`),
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
    const summaryCopy = Q("#an-summary-copy", root);
    const summaryMeta = Q("#an-summary-meta", root);
    const issuesCount = Q("#an-issues-count", root);
    const systemCount = Q("#an-system-count", root);
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
    let EXTRA_FINDINGS = [];
    let SUMMARY = {};
    let LIMIT_INFO = {};
    let LIMIT_AFFECTED = new Map();
    let BLOCKS_BY_PF = new Map();
    const setSummary = () => {
      const scoped = ITEMS.filter(inPairScope);
      if (summaryCopy) {
        summaryCopy.textContent = SCOPE === "issues"
          ? "Showing only items with detected delta issues for the selected pairs."
          : "Showing all scoped items for the selected pairs, including healthy matches.";
      }
      if (summaryMeta) {
        summaryMeta.innerHTML = `<span class="mini" title="Scoped items are rows included by the selected pair filter.">Scoped ${scoped.length}</span><span class="mini" title="Visible items are the rows currently shown in the top table after search and scope filters.">Visible ${VIEW.length}</span><span class="mini" title="Issues are sync delta problems in the selected pairs, such as missing peers.">Issues ${UNSYNCED.size}</span><span class="mini" title="System findings are analyzer diagnostics about files, metadata, providers, or state health.">System ${EXTRA_FINDINGS.length}</span>`;
      }
    };

    function selectedPairIds() {
      const all = (PAIRS || [])
        .map(p => String((p && p.id) || ""))
        .filter(Boolean);
      if (PAIR_FILTER && PAIR_FILTER.size) {
        const sel = Array.from(PAIR_FILTER)
          .map(x => String(x || ""))
          .filter(id => id && all.includes(id));
        return sel.length ? sel : all;
      }
      return all;
    }

    function withPairs(url) {
      const ids = selectedPairIds();
      if (!ids.length) return url;
      const q = `pairs=${encodeURIComponent(ids.join(","))}`;
      return url.includes("?") ? `${url}&${q}` : `${url}?${q}`;
    }


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
        SORT_KEY === k ? (SORT_DIR === "asc" ? "^" : "v") : "";
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
            <div class="title-stack">
              <div class="title">${
                blk
                  ? `<span class="blocked-ico" title="Blocked (manual)">[blocked]</span>`
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
                      const tip = reason ? `${text} - ${reason}` : text;
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
      setCols();
      setSummary();
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

    grid.addEventListener("click", e => {
      const sortEl = e.target.closest(".head .sort");
      if (sortEl) {
        const k = sortEl.dataset.k;
        SORT_DIR = SORT_KEY === k && SORT_DIR === "asc" ? "desc" : "asc";
        SORT_KEY = k;
        draw();
        return;
      }
      const row = e.target.closest(".row:not(.head)");
      if (row) select(row.getAttribute("data-tag"));
    });

    function escHtml(s) {
      return String(s)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function dedupeInfoFindings(list, typeName) {
      if (!Array.isArray(list) || !list.length) return [];
      const deduped = [];
      const grouped = new Map();
      list.forEach(p => {
        if (!p || typeof p !== "object") return;
        const sev = String(p.severity || "info").toLowerCase();
        const typ = String(p.type || "").toLowerCase();
        if (!(sev === "info" && typ === typeName)) {
          deduped.push(p);
          return;
        }
        const idsKey = p.ids && typeof p.ids === "object"
          ? Object.entries(p.ids)
              .filter(([, v]) => v != null && String(v) !== "")
              .map(([k, v]) => `${String(k)}:${String(v)}`)
              .sort()
              .join("|")
          : "";
        const missingKey = Array.isArray(p.missing)
          ? p.missing.map(v => String(v)).sort().join("|")
          : "";
        const sig = [typ, String(p.message || ""), String(p.item_title || ""), String(p.key || ""), idsKey, missingKey].join("::");
        const scope = p.provider && p.feature ? `${String(p.provider)} | ${String(p.feature)}` : "";
        const current = grouped.get(sig);
        if (current) {
          if (scope) current.scopes.add(scope);
          return;
        }
        const clone = { ...p, scopes: new Set(scope ? [scope] : []) };
        grouped.set(sig, clone);
        deduped.push(clone);
      });
      return deduped;
    }

    function dedupeMissingIdInfo(list) {
      return dedupeInfoFindings(list, "missing_ids");
    }

    function renderMaybeCollapsedList(items) {
      if (!items || !items.length) return `<span class="mono">none</span>`;
      const values = items.map(x => String(x));
      if (values.length <= 10) {
        return `<ul>${values.map(x => `<li>${escHtml(x)}</li>`).join("")}</ul>`;
      }
      const preview = values.slice(0, 8);
      const rest = values.slice(8);
      return `<details class="an-collapse">
        <summary class="mono">Show ${values.length} items</summary>
        <ul>${preview.map(x => `<li>${escHtml(x)}</li>`).join("")}</ul>
        <ul>${rest.map(x => `<li>${escHtml(x)}</li>`).join("")}</ul>
      </details>`;
    }

    function renderScopedList(items, label) {
      return `<div>
        <div class="h" style="font-size:12px">${escHtml(label)}</div>
        ${renderMaybeCollapsedList(items)}
      </div>`;
    }

    function renderAffectedItems(items, title = "Affected items") {
      if (!Array.isArray(items) || !items.length) return "";
      const rows = items.map(entry => {
        if (!entry || typeof entry !== "object") return "";
        const parts = [];
        if (entry.label) parts.push(String(entry.label));
        if (entry.key && String(entry.key) !== String(entry.label || "")) {
          parts.push(`Key: ${String(entry.key)}`);
        }
        if (entry.type) parts.push(`Type: ${String(entry.type)}`);
        if (entry.reason) parts.push(`Reason: ${String(entry.reason)}`);
        if (entry.attempts != null) parts.push(`Attempts: ${String(entry.attempts)}`);
        if (entry.ids && typeof entry.ids === "object") {
          const ids = Object.entries(entry.ids)
            .filter(([, v]) => v != null && String(v) !== "")
            .map(([k, v]) => `${String(k)}:${String(v)}`);
          if (ids.length) parts.push(`IDs: ${ids.join(", ")}`);
        }
        return `<li>${escHtml(parts.join(" | "))}</li>`;
      }).filter(Boolean).join("");
      if (!rows) return "";
      return `<details class="an-collapse">
        <summary class="mono">${escHtml(title)} (${items.length})</summary>
        <ul>${rows}</ul>
      </details>`;
    }
    function manualIdsBlock(it) {
      const ids = it.ids || {};
      const inputs = ID_FIELDS.map(name => {
        const val = ids[name] || "";
        return `<label><span>${name}</span><input type="text" name="${name}" data-idfield="${name}" value="${String(
          val
        )}"></label>`;
      }).join("");
      return `
        <div class="manual-ids">
          <details class="an-collapse" id="an-manual-ids">
            <summary class="mono">Edit Manual IDs</summary>
            <div class="ids-edit" style="margin-top:10px">
              <div class="ids-edit-row">
                ${inputs}
              </div>
              <div class="ids-edit-actions">
                <button type="button" class="pill" data-act="patch-ids">Save IDs</button>
                <button type="button" class="pill ghost" data-act="reset-ids">Reset</button>
              </div>
            </div>
          </details>
        </div>`;
    }

    function renderNormalizationPanel(list) {
      if (!list || !list.length) return "";
      const normalized = dedupeMissingIdInfo(list);
      return normalized
        .map(p => {
          const src = String(p.source || "").toUpperCase();
          const dst = String(p.target || "").toUpperCase();
          const sev = String(p.severity || "info").toLowerCase();
          const badge = sev === "warn" ? "Warning" : sev === "error" ? "Error" : "Info";
          const delta = p.show_delta || {};
          const gap = p.show_gap || {};
          const srcCount = delta.source ?? "?";
          const dstCount = delta.target ?? "?";
          const srcOnly = gap.source_only ?? "?";
          const dstOnly = gap.target_only ?? "?";
          const ratio = typeof gap.ratio === "number" && Number.isFinite(gap.ratio)
            ? gap.ratio.toFixed(2)
            : null;

          const srcTitles = p.extra_source_titles || [];
          const dstTitles = p.extra_target_titles || [];
          const srcIds = p.extra_source || [];
          const dstIds = p.extra_target || [];

          const listSrc = srcTitles.length ? srcTitles : srcIds;
          const listDst = dstTitles.length ? dstTitles : dstIds;
          const detail = p.message ||
            "These counts can sometimes differ because some shows are split or merged differently between providers.";

          return `
          <div class="issue">
            <div class="h">History normalization: ${src} <-> ${dst}</div>
            ${sev !== "info" ? `<div><span class="badge mono">${escHtml(badge)}</span></div>` : ""}
            <div>${src} has ${srcCount} shows, ${dst} has ${dstCount} shows.</div>
            <div>${escHtml(detail)}</div>
            <div class="mono" style="opacity:.8;margin-top:6px">Only in ${src}: ${srcOnly} | Only in ${dst}: ${dstOnly}${ratio ? ` | Ratio: ${ratio}x` : ""}</div>
            <div style="margin-top:6px">
              ${renderScopedList(listSrc, `Only in ${src}`)}
              ${renderScopedList(listDst, `Only in ${dst}`)}
            </div>
          </div>`;
        })
        .join("");
    }

    function renderGenericFindingBlocks(list) {
      if (!Array.isArray(list) || !list.length) return "";
      const normalized = dedupeMissingIdInfo(list);
      return normalized
        .map(p => {
          const sev = String(p.severity || "info").toLowerCase();
          const badge = sev === "error" ? "Error" : sev === "warn" ? "Warning" : "Info";
          const title =
            p.title ||
            p.message ||
            p.type ||
            "Analyzer finding";
          const parts = [];
          if (p.message && p.message !== title) parts.push(String(p.message));
          if (p.item_title) parts.push(`Item: ${String(p.item_title)}`);
          if (p.key) parts.push(`Key: ${String(p.key)}`);
          if (p.path) parts.push(`Path: ${String(p.path)}`);
          if (p.module) parts.push(`Module: ${String(p.module)}`);
          if (p.scopes instanceof Set && p.scopes.size > 0) {
            const scopes = Array.from(p.scopes.values());
            parts.push(`${scopes.length > 1 ? "Scopes" : "Scope"}: ${scopes.join(" | ")}`);
          }
          if (!(p.scopes instanceof Set && p.scopes.size > 0) && p.provider && p.feature)
            parts.push(`Scope: ${String(p.provider)} | ${String(p.feature)}`);
          if (Array.isArray(p.missing) && p.missing.length)
            parts.push(`Missing IDs: ${p.missing.map(v => String(v)).join(", ")}`);
          if (p.id_name)
            parts.push(`ID field: ${String(p.id_name)}`);
          if (p.id_value != null)
            parts.push(`ID value: ${String(p.id_value)}`);
          if (p.key_base)
            parts.push(`Key base: ${String(p.key_base)}`);
          if (p.ids && typeof p.ids === "object") {
            const idEntries = Object.entries(p.ids)
              .filter(([, v]) => v != null && String(v) !== "")
              .map(([k, v]) => `${String(k)}:${String(v)}`);
            if (idEntries.length) parts.push(`IDs: ${idEntries.join(", ")}`);
          }
          if (p.watermark_key)
            parts.push(`Watermark key: ${String(p.watermark_key)}`);
          if (p.value != null)
            parts.push(`Value: ${String(p.value)}`);
          if (p.kind && !p.provider && !p.feature)
            parts.push(`Kind: ${String(p.kind)}`);
          if (p.error) parts.push(`Error: ${String(p.error)}`);
          const affectedItems = renderAffectedItems(
            Array.isArray(p.affected_items) ? p.affected_items : [],
            p.type === "cw_state_blackbox_active"
              ? "Blocked items"
              : p.type === "cw_state_unresolved_backlog"
                ? "Unresolved items"
                : "Affected items"
          );
          return `<div class="issue">
            <div class="h">${escHtml(title)}</div>
            ${sev !== "info" ? `<div><span class="badge mono">${escHtml(badge)}</span></div>` : ""}
            ${parts
              .map(
                line =>
                  `<div class="mono" style="opacity:.8;margin-top:6px">${escHtml(line)}</div>`
              )
              .join("")}
            ${affectedItems}
          </div>`;
        })
        .join("");
    }

    function renderSystemFindingsSection(title, findings, open = false) {
      if (!Array.isArray(findings) || !findings.length) return "";
      const blocks = renderGenericFindingBlocks(findings);
      if (!blocks) return "";
      return `<div class="issue">
        <div class="h">${escHtml(title)}</div>
        <div style="opacity:.85">System findings are background diagnostics. They don't always mean the selected pair is currently unsynced, but they can still explain risky or inconsistent state.</div>
        <details class="an-collapse"${open ? " open" : ""}>
          <summary class="mono">Show ${findings.length} finding${findings.length === 1 ? "" : "s"}</summary>
          <div style="margin-top:8px">${blocks}</div>
        </details>
      </div>`;
    }

    function bindManualIds(provider, feature, key, it) {
      const box = Q(".manual-ids", issues);
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
        btnSave.textContent = "Saving...";
        try {
          const body = {
            provider,
            feature,
            key,
            ids: idsPayload,
            rekey: true,
            merge_peer_ids: false
          };
          const res = await fjson(withPairs("/api/analyzer/patch"), {
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

      const manual = manualIdsBlock(it);
      const header = `<div class="issue">
        <div class="h">${heading}</div>
        <div>${status}</div>
        ${manual}
      </div>`;
      const normalizationBlock = renderNormalizationPanel(NORMALIZATION);
      const localFindings = EXTRA_FINDINGS.filter(
        p =>
          String(p.provider || "").toUpperCase() === provider &&
          String(p.feature || "").toLowerCase() === String(feature || "").toLowerCase() &&
          (!p.key || String(p.key) === String(key))
      );
      const localFindingKeys = new Set(localFindings.map(p => JSON.stringify(p)));
      const otherSystemFindings = EXTRA_FINDINGS.filter(
        p => !localFindingKeys.has(JSON.stringify(p))
      );
      const localSystemSection = renderSystemFindingsSection(
        "System findings for this selection",
        localFindings,
        true
      );
      const allSystemSection = renderSystemFindingsSection(
        "Other system findings",
        otherSystemFindings,
        false
      );
      const limitBlock = renderLimitPanel(tag);
      const scopeBlock = renderScopeExclusions();
      issues.innerHTML =
        limitBlock +
        header +
        normalizationBlock +
        scopeBlock +
        localSystemSection +
        allSystemSection;
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
        `${src} -> ${dst} | ${feat}: ${countStr}${allowed}`
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
              ? "swap_horiz"
              : "arrow_forward";
          const badge = total
            ? `<span class="mono">${unsynced || 0}/${total}</span>`
            : "";
          const cls = `an-pair-chip${on ? " on" : ""}`;
          return `<button type="button" class="${cls}" data-id="${esc(
            String(p.id || "")
          )}"><span class="mono">${src}</span><span class="dir"><span class="material-symbols-rounded" aria-hidden="true">${dir}</span></span><span class="mono">${dst}</span>${badge}</button>`;
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
            const srcLabel = providerInstanceLabel(src, p.source_instance);
            const dstLabel = providerInstanceLabel(dst, p.target_instance);
            return Object.assign({}, p, {
              source: src,
              target: dst,
              source_label: srcLabel,
              target_label: dstLabel,
              id
            });
          });
        renderPairs();
        for (const p of PAIRS) {
          if (!p.enabled) continue;
          if (PAIR_FILTER.size && !PAIR_FILTER.has(String(p.id)))
            continue;
          const src = p.source;
          const dst = p.target;
          const srcLabel = p.source_label || src;
          const dstLabel = p.target_label || dst;
          const F = p.features || {};
          for (const feat of ["history", "watchlist", "ratings"]) {
            if (!on(F[feat])) continue;
            add(src, feat, dst);
            add(src, feat, dstLabel);
            add(srcLabel, feat, dst);
            add(srcLabel, feat, dstLabel);
            if (_isTwoWayMode(p.mode)) {
              add(dst, feat, src);
              add(dst, feat, srcLabel);
              add(dstLabel, feat, src);
              add(dstLabel, feat, srcLabel);
            }
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
      showWait("Loading pairs...");
      await getActivePairMap();
      setWaitText("Reading scoped state...");
      let s;
      try {
        s = await fjson(withPairs("/api/analyzer/state"));
      } catch {
        s = { counts: {}, items: [] };
        issues.innerHTML = `
          <div class="issue">
            <div class="h">No scoped state yet</div>
            <div>Run a sync for the selected pair(s), then reopen Analyzer.</div>
          </div>`;
      }
      ITEMS = s.items || [];
      VIEW = ITEMS.slice();
      const countsText = renderCounts(s.counts);
      stats.innerHTML = countsText;
      if (!countsText) stats.classList.add("empty");
      else stats.classList.remove("empty");
      issuesCount.textContent = "Issues: 0";
      if (systemCount) systemCount.textContent = "System: 0";
      if (blockedCount) blockedCount.textContent = "Blocked: 0";
      draw();
      setWaitText("Analyzing...");
      try {
        await analyze(true);
      } finally {
        hideWait();
      }
    }

    async function analyze(silent = false) {
      if (!silent) showWait("Analyzing...");
      const pairMap = await getActivePairMap();
      const [meta, status] = await Promise.all([
        fjson(withPairs("/api/analyzer/problems")).catch(() => ({ problems: [] })),
        fjson("/api/status").catch(() => null),
        refreshBlocked().catch(() => null)
      ]);

      PAIR_STATS = meta.pair_stats || [];
      PAIR_EXCLUSIONS = meta.pair_exclusions || [];
      SUMMARY = meta.summary || {};
      PAIR_SCOPE_KEYS = buildPairScopeKeys(pairMap);
      renderPairs();

      const all = meta.problems || [];
      const normalization = all.filter(
        p => p && p.type === "history_show_normalization"
      );
      NORMALIZATION = normalization;
      EXTRA_FINDINGS = all.filter(
        p =>
          p &&
          p.type !== "missing_peer" &&
          p.type !== "blocked_manual" &&
          p.type !== "history_show_normalization"
      );

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
      issuesCount.textContent = parts.join(" | ");
      if (systemCount) systemCount.textContent = `System: ${EXTRA_FINDINGS.length}`;
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
        const extras = renderGenericFindingBlocks(EXTRA_FINDINGS);
        if ((NORMALIZATION && NORMALIZATION.length) || extras) {
          issues.innerHTML = renderNormalizationPanel(NORMALIZATION) + extras + notes;
        } else {
          const ok = `<div class="issue"><div class="h">No issues detected</div><div>The selected source and destination pairs are currently aligned for this scope.</div></div>`;
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
      btnRun.textContent = "Analyzing...";
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



