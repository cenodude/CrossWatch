/* assets/js/modals/analyzer/index.js */
/* Modal for analyzing sync issues and provider feature coverage. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const fjson = async (u, o = {}) => {
  const controller = new AbortController();
  const external = o.signal;
  const abort = () => controller.abort(external?.reason);
  if (external?.aborted) abort();
  else external?.addEventListener?.("abort", abort, { once: true });
  const timeoutMs = Number(o.timeoutMs) > 0 ? Number(o.timeoutMs) : 120000;
  const timer = setTimeout(() => controller.abort("Analyzer request timed out"), timeoutMs);
  try {
    const r = await fetch(u, { ...o, signal: controller.signal });
    if (!r.ok) throw new Error(r.status);
    return r.json();
  } finally {
    clearTimeout(timer);
    external?.removeEventListener?.("abort", abort);
  }
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
  "mdblist",
  "publicmetadb"
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
  .an-modal .an-intro{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:12px;align-items:center;min-height:52px;padding:7px 12px;border-bottom:1px solid rgba(255,255,255,.06);background:linear-gradient(180deg,rgba(255,255,255,.02),rgba(255,255,255,.006))}
  .an-modal .an-intro-copy{min-width:0}
  .an-modal .an-intro-title{font-size:14px;font-weight:800;letter-spacing:.01em;color:#f4f7ff}
  .an-modal .an-intro-sub{margin-top:4px;font-size:12px;line-height:1.45;color:rgba(205,215,235,.74)}
  .an-modal .an-intro-meta{display:flex;align-items:center;justify-content:flex-end;gap:8px;flex-wrap:wrap}
  .an-modal .an-intro-meta .mini{display:inline-flex;align-items:center;min-height:28px;padding:0 10px;border-radius:999px;border:1px solid rgba(255,255,255,.09);background:rgba(255,255,255,.035);font-size:11px;font-weight:800;letter-spacing:.04em;text-transform:uppercase;color:rgba(230,237,250,.84)}
  .an-modal .an-intro-meta button.mini{cursor:pointer;font-family:inherit}
  .an-modal .an-intro-meta button.mini:disabled{cursor:default;opacity:.45}
  .an-modal .pill,.an-modal .close-btn{appearance:none;border:1px solid rgba(255,255,255,.12);background:linear-gradient(180deg,rgba(255,255,255,.055),rgba(255,255,255,.02));color:#edf3ff;border-radius:14px;padding:8px 12px;font-size:12px;font-weight:800;letter-spacing:.05em;text-transform:uppercase;display:inline-flex;align-items:center;justify-content:center;gap:6px;white-space:nowrap;flex:0 0 auto;box-shadow:0 10px 24px rgba(0,0,0,.16),inset 0 1px 0 rgba(255,255,255,.04);transition:transform .14s ease,box-shadow .14s ease,border-color .14s ease,background .14s ease}
  .an-modal .pill:hover,.an-modal .close-btn:hover{transform:translateY(-1px);border-color:rgba(123,112,255,.4);box-shadow:0 14px 30px rgba(0,0,0,.24),0 0 0 1px rgba(123,112,255,.14) inset}
  .an-modal .pill:active,.an-modal .close-btn:active{transform:none}
  .an-modal .pill.ghost,.an-modal .close-btn{background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.012))}
  .an-modal #an-run{background:linear-gradient(135deg,rgba(112,92,255,.92),rgba(72,144,255,.88));border-color:rgba(143,165,255,.38);box-shadow:0 16px 34px rgba(45,96,255,.26),0 0 18px rgba(116,97,255,.18)}
  .an-modal .pill[disabled],.an-modal .close-btn[disabled]{opacity:.55;pointer-events:none}
  .an-modal #an-toggle-ids{width:40px;padding-inline:0}
  .an-modal .badge{display:inline-flex;align-items:center;gap:6px;padding:4px 9px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.045);box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
  .an-modal input[type=search]{flex:1 1 320px;min-width:240px;max-width:440px;width:auto;height:38px;background:rgba(6,10,19,.82);border:1px solid rgba(255,255,255,.1);color:#e6eeff;border-radius:14px;padding:0 13px;box-shadow:inset 0 0 0 1px rgba(255,255,255,.02),0 8px 24px rgba(0,0,0,.12)}
  .an-modal input[type=search]:focus{outline:none;border-color:rgba(120,136,255,.52);box-shadow:0 0 0 3px rgba(115,97,255,.14),inset 0 0 0 1px rgba(255,255,255,.02)}
  .an-modal .an-pairs{display:flex;flex-wrap:wrap;gap:8px;padding:8px 14px 10px;border-bottom:1px solid rgba(255,255,255,.06);background:linear-gradient(180deg,rgba(255,255,255,.02),rgba(255,255,255,.008))}
  .an-modal .an-pair-chip{font-size:11px;cursor:pointer;display:inline-flex;align-items:center;gap:7px;padding:7px 11px;border-radius:999px;border:1px solid rgba(255,255,255,.1);background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.02));color:#dbe6ff;font-weight:800;letter-spacing:.06em;text-transform:uppercase;box-shadow:0 10px 22px rgba(0,0,0,.16);transition:transform .14s ease,box-shadow .14s ease,border-color .14s ease,background .14s ease;opacity:.92}
  .an-modal .an-pair-chip:hover{transform:translateY(-1px);border-color:rgba(139,92,246,.4);box-shadow:0 14px 28px rgba(0,0,0,.22),0 0 0 1px rgba(139,92,246,.12) inset}
  .an-modal .an-pair-chip.on{background:linear-gradient(180deg,rgba(107,92,255,.28),rgba(58,130,246,.12));border-color:rgba(118,110,255,.5);box-shadow:0 16px 30px rgba(0,0,0,.24),0 0 18px rgba(110,94,255,.14)}
  .an-modal .an-pair-prof{flex:0 0 auto;font-size:9px;font-weight:800;letter-spacing:.03em;line-height:1;padding:2px 5px;margin-left:-2px;border-radius:999px;background:color-mix(in srgb,var(--an-accent,#7b5cff) 22%,transparent);border:1px solid color-mix(in srgb,var(--an-accent,#7b5cff) 42%,transparent);color:var(--an-text,#dbe3ff)}
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
  .an-modal .head .cell{position:relative}
  .an-modal .col-resize{position:absolute;top:0;right:-5px;width:11px;height:100%;cursor:col-resize;z-index:4;touch-action:none}
  .an-modal .col-resize::after{content:"";position:absolute;top:22%;bottom:22%;left:5px;width:2px;border-radius:2px;background:rgba(255,255,255,.16)}
  .an-modal .col-resize:hover::after,.an-modal .col-resize.dragging::after{background:var(--an-accent,#7b6bff);box-shadow:0 0 8px rgba(123,107,255,.5)}
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
  .wait-retry{cursor:pointer;font-family:inherit;font-size:12px;font-weight:800;letter-spacing:.04em;text-transform:uppercase;color:#dbe8ff;padding:7px 16px;border-radius:999px;border:1px solid rgba(122,107,255,.45);background:rgba(122,107,255,.14)}
  .wait-retry:hover{background:rgba(122,107,255,.26)}
  .wait-retry.hidden{display:none}
  @keyframes wait-spin{to{transform:rotate(360deg)}}
  html[data-cw-theme="flat-dark"] .an-modal,
  html[data-cw-theme="flat-dark"] .an-modal .cx-head,
  html[data-cw-theme="flat-dark"] .an-modal .an-intro,
  html[data-cw-theme="flat-dark"] .an-modal .an-intro-meta .mini,
  html[data-cw-theme="flat-dark"] .an-modal .pill,
  html[data-cw-theme="flat-dark"] .an-modal .close-btn,
  html[data-cw-theme="flat-dark"] .an-modal .badge,
  html[data-cw-theme="flat-dark"] .an-modal input[type=search],
  html[data-cw-theme="flat-dark"] .an-modal .an-pairs,
  html[data-cw-theme="flat-dark"] .an-modal .an-pair-chip,
  html[data-cw-theme="flat-dark"] .an-modal .an-grid,
  html[data-cw-theme="flat-dark"] .an-modal .an-issues,
  html[data-cw-theme="flat-dark"] .an-modal .row.head,
  html[data-cw-theme="flat-dark"] .an-modal .issue,
  html[data-cw-theme="flat-dark"] .an-modal .an-collapse,
  html[data-cw-theme="flat-dark"] .an-modal .ids-edit-row label,
  html[data-cw-theme="flat-dark"] .an-modal .ids-edit-row input,
  html[data-cw-theme="flat-dark"] .an-modal .an-footer,
  html[data-cw-theme="flat-dark"] .an-modal .an-footer .count-stack > span,
  html[data-cw-theme="flat-dark"] .an-modal .an-footer .prov-stat,
  html[data-cw-theme="flat-dark"] .wait-card{background:#20242d!important;border-color:rgba(255,255,255,.14)!important;box-shadow:none!important;text-shadow:none!important;filter:none!important}
  html[data-cw-theme="flat-dark"] .an-modal::before,
  html[data-cw-theme="flat-dark"] .an-modal .an-split::after{content:none!important;display:none!important;background:none!important;box-shadow:none!important}
  html[data-cw-theme="flat-dark"] .an-modal .pill:hover,
  html[data-cw-theme="flat-dark"] .an-modal .close-btn:hover,
  html[data-cw-theme="flat-dark"] .an-modal .an-pair-chip:hover,
  html[data-cw-theme="flat-dark"] .an-modal .row:not(.head):hover{background:#2b313d!important;border-color:rgba(255,255,255,.19)!important;box-shadow:none!important;filter:none!important;transform:none!important}
  html[data-cw-theme="flat-dark"] .unsync-dot,
  html[data-cw-theme="flat-dark"] .blocked-ico,
  html[data-cw-theme="flat-dark"] .wait-ring,
  html[data-cw-theme="flat-dark"] .wait-text{box-shadow:none!important;filter:none!important;text-shadow:none!important}
  html[data-cw-theme="flat-dark"] .an-modal #an-run,
  html[data-cw-theme="flat-dark"] .an-modal .an-pair-chip.on{background:#252b3d!important;border-color:rgba(125,134,201,.45)!important;box-shadow:none!important}
  html[data-cw-theme="flat-dark"] .an-modal .an-grid,
  html[data-cw-theme="flat-dark"] .an-modal .an-issues{scrollbar-color:#3a414c #151821!important}
  html[data-cw-theme="flat-dark"] .an-modal .an-grid::-webkit-scrollbar-track,
  html[data-cw-theme="flat-dark"] .an-modal .an-issues::-webkit-scrollbar-track{background:#151821!important}
  html[data-cw-theme="flat-dark"] .an-modal .an-grid::-webkit-scrollbar-thumb,
  html[data-cw-theme="flat-dark"] .an-modal .an-issues::-webkit-scrollbar-thumb{background:#3a414c!important;border-color:#151821!important;box-shadow:none!important}
  html[data-cw-theme="flat-light"] .an-modal,
  html[data-cw-theme="flat-light"] .an-modal .cx-head,
  html[data-cw-theme="flat-light"] .an-modal .an-intro,
  html[data-cw-theme="flat-light"] .an-modal .an-intro-meta .mini,
  html[data-cw-theme="flat-light"] .an-modal .pill,
  html[data-cw-theme="flat-light"] .an-modal .close-btn,
  html[data-cw-theme="flat-light"] .an-modal .badge,
  html[data-cw-theme="flat-light"] .an-modal input[type=search],
  html[data-cw-theme="flat-light"] .an-modal .an-pairs,
  html[data-cw-theme="flat-light"] .an-modal .an-pair-chip,
  html[data-cw-theme="flat-light"] .an-modal .an-grid,
  html[data-cw-theme="flat-light"] .an-modal .an-issues,
  html[data-cw-theme="flat-light"] .an-modal .row.head,
  html[data-cw-theme="flat-light"] .an-modal .issue,
  html[data-cw-theme="flat-light"] .an-modal .an-collapse,
  html[data-cw-theme="flat-light"] .an-modal .ids-edit-row label,
  html[data-cw-theme="flat-light"] .an-modal .ids-edit-row input,
  html[data-cw-theme="flat-light"] .an-modal .an-footer,
  html[data-cw-theme="flat-light"] .an-modal .an-footer .count-stack > span,
  html[data-cw-theme="flat-light"] .an-modal .an-footer .prov-stat,
  html[data-cw-theme="flat-light"] .wait-card{background:#ffffff!important;border-color:rgba(21,31,48,.14)!important;color:#172033!important}
  html[data-cw-theme="flat-light"] .an-modal .pill:hover,
  html[data-cw-theme="flat-light"] .an-modal .close-btn:hover,
  html[data-cw-theme="flat-light"] .an-modal .an-pair-chip:hover,
  html[data-cw-theme="flat-light"] .an-modal .row:not(.head):hover{background:#eef2f7!important;border-color:rgba(21,31,48,.20)!important}
  html[data-cw-theme="flat-light"] .an-modal #an-run,
  html[data-cw-theme="flat-light"] .an-modal .an-pair-chip.on{background:#e9ecf7!important;border-color:rgba(88,101,168,.34)!important;color:#172033!important;-webkit-text-fill-color:#172033!important}
  html[data-cw-theme="flat-light"] .an-modal .an-pair-chip.on *{color:#172033!important;-webkit-text-fill-color:#172033!important;opacity:1!important}
  html[data-cw-theme="flat-light"] .an-modal .an-grid,
  html[data-cw-theme="flat-light"] .an-modal .an-issues{scrollbar-color:#c4ccd8 #eef2f7!important}
  html[data-cw-theme="flat-light"] .an-modal .an-grid::-webkit-scrollbar-track,
  html[data-cw-theme="flat-light"] .an-modal .an-issues::-webkit-scrollbar-track{background:#eef2f7!important}
  html[data-cw-theme="flat-light"] .an-modal .an-grid::-webkit-scrollbar-thumb,
  html[data-cw-theme="flat-light"] .an-modal .an-issues::-webkit-scrollbar-thumb{background:#c4ccd8!important;border-color:#eef2f7!important}
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

function injectAnalyzerCSS() {
  const existing = document.getElementById("cw-analyzer-redesign-css");
  if (existing?.tagName === "LINK") return Promise.resolve();
  existing?.remove();
  const link = document.createElement("link");
  const cssUrl = new URL("./styles.css", import.meta.url);
  const version = new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__;
  if (version) cssUrl.searchParams.set("v", version);
  link.id = "cw-analyzer-redesign-css";
  link.rel = "stylesheet";
  link.href = cssUrl.href;
  return new Promise(resolve => {
    link.addEventListener("load", resolve, { once: true });
    link.addEventListener("error", resolve, { once: true });
    document.head.appendChild(link);
  });
}

function gridTemplateFrom(widths) {
  return widths.map(w => `${w}px`).join(" ");
}

let activeCleanup = null;

export default {
  async mount(root) {
    activeCleanup?.();
    css();
    await injectAnalyzerCSS();
    root.classList.add("modal-root","an-modal");
    const shell = root.closest(".cx-modal-shell");
    if (shell) {
      shell.classList.add("analyzer-modal-shell");
      shell.style.setProperty("--cxModalW", "1180px");
      shell.style.setProperty("--cxModalMaxW", "1180px");
      shell.style.setProperty("--cxModalMaxH", "700px");
    }
    root.innerHTML = `
      <div class="an-app">
        <div class="cx-head">
          <div class="cx-head-left">
            <div class="an-head-text">
              <div class="cx-title">Sync Analyzer</div>
              <div class="an-head-sub">Understand what is aligned, what needs attention, and why.</div>
            </div>
          </div>
          <div class="an-actions">
            <button class="pill" id="an-run" type="button"><span class="material-symbols-rounded" aria-hidden="true">refresh</span><span>Analyze</span></button>
            <button class="close-btn" id="an-close" type="button"><span class="material-symbols-rounded" aria-hidden="true">close</span><span>Close</span></button>
          </div>
        </div>

        <div class="an-layout">
          <aside class="an-sidebar">
            <div class="an-side-label">View</div>
            <nav class="an-view-nav" aria-label="Analyzer view">
              <button class="an-view-btn active" id="an-scope-issues" type="button" data-scope="issues" aria-pressed="true">
                <span class="material-symbols-rounded" aria-hidden="true">problem</span>
                <span><strong>Needs attention</strong><small>Missing, blocked and risky items</small></span>
              </button>
              <button class="an-view-btn" id="an-scope-all" type="button" data-scope="all" aria-pressed="false">
                <span class="material-symbols-rounded" aria-hidden="true">dataset</span>
                <span><strong>All scoped items</strong><small>Browse healthy and affected data</small></span>
              </button>
            </nav>

            <div class="an-side-label an-pair-label">Sync pairs</div>
            <p class="an-side-hint">Choose the routes you want to compare.</p>
            <div class="an-pairs" id="an-pairs"></div>

            <div class="an-side-status">
              <div class="an-status-title">Analysis status</div>
              <div class="count-stack">
                <button type="button" id="an-issues-count" class="an-status-row active" aria-pressed="true" title="Items missing at one or more destinations."><i class="an-status-dot issue-dot"></i><b>Issues</b><em>0</em></button>
                <button type="button" id="an-system-count" class="an-status-row" aria-pressed="false" title="Background state and integrity diagnostics."><i class="an-status-dot system-dot"></i><b>System</b><em>0</em></button>
                <button type="button" id="an-blocked-count" class="an-status-row" aria-pressed="false" title="Items held back by a manual block list or an active blackbox."><i class="an-status-dot blocked-dot"></i><b>Blocked</b><em>0</em></button>
              </div>
            </div>
          </aside>

          <main class="an-main">
            <section class="an-intro">
              <div class="an-intro-copy">
                <div class="an-intro-title">Items that need your attention</div>
              </div>
              <div class="an-intro-meta" id="an-summary-meta">
              </div>
            </section>

            <section class="an-workspace">
              <div class="an-workspace-head">
                <div class="an-workspace-copy">
                  <span class="material-symbols-rounded" aria-hidden="true">fact_check</span>
                  <span><strong id="an-results-title">Issue results</strong><small>Select a row to see the reason and available details.</small></span>
                </div>
                <div class="an-tools">
                  <label class="an-search-wrap">
                    <span class="material-symbols-rounded" aria-hidden="true">search</span>
                    <input id="an-search" type="search" placeholder="Search title, provider or feature">
                  </label>
                  <button class="pill ghost" id="an-toggle-ids" type="button" title="Show IDs" aria-label="Show IDs" aria-pressed="false"><span class="material-symbols-rounded" aria-hidden="true">key</span></button>
                </div>
              </div>
              <div class="an-wrap" id="an-wrap">
                <div class="an-grid" id="an-grid"></div>
                <div class="an-split" id="an-split" title="Drag to resize the result and detail panes"></div>
                <div class="an-issues" id="an-issues"></div>
              </div>
            </section>
          </main>
        </div>
      </div>
    `;

    const wait = document.createElement("div");
    wait.id = "an-wait";
    wait.className = "wait-overlay hidden";
    wait.innerHTML = `
      <div class="wait-card" role="status" aria-live="assertive">
        <div class="wait-ring"></div>
        <div class="wait-text" id="an-wait-text">Loading...</div>
        <button class="wait-retry hidden" id="an-wait-retry" type="button">Retry</button>
      </div>`;
    root.appendChild(wait);

    let waitSlowTimer = null;
    let waitRetryTimer = null;
    let waitShownAt = 0;
    const setWaitText = t => {
      const el = Q("#an-wait-text", root);
      if (el) el.textContent = t;
    };
    const setWaitRetryVisible = v => {
      const b = Q("#an-wait-retry", root);
      if (b) b.classList.toggle("hidden", !v);
    };
    function showWait(text) {
      waitShownAt = performance.now();
      const el = Q("#an-wait", root);
      if (el) el.classList.remove("hidden");
      setWaitText(text || "Working...");
      setWaitRetryVisible(false);
      clearTimeout(waitSlowTimer);
      clearTimeout(waitRetryTimer);
      waitSlowTimer = setTimeout(
        () => setWaitText(`${text} (still working...)`),
        3000
      );
      waitRetryTimer = setTimeout(() => setWaitRetryVisible(true), 8000);
    }
    function hideWait() {
      clearTimeout(waitSlowTimer);
      clearTimeout(waitRetryTimer);
      waitSlowTimer = null;
      waitRetryTimer = null;
      setWaitRetryVisible(false);
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
    const summaryMeta = Q("#an-summary-meta", root);
    const issuesCount = Q("#an-issues-count", root);
    const systemCount = Q("#an-system-count", root);
    const blockedCount = Q("#an-blocked-count", root);
    const search = Q("#an-search", root);
    const btnRun = Q("#an-run", root);
    const btnToggleIDs = Q("#an-toggle-ids", root);
    const btnClose = Q("#an-close", root);
    const btnScopeIssues = Q("#an-scope-issues", root);
    const btnScopeAll = Q("#an-scope-all", root);
    const introTitle = Q(".an-intro-title", root);
    const resultsTitle = Q("#an-results-title", root);
    const split = Q("#an-split", root);

    const setStatusCount = (el, label, value, detail = "") => {
      if (!el) return;
      const labelEl = Q("b", el);
      const valueEl = Q("em", el);
      if (labelEl) labelEl.textContent = label;
      if (valueEl) valueEl.textContent = String(value);
      if (detail) el.title = detail;
    };

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
    let PAIR_ISSUE_COUNTS = {};
    let PAIR_EXCLUSIONS = [];
    let PAIR_SCOPE_KEYS = new Set();
    let UNSYNCED = new Set();
    let UNSYNCED_META = new Map();
    let UNSYNCED_REASON = new Map();
    let SCOPE = "issues";
    let NORMALIZATION = [];
    let EXTRA_FINDINGS = [];
    let UNRESOLVED_FINDINGS = [];
    let SUMMARY = {};
    let LIMIT_INFO = {};
    let LIMIT_AFFECTED = new Map();
    let BLOCKS_BY_PF = new Map();
    let BLOCKED_FINDINGS = [];
    let CORE_EXTRA = [];
    let BLOCKED_MANUAL = [];
    let PAIR_ACTIVITY = {};
    let PROFILE_LABELS = {};
    let DETAIL_LOADED = new Set();
    let DETAIL_VIEW = "issues";
    let ISSUE_ITEMS = [];
    let ISSUE_OFFSET = 0;
    let ALL_TOTAL = 0;
    let ALL_OFFSET = 0;
    let ANALYSIS_TIMINGS = {};
    let requestController = new AbortController();
    let analyzePromise = null;
    const PAGE_SIZE = 250;
    const cleanup = () => {
      requestController.abort();
      clearTimeout(waitSlowTimer);
      if (activeCleanup === cleanup) activeCleanup = null;
    };
    activeCleanup = cleanup;
    const setSummary = () => {
      if (introTitle) introTitle.textContent = SCOPE === "issues" ? "Items that need your attention" : "All items in the selected routes";
      if (resultsTitle) resultsTitle.textContent = SCOPE === "issues" ? "Issue results" : "Scoped item browser";
      if (summaryMeta) {
        const total = SCOPE === "all" ? ALL_TOTAL : ISSUE_ITEMS.length;
        const offset = SCOPE === "all" ? ALL_OFFSET : ISSUE_OFFSET;
        const page = total
          ? `<button class="mini" id="an-page-prev" type="button" ${offset <= 0 ? "disabled" : ""}>Previous</button><span class="mini">${offset + 1}-${Math.min(offset + ITEMS.length, total)} of ${total}</span><button class="mini" id="an-page-next" type="button" ${offset + ITEMS.length >= total ? "disabled" : ""}>Next</button>`
          : "";
        summaryMeta.innerHTML = page;
        Q("#an-page-prev", summaryMeta)?.addEventListener("click", () => SCOPE === "all" ? loadAllPage(Math.max(0, ALL_OFFSET - PAGE_SIZE)) : showIssuePage(Math.max(0, ISSUE_OFFSET - PAGE_SIZE)));
        Q("#an-page-next", summaryMeta)?.addEventListener("click", () => SCOPE === "all" ? loadAllPage(ALL_OFFSET + PAGE_SIZE) : showIssuePage(ISSUE_OFFSET + PAGE_SIZE));
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
      const cols = [
        ["provider", "Provider"],
        ["feature", "Feature"],
        ["title", "Title"],
        ["type", "Type"]
      ];
      const cells = cols
        .map(
          ([k, label], i) =>
            `<div class="cell sort" data-k="${k}"><span class="label">${label}</span><span class="dir">${dirMark(
              k
            )}</span>${
              i < cols.length - 1
                ? `<span class="col-resize" data-ci="${i}" title="Drag to resize"></span>`
                : ""
            }</div>`
        )
        .join("");
      return `
        <div class="row head" style="grid-template-columns:${gridTemplateFrom(
          COLS
        )}">${cells}</div>`;
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
      if (row) {
        DETAIL_VIEW = "issues";
        updateStatusActive();
        select(row.getAttribute("data-tag"));
      }
    });

    grid.addEventListener("pointerdown", e => {
      const rez = e.target.closest(".col-resize");
      if (!rez) return;
      e.preventDefault();
      e.stopPropagation();
      const ci = Number(rez.getAttribute("data-ci"));
      if (!Number.isInteger(ci) || ci < 0 || ci >= COLS.length) return;
      const startX = e.clientX;
      const startW = COLS[ci] || 60;
      const MINW = 60;
      let moved = false;
      rez.classList.add("dragging");
      try { rez.setPointerCapture(e.pointerId); } catch {}
      const mv = ev => {
        const dx = ev.clientX - startX;
        if (Math.abs(dx) > 2) moved = true;
        COLS[ci] = Math.max(MINW, Math.round(startW + dx));
        setCols();
      };
      const up = () => {
        rez.removeEventListener("pointermove", mv);
        rez.removeEventListener("pointerup", up);
        rez.removeEventListener("pointercancel", up);
        rez.classList.remove("dragging");
        if (moved) {
          try { localStorage.setItem("an.cols", JSON.stringify(COLS)); } catch {}
        }
        const suppress = ev => {
          ev.stopPropagation();
          grid.removeEventListener("click", suppress, true);
        };
        grid.addEventListener("click", suppress, true);
        setTimeout(() => grid.removeEventListener("click", suppress, true), 0);
      };
      rez.addEventListener("pointermove", mv);
      rez.addEventListener("pointerup", up);
      rez.addEventListener("pointercancel", up);
    });

    issuesCount?.addEventListener("click", () => setDetailView("issues"));
    systemCount?.addEventListener("click", () => setDetailView("system"));
    blockedCount?.addEventListener("click", () => setDetailView("blocked"));

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
      if (DETAIL_VIEW === "issues") {
        renderIssueDetail(tag);
        if (UNSYNCED.has(tag) && !DETAIL_LOADED.has(tag)) {
          await loadItemDetail(tag);
          if (SELECTED === tag && DETAIL_VIEW === "issues") {
            draw();
            renderIssueDetail(tag);
          }
        }
      }
    }

    function hintMessage(h) {
      if (!h || typeof h !== "object") return "";
      if (h.message) return String(h.message).trim();
      const parts = [];
      if (h.provider) parts.push(String(h.provider).toUpperCase());
      if (Array.isArray(h.reasons) && h.reasons.length) parts.push(h.reasons.map(String).join(", "));
      else if (h.reason) parts.push(String(h.reason));
      else if (h.kind) parts.push(String(h.kind));
      return parts.join(": ").trim();
    }

    async function loadItemDetail(tag) {
      const [provider, feature, key] = tag.split("::");
      let d;
      try {
        const base = withPairs("/api/analyzer/detail");
        const sep = base.includes("?") ? "&" : "?";
        d = await fjson(
          `${base}${sep}provider=${encodeURIComponent(provider)}&feature=${encodeURIComponent(feature)}&key=${encodeURIComponent(key)}`,
          { signal: requestController.signal }
        );
      } catch {
        return;
      }
      DETAIL_LOADED.add(tag);
      const details = Array.isArray(d.target_show_info) ? d.target_show_info : [];
      const msgs = details.map(x => String((x && x.message) || "").trim()).filter(Boolean);
      const hintMsgs = (Array.isArray(d.hints) ? d.hints : []).map(hintMessage).filter(Boolean);
      const merged = [];
      for (const r of [...hintMsgs, ...(UNSYNCED_REASON.get(tag) || []), ...msgs]) {
        if (r && !merged.includes(r)) merged.push(r);
      }
      if (merged.length) UNSYNCED_REASON.set(tag, merged);
    }

    async function loadSystemFindings() {
      let sys;
      try {
        sys = await fjson(withPairs("/api/analyzer/system"), { signal: requestController.signal });
      } catch {
        return;
      }
      if (requestController.signal.aborted) return;
      const list = Array.isArray(sys && sys.problems) ? sys.problems : [];
      EXTRA_FINDINGS = CORE_EXTRA.concat(list.filter(p => p && p.type !== "cw_state_unresolved_backlog"));
      UNRESOLVED_FINDINGS = list.filter(p => p && p.type === "cw_state_unresolved_backlog");
      BLOCKED_FINDINGS = BLOCKED_MANUAL.concat(list.filter(p => p && p.type === "cw_state_blackbox_active"));
      setStatusCount(systemCount, "System", systemFindings().length);
      const blockedTotal = BLOCKED_FINDINGS.reduce((acc, p) => {
        if (p.type === "cw_state_blackbox_active") {
          const c = Number(p.count);
          const n = Number.isFinite(c) && c > 0 ? c : (Array.isArray(p.affected_items) ? p.affected_items.length : 0) || 1;
          return acc + n;
        }
        return acc + 1;
      }, 0);
      setStatusCount(blockedCount, "Blocked", blockedTotal);
      const unresolvedTotal = unresolvedIssueCount();
      setStatusCount(issuesCount, "Issues", UNSYNCED.size, "Unique items needing attention");
      setSummary();
      renderActiveDetail();
    }

    function updateStatusActive() {
      const map = [[issuesCount, "issues"], [systemCount, "system"], [blockedCount, "blocked"]];
      for (const [el, v] of map) {
        if (!el) continue;
        const on = v === DETAIL_VIEW;
        el.classList.toggle("active", on);
        el.setAttribute("aria-pressed", on ? "true" : "false");
      }
    }

    function systemFindings() {
      return (EXTRA_FINDINGS || []).filter(p => p && p.type !== "cw_state_blackbox_active");
    }

    function unresolvedIssueCount() {
      return (UNRESOLVED_FINDINGS || []).reduce((acc, p) => {
        const c = Number(p && p.count);
        const n = Number.isFinite(c) && c > 0
          ? c
          : (Array.isArray(p && p.affected_items) ? p.affected_items.length : 0) || 1;
        return acc + n;
      }, 0);
    }

    function renderUnresolvedSection() {
      const blocks = renderGenericFindingBlocks(UNRESOLVED_FINDINGS);
      if (!blocks) return "";
      return `<div class="issue"><div class="h">Unresolved items</div><div style="opacity:.85">These were attempted during the last sync but could not be matched or written at the destination. They need attention until they resolve or are cleared.</div></div>` + blocks;
    }

    function renderIssuesEmpty() {
      const notes = renderScopeExclusions();
      const unresolved = renderUnresolvedSection();
      const extras = renderGenericFindingBlocks(EXTRA_FINDINGS);
      if (unresolved || (NORMALIZATION && NORMALIZATION.length) || extras) {
        issues.innerHTML = renderNormalizationPanel(NORMALIZATION) + unresolved + extras + notes;
      } else {
        const ok = `<div class="issue"><div class="h">No issues detected</div><div>The selected source and destination pairs are currently aligned for this scope.</div></div>`;
        issues.innerHTML = notes + ok;
      }
      issues.scrollTop = 0;
    }

    function renderSystemDetail() {
      const findings = systemFindings();
      const blocks = renderGenericFindingBlocks(findings);
      if (!blocks && !(NORMALIZATION && NORMALIZATION.length)) {
        issues.innerHTML =
          `<div class="issue"><div class="h">No system findings</div><div>No background state or integrity diagnostics for the selected routes.</div></div>`;
        issues.scrollTop = 0;
        return;
      }
      const header = findings.length
        ? `<div class="issue"><div class="h">System findings</div><div style="opacity:.85">Background diagnostics for the current state. They don't always mean the selected pair is unsynced, but they can explain risky or inconsistent state.</div></div>`
        : "";
      issues.innerHTML = renderNormalizationPanel(NORMALIZATION) + header + blocks;
      issues.scrollTop = 0;
    }

    function renderBlockedDetail() {
      const blocks = renderGenericFindingBlocks(BLOCKED_FINDINGS);
      if (!blocks) {
        issues.innerHTML =
          `<div class="issue"><div class="h">No blocked items</div><div>Nothing is currently held back by a manual block list or an active blackbox for the selected routes.</div></div>`;
        issues.scrollTop = 0;
        return;
      }
      issues.innerHTML =
        `<div class="issue"><div class="h">Blocked items</div><div style="opacity:.85">Items intentionally held back by a manual block list or an active blackbox. They are not synced by design.</div></div>` + blocks;
      issues.scrollTop = 0;
    }

    function renderActiveDetail() {
      if (DETAIL_VIEW === "system") { renderSystemDetail(); return; }
      if (DETAIL_VIEW === "blocked") { renderBlockedDetail(); return; }
      if (SELECTED && ITEMS.some(r => tagOf(r.provider, r.feature, r.key) === SELECTED)) {
        renderIssueDetail(SELECTED);
      } else {
        renderIssuesEmpty();
      }
    }

    function setDetailView(view) {
      DETAIL_VIEW = (view === "system" || view === "blocked") ? view : "issues";
      updateStatusActive();
      renderActiveDetail();
      if (issues) issues.scrollTop = 0;
    }

    function renderIssueDetail(tag) {
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

      const allReasons = UNSYNCED_REASON.get(tag) || [];
      // Drop any reason fully contained in a longer.
      const reasons = allReasons.filter(
        (r, i) =>
          !allReasons.some(
            (o, j) => j !== i && o.length > r.length && o.includes(r)
          )
      );
      const reasonBadge =
        unsynced && reasons.length
          ? reasons
              .map(r => ` <span class="badge mono">${escHtml(r)}</span>`)
              .join("")
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
      const unresolvedSection = renderUnresolvedSection();
      issues.innerHTML =
        limitBlock +
        header +
        normalizationBlock +
        scopeBlock +
        unresolvedSection +
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

function profileLabel(provider, instance) {
      const inst = String(instance || "").trim();
      if (!inst || inst.toLowerCase() === "default") return "";
      const prov = String(provider || "").toUpperCase();
      return (PROFILE_LABELS[prov] && PROFILE_LABELS[prov][inst]) || inst;
    }

    function lastRunPairId(list) {
      let best = null;
      let bestNs = -1;
      for (const p of list) {
        const id = String(p.id || "");
        const ns = Number((PAIR_ACTIVITY && PAIR_ACTIVITY[id]) || 0);
        if (ns > bestNs) {
          bestNs = ns;
          best = id;
        }
      }
      return bestNs > 0 ? best : null;
    }

    async function loadPairActivity() {
      try {
        const j = await fjson("/api/analyzer/pair-activity", { cache: "no-store", timeoutMs: 30000, signal: requestController.signal });
        const out = {};
        for (const p of (j && Array.isArray(j.pairs) ? j.pairs : [])) {
          if (p && p.id) out[String(p.id)] = Number(p.last_run_ns || 0);
        }
        PAIR_ACTIVITY = out;
      } catch {
        PAIR_ACTIVITY = {};
      }
    }

    async function loadProfileLabels() {
      try {
        const j = await fjson("/api/provider-instances", { cache: "no-store", timeoutMs: 30000, signal: requestController.signal });
        const map = j && typeof j === "object" ? j : {};
        const out = {};
        for (const [prov, raw] of Object.entries(map)) {
          const ids = [];
          for (const x of (Array.isArray(raw) ? raw : [])) {
            const id = typeof x === "string" ? x : (x && x.id ? String(x.id) : "");
            const norm = String(id || "").trim();
            if (norm && norm.toLowerCase() !== "default") ids.push(norm);
          }
          const labels = {};
          ids.forEach((id, i) => { labels[id] = `P${String(i + 1).padStart(2, "0")}`; });
          out[String(prov || "").toUpperCase()] = labels;
        }
        PROFILE_LABELS = out;
      } catch {
        PROFILE_LABELS = {};
      }
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
              ids.forEach(id => {
                if (list.some(p => String(p.id) === String(id))) PAIR_FILTER.add(String(id));
              });
          }
        } catch {}
        if (!PAIR_FILTER.size) {
          if (list.length === 1) {
            PAIR_FILTER.add(String(list[0].id));
          } else {
            const lastId = lastRunPairId(list);
            PAIR_FILTER.add(String(lastId || list[0].id));
          }
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
          const issueN = PAIR_ISSUE_COUNTS[String(p.id)] || 0;
          const badge = `<span class="an-pair-count${issueN ? " has" : ""}" title="${issueN} issue${issueN === 1 ? "" : "s"} for this pair">${issueN}</span>`;
          const cls = `an-pair-chip${on ? " on" : ""}`;
          const srcProf = profileLabel(src, p.source_instance);
          const dstProf = profileLabel(dst, p.target_instance);
          const prof = (label, name) =>
            label ? `<span class="an-pair-prof" title="${escHtml(name)} profile ${escHtml(label)}">${escHtml(label)}</span>` : "";
          return `<button type="button" class="${cls}" data-id="${esc(
            String(p.id || "")
          )}" aria-pressed="${on ? "true" : "false"}"><span class="mono">${src}</span>${prof(srcProf, src)}<span class="dir"><span class="material-symbols-rounded" aria-hidden="true">${dir}</span></span><span class="mono">${dst}</span>${prof(dstProf, dst)}${badge}</button>`;
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

    async function getActivePairMap(signal) {
      try {
        const arr = await fjson("/api/pairs", { cache: "no-store", timeoutMs: 30000, signal });
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
          for (const feat of ["history", "watchlist", "ratings", "progress"]) {
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

    async function loadAllPage(offset = 0) {
      showWait("Loading item page...");
      try {
        const sep = withPairs("/api/analyzer/state").includes("?") ? "&" : "?";
        const data = await fjson(
          `${withPairs("/api/analyzer/state")}${sep}offset=${Math.max(0, offset)}&limit=${PAGE_SIZE}`,
          { signal: requestController.signal }
        );
        ITEMS = Array.isArray(data.items) ? data.items : [];
        ALL_TOTAL = Number(data.total || 0);
        ALL_OFFSET = Number(data.offset || 0);
        filter(search.value || "");
      } catch (error) {
        if (!requestController.signal.aborted) {
          issues.innerHTML = `<div class="issue"><div class="h">Could not load this page</div><div>${escHtml(error?.message || error)}</div></div>`;
        }
      } finally {
        hideWait();
      }
    }

    function showIssuePage(offset = 0) {
      ISSUE_OFFSET = Math.max(0, Math.min(Number(offset || 0), Math.max(0, ISSUE_ITEMS.length - 1)));
      ITEMS = ISSUE_ITEMS.slice(ISSUE_OFFSET, ISSUE_OFFSET + PAGE_SIZE);
      filter(search.value || "");
    }

    async function load() {
      restoreSplit();
      dragY();
      showWait("Loading pairs...");
      await Promise.all([loadPairActivity(), loadProfileLabels()]);
      await getActivePairMap(requestController.signal);
      if (requestController.signal.aborted) {
        hideWait();
        return;
      }
      setWaitText("Reading scoped state...");
      let s;
      try {
        const stateUrl = withPairs("/api/analyzer/state");
        s = await fjson(`${stateUrl}${stateUrl.includes("?") ? "&" : "?"}offset=0&limit=0`, {
          signal: requestController.signal
        });
      } catch {
        if (requestController.signal.aborted) {
          hideWait();
          return;
        }
        s = { counts: {}, items: [], total: 0 };
        issues.innerHTML = `
          <div class="issue">
            <div class="h">No scoped state yet</div>
            <div>Run a sync for the selected pair(s), then reopen Analyzer.</div>
          </div>`;
      }
      ITEMS = s.items || [];
      ALL_TOTAL = Number(s.total || 0);
      VIEW = ITEMS.slice();
      setStatusCount(issuesCount, "Issues", 0);
      setStatusCount(systemCount, "System", 0);
      setStatusCount(blockedCount, "Blocked", 0);
      draw();
      setWaitText("Analyzing...");
      try {
        await analyze(true);
      } finally {
        hideWait();
      }
    }

    async function analyze(silent = false) {
      if (analyzePromise) return analyzePromise;
      analyzePromise = analyzeRun(silent).finally(() => {
        analyzePromise = null;
      });
      return analyzePromise;
    }

    async function analyzeRun(silent = false) {
      if (!silent) showWait("Analyzing...");
      const pairMap = await getActivePairMap(requestController.signal);
      if (requestController.signal.aborted) return;
      let meta;
      let status;
      try {
        [meta, status] = await Promise.all([
          fjson(withPairs("/api/analyzer/problems"), { signal: requestController.signal }),
          fjson("/api/status", { signal: requestController.signal }).catch(() => null)
        ]);
      } catch (error) {
        if (requestController.signal.aborted) return;
        issues.innerHTML = `<div class="issue"><div class="h">Analyzer failed</div><div>${escHtml(error?.message || error)}</div></div>`;
        if (!silent) hideWait();
        return;
      }

      PAIR_STATS = meta.pair_stats || [];
      PAIR_EXCLUSIONS = meta.pair_exclusions || [];
      SUMMARY = meta.summary || {};
      ANALYSIS_TIMINGS = meta.timings_ms || {};
      PAIR_SCOPE_KEYS = buildPairScopeKeys(pairMap);
      renderPairs();

      const all = meta.problems || [];
      BLOCKS_BY_PF = new Map();
      for (const finding of all) {
        if (!finding || finding.type !== "blocked_manual" || !finding.key) continue;
        const pf = _pfKey(finding.provider, finding.feature);
        if (!BLOCKS_BY_PF.has(pf)) BLOCKS_BY_PF.set(pf, new Set());
        BLOCKS_BY_PF.get(pf).add(_normKey(finding.key));
      }
      const normalization = all.filter(
        p => p && p.type === "history_show_normalization"
      );
      NORMALIZATION = normalization;
      EXTRA_FINDINGS = all.filter(
        p =>
          p &&
          p.type !== "missing_peer" &&
          p.type !== "blocked_manual" &&
          p.type !== "history_show_normalization" &&
          p.type !== "cw_state_unresolved_backlog"
      );
      UNRESOLVED_FINDINGS = all.filter(
        p => p && p.type === "cw_state_unresolved_backlog"
      );
      BLOCKED_FINDINGS = all.filter(
        p => p && (p.type === "blocked_manual" || p.type === "cw_state_blackbox_active")
      );
      CORE_EXTRA = EXTRA_FINDINGS.slice();
      BLOCKED_MANUAL = BLOCKED_FINDINGS.slice();
      DETAIL_LOADED = new Set();

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

      PAIR_ISSUE_COUNTS = {};
      const onFeat = f => f && (typeof f.enable === "boolean" ? f.enable : !!f);
      for (const pr of (PAIRS || [])) {
        const src = String(pr.source || "").toUpperCase();
        const dst = String(pr.target || "").toUpperCase();
        const srcL = String(pr.source_label || src).toUpperCase();
        const dstL = String(pr.target_label || dst).toUpperCase();
        const two = _isTwoWayMode(pr.mode);
        const F = pr.features || {};
        let n = 0;
        for (const it of keep) {
          if (!onFeat(F[String(it.feature || "").toLowerCase()])) continue;
          const ip = String(it.provider || "").toUpperCase();
          const tgts = (it.targets || []).map(t => String(t || "").toUpperCase());
          const fwd = (ip === src || ip === srcL) && (tgts.includes(dst) || tgts.includes(dstL));
          const rev = two && (ip === dst || ip === dstL) && (tgts.includes(src) || tgts.includes(srcL));
          if (fwd || rev) n++;
        }
        PAIR_ISSUE_COUNTS[String(pr.id)] = n;
      }
      renderPairs();

      UNSYNCED = new Set(
        keep.map(p => tagOf(p.provider, p.feature, p.key))
      );
      ISSUE_ITEMS = keep.map(p => ({
        provider: p.provider,
        feature: p.feature,
        key: p.key,
        title: p.title || p.item_title || "",
        year: p.year,
        type: p.item_type || p.type_name || "",
        series_title: p.series_title || "",
        season: p.season,
        episode: p.episode,
        ids: p.ids || {}
      }));
      if (SCOPE === "issues") showIssuePage(0);

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
        const hintMsgs = (Array.isArray(p.hints) ? p.hints : [])
          .map(h => {
            if (!h || typeof h !== "object") return "";
            if (h.message) return String(h.message).trim();
            const parts = [];
            if (h.provider) parts.push(String(h.provider).toUpperCase());
            if (Array.isArray(h.reasons) && h.reasons.length) parts.push(h.reasons.map(String).join(", "));
            else if (h.reason) parts.push(String(h.reason));
            else if (h.kind) parts.push(String(h.kind));
            return parts.join(": ").trim();
          })
          .filter(Boolean);
        // Provider specific hints will be shown first
        const reasons = [...hintMsgs, ...msgs];
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

      const unresolvedTotal = unresolvedIssueCount();
      const parts = [`Issues: ${keep.length}`];
      if (per.history) parts.push(`H:${per.history}`);
      if (per.watchlist) parts.push(`W:${per.watchlist}`);
      if (per.ratings) parts.push(`R:${per.ratings}`);
      if (per.progress) parts.push(`P:${per.progress}`);
      if (unresolvedTotal) parts.push(`U:${unresolvedTotal}`);
      setStatusCount(issuesCount, "Issues", keep.length, parts.slice(1).join(" · ") || "Unique items needing attention");
      setStatusCount(systemCount, "System", systemFindings().length);
      const blockedTotal = BLOCKED_FINDINGS.reduce((acc, p) => {
        if (p.type === "cw_state_blackbox_active") {
          const c = Number(p.count);
          const n = Number.isFinite(c) && c > 0
            ? c
            : (Array.isArray(p.affected_items) ? p.affected_items.length : 0) || 1;
          return acc + n;
        }
        return acc + 1;
      }, 0);
      setStatusCount(blockedCount, "Blocked", blockedTotal);

      if (keep.length) {
        const first = keep[0];
        SELECTED = tagOf(first.provider, first.feature, first.key);
      } else if (SELECTED && !ITEMS.some(r => tagOf(r.provider, r.feature, r.key) === SELECTED)) {
        SELECTED = null;
      }
      filter(search.value || "");
      setDetailView(DETAIL_VIEW);
      if (!silent) hideWait();

      if (SELECTED && UNSYNCED.has(SELECTED) && DETAIL_VIEW === "issues") {
        loadItemDetail(SELECTED).then(() => {
          if (SELECTED && DETAIL_VIEW === "issues" && !requestController.signal.aborted) {
            draw();
            renderIssueDetail(SELECTED);
          }
        });
      }
      loadSystemFindings();
    }

    btnRun.addEventListener("click", async e => {
      e.preventDefault();
      e.stopPropagation();
      if (btnRun.disabled) return;
      const label = Q("span:last-child", btnRun);
      const prev = label?.textContent || "Analyze";
      btnRun.disabled = true;
      btnRun.classList.add("busy");
      if (label) label.textContent = "Analyzing";
      try {
        await analyze(false);
      } finally {
        btnRun.disabled = false;
        btnRun.classList.remove("busy");
        if (label) label.textContent = prev;
      }
    });

    btnToggleIDs.onclick = () => {
      SHOW_IDS = !SHOW_IDS;
      const label = SHOW_IDS ? "Hide IDs" : "Show IDs";
      btnToggleIDs.title = label;
      btnToggleIDs.setAttribute("aria-label", label);
      btnToggleIDs.setAttribute("aria-pressed", String(SHOW_IDS));
      grid.classList.toggle("show-ids", SHOW_IDS);
    };
    const selectScope = async nextScope => {
      if (nextScope === SCOPE) return;
      SCOPE = nextScope;
      btnScopeIssues?.classList.toggle("active", SCOPE === "issues");
      btnScopeAll?.classList.toggle("active", SCOPE === "all");
      btnScopeIssues?.setAttribute("aria-pressed", String(SCOPE === "issues"));
      btnScopeAll?.setAttribute("aria-pressed", String(SCOPE === "all"));
      if (SCOPE === "all") await loadAllPage(0);
      else {
        showIssuePage(0);
        ALL_OFFSET = 0;
      }
    };
    btnScopeIssues.onclick = () => selectScope("issues");
    btnScopeAll.onclick = () => selectScope("all");
    search.addEventListener("input", e => filter(e.target.value));
    btnClose.addEventListener("click", () => {
      cleanup();
      if (window.cxCloseModal) window.cxCloseModal();
    });

    Q("#an-wait-retry", root)?.addEventListener("click", () => {
      try { requestController.abort("retry"); } catch {}
      requestController = new AbortController();
      analyzePromise = null;
      load();
    });

    await load();
  },
  unmount() {
    activeCleanup?.();
  }
};
