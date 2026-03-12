/* snapshots.js - Provider snapshots (watchlist/ratings/history/progress) */
/* Refactored */
/* CrossWatch - Snapshots page UI logic */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {

  const css = `#page-snapshots{--ss-shell:linear-gradient(180deg,rgba(5,6,10,.995),rgba(1,2,5,.99));--ss-panel:linear-gradient(180deg,rgba(11,12,18,.94),rgba(3,4,8,.98));--ss-panel-strong:linear-gradient(180deg,rgba(9,10,16,.97),rgba(2,3,7,.995));--ss-border:rgba(255,255,255,.09);--ss-fg:rgba(244,247,255,.97);--ss-muted-fg:rgba(197,206,224,.72);--ss-shadow:0 18px 52px rgba(0,0,0,.36),inset 0 1px 0 rgba(255,255,255,.04);--ss-accent:rgba(92,96,182,.62);--ss-accent-soft:rgba(92,96,182,.10);--ss-accent-rose:rgba(92,96,182,.04)}#page-snapshots .ss-top{display:flex;align-items:flex-start;justify-content:space-between;gap:14px;flex-wrap:wrap;margin-bottom:14px;padding:16px 18px;border:1px solid var(--ss-border);border-radius:24px;background:radial-gradient(120% 140% at 0% 0%,rgba(86,90,180,.11),transparent 38%),radial-gradient(90% 120% at 100% 100%,rgba(56,64,132,.06),transparent 48%),var(--ss-shell);box-shadow:var(--ss-shadow);backdrop-filter:blur(16px) saturate(130%);-webkit-backdrop-filter:blur(16px) saturate(130%)}#page-snapshots .ss-top-copy{display:grid;gap:8px;min-width:0}#page-snapshots .ss-kicker{display:inline-flex;align-items:center;width:max-content;max-width:100%;padding:4px 10px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.05);color:var(--ss-muted-fg);font-size:11px;font-weight:800;letter-spacing:.12em;text-transform:uppercase}#page-snapshots .ss-title{font-weight:900;font-size:26px;letter-spacing:-.02em;line-height:1.02;color:var(--ss-fg)}#page-snapshots .ss-sub{color:var(--ss-muted-fg);font-size:13px;line-height:1.45;max-width:76ch}#page-snapshots .ss-actions,#page-snapshots .ss-topstats{display:flex;gap:8px;flex-wrap:wrap;align-items:center}#page-snapshots .ss-topstats{margin-left:auto;justify-content:flex-end}#page-snapshots .ss-topstat{display:inline-flex;align-items:center;gap:8px;min-height:38px;padding:0 12px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.025));color:#f5f7ff;box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}#page-snapshots .ss-topstat strong{font-size:15px;font-weight:900}#page-snapshots .ss-topstat span{font-size:12px;color:var(--ss-muted-fg);font-weight:700}#page-snapshots .ss-topstat[data-stat="captures"]{background:linear-gradient(180deg,rgba(88,94,170,.14),rgba(255,255,255,.025));border-color:rgba(102,108,188,.18)}#page-snapshots .ss-wrap{display:grid;grid-template-columns:360px minmax(0,1fr) 390px;gap:14px;align-items:start}#page-snapshots .ss-col{display:flex;flex-direction:column;gap:12px}#page-snapshots .ss-card{position:relative;padding:14px;border-radius:22px;border:1px solid var(--ss-border);background:radial-gradient(120% 120% at 0% 0%,rgba(86,90,180,.07),transparent 38%),radial-gradient(90% 110% at 100% 100%,rgba(44,52,108,.04),transparent 50%),var(--ss-panel);box-shadow:var(--ss-shadow);overflow:hidden}#page-snapshots .ss-card::before{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(135deg,rgba(255,255,255,.04),transparent 50%)}#page-snapshots .ss-card>*{position:relative;z-index:1}#page-snapshots .ss-card.ss-overflow{overflow:visible;z-index:6}#page-snapshots .ss-card h3{margin:0;font-size:12px;letter-spacing:.13em;text-transform:uppercase;color:rgba(225,232,246,.72)}#page-snapshots .ss-card.ss-accent{background:radial-gradient(120% 130% at 0% 0%,rgba(92,96,182,.12),transparent 36%),radial-gradient(80% 100% at 100% 100%,rgba(50,58,118,.05),transparent 46%),var(--ss-panel-strong)}#page-snapshots .ss-card-head{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;margin-bottom:12px}#page-snapshots .ss-headcopy{display:grid;gap:5px;min-width:0}#page-snapshots .ss-headtitle{font-size:18px;font-weight:850;letter-spacing:-.02em;color:var(--ss-fg)}#page-snapshots .ss-headsub,#page-snapshots .ss-note,#page-snapshots .ss-muted{color:var(--ss-muted-fg)}#page-snapshots .ss-note,#page-snapshots .ss-small{font-size:12px;line-height:1.45}#page-snapshots .ss-row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}#page-snapshots .ss-row>*{flex:0 0 auto}#page-snapshots .ss-row .grow{flex:1 1 auto;min-width:180px}#page-snapshots .ss-grid2{display:grid;grid-template-columns:1fr 1fr;gap:10px}#page-snapshots .ss-hero-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}#page-snapshots .ss-hero-stat{padding:12px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(10,11,18,.72),rgba(3,4,8,.86));box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}#page-snapshots .ss-hero-stat .v{font-size:20px;font-weight:900;color:#f7f9ff;line-height:1}#page-snapshots .ss-hero-stat .k{margin-top:6px;font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:var(--ss-muted-fg);font-weight:800}#page-snapshots .ss-pill{display:inline-flex;align-items:center;gap:6px;min-height:28px;padding:0 10px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.05);font-size:12px;color:#eef3ff}#page-snapshots .ss-pill strong{font-weight:900}#page-snapshots .ss-hr{height:1px;background:rgba(255,255,255,.07);margin:12px 0}#page-snapshots #ss-refresh.iconbtn{width:38px;height:38px;padding:0;display:inline-flex;align-items:center;justify-content:center;border-radius:14px;border:1px solid rgba(255,255,255,.10);background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.03))}#page-snapshots #ss-refresh-icon{font-size:20px;line-height:1}#page-snapshots .ss-refresh-icon.ss-spin{animation:ssrot .8s linear infinite}@keyframes ssrot{to{transform:rotate(360deg)}}#page-snapshots .ss-progress{display:flex;align-items:center;gap:10px;margin-top:12px}#page-snapshots .ss-progress.hidden{display:none}#page-snapshots .ss-pbar{position:relative;flex:1 1 auto;height:8px;border-radius:999px;background:rgba(255,255,255,.08);overflow:hidden}#page-snapshots .ss-pbar::before{content:"";position:absolute;inset:0;width:40%;transform:translateX(-60%);background:linear-gradient(90deg,transparent,var(--pcol,var(--accent)),transparent);animation:ssprog 1.05s ease-in-out infinite}@keyframes ssprog{0%{transform:translateX(-60%)}100%{transform:translateX(220%)}}#page-snapshots .ss-plabel{flex:0 0 auto;font-size:12px;color:var(--ss-muted-fg);white-space:nowrap}#page-snapshots button:disabled{opacity:.42;cursor:not-allowed;filter:saturate(.55)}#page-snapshots .ss-field{position:relative;display:flex;align-items:center;gap:10px;padding:0 12px;min-height:42px;border-radius:14px;border:1px solid rgba(255,255,255,.09);background:linear-gradient(180deg,rgba(8,10,18,.82),rgba(7,8,15,.92));box-shadow:inset 0 1px 0 rgba(255,255,255,.02)}#page-snapshots .ss-field.ss-open{z-index:34}#page-snapshots .ss-field .material-symbol,#page-snapshots .ss-field .chev{opacity:.72}#page-snapshots .ss-field select,#page-snapshots .ss-field input{flex:1 1 auto;min-width:0;height:40px;background:transparent;border:0;outline:0;color:inherit;font:inherit}#page-snapshots .ss-field select{appearance:none;color-scheme:dark}#page-snapshots .ss-field select option{background:#141418;color:#f3f3f5}#page-snapshots .ss-field select option:disabled{color:#7b7b86}#page-snapshots .ss-native{display:none!important}#page-snapshots .ss-bsel{position:relative;flex:1 1 auto;min-width:0}#page-snapshots .ss-bsel.is-open .ss-bsel-btn{color:#f7f9ff}#page-snapshots .ss-bsel-btn{width:100%;display:flex;align-items:center;gap:10px;background:transparent;border:0;outline:0;color:inherit;font:inherit;cursor:pointer;padding:0;text-align:left}#page-snapshots .ss-bsel-label{flex:1 1 auto;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;text-align:left}#page-snapshots .ss-bsel-chev{opacity:.6;flex:0 0 auto}#page-snapshots .ss-bsel-menu{position:absolute;left:-12px;right:-12px;top:calc(100% + 10px);z-index:80;border:1px solid rgba(255,255,255,.10);border-radius:16px;background:linear-gradient(180deg,rgba(255,255,255,.025),transparent),linear-gradient(180deg,rgba(9,10,16,.99),rgba(3,4,8,.995));box-shadow:0 14px 40px rgba(0,0,0,.58);padding:6px;max-height:320px;overflow:auto;pointer-events:auto}#page-snapshots .ss-bsel-menu.hidden{display:none}#page-snapshots .ss-bsel-item{width:100%;display:flex;align-items:center;gap:10px;padding:10px;border-radius:12px;border:1px solid transparent;background:transparent;color:inherit;cursor:pointer;text-align:left}#page-snapshots .ss-bsel-item:hover{background:rgba(255,255,255,.04);border-color:rgba(255,255,255,.10)}#page-snapshots .ss-bsel-item:disabled{opacity:.45;cursor:not-allowed}#page-snapshots .ss-provico{width:18px;height:18px;flex:0 0 18px;border-radius:7px;border:1px solid rgba(255,255,255,.16);background:rgba(0,0,0,.18);background-image:var(--wm);background-repeat:no-repeat;background-position:center;background-size:contain;filter:grayscale(.05) brightness(1.12);opacity:.95}#page-snapshots .ss-bsel-menu .ss-provico{width:20px;height:20px;flex-basis:20px}#page-snapshots .ss-provico.empty{background-image:none;background:rgba(255,255,255,.05)}#page-snapshots .ss-comparehint{display:flex;align-items:flex-start;gap:10px;padding:11px 12px;border-radius:16px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.02));font-size:12px;color:var(--ss-muted-fg);margin:12px 0}#page-snapshots .ss-comparehint .material-symbol{font-size:18px;opacity:.9;color:#eef3ff}#page-snapshots .ss-list{display:flex;flex-direction:column;gap:10px;max-height:620px;overflow:auto;padding:2px 2px 2px 0}#page-snapshots .ss-item{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:12px;align-items:center;cursor:pointer;padding:12px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.02));transition:transform .12s ease,border-color .14s ease,background .14s ease,box-shadow .14s ease}#page-snapshots .ss-item:hover{transform:translateY(-1px);border-color:rgba(255,255,255,.14);background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.03))}#page-snapshots .ss-item.active{border-color:rgba(92,96,182,.34);background:linear-gradient(180deg,rgba(92,96,182,.06),rgba(255,255,255,.02));box-shadow:0 0 0 1px rgba(92,96,182,.16),0 14px 28px rgba(0,0,0,.24)}#page-snapshots .ss-item.child{margin-left:16px;background:rgba(255,255,255,.02)}#page-snapshots .ss-item-main{min-width:0;display:grid;gap:8px}#page-snapshots .ss-item-top{display:flex;align-items:center;justify-content:space-between;gap:12px}#page-snapshots .ss-item-title{font-weight:850;color:#f6f8ff;letter-spacing:-.01em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}#page-snapshots .ss-item-meta{display:flex;gap:6px;flex-wrap:wrap;align-items:center}#page-snapshots .ss-item .d{font-size:12px;color:var(--ss-muted-fg);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}#page-snapshots .ss-path{opacity:.52}#page-snapshots .ss-badge{display:inline-flex;align-items:center;min-height:24px;padding:0 8px;border-radius:999px;border:1px solid rgba(255,255,255,.11);background:rgba(255,255,255,.04);font-size:11px;letter-spacing:.05em;text-transform:uppercase;color:#eef3ff}#page-snapshots .ss-badge.ok{border-color:rgba(91,226,173,.24)}#page-snapshots .ss-badge.warn{border-color:rgba(255,181,92,.24)}#page-snapshots .ss-badge.add{border-color:rgba(48,255,138,.35)}#page-snapshots .ss-badge.del{border-color:rgba(255,80,80,.35)}#page-snapshots .ss-badge.upd{border-color:rgba(255,180,80,.35)}#page-snapshots .ss-mini{display:inline-flex;align-items:center;justify-content:center;min-height:24px;padding:0 9px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.04);color:#eef3ff;font-size:11px;font-weight:800}#page-snapshots .ss-right{display:flex;align-items:center;gap:10px}#page-snapshots .ss-item-right{display:grid;gap:8px;justify-items:end}#page-snapshots .ss-item-action{display:inline-flex;align-items:center;gap:8px}#page-snapshots .ss-chk{width:18px;height:18px;accent-color:#6f6cff}#page-snapshots .ss-ab{display:inline-flex;align-items:center;justify-content:center;min-width:22px;height:22px;border-radius:999px;border:1px solid rgba(255,255,255,.14);font-size:11px;font-weight:900;letter-spacing:.03em;color:#f4f7ff}#page-snapshots .ss-ab.a{border-color:rgba(92,96,182,.30);background:rgba(92,96,182,.08)}#page-snapshots .ss-ab.b{border-color:rgba(255,180,80,.38);background:rgba(255,180,80,.08)}#page-snapshots .ss-item .chev{opacity:.5;font-size:20px;line-height:1}#page-snapshots .ss-empty{padding:24px;border-radius:18px;border:1px dashed rgba(255,255,255,.14);text-align:center;color:var(--ss-muted-fg);background:rgba(255,255,255,.02)}#page-snapshots .ss-picked{display:grid;grid-template-columns:1fr 1fr;gap:10px}#page-snapshots .ss-pick-card{padding:12px;border-radius:18px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.08);min-width:0;cursor:grab;user-select:none}#page-snapshots .ss-pick-date{font-weight:900;font-size:16px}#page-snapshots .ss-pick-meta{margin-top:6px;font-size:12px;color:var(--ss-muted-fg)}#page-snapshots .ss-pick-card.dragging{opacity:.65}#page-snapshots [data-coll-body="compare"]{overflow-x:hidden}#page-snapshots .ss-difflist{display:flex;flex-direction:column;gap:10px;max-height:360px;overflow:auto;padding:3px 2px 3px 0}#page-snapshots .ss-diffitem,#page-snapshots .ss-diffrow{padding:12px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.03)}#page-snapshots .ss-diffhead{display:flex;align-items:center;gap:8px;flex-wrap:wrap}#page-snapshots .ss-difftitle{flex:1 1 auto;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:700}#page-snapshots .ss-diffkey,#page-snapshots .ss-code{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace;font-size:11px}#page-snapshots .ss-diffkey{opacity:.72;margin-top:6px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}#page-snapshots .ss-code{white-space:pre-wrap;word-break:break-word;line-height:1.35;padding:10px;border-radius:14px;border:1px solid rgba(255,255,255,.08);background:rgba(0,0,0,.20);margin-top:8px}#page-snapshots .ss-diff-summary{display:flex;flex-wrap:wrap;gap:8px;align-items:center}#page-snapshots .ss-coll-head{display:flex;align-items:center;gap:10px;cursor:pointer}#page-snapshots .ss-coll-head:focus{outline:2px solid rgba(255,255,255,.18);outline-offset:4px;border-radius:14px}#page-snapshots .ss-coll-ico{margin-left:auto;opacity:.7;transition:transform .12s ease}#page-snapshots .ss-card.is-collapsed .ss-coll-ico{transform:rotate(-90deg)}#page-snapshots .ss-coll-body{margin-top:12px}#page-snapshots .ss-selected-card{padding:12px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(9,10,16,.78),rgba(3,4,8,.88))}#page-snapshots .ss-selected-empty{display:grid;gap:8px;justify-items:start}#page-snapshots .ss-selected-title{font-weight:850;font-size:15px;color:#f4f7ff}@media (max-width:1280px){#page-snapshots .ss-wrap{grid-template-columns:minmax(0,1fr) minmax(0,1fr)}#page-snapshots .ss-col{grid-column:1 / -1}}@media (max-width:900px){#page-snapshots .ss-wrap{grid-template-columns:1fr}#page-snapshots .ss-top{padding:14px}#page-snapshots .ss-topstats{width:100%;justify-content:flex-start}#page-snapshots .ss-hero-grid{grid-template-columns:1fr 1fr}}@media (max-width:640px){#page-snapshots .ss-grid2,#page-snapshots .ss-picked,#page-snapshots .ss-hero-grid{grid-template-columns:1fr}#page-snapshots .ss-item{grid-template-columns:1fr}#page-snapshots .ss-item-right{justify-items:start}#page-snapshots .ss-item.child{margin-left:10px}}`;
  const cssTuning = `#page-snapshots .ss-wrap{grid-template-columns:320px minmax(0,1fr) 340px}#page-snapshots .ss-toolbar{display:grid;gap:10px;margin-bottom:12px;position:relative;z-index:9}#page-snapshots .ss-list-head{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;flex-wrap:wrap}#page-snapshots .ss-list-head .ss-headsub{max-width:60ch}#page-snapshots .ss-inline-pills{display:flex;gap:8px;flex-wrap:wrap}#page-snapshots .ss-inline-pills .ss-pill{min-height:26px;padding:0 9px;font-size:11px}#page-snapshots .ss-steps{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:10px}#page-snapshots .ss-step{padding:10px 12px;border-radius:16px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(10,11,18,.68),rgba(4,5,9,.88))}#page-snapshots .ss-step-num{font-size:11px;font-weight:900;letter-spacing:.08em;color:#f2f5ff;text-transform:uppercase}#page-snapshots .ss-step-label{margin-top:4px;font-size:12px;color:var(--ss-muted-fg);font-weight:800}#page-snapshots .ss-comparehint{align-items:center;padding:10px 12px;border-radius:14px;margin:0;background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.018))}#page-snapshots .ss-card.ss-overflow{z-index:40}#page-snapshots .ss-field.ss-open,#page-snapshots .ss-bsel.is-open{z-index:120}#page-snapshots .ss-bsel-menu{z-index:160;background:linear-gradient(180deg,rgba(14,17,28,.99),rgba(7,10,18,.995))!important;box-shadow:0 24px 44px rgba(0,0,0,.52)!important;backdrop-filter:blur(14px) saturate(125%);-webkit-backdrop-filter:blur(14px) saturate(125%)}#page-snapshots .ss-list{gap:8px;max-height:640px}#page-snapshots .ss-list,#page-snapshots .ss-difflist{scrollbar-width:thin;scrollbar-color:#8b5cf6 #10131a}#page-snapshots .ss-list::-webkit-scrollbar,#page-snapshots .ss-difflist::-webkit-scrollbar{width:8px;height:8px}#page-snapshots .ss-list::-webkit-scrollbar-corner,#page-snapshots .ss-difflist::-webkit-scrollbar-corner{background:transparent}#page-snapshots .ss-list::-webkit-scrollbar-track,#page-snapshots .ss-difflist::-webkit-scrollbar-track{background:rgba(255,255,255,.04);border-radius:12px;box-shadow:inset 0 0 0 1px rgba(255,255,255,.08)}#page-snapshots .ss-list::-webkit-scrollbar-thumb,#page-snapshots .ss-difflist::-webkit-scrollbar-thumb{border-radius:12px;background:linear-gradient(180deg,#8b5cf6 0%,#3b82f6 100%);border:2px solid #14161c;box-shadow:inset 0 0 0 1px rgba(139,92,246,.35),0 0 10px rgba(139,92,246,.55),0 0 18px rgba(59,130,246,.4)}#page-snapshots .ss-list::-webkit-scrollbar-thumb:hover,#page-snapshots .ss-difflist::-webkit-scrollbar-thumb:hover{background:linear-gradient(180deg,#a78bfa 0%,#60a5fa 100%);box-shadow:inset 0 0 0 1px rgba(139,92,246,.45),0 0 14px rgba(139,92,246,.7),0 0 26px rgba(59,130,246,.55)}#page-snapshots .ss-list::-webkit-scrollbar-thumb:active,#page-snapshots .ss-difflist::-webkit-scrollbar-thumb:active{background:linear-gradient(180deg,#c4b5fd 0%,#93c5fd 100%);box-shadow:inset 0 0 0 1px rgba(139,92,246,.55),0 0 10px rgba(139,92,246,.6),0 0 18px rgba(59,130,246,.5)}#page-snapshots .ss-item{border-color:rgba(255,255,255,.07);background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.018))}#page-snapshots .ss-item:hover{border-color:rgba(255,255,255,.12);background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.026))}#page-snapshots .ss-item.active{border-color:rgba(92,96,182,.30);box-shadow:0 0 0 1px rgba(92,96,182,.12),0 12px 24px rgba(0,0,0,.22)}#page-snapshots .ss-item-main{gap:7px}#page-snapshots .ss-file{font-weight:700;color:rgba(236,240,255,.82)}#page-snapshots .ss-path{opacity:.46}#page-snapshots .ss-item .chev{opacity:.45}#page-snapshots .ss-card[data-coll="restore"] .ss-inline-pills{margin-top:12px}@media (max-width:900px){#page-snapshots .ss-steps{grid-template-columns:1fr 1fr}}@media (max-width:640px){#page-snapshots .ss-steps{grid-template-columns:1fr}}`;

  function injectCss() {
    if (document.getElementById("cw-snapshots-css")) return;
    const s = document.createElement("style");
    s.id = "cw-snapshots-css";
    s.textContent = css + cssTuning;
    document.head.appendChild(s);
  }

  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

function escapeHtml(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

  function _uiCaptureLabel(label) {
    const t = String(label || "").trim();
    if (!t) return "";
    const low = t.toLowerCase();
    if (low === "snapshot" || low === "snapshots" || low === "capture" || low === "captures") return "CAPTURE";
    return t;
  }

  const API = () => (window.CW && window.CW.API && window.CW.API.j) ? window.CW.API.j : async (u, opt) => {
    const r = await fetch(u, { cache: "no-store", ...(opt || {}) });
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  };

    function apiJson(url, opt = {}, timeoutMs = 180000) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    return fetch(url, { cache: "no-store", signal: ctrl.signal, ...(opt || {}) })
      .then(async (r) => {
        clearTimeout(t);
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
        return r.json();
      })
      .catch((e) => {
        clearTimeout(t);
        if (e && e.name === "AbortError") throw new Error("timeout");
        throw e;
      });
  }

const toast = (msg, ok = true) => {
    try { window.CW?.DOM?.showToast?.(msg, ok); } catch {}
    if (!window.CW?.DOM?.showToast) console.log(msg);
  };
  const POST_JSON = (url, body, timeoutMs) => apiJson(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) }, timeoutMs);
  function diffSelection() {
    const pick = _diffPickAB();
    const pa = String(pick.sa?.provider || "").toLowerCase();
    const pb = String(pick.sb?.provider || "").toLowerCase();
    const ia = String(pick.sa?.instance || pick.sa?.instance_id || pick.sa?.profile || "default").toLowerCase();
    const ib = String(pick.sb?.instance || pick.sb?.instance_id || pick.sb?.profile || "default").toLowerCase();
    const fa = String(pick.sa?.feature || "").toLowerCase();
    const fb = String(pick.sb?.feature || "").toLowerCase();
    const sameFeature = fa && fb && fa === fb;
    const sameBundle = fa === "all" && fb === "all";
    pick.ok = !!pick.a && !!pick.b && pick.a !== pick.b && !!pick.sa && !!pick.sb && pa === pb && ia === ib && (sameFeature || sameBundle);
    return pick;
  }

  const state = {
    providers: [], snapshots: [], selectedPath: "", selectedSnap: null, diffPick: [], diffResult: null,
    diffKind: "all", diffQ: "", diffLimit: 200, diffExpanded: {}, busy: false, lastRefresh: 0,
    listLimit: 5, showAll: false, expandedBundles: {}, _spinUntil: 0,
  };

  function _provBrand(pid) {
    const v = String(pid || "").trim().toLowerCase().replace(/[^a-z0-9_-]/g, "");
    return v ? ("brand-" + v) : "";
  }

  function _setBrandMenuState(menu, open) {
    if (!menu) return;
    menu.classList.toggle("hidden", !open);
    menu.closest(".ss-bsel")?.classList.toggle("is-open", !!open);
    menu.closest(".ss-field")?.classList.toggle("ss-open", !!open);
    menu.closest(".ss-card")?.classList.toggle("ss-overflow", !!open);
  }

  function _closeAllBrandMenus(exceptMenu) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    $$(".ss-bsel-menu", page).forEach((m) => {
      if (exceptMenu && m === exceptMenu) return;
      _setBrandMenuState(m, false);
    });
  }

  function _ensureBrandSelect(sel) {
    if (!sel || !sel.id) return null;
    const parent = sel.parentElement;
    if (!parent) return null;
    const noIcon = String(sel?.dataset?.bselNoicon || "").trim() === "1" || String(sel?.dataset?.bselIcon || "").trim() === "0";

    let wrap = parent.querySelector(`.ss-bsel[data-for="${sel.id}"]`);
    if (!wrap) {
      wrap = document.createElement("div");
      wrap.className = "ss-bsel";
      wrap.dataset.for = sel.id;

      // Keep only layout classes
      const keep = String(sel.className || "").split(/\s+/).filter((c) => c === "grow").join(" ");
      if (keep) wrap.className += " " + keep;

      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "ss-bsel-btn";

      const ico = noIcon ? null : document.createElement("span");
      if (ico) ico.className = "ss-provico empty";

      const label = document.createElement("span");
      label.className = "ss-bsel-label";
      label.textContent = "-";

      const chev = document.createElement("span");
      chev.className = "ss-bsel-chev";
      chev.textContent = "v";

      if (ico) btn.appendChild(ico);
      btn.appendChild(label);
      btn.appendChild(chev);

      const menu = document.createElement("div");
      menu.className = "ss-bsel-menu hidden";

      wrap.appendChild(btn);
      wrap.appendChild(menu);

      // Hide native select
      sel.classList.add("ss-native");

      parent.insertBefore(wrap, sel.nextSibling);

      btn.addEventListener("click", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        const shouldOpen = menu.classList.contains("hidden");
        _closeAllBrandMenus(menu);
        _setBrandMenuState(menu, shouldOpen);
      });

      if (!state._brandSelectDocBound) {
        state._brandSelectDocBound = true;
        document.addEventListener("click", () => _closeAllBrandMenus(null));
        document.addEventListener("keydown", (ev) => {
          if (ev.key === "Escape") _closeAllBrandMenus(null);
        });
      }

      sel.addEventListener("change", () => _syncBrandSelectFromNative(sel));
    }

    return wrap;
  }

  function _syncBrandSelectFromNative(sel) {
    const wrap = _ensureBrandSelect(sel);
    if (!wrap) return;
    const btn = wrap.querySelector(".ss-bsel-btn");
    const ico = wrap.querySelector(".ss-provico");
    const lab = wrap.querySelector(".ss-bsel-label");
    if (!btn || !lab) return;

    const opt = sel.options && sel.selectedIndex >= 0 ? sel.options[sel.selectedIndex] : null;
    const value = opt ? String(opt.value || "") : "";
    const text = opt ? String(opt.textContent || "") : "";

    if (ico) {
      const brand = _provBrand(value);
      ico.className = "ss-provico " + (brand ? ("prov-card " + brand) : "empty");
    }
    lab.textContent = text || "-";
  }

  function _rebuildBrandSelectMenu(sel) {
    const wrap = _ensureBrandSelect(sel);
    if (!wrap) return;
    const menu = wrap.querySelector(".ss-bsel-menu");
    if (!menu) return;
    const noIcon = String(sel?.dataset?.bselNoicon || "").trim() === "1" || String(sel?.dataset?.bselIcon || "").trim() === "0";

    menu.innerHTML = "";
    Array.from(sel.options || []).forEach((opt) => {
      const b = document.createElement("button");
      b.type = "button";
      b.className = "ss-bsel-item";
      b.disabled = !!opt.disabled;

      const value = String(opt.value || "");

      const ico = noIcon ? null : document.createElement("span");
      if (ico) {
        const brand = _provBrand(value);
        ico.className = "ss-provico " + (brand ? ("prov-card " + brand) : "empty");
      }

      const lab = document.createElement("span");
      lab.className = "ss-bsel-label";
      lab.textContent = String(opt.textContent || "-");

      if (ico) b.appendChild(ico);
      b.appendChild(lab);

      b.addEventListener("click", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        sel.value = value;
        sel.dispatchEvent(new Event("change", { bubbles: true }));
        _setBrandMenuState(menu, false);
      });

      menu.appendChild(b);
    });

    _syncBrandSelectFromNative(sel);
  }

  function fmtTsFromStamp(stamp) {
    // stamp: 20260127T135959Z
    const m = String(stamp || "").match(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/);
    if (!m) return "";
    const d = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]));
    return d.toLocaleString();
  }

  function _findSnapByPath(path) {
  const rows = Array.isArray(state.snapshots) ? state.snapshots : [];
  const p = String(path || "");
  if (!p) return null;
  for (const s of rows) {
    if (s && String(s.path || "") === p) return s;
  }
  return null;
}

function _stampEpoch(stamp) {
  const m = String(stamp || "").match(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/);
  if (!m) return 0;
  return Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]);
}

function _diffScope() {
  const picks = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];
  if (!picks.length) return null;
  const s0 = _findSnapByPath(String(picks[0] || ""));
  if (!s0) return null;
  return {
    provider: String(s0.provider || "").toLowerCase(),
    instance: String(s0.instance || s0.instance_id || s0.profile || "default").toLowerCase(),
    feature: String(s0.feature || "").toLowerCase(),
  };
}

function _snapMatchesScope(s, scope) {
  if (!s || !scope) return true;
  const p = String(s.provider || "").toLowerCase();
  const i = String(s.instance || s.instance_id || s.profile || "default").toLowerCase();
  const f = String(s.feature || "").toLowerCase();
  return p === scope.provider && i === scope.instance && f === scope.feature;
}

function _diffPickAB() {
  const picks = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];
  if (picks.length !== 2) return { a: "", b: "", sa: null, sb: null };
  const p0 = String(picks[0] || "");
  const p1 = String(picks[1] || "");
  const s0 = _findSnapByPath(p0);
  const s1 = _findSnapByPath(p1);

  // Keep explicit UI selection order stable: first checked/dragged card is A, second is B.
  return { a: p0, b: p1, sa: s0, sb: s1 };
}

function clearDiffPicks() {
  state.diffPick = [];
  state.diffResult = null;
  try { renderList(); renderDiffPicked(); renderDiff(); updateDiffAvailability(); } catch {}
}

function toggleDiffPick(path, checked) {
  const p = String(path || "");
  if (!p) return;

  const snap = _findSnapByPath(p);
  const picks = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];
  const scope = _diffScope();

  if (checked) {
    if (!snap) return;
    if (scope && !_snapMatchesScope(snap, scope)) return;
    if (!picks.includes(p)) {
      if (picks.length >= 2) picks.shift();
      picks.push(p);
    }
  } else {
    const ix = picks.indexOf(p);
    if (ix !== -1) picks.splice(ix, 1);
  }

  state.diffPick = picks;

  try {
    if (checked) {
      setCollapsed("restore", true);
      setCollapsed("compare", false);
    } else {
      setCollapsed("compare", true);
      setCollapsed("restore", false);
    }
  } catch {}

  if (picks.length < 2) state.diffResult = null;

  renderList();
  renderDiffPicked();
  renderDiff();
  updateDiffAvailability();
}

function bundleKey(s) {
    const stamp = String((s && s.stamp) || "");
    const prov = String((s && s.provider) || "").toLowerCase();
    const inst = String((s && (s.instance || s.instance_id || s.profile)) || "default").toLowerCase();
    const label = String((s && s.label) || "").toLowerCase();
    return stamp + "|" + prov + "|" + inst + "|" + label;
  }

  function buildBundleIndex(allRows) {
    const bundlesByKey = {};
    const childrenByKey = {};
    (allRows || []).forEach((s) => {
      const feat = String((s && s.feature) || "").toLowerCase();
      if (feat !== "all") return;
      const k = bundleKey(s);
      if (k) bundlesByKey[k] = s;
    });
    (allRows || []).forEach((s) => {
      const feat = String((s && s.feature) || "").toLowerCase();
      if (feat === "all") return;
      const k = bundleKey(s);
      if (!k || !bundlesByKey[k]) return;
      if (!childrenByKey[k]) childrenByKey[k] = [];
      childrenByKey[k].push(s);
    });
    return { bundlesByKey, childrenByKey };
  }

  function humanBytes(n) {
    const v = Number(n || 0);
    if (!isFinite(v) || v <= 0) return "0 B";
    const u = ["B", "KB", "MB", "GB"];
    let i = 0, x = v;
    while (x >= 1024 && i < u.length - 1) { x /= 1024; i++; }
    return `${x.toFixed(i === 0 ? 0 : 1)} ${u[i]}`;
  }

  function snapFile(path) {
    return String(path || "").split(/[\\/]/).pop() || "";
  }

  function snapshotOverview() {
    const snaps = Array.isArray(state.snapshots) ? state.snapshots.filter(Boolean) : [];
    const idx = buildBundleIndex(snaps);
    const hiddenChildPaths = new Set();

    Object.values(idx.childrenByKey || {}).forEach((kids) => {
      (kids || []).forEach((s) => {
        if (s && s.path) hiddenChildPaths.add(String(s.path));
      });
    });

    const configuredProviders = (Array.isArray(state.providers) ? state.providers : []).filter((p) => p && p.configured !== false);
    const providerIds = new Set(
      (configuredProviders.length ? configuredProviders : (Array.isArray(state.providers) ? state.providers : []))
        .map((p) => String(p?.id || p?.label || "").trim().toUpperCase())
        .filter(Boolean)
    );

    if (!providerIds.size) {
      snaps.forEach((s) => {
        const prov = String(s?.provider || "").trim().toUpperCase();
        if (prov) providerIds.add(prov);
      });
    }

    const total = snaps.filter((s) => s && !hiddenChildPaths.has(String(s.path || ""))).length;
    const fullSets = Object.keys(idx.bundlesByKey || {}).length;
    return { total, providers: providerIds.size, fullSets };
  }

  function updateTopStats() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const overview = snapshotOverview();
    const setStat = (name, value) => {
      const el = page.querySelector(`[data-stat="${name}"] strong`);
      if (el) el.textContent = String(value);
    };
    setStat("captures", overview.total);
    setStat("providers", overview.providers);
    setStat("full-sets", overview.fullSets);
  }

  function render() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const overview = snapshotOverview();
    page.innerHTML = `<div class="ss-top"><div class="ss-top-copy"><div class="ss-title">Captures</div><div class="ss-sub">Create point-in-time provider captures, restore them, and compare changes</div></div><div class="ss-topstats"><div class="ss-topstat" data-stat="captures"><strong>${overview.total}</strong><span>captures</span></div><div class="ss-topstat" data-stat="providers"><strong>${overview.providers}</strong><span>providers</span></div><div class="ss-topstat" data-stat="full-sets" title="Provider-wide captures saved with all supported features"><strong>${overview.fullSets}</strong><span>full sets</span></div><div class="ss-actions"><button id="ss-refresh" class="iconbtn" title="Refresh" aria-label="Refresh"><span id="ss-refresh-icon" class="material-symbol ss-refresh-icon">sync</span></button></div></div></div><div class="ss-wrap"><div class="ss-card ss-accent"><div class="ss-card-head"><div class="ss-headcopy"><h3>Capture state</h3><div class="ss-headsub">Pick a source, choose what to export, add an optional label, and save a clean restore point.</div></div></div><div class="ss-hero-grid"><div class="ss-hero-stat"><div class="v">1</div><div class="k">Provider</div></div><div class="ss-hero-stat"><div class="v">2</div><div class="k">Feature</div></div><div class="ss-hero-stat"><div class="v">3</div><div class="k">Capture</div></div></div><div class="ss-field" style="margin-top:12px"><select id="ss-prov"></select></div><div class="ss-field" style="margin-top:10px"><select id="ss-prov-inst" class="input grow"></select></div><div class="ss-field" style="margin-top:10px"><select id="ss-feature"></select><span class="chev">v</span></div><div class="ss-field" style="margin-top:10px"><input id="ss-label" placeholder="Optional label for this capture" /></div><div class="ss-row" style="margin-top:12px"><button id="ss-create" class="btn primary" style="width:100%">Create capture</button></div><div id="ss-create-progress" class="ss-progress hidden"><div class="ss-pbar"></div><div class="ss-plabel">Working…</div></div></div><div class="ss-card"><div class="ss-card-head"><div class="ss-headcopy"><h3>Capture Browser</h3><div class="ss-headsub">Filter, click once to restore, or tick two matching captures to compare</div></div></div><div class="ss-row"><input id="ss-filter" class="input grow" placeholder="Search provider, feature, label or path..."/></div><div class="ss-row" style="margin-top:10px"><select id="ss-filter-provider" class="input grow"></select><select id="ss-filter-feature" class="input grow"></select></div><div class="ss-comparehint"><span class="material-symbol">compare_arrows</span><div><b>Quick flow</b><br>Click a capture to restore it, or tick two matching ones on the right to compare them side by side.</div></div><div id="ss-list" class="ss-list"></div><div id="ss-list-footer" class="ss-row" style="justify-content:space-between;margin-top:10px"></div></div><div class="ss-col"><div class="ss-card ss-coll" data-coll="restore"><div class="ss-coll-head" data-coll-head="restore" role="button" tabindex="0" aria-expanded="true"><h3>Restore</h3><span class="material-symbol ss-coll-ico">expand_more</span></div><div class="ss-coll-body" data-coll-body="restore"><div class="ss-card-head" style="margin-bottom:0"><div class="ss-headcopy"><div class="ss-headsub">Restore your capture</div></div></div><div id="ss-selected" class="ss-selected-card ss-selected-empty" style="margin-top:12px">Pick a capture from the list.</div><div class="ss-note" style="margin-top:12px"><b>Merge</b> adds missing items only. <b>Clear and restore</b> wipes the provider feature first, then restores the capture exactly.</div><div class="ss-row" style="margin-top:12px"><select id="ss-restore-inst" class="input grow"></select></div><div class="ss-row" style="margin-top:12px"><select id="ss-restore-mode" class="input grow"><option value="merge">Merge</option><option value="clear_restore">Clear and restore</option></select></div><div class="ss-row" style="margin-top:10px"><button id="ss-restore" class="btn danger" style="width:100%">Restore capture</button><button id="ss-delete" class="btn" style="width:100%">Delete capture</button></div><div id="ss-restore-progress" class="ss-progress hidden"><div class="ss-pbar"></div><div class="ss-plabel">Working…</div></div><div id="ss-restore-out" class="ss-small ss-muted" style="margin-top:10px"></div></div></div><div class="ss-card ss-coll is-collapsed" data-coll="compare"><div class="ss-coll-head" data-coll-head="compare" role="button" tabindex="0" aria-expanded="false"><h3>Compare</h3><span class="material-symbol ss-coll-ico">expand_more</span></div><div class="ss-coll-body hidden" data-coll-body="compare"><div class="ss-card-head" style="margin-bottom:0"><div class="ss-headcopy"><div class="ss-headtitle">Compare two captures</div><div class="ss-headsub">See what was added, removed, or changed before you commit to a restore.</div></div></div><div id="ss-diff-picked" class="ss-picked" style="margin-top:12px"></div><div class="ss-row" style="margin-top:10px"><select id="ss-diff-kind" class="input grow"><option value="all">All changes</option><option value="added">Added</option><option value="removed">Deleted</option><option value="updated">Updated</option></select><select id="ss-diff-limit" class="input" style="min-width:110px"><option value="100">100</option><option value="200" selected>200</option><option value="500">500</option><option value="1000">1000</option></select></div><div class="ss-row" style="margin-top:10px"><input id="ss-diff-q" class="input grow" placeholder="Filter compare results..."/></div><div class="ss-row" style="margin-top:10px"><button id="ss-diff-run" class="btn grow">Run compare</button><button id="ss-diff-extend" class="btn grow">Open advanced</button></div><div class="ss-small ss-muted" style="margin-top:8px">Advanced opens the full compare modal, including unchanged records.</div><div id="ss-diff-progress" class="ss-progress hidden"><div class="ss-pbar"></div><div class="ss-plabel">Working…</div></div><div id="ss-diff-out" class="ss-muted ss-small" style="margin-top:10px"></div><div id="ss-diff-list" class="ss-difflist" style="margin-top:10px"></div></div></div><div class="ss-card ss-coll is-collapsed" data-coll="tools"><div class="ss-coll-head" data-coll-head="tools" role="button" tabindex="0" aria-expanded="false"><h3>Tools</h3><span class="material-symbol ss-coll-ico">expand_more</span></div><div class="ss-coll-body hidden" data-coll-body="tools"><div class="ss-card-head" style="margin-bottom:0"><div class="ss-headcopy"><div class="ss-headtitle">Cleanup tools</div><div class="ss-headsub">Destructive actions live here, away from the normal flow. As they should.</div></div></div><div class="ss-row" style="margin-top:12px"><select id="ss-tools-prov" class="input grow"></select></div><div class="ss-row" style="margin-top:10px"><select id="ss-tools-inst" class="input grow"></select></div><div class="ss-grid2" style="margin-top:12px"><button class="btn danger" id="ss-clear-watchlist">Clear watchlist</button><button class="btn danger" id="ss-clear-ratings">Clear ratings</button><button class="btn danger" id="ss-clear-history">Clear history</button><button class="btn danger" id="ss-clear-progress">Clear progress</button><button class="btn danger" id="ss-clear-all">Clear all</button></div><div id="ss-tools-progress" class="ss-progress hidden"><div class="ss-pbar"></div><div class="ss-plabel">Working…</div></div><div class="ss-note" style="margin-top:10px">These actions are destructive. Double-check the target before you use them.</div><div id="ss-tools-out" class="ss-small ss-muted" style="margin-top:10px"></div></div></div></div></div>`;

    $(".ss-sub", page) && ($(".ss-sub", page).textContent = "Create point-in-time provider captures, restore them, and compare changes.");
    const createCard = $(".ss-card.ss-accent", page);
    if (createCard) {
      const h3 = $("h3", createCard);
      const sub = $(".ss-headsub", createCard);
      const hero = $(".ss-hero-grid", createCard);
      if (h3) h3.textContent = "Create capture";
      if (sub) sub.textContent = "Pick a source, choose a feature, then save a restore point. Keep the label optional.";
      if (hero) hero.outerHTML = `<div class="ss-steps"><div class="ss-step"><div class="ss-step-num">Step 1</div><div class="ss-step-label">Provider</div></div><div class="ss-step"><div class="ss-step-num">Step 2</div><div class="ss-step-label">Feature</div></div><div class="ss-step"><div class="ss-step-num">Step 3</div><div class="ss-step-label">Capture</div></div></div>`;
      const progLabel = $("#ss-create-progress .ss-plabel", createCard);
      if (progLabel) progLabel.textContent = "Working...";
    }
    const browserCard = $$(".ss-card", page)[1];
    if (browserCard) {
      const head = $(".ss-card-head", browserCard);
      const h3 = $("h3", browserCard);
      const sub = $(".ss-headsub", browserCard);
      const hint = $(".ss-comparehint", browserCard);
      if (h3) h3.textContent = "Capture browser";
      if (sub) sub.textContent = "Browse first. Click a row to prepare restore, or tick two matching captures to compare them.";
      head?.classList.add("ss-list-head");
      if (head && !$(".ss-inline-pills", head)) head.insertAdjacentHTML("beforeend", `<div class="ss-inline-pills"><span class="ss-pill"><strong>Click</strong> restore</span><span class="ss-pill"><strong>2 checks</strong> compare</span></div>`);
      if (hint) hint.innerHTML = `<span class="material-symbol">compare_arrows</span><div>Two checked captures must share the same provider and instance. Use two feature captures, or two full captures.</div>`;
      const filterRow = $("#ss-filter", browserCard)?.closest(".ss-row");
      const filterSelectRow = $("#ss-filter-provider", browserCard)?.closest(".ss-row");
      if (filterRow && filterSelectRow && hint) {
        const toolbar = document.createElement("div");
        toolbar.className = "ss-toolbar";
        browserCard.insertBefore(toolbar, filterRow);
        toolbar.appendChild(filterRow);
        toolbar.appendChild(filterSelectRow);
        toolbar.appendChild(hint);
      }
    }
    const restoreCard = $('[data-coll="restore"]', page);
    if (restoreCard) {
      const sub = $(".ss-headsub", restoreCard);
      const note = $(".ss-note", restoreCard);
      const progLabel = $("#ss-restore-progress .ss-plabel", restoreCard);
      if (sub) sub.textContent = "Restore the selected capture into a target profile.";
      if (note) note.outerHTML = `<div class="ss-inline-pills"><span class="ss-pill"><strong>Merge</strong> add missing only</span><span class="ss-pill"><strong>Clear restore</strong> replace exactly</span></div>`;
      if (progLabel) progLabel.textContent = "Working...";
    }
    const compareCard = $('[data-coll="compare"]', page);
    if (compareCard) {
      const sub = $(".ss-headsub", compareCard);
      const progLabel = $("#ss-diff-progress .ss-plabel", compareCard);
      if (sub) sub.textContent = "Review adds, deletes, and updates before you restore.";
      if (progLabel) progLabel.textContent = "Working...";
    }
    const toolsCard = $('[data-coll="tools"]', page);
    if (toolsCard) {
      const sub = $(".ss-headsub", toolsCard);
      const note = $(".ss-note", toolsCard);
      const progLabel = $("#ss-tools-progress .ss-plabel", toolsCard);
      if (sub) sub.textContent = "Destructive actions stay out of the main flow.";
      if (note) note.textContent = "Double-check the target before you use these.";
      if (progLabel) progLabel.textContent = "Working...";
    }

    wireCollapsible("restore");
    wireCollapsible("compare");
    wireCollapsible("tools");

    $("#ss-refresh", page)?.addEventListener("click", () => {
      state._spinUntil = Date.now() + 550;
      setRefreshSpinning(true);
      refresh(true, true);
      setTimeout(() => { if (!state.busy) setRefreshSpinning(false); }, 600);
    });
    $("#ss-create", page)?.addEventListener("click", () => onCreate());
    $("#ss-prov", page)?.addEventListener("change", () => { repopFeatures(); repopCreateInstances(); });
    $("#ss-filter", page)?.addEventListener("input", () => { state.showAll = false; renderList(); });
    $("#ss-filter-provider", page)?.addEventListener("change", () => { state.showAll = false; renderList(); });
    $("#ss-filter-feature", page)?.addEventListener("change", () => { state.showAll = false; renderList(); });

    $("#ss-restore", page)?.addEventListener("click", () => onRestore());
    $("#ss-delete", page)?.addEventListener("click", () => onDeleteSelected());
    $("#ss-restore-inst", page)?.addEventListener("change", () => updateRestoreAvailability());
    updateRestoreAvailability();

    $("#ss-clear-watchlist", page)?.addEventListener("click", () => onClearTool(["watchlist"]));
    $("#ss-clear-ratings", page)?.addEventListener("click", () => onClearTool(["ratings"]));
    $("#ss-clear-history", page)?.addEventListener("click", () => onClearTool(["history"]));
    $("#ss-clear-progress", page)?.addEventListener("click", () => onClearTool(["progress"]));
    $("#ss-clear-all", page)?.addEventListener("click", () => onClearTool(getClearableFeatures($("#ss-tools-prov", page)?.value)));
    $("#ss-tools-prov", page)?.addEventListener("change", () => { repopToolsInstances(); updateToolsAvailability(); });
    $("#ss-tools-inst", page)?.addEventListener("change", () => updateToolsAvailability());
  }

  function setProgress(sel, on, label, tone) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const el = $(sel, page);
    if (!el) return;
    const lab = $(".ss-plabel", el);
    if (lab) lab.textContent = label || "Working…";
    el.style.setProperty("--pcol", tone === "danger" ? "var(--danger)" : "var(--accent)");
    el.classList.toggle("hidden", !on);
  }
  function setCollapsed(id, collapsed) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const card = page.querySelector(`.ss-card[data-coll="${id}"]`);
    const head = page.querySelector(`[data-coll-head="${id}"]`);
    const body = page.querySelector(`[data-coll-body="${id}"]`);
    if (!card || !head || !body) return;
    card.classList.toggle("is-collapsed", !!collapsed);
    body.classList.toggle("hidden", !!collapsed);
    head.setAttribute("aria-expanded", collapsed ? "false" : "true");
  }

  function wireCollapsible(id) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const card = page.querySelector(`.ss-card[data-coll="${id}"]`);
    const head = page.querySelector(`[data-coll-head="${id}"]`);
    const body = page.querySelector(`[data-coll-body="${id}"]`);
    if (!card || !head || !body) return;

    const toggle = () => {
      const collapsed = card.classList.toggle("is-collapsed");
      body.classList.toggle("hidden", collapsed);
      head.setAttribute("aria-expanded", collapsed ? "false" : "true");
    };

    head.addEventListener("click", (e) => { e.preventDefault(); toggle(); });
    head.addEventListener("keydown", (e) => {
      const k = e.key;
      if (k === "Enter" || k === " ") { e.preventDefault(); toggle(); }
    });
  }

  function updateRestoreAvailability() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const b = $("#ss-restore", page);
    const d = $("#ss-delete", page);
    const instSel = $("#ss-restore-inst", page);
    if (!b) return;
    const pid = String(state.selectedSnap?.provider || "").toUpperCase();
    const targetInst = String($("#ss-restore-inst", page)?.value || "default");
    const p = _providerById(pid);
    const instMeta = Array.isArray(p?.instances) ? p.instances.find((x) => String(x?.id || "") === targetInst) : null;
    const instOk = instMeta ? !!instMeta.configured : true;

    b.disabled = state.busy || !state.selectedPath || !instOk;
    b.title = !state.selectedPath ? "Select a snapshot first" : (!instOk ? "Target profile not configured" : "");
    if (d) {
      d.disabled = state.busy || !state.selectedPath;
      d.title = state.selectedPath ? "" : "Select a snapshot first";
    }

    if (instSel) {
      instSel.disabled = state.busy || !state.selectedPath || instSel.options.length <= 1;
    }
  }

function repopDiffSelects() {
  renderDiffPicked();
  updateDiffAvailability();
  renderDiff();
}

function updateDiffAvailability() {
  const page = document.getElementById("page-snapshots");
  if (!page) return;
  const { ok } = diffSelection(), hint = "Pick two captures from the same provider and instance";
  [["#ss-diff-run", hint], ["#ss-diff-extend", ok ? "Open advanced diff" : hint]].forEach(([id, title]) => {
    const el = $(id, page);
    if (!el) return;
    el.disabled = state.busy || !ok;
    el.title = title;
  });
}

async function onDiffExtend() {
  const page = document.getElementById("page-snapshots");
  if (!page) return;
  const { a, b, ok } = diffSelection();
  if (!ok) return toast("Pick two captures from the same provider and instance", false);
  if (!window.openCaptureCompare) return toast("Capture Compare modal not available", false);
  window.openCaptureCompare({ aPath: a, bPath: b });
}

function renderDiffPicked() {
  const page = document.getElementById("page-snapshots");
  if (!page) return;
  const host = $("#ss-diff-picked", page);
  if (!host) return;

  const picks = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];
  if (picks.length !== 2) {
    const scope = _diffScope();
    host.innerHTML = `<div class="ss-muted ss-small">Tick <b>two</b> boxes to compare${scope ? ` (<b>${escapeHtml(scope.provider)}</b> • <b>${escapeHtml(scope.feature)}</b>)` : ""}.</div>` +
      `<div class="ss-muted ss-small" style="margin-top:6px">Drag A/B cards to swap order</div>`;
    return;
  }

  const aPath = String(picks[0] || "");
  const bPath = String(picks[1] || "");
  const sa = _findSnapByPath(aPath);
  const sb = _findSnapByPath(bPath);

  const mkCard = (snap, tag, path, idx) => {
    const d = document.createElement("div");
    d.className = "ss-pick-card";
    d.setAttribute("draggable", "true");
    d.dataset.diffIndex = String(idx);
    d.dataset.diffPath = String(path || "");
    d.title = "Drag to swap A/B";

    if (!snap) {
      d.innerHTML = `<div class="ss-pick-date">${tag}</div><div class="ss-muted ss-small">Capture not found</div>`;
      return d;
    }

    const feat = String(snap.feature || "-").toLowerCase();
    const inst = String(snap.instance || snap.instance_id || snap.profile || "default");
    const showInst = inst && String(inst).toLowerCase() !== "default";
    const when = snap.stamp ? fmtTsFromStamp(snap.stamp) : (snap.mtime ? new Date(Number(snap.mtime || 0) * 1000).toLocaleString() : "");
    const meta = `${(snap.provider || "-").toUpperCase()}${showInst ? " • " + inst : ""} • ${feat}`;
    const sub = snap.label ? String(snap.label).slice(0, 60) : String(snap.path || "").slice(0, 80);

    d.innerHTML = `<div class="ss-pick-date">${tag}: ${escapeHtml(when || "-")}</div>` +
      `<div class="ss-pick-meta">${escapeHtml(meta)}</div>` +
      `<div class="ss-muted ss-small">${escapeHtml(sub)}</div>`;
    return d;
  };

  host.innerHTML = "";
  const ca = mkCard(sa, "A", aPath, 0);
  const cb = mkCard(sb, "B", bPath, 1);
  host.appendChild(ca);
  host.appendChild(cb);

  const wireDnD = (el) => {
    el.addEventListener("dragstart", (e) => {
      el.classList.add("dragging");
      e.dataTransfer.effectAllowed = "move";
      e.dataTransfer.setData("text/plain", String(el.dataset.diffIndex || ""));
    });
    el.addEventListener("dragend", () => { el.classList.remove("dragging"); });
    el.addEventListener("dragover", (e) => { e.preventDefault(); e.dataTransfer.dropEffect = "move"; });
    el.addEventListener("drop", (e) => {
      e.preventDefault();
      const from = Number(e.dataTransfer.getData("text/plain"));
      const to = Number(el.dataset.diffIndex || "0");
      if (!Number.isFinite(from) || !Number.isFinite(to) || from === to) return;
      const arr = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];
      if (arr.length !== 2) return;
      const tmp = arr[from];
      arr[from] = arr[to];
      arr[to] = tmp;
      state.diffPick = arr;
      renderList();
      renderDiffPicked();
      updateDiffAvailability();
    });
  };

  wireDnD(ca);
  wireDnD(cb);
}

function _matchesDiffQ(row, q) {
  const s = JSON.stringify(row || {});
  return s.toLowerCase().includes(q);
}

function renderDiff() {
  const page = document.getElementById("page-snapshots");
  if (!page) return;

  const out = $("#ss-diff-out", page);
  const list = $("#ss-diff-list", page);
  if (!out || !list) return;

  const r = state.diffResult;
  if (!r) {
    out.textContent = "Pick two captures and hit Compare.";
    list.innerHTML = "";
    return;
  }

  const sum = r.summary || {};
  const trunc = r.truncated || {};
  const extra = (trunc.added || trunc.removed || trunc.updated) ? ` (showing up to ${r.limit} per section)` : "";
  out.innerHTML = `
<div class="ss-diff-summary">
  <span class="ss-pill"><strong>${sum.added ?? 0}</strong> <span class="lbl">added</span></span>
  <span class="ss-pill"><strong>${sum.removed ?? 0}</strong> <span class="lbl">deleted</span></span>
  <span class="ss-pill"><strong>${sum.updated ?? 0}</strong> <span class="lbl">updated</span></span>
  <span class="ss-pill"><strong>${sum.unchanged ?? 0}</strong> <span class="lbl">unchanged</span></span>
</div>
<div class="ss-small ss-muted" style="margin-top:8px">${extra}</div>`;

  const kind = String(state.diffKind || "all");
  const q = String(state.diffQ || "").trim().toLowerCase();

  const add = Array.isArray(r.added) ? r.added.map((x) => ({ ...x, _k: "added" })) : [];
  const rem = Array.isArray(r.removed) ? r.removed.map((x) => ({ ...x, _k: "removed" })) : [];
  const upd = Array.isArray(r.updated) ? r.updated.map((x) => ({ ...x, _k: "updated" })) : [];

  let rows = [];
  if (kind === "added") rows = add;
  else if (kind === "removed") rows = rem;
  else if (kind === "updated") rows = upd;
  else rows = add.concat(rem).concat(upd);

  if (q) rows = rows.filter((x) => _matchesDiffQ(x, q));

  if (!rows.length) {
    list.innerHTML = `<div class="ss-empty">No matches.</div>`;
    return;
  }

  const badge = (k) => {
    if (k === "added") return `<span class="ss-badge add">ADDED</span>`;
    if (k === "removed") return `<span class="ss-badge del">DELETED</span>`;
    return `<span class="ss-badge upd">UPDATED</span>`;
  };

  const line = (v) => {
    if (v === null) return "null";
    if (v === undefined) return "—";
    if (typeof v === "string") return v.length > 160 ? (v.slice(0, 160) + "…") : v;
    try {
      const s = JSON.stringify(v);
      return s.length > 160 ? (s.slice(0, 160) + "…") : s;
    } catch {
      return String(v);
    }
  };

    function _diffName(it) {
    const item = it && typeof it === "object" ? it : {};
    const t = String(item.type || "").toLowerCase();
    const title = String(item.series_title || item.show_title || item.title || "").trim();
    const year = item.year ? ` (${item.year})` : "";
    const sN = item.season != null ? String(item.season).padStart(2, "0") : "";
    const eN = item.episode != null ? String(item.episode).padStart(2, "0") : "";
    const ep = (sN && eN) ? ` - S${sN}E${eN}` : "";
    if (t === "episode") return `${title || "Episode"}${ep}`;
    return `${title || (t ? t : "Item")}${year}`;
  }

  list.innerHTML = rows.map((row) => {
    const k = row._k;
    const key = String(row.key || "");
    const item = row.item || row.new || row.old || {};
    const head = _diffName(item);
    const featTag = String(item.feature || "").toLowerCase();

    const exp = !!state.diffExpanded[key];
    const btn = (k === "updated")
      ? `<button class="btn" data-diff-toggle="${encodeURIComponent(key)}" style="margin-left:auto">${exp ? "Hide" : "Details"}</button>`
      : "";

    const ch = (k === "updated" && Array.isArray(row.changes)) ? row.changes : [];
    const chLines = ch.map((c) => `${c.path}: ${line(c.old)}  →  ${line(c.new)}`).join("\n");

    return `
      <div class="ss-diffrow">
        <div class="ss-diffhead">
          ${badge(k)}
          ${featTag ? `<span class="ss-badge">${escapeHtml(featTag)}</span>` : ``}
          <div class="ss-difftitle">${escapeHtml(head)}</div>
          ${btn}
        </div>
        ${k === "updated" && exp ? `<div class="ss-code">${escapeHtml(chLines || "(no details)")}</div>` : ``}
      </div>
    `;
  }).join("");

  $$("[data-diff-toggle]", list).forEach((b) => {
    b.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      const k = decodeURIComponent(String(b.getAttribute("data-diff-toggle") || ""));
      if (!k) return;
      state.diffExpanded[k] = !state.diffExpanded[k];
      renderDiff();
    });
  });
}

async function onDiffRun() {
  const page = document.getElementById("page-snapshots");
  if (!page) return;
  const { a, b, ok } = diffSelection();
  const kind = String($("#ss-diff-kind", page)?.value || "all");
  const lim = parseInt(String($("#ss-diff-limit", page)?.value || "200"), 10) || 200;
  if (!ok) return toast("Pick two captures from the same provider and instance", false);
  state.diffKind = kind;
  state.diffLimit = lim;
  setProgress("#ss-diff-progress", true, "Comparing…", "accent");
  setBusy(true);
  try {
    const r = await API()(`/api/snapshots/diff?a=${encodeURIComponent(a)}&b=${encodeURIComponent(b)}&limit=${encodeURIComponent(String(lim))}&max_changes=25`);
    state.diffResult = r && r.diff ? r.diff : null;
    state.diffExpanded = {};
    renderDiff();
    toast("Diff ready", true);
  } catch (e) {
    console.warn("[snapshots] diff failed", e);
    state.diffResult = null;
    renderDiff();
    toast(`Diff failed: ${String(e?.message || e || "unknown")}`, false);
  } finally {
    setBusy(false);
    setProgress("#ss-diff-progress", false);
    updateDiffAvailability();
  }
}

  function setBusy(on) {
    state.busy = !!on;
    if (!on) {
      setProgress("#ss-create-progress", false, "", "accent");
      setProgress("#ss-restore-progress", false, "", "danger");
      setProgress("#ss-tools-progress", false, "", "danger");
      setProgress("#ss-diff-progress", false, "", "accent");
    }
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    $$("#page-snapshots button, #page-snapshots input, #page-snapshots select").forEach((el) => {
      if (!el) return;
      el.disabled = !!on;
    });
    if (!on) {
      // Restore feature-based disabling after busy state.
      try { updateToolsAvailability(); } catch {}
      try { updateRestoreAvailability(); } catch {}
    }
  }

  function repopProviders() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const provSel = $("#ss-prov", page);
    const toolsSel = $("#ss-tools-prov", page);
    const fProv = $("#ss-filter-provider", page);

    const configured = (state.providers || []).filter((p) => !!p.configured);
    const opts = [{ id: "", label: "- provider -", configured: true }].concat(configured);
    const fill = (sel, addAll = false) => {
      if (!sel) return;
      const cur = String(sel.value || "");
      sel.innerHTML = "";
      const rows = addAll ? [{ id: "", label: "All providers", configured: true }].concat(configured) : opts;
      rows.forEach((p) => {
        const o = document.createElement("option");
        o.value = p.id || "";
        o.textContent = (p.label || p.id || "-");
        sel.appendChild(o);
      });
      const has = Array.from(sel.options).some((o) => String(o.value) === cur);
      sel.value = has ? cur : "";
    };

    fill(provSel, false);
    fill(toolsSel, false);
    fill(fProv, true);

    // Provider dropdowns with brand icons
    _rebuildBrandSelectMenu(provSel);
    _rebuildBrandSelectMenu(toolsSel);
    _rebuildBrandSelectMenu(fProv);

    repopFeatures();
    repopCreateInstances();
    repopToolsInstances();
    repopRestoreInstances(state.selectedSnap);
    updateToolsAvailability();

// Diff UI
const diffRun = $("#ss-diff-run", page);
const diffExt = $("#ss-diff-extend", page);
const diffKind = $("#ss-diff-kind", page);
const diffLim = $("#ss-diff-limit", page);
const diffQ = $("#ss-diff-q", page);

if (diffKind) diffKind.addEventListener("change", () => { state.diffKind = String(diffKind.value || "all"); renderDiff(); });
if (diffLim) diffLim.addEventListener("change", () => { state.diffLimit = parseInt(String(diffLim.value || "200"), 10) || 200; updateDiffAvailability(); });
if (diffQ) diffQ.addEventListener("input", () => { state.diffQ = String(diffQ.value || ""); renderDiff(); });

if (diffRun) diffRun.addEventListener("click", (e) => { e.preventDefault(); onDiffRun(); });
if (diffExt) diffExt.addEventListener("click", (e) => { e.preventDefault(); onDiffExtend(); });

repopDiffSelects();
}

  function _providerById(pid) {
    const id = String(pid || "").toUpperCase();
    return (state.providers || []).find((x) => String(x.id || "").toUpperCase() === id) || null;
  }

  function _fillInstanceSelect(sel, pid, prefer) {
    if (!sel) return;
    const p = _providerById(pid);
    const insts = Array.isArray(p?.instances) ? p.instances : [{ id: "default", label: "Default", configured: true }];
    const cur = String(prefer ?? sel.value ?? "");
    sel.innerHTML = "";

    const options = insts.length ? insts : [{ id: "default", label: "Default", configured: true }];
    options.forEach((it) => {
      const id = String(it?.id || "default");
      const label = String(it?.label || id || "default");
      const configured = (typeof it?.configured === "boolean") ? !!it.configured : true;

      const o = document.createElement("option");
      o.value = id;
      o.textContent = configured ? label : `${label} (not configured)`;
      o.disabled = !configured;
      sel.appendChild(o);
    });

    const has = Array.from(sel.options).some((o) => String(o.value) === cur && !o.disabled);
    if (has) {
      sel.value = cur;
    } else {
      const firstOk = Array.from(sel.options).find((o) => !o.disabled);
      sel.value = firstOk ? String(firstOk.value) : "default";
    }

    sel.disabled = sel.options.length <= 1;
  }

  function repopInstances(fromSel, toSel) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    _fillInstanceSelect($(toSel, page), String($(fromSel, page)?.value || "").toUpperCase(), null);
  }
  const repopCreateInstances = () => repopInstances("#ss-prov", "#ss-prov-inst");
  const repopToolsInstances = () => repopInstances("#ss-tools-prov", "#ss-tools-inst");

  function repopRestoreInstances(snap) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const s = snap || state.selectedSnap || {};
    const pid = String(s.provider || "").toUpperCase();
    const inst = String(s.instance || s.instance_id || s.profile || "default");
    const sel = $("#ss-restore-inst", page);
    _fillInstanceSelect(sel, pid, inst);

    if (sel && pid && inst && !Array.from(sel.options).some((o) => String(o.value) === inst)) {
      const o = document.createElement("option");
      o.value = inst;
      o.textContent = `${inst} (missing)`;
      o.disabled = true;
      sel.appendChild(o);
      sel.value = inst;
    }
  }

  function repopFeatures() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const provId = String($("#ss-prov", page)?.value || "").toUpperCase();
    const p = (state.providers || []).find((x) => String(x.id || "").toUpperCase() === provId);
    const feats = (p && p.features) ? p.features : {};
    const fSel = $("#ss-feature", page);

    const featureOptions = ["all", "watchlist", "ratings", "history", "progress"];

    if (fSel) {
      const cur = String(fSel.value || "");
      fSel.innerHTML = "";
      featureOptions.forEach((k) => {
        const o = document.createElement("option");
        o.value = k;
        o.textContent = (k === "all") ? "All features" : k;
        if (k === "all") o.disabled = !featureOptions.slice(1).some((name) => !!feats[name]);
        else o.disabled = !feats[k];
        fSel.appendChild(o);
      });
      if (cur) fSel.value = cur;
    }

    const fFeat = $("#ss-filter-feature", page);
    if (fFeat && fFeat.options.length === 0) {
      ["", ...featureOptions.slice(1)].forEach((k) => {
        const o = document.createElement("option");
        o.value = k;
        o.textContent = k ? `Feature: ${k}` : "All features";
        fFeat.appendChild(o);
      });
    }
  }

  function renderList() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const list = $("#ss-list", page);
    if (!list) return;

    const q = String($("#ss-filter", page)?.value || "").trim().toLowerCase();
    const fp = String($("#ss-filter-provider", page)?.value || "").trim().toLowerCase();
    const ff = String($("#ss-filter-feature", page)?.value || "").trim().toLowerCase();

    const all = state.snapshots || [];
    const idx = buildBundleIndex(all);

    const hiddenChildPaths = new Set();
    const childFeaturesByKey = {};

    Object.keys(idx.childrenByKey || {}).forEach((k) => {
      const kids = idx.childrenByKey[k] || [];
      childFeaturesByKey[k] = new Set(kids.map((x) => String(x.feature || "").toLowerCase()));
      kids.forEach((x) => {
        if (x && x.path) hiddenChildPaths.add(String(x.path));
      });
    });

    const matches = (s) => {
      const prov = String(s.provider || "").toLowerCase();
      const feat = String(s.feature || "").toLowerCase();
      const lab = String(s.label || "").toLowerCase();

      if (fp && prov !== fp) return false;

      if (ff) {
        if (feat === ff) {
          // ok
        } else if (feat === "all") {
          const k = bundleKey(s);
          const set = childFeaturesByKey[k];
          if (!set || !set.has(ff)) return false;
        } else {
          return false;
        }
      }

      if (!q) return true;

      const hay = (prov + " " + feat + " " + lab + " " + String(s.path || "")).toLowerCase();
      if (hay.includes(q)) return true;

      if (feat === "all") {
        const k = bundleKey(s);
        const kids = idx.childrenByKey[k] || [];
        const childHay = kids.map((c) => `${c.feature || ""} ${c.label || ""} ${c.path || ""}`.toLowerCase()).join(" ");
        return childHay.includes(q);
      }

      return false;
    };

    const allowChildren = !!ff || !!q;

    const top = [];
    all.forEach((s) => {
      if (!s) return;
      const isChild = hiddenChildPaths.has(String(s.path || ""));
      if (!allowChildren && isChild) return;
      if (!matches(s)) return;
      top.push(s);
    });

    const topOnly = allowChildren ? top : top.filter((s) => !hiddenChildPaths.has(String(s.path || "")));

    const limit = state.showAll ? topOnly.length : (state.listLimit || 5);
    const rows = topOnly.slice(0, limit);

    const footer = $("#ss-list-footer", page);
    if (footer) {
      footer.innerHTML = "";
      if (topOnly.length > limit) {
        footer.innerHTML = `<div class="ss-small ss-muted">Showing ${limit} of ${topOnly.length}</div><button id="ss-more" class="btn">Show all (${topOnly.length})</button>`;
      } else if (state.showAll && topOnly.length > (state.listLimit || 5)) {
        footer.innerHTML = `<div class="ss-small ss-muted">Showing ${topOnly.length} of ${topOnly.length}</div><button id="ss-less" class="btn">Show less</button>`;
      } else {
        footer.innerHTML = topOnly.length ? `<div class="ss-small ss-muted">${topOnly.length} capture(s)</div>` : "";
      }

      const more = $("#ss-more", footer);
      const less = $("#ss-less", footer);
      if (more) more.addEventListener("click", () => { state.showAll = true; renderList(); });
      if (less) less.addEventListener("click", () => { state.showAll = false; renderList(); });
    }

    if (rows.length === 0) {
      list.innerHTML = `<div class="ss-empty">No captures found.</div>`;
      return;
    }

    list.innerHTML = "";

    const pathToSnap = new Map();
    (all || []).forEach((s) => { if (s && s.path) pathToSnap.set(String(s.path), s); });

    const renderRow = (s, opts = {}) => {
      const child = !!opts.child;
      const childCount = Number(opts.childCount || 0);

      const item = document.createElement("div");
      item.className = "ss-item" + (child ? " child" : "") + (state.selectedPath === s.path ? " active" : "");
      item.dataset.path = s.path || "";

      const stamp = s.stamp ? fmtTsFromStamp(s.stamp) : "";
      const when = stamp || (s.mtime ? new Date(Number(s.mtime || 0) * 1000).toLocaleString() : "");

      const feat = String(s.feature || "-").toLowerCase();
      const isBundle = feat === "all";
      const inst = String(s.instance || s.instance_id || s.profile || "default");
      const showInst = inst && String(inst).toLowerCase() !== "default";
      const exp = !!(state.expandedBundles && state.expandedBundles[String(s.path || "")]);

      const extra = isBundle && childCount
        ? `<button class="ss-mini" data-act="toggle">${exp ? "Hide" : "Show"} ${childCount}</button>`
        : "";

const picks = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];
      const scope = _diffScope();
      const inScope = !scope || _snapMatchesScope(s, scope);
      const pth = String(s.path || "");
      const ixPick = pth ? picks.indexOf(pth) : -1;
      const abTag = ixPick === 0 ? "A" : (ixPick === 1 ? "B" : "");
      const showPick = inScope || ixPick !== -1;
      const pickHtml = showPick
        ? `${abTag ? `<span class="ss-ab ${abTag === "A" ? "a" : "b"}">${abTag}</span>` : ""}` +
          `<input class="ss-chk" type="checkbox" name="ss-diffpick" title="Select for compare" data-act="diffpick" ${ixPick !== -1 ? "checked" : ""} />`
        : "";

      item.innerHTML = `
        <div class="ss-item-main">
          <div class="ss-item-top">
            <div class="ss-item-title">${escapeHtml((s.provider || "-").toUpperCase())} · ${escapeHtml(feat)}${s.label ? ` · ${escapeHtml(_uiCaptureLabel(s.label)).slice(0, 40)}` : ``}</div>
            ${extra}
          </div>
          <div class="ss-item-meta">
            <span class="ss-badge ok">${(s.provider || "-").toUpperCase()}</span>
            ${showInst ? `<span class="ss-badge">${escapeHtml(inst)}</span>` : ``}
            <span class="ss-badge">${escapeHtml(feat)}</span>
            ${s.label ? `<span class="ss-badge warn">${escapeHtml(_uiCaptureLabel(s.label)).slice(0, 40)}</span>` : ``}
          </div>
          <div class="d">${escapeHtml(when || "-")} · ${humanBytes(s.size)} · <span class="ss-path">${escapeHtml(s.path || "")}</span></div>
        </div>
        <div class="ss-item-right">
          <div class="ss-item-action">${pickHtml || `<span class="ss-small ss-muted">restore</span>`}</div>
          <div class="chev">›</div>
        </div>
      `;

      const titleBits = [String(s.provider || "-").toUpperCase(), feat];
      if (s.label) titleBits.push(_uiCaptureLabel(s.label));
      const metaBits = [when || "-", humanBytes(s.size)];
      if (showInst) metaBits.push(inst);
      const fileName = snapFile(s.path);
      const titleEl = $(".ss-item-title", item);
      const metaEl = $(".ss-item-meta", item);
      const detailEl = $(".ss-item-main > .d", item);
      if (titleEl) titleEl.textContent = titleBits.join(" · ");
      if (metaEl) {
        const badges = $$(".ss-badge", metaEl);
        if (showInst && badges[1]) badges[1].remove();
      }
      if (detailEl) {
        detailEl.innerHTML = `${escapeHtml(metaBits.join(" · "))}${fileName ? ` · <span class="ss-file">${escapeHtml(fileName)}</span>` : ""}`;
        if (s.path) detailEl.insertAdjacentHTML("afterend", `<div class="d ss-path">${escapeHtml(s.path || "")}</div>`);
      }

      const pick = item.querySelector('input[data-act="diffpick"]');
      if (pick) {
        pick.addEventListener("click", (ev) => { ev.stopPropagation(); });
        pick.addEventListener("change", () => { toggleDiffPick(String(s.path || ""), !!pick.checked); });
      }

const toggleBtn = item.querySelector('[data-act="toggle"]');
      if (toggleBtn) {
        toggleBtn.addEventListener("click", (ev) => {
          ev.preventDefault();
          ev.stopPropagation();
          const key = String(s.path || "");
          state.expandedBundles = state.expandedBundles || {};
          state.expandedBundles[key] = !state.expandedBundles[key];
          renderList();
      try { repopDiffSelects(); } catch {}
        });
      }

      item.addEventListener("click", () => {
        clearDiffPicks();
        try { setCollapsed("compare", true); setCollapsed("restore", false); } catch {}
        const p = String(s.path || "");
        if (p && state.selectedPath === p) {
          state.selectedPath = "";
          state.selectedSnap = null;
          renderList();
      try { repopDiffSelects(); } catch {}
          renderSelected();
          updateRestoreAvailability();
          return;
        }
        selectSnapshot(p);
      });

      list.appendChild(item);
    };

    rows.forEach((s) => {
      const feat = String(s.feature || "").toLowerCase();
      if (feat === "all") {
        const k = bundleKey(s);
        const kids = idx.childrenByKey[k] || [];
        renderRow(s, { childCount: kids.length });

        const exp = !!(state.expandedBundles && state.expandedBundles[String(s.path || "")]);
        if (exp) {
          kids.forEach((c) => {
            const snap = pathToSnap.get(String(c.path || "")) || c;
            renderRow(snap, { child: true });
          });
        }
      } else {
        renderRow(s);
      }
    });
  }

function renderSelected() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const host = $("#ss-selected", page);
    if (!host) return;

    const s = state.selectedSnap;
    if (!s) {
      host.classList.add("ss-selected-empty");
      host.classList.remove("ss-muted");
      host.innerHTML = `<div class="ss-selected-title">No capture selected</div><div class="ss-small ss-muted">Pick one from the browser to inspect or restore it.</div>`;
      return;
    }

    const stats = s.stats || {};
    const by = stats.by_type || {};
    const featStats = stats.features || null;
    const inst = String(s.instance || s.instance_id || s.profile || "default");
    const showInst = inst && String(inst).toLowerCase() !== "default";
    const pills = featStats ? Object.keys(featStats).slice(0, 6).map((k) =>
      `<span class="ss-pill"><strong>${featStats[k]}</strong><span class="ss-muted">${k}</span></span>`
    ).join("")
    : Object.keys(by).slice(0, 6).map((k) =>
      `<span class="ss-pill"><strong>${by[k]}</strong><span class="ss-muted">${k}</span></span>`
    ).join("");

    host.classList.remove("ss-selected-empty","ss-muted");
    host.innerHTML = `
      <div class="ss-item-title">${String(s.provider || "").toUpperCase()} · ${String(s.feature || "").toLowerCase()}</div>
      <div class="ss-item-meta" style="margin-top:8px">
        <span class="ss-badge ok">${String(s.provider || "").toUpperCase()}</span>
        ${showInst ? `<span class="ss-badge">${escapeHtml(inst)}</span>` : ``}
        <span class="ss-badge">${String(s.feature || "").toLowerCase()}</span>
        ${s.label ? `<span class="ss-badge warn">${escapeHtml(_uiCaptureLabel(s.label)).slice(0, 40)}</span>` : ``}
        <span class="ss-badge">${Number(stats.count || 0)} items</span>
      </div>
      <div class="ss-small ss-muted" style="margin-top:8px">${s.created_at ? new Date(String(s.created_at)).toLocaleString() : "-"}${s.path ? ` · <span class="ss-path">${escapeHtml(s.path)}</span>` : ""}</div>
      ${pills ? `<div class="ss-row" style="margin-top:10px;flex-wrap:wrap">${pills}</div>` : ``}
    `;
  }

  function setRefreshSpinning(on) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const icon = $("#ss-refresh-icon", page);
    if (!icon) return;
    if (on) { icon.classList.add("ss-spin"); return; }
    if (Date.now() < (state._spinUntil || 0)) return;
    icon.classList.remove("ss-spin");
  }

  async function refresh(force = false, announce = true) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const now = Date.now();
    if (!force && now - state.lastRefresh < 2500) return;
    state.lastRefresh = now;

    const wasBusy = !!state.busy;
    if (!wasBusy) setBusy(true);
    setRefreshSpinning(true);
    try {
      const [m, l] = await Promise.all([
        API()("/api/snapshots/manifest"),
        API()("/api/snapshots/list"),
      ]);

      state.providers = (m && m.providers) ? m.providers : [];
      state.snapshots = (l && l.snapshots) ? l.snapshots : [];

      updateTopStats();
      repopProviders();
      renderList();
      try { repopDiffSelects(); } catch {}

      // keep selection
      if (state.selectedPath) {
        const still = state.snapshots.find((x) => x.path === state.selectedPath);
        if (!still) {
          state.selectedPath = "";
          state.selectedSnap = null;
          renderSelected();
        } else {
          try { repopRestoreInstances(state.selectedSnap); } catch {}
        }
      }
    } catch (e) {
      console.warn("[snapshots] refresh failed", e);
      console.warn("[snapshots]", `Refresh failed: ${e.message || e}`);
      toast(`Snapshots refresh failed: ${e.message || e}`, false);
    } finally {
      setRefreshSpinning(false);
      if (!wasBusy) setBusy(false);
    }
  }

  async function selectSnapshot(path) {
    if (!path) return;
    setBusy(true);
    try {
      const r = await API()(`/api/snapshots/read?path=${encodeURIComponent(path)}`);
      state.selectedPath = path;
      state.selectedSnap = r && r.snapshot ? r.snapshot : null;
      repopRestoreInstances(state.selectedSnap);
      renderList();
      try { repopDiffSelects(); } catch {}
      renderSelected();
      updateRestoreAvailability();
      $("#ss-restore-out") && ($("#ss-restore-out").textContent = "");
      toast("Snapshot loaded", true);
    } catch (e) {
      console.warn("[snapshots] read failed", e);
      toast(`Snapshot read failed: ${e.message || e}`, false);
    } finally {
      setProgress("#ss-restore-progress", false, "", "danger");
      setRefreshSpinning(false);
      setBusy(false);
    }
  }

  async function onCreate() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const provider = String($("#ss-prov", page)?.value || "").toUpperCase();
    const instance = String($("#ss-prov-inst", page)?.value || "default");
    const feature = String($("#ss-feature", page)?.value || "").toLowerCase();
    const label = String($("#ss-label", page)?.value || "").trim();

    if (!provider) return toast("Pick a provider first", false);
    if (!feature) return toast("Pick a feature", false);

    setProgress("#ss-create-progress", true, "Creating snapshot…", "accent");
    setBusy(true);
    try {
      const r = await POST_JSON("/api/snapshots/create", { provider, instance, feature, label });

      const snap = r && r.snapshot ? r.snapshot : null;
      $("#ss-label", page).value = "";
      await refresh(true, false);

      if (snap && snap.path) {
        await selectSnapshot(snap.path);
      }
      toast("Capture created", true);
    } catch (e) {
      console.warn("[snapshots] create failed", e);
      const msg = String(e && e.message ? e.message : e);
      if (msg.toLowerCase().includes("timeout")) {
        toast("Create is taking longer than expected. Refreshing…", true);
        setTimeout(() => refresh(true, false), 1200);
        setTimeout(() => refresh(true, false), 5000);
      } else {
        toast(`Snapshot create failed: ${msg}`, false);
      }
    } finally {
      setProgress("#ss-create-progress", false, "", "accent");
      setBusy(false);
    }
  }

  async function onDeleteSelected() {
    if (!state.selectedPath) return;

    const s = state.selectedSnap || {};
    const prov = String(s.provider || "").toUpperCase();
    const feat = String(s.feature || "");
    const label = s.label ? " (" + _uiCaptureLabel(s.label) + ")" : "";
    const isBundle = feat.toLowerCase() === "all";
    const msg = isBundle
      ? "Delete this bundle snapshot" + label + " and its child snapshots?\n\n" + prov + " - ALL"
      : "Delete this snapshot" + label + "?\n\n" + prov + " - " + feat;

    if (!confirm(msg)) return;

    setBusy(true);
    setRefreshSpinning(true);
    try {
      const r = await API()("/api/snapshots/delete", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ path: state.selectedPath, delete_children: true }),
      });

      const res = r && r.result ? r.result : null;
      const ok = res ? !!res.ok : !!(r && r.ok);
      if (!ok) {
        const err = (res && res.errors && res.errors.length) ? res.errors.join(" | ") : (r && r.error) ? r.error : "Delete failed";
      console.warn("[snapshots]", err);
        toast(err, false);
        return;
      }

      state.selectedPath = "";
      state.selectedSnap = null;
      renderSelected();
      updateRestoreAvailability();

      await refresh(true, false);
      toast("Snapshot deleted", true);
    } catch (e) {
      console.warn("[snapshots]", "Delete failed: " + (e.message || e));
      toast("Delete failed: " + (e.message || e), false);
    } finally {
      setRefreshSpinning(false);
      setBusy(false);
    }
  }

  async function onRestore() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    if (!state.selectedPath) return toast("Select a snapshot first", false);
    const mode = String($("#ss-restore-mode", page)?.value || "merge").toLowerCase();
    const instance = String($("#ss-restore-inst", page)?.value || "default");

    if (mode === "clear_restore") {
      const ok = confirm("Clear + restore will wipe the provider feature before restoring. Continue?");
      if (!ok) return;
    }

    setProgress("#ss-restore-progress", true, "Restoring snapshot…", "danger");
    setBusy(true);
    try {
      const r = await POST_JSON("/api/snapshots/restore", { path: state.selectedPath, mode, instance });

      const res = r && r.result ? r.result : {};
      const out = $("#ss-restore-out", page);
      if (out) {
        if (res.ok) out.textContent = `Done. Added ${res.added || 0}, removed ${res.removed || 0}.`;
        else out.textContent = `Restore finished with errors: ${(res.errors || []).join("; ") || "unknown error"}`;
      }

      toast(res.ok ? "Restore complete" : "Restore finished with errors", !!res.ok);
    } catch (e) {
      console.warn("[snapshots] restore failed", e);
      toast(`Restore failed: ${e.message || e}`, false);
      const out = $("#ss-restore-out", page);
      if (out) out.textContent = `Restore failed: ${e.message || e}`;
    } finally {
      setProgress("#ss-restore-progress", false, "", "danger");
      setBusy(false);
    }
  }

  async function onClearTool(features) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const provider = String($("#ss-tools-prov", page)?.value || "").toUpperCase();
    const instance = String($("#ss-tools-inst", page)?.value || "default");
    if (!provider) return toast("Pick a provider first", false);

    features = Array.isArray(features) ? features.filter(Boolean) : [];
    if (!features.length) return toast("Nothing to clear for this provider", false);

    const what = features.join(", ");
    const ok = confirm(`This will clear ${what} on ${provider} (${instance}). Continue?`);
    if (!ok) return;

    setProgress("#ss-tools-progress", true, `Clearing ${what}…`, "danger");
    setBusy(true);
    try {
      const r = await POST_JSON("/api/snapshots/tools/clear", { provider, instance, features });

      const res = r && r.result ? r.result : {};
      const out = $("#ss-tools-out", page);
      if (out) {
        if (res.ok) {
          const parts = Object.keys(res.results || {}).map((k) => {
            const x = res.results[k] || {};
            if (x.skipped) return `${k}: skipped (${x.reason || "n/a"})`;

            const u = (x.unresolved_count != null) ? Number(x.unresolved_count || 0)
              : (Array.isArray(x.unresolved) ? x.unresolved.length : 0);
            return u > 0
              ? `${k}: removed ${x.removed || 0} (had ${x.count || 0}, unresolved ${u})`
              : `${k}: removed ${x.removed || 0} (had ${x.count || 0})`;
          });
          out.textContent = parts.join(" * ");
        } else {
          out.textContent = `Clear finished with errors.`;
        }
      }
      if (!res.ok) console.warn("[snapshots]", "Tool finished with errors.");
      toast(res.ok ? "Clear complete" : "Clear finished with errors", !!res.ok);
    } catch (e) {
      console.warn("[snapshots] clear failed", e);
      console.warn("[snapshots]", `Tool failed: ${e.message || e}`);
      toast(`Clear failed: ${e.message || e}`, false);
      const out = $("#ss-tools-out", page);
      if (out) out.textContent = `Clear failed: ${e.message || e}`;
    } finally {
      setProgress("#ss-tools-progress", false, "", "danger");
      setProgress("#ss-diff-progress", false, "", "accent");
      setBusy(false);
    }
  }

  function updateToolsAvailability() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const pid = String($("#ss-tools-prov", page)?.value || "").toUpperCase();
    const inst = String($("#ss-tools-inst", page)?.value || "default");
    const p = (state.providers || []).find((x) => String(x.id || "").toUpperCase() === pid);
    const feats = (p && p.features) ? p.features : {};
    const instMeta = Array.isArray(p?.instances) ? p.instances.find((x) => String(x?.id || "") === inst) : null;
    const instOk = instMeta ? !!instMeta.configured : true;

    const clearable = getClearableFeatures(pid);
    const setBtn = (id, enabled, why) => {
      const b = $(id, page);
      if (!b) return;
      const ok = !!enabled && !!pid && !!instOk;
      b.disabled = !ok || !!state.busy;
      b.title = ok ? "" : (!pid ? "Pick a provider" : (!instOk ? "Profile not configured" : (why || "Not supported by provider")));
    };

    setBtn("#ss-clear-watchlist", !!feats.watchlist, "Watchlist not supported");
    setBtn("#ss-clear-ratings", !!feats.ratings, "Ratings not supported");
    setBtn("#ss-clear-history", !!feats.history, "History not supported");
    setBtn("#ss-clear-progress", clearable.includes("progress"), pid === "PLEX" ? "Progress clear not supported for Plex" : "Progress not supported");
    setBtn("#ss-clear-all", clearable.length > 0, "Nothing to clear");
  }

  function getClearableFeatures(provider) {
    const pid = String(provider || "").toUpperCase();
    const p = (state.providers || []).find((x) => String(x.id || "").toUpperCase() === pid);
    const feats = (p && p.features) ? p.features : {};
    return ["watchlist", "ratings", "history", "progress"].filter((feature) => {
      if (!feats[feature]) return false;
      return feature !== "progress" || (pid !== "PLEX");
    });
  }

  async function init() {
    injectCss();
    render();
    await refresh(true, false);
  }

  // public hook for core.js
  window.Snapshots = {
    refresh: (force = false) => refresh(!!force),
    init,
  };

  if (document.getElementById("page-snapshots")) {
    init();
  } else {
    document.addEventListener("tab-changed", (e) => {
      if (e?.detail?.id === "snapshots") {
        try { init(); } catch {}
      }
    });
  }

})();
