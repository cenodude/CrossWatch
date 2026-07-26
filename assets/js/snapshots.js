/* captures.js - Provider captures (watchlist/ratings/history/progress) */
/* CrossWatch - Captures/Snapshots page UI logic */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const authSetupPending = () => window.cwIsAuthSetupPending?.() === true;
  let authRetryWired = false;

  const css = `
#page-snapshots{max-width:none;width:100%;grid-column:1/-1;padding:0!important;background:transparent!important;background-image:none!important;border:0!important;box-shadow:none!important;outline:0!important;overflow:visible!important;--ss-shell:linear-gradient(180deg,rgba(5,6,10,.995),rgba(1,2,5,.99));--ss-panel:linear-gradient(180deg,rgba(11,12,18,.94),rgba(3,4,8,.98));--ss-panel-strong:linear-gradient(180deg,rgba(9,10,16,.97),rgba(2,3,7,.995));--ss-border:rgba(255,255,255,.09);--ss-fg:rgba(244,247,255,.97);--ss-muted-fg:rgba(197,206,224,.72);--ss-shadow:0 18px 52px rgba(0,0,0,.36),inset 0 1px 0 rgba(255,255,255,.04);--ss-accent:rgba(92,96,182,.62);--ss-accent-soft:rgba(92,96,182,.10);--ss-accent-rose:rgba(92,96,182,.04)}
#page-snapshots .ss-top{display:flex;align-items:flex-start;justify-content:space-between;gap:14px;flex-wrap:wrap;margin-bottom:14px;padding:16px 18px;border:1px solid var(--ss-border);border-radius:24px;background:radial-gradient(120% 140% at 0% 0%,rgba(86,90,180,.11),transparent 38%),radial-gradient(90% 120% at 100% 100%,rgba(56,64,132,.06),transparent 48%),var(--ss-shell);box-shadow:var(--ss-shadow);backdrop-filter:blur(16px) saturate(130%);-webkit-backdrop-filter:blur(16px) saturate(130%)}
#page-snapshots .ss-top-copy{display:block;min-width:0}
#page-snapshots .ss-kicker{display:inline-flex;align-items:center;width:max-content;max-width:100%;padding:4px 10px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.05);color:var(--ss-muted-fg);font-size:11px;font-weight:800;letter-spacing:.12em;text-transform:uppercase}
#page-snapshots .ss-title{font-weight:900;font-size:26px;letter-spacing:-.02em;line-height:1.02;color:var(--ss-fg)}
#page-snapshots .ss-sub{color:var(--ss-muted-fg);font-size:13px;line-height:1.45;max-width:76ch}
#page-snapshots .ss-actions,#page-snapshots .ss-topstats{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
#page-snapshots .ss-topstats{margin-left:auto;justify-content:flex-end}
#page-snapshots .ss-topstat{display:inline-flex;align-items:center;gap:8px;min-height:38px;padding:0 12px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.025));color:#f5f7ff;box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
#page-snapshots .ss-topstat strong{font-size:15px;font-weight:900}
#page-snapshots .ss-topstat span{font-size:12px;color:var(--ss-muted-fg);font-weight:700}
#page-snapshots .ss-topstat[data-stat="captures"]{background:linear-gradient(180deg,rgba(88,94,170,.14),rgba(255,255,255,.025));border-color:rgba(102,108,188,.18)}
#page-snapshots .ss-wrap{display:grid;grid-template-columns:360px minmax(0,1fr) 390px;gap:14px;align-items:start}
#page-snapshots .ss-col{display:flex;flex-direction:column;gap:12px}
#page-snapshots .ss-card{position:relative;padding:14px;border-radius:22px;border:1px solid var(--ss-border);background:radial-gradient(120% 120% at 0% 0%,rgba(86,90,180,.07),transparent 38%),radial-gradient(90% 110% at 100% 100%,rgba(44,52,108,.04),transparent 50%),var(--ss-panel);box-shadow:var(--ss-shadow);overflow:hidden}
#page-snapshots .ss-card::before{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(135deg,rgba(255,255,255,.04),transparent 50%)}
#page-snapshots .ss-card>*{position:relative;z-index:1}
#page-snapshots .ss-card.ss-overflow{overflow:visible;z-index:6}
#page-snapshots .ss-card h3{margin:0;font-size:12px;letter-spacing:.13em;text-transform:uppercase;color:rgba(225,232,246,.72)}
#page-snapshots .ss-card.ss-accent{background:radial-gradient(120% 130% at 0% 0%,rgba(92,96,182,.12),transparent 36%),radial-gradient(80% 100% at 100% 100%,rgba(50,58,118,.05),transparent 46%),var(--ss-panel-strong)}
#page-snapshots .ss-card-head{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;margin-bottom:12px}
#page-snapshots .ss-headcopy{display:grid;gap:5px;min-width:0}
#page-snapshots .ss-headtitle{font-size:18px;font-weight:850;letter-spacing:-.02em;color:var(--ss-fg)}
#page-snapshots .ss-headsub,#page-snapshots .ss-note,#page-snapshots .ss-muted{color:var(--ss-muted-fg)}
#page-snapshots .ss-note,#page-snapshots .ss-small{font-size:12px;line-height:1.45}
#page-snapshots .ss-row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
#page-snapshots .ss-row>*{flex:0 0 auto}
#page-snapshots .ss-row .grow{flex:1 1 auto;min-width:180px}
#page-snapshots .ss-grid2{display:grid;grid-template-columns:1fr 1fr;gap:10px}
#page-snapshots .ss-hero-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}
#page-snapshots .ss-hero-stat{padding:12px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(10,11,18,.72),rgba(3,4,8,.86));box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
#page-snapshots .ss-hero-stat .v{font-size:20px;font-weight:900;color:#f7f9ff;line-height:1}
#page-snapshots .ss-hero-stat .k{margin-top:6px;font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:var(--ss-muted-fg);font-weight:800}
#page-snapshots .ss-pill{display:inline-flex;align-items:center;gap:6px;min-height:28px;padding:0 10px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.05);font-size:12px;color:#eef3ff}
#page-snapshots .ss-pill strong{font-weight:900}
#page-snapshots .ss-hr{height:1px;background:rgba(255,255,255,.07);margin:12px 0}
#page-snapshots #ss-refresh.iconbtn{width:38px;height:38px;padding:0;display:inline-flex;align-items:center;justify-content:center;border-radius:14px;border:1px solid rgba(255,255,255,.10);background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.03))}
#page-snapshots #ss-refresh-icon{font-size:20px;line-height:1}
#page-snapshots .ss-refresh-icon.ss-spin{animation:ssrot .8s linear infinite}
@keyframes ssrot{to{transform:rotate(360deg)}}
#page-snapshots .ss-progress{display:flex;align-items:center;gap:10px;margin-top:12px}
#page-snapshots .ss-progress.hidden{display:none}
#page-snapshots .ss-pbar{position:relative;flex:1 1 auto;height:8px;border-radius:999px;background:rgba(255,255,255,.08);overflow:hidden}
#page-snapshots .ss-pbar::before{content:"";position:absolute;inset:0;width:40%;transform:translateX(-60%);background:linear-gradient(90deg,transparent,var(--pcol,var(--accent)),transparent);animation:ssprog 1.05s ease-in-out infinite}
@keyframes ssprog{0%{transform:translateX(-60%)}100%{transform:translateX(220%)}}
#page-snapshots .ss-plabel{flex:0 0 auto;font-size:12px;color:var(--ss-muted-fg);white-space:nowrap}
#page-snapshots button:disabled{opacity:.42;cursor:not-allowed;filter:saturate(.55)}
#page-snapshots .ss-field{position:relative;display:flex;align-items:center;gap:10px;padding:0 12px;min-height:42px;border-radius:14px;border:1px solid rgba(255,255,255,.09);background:linear-gradient(180deg,rgba(8,10,18,.82),rgba(7,8,15,.92));box-shadow:inset 0 1px 0 rgba(255,255,255,.02)}
#page-snapshots .ss-field.ss-open{z-index:34}
#page-snapshots .ss-field .material-symbol,#page-snapshots .ss-field .chev{opacity:.72}
#page-snapshots .ss-field select,#page-snapshots .ss-field input{flex:1 1 auto;min-width:0;height:40px;background:transparent;border:0;outline:0;color:inherit;font:inherit}
#page-snapshots .ss-field select{appearance:none;color-scheme:dark}
#page-snapshots .ss-field select option{background:#141418;color:#f3f3f5}
#page-snapshots .ss-field select option:disabled{color:#7b7b86}
#page-snapshots .ss-native{display:none!important}
#page-snapshots .ss-bsel{position:relative;flex:1 1 auto;min-width:0}
#page-snapshots .ss-bsel.is-open .ss-bsel-btn{color:#f7f9ff}
#page-snapshots .ss-bsel-btn{width:100%;display:flex;align-items:center;gap:10px;background:transparent;border:0;outline:0;color:inherit;font:inherit;cursor:pointer;padding:0;text-align:left}
#page-snapshots .ss-bsel-label{flex:1 1 auto;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;text-align:left}
#page-snapshots .ss-bsel-chev{opacity:.6;flex:0 0 auto}
#page-snapshots .ss-bsel-menu{position:absolute;left:-12px;right:-12px;top:calc(100%+10px);z-index:80;border:1px solid rgba(255,255,255,.10);border-radius:16px;background:linear-gradient(180deg,rgba(255,255,255,.025),transparent),linear-gradient(180deg,rgba(9,10,16,.99),rgba(3,4,8,.995));box-shadow:0 14px 40px rgba(0,0,0,.58);padding:6px;max-height:320px;overflow:auto;pointer-events:auto}
#page-snapshots .ss-bsel-menu.hidden{display:none}
#page-snapshots .ss-bsel-item{width:100%;display:flex;align-items:center;gap:10px;padding:10px;border-radius:12px;border:1px solid transparent;background:transparent;color:inherit;cursor:pointer;text-align:left}
#page-snapshots .ss-bsel-item:hover{background:rgba(255,255,255,.04);border-color:rgba(255,255,255,.10)}
#page-snapshots .ss-bsel-item:disabled{opacity:.45;cursor:not-allowed}
#page-snapshots .ss-provico{width:18px;height:18px;flex:0 0 18px;border-radius:7px;border:1px solid rgba(255,255,255,.16);background:rgba(0,0,0,.18);background-image:var(--wm);background-repeat:no-repeat;background-position:center;background-size:contain;filter:grayscale(.05) brightness(1.12);opacity:.95}
#page-snapshots .ss-bsel-menu .ss-provico{width:20px;height:20px;flex-basis:20px}
#page-snapshots .ss-provico.empty{background-image:none;background:rgba(255,255,255,.05)}
#page-snapshots .ss-comparehint{display:flex;align-items:flex-start;gap:10px;padding:11px 12px;border-radius:16px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.02));font-size:12px;color:var(--ss-muted-fg);margin:12px 0}
#page-snapshots .ss-comparehint .material-symbol{font-size:18px;opacity:.9;color:#eef3ff}
#page-snapshots .ss-list{display:flex;flex-direction:column;gap:10px;max-height:620px;overflow:auto;padding:2px 2px 2px 0}
#page-snapshots .ss-item{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:12px;align-items:center;cursor:pointer;padding:12px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.02));transition:transform .12s ease,border-color .14s ease,background .14s ease,box-shadow .14s ease}
#page-snapshots .ss-item:hover{transform:translateY(-1px);border-color:rgba(255,255,255,.14);background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.03))}
#page-snapshots .ss-item.active{border-color:rgba(92,96,182,.34);background:linear-gradient(180deg,rgba(92,96,182,.06),rgba(255,255,255,.02));box-shadow:0 0 0 1px rgba(92,96,182,.16),0 14px 28px rgba(0,0,0,.24)}
#page-snapshots .ss-item.child{margin-left:16px;background:rgba(255,255,255,.02)}
#page-snapshots .ss-item-main{min-width:0;display:grid;gap:8px}
#page-snapshots .ss-item-top{display:flex;align-items:center;justify-content:space-between;gap:12px}
#page-snapshots .ss-item-title{font-weight:850;color:#f6f8ff;letter-spacing:-.01em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#page-snapshots .ss-item-meta{display:flex;gap:6px;flex-wrap:wrap;align-items:center}
#page-snapshots .ss-item .d{font-size:12px;color:var(--ss-muted-fg);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#page-snapshots .ss-path{opacity:.52}
#page-snapshots .ss-badge{display:inline-flex;align-items:center;min-height:24px;padding:0 8px;border-radius:999px;border:1px solid rgba(255,255,255,.11);background:rgba(255,255,255,.04);font-size:11px;letter-spacing:.05em;text-transform:uppercase;color:#eef3ff}
#page-snapshots .ss-badge.ok{border-color:rgba(91,226,173,.24)}
#page-snapshots .ss-badge.warn{border-color:rgba(255,181,92,.24)}
#page-snapshots .ss-badge.add{border-color:rgba(48,255,138,.35)}
#page-snapshots .ss-badge.del{border-color:rgba(255,80,80,.35)}
#page-snapshots .ss-badge.upd{border-color:rgba(255,180,80,.35)}
#page-snapshots .ss-mini{display:inline-flex;align-items:center;justify-content:center;min-height:24px;padding:0 9px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.04);color:#eef3ff;font-size:11px;font-weight:800}
#page-snapshots .ss-right{display:flex;align-items:center;gap:10px}
#page-snapshots .ss-item-right{display:grid;gap:8px;justify-items:end}
#page-snapshots .ss-item-action{display:inline-flex;align-items:center;gap:8px}
#page-snapshots .ss-chk{width:18px;height:18px;accent-color:#6f6cff}
#page-snapshots .ss-ab{display:inline-flex;align-items:center;justify-content:center;min-width:22px;height:22px;border-radius:999px;border:1px solid rgba(255,255,255,.14);font-size:11px;font-weight:900;letter-spacing:.03em;color:#f4f7ff}
#page-snapshots .ss-ab.a{border-color:rgba(92,96,182,.30);background:rgba(92,96,182,.08)}
#page-snapshots .ss-ab.b{border-color:rgba(255,180,80,.38);background:rgba(255,180,80,.08)}
#page-snapshots .ss-item .chev{opacity:.5;font-size:20px;line-height:1}
#page-snapshots .ss-empty{padding:24px;border-radius:18px;border:1px dashed rgba(255,255,255,.14);text-align:center;color:var(--ss-muted-fg);background:rgba(255,255,255,.02)}
#page-snapshots .ss-picked{display:grid;grid-template-columns:1fr 1fr;gap:10px}
#page-snapshots .ss-pick-card{padding:12px;border-radius:18px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.08);min-width:0;cursor:grab;user-select:none}
#page-snapshots .ss-pick-date{font-weight:900;font-size:16px}
#page-snapshots .ss-pick-meta{margin-top:6px;font-size:12px;color:var(--ss-muted-fg)}
#page-snapshots .ss-pick-card.dragging{opacity:.65}
#page-snapshots [data-coll-body="compare"]{overflow-x:hidden}
#page-snapshots .ss-code{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace;font-size:11px}
#page-snapshots .ss-code{white-space:pre-wrap;word-break:break-word;line-height:1.35;padding:10px;border-radius:14px;border:1px solid rgba(255,255,255,.08);background:rgba(0,0,0,.20);margin-top:8px}
#page-snapshots .ss-diff-summary{display:flex;flex-wrap:wrap;gap:8px;align-items:center;justify-content:center}
#page-snapshots .ss-diff-summary .material-symbols-rounded{font-size:16px;line-height:1;color:#aebfff}
#page-snapshots .ss-coll-head{display:flex;align-items:center;gap:10px;cursor:pointer}
#page-snapshots .ss-coll-head:focus{outline:2px solid rgba(255,255,255,.18);outline-offset:4px;border-radius:14px}
#page-snapshots .ss-coll-ico{margin-left:auto;opacity:.7;transition:transform .12s ease}
#page-snapshots .ss-card.is-collapsed .ss-coll-ico{transform:rotate(-90deg)}
#page-snapshots .ss-coll-body{margin-top:12px}
#page-snapshots .ss-selected-card{padding:12px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(9,10,16,.78),rgba(3,4,8,.88))}
#page-snapshots .ss-selected-empty{display:grid;gap:8px;justify-items:start}
#page-snapshots .ss-selected-title{font-weight:850;font-size:15px;color:#f4f7ff}
@media (max-width:1280px){#page-snapshots .ss-wrap{grid-template-columns:minmax(0,1fr) minmax(0,1fr)}#page-snapshots .ss-col{grid-column:1 / -1}}
@media (max-width:900px){#page-snapshots .ss-wrap{grid-template-columns:1fr}#page-snapshots .ss-top{padding:14px}#page-snapshots .ss-topstats{width:100%;justify-content:flex-start}#page-snapshots .ss-hero-grid{grid-template-columns:1fr 1fr}}
@media (max-width:640px){#page-snapshots .ss-grid2,#page-snapshots .ss-picked,#page-snapshots .ss-hero-grid{grid-template-columns:1fr}#page-snapshots .ss-item{grid-template-columns:1fr}#page-snapshots .ss-item-right{justify-items:start}#page-snapshots .ss-item.child{margin-left:10px}}
  `;
  const cssTuning = `
#page-snapshots .ss-wrap{grid-template-columns:320px minmax(0,1fr) 340px}
#page-snapshots .ss-toolbar{display:grid;gap:10px;margin-bottom:12px;position:relative;z-index:9}
#page-snapshots .ss-list-head{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;flex-wrap:wrap}
#page-snapshots .ss-list-head .ss-headsub{max-width:60ch}
#page-snapshots .ss-inline-pills{display:flex;gap:8px;flex-wrap:wrap}
#page-snapshots .ss-inline-pills .ss-pill{min-height:36px;padding:0 14px;font-size:12px;font-weight:700;border-radius:999px}
#page-snapshots .ss-steps{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:10px}
#page-snapshots .ss-step{padding:10px 12px;border-radius:16px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(10,11,18,.68),rgba(4,5,9,.88))}
#page-snapshots .ss-step-num{font-size:11px;font-weight:900;letter-spacing:.08em;color:#f2f5ff;text-transform:uppercase}
#page-snapshots .ss-step-label{margin-top:4px;font-size:12px;color:var(--ss-muted-fg);font-weight:800}
#page-snapshots .ss-comparehint{align-items:center;padding:10px 12px;border-radius:14px;margin:0;background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.018))}
#page-snapshots .ss-card.ss-overflow{z-index:40}
#page-snapshots .ss-field.ss-open,#page-snapshots .ss-bsel.is-open{z-index:120}
#page-snapshots .cw-icon-select{flex:1 1 auto;min-width:0;width:100%}
#page-snapshots .cw-icon-select-btn{width:100%;min-height:40px;padding:0 42px 0 22px;background:transparent;border:0;border-radius:0;box-shadow:none;color:inherit}
#page-snapshots .cw-icon-select-btn:focus-visible{box-shadow:none}
#page-snapshots .cw-icon-select-main{gap:10px}
#page-snapshots .cw-icon-select-icons{gap:10px}
#page-snapshots .cw-icon-select-label{text-align:left}
#page-snapshots .cw-icon-select-caret{opacity:.6}
#page-snapshots .cw-icon-select-icon{width:18px;height:18px;border-radius:7px;border:1px solid rgba(255,255,255,.16);background:rgba(0,0,0,.18)}
#page-snapshots .cw-icon-select-menu{z-index:160;background:linear-gradient(180deg,rgba(14,17,28,.99),rgba(7,10,18,.995))!important;box-shadow:0 24px 44px rgba(0,0,0,.52)!important;backdrop-filter:blur(14px) saturate(125%);-webkit-backdrop-filter:blur(14px) saturate(125%)}
#page-snapshots .ss-list{gap:8px;max-height:640px}
#page-snapshots .ss-list,#page-snapshots .ss-detail-pre{scrollbar-width:thin;scrollbar-color:#8b5cf6 #10131a}
#page-snapshots .ss-list::-webkit-scrollbar,#page-snapshots .ss-detail-pre::-webkit-scrollbar{width:8px;height:8px}
#page-snapshots .ss-list::-webkit-scrollbar-corner,#page-snapshots .ss-detail-pre::-webkit-scrollbar-corner{background:transparent}
#page-snapshots .ss-list::-webkit-scrollbar-track,#page-snapshots .ss-detail-pre::-webkit-scrollbar-track{background:rgba(255,255,255,.04);border-radius:12px;box-shadow:inset 0 0 0 1px rgba(255,255,255,.08)}
#page-snapshots .ss-list::-webkit-scrollbar-thumb,#page-snapshots .ss-detail-pre::-webkit-scrollbar-thumb{border-radius:12px;background:linear-gradient(180deg,#8b5cf6 0%,#3b82f6 100%);border:2px solid #14161c;box-shadow:inset 0 0 0 1px rgba(139,92,246,.35),0 0 10px rgba(139,92,246,.55),0 0 18px rgba(59,130,246,.4)}
#page-snapshots .ss-list::-webkit-scrollbar-thumb:hover,#page-snapshots .ss-detail-pre::-webkit-scrollbar-thumb:hover{background:linear-gradient(180deg,#a78bfa 0%,#60a5fa 100%);box-shadow:inset 0 0 0 1px rgba(139,92,246,.45),0 0 14px rgba(139,92,246,.7),0 0 26px rgba(59,130,246,.55)}
#page-snapshots .ss-item{border-color:rgba(255,255,255,.07);background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.018))}
#page-snapshots .ss-item:hover{border-color:rgba(255,255,255,.12);background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.026))}
#page-snapshots .ss-item.active{border-color:rgba(92,96,182,.30);box-shadow:0 0 0 1px rgba(92,96,182,.12),0 12px 24px rgba(0,0,0,.22)}
#page-snapshots .ss-item-main{gap:7px}
#page-snapshots .ss-file{font-weight:700;color:rgba(236,240,255,.82)}
#page-snapshots .ss-path{opacity:.46}
#page-snapshots .ss-item .chev{opacity:.45}
#page-snapshots .ss-create-actions{display:grid;grid-template-columns:1fr;gap:10px}
#page-snapshots #ss-create{min-height:48px;font-weight:900}
#page-snapshots #ss-create:hover:not(:disabled){filter:brightness(1.04);transform:translateY(-1px)}
#page-snapshots .ss-queue{display:grid;gap:10px;margin-top:14px;padding-top:14px;border-top:1px solid rgba(255,255,255,.08)}
#page-snapshots .ss-queue-head{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;flex-wrap:wrap}
#page-snapshots .ss-queue-title{font-size:13px;font-weight:900;color:#f3f6ff}
#page-snapshots .ss-queue-note{font-size:12px;line-height:1.45;color:var(--ss-muted-fg);max-width:34ch}
#page-snapshots .ss-queue-actions{display:flex;gap:8px;flex-wrap:wrap}
#page-snapshots .ss-queue-actions .btn{min-width:120px}
#page-snapshots .ss-queue-list{display:grid;gap:8px}
#page-snapshots .ss-queue-item{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;padding:10px 12px;border-radius:16px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.018))}
#page-snapshots .ss-queue-copy{min-width:0;display:grid;gap:4px}
#page-snapshots .ss-queue-main{font-size:12px;font-weight:800;color:#eef3ff;text-transform:uppercase;letter-spacing:.04em}
#page-snapshots .ss-queue-sub{font-size:12px;color:var(--ss-muted-fg);word-break:break-word}
#page-snapshots .ss-filterbar{display:flex;align-items:center;gap:10px;flex-wrap:nowrap}
#page-snapshots .ss-filtermini{position:relative;display:flex;align-items:center;min-height:42px;height:42px;padding:0 14px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.02));box-shadow:inset 0 1px 0 rgba(255,255,255,.03);flex:1 1 0;min-width:0;transition:border-color .14s ease,background .14s ease,box-shadow .14s ease}
#page-snapshots #ss-filter-provider-wrap{flex:1.2 1 0}
#page-snapshots #ss-filter-feature-wrap{flex:1.05 1 0}
#page-snapshots #ss-filter-kind-wrap{flex:.8 1 0}
#page-snapshots .ss-filtermini.active{border-color:rgba(92,96,182,.30);background:linear-gradient(180deg,rgba(92,96,182,.09),rgba(255,255,255,.02));box-shadow:0 0 0 1px rgba(92,96,182,.10),inset 0 1px 0 rgba(255,255,255,.03)}
#page-snapshots .ss-filtermini select{width:100%;height:40px;padding:0 42px 0 22px;border:0;background:transparent;color:#f3f6ff;font:inherit;outline:0;appearance:none}
#page-snapshots .ss-filtermini select option{background:#141418;color:#f3f3f5}
#page-snapshots .ss-filterchev{position:absolute;right:14px;opacity:.72;pointer-events:none}
#page-snapshots .ss-filterclear{flex:0 0 auto;min-height:42px;height:42px;padding:0 16px;border-radius:999px;white-space:nowrap}
#page-snapshots .ss-filterclear.active{border-color:rgba(92,96,182,.30)}
#page-snapshots .ss-restore-modebar{display:grid;gap:10px;margin-top:12px}
#page-snapshots .ss-restore-modes{display:grid;grid-template-columns:1fr 1fr;gap:8px}
#page-snapshots .ss-modebtn{display:grid;gap:2px;justify-items:start;padding:12px 14px;border-radius:16px;border:1px solid rgba(255,255,255,.10);background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.025));color:#eef3ff;text-align:left;cursor:pointer}
#page-snapshots .ss-modebtn strong{font-size:13px;font-weight:900}
#page-snapshots .ss-modebtn span{font-size:12px;color:var(--ss-muted-fg)}
#page-snapshots .ss-modebtn.active{border-color:rgba(92,96,182,.34);background:linear-gradient(180deg,rgba(92,96,182,.12),rgba(255,255,255,.03));box-shadow:0 0 0 1px rgba(92,96,182,.12)}
#page-snapshots .ss-restore-warning{padding:10px 12px;border-radius:14px;border:1px solid rgba(255,181,92,.18);background:rgba(255,181,92,.06);font-size:12px;line-height:1.45;color:rgba(255,222,181,.94)}
#page-snapshots .ss-selected-summary{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;flex-wrap:wrap}
#page-snapshots .ss-selected-count{display:grid;justify-items:end;gap:2px;padding:8px 12px;border-radius:16px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.03)}
#page-snapshots .ss-selected-count strong{font-size:24px;line-height:1;color:#f7f9ff}
#page-snapshots .ss-selected-count span{font-size:11px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:var(--ss-muted-fg)}
#page-snapshots .ss-selected-meta{margin-top:10px;display:grid;gap:6px}
#page-snapshots .ss-selected-kv{font-size:12px;color:var(--ss-muted-fg)}
#page-snapshots .ss-selected-kv b{color:#eef3ff;font-weight:800;margin-right:6px}
#page-snapshots .ss-selected-stats{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:12px}
#page-snapshots .ss-selected-stat{display:grid;gap:3px;padding:10px 12px;border-radius:16px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.018))}
#page-snapshots .ss-selected-stat strong{font-size:18px;line-height:1;color:#f4f7ff}
#page-snapshots .ss-selected-stat span{font-size:12px;color:var(--ss-muted-fg);text-transform:lowercase}
#page-snapshots .ss-card[data-coll="restore"] .hidden{display:none!important}
@media (max-width:900px){#page-snapshots .ss-wrap{grid-template-columns:1fr}#page-snapshots .ss-steps{grid-template-columns:1fr 1fr}#page-snapshots .ss-restore-modes{grid-template-columns:1fr}#page-snapshots .ss-selected-stats{grid-template-columns:1fr 1fr}#page-snapshots .ss-filterbar{flex-wrap:wrap}}
@media (max-width:640px){#page-snapshots .ss-steps{grid-template-columns:1fr}#page-snapshots .ss-filterbar{flex-direction:column;align-items:stretch}#page-snapshots .ss-filtermini,#page-snapshots #ss-filter-provider-wrap,#page-snapshots #ss-filter-feature-wrap,#page-snapshots #ss-filter-kind-wrap{min-width:0;flex:1 1 auto}#page-snapshots .ss-filterclear{width:100%}#page-snapshots .ss-queue-item{flex-direction:column;align-items:stretch}#page-snapshots .ss-queue-actions{width:100%}#page-snapshots .ss-queue-actions .btn{flex:1 1 160px}#page-snapshots .ss-selected-stats{grid-template-columns:1fr}}
  `;
  const cssCaptureLock = `
#page-snapshots .ss-lockable{transition:opacity .14s ease,filter .14s ease,border-color .14s ease}
#page-snapshots .ss-lockable.ss-locked{opacity:.56;filter:saturate(.72)}
#page-snapshots .ss-lockable.ss-locked .ss-coll-head,#page-snapshots .ss-lockable.ss-locked .ss-coll-body,#page-snapshots .ss-lockable.ss-locked .ss-list,#page-snapshots .ss-lockable.ss-locked #ss-list-footer{pointer-events:none}
#page-snapshots .ss-lockmsg{display:flex;align-items:center;gap:10px;padding:10px 12px;border-radius:14px;border:1px solid rgba(48,255,138,.18);background:rgba(48,255,138,.07);font-size:12px;line-height:1.45;color:rgba(224,255,236,.94);margin:0}
#page-snapshots .ss-lockmsg.hidden{display:none}
#page-snapshots .ss-lockmsg .material-symbol{font-size:18px;opacity:.9;color:#d8ffe6}
#page-snapshots .ss-capture-running .ss-item{cursor:not-allowed}
#page-snapshots .ss-capture-running .ss-item:hover{transform:none}
  `;
  const cssRedesign = `
#page-snapshots .ss-top{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:start}
#page-snapshots .ss-topstats{grid-column:1 / -1;width:100%;display:grid;grid-template-columns:repeat(5,minmax(150px,1fr));gap:12px;margin:10px 0 0}
#page-snapshots .ss-summary-card{display:flex;align-items:center;gap:12px;min-height:78px;padding:13px 14px;border-radius:18px;border:1px solid var(--ss-border);background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.022));box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
#page-snapshots .ss-summary-icon{width:42px;height:42px;display:grid;place-items:center;border-radius:16px;background:rgba(124,92,255,.14);color:#c9bbff}
#page-snapshots .ss-summary-icon .material-symbols-rounded{font-size:23px}
#page-snapshots .ss-summary-copy{min-width:0;display:grid;gap:2px}
#page-snapshots .ss-summary-label{font-size:11px;font-weight:850;color:var(--ss-muted-fg);text-transform:uppercase;letter-spacing:.04em}
#page-snapshots .ss-summary-value{font-size:22px;font-weight:950;color:var(--ss-fg);line-height:1.05;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#page-snapshots .ss-summary-sub{font-size:12px;color:var(--ss-muted-fg);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#page-snapshots .ss-wrap{grid-template-columns:320px minmax(620px,1fr) 340px}
#page-snapshots .ss-create-flow{display:grid;gap:12px}
#page-snapshots .ss-flow-step{display:grid;gap:10px;padding:12px;border-radius:16px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.025)}
#page-snapshots .ss-flow-head{display:flex;align-items:center;gap:10px}
#page-snapshots .ss-flow-num{width:24px;height:24px;display:grid;place-items:center;border-radius:999px;background:rgba(124,92,255,.18);border:1px solid rgba(124,92,255,.24);font-size:12px;font-weight:900;color:#f6f3ff}
#page-snapshots .ss-flow-title{font-size:13px;font-weight:900;color:var(--ss-fg)}
#page-snapshots .ss-review-line{display:flex;align-items:center;justify-content:space-between;gap:10px;font-size:12px;color:var(--ss-muted-fg)}
#page-snapshots .ss-review-line b{color:var(--ss-fg)}
#page-snapshots .ss-create-actions{display:grid;grid-template-columns:1fr;gap:8px;margin-top:4px}
#page-snapshots .ss-browser-card{padding:0;overflow:hidden}
#page-snapshots .ss-browser-card .ss-card-head,#page-snapshots .ss-browser-card .ss-toolbar,#page-snapshots .ss-browser-card #ss-list-footer{padding-left:14px;padding-right:14px}
#page-snapshots .ss-browser-card .ss-card-head{padding-top:14px}
#page-snapshots .ss-toolbar{display:grid;gap:10px;margin:0 0 12px;position:relative;z-index:9}
#page-snapshots .ss-filterbar{display:grid;grid-template-columns:minmax(190px,1.4fr) minmax(130px,1fr) minmax(120px,.9fr) minmax(100px,auto);gap:10px}
#page-snapshots .ss-table-wrap{max-height:640px;overflow:auto;border-top:1px solid rgba(255,255,255,.08);border-bottom:1px solid rgba(255,255,255,.08)}
#page-snapshots .ss-table{width:100%;border-collapse:separate;border-spacing:0;table-layout:fixed;font-size:12px}
#page-snapshots .ss-table th{position:sticky;top:0;z-index:3;padding:10px 8px;text-align:left;background:rgba(12,15,24,.96);border-bottom:1px solid rgba(255,255,255,.08);color:var(--ss-muted-fg);font-weight:900;letter-spacing:.04em;text-transform:uppercase}
#page-snapshots .ss-table td{padding:10px 8px;border-bottom:1px solid rgba(255,255,255,.07);color:var(--ss-fg);vertical-align:middle;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#page-snapshots .ss-table tr{cursor:pointer}
#page-snapshots .ss-table tr:hover td{background:rgba(255,255,255,.035)}
#page-snapshots .ss-table tr.active td{background:rgba(92,96,182,.10)}
#page-snapshots .ss-table tr.checked td{background:rgba(92,96,182,.13)}
#page-snapshots .ss-table .ss-col-check{width:74px;text-align:center}
#page-snapshots .ss-table .ss-col-provider{width:108px}
#page-snapshots .ss-table .ss-col-feature{width:220px}
#page-snapshots .ss-table .ss-col-type{width:90px}
#page-snapshots .ss-table .ss-col-label{width:150px}
#page-snapshots .ss-table .ss-col-created{width:150px}
#page-snapshots .ss-table .ss-col-profile{width:110px}
#page-snapshots .ss-table .ss-col-menu{width:38px;text-align:center}
#page-snapshots .ss-row-muted{color:var(--ss-muted-fg)!important}
#page-snapshots .ss-compare-state{display:flex;align-items:flex-start;gap:10px;padding:11px 12px;border-radius:14px;border:1px solid rgba(92,147,255,.22);background:rgba(54,115,210,.08);font-size:12px;line-height:1.4;color:var(--ss-muted-fg);margin-bottom:10px}
#page-snapshots .ss-compare-state.ok{border-color:rgba(87,181,138,.36);background:rgba(87,181,138,.10)}
#page-snapshots .ss-compare-state .material-symbols-rounded{font-size:20px;color:#9ebdff}
#page-snapshots .ss-compare-state.ok .material-symbols-rounded{color:#8be0b4}
#page-snapshots .ss-tools-list{display:grid;gap:8px}
#page-snapshots .ss-tool-btn{width:100%;display:grid;grid-template-columns:auto minmax(0,1fr) auto;align-items:center;gap:10px;padding:10px 11px;border-radius:14px;border:1px solid rgba(255,255,255,.09);background:rgba(255,255,255,.03);color:var(--ss-fg);text-align:left;cursor:pointer}
#page-snapshots .ss-tool-btn .material-symbols-rounded{font-size:19px;color:var(--ss-muted-fg)}
#page-snapshots .ss-tool-main{display:grid;gap:2px;min-width:0}
#page-snapshots .ss-tool-title{font-size:13px;font-weight:850;color:var(--ss-fg);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#page-snapshots .ss-tool-sub{font-size:12px;color:var(--ss-muted-fg);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#page-snapshots .ss-tool-btn.danger{background:#432630!important;border-color:rgba(184,91,105,.34)!important;color:#f2d9de!important}
#page-snapshots .ss-tool-btn.danger:hover{background:#4b2a34!important;border-color:rgba(184,91,105,.44)!important}
#page-snapshots .ss-tool-btn.danger .ss-tool-title,#page-snapshots .ss-tool-btn.danger .material-symbols-rounded:first-child{color:#d99aa4}
#page-snapshots .ss-restore-modes{grid-template-columns:1fr}
#page-snapshots .ss-modebtn{grid-template-columns:minmax(0,1fr);align-items:center}
#page-snapshots .ss-target-label{font-size:12px;font-weight:850;color:var(--ss-muted-fg);margin:10px 0 6px}
#page-snapshots .ss-detail-pre{max-height:220px;overflow:auto}
#page-snapshots,#page-snapshots.card{max-width:none!important;width:100%!important;grid-column:1/-1!important;padding:0!important;--ss-page:var(--panel);--ss-panel:var(--panel);--ss-panel-strong:var(--cw-theme-surface-raised);--ss-border:var(--border);--ss-fg:var(--fg);--ss-muted-fg:var(--muted);background:transparent!important;background-image:none!important;border:0!important;box-shadow:none!important;outline:0!important;overflow:visible!important;color:var(--fg)!important}
body:has(#page-snapshots:not(.hidden)),body:has(#page-snapshots:not(.hidden)) :is(main,#app,#content,#pages,.page-wrap,.view,.shell-main){background:var(--bg)!important;background-image:none!important}
#page-snapshots .ss-top:not(.cw-page-hero){display:flex!important;align-items:flex-start!important;justify-content:space-between!important;gap:16px!important;padding:18px 20px!important;margin:0 0 18px!important;border:1px solid var(--ss-border)!important;border-radius:20px!important;background:var(--panel)!important;background-image:none!important;box-shadow:none!important;backdrop-filter:none!important;-webkit-backdrop-filter:none!important}
#page-snapshots .ss-top.cw-page-hero{display:grid!important;grid-template-columns:minmax(0,1fr) auto!important;align-items:center!important;gap:18px!important;min-height:112px!important;padding:20px 30px!important;margin:0 0 16px!important;border:1px solid rgba(95,111,214,.26)!important;border-radius:22px!important;background:radial-gradient(820px 250px at 100% 0%,rgba(112,96,245,.10),transparent 68%),linear-gradient(100deg,#111821 0%,#151d31 48%,#181a38 100%)!important;box-shadow:none!important;overflow:hidden!important;isolation:isolate!important;position:relative!important}
#page-snapshots .ss-top.cw-page-hero::before{content:""!important;display:block!important;position:absolute!important;inset:0!important;z-index:-2!important;background:linear-gradient(90deg,rgba(255,255,255,.035) 0 1px,transparent 1px 100%),linear-gradient(180deg,rgba(255,255,255,.026) 0 1px,transparent 1px 100%)!important;background-size:42px 42px!important;opacity:.34!important}
#page-snapshots .ss-card::before{content:none!important;display:none!important}
#page-snapshots .ss-top-copy{display:block!important;min-width:0!important}
#page-snapshots .ss-title{font-size:28px!important;letter-spacing:0!important;line-height:1!important;color:#f4f8ff!important}
#page-snapshots .ss-sub{font-size:13px!important;color:#aebcd0!important;max-width:760px!important}
#page-snapshots .ss-actions{display:flex!important;align-items:center!important;gap:8px!important;flex:0 0 auto!important}
#page-snapshots .ss-help-btn{width:38px!important;min-width:38px!important;height:38px!important;padding:0!important;border-radius:12px!important;background:rgba(13,17,27,.56)!important;border-color:rgba(255,255,255,.14)!important;color:rgba(245,248,255,.92)!important;font-size:0!important;box-shadow:none!important;backdrop-filter:blur(6px) saturate(120%)!important;-webkit-backdrop-filter:blur(6px) saturate(120%)!important}
#page-snapshots .ss-help-btn .material-symbol{font-size:20px!important}
#page-snapshots .ss-topstats{display:grid!important;grid-template-columns:repeat(5,minmax(0,1fr))!important;gap:16px!important;margin:0 0 18px!important}
#page-snapshots .ss-summary-card{min-height:96px!important;padding:16px 18px!important;border-radius:8px!important;background:var(--cw-theme-surface-raised)!important;background-image:none!important;border-color:var(--ss-border)!important;box-shadow:none!important}
#page-snapshots .ss-summary-icon{width:58px!important;height:58px!important;border-radius:24px!important;background:rgba(124,92,255,.24)!important;color:#9b7cff!important}
#page-snapshots .ss-summary-card[data-stat="providers"] .ss-summary-icon{background:rgba(45,161,255,.18)!important;color:#48a3ff!important}
#page-snapshots .ss-summary-card[data-stat="full-sets"] .ss-summary-icon{background:rgba(25,195,125,.18)!important;color:#37db91!important}
#page-snapshots .ss-summary-card[data-stat="latest"] .ss-summary-icon{background:rgba(255,145,32,.18)!important;color:#ffa526!important}
#page-snapshots .ss-summary-card[data-stat="scheduled"] .ss-summary-icon{background:rgba(124,92,255,.24)!important;color:#9f7cff!important}
#page-snapshots .ss-summary-icon .material-symbols-rounded{font-size:30px!important}
#page-snapshots .ss-summary-label{text-transform:none!important;letter-spacing:0!important;font-size:12px!important;font-weight:800!important;color:#aebcd0!important}
#page-snapshots .ss-summary-value{font-size:22px!important;color:#f7fbff!important}
#page-snapshots .ss-summary-card[data-stat="latest"] .ss-summary-value{font-size:18px!important;overflow:visible!important;text-overflow:clip!important}
#page-snapshots .ss-summary-sub{font-size:12px!important;color:#b8c7dc!important}
#page-snapshots .ss-wrap{display:grid!important;grid-template-columns:320px minmax(560px,1fr) 340px!important;gap:16px!important;align-items:start!important}
#page-snapshots .ss-center-col{display:flex;flex-direction:column;gap:10px;min-width:0}
#page-snapshots .ss-card{border-radius:8px!important;background:var(--panel)!important;background-image:none!important;border-color:var(--ss-border)!important;box-shadow:none!important;padding:16px!important}
#page-snapshots .ss-card.ss-accent{background:var(--panel)!important;background-image:none!important;border-color:var(--ss-border)!important}
#page-snapshots .ss-card h3{font-size:16px!important;letter-spacing:0!important;text-transform:none!important;color:#f2f7ff!important}
#page-snapshots .ss-headsub{font-size:13px!important;line-height:1.55!important;color:#aebcd0!important}
#page-snapshots .ss-card-head{margin-bottom:14px!important}
#page-snapshots .ss-stepper{display:grid!important;grid-template-columns:repeat(3,minmax(0,1fr))!important;gap:0!important;margin:4px 0 18px!important;padding:0 6px!important;position:relative!important}
#page-snapshots .ss-create-flow .ss-stepper{background:transparent!important;background-image:none!important;border:0!important;border-radius:0!important;box-shadow:none!important;overflow:visible!important}
#page-snapshots .ss-stepper::before{content:""!important;position:absolute!important;left:36px!important;right:36px!important;top:13px!important;height:1px!important;background:rgba(158,169,188,.34)!important}
#page-snapshots .ss-step{position:relative!important;z-index:1!important;display:grid!important;justify-items:center!important;gap:6px!important;padding:0!important;border:0!important;border-radius:0!important;background:transparent!important;box-shadow:none!important;font-size:11px!important;color:#8f9bb0!important}
#page-snapshots .ss-create-flow .ss-stepper .ss-step{background:transparent!important;background-image:none!important;border:0!important;border-radius:0!important;box-shadow:none!important;padding:0!important;min-height:0!important}
#page-snapshots .ss-step span{width:26px!important;height:26px!important;display:grid!important;place-items:center!important;border-radius:999px!important;background:var(--cw-theme-surface-raised)!important;border:1px solid rgba(255,255,255,.14)!important;box-shadow:0 0 0 3px var(--panel)!important;font-size:12px!important;font-weight:900!important;color:#8f9bb0!important;line-height:1!important}
#page-snapshots .ss-step.on span{background:#6b58ef!important;border-color:#7d6dff!important;color:#fff!important;box-shadow:0 0 0 3px var(--panel),0 8px 18px rgba(107,88,239,.22)!important}
#page-snapshots .ss-step b{display:block!important;font-size:11px!important;font-weight:850!important;line-height:1.2!important;color:#9aa8bd!important;text-align:center!important}
#page-snapshots .ss-step.on b{color:#8e7dff!important}
#page-snapshots .ss-create-flow{display:block!important}
#page-snapshots .ss-create-fields{display:grid;gap:10px}
#page-snapshots .ss-create-fields label{display:grid;gap:6px;font-size:12px;font-weight:800;color:#b8c7dc}
#page-snapshots .ss-create-fields small{margin-top:-4px;text-align:center;font-size:11px;color:#8391a6}
#page-snapshots .ss-field,#page-snapshots .ss-filtermini,#page-snapshots .ss-row>select.input,#page-snapshots .ss-row>input.input,#page-snapshots input.input{min-height:42px!important;height:42px!important;border-radius:7px!important;background:#0e1521!important;border:1px solid rgba(89,109,139,.30)!important;box-shadow:none!important}
#page-snapshots .ss-field select,#page-snapshots .ss-field input,#page-snapshots .ss-filtermini select,#page-snapshots input.input{height:40px!important;color:#e9f1ff!important}
#page-snapshots .ss-field:has(select)::after,#page-snapshots .ss-field:has(.cw-icon-select)::after,#page-snapshots .ss-filtermini::after{content:none!important;display:none!important}
#page-snapshots .ss-field .chev,#page-snapshots .ss-filterchev,#page-snapshots .ss-field .chev::before,#page-snapshots .ss-filterchev::before{content:none!important;display:none!important}
#page-snapshots .ss-field select:not(.cw-icon-select-native),#page-snapshots .ss-filtermini select:not(.cw-icon-select-native),#page-snapshots .ss-row>select.input:not(.cw-icon-select-native),#page-snapshots select.input:not(.cw-icon-select-native){appearance:auto!important;-webkit-appearance:auto!important;background-image:initial!important}
#page-snapshots .ss-field>select:not(.hidden),#page-snapshots .ss-filtermini>select:not(.hidden),#page-snapshots .ss-row>select.input:not(.hidden){padding-right:30px!important;background-image:initial!important}
#page-snapshots .ss-field .cw-icon-select-btn{padding-right:38px!important}
#page-snapshots .ss-field .cw-icon-select-caret,#page-snapshots .ss-filtermini .cw-icon-select-caret{display:inline-flex!important;opacity:.72!important}
#page-snapshots .cw-icon-select-native{display:none!important}
#page-snapshots :is(.ss-field,.ss-filtermini) .cw-icon-select{width:100%!important;height:100%!important;min-width:0!important}
#page-snapshots :is(.ss-field,.ss-filtermini) .cw-icon-select-btn,#page-snapshots :is(.ss-field,.ss-filtermini) .cw-icon-select-btn:is(:hover,:focus,:focus-visible),html[data-cw-theme] #page-snapshots :is(.ss-field,.ss-filtermini) .cw-icon-select-btn{height:100%!important;min-height:0!important;width:100%!important;background:transparent!important;background-image:none!important;border:0!important;border-radius:0!important;box-shadow:none!important;outline:0!important;color:inherit!important;-webkit-text-fill-color:inherit!important}
#page-snapshots .ss-create-fields .ss-field{height:44px!important;min-height:44px!important;border-radius:16px!important;padding:0 16px!important;overflow:hidden!important;background:var(--cw-theme-input)!important}
#page-snapshots .ss-create-fields .ss-field>select,#page-snapshots .ss-create-fields .ss-field>input,#page-snapshots .ss-create-fields .ss-field .cw-icon-select,#page-snapshots .ss-create-fields .ss-field .cw-icon-select-btn{width:100%!important;height:100%!important;min-height:0!important;background:transparent!important;background-image:none!important;border:0!important;border-radius:0!important;box-shadow:none!important;color:#e9f1ff!important}
#page-snapshots .ss-create-fields .ss-field>select{appearance:auto!important;-webkit-appearance:auto!important;padding:0 30px 0 0!important}
#page-snapshots .ss-create-fields .ss-field>input{padding:0!important}
#page-snapshots .ss-create-fields .ss-field .cw-icon-select-btn{padding:0 30px 0 0!important}
#page-snapshots #ss-create-review{margin:14px 0 12px!important;border-radius:7px!important;background:#182233!important;border-color:rgba(89,109,139,.28)!important}
#page-snapshots .ss-review-line{font-size:12px!important}
#page-snapshots .ss-create-actions .btn{min-height:48px!important;border-radius:16px!important;font-weight:900!important;display:flex!important;align-items:center!important;justify-content:center!important;gap:8px!important}
#page-snapshots .ss-create-actions .material-symbols-rounded{font-size:20px!important;line-height:1!important;flex:0 0 auto!important}
#page-snapshots .ss-create-actions .ss-btn-label{display:inline-block!important;line-height:1.1!important}
#page-snapshots #ss-create{background:#1f5b43!important;background-image:none!important;border-color:rgba(95,215,155,.72)!important;color:#eafff4!important;-webkit-text-fill-color:#eafff4!important;box-shadow:inset 0 0 0 1px rgba(95,215,155,.12)!important}
#page-snapshots #ss-create:hover:not(:disabled){background:#24694e!important;border-color:rgba(116,232,174,.82)!important;color:#fff!important;-webkit-text-fill-color:#fff!important}
#page-snapshots #ss-add-schedule{background:#563bd9!important;border-color:#674eea!important;color:#fff!important}
#page-snapshots .ss-queue{margin-top:16px!important;padding-top:16px!important}
#page-snapshots .ss-queue-title{font-size:16px!important;color:#f2f7ff!important}
#page-snapshots .ss-browser-card{padding:0!important}
#page-snapshots .ss-browser-card .ss-card-head{padding:16px 16px 0!important;margin-bottom:8px!important}
#page-snapshots .ss-browser-card h3{font-size:15px!important;text-transform:none!important;letter-spacing:0!important}
#page-snapshots .ss-browser-card .ss-headsub{white-space:nowrap!important;overflow:hidden!important;text-overflow:ellipsis!important;max-width:none!important}
#page-snapshots .ss-toolbar{padding:0 16px 12px!important}
#page-snapshots .ss-filterbar{grid-template-columns:minmax(170px,1.15fr) minmax(150px,1fr) minmax(120px,.78fr) 88px!important;gap:10px!important}
#page-snapshots .ss-filtermini{min-width:0!important}
#page-snapshots .ss-filtermini>select.input:not(.hidden){padding-right:30px!important}
#page-snapshots .ss-filtermini .cw-icon-select-btn{padding-right:34px!important}
#page-snapshots .ss-filterclear{border-radius:7px!important;display:flex!important;align-items:center!important;justify-content:center!important;gap:6px!important}
#page-snapshots .ss-table-wrap{border-color:rgba(89,109,139,.20)!important;max-height:560px!important;overflow-x:hidden!important;overflow-y:auto!important}
#page-snapshots .ss-table{table-layout:fixed!important;min-width:0!important;font-size:11px!important}
#page-snapshots .ss-table th{background:var(--cw-theme-surface-raised)!important;color:#a9b0bd!important;border-color:var(--border)!important;font-size:11px!important;text-transform:none!important;letter-spacing:0!important;padding:8px 6px!important}
#page-snapshots .ss-table td{background:var(--cw-theme-surface-raised)!important;border-color:rgba(255,255,255,.10)!important;color:#eef1f6!important;padding:8px 6px!important}
#page-snapshots .ss-table .ss-col-check{width:54px!important;text-align:left!important}
#page-snapshots .ss-table th.ss-col-check{padding-left:14px!important;padding-right:8px!important;vertical-align:middle!important}
#page-snapshots .ss-table td.ss-col-check{display:table-cell!important;text-align:left!important;vertical-align:middle!important;padding-left:14px!important;padding-right:8px!important}
#page-snapshots .ss-table .ss-pickcell{display:flex!important;align-items:center!important;justify-content:flex-start!important;gap:7px!important;min-width:0!important}
#page-snapshots .ss-table td.ss-col-check .ss-chk,#page-snapshots .ss-table th.ss-col-check .ss-chk{width:16px!important;height:16px!important;min-width:16px!important;flex:0 0 16px!important;border-radius:5px!important}
#page-snapshots .ss-table input[type=checkbox].ss-chk{appearance:none!important;-webkit-appearance:none!important;box-sizing:border-box!important;width:14px!important;height:14px!important;min-width:14px!important;max-width:14px!important;min-height:14px!important;max-height:14px!important;flex:0 0 14px!important;margin:0!important;padding:0!important;border-radius:4px!important;border:1.5px solid rgba(142,157,185,.62)!important;background:transparent!important;position:relative!important;transform:none!important;accent-color:#7d86c9!important}
#page-snapshots .ss-table input[type=checkbox].ss-chk:checked{background:#8b92d6!important;border-color:#8b92d6!important}
#page-snapshots .ss-table input[type=checkbox].ss-chk:checked::after{content:""!important;position:absolute!important;left:50%!important;top:45%!important;width:3px!important;height:6px!important;border:solid #fff!important;border-width:0 1.6px 1.6px 0!important;transform:translate(-50%,-50%) rotate(45deg)!important}
#page-snapshots .ss-table td.ss-col-check .ss-ab{margin-right:0!important}
#page-snapshots .ss-table td.ss-col-check .ss-ab{cursor:pointer!important}
#page-snapshots .ss-table .ss-col-provider{width:112px!important}
#page-snapshots .ss-table .ss-col-feature{width:158px!important;color:#edf5ff!important;font-weight:850!important}
#page-snapshots .ss-table td.ss-col-feature{white-space:nowrap!important;overflow:hidden!important;text-overflow:clip!important}
#page-snapshots .ss-feature-cell{display:flex!important;align-items:center!important;gap:8px!important;min-width:0!important}
#page-snapshots .ss-feature-label{min-width:0!important;overflow:hidden!important;text-overflow:ellipsis!important;white-space:nowrap!important;color:#f3f8ff!important;font-weight:850!important}
#page-snapshots .ss-feature-cell .ss-mini{flex:0 0 auto!important}
#page-snapshots .ss-table .ss-col-type{width:58px!important}
#page-snapshots .ss-table .ss-col-label{width:96px!important}
#page-snapshots .ss-table .ss-col-created{width:122px!important}
#page-snapshots .ss-table .ss-col-profile{width:72px!important}
#page-snapshots .ss-table .ss-col-menu{width:24px!important}
#page-snapshots .ss-table tr.checked td{background:rgba(86,67,214,.25)!important}
#page-snapshots .ss-table tr.checked td.ss-col-check{background:rgba(86,67,214,.25)!important}
#page-snapshots .ss-empty{border-radius:8px!important;border-color:var(--border)!important;background:var(--cw-theme-surface-raised)!important;padding:28px!important}
#page-snapshots [data-coll="restore"],#page-snapshots [data-coll="compare"],#page-snapshots [data-coll="tools"]{background:var(--panel)!important;background-image:none!important}
#page-snapshots .ss-selected-card{border-radius:7px!important;background:rgba(255,255,255,.04)!important;border-color:rgba(89,109,139,.28)!important}
#page-snapshots .ss-restore-modes{grid-template-columns:repeat(2,minmax(0,1fr))!important;gap:10px!important}
#page-snapshots .ss-modebtn{min-width:0!important;min-height:96px!important;grid-template-columns:auto minmax(0,1fr)!important;grid-template-rows:auto 1fr!important;align-items:start!important;align-content:start!important;justify-items:start!important;column-gap:10px!important;row-gap:6px!important;border-radius:12px!important;background:rgba(15,22,34,.72)!important;border-color:rgba(89,109,139,.28)!important;padding:14px!important;text-align:left!important;overflow:hidden!important}
#page-snapshots .ss-modebtn strong{display:block!important;min-width:0!important;font-size:13px!important;line-height:1.25!important;white-space:normal!important;overflow-wrap:anywhere!important;color:#eaf2ff!important}
#page-snapshots .ss-modebtn span{grid-column:2!important;display:block!important;min-width:0!important;font-size:12px!important;line-height:1.35!important;white-space:normal!important;overflow-wrap:anywhere!important;color:#8f9db2!important}
#page-snapshots .ss-modebtn.active{background:rgba(86,67,214,.24)!important;border-color:#674eea!important}
#page-snapshots .ss-modebtn{grid-template-columns:minmax(0,1fr)!important;min-height:78px!important;padding:14px 16px!important}
#page-snapshots .ss-modebtn span{grid-column:auto!important}
#page-snapshots .ss-modebtn.active{background:rgba(32,88,74,.58)!important;border-color:rgba(67,201,153,.42)!important}
#page-snapshots #ss-restore{display:flex!important;align-items:center!important;justify-content:center!important;gap:8px!important;background:#563bd9!important;border-color:#674eea!important;color:#fff!important;border-radius:16px!important;min-height:48px!important;line-height:1!important}
#page-snapshots #ss-restore.is-confirming{position:relative!important;overflow:hidden!important;background:linear-gradient(135deg,#9b2637,#d15d3c)!important;border-color:rgba(255,160,115,.72)!important;color:#fff!important;-webkit-text-fill-color:#fff!important;animation:cwConnectionDeleteConfirmPulse .9s ease-in-out infinite alternate!important}
#page-snapshots #ss-restore.is-confirming::after{content:""!important;position:absolute!important;left:0!important;bottom:0!important;height:4px!important;width:var(--ss-confirm-progress,100%)!important;background:linear-gradient(90deg,#fff2b8,#ff9e7a)!important;box-shadow:0 0 12px rgba(255,230,150,.48)!important;transition:width .08s linear!important;z-index:0!important}
#page-snapshots #ss-restore.is-confirming>*{position:relative!important;z-index:1!important}
#page-snapshots #ss-restore .material-symbols-rounded{font-size:20px!important;line-height:1!important;display:inline-flex!important;align-items:center!important;justify-content:center!important}
#page-snapshots #ss-restore span:not(.material-symbols-rounded){display:inline-flex!important;align-items:center!important;line-height:1!important}
#page-snapshots .ss-compare-panel{display:grid!important;gap:10px!important}
#page-snapshots .ss-compare-state{display:grid!important;grid-template-columns:auto minmax(0,1fr) auto!important;align-items:center!important;gap:10px!important;border-radius:7px!important;background:#182233!important;border-color:rgba(89,109,139,.28)!important;margin:0!important;padding:12px!important}
#page-snapshots .ss-compare-state .material-symbols-rounded{width:28px!important;height:28px!important;display:grid!important;place-items:center!important;border-radius:999px!important;background:rgba(120,166,255,.14)!important;color:#9fc2ff!important;font-size:18px!important}
#page-snapshots .ss-compare-state.ok .material-symbols-rounded{background:rgba(87,181,138,.16)!important;color:#91e2ba!important}
#page-snapshots .ss-compare-state-copy{display:grid!important;gap:3px!important;min-width:0!important}
#page-snapshots .ss-compare-state-copy b{font-size:13px!important;line-height:1.2!important;color:#edf5ff!important}
#page-snapshots .ss-compare-state-copy span{font-size:12px!important;line-height:1.35!important;color:#aebcd0!important}
#page-snapshots .ss-compare-count{display:grid!important;justify-items:end!important;gap:1px!important;padding:6px 8px!important;min-width:58px!important;border-radius:7px!important;background:rgba(255,255,255,.04)!important;border:1px solid rgba(89,109,139,.24)!important}
#page-snapshots .ss-compare-count strong{font-size:18px!important;line-height:1!important;color:#fff!important}
#page-snapshots .ss-compare-count span{font-size:10px!important;line-height:1.1!important;color:#8f9db2!important;text-transform:uppercase!important;font-weight:850!important}
#page-snapshots .ss-compare-picks{display:grid!important;grid-template-columns:1fr!important;gap:8px!important}
#page-snapshots .ss-compare-empty{min-height:54px!important;display:flex!important;align-items:center!important;padding:10px 12px!important;border-radius:7px!important;border:1px dashed rgba(143,157,185,.24)!important;background:rgba(15,22,34,.52)!important}
#page-snapshots .ss-compare-empty div{display:grid!important;gap:3px!important}
#page-snapshots .ss-compare-empty b{font-size:12px!important;color:#dce8ff!important}
#page-snapshots .ss-compare-empty span{font-size:11px!important;color:#8f9db2!important}
#page-snapshots [data-coll="compare"] .ss-pick-card{display:grid!important;grid-template-columns:auto minmax(0,1fr)!important;gap:10px!important;align-items:center!important;border-radius:7px!important;background:#151e2e!important;border-color:rgba(89,109,139,.24)!important;padding:10px!important;cursor:grab!important}
#page-snapshots .ss-pick-tag{width:28px!important;height:28px!important;display:grid!important;place-items:center!important;border-radius:999px!important;background:rgba(86,67,214,.22)!important;border:1px solid rgba(119,104,255,.32)!important;color:#fff!important;font-size:12px!important;font-weight:950!important}
#page-snapshots .ss-pick-main{min-width:0!important;display:grid!important;gap:2px!important}
#page-snapshots [data-coll="compare"] .ss-pick-date{font-size:12px!important;line-height:1.2!important;color:#f0f6ff!important;white-space:nowrap!important;overflow:hidden!important;text-overflow:ellipsis!important}
#page-snapshots [data-coll="compare"] .ss-pick-meta,#page-snapshots [data-coll="compare"] .ss-pick-main .ss-small{font-size:11px!important;line-height:1.25!important;white-space:nowrap!important;overflow:hidden!important;text-overflow:ellipsis!important}
#page-snapshots .ss-compare-actions{display:grid!important;grid-template-columns:1fr!important;gap:8px!important}
#page-snapshots .ss-compare-actions .btn{width:100%!important;min-height:38px!important;border-radius:7px!important;font-weight:900!important}
#page-snapshots #ss-diff-out{margin-top:0!important}
#page-snapshots .ss-tool-btn{border-radius:7px!important;background:#151e2e!important;border-color:rgba(89,109,139,.22)!important;padding:9px 10px!important}
#page-snapshots .ss-tool-title{font-size:12px!important}
#page-snapshots .ss-tool-sub{font-size:11px!important}
#page-snapshots .ss-capture-modal{position:fixed!important;inset:0!important;z-index:12000!important;display:grid!important;place-items:center!important;padding:18px!important;background:rgba(8,13,22,.58)!important;backdrop-filter:blur(7px) saturate(120%)!important;-webkit-backdrop-filter:blur(7px) saturate(120%)!important}
#page-snapshots .ss-capture-modal.hidden{display:none!important}
#page-snapshots .ss-capture-dialog{width:min(440px,calc(100vw - 32px))!important;border-radius:16px!important;border:1px solid rgba(111,136,171,.34)!important;background:linear-gradient(180deg,#1b2433 0%,#121a28 100%)!important;box-shadow:0 24px 70px rgba(0,0,0,.42)!important;padding:18px!important;color:#edf5ff!important}
#page-snapshots .ss-capture-modal-head{display:grid!important;grid-template-columns:auto minmax(0,1fr)!important;gap:12px!important;align-items:center!important;margin-bottom:16px!important}
#page-snapshots .ss-capture-icon{width:42px!important;height:42px!important;border-radius:12px!important;display:grid!important;place-items:center!important;background:rgba(86,67,214,.22)!important;color:#9bb7ff!important;border:1px solid rgba(119,104,255,.30)!important}
#page-snapshots .ss-capture-icon .material-symbols-rounded{font-size:24px!important}
#page-snapshots .ss-capture-title{font-size:18px!important;line-height:1.15!important;font-weight:900!important;color:#fff!important}
#page-snapshots .ss-capture-sub{font-size:12px!important;line-height:1.35!important;color:#aebcd0!important;margin-top:3px!important}
#page-snapshots .ss-capture-meter{position:relative!important;height:10px!important;border-radius:999px!important;background:#0d1420!important;border:1px solid rgba(89,109,139,.28)!important;overflow:hidden!important}
#page-snapshots .ss-capture-meter-fill{position:relative!important;height:100%!important;width:0%;border-radius:999px!important;background:linear-gradient(90deg,#4b7bff,#6b58ef,#31d08b)!important;transition:width .22s ease!important;overflow:hidden!important}
#page-snapshots .ss-capture-modal.is-active .ss-capture-meter-fill{background-image:linear-gradient(90deg,#4b7bff,#6b58ef,#31d08b),repeating-linear-gradient(120deg,rgba(255,255,255,.00) 0,rgba(255,255,255,.00) 8px,rgba(255,255,255,.34) 8px,rgba(255,255,255,.34) 14px)!important;background-blend-mode:screen!important;background-size:100% 100%,28px 28px!important;animation:sscapstripes .7s linear infinite!important}
#page-snapshots .ss-capture-modal.is-active .ss-capture-meter::after{content:""!important;position:absolute!important;top:0!important;bottom:0!important;left:0!important;width:28%!important;min-width:72px!important;border-radius:999px!important;background:linear-gradient(90deg,transparent,rgba(130,180,255,.22),transparent)!important;animation:sscappulse 1.35s ease-in-out infinite!important;pointer-events:none!important}
@keyframes sscapstripes{to{background-position:0 0,28px 0}}
@keyframes sscappulse{0%{transform:translateX(-110%);opacity:.35}50%{opacity:1}100%{transform:translateX(420%);opacity:.35}}
#page-snapshots .ss-capture-progress-line{display:flex!important;align-items:center!important;justify-content:space-between!important;gap:12px!important;margin:9px 0 14px!important;font-size:12px!important;color:#aebcd0!important}
#page-snapshots .ss-capture-progress-line span{font-weight:900!important;color:#dce8ff!important}
#page-snapshots .ss-capture-progress-line b{font-size:12px!important;color:#86b7ff!important}
#page-snapshots .ss-capture-grid{display:grid!important;grid-template-columns:1fr 1fr!important;gap:8px!important;margin-bottom:12px!important}
#page-snapshots .ss-capture-grid div{display:grid!important;gap:3px!important;padding:10px!important;border-radius:10px!important;background:#151f30!important;border:1px solid rgba(89,109,139,.26)!important;min-width:0!important}
#page-snapshots .ss-capture-grid span{font-size:10px!important;font-weight:900!important;text-transform:uppercase!important;color:#8fa0b8!important}
#page-snapshots .ss-capture-grid b{font-size:13px!important;line-height:1.25!important;color:#f5f9ff!important;white-space:nowrap!important;overflow:hidden!important;text-overflow:ellipsis!important}
#page-snapshots .ss-capture-message{padding:10px 12px!important;border-radius:10px!important;background:#0f1828!important;border:1px solid rgba(89,109,139,.24)!important;font-size:12px!important;line-height:1.4!important;color:#b9c7da!important}
#page-snapshots .ss-capture-actions{display:none!important;margin-top:12px!important;grid-template-columns:1fr!important}
#page-snapshots .ss-capture-actions.show{display:grid!important}
#page-snapshots .ss-capture-actions .btn{min-height:38px!important;border-radius:7px!important;font-weight:900!important;background:#563bd9!important;border-color:#674eea!important;color:#fff!important}
#page-snapshots .ss-capture-modal.is-done .ss-capture-icon{background:rgba(49,208,139,.18)!important;color:#80f0bc!important;border-color:rgba(49,208,139,.30)!important}
#page-snapshots .ss-capture-modal.is-error .ss-capture-icon{background:rgba(239,68,68,.14)!important;color:#ff9b9b!important;border-color:rgba(239,68,68,.28)!important}
#page-snapshots .ss-capture-modal.is-done .ss-capture-meter-fill{animation:none!important;background:linear-gradient(90deg,#31d08b,#5ee0aa)!important}
#page-snapshots .ss-capture-modal.is-error .ss-capture-meter-fill{background:linear-gradient(90deg,#ef4444,#f97316)!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-capture-modal{background:rgba(233,237,245,.62)!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-capture-dialog{background:#ffffff!important;border-color:rgba(16,24,40,.16)!important;color:#111827!important;box-shadow:0 24px 70px rgba(16,24,40,.18)!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-capture-title{color:#111827!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-capture-sub,html[data-cw-theme="flat-light"] #page-snapshots .ss-capture-progress-line{color:#475467!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-capture-meter{background:#eef2f7!important;border-color:rgba(16,24,40,.14)!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-capture-grid div,html[data-cw-theme="flat-light"] #page-snapshots .ss-capture-message{background:#f8fafc!important;border-color:rgba(16,24,40,.14)!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-capture-grid span{color:#667085!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-capture-grid b{color:#111827!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-capture-message{color:#475467!important}
html[data-cw-theme="flat-dark"] #page-snapshots .ss-capture-dialog{background:var(--cw-theme-surface-raised)!important;border-color:rgba(218,226,242,.16)!important}
html[data-cw-theme="flat-dark"] #page-snapshots .ss-capture-grid div,html[data-cw-theme="flat-dark"] #page-snapshots .ss-capture-message{background:var(--panel)!important;border-color:rgba(218,226,242,.13)!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-table th{background:#eef3f8!important;color:#475467!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-table td{color:#111827!important;border-color:rgba(21,31,48,.12)!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-summary-card,html[data-cw-theme="flat-light"] #page-snapshots .ss-flow-step,html[data-cw-theme="flat-light"] #page-snapshots .ss-tool-btn,html[data-cw-theme="flat-light"] #page-snapshots .ss-compare-state{background:#ffffff!important;border-color:rgba(21,31,48,.16)!important;color:#111827!important}
html[data-cw-theme="flat-dark"] #page-snapshots .ss-table th{background:var(--cw-theme-surface-raised)!important;color:#b8c1d2!important}
html[data-cw-theme="flat-dark"] #page-snapshots .ss-summary-card,html[data-cw-theme="flat-dark"] #page-snapshots .ss-flow-step,html[data-cw-theme="flat-dark"] #page-snapshots .ss-tool-btn,html[data-cw-theme="flat-dark"] #page-snapshots .ss-compare-state{background:var(--cw-theme-surface-raised)!important;border-color:var(--border)!important}
html[data-cw-theme="flat-light"] #page-snapshots,html[data-cw-theme="flat-light"] #page-snapshots.card{max-width:none!important;width:100%!important;grid-column:1/-1!important;padding:0!important;--ss-page:#e9edf5;--ss-panel:#f8fafc;--ss-panel-strong:#ffffff;--ss-border:rgba(16,24,40,.16);--ss-fg:#111827;--ss-muted-fg:#475467;background:transparent!important;background-image:none!important;border:0!important;border-radius:0!important;box-shadow:none!important;color:#111827!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-title,html[data-cw-theme="flat-light"] #page-snapshots .ss-card h3,html[data-cw-theme="flat-light"] #page-snapshots .ss-summary-value,html[data-cw-theme="flat-light"] #page-snapshots .ss-step b,html[data-cw-theme="flat-light"] #page-snapshots .ss-tool-title{color:#111827!important;-webkit-text-fill-color:#111827!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-sub,html[data-cw-theme="flat-light"] #page-snapshots .ss-headsub,html[data-cw-theme="flat-light"] #page-snapshots .ss-summary-label,html[data-cw-theme="flat-light"] #page-snapshots .ss-summary-sub,html[data-cw-theme="flat-light"] #page-snapshots .ss-create-fields label,html[data-cw-theme="flat-light"] #page-snapshots .ss-tool-sub{color:#475467!important;-webkit-text-fill-color:#475467!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-card,html[data-cw-theme="flat-light"] #page-snapshots .ss-card.ss-accent,html[data-cw-theme="flat-light"] #page-snapshots .ss-summary-card,html[data-cw-theme="flat-light"] #page-snapshots [data-coll="restore"],html[data-cw-theme="flat-light"] #page-snapshots [data-coll="compare"],html[data-cw-theme="flat-light"] #page-snapshots [data-coll="tools"]{background:#f8fafc!important;border-color:rgba(16,24,40,.16)!important;color:#111827!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-field,html[data-cw-theme="flat-light"] #page-snapshots .ss-filtermini,html[data-cw-theme="flat-light"] #page-snapshots .ss-row>select.input,html[data-cw-theme="flat-light"] #page-snapshots .ss-row>input.input,html[data-cw-theme="flat-light"] #page-snapshots input.input{background:#ffffff!important;border-color:rgba(16,24,40,.18)!important;color:#111827!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-field select,html[data-cw-theme="flat-light"] #page-snapshots .ss-field input,html[data-cw-theme="flat-light"] #page-snapshots .ss-filtermini select,html[data-cw-theme="flat-light"] #page-snapshots input.input{background:#ffffff!important;color:#111827!important;-webkit-text-fill-color:#111827!important;color-scheme:light!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-selected-card,html[data-cw-theme="flat-light"] #page-snapshots .ss-modebtn,html[data-cw-theme="flat-light"] #page-snapshots .ss-tool-btn,html[data-cw-theme="flat-light"] #page-snapshots .ss-empty{background:#ffffff!important;border-color:rgba(16,24,40,.16)!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-stepper::before{background:rgba(71,84,103,.24)!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-step span{background:#eef2f7!important;border-color:rgba(16,24,40,.18)!important;box-shadow:0 0 0 3px #f8fafc!important;color:#667085!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-step.on span{background:#6b58ef!important;border-color:#7d6dff!important;color:#fff!important;box-shadow:0 0 0 3px #f8fafc,0 8px 18px rgba(107,88,239,.18)!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-modebtn.active{background:#e9ecf7!important;border-color:rgba(70,86,166,.34)!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-table th{background:#eef2f7!important;color:#475467!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-table td{background:#f8fafc!important;color:#111827!important;border-color:rgba(16,24,40,.12)!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-compare-state,html[data-cw-theme="flat-light"] #page-snapshots .ss-compare-empty,html[data-cw-theme="flat-light"] #page-snapshots [data-coll="compare"] .ss-pick-card,html[data-cw-theme="flat-light"] #page-snapshots .ss-compare-count{background:#ffffff!important;border-color:rgba(16,24,40,.16)!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-compare-state-copy b,html[data-cw-theme="flat-light"] #page-snapshots .ss-compare-count strong,html[data-cw-theme="flat-light"] #page-snapshots .ss-compare-empty b,html[data-cw-theme="flat-light"] #page-snapshots [data-coll="compare"] .ss-pick-date{color:#111827!important;-webkit-text-fill-color:#111827!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-compare-state-copy span,html[data-cw-theme="flat-light"] #page-snapshots .ss-compare-count span,html[data-cw-theme="flat-light"] #page-snapshots .ss-compare-empty span,html[data-cw-theme="flat-light"] #page-snapshots [data-coll="compare"] .ss-pick-meta,html[data-cw-theme="flat-light"] #page-snapshots [data-coll="compare"] .ss-pick-main .ss-small{color:#475467!important;-webkit-text-fill-color:#475467!important}
html[data-cw-theme="flat-dark"] #page-snapshots,html[data-cw-theme="flat-dark"] #page-snapshots.card{max-width:none!important;width:100%!important;grid-column:1/-1!important;padding:0!important;--ss-page:var(--panel);--ss-panel:var(--panel);--ss-panel-strong:var(--cw-theme-surface-raised);--ss-border:var(--border);--ss-fg:var(--fg);--ss-muted-fg:var(--muted);background:transparent!important;background-image:none!important;border:0!important;box-shadow:none!important}
html[data-cw-theme="flat-dark"] #page-snapshots .ss-card,html[data-cw-theme="flat-dark"] #page-snapshots .ss-card.ss-accent,html[data-cw-theme="flat-dark"] #page-snapshots [data-coll="restore"],html[data-cw-theme="flat-dark"] #page-snapshots [data-coll="compare"],html[data-cw-theme="flat-dark"] #page-snapshots [data-coll="tools"]{background:var(--panel)!important;background-image:none!important;border-color:var(--border)!important}
html[data-cw-theme="flat-dark"] #page-snapshots .ss-summary-card{background:var(--cw-theme-surface-raised)!important;background-image:none!important;border-color:var(--border)!important}
html[data-cw-theme="flat-dark"] #page-snapshots .ss-field,html[data-cw-theme="flat-dark"] #page-snapshots .ss-filtermini,html[data-cw-theme="flat-dark"] #page-snapshots .ss-row>select.input,html[data-cw-theme="flat-dark"] #page-snapshots .ss-row>input.input,html[data-cw-theme="flat-dark"] #page-snapshots input.input{background:var(--cw-theme-input)!important;border-color:rgba(255,255,255,.15)!important}
html[data-cw-theme="flat-dark"] #page-snapshots .ss-step span{background:var(--cw-theme-surface-strong)!important;border-color:rgba(255,255,255,.16)!important;box-shadow:0 0 0 3px var(--cw-theme-surface-raised)!important}
html[data-cw-theme="flat-dark"] #page-snapshots .ss-step.on span{background:#6b58ef!important;border-color:#7d6dff!important;color:#fff!important;box-shadow:0 0 0 3px var(--cw-theme-surface-raised),0 8px 18px rgba(107,88,239,.22)!important}
#page-snapshots .ss-top.cw-page-hero,html[data-cw-theme] #page-snapshots .ss-top.cw-page-hero{display:grid!important;grid-template-columns:minmax(0,1fr) auto!important;align-items:center!important;gap:18px!important;min-height:112px!important;margin:0 0 16px!important;padding:20px 30px!important;border:1px solid rgba(95,111,214,.26)!important;border-radius:22px!important;background:radial-gradient(820px 250px at 100% 0%,rgba(112,96,245,.10),transparent 68%),linear-gradient(100deg,#111821 0%,#151d31 48%,#181a38 100%)!important;box-shadow:none!important;color:#f4f7ff!important;overflow:hidden!important;isolation:isolate!important;position:relative!important;backdrop-filter:none!important;-webkit-backdrop-filter:none!important}
#page-snapshots .ss-top.cw-page-hero::before,html[data-cw-theme] #page-snapshots .ss-top.cw-page-hero::before{content:""!important;display:block!important;position:absolute!important;inset:0!important;z-index:-2!important;pointer-events:none!important;background:linear-gradient(90deg,rgba(255,255,255,.035) 0 1px,transparent 1px 100%),linear-gradient(180deg,rgba(255,255,255,.026) 0 1px,transparent 1px 100%)!important;background-size:42px 42px!important;opacity:.34!important}
html[data-cw-theme="flat-light"] #page-snapshots .ss-top.cw-page-hero{border-color:rgba(70,86,166,.24)!important;background:radial-gradient(820px 250px at 100% 0%,rgba(112,96,245,.18),transparent 68%),linear-gradient(100deg,#fff 0%,#eef4ff 50%,#dfe3ff 100%)!important;color:#172033!important}
@media (max-width:1380px){#page-snapshots .ss-wrap{grid-template-columns:300px minmax(520px,1fr) 320px}#page-snapshots .ss-topstats{grid-template-columns:repeat(3,minmax(150px,1fr))}}
@media (max-width:1100px){#page-snapshots .ss-wrap{grid-template-columns:1fr}#page-snapshots .ss-topstats{grid-template-columns:repeat(2,minmax(150px,1fr))}#page-snapshots .ss-table-wrap{max-height:none}}
@media (max-width:720px){#page-snapshots .ss-top{grid-template-columns:1fr}#page-snapshots .ss-topstats{grid-template-columns:1fr}#page-snapshots .ss-filterbar{grid-template-columns:1fr}#page-snapshots .ss-table{min-width:980px}}
  `;

  function injectCss() {
    if (document.getElementById("cw-snapshots-css")) return;
    const s = document.createElement("style");
    s.id = "cw-snapshots-css";
    s.textContent = css + cssTuning + cssCaptureLock + cssRedesign;
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
    return compareSelectionState();
  }

  const state = {
    providers: [], snapshots: [], selectedPath: "", selectedSnap: null, diffPick: [], diffResult: null,
    busy: false, captureBusy: false, lastRefresh: 0,
    listLimit: 8, showAll: false, expandedBundles: {}, scheduleQueue: [], _spinUntil: 0,
    captureProgressId: "", captureProgressTimer: null, restoreConfirmUntil: 0, restoreConfirmTimer: null, restoreConfirmRaf: 0,
  };

  const configuredProviderIds = () => new Set(
    (Array.isArray(state.providers) ? state.providers : [])
      .filter((p) => !!p?.configured)
      .map((p) => String(p.id || "").trim().toLowerCase())
      .filter(Boolean)
  );

  function _providerMetaById(pid) {
    const id = String(pid || "").trim().toLowerCase();
    const meta = window.CW?.ProviderMeta || {};
    const label = meta.label?.(id) || String(pid || "").trim() || "-";
    const icon = meta.logLogoPath?.(id) || meta.logoPath?.(id) || "";
    return { id, label, icon };
  }

  function _enhanceProviderIconSelect(sel) {
    const helper = window.CW?.IconSelect?.enhance;
    if (!sel || typeof helper !== "function") return;
    helper(sel, {
      className: "ss-icon-select",
      getOptionData: (value, option) => {
        const meta = _providerMetaById(value);
        return {
          label: String(option?.textContent || meta.label || "-").trim() || "-",
          icons: meta.icon && value ? [{ src: meta.icon, alt: meta.label || value }] : [],
          disabled: !!option?.disabled,
        };
      },
    });
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
  if (picks.length !== 1) return null;
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

  if (checked) {
    if (!snap) return;
    if (!picks.includes(p)) {
      picks.push(p);
    }
  } else {
    const ix = picks.indexOf(p);
    if (ix !== -1) picks.splice(ix, 1);
  }

  state.diffPick = picks;

  if (picks.length !== 2) state.diffResult = null;

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
    const latest = snaps.slice().sort((a, b) => {
      const am = Number(a?.mtime || 0) || (_stampEpoch(a?.stamp) / 1000);
      const bm = Number(b?.mtime || 0) || (_stampEpoch(b?.stamp) / 1000);
      return bm - am;
    })[0] || null;
    const scheduledJobs = countScheduledCaptureJobs();
    return { total, providers: providerIds.size, fullSets, latest, scheduledJobs };
  }

  function countScheduledCaptureJobs() {
    const queued = Array.isArray(state.scheduleQueue) ? state.scheduleQueue.length : 0;
    const draft = Array.isArray(window.__cwCaptureSchedulerDraftJobs) ? window.__cwCaptureSchedulerDraftJobs : [];
    const pending = Array.isArray(window.__cwCaptureSchedulerPrefillQueue) ? window.__cwCaptureSchedulerPrefillQueue : [];
    return queued + draft.filter(Boolean).length + pending.filter(Boolean).length;
  }

  function snapInstance(snap) {
    return String(snap?.instance || snap?.instance_id || snap?.profile || "default");
  }

  function snapCreatedText(snap) {
    if (!snap) return "-";
    if (snap.stamp) return fmtTsFromStamp(snap.stamp) || "-";
    if (snap.created_at) return new Date(String(snap.created_at)).toLocaleString();
    if (snap.mtime) return new Date(Number(snap.mtime || 0) * 1000).toLocaleString();
    return "-";
  }

  function snapCreatedMinuteText(snap) {
    let d = null;
    if (snap?.stamp) {
      const ms = _stampEpoch(snap.stamp);
      d = ms ? new Date(ms) : null;
    } else if (snap?.created_at) {
      d = new Date(String(snap.created_at));
    } else if (snap?.mtime) {
      d = new Date(Number(snap.mtime || 0) * 1000);
    }
    if (!d || Number.isNaN(d.getTime())) return snapCreatedText(snap);
    return d.toLocaleString(undefined, {
      year: "numeric",
      month: "numeric",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  }

  function snapItemCount(snap) {
    const stats = snap?.stats || {};
    const count = stats.count ?? stats.total ?? snap?.count;
    return Number(count || 0);
  }

  function snapTypeLabel(snap) {
    const feature = String(snap?.feature || "").toLowerCase();
    return feature === "all" ? "Full set" : "Feature";
  }

  function compareSelectionState() {
    const pick = _diffPickAB();
    const selected = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];
    const result = { ...pick, selected, ok: false, reason: "" };
    if (selected.length !== 2) {
      result.reason = selected.length
        ? `Comparison needs exactly two captures. ${selected.length} selected.`
        : "Select exactly two captures to enable comparison.";
      return result;
    }
    if (!pick.sa || !pick.sb) {
      result.reason = "One selected capture is no longer available.";
      return result;
    }
    const pa = String(pick.sa.provider || "").toLowerCase();
    const pb = String(pick.sb.provider || "").toLowerCase();
    const ia = snapInstance(pick.sa).toLowerCase();
    const ib = snapInstance(pick.sb).toLowerCase();
    const fa = String(pick.sa.feature || "").toLowerCase();
    const fb = String(pick.sb.feature || "").toLowerCase();
    if (pa !== pb) {
      result.reason = "Selected captures use different providers.";
      return result;
    }
    if (ia !== ib) {
      result.reason = "Selected captures use different profiles.";
      return result;
    }
    if (fa !== fb) {
      result.reason = "Selected captures use different features.";
      return result;
    }
    result.ok = !!pick.a && !!pick.b && pick.a !== pick.b;
    result.reason = result.ok
      ? `Comparison enabled for ${(pick.sa.provider || "-").toUpperCase()} / ${fa || "-"} / ${snapInstance(pick.sa)}.`
      : "Select two different captures.";
    return result;
  }

  function updateTopStats() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const overview = snapshotOverview();
    const setStat = (name, value) => {
      const el = page.querySelector(`[data-stat="${name}"] strong`);
      if (el) el.textContent = String(value);
    };
    const setSub = (name, value) => {
      const el = page.querySelector(`[data-stat="${name}"] .ss-summary-sub`);
      if (el) el.textContent = String(value);
    };
    setStat("captures", overview.total);
    setStat("providers", overview.providers);
    setStat("full-sets", overview.fullSets);
    setStat("latest", overview.latest ? snapCreatedMinuteText(overview.latest) : "-");
    setSub("latest", overview.latest ? `${String(overview.latest.provider || "-").toUpperCase()} / ${String(overview.latest.feature || "-").toLowerCase()}` : "No captures yet");
    setStat("scheduled", overview.scheduledJobs);
    setSub("scheduled", overview.scheduledJobs ? "Queued or drafted" : "No queued jobs");
  }

  function render() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    delete page.dataset.diffControlsWired;

    const overview = snapshotOverview();
    const latest = overview.latest;
    const latestMain = latest ? snapCreatedMinuteText(latest) : "-";
    const latestSub = latest ? `${String(latest.provider || "-").toUpperCase()} / ${String(latest.feature || "-").toLowerCase()}` : "No captures yet";
    page.innerHTML = `
      <div class="ss-top cw-page-hero cw-page-hero-captures" data-hero-icon="database">
        <div class="ss-top-copy cw-page-hero-copy">
          <div class="cw-page-hero-kicker">CAPTURES</div>
          <div class="ss-title cw-page-hero-title">Captures</div>
          <div class="ss-sub cw-page-hero-sub">Point-in-time snapshots of provider data. Create, compare, and restore with confidence.</div>
        </div>
        <div class="ss-actions cw-page-hero-actions">
          <button id="ss-refresh" class="iconbtn ss-help-btn" title="Refresh captures" aria-label="Refresh captures"><span id="ss-refresh-icon" class="material-symbol ss-refresh-icon">sync</span></button>
        </div>
      </div>
      <div class="ss-topstats">
        ${summaryCard("database", "Total captures", overview.total, "Across all providers", "captures")}
        ${summaryCard("groups", "Providers", overview.providers, "Active providers", "providers")}
        ${summaryCard("layers", "Full sets", overview.fullSets, "Complete snapshots", "full-sets")}
        ${summaryCard("event", "Latest capture", latestMain, latestSub, "latest")}
        ${summaryCard("schedule", "Scheduled jobs", overview.scheduledJobs, overview.scheduledJobs ? "Queued or drafted" : "No queued jobs", "scheduled")}
      </div>
      <div class="ss-wrap">
        <div class="ss-col">
          <div class="ss-card ss-accent">
            <div class="ss-card-head">
              <div class="ss-headcopy">
                <h3>Create capture</h3>
                <div class="ss-headsub">Choose the source, set capture options, then create now or queue for scheduler.</div>
              </div>
            </div>
            <div class="ss-create-flow">
              <div class="ss-stepper">
                <div class="ss-step on"><span>1</span><b>Source</b></div>
                <div class="ss-step"><span>2</span><b>Options</b></div>
                <div class="ss-step"><span>3</span><b>Review</b></div>
              </div>
              <div class="ss-create-fields">
                <label>Provider<div class="ss-field"><select id="ss-prov"></select></div></label>
                <label>Profile<div class="ss-field"><select id="ss-prov-inst" class="input grow"></select></div></label>
                <label>Capture type<div class="ss-field"><select id="ss-feature"></select></div></label>
                <label>Optional label<div class="ss-field"><input id="ss-label" maxlength="12" placeholder="e.g. Maint" /></div><small>Max 12 characters</small></label>
              </div>
              <div id="ss-create-review" class="ss-selected-card ss-small"></div>
              <div class="ss-create-actions">
                <button id="ss-create" class="btn primary" type="button"><span class="material-symbols-rounded" aria-hidden="true">bolt</span><span class="ss-btn-label">Create now</span></button>
                <button id="ss-add-schedule" class="btn" type="button"><span class="material-symbols-rounded" aria-hidden="true">schedule</span><span class="ss-btn-label">Queue for scheduler</span></button>
              </div>
            </div>
            <div id="ss-schedule-queue-wrap" class="ss-queue hidden">
              <div class="ss-queue-head">
                <div>
                  <div class="ss-queue-title">Scheduler queue</div>
                  <div class="ss-queue-note">Queue capture schedules here, then send them to Advanced Scheduling.</div>
                </div>
                <div class="ss-queue-actions">
                  <button id="ss-send-schedule-queue" class="btn" type="button">Send queue</button>
                  <button id="ss-clear-schedule-queue" class="btn" type="button">Clear queue</button>
                </div>
              </div>
              <div id="ss-queue-feedback" class="ss-small ss-muted" style="display:none;padding:9px 10px;border-radius:12px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.03)"></div>
              <div id="ss-schedule-queue" class="ss-queue-list"></div>
            </div>
          </div>
        </div>
        <div class="ss-center-col">
          <div class="ss-card ss-browser-card ss-lockable">
            <div class="ss-card-head ss-list-head">
              <div class="ss-headcopy">
                <h3>Capture browser</h3>
                <div class="ss-headsub">Select rows with checkboxes for compare and tools. Click a row to load it for restore.</div>
              </div>
            </div>
            <div class="ss-toolbar">
              <div class="ss-filterbar">
                <div id="ss-filter-provider-wrap" class="ss-filtermini"><select id="ss-filter-provider" class="input"></select></div>
                <div id="ss-filter-feature-wrap" class="ss-filtermini"><select id="ss-filter-feature" class="input"></select></div>
                <div id="ss-filter-kind-wrap" class="ss-filtermini"><select id="ss-filter-kind" class="input"></select></div>
                <button id="ss-filter-clear" class="btn ss-filterclear" type="button"><span class="material-symbols-rounded">filter_list</span>Filters</button>
              </div>
              <div id="ss-capture-lock" class="ss-lockmsg hidden"><span class="material-symbol">hourglass_top</span><div>Creating capture... browser, restore, compare, and tools unlock after refresh.</div></div>
            </div>
            <div id="ss-list" class="ss-list"></div>
            <div id="ss-list-footer" class="ss-row" style="justify-content:space-between;margin:10px 0 14px"></div>
          </div>
        </div>
        <div class="ss-col">
          <div class="ss-card ss-lockable" data-coll="restore">
            <div class="ss-card-head">
              <div class="ss-headcopy">
                <h3>Restore capture</h3>
                <div class="ss-headsub">Restore stays visible so the target and method are always clear.</div>
              </div>
            </div>
            <div id="ss-selected" class="ss-selected-card ss-selected-empty">Pick a capture from the browser.</div>
            <div class="ss-target-label">Target profile</div>
            <div class="ss-row"><select id="ss-restore-inst" class="input grow"></select></div>
            <div class="ss-restore-modebar">
              <div class="ss-restore-modes">
                <button class="ss-modebtn active" type="button" data-restore-mode="merge"><strong>Merge missing</strong><span>Only add items</span></button>
                <button class="ss-modebtn" type="button" data-restore-mode="clear_restore"><strong>Replace</strong><span>Match capture</span></button>
              </div>
              <select id="ss-restore-mode" class="input hidden" aria-hidden="true"><option value="merge">Merge missing only</option><option value="clear_restore">Replace exactly</option></select>
              <div id="ss-restore-warning" class="ss-restore-warning hidden">Replace exactly clears the target feature first, then restores this capture.</div>
            </div>
            <div class="ss-row" style="margin-top:12px"><button id="ss-restore" class="btn danger cw-danger-confirm" style="width:100%" type="button"><span class="material-symbols-rounded" aria-hidden="true">restore</span><span>Restore capture</span></button></div>
            <div id="ss-restore-progress" class="ss-progress hidden"><div class="ss-pbar"></div><div class="ss-plabel">Working...</div></div>
            <div id="ss-restore-out" class="ss-small ss-muted" style="margin-top:10px"></div>
          </div>
          <div class="ss-card ss-lockable" data-coll="compare">
            <div class="ss-card-head">
              <div class="ss-headcopy">
                <h3>Compare captures</h3>
                <div class="ss-headsub">Comparison requires exactly two compatible captures.</div>
              </div>
            </div>
            <div class="ss-compare-panel">
              <div id="ss-compare-state" class="ss-compare-state"></div>
              <div id="ss-diff-picked" class="ss-picked ss-compare-picks"></div>
              <div class="ss-compare-actions">
                <button id="ss-diff-run" class="btn" type="button">Compare Captures</button>
              </div>
              <div id="ss-diff-out" class="ss-muted ss-small"></div>
            </div>
          </div>
          <div class="ss-card ss-lockable" data-coll="tools">
            <div class="ss-card-head">
              <div class="ss-headcopy">
                <h3>Tools</h3>
                <div class="ss-headsub">Secondary actions for selected captures and stored capture cleanup.</div>
              </div>
            </div>
            <div class="ss-tools-list">
              <button id="ss-export-selected" class="ss-tool-btn" type="button"><span class="material-symbols-rounded">download</span><span class="ss-tool-main"><span class="ss-tool-title">Export selected</span><span class="ss-tool-sub">Download selected capture data</span></span><span class="material-symbols-rounded">chevron_right</span></button>
              <button id="ss-delete" class="ss-tool-btn danger" type="button"><span class="material-symbols-rounded">delete</span><span class="ss-tool-main"><span class="ss-tool-title">Delete selected</span><span class="ss-tool-sub">Remove selected captures</span></span><span class="material-symbols-rounded">chevron_right</span></button>
              <button id="ss-cleanup-old" class="ss-tool-btn danger" type="button"><span class="material-symbols-rounded">auto_delete</span><span class="ss-tool-main"><span class="ss-tool-title">Cleanup Captures</span><span class="ss-tool-sub">Remove all stored captures</span></span><span class="material-symbols-rounded">chevron_right</span></button>
              <button id="ss-view-details" class="ss-tool-btn" type="button"><span class="material-symbols-rounded">info</span><span class="ss-tool-main"><span class="ss-tool-title">View details</span><span class="ss-tool-sub">Show technical details</span></span><span class="material-symbols-rounded">chevron_right</span></button>
            </div>
            <div id="ss-tools-progress" class="ss-progress hidden"><div class="ss-pbar"></div><div class="ss-plabel">Working...</div></div>
            <div id="ss-tools-out" class="ss-small ss-muted hidden" style="margin-top:10px"></div>
            <div id="ss-detail-out" class="ss-code ss-detail-pre hidden"></div>
          </div>
        </div>
      </div>
      <div id="ss-capture-modal" class="ss-capture-modal hidden" role="dialog" aria-modal="true" aria-labelledby="ss-capture-modal-title">
        <div class="ss-capture-dialog">
          <div class="ss-capture-modal-head">
            <div class="ss-capture-icon"><span class="material-symbols-rounded">database_upload</span></div>
            <div>
              <div id="ss-capture-modal-title" class="ss-capture-title">Creating capture</div>
              <div id="ss-capture-modal-sub" class="ss-capture-sub">Preparing provider capture...</div>
            </div>
          </div>
          <div class="ss-capture-meter" aria-hidden="true"><div id="ss-capture-meter-fill" class="ss-capture-meter-fill"></div></div>
          <div class="ss-capture-progress-line"><span id="ss-capture-percent">0%</span><b id="ss-capture-stage">Starting</b></div>
          <div class="ss-capture-grid">
            <div><span>Provider</span><b id="ss-capture-provider">-</b></div>
            <div><span>Profile</span><b id="ss-capture-instance">default</b></div>
            <div><span>Feature</span><b id="ss-capture-feature">-</b></div>
            <div><span>Items</span><b id="ss-capture-items">-</b></div>
          </div>
          <div id="ss-capture-message" class="ss-capture-message">Starting capture...</div>
          <div id="ss-capture-actions" class="ss-capture-actions">
            <button id="ss-capture-ack" class="btn" type="button">OK</button>
          </div>
        </div>
      </div>`;

    updateCaptureBusyUI();

    $("#ss-refresh", page)?.addEventListener("click", () => {
      state._spinUntil = Date.now() + 550;
      setRefreshSpinning(true);
      refresh(true, true);
      setTimeout(() => { if (!state.busy) setRefreshSpinning(false); }, 600);
    });
    $("#ss-capture-ack", page)?.addEventListener("click", () => {
      hideCaptureProgressModal(0);
      window.setTimeout(() => refresh(true, false), 80);
    });
    $("#ss-create", page)?.addEventListener("click", () => onCreate());
    $("#ss-add-schedule", page)?.addEventListener("click", () => onAddToScheduler());
    $("#ss-send-schedule-queue", page)?.addEventListener("click", () => onSendScheduleQueue());
    $("#ss-clear-schedule-queue", page)?.addEventListener("click", () => {
      state.scheduleQueue = [];
      renderScheduleQueue();
      setScheduleQueueFeedback("Queue cleared", true);
    });
    $("#ss-prov", page)?.addEventListener("change", () => { repopFeatures(); repopCreateInstances(); updateCreateReview(); });
    $("#ss-prov-inst", page)?.addEventListener("change", () => updateCreateReview());
    $("#ss-feature", page)?.addEventListener("change", () => updateCreateReview());
    $("#ss-label", page)?.addEventListener("input", (ev) => {
      const el = ev.currentTarget;
      const next = String(el?.value || "").slice(0, 12);
      if (el && el.value !== next) el.value = next;
      updateCreateReview();
    });
    $("#ss-filter-provider", page)?.addEventListener("change", () => { state.showAll = false; updateBrowserFilterBar(); renderList(); });
    $("#ss-filter-feature", page)?.addEventListener("change", () => { state.showAll = false; updateBrowserFilterBar(); renderList(); });
    $("#ss-filter-kind", page)?.addEventListener("change", () => { state.showAll = false; updateBrowserFilterBar(); renderList(); });
    $("#ss-filter-clear", page)?.addEventListener("click", () => {
      const provSel = $("#ss-filter-provider", page);
      const featSel = $("#ss-filter-feature", page);
      const kindSel = $("#ss-filter-kind", page);
      const resetSelect = (sel) => {
        if (!sel) return;
        sel.selectedIndex = 0;
        sel.dispatchEvent(new Event("change", { bubbles: true }));
      };
      resetSelect(provSel);
      resetSelect(featSel);
      resetSelect(kindSel);
      state.showAll = false;
      updateBrowserFilterBar();
      renderList();
    });

    $("#ss-restore", page)?.addEventListener("click", () => onRestore());
    $("#ss-delete", page)?.addEventListener("click", () => onDeleteSelected());
    $("#ss-export-selected", page)?.addEventListener("click", () => onExportSelected());
    $("#ss-cleanup-old", page)?.addEventListener("click", () => onCleanupOldCaptures());
    $("#ss-view-details", page)?.addEventListener("click", () => onViewDetails());
    $("#ss-restore-inst", page)?.addEventListener("change", () => { resetRestoreConfirm(); updateRestoreAvailability(); });
    $("#ss-restore-mode", page)?.addEventListener("change", () => updateRestoreModeUI());
    $$("[data-restore-mode]", page).forEach((btn) => btn.addEventListener("click", () => {
      resetRestoreConfirm();
      const sel = $("#ss-restore-mode", page);
      const mode = String(btn.getAttribute("data-restore-mode") || "merge");
      if (sel) sel.value = mode;
      updateRestoreModeUI();
    }));
    updateRestoreAvailability();
    updateRestoreModeUI();

    wireDiffControls();
    renderScheduleQueue();
    updateCreateReview();
  }

  function summaryCard(icon, label, value, sub, stat) {
    return `<div class="ss-summary-card" data-stat="${escapeHtml(stat)}"><div class="ss-summary-icon"><span class="material-symbols-rounded">${escapeHtml(icon)}</span></div><div class="ss-summary-copy"><div class="ss-summary-label">${escapeHtml(label)}</div><div class="ss-summary-value"><strong>${escapeHtml(value)}</strong></div><div class="ss-summary-sub">${escapeHtml(sub)}</div></div></div>`;
  }

  function setProgress(sel, on, label, tone) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const el = $(sel, page);
    if (!el) return;
    const lab = $(".ss-plabel", el);
    if (lab) lab.textContent = label || "Working...";
    el.style.setProperty("--pcol", tone === "danger" ? "var(--danger)" : "var(--accent)");
    el.classList.toggle("hidden", !on);
  }

  function newCaptureProgressId() {
    try {
      if (window.crypto?.randomUUID) return window.crypto.randomUUID();
    } catch {}
    return `capture-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  }

  function stopCaptureProgressPoll() {
    if (state.captureProgressTimer) {
      clearTimeout(state.captureProgressTimer);
      state.captureProgressTimer = null;
    }
  }

  function resetRestoreConfirm() {
    const page = document.getElementById("page-snapshots");
    state.restoreConfirmUntil = 0;
    if (state.restoreConfirmTimer) {
      clearTimeout(state.restoreConfirmTimer);
      state.restoreConfirmTimer = null;
    }
    if (state.restoreConfirmRaf) {
      cancelAnimationFrame(state.restoreConfirmRaf);
      state.restoreConfirmRaf = 0;
    }
    const btn = page ? $("#ss-restore", page) : null;
    if (!btn) return;
    btn.classList.remove("is-confirming");
    btn.style.removeProperty("--ss-confirm-progress");
    btn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">restore</span><span>Restore capture</span>`;
  }

  function armRestoreConfirm() {
    const page = document.getElementById("page-snapshots");
    const btn = page ? $("#ss-restore", page) : null;
    state.restoreConfirmUntil = Date.now() + 4200;
    if (!btn) return;
    btn.classList.add("is-confirming");
    btn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">warning</span><span>Confirm restore</span>`;
    if (state.restoreConfirmTimer) clearTimeout(state.restoreConfirmTimer);
    if (state.restoreConfirmRaf) cancelAnimationFrame(state.restoreConfirmRaf);
    const tick = () => {
      const remaining = Math.max(0, state.restoreConfirmUntil - Date.now());
      const pct = Math.max(0, Math.min(100, (remaining / 4200) * 100));
      btn.style.setProperty("--ss-confirm-progress", `${pct.toFixed(2)}%`);
      if (remaining > 0 && btn.classList.contains("is-confirming")) {
        state.restoreConfirmRaf = requestAnimationFrame(tick);
      } else {
        state.restoreConfirmRaf = 0;
      }
    };
    tick();
    state.restoreConfirmTimer = setTimeout(resetRestoreConfirm, 4200);
  }

  function captureStageLabel(stage) {
    const key = String(stage || "").toLowerCase();
    if (key === "queued") return "Queued";
    if (key === "waiting") return "Waiting";
    if (key === "reading") return "Reading provider";
    if (key === "planning") return "Planning changes";
    if (key === "clearing") return "Clearing target";
    if (key === "restoring") return "Restoring items";
    if (key === "normalizing") return "Normalizing items";
    if (key === "writing") return "Writing capture";
    if (key === "feature_done") return "Feature complete";
    if (key === "bundling") return "Bundling full set";
    if (key === "done") return "Done";
    if (key === "error") return "Failed";
    return "Starting";
  }

  function captureFeatureLabel(feature) {
    const raw = String(feature || "").trim().toLowerCase();
    if (!raw) return "-";
    return raw === "all" ? "All features" : raw.charAt(0).toUpperCase() + raw.slice(1);
  }

  function captureItemText(progress) {
    if (String(progress?.operation || "").toLowerCase() === "restore") {
      const added = Number(progress?.added);
      const removed = Number(progress?.removed);
      const hasAdded = Number.isFinite(added) && added > 0;
      const hasRemoved = Number.isFinite(removed) && removed > 0;
      if (hasAdded || hasRemoved) return `${hasAdded ? added.toLocaleString() : "0"} added, ${hasRemoved ? removed.toLocaleString() : "0"} removed`;
    }
    const done = Number(progress?.items_done ?? progress?.total_items);
    const featureItems = Number(progress?.feature_items);
    const hasDone = Number.isFinite(done) && done > 0;
    const hasFeature = Number.isFinite(featureItems) && featureItems > 0;
    if (hasDone && hasFeature && done !== featureItems) return `${done.toLocaleString()} total, ${featureItems.toLocaleString()} current`;
    if (hasDone) return `${done.toLocaleString()} items`;
    if (hasFeature) return `${featureItems.toLocaleString()} current`;
    const seen = Number(progress?.activity_seen);
    if (Number.isFinite(seen) && seen > 0) return `${seen.toLocaleString()} seen`;
    if (String(progress?.stage || "").toLowerCase() === "reading") return "Counting...";
    return "-";
  }

  function updateCaptureProgressModal(progress) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const modal = $("#ss-capture-modal", page);
    if (!modal) return;
    const data = progress || {};
    const percent = Math.max(0, Math.min(100, Math.round(Number(data.percent || 0))));
    const currentFeature = String(data.current_feature || data.feature || "").trim();
    const featureTotal = Number(data.feature_total || 1);
    const featureIndex = Number(data.feature_index || 0);
    const sub = featureTotal > 1 && featureIndex > 0
      ? `Feature ${featureIndex} of ${featureTotal}`
      : "Single capture";

    $("#ss-capture-meter-fill", page)?.style.setProperty("width", `${percent}%`);
    const pct = $("#ss-capture-percent", page);
    if (pct) pct.textContent = `${percent}%`;
    const stageKey = String(data.stage || "").toLowerCase();
    const operation = String(data.operation || "capture").toLowerCase();
    const restore = operation === "restore";
    const failed = (!!data.done && data.ok === false) || stageKey === "error";
    const stage = $("#ss-capture-stage", page);
    if (stage) stage.textContent = captureStageLabel(data.stage);
    const title = $("#ss-capture-modal-title", page);
    if (title) title.textContent = failed ? (restore ? "Restore failed" : "Capture failed") : data.done ? (restore ? "Restore complete" : "Capture complete") : (restore ? "Restoring capture" : "Creating capture");
    const subtitle = $("#ss-capture-modal-sub", page);
    if (subtitle) subtitle.textContent = sub;
    const provider = $("#ss-capture-provider", page);
    if (provider) provider.textContent = String(data.provider || "-").toUpperCase();
    const instance = $("#ss-capture-instance", page);
    if (instance) instance.textContent = String(data.instance || "default");
    const feature = $("#ss-capture-feature", page);
    if (feature) feature.textContent = captureFeatureLabel(currentFeature);
    const items = $("#ss-capture-items", page);
    if (items) items.textContent = captureItemText(data);
    const msg = $("#ss-capture-message", page);
    if (msg) msg.textContent = String(data.message || "Working...");
    const actions = $("#ss-capture-actions", page);
    if (actions) actions.classList.toggle("show", !!data.done || failed);
    modal.classList.toggle("is-error", failed);
    modal.classList.toggle("is-done", !!data.done && data.ok !== false);
    modal.classList.toggle("is-active", !data.done && !failed);
  }

  function showCaptureProgressModal(seed) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const modal = $("#ss-capture-modal", page);
    if (!modal) return;
    modal.classList.remove("hidden", "is-error", "is-done");
    $("#ss-capture-actions", page)?.classList.remove("show");
    updateCaptureProgressModal({
      ok: true,
      done: false,
      operation: seed?.operation || "capture",
      stage: "starting",
      message: seed?.operation === "restore" ? "Preparing restore..." : "Preparing capture...",
      percent: 3,
      provider: seed?.provider || "",
      instance: seed?.instance || "default",
      feature: seed?.feature || "",
      current_feature: seed?.feature === "all" ? "" : seed?.feature,
      feature_index: 0,
      feature_total: seed?.feature === "all" ? 4 : 1,
      items_done: 0,
    });
  }

  function hideCaptureProgressModal(delay = 900) {
    const page = document.getElementById("page-snapshots");
    stopCaptureProgressPoll();
    if (!page) return;
    window.setTimeout(() => {
      const modal = $("#ss-capture-modal", page);
      if (modal) modal.classList.add("hidden");
      $("#ss-capture-actions", page)?.classList.remove("show");
      state.captureProgressId = "";
    }, delay);
  }

  function sleep(ms) {
    return new Promise((resolve) => window.setTimeout(resolve, ms));
  }

  async function waitForCaptureProgress(progressId) {
    stopCaptureProgressPoll();
    const id = String(progressId || "");
    if (!id) return null;
    let misses = 0;
    while (state.captureProgressId === id) {
      try {
        const r = await API()(`/api/snapshots/capture-progress/${encodeURIComponent(id)}?_=${Date.now()}`);
        const progress = r && r.progress ? r.progress : null;
        misses = 0;
        if (progress) {
          updateCaptureProgressModal(progress);
          if (progress.done) return progress;
        }
      } catch (e) {
        misses += 1;
        updateCaptureProgressModal({ stage: "waiting", message: "Waiting for progress update...", percent: 8 });
        if (misses >= 8) throw e;
      }
      await sleep(700);
    }
    return null;
  }

  function pollCaptureProgress(progressId) {
    stopCaptureProgressPoll();
    const id = String(progressId || "");
    if (!id) return;
    const tick = async () => {
      if (!state.captureProgressId || state.captureProgressId !== id) return;
      try {
        const r = await API()(`/api/snapshots/capture-progress/${encodeURIComponent(id)}?_=${Date.now()}`);
        const progress = r && r.progress ? r.progress : null;
        if (progress) {
          updateCaptureProgressModal(progress);
          if (progress.done) return;
        }
      } catch (e) {
        updateCaptureProgressModal({ stage: "waiting", message: "Waiting for progress update...", percent: 8 });
      }
      state.captureProgressTimer = window.setTimeout(tick, 650);
    };
    tick();
  }

  function selectedCapturePaths() {
    const picks = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];
    if (picks.length) return picks.slice();
    return state.selectedPath ? [String(state.selectedPath)] : [];
  }

  function updateCreateReview() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const host = $("#ss-create-review", page);
    if (!host) return;
    const provider = String($("#ss-prov", page)?.value || "").toUpperCase() || "-";
    const instance = String($("#ss-prov-inst", page)?.value || "default") || "default";
    const feature = String($("#ss-feature", page)?.value || "").toLowerCase() || "-";
    const label = String($("#ss-label", page)?.value || "").trim().slice(0, 12) || "No label";
    host.innerHTML = `
      <div class="ss-review-line"><span>Source</span><b>${escapeHtml(provider)} / ${escapeHtml(instance)}</b></div>
      <div class="ss-review-line"><span>Feature</span><b>${escapeHtml(feature === "all" ? "All features" : feature)}</b></div>
      <div class="ss-review-line"><span>Label</span><b>${escapeHtml(label)}</b></div>
    `;
    updateCreateActionsAvailability();
  }

  function updateCreateActionsAvailability() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const provider = String($("#ss-prov", page)?.value || "").trim();
    const feature = String($("#ss-feature", page)?.value || "").trim();
    const enabled = !!provider && !!feature && !state.busy && !state.captureBusy;
    ["#ss-create", "#ss-add-schedule"].forEach((id) => {
      const btn = $(id, page);
      if (!btn) return;
      btn.disabled = !enabled;
      btn.title = enabled ? "" : "Pick a provider first";
    });
  }

  function updateSelectionToolsAvailability() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const selected = selectedCapturePaths();
    const set = (id, enabled, title) => {
      const btn = $(id, page);
      if (!btn) return;
      btn.disabled = !!state.busy || !enabled;
      btn.title = enabled ? "" : title;
    };
    set("#ss-export-selected", selected.length > 0, "Select at least one capture");
    set("#ss-delete", selected.length > 0, "Select or load a capture");
    set("#ss-view-details", !!state.selectedSnap || selected.length > 0, "Select or load a capture");
  }

  function wireDiffControls() {
    const page = document.getElementById("page-snapshots");
    if (!page || page.dataset.diffControlsWired === "1") return;
    page.dataset.diffControlsWired = "1";
    const diffRun = $("#ss-diff-run", page);
    if (diffRun) diffRun.addEventListener("click", (e) => { e.preventDefault(); onDiffRun(); });
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
    $$("[data-restore-mode]", page).forEach((btn) => {
      btn.disabled = state.busy || !state.selectedPath;
    });
  }

  function updateRestoreModeUI() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const mode = String($("#ss-restore-mode", page)?.value || "merge").toLowerCase();
    $$("[data-restore-mode]", page).forEach((btn) => {
      btn.classList.toggle("active", String(btn.getAttribute("data-restore-mode") || "") === mode);
    });
    const warning = $("#ss-restore-warning", page);
    if (warning) warning.classList.toggle("hidden", mode !== "clear_restore");
  }

function repopDiffSelects() {
  renderDiffPicked();
  updateDiffAvailability();
  renderDiff();
}

function updateDiffAvailability() {
  const page = document.getElementById("page-snapshots");
  if (!page) return;
  const stateInfo = diffSelection();
  const { ok } = stateInfo;
  const hint = stateInfo.reason || "Pick two compatible captures";
  const count = (Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : []).length;
  const status = $("#ss-compare-state", page);
  if (status) {
    status.classList.toggle("ok", !!ok);
    status.innerHTML = `
      <span class="material-symbols-rounded" aria-hidden="true">${ok ? "compare_arrows" : "info"}</span>
      <div class="ss-compare-state-copy"><b>${ok ? "Ready to compare" : "Comparison disabled"}</b><span>${escapeHtml(hint)}</span></div>
      <div class="ss-compare-count"><strong>${count}</strong><span>selected</span></div>
    `;
  }
  [["#ss-diff-run", ok ? "Compare Captures" : hint]].forEach(([id, title]) => {
    const el = $(id, page);
    if (!el) return;
    el.disabled = state.busy || !ok;
    el.title = title;
  });
  updateSelectionToolsAvailability();
}

function renderDiffPicked() {
  const page = document.getElementById("page-snapshots");
  if (!page) return;
  const host = $("#ss-diff-picked", page);
  if (!host) return;

  const picks = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];
  if (picks.length !== 2) {
    const scope = _diffScope();
    const scopeText = scope ? `${scope.provider} / ${scope.feature}` : "No compare scope yet";
    host.innerHTML = `
      <div class="ss-compare-empty">
        <div><b>${picks.length}/2 captures selected</b><span>${escapeHtml(scopeText)}</span></div>
      </div>
    `;
    return;
  }

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
    const inst = snapInstance(snap);
    const meta = `${(snap.provider || "-").toUpperCase()} / ${inst} / ${feat}`;
    const sub = snap.label ? String(snap.label).slice(0, 60) : String(snap.path || "").slice(0, 80);
    d.innerHTML = `
      <div class="ss-pick-tag">${tag}</div>
      <div class="ss-pick-main">
        <div class="ss-pick-date">${escapeHtml(snapCreatedText(snap))}</div>
        <div class="ss-pick-meta">${escapeHtml(meta)}</div>
        <div class="ss-muted ss-small">${escapeHtml(sub)}</div>
      </div>
    `;
    return d;
  };

  host.innerHTML = "";
  const cards = [
    mkCard(_findSnapByPath(picks[0]), "A", picks[0], 0),
    mkCard(_findSnapByPath(picks[1]), "B", picks[1], 1),
  ];
  cards.forEach((card) => host.appendChild(card));
  cards.forEach((el) => {
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
  });
}

function renderDiff() {
  const page = document.getElementById("page-snapshots");
  if (!page) return;

  const out = $("#ss-diff-out", page);
  if (!out) return;

  const r = state.diffResult;
  if (!r) {
    out.innerHTML = "";
    return;
  }

  const sum = r.summary || {};
  const trunc = r.truncated || {};
  const extra = (trunc.added || trunc.removed || trunc.updated) ? ` (showing up to ${r.limit} per section)` : "";
  out.innerHTML = `
<div class="ss-diff-summary">
  <span class="ss-pill" title="${sum.added ?? 0} added" aria-label="${sum.added ?? 0} added"><span class="material-symbols-rounded" aria-hidden="true">add_circle</span><strong>${sum.added ?? 0}</strong></span>
  <span class="ss-pill" title="${sum.removed ?? 0} deleted" aria-label="${sum.removed ?? 0} deleted"><span class="material-symbols-rounded" aria-hidden="true">delete</span><strong>${sum.removed ?? 0}</strong></span>
  <span class="ss-pill" title="${sum.updated ?? 0} updated" aria-label="${sum.updated ?? 0} updated"><span class="material-symbols-rounded" aria-hidden="true">edit</span><strong>${sum.updated ?? 0}</strong></span>
  <span class="ss-pill" title="${sum.unchanged ?? 0} unchanged" aria-label="${sum.unchanged ?? 0} unchanged"><span class="material-symbols-rounded" aria-hidden="true">check_circle</span><strong>${sum.unchanged ?? 0}</strong></span>
</div>
${extra ? `<div class="ss-small ss-muted" style="margin-top:8px;text-align:center">${extra}</div>` : ""}`;
}

async function onDiffRun() {
  const page = document.getElementById("page-snapshots");
  if (!page) return;
  const { a, b, ok } = diffSelection();
  if (!ok) return toast("Pick two captures from the same provider and instance", false);
  if (!window.openCaptureCompare) return toast("Capture Compare modal not available", false);
  window.openCaptureCompare({ aPath: a, bPath: b });
}

  function setBusy(on) {
    state.busy = !!on;
    if (!on) {
      setProgress("#ss-restore-progress", false, "", "danger");
      setProgress("#ss-tools-progress", false, "", "danger");
    }
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    $$("#page-snapshots button, #page-snapshots input, #page-snapshots select").forEach((el) => {
      if (!el) return;
      el.disabled = !!on;
    });
    if (!on) {
      // Restore feature-based disabling after busy state.
      try { updateCreateActionsAvailability(); } catch {}
      try { updateRestoreAvailability(); } catch {}
      try { updateSelectionToolsAvailability(); } catch {}
      try { updateDiffAvailability(); } catch {}
    }
  }

  function updateCaptureBusyUI() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const on = !!state.captureBusy;
    page.classList.toggle("ss-capture-running", on);
    $$(".ss-lockable", page).forEach((card) => card.classList.toggle("ss-locked", on));
    const lockMsg = $("#ss-capture-lock", page);
    if (lockMsg) lockMsg.classList.toggle("hidden", !on);
    updateCreateActionsAvailability();
  }

  function repopProviders() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const provSel = $("#ss-prov", page);
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
    fill(fProv, true);

    // Provider dropdowns with brand icons
    _enhanceProviderIconSelect(provSel);
    _enhanceProviderIconSelect(fProv);

    repopFeatures();
    repopCreateInstances();
    repopRestoreInstances(state.selectedSnap);
    updateBrowserFilterBar();
    updateCreateReview();

wireDiffControls();
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
        o.textContent = k || "All features";
        fFeat.appendChild(o);
      });
    }
    const fKind = $("#ss-filter-kind", page);
    if (fKind && fKind.options.length === 0) {
      [
        ["", "All types"],
        ["manual", "Manual"],
        ["auto", "Auto"],
      ].forEach(([value, label]) => {
        const o = document.createElement("option");
        o.value = value;
        o.textContent = label;
        fKind.appendChild(o);
      });
    }
    updateBrowserFilterBar();
  }

  function isAutoCapture(snap) {
    const label = String(snap?.label || "").trim().toLowerCase();
    return label.startsWith("auto-");
  }

  function updateBrowserFilterBar() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const provSel = $("#ss-filter-provider", page);
    const featSel = $("#ss-filter-feature", page);
    const kindSel = $("#ss-filter-kind", page);
    const clearBtn = $("#ss-filter-clear", page);
    const provWrap = $("#ss-filter-provider-wrap", page);
    const featWrap = $("#ss-filter-feature-wrap", page);
    const kindWrap = $("#ss-filter-kind-wrap", page);

    const sync = (sel, wrap) => {
      if (!sel || !wrap) return false;
      const hasValue = !!String(sel.value || "").trim();
      wrap.classList.toggle("active", hasValue);
      return hasValue;
    };

    const hasProvider = sync(provSel, provWrap);
    const hasFeature = sync(featSel, featWrap);
    const hasKind = sync(kindSel, kindWrap);

    if (clearBtn) {
      clearBtn.disabled = !hasProvider && !hasFeature && !hasKind;
      clearBtn.classList.toggle("active", hasProvider || hasFeature || hasKind);
    }
  }

  function renderList() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    updateCaptureBusyUI();

    const list = $("#ss-list", page);
    if (!list) return;

    const fp = String($("#ss-filter-provider", page)?.value || "").trim().toLowerCase();
    const ff = String($("#ss-filter-feature", page)?.value || "").trim().toLowerCase();
    const fk = String($("#ss-filter-kind", page)?.value || "").trim().toLowerCase();
    const all = state.snapshots || [];
    const idx = buildBundleIndex(all);
    const hiddenChildPaths = new Set();
    const childFeaturesByKey = {};

    Object.keys(idx.childrenByKey || {}).forEach((k) => {
      const kids = idx.childrenByKey[k] || [];
      childFeaturesByKey[k] = new Set(kids.map((x) => String(x.feature || "").toLowerCase()));
      kids.forEach((x) => { if (x && x.path) hiddenChildPaths.add(String(x.path)); });
    });

    const matches = (s) => {
      const prov = String(s.provider || "").toLowerCase();
      const feat = String(s.feature || "").toLowerCase();
      const kind = isAutoCapture(s) ? "auto" : "manual";
      if (fp && prov !== fp) return false;
      if (fk && kind !== fk) return false;
      if (ff) {
        if (feat === ff) {
          // direct feature match
        } else if (feat === "all") {
          const set = childFeaturesByKey[bundleKey(s)];
          if (!set || !set.has(ff)) return false;
        } else {
          return false;
        }
      }
      return true;
    };

    const allowChildren = !!ff;
    const top = [];
    all.forEach((s) => {
      if (!s) return;
      const isChild = hiddenChildPaths.has(String(s.path || ""));
      if (!allowChildren && isChild) return;
      if (matches(s)) top.push(s);
    });
    const topOnly = allowChildren ? top : top.filter((s) => !hiddenChildPaths.has(String(s.path || "")));
    const limit = state.showAll ? topOnly.length : (state.listLimit || 8);
    const rows = topOnly.slice(0, limit);

    const footer = $("#ss-list-footer", page);
    if (footer) {
      footer.innerHTML = "";
      if (topOnly.length > limit) {
        footer.innerHTML = `<div class="ss-small ss-muted">Showing ${limit} of ${topOnly.length} captures</div><button id="ss-more" class="btn">Show all (${topOnly.length})</button>`;
      } else if (state.showAll && topOnly.length > (state.listLimit || 8)) {
        footer.innerHTML = `<div class="ss-small ss-muted">Showing ${topOnly.length} of ${topOnly.length} captures</div><button id="ss-less" class="btn">Show less</button>`;
      } else {
        footer.innerHTML = topOnly.length ? `<div class="ss-small ss-muted">Showing ${topOnly.length} capture${topOnly.length === 1 ? "" : "s"}</div>` : "";
      }
      $("#ss-more", footer)?.addEventListener("click", () => { state.showAll = true; renderList(); });
      $("#ss-less", footer)?.addEventListener("click", () => { state.showAll = false; renderList(); });
    }

    if (rows.length === 0) {
      list.innerHTML = `<div class="ss-empty">No captures found.</div>`;
      updateSelectionToolsAvailability();
      return;
    }

    list.innerHTML = `
      <div class="ss-table-wrap">
        <table class="ss-table">
          <thead>
            <tr>
              <th class="ss-col-check"></th>
              <th class="ss-col-provider">Provider</th>
              <th class="ss-col-feature">Feature</th>
              <th class="ss-col-type">Type</th>
              <th class="ss-col-label">Label</th>
              <th class="ss-col-created">Created</th>
              <th class="ss-col-profile">Profile</th>
              <th class="ss-col-menu"></th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>`;
    const body = $("tbody", list);
    const pathToSnap = new Map();
    (all || []).forEach((s) => { if (s && s.path) pathToSnap.set(String(s.path), s); });

    const renderRow = (s, opts = {}) => {
      const child = !!opts.child;
      const childCount = Number(opts.childCount || 0);
      const path = String(s.path || "");
      const picks = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];
      const checked = path && picks.includes(path);
      const feat = String(s.feature || "-").toLowerCase();
      const featureLabel = feat === "all" ? "All features" : feat;
      const isBundle = feat === "all";
      const exp = !!(state.expandedBundles && state.expandedBundles[path]);
      const ixPick = path ? picks.indexOf(path) : -1;
      const abTag = ixPick === 0 ? "A" : (ixPick === 1 ? "B" : "");
      const extra = isBundle && childCount ? `<button class="ss-mini" data-act="toggle">${exp ? "Hide" : "Show"} ${childCount}</button>` : "";
      const label = s.label ? _uiCaptureLabel(s.label) : "-";

      const item = document.createElement("tr");
      item.className = `${child ? "child " : ""}${state.selectedPath === path ? "active " : ""}${checked ? "checked" : ""}`;
      item.dataset.path = path;
      item.innerHTML = `
        <td class="ss-col-check"><span class="ss-pickcell">${abTag ? `<span class="ss-ab ${abTag === "A" ? "a" : "b"}" data-act="diffremove" title="Unselect capture">${abTag}</span>` : ""}<input class="ss-chk" type="checkbox" name="ss-diffpick" title="Select capture" data-act="diffpick" ${checked ? "checked" : ""} /></span></td>
        <td class="ss-col-provider"><span class="ss-badge ok">${escapeHtml((s.provider || "-").toUpperCase())}</span></td>
        <td class="ss-col-feature" title="${escapeHtml(featureLabel)}"><span class="ss-feature-cell"><span class="ss-feature-label">${escapeHtml(featureLabel)}</span>${extra}</span></td>
        <td class="ss-col-type">${escapeHtml(snapTypeLabel(s))}</td>
        <td class="ss-col-label ${s.label ? "" : "ss-row-muted"}">${escapeHtml(String(label).slice(0, 60))}</td>
        <td class="ss-col-created">${escapeHtml(snapCreatedText(s))}</td>
        <td class="ss-col-profile">${escapeHtml(snapInstance(s))}</td>
        <td class="ss-col-menu"><span class="material-symbols-rounded">chevron_right</span></td>
      `;

      const pick = item.querySelector('input[data-act="diffpick"]');
      pick?.addEventListener("click", (ev) => { ev.stopPropagation(); });
      pick?.addEventListener("change", (ev) => toggleDiffPick(path, !!ev.currentTarget.checked));
      item.querySelector('[data-act="diffremove"]')?.addEventListener("click", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        toggleDiffPick(path, false);
      });
      item.querySelector('[data-act="toggle"]')?.addEventListener("click", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        state.expandedBundles = state.expandedBundles || {};
        state.expandedBundles[path] = !state.expandedBundles[path];
        renderList();
        try { repopDiffSelects(); } catch {}
      });
      item.addEventListener("click", () => {
        if (state.captureBusy) return;
        if (path && state.selectedPath === path) {
          state.selectedPath = "";
          state.selectedSnap = null;
          renderList();
          renderSelected();
          updateRestoreAvailability();
          return;
        }
        selectSnapshot(path);
      });
      body?.appendChild(item);
    };

    rows.forEach((s) => {
      const feat = String(s.feature || "").toLowerCase();
      if (feat === "all") {
        const kids = idx.childrenByKey[bundleKey(s)] || [];
        renderRow(s, { childCount: kids.length });
        if (state.expandedBundles && state.expandedBundles[String(s.path || "")]) {
          kids.forEach((c) => renderRow(pathToSnap.get(String(c.path || "")) || c, { child: true }));
        }
      } else {
        renderRow(s);
      }
    });

    updateSelectionToolsAvailability();
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
    const created = s.created_at ? new Date(String(s.created_at)).toLocaleString() : "-";
    const selectedStats = (featStats ? Object.entries(featStats) : Object.entries(by))
      .slice(0, 4)
      .map(([k, v]) => `<div class="ss-selected-stat"><strong>${Number(v || 0)}</strong><span>${escapeHtml(String(k || ""))}</span></div>`)
      .join("");

    host.classList.remove("ss-selected-empty","ss-muted");
    host.innerHTML = `
      <div class="ss-selected-summary">
        <div class="ss-item-meta">
          <span class="ss-badge ok">${String(s.provider || "").toUpperCase()}</span>
          ${showInst ? `<span class="ss-badge">${escapeHtml(inst)}</span>` : ``}
          <span class="ss-badge">${String(s.feature || "").toLowerCase()}</span>
          ${s.label ? `<span class="ss-badge warn">${escapeHtml(_uiCaptureLabel(s.label)).slice(0, 40)}</span>` : ``}
        </div>
        <div class="ss-selected-count"><strong>${Number(stats.count || 0)}</strong><span>items</span></div>
      </div>
      <div class="ss-selected-meta">
        <div class="ss-selected-kv"><b>Captured</b> ${escapeHtml(created)}</div>
      </div>
      ${selectedStats ? `<div class="ss-selected-stats">${selectedStats}</div>` : ``}
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
    if (authSetupPending()) return;
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const now = Date.now();
    if (!force && now - state.lastRefresh < 2500) return;
    state.lastRefresh = now;
    const bust = `_=${now}`;
    const manifestUrl = `/api/snapshots/manifest?${bust}`;
    const listUrl = `/api/snapshots/list?${bust}`;

    const wasBusy = !!state.busy;
    if (!wasBusy) setBusy(true);
    setRefreshSpinning(true);
    try {
      const [m, l] = await Promise.all([
        API()(manifestUrl),
        API()(listUrl),
      ]);

      state.providers = (m && m.providers) ? m.providers : [];
      {
        const visibleProviders = configuredProviderIds();
        const rows = (l && l.snapshots) ? l.snapshots : [];
        state.snapshots = Array.isArray(rows)
          ? rows.filter((snap) => visibleProviders.has(String(snap?.provider || "").trim().toLowerCase()))
          : [];
      }

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
      resetRestoreConfirm();
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
    const label = String($("#ss-label", page)?.value || "").trim().slice(0, 12);

    if (!provider) return toast("Pick a provider first", false);
    if (!feature) return toast("Pick a feature", false);
    const progressId = newCaptureProgressId();
    state.captureProgressId = progressId;
    state.captureBusy = true;
    updateCaptureBusyUI();

    showCaptureProgressModal({ provider, instance, feature });
    setBusy(true);
    try {
      const r = await POST_JSON("/api/snapshots/create", { provider, instance, feature, label, progress_id: progressId, background: true }, 30000);
      const snap = r && r.snapshot ? r.snapshot : null;
      const progress = snap ? {
        ok: true,
        done: true,
        stage: "done",
        message: "Capture complete.",
        percent: 100,
        provider,
        instance,
        feature: snap.feature || feature,
        current_feature: snap.feature || feature,
        items_done: Number(snap?.stats?.count || 0),
        total_items: Number(snap?.stats?.count || 0),
        result_path: snap.path || "",
      } : await waitForCaptureProgress(progressId);
      if (!progress) throw new Error("Capture progress was interrupted.");
      if (progress.ok === false) throw new Error(progress.error || progress.message || "Capture failed");
      updateCaptureProgressModal(progress);
      $("#ss-label", page).value = "";
      await refresh(true, false);

      const resultPath = String(progress.result_path || snap?.path || "");
      if (resultPath) {
        await selectSnapshot(resultPath);
      }
      setTimeout(() => refresh(true, false), 450);
      setTimeout(() => refresh(true, false), 1600);
      toast("Capture created", true);
    } catch (e) {
      console.warn("[snapshots] create failed", e);
      const msg = String(e && e.message ? e.message : e);
      updateCaptureProgressModal({
        ok: false,
        done: true,
        stage: "error",
        message: msg.toLowerCase().includes("timeout") ? "Capture is still running longer than expected." : msg,
        percent: 100,
        provider,
        instance,
        feature,
        current_feature: feature,
      });
      if (msg.toLowerCase().includes("timeout")) {
        toast("Create is taking longer than expected. Refreshing...", true);
        setTimeout(() => refresh(true, false), 1200);
        setTimeout(() => refresh(true, false), 5000);
      } else {
        toast(`Snapshot create failed: ${msg}`, false);
      }
    } finally {
      state.captureBusy = false;
      updateCaptureBusyUI();
      stopCaptureProgressPoll();
      setBusy(false);
    }
  }

  function readScheduleDraft() {
    const page = document.getElementById("page-snapshots");
    if (!page) return null;
    const provider = String($("#ss-prov", page)?.value || "").toUpperCase();
    const instance = String($("#ss-prov-inst", page)?.value || "default");
    const feature = String($("#ss-feature", page)?.value || "").toLowerCase();
    const label = String($("#ss-label", page)?.value || "").trim().slice(0, 12) || "auto-{provider}-{feature}-{date}";
    if (!provider) {
      toast("Pick a provider first", false);
      return null;
    }
    if (!feature) {
      toast("Pick a feature first", false);
      return null;
    }
    return { provider, instance, feature, label_template: label };
  }

  function scheduleQueueKey(item) {
    return [
      String(item?.provider || "").trim().toUpperCase(),
      String(item?.instance || "default").trim() || "default",
      String(item?.feature || "").trim().toLowerCase(),
      String(item?.label_template || "").trim(),
    ].join("|");
  }

  function renderScheduleQueue() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const host = $("#ss-schedule-queue", page);
    const wrap = $("#ss-schedule-queue-wrap", page);
    const sendBtn = $("#ss-send-schedule-queue", page);
    const clearBtn = $("#ss-clear-schedule-queue", page);
    if (!host) return;
    const items = Array.isArray(state.scheduleQueue) ? state.scheduleQueue : [];
    if (wrap) wrap.classList.toggle("hidden", !items.length);
    if (sendBtn) sendBtn.disabled = !items.length;
    if (clearBtn) clearBtn.disabled = !items.length;
    if (!items.length) {
      host.innerHTML = "";
      return;
    }
    host.innerHTML = items.map((item, index) => {
      const provider = String(item.provider || "").toUpperCase();
      const instance = String(item.instance || "default");
      const feature = String(item.feature || "").toLowerCase();
      const label = String(item.label_template || "");
      const showInst = instance && instance.toLowerCase() !== "default";
      const main = [provider, ...(showInst ? [instance] : []), feature === "all" ? "all features" : feature].filter(Boolean).join(" / ");
      return `<div class="ss-queue-item">
        <div class="ss-queue-copy">
          <div class="ss-queue-main">${escapeHtml(main)}</div>
          <div class="ss-queue-sub">${escapeHtml(label)}</div>
        </div>
        <button class="btn" type="button" data-queue-remove="${index}">Remove</button>
      </div>`;
    }).join("");
    $$("[data-queue-remove]", host).forEach((btn) => {
      btn.addEventListener("click", () => {
        const ix = parseInt(String(btn.getAttribute("data-queue-remove") || "-1"), 10);
        if (!Number.isFinite(ix) || ix < 0) return;
        state.scheduleQueue.splice(ix, 1);
        renderScheduleQueue();
        setScheduleQueueFeedback("Removed from queue", true);
      });
    });
  }

  function setScheduleQueueFeedback(message, ok = true, ms = 1800) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const feedback = $("#ss-queue-feedback", page);
    if (feedback) {
      feedback.textContent = String(message || "");
      feedback.style.display = message ? "block" : "none";
      feedback.style.color = ok ? "rgba(224,255,236,.94)" : "rgba(255,232,232,.94)";
      feedback.style.borderColor = ok ? "rgba(98,238,181,.22)" : "rgba(255,122,122,.22)";
      feedback.style.background = ok ? "rgba(25,195,125,.08)" : "rgba(255,77,79,.08)";
    }

    const btn = $("#ss-add-schedule", page);
    if (!btn || !message) return;
    const label = btn.querySelector(".ss-btn-label");
    btn.dataset.defaultText ||= label?.textContent || "Queue for scheduler";
    if (btn.__ssQueueFlashTimer) clearTimeout(btn.__ssQueueFlashTimer);
    if (label) label.textContent = message;
    else btn.textContent = message;
    btn.style.borderColor = ok ? "rgba(98,238,181,.30)" : "rgba(255,122,122,.26)";
    btn.style.boxShadow = ok ? "0 0 12px rgba(25,195,125,.18)" : "0 0 12px rgba(255,77,79,.18)";
    btn.__ssQueueFlashTimer = setTimeout(() => {
      if (label) label.textContent = btn.dataset.defaultText || "Queue for scheduler";
      else btn.textContent = btn.dataset.defaultText || "Queue for scheduler";
      btn.style.borderColor = "";
      btn.style.boxShadow = "";
    }, ms);
  }

  function setToolsOutput(message, ok = true, ms = 3200) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const out = $("#ss-tools-out", page);
    if (!out) return;
    const text = String(message || "").trim();
    out.textContent = text;
    out.classList.toggle("hidden", !text);
    out.style.color = ok ? "rgba(224,255,236,.94)" : "rgba(255,232,232,.94)";
    out.style.borderColor = ok ? "rgba(98,238,181,.22)" : "rgba(255,122,122,.22)";
    out.style.background = ok ? "rgba(25,195,125,.08)" : "rgba(255,77,79,.08)";
    out.style.padding = text ? "9px 10px" : "";
    out.style.borderRadius = text ? "7px" : "";
    out.style.borderStyle = text ? "solid" : "";
    out.style.borderWidth = text ? "1px" : "";
    if (out.__ssToolsTimer) clearTimeout(out.__ssToolsTimer);
    if (text && ms > 0) {
      out.__ssToolsTimer = setTimeout(() => {
        out.textContent = "";
        out.classList.add("hidden");
      }, ms);
    }
  }

  function onQueueScheduleDraft() {
    const payload = readScheduleDraft();
    if (!payload) {
      setScheduleQueueFeedback("Check provider and feature first", false);
      return;
    }
    const key = scheduleQueueKey(payload);
    if (state.scheduleQueue.some((item) => scheduleQueueKey(item) === key)) {
      setScheduleQueueFeedback("Already in queue", false);
      toast("That capture schedule is already queued", false);
      return;
    }
    state.scheduleQueue.push(payload);
    renderScheduleQueue();
    setScheduleQueueFeedback("Added to queue", true);
    toast(`Queued ${state.scheduleQueue.length} capture schedule${state.scheduleQueue.length === 1 ? "" : "s"}`, true);
  }

  async function onSendScheduleQueue() {
    const items = Array.isArray(state.scheduleQueue) ? state.scheduleQueue.slice() : [];
    if (!items.length) return toast("Queue at least one capture schedule first", false);

    try { window.showTab?.("settings"); } catch {}
    setTimeout(async () => {
      try {
        window.cwSettingsSelect?.("scheduling");
        try { await window.loadScheduling?.(); } catch {}
        const sec = document.getElementById("sec-scheduling");
        if (sec && !sec.classList.contains("open")) window.toggleSection?.("sec-scheduling");
        window.cwSchedSettingsSelect?.("advanced");
        let applied = false;
        for (let attempt = 0; attempt < 6 && !applied; attempt += 1) {
          applied = !!window.prefillCaptureSchedules?.(items.slice());
          if (!applied) await new Promise((resolve) => setTimeout(resolve, 80));
        }
        if (!applied) throw new Error("Unable to add queued capture schedules.");
        state.scheduleQueue = [];
        renderScheduleQueue();
        sec?.scrollIntoView({ behavior: "smooth", block: "start" });
        toast(`Added ${items.length} capture schedule${items.length === 1 ? "" : "s"} to Scheduling. Pick times, then save settings.`, true);
      } catch (e) {
        console.warn("[snapshots] queued scheduler prefill failed", e);
        const msg = String(e && e.message ? e.message : e) || "Unable to add queued capture schedules";
        toast(`Queue send failed: ${msg}`, false);
      }
    }, 80);
  }

  async function onAddToScheduler() {
    onQueueScheduleDraft();
  }

  async function onDeleteSelected() {
    const paths = selectedCapturePaths();
    if (!paths.length) return toast("Select a capture first", false);
    const msg = paths.length === 1
      ? "Delete the selected capture? Bundle captures also remove their child captures."
      : `Delete ${paths.length} selected captures? Bundle captures also remove their child captures.`;
    if (!confirm(msg)) return;

    setBusy(true);
    setRefreshSpinning(true);
    try {
      for (const path of paths) {
        const r = await API()("/api/snapshots/delete", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ path, delete_children: true }),
        });
        const res = r && r.result ? r.result : null;
        const ok = res ? !!res.ok : !!(r && r.ok);
        if (!ok) {
          const err = (res && res.errors && res.errors.length) ? res.errors.join(" | ") : (r && r.error) ? r.error : "Delete failed";
          console.warn("[snapshots]", err);
          toast(err, false);
          return;
        }
      }

      state.selectedPath = "";
      state.selectedSnap = null;
      state.diffPick = [];
      state.diffResult = null;
      renderSelected();
      updateRestoreAvailability();

      await refresh(true, false);
      toast(paths.length === 1 ? "Capture deleted" : "Captures deleted", true);
    } catch (e) {
      console.warn("[snapshots]", "Delete failed: " + (e.message || e));
      toast("Delete failed: " + (e.message || e), false);
    } finally {
      setRefreshSpinning(false);
      setBusy(false);
    }
  }

  async function onExportSelected() {
    const paths = selectedCapturePaths();
    if (!paths.length) return toast("Select at least one capture first", false);
    setBusy(true);
    try {
      const captures = [];
      for (const path of paths) {
        const r = await API()(`/api/snapshots/read?path=${encodeURIComponent(path)}`);
        captures.push({ path, snapshot: r && r.snapshot ? r.snapshot : null });
      }
      const payload = {
        kind: "capture_export",
        exported_at: new Date().toISOString(),
        count: captures.length,
        captures,
      };
      const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = captures.length === 1 ? "crosswatch-capture.json" : "crosswatch-captures.json";
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
      toast("Capture export ready", true);
    } catch (e) {
      console.warn("[snapshots] export failed", e);
      toast(`Export failed: ${e.message || e}`, false);
    } finally {
      setBusy(false);
    }
  }

  async function onCleanupOldCaptures() {
    if (!confirm("Cleanup Captures removes all stored captures. Continue?")) return;
    setProgress("#ss-tools-progress", true, "Cleaning up captures...", "danger");
    setBusy(true);
    try {
      const r = await POST_JSON("/api/snapshots/clear", {});
      const res = r && r.result ? r.result : {};
      const summary = res.summary || {};
      const removedFiles = Number(summary.removed_files ?? res.deleted_count ?? 0);
      const freed = Number(summary.freed_bytes || 0);
      setToolsOutput(`Cleanup complete: removed ${removedFiles.toLocaleString()} capture file${removedFiles === 1 ? "" : "s"}${freed ? `, freed ${humanBytes(freed)}` : ""}.`, true);
      state.selectedPath = "";
      state.selectedSnap = null;
      state.diffPick = [];
      state.diffResult = null;
      renderSelected();
      await refresh(true, false);
      toast("Capture cleanup complete", true);
    } catch (e) {
      console.warn("[snapshots] cleanup failed", e);
      toast(`Cleanup failed: ${e.message || e}`, false);
    } finally {
      setProgress("#ss-tools-progress", false, "", "danger");
      setBusy(false);
    }
  }

  async function onViewDetails() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const out = $("#ss-detail-out", page);
    if (!out) return;
    if (!out.classList.contains("hidden")) {
      out.classList.add("hidden");
      out.textContent = "";
      return;
    }
    const paths = selectedCapturePaths();
    const path = state.selectedPath || paths[0] || "";
    if (!path) return toast("Select or load a capture first", false);
    setBusy(true);
    try {
      const snap = state.selectedPath === path && state.selectedSnap
        ? state.selectedSnap
        : (await API()(`/api/snapshots/read?path=${encodeURIComponent(path)}`))?.snapshot;
      out.classList.remove("hidden");
      out.textContent = JSON.stringify({ path, snapshot: snap }, null, 2);
    } catch (e) {
      console.warn("[snapshots] details failed", e);
      toast(`Details failed: ${e.message || e}`, false);
    } finally {
      setBusy(false);
    }
  }

  async function onRestore() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    if (!state.selectedPath) return toast("Select a snapshot first", false);
    const mode = String($("#ss-restore-mode", page)?.value || "merge").toLowerCase();
    const instance = String($("#ss-restore-inst", page)?.value || "default");

    if (Date.now() > Number(state.restoreConfirmUntil || 0)) {
      armRestoreConfirm();
      return;
    }
    resetRestoreConfirm();

    const provider = String(state.selectedSnap?.provider || "").toUpperCase();
    const feature = String(state.selectedSnap?.feature || "").toLowerCase();
    const progressId = newCaptureProgressId();
    state.captureProgressId = progressId;
    state.captureBusy = true;
    updateCaptureBusyUI();
    showCaptureProgressModal({ operation: "restore", provider, instance, feature });
    setBusy(true);
    try {
      const r = await POST_JSON("/api/snapshots/restore", { path: state.selectedPath, mode, instance, progress_id: progressId, background: true }, 30000);
      const direct = r && r.result ? r.result : null;
      const progress = direct ? {
        ok: !!direct.ok,
        done: true,
        operation: "restore",
        stage: direct.ok ? "done" : "error",
        message: direct.message || "Restore complete.",
        percent: 100,
        provider: direct.provider || provider,
        instance: direct.instance || instance,
        feature: direct.feature || feature,
        current_feature: direct.feature || feature,
        added: Number(direct.added || 0),
        removed: Number(direct.removed || 0),
        restore_result: direct,
      } : await waitForCaptureProgress(progressId);

      if (!progress) throw new Error("Restore progress was interrupted.");
      if (progress.ok === false) throw new Error(progress.error || progress.message || "Restore failed");
      updateCaptureProgressModal(progress);
      const res = progress.restore_result || progress.result || {};
      const out = $("#ss-restore-out", page);
      if (out) {
        if (res.ok) out.textContent = `Done. Added ${res.added || 0}, removed ${res.removed || 0}.`;
        else out.textContent = `Restore finished with errors: ${(res.errors || []).join("; ") || "unknown error"}`;
      }

      toast(res.ok ? "Restore complete" : "Restore finished with errors", !!res.ok);
      await refresh(true, false);
    } catch (e) {
      console.warn("[snapshots] restore failed", e);
      toast(`Restore failed: ${e.message || e}`, false);
      const out = $("#ss-restore-out", page);
      if (out) out.textContent = `Restore failed: ${e.message || e}`;
      updateCaptureProgressModal({
        ok: false,
        done: true,
        operation: "restore",
        stage: "error",
        message: String(e?.message || e || "Restore failed"),
        percent: 100,
        provider,
        instance,
        feature,
        current_feature: feature,
      });
    } finally {
      state.captureBusy = false;
      updateCaptureBusyUI();
      stopCaptureProgressPoll();
      setBusy(false);
    }
  }
  async function init() {
    if (authSetupPending()) return retryInitAfterAuth();
    injectCss();
    render();
    await refresh(true, false);
  }

  function retryInitAfterAuth() {
    if (authRetryWired) return;
    authRetryWired = true;
    const retry = () => {
      if (authSetupPending()) return;
      authRetryWired = false;
      window.removeEventListener("cw-auth-setup-pending", onAuth);
      init();
    };
    const onAuth = (e) => { if (e?.detail?.pending === false) retry(); };
    window.addEventListener("cw-auth-setup-pending", onAuth);
    Promise.resolve(window.__cwAuthBootstrapPromise).catch(() => null).finally(retry);
  }

  function refreshOrInit(force = false) {
    const page = document.getElementById("page-snapshots");
    return page && !page.children.length ? init() : refresh(!!force);
  }

  // public hook for core.js
  window.Snapshots = {
    refresh: refreshOrInit,
    init,
  };

  if (document.getElementById("page-snapshots")) {
    init();
  } else {
    document.addEventListener("tab-changed", (e) => {
      if (authSetupPending()) return;
      if (e?.detail?.id === "snapshots") {
        try { init(); } catch {}
      }
    });
  }

})();
