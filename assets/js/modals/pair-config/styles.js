/* assets/js/modals/pair-config/styles.js */
/* Injected styles for the pair-config modal. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

export function ensurePairConfigStyles(){
  const id = "cx-pair-config-css";
  let el = document.getElementById(id);
  if(el) return;
  el = document.createElement("style");
  el.id = id;
  el.textContent = `
    .cx-modal-shell.pair-config-modal{width:var(--cxModalW,min(var(--cxModalMaxW,1280px),calc(100vw - 64px)))!important;max-width:min(var(--cxModalMaxW,1280px),calc(100vw - 64px))!important;height:min(var(--cxModalMaxH,92vh),calc(100vh - 48px))!important}
    .cx-modal-shell.pair-config-modal #cx-modal.cx-card{background:
      radial-gradient(95% 140% at 0% 0%,rgba(90,72,193,.2) 0%,rgba(90,72,193,0) 38%),
      radial-gradient(85% 120% at 100% 100%,rgba(31,89,173,.14) 0%,rgba(31,89,173,0) 42%),
      linear-gradient(180deg,rgba(7,10,18,.992),rgba(3,5,11,.992));
      border:1px solid rgba(255,255,255,.06);border-radius:24px;
      box-shadow:0 36px 110px rgba(0,0,0,.62),inset 0 1px 0 rgba(255,255,255,.03),0 0 0 1px rgba(108,92,213,.08);
      overflow:hidden!important}
    .cx-modal-shell.pair-config-modal #cx-modal .cx-head{padding:15px 19px 14px;border-bottom:1px solid rgba(255,255,255,.05);background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.006));backdrop-filter:blur(12px)}
    .cx-modal-shell.pair-config-modal #cx-modal .title-wrap{display:flex;align-items:flex-start;gap:12px;min-width:0}
    .cx-modal-shell.pair-config-modal #cx-modal .app-logo{display:grid;place-items:center;width:42px;height:42px;border-radius:14px;background:linear-gradient(180deg,rgba(89,79,192,.34),rgba(58,87,178,.18));border:1px solid rgba(255,255,255,.07);box-shadow:0 14px 28px rgba(0,0,0,.34),inset 0 1px 0 rgba(255,255,255,.08);color:#eef2ff}
    .cx-modal-shell.pair-config-modal #cx-modal .app-name{font-size:18px;line-height:1.1;letter-spacing:-.01em}
    .cx-modal-shell.pair-config-modal #cx-modal .app-sub{margin-top:4px;color:rgba(205,214,231,.56)}
    .cx-modal-shell.pair-config-modal #cx-modal .cx-body{padding:13px 19px 7px}
    .cx-modal-shell.pair-config-modal #cx-modal .cx-top{grid-template-columns:minmax(520px,1.02fr) minmax(320px,.98fr);gap:12px 18px;align-items:start}
    .cx-modal-shell.pair-config-modal #cx-modal .cx-main{grid-template-columns:minmax(0,1.02fr) minmax(320px,.98fr);gap:14px 18px}
    .cx-modal-shell.pair-config-modal #cx-modal .top-left{display:grid;gap:8px}
    .cx-modal-shell.pair-config-modal #cx-modal .top-right{display:grid;gap:8px;align-content:stretch}
    .cx-modal-shell.pair-config-modal #cx-modal .cx-st-row{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px 14px;margin:0}
    .cx-modal-shell.pair-config-modal #cx-modal .field label{margin-bottom:4px;color:rgba(209,218,235,.64);letter-spacing:.08em}
    .cx-modal-shell.pair-config-modal #cx-modal .endpoint-field > label,.cx-modal-shell.pair-config-modal #cx-modal .cx-inst-row{display:none}
    .cx-modal-shell.pair-config-modal #cx-modal .endpoint-field{position:relative}
    .cx-modal-shell.pair-config-modal #cx-modal .endpoint-card{--endpoint-rgb:124,92,255;--endpoint-watermark:none;position:relative;overflow:hidden;display:grid;gap:12px;padding:14px 14px 12px;border-radius:20px;border:1px solid rgba(255,255,255,.06);background:
      radial-gradient(120% 140% at 0% 0%,rgba(var(--endpoint-rgb),.12),rgba(var(--endpoint-rgb),0) 40%),
      linear-gradient(180deg,rgba(14,18,30,.88),rgba(5,8,16,.97));box-shadow:inset 0 1px 0 rgba(255,255,255,.025),0 12px 30px rgba(0,0,0,.18)}
    .cx-modal-shell.pair-config-modal #cx-modal .endpoint-card.is-selected{border-color:rgba(var(--endpoint-rgb),.22);box-shadow:inset 0 1px 0 rgba(255,255,255,.035),0 16px 34px rgba(0,0,0,.22),0 0 0 1px rgba(var(--endpoint-rgb),.08)}
    .cx-modal-shell.pair-config-modal #cx-modal .endpoint-card::after{content:"";position:absolute;right:-18px;bottom:-16px;width:170px;height:170px;border-radius:50%;background-image:var(--endpoint-watermark);background-repeat:no-repeat;background-position:center;background-size:contain;opacity:.11;filter:drop-shadow(0 0 24px rgba(var(--endpoint-rgb),.22));mix-blend-mode:screen;pointer-events:none;transform:scale(1.16)}
    .cx-modal-shell.pair-config-modal #cx-modal .endpoint-top{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;cursor:pointer}
    .cx-modal-shell.pair-config-modal #cx-modal .endpoint-identity{display:flex;align-items:center;gap:12px;min-width:0}
    .cx-modal-shell.pair-config-modal #cx-modal .endpoint-logo{display:grid;place-items:center;flex:0 0 auto;width:46px;height:46px;border-radius:16px;border:1px solid rgba(var(--endpoint-rgb),.24);background:linear-gradient(180deg,rgba(16,20,34,.96),rgba(7,10,18,.98));box-shadow:0 8px 22px rgba(0,0,0,.22),0 0 24px rgba(var(--endpoint-rgb),.14)}
    .cx-modal-shell.pair-config-modal #cx-modal .endpoint-logo .prov-logo{width:34px!important;height:34px!important}
    .cx-modal-shell.pair-config-modal #cx-modal .endpoint-logo .prov-fallback{font-weight:900;color:#f5f8ff}
    .cx-modal-shell.pair-config-modal #cx-modal .endpoint-copy{display:grid;gap:2px;min-width:0}
    .cx-modal-shell.pair-config-modal #cx-modal .endpoint-name{font-size:16px;font-weight:900;color:#f6f9ff;letter-spacing:-.01em}
    .cx-modal-shell.pair-config-modal #cx-modal .endpoint-meta{color:rgba(203,212,228,.62);font-size:12px;line-height:1.4}
    .cx-modal-shell.pair-config-modal #cx-modal .endpoint-meta-type,.cx-modal-shell.pair-config-modal #cx-modal .endpoint-meta-cap{display:block}
    .cx-modal-shell.pair-config-modal #cx-modal .endpoint-meta-cap{color:rgba(203,212,228,.52)}
    .cx-modal-shell.pair-config-modal #cx-modal .endpoint-role{position:relative;z-index:1;flex:0 0 auto;display:inline-flex;align-items:center;justify-content:center;min-height:28px;padding:0 10px;border-radius:999px;border:1px solid rgba(255,255,255,.08);background:rgba(17,21,33,.72);font-size:11px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:rgba(236,241,252,.78)}
    .cx-modal-shell.pair-config-modal #cx-modal .endpoint-profile-select{border-color:rgba(255,255,255,.07);box-shadow:inset 0 1px 0 rgba(255,255,255,.03),0 0 0 1px rgba(var(--endpoint-rgb),.04);margin-top:2px}
    .cx-modal-shell.pair-config-modal #cx-modal .endpoint-profile-select:hover{border-color:rgba(var(--endpoint-rgb),.26)}
    .cx-modal-shell.pair-config-modal #cx-modal .cx-row .input,
    .cx-modal-shell.pair-config-modal #cx-modal .cx-row select{background:linear-gradient(180deg,rgba(2,6,17,.98),rgba(3,7,18,.995));border-color:rgba(255,255,255,.06);border-radius:14px;box-shadow:inset 0 1px 0 rgba(255,255,255,.025),0 0 0 1px rgba(255,255,255,.015)}
    .cx-modal-shell.pair-config-modal #cx-modal .cx-row .input:hover,
    .cx-modal-shell.pair-config-modal #cx-modal .cx-row select:hover{border-color:rgba(122,108,228,.28)}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-card{--flow-src-rgb:87,160,255;--flow-dst-rgb:167,139,250;--flow-feature-rgb:124,92,255;min-width:0;height:100%;padding:13px 15px;border-radius:22px;border:1px solid rgba(255,255,255,.06);background:
      radial-gradient(95% 130% at 0% 0%,rgba(var(--flow-src-rgb),.16) 0%,rgba(var(--flow-src-rgb),0) 42%),
      radial-gradient(95% 130% at 100% 100%,rgba(var(--flow-dst-rgb),.15) 0%,rgba(var(--flow-dst-rgb),0) 42%),
      linear-gradient(180deg,rgba(16,21,36,.72),rgba(8,11,20,.9));box-shadow:inset 0 1px 0 rgba(255,255,255,.035),0 0 0 1px rgba(255,255,255,.015)}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-card{display:grid;grid-template-rows:auto minmax(0,1fr);gap:11px}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-head{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-copy{display:grid;gap:8px}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-title{margin-bottom:0;font-size:12px;color:rgba(206,214,229,.62);text-transform:uppercase;letter-spacing:.06em}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-title span{color:#fff;text-transform:none;letter-spacing:0}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty{position:relative;overflow:hidden;isolation:isolate;width:100%!important;min-height:74px!important;height:auto!important;padding:9px 16px!important;border-radius:18px!important;border-color:rgba(255,255,255,.06)!important;background:linear-gradient(180deg,rgba(18,23,38,.78),rgba(8,11,19,.94))!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.035),0 0 0 1px rgba(255,255,255,.015),0 18px 40px rgba(0,0,0,.22);align-items:center!important;justify-self:stretch!important}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty::before{content:none}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty::after{content:none}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty .token{position:relative;overflow:hidden;width:56px!important;height:56px!important;border-radius:17px!important;box-shadow:0 12px 30px rgba(0,0,0,.34),inset 0 0 0 1px rgba(255,255,255,.04)!important}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty .token::before{content:"";position:absolute;inset:0;border-radius:inherit;background:linear-gradient(180deg,rgba(255,255,255,.10),rgba(255,255,255,0) 42%);opacity:.8;pointer-events:none}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty .token::after{content:"";position:absolute;inset:-24%;border-radius:inherit;filter:blur(16px);opacity:.55;pointer-events:none}
    .cx-modal-shell.pair-config-modal #cx-modal #cx-flow-src.token{background:linear-gradient(180deg,rgba(18,22,34,.96),rgba(7,10,18,.98)),radial-gradient(120% 120% at 50% 0%,rgba(var(--flow-src-rgb),.30),rgba(var(--flow-src-rgb),0) 64%)!important;box-shadow:0 0 0 1px rgba(var(--flow-src-rgb),.28),0 16px 36px rgba(0,0,0,.34),0 0 34px rgba(var(--flow-src-rgb),.16),inset 0 0 0 1px rgba(255,255,255,.04)!important}
    .cx-modal-shell.pair-config-modal #cx-modal #cx-flow-dst.token{background:linear-gradient(180deg,rgba(18,22,34,.96),rgba(7,10,18,.98)),radial-gradient(120% 120% at 50% 0%,rgba(var(--flow-dst-rgb),.30),rgba(var(--flow-dst-rgb),0) 64%)!important;box-shadow:0 0 0 1px rgba(var(--flow-dst-rgb),.28),0 16px 36px rgba(0,0,0,.34),0 0 34px rgba(var(--flow-dst-rgb),.16),inset 0 0 0 1px rgba(255,255,255,.04)!important}
    .cx-modal-shell.pair-config-modal #cx-modal #cx-flow-src.token::after{background:radial-gradient(circle at 50% 50%,rgba(var(--flow-src-rgb),.45),rgba(var(--flow-src-rgb),0) 62%)}
    .cx-modal-shell.pair-config-modal #cx-modal #cx-flow-dst.token::after{background:radial-gradient(circle at 50% 50%,rgba(var(--flow-dst-rgb),.45),rgba(var(--flow-dst-rgb),0) 62%)}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty .token .prov-wrap{width:100%;height:100%;display:grid;place-items:center}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty .token .prov-logo{width:44px!important;height:44px!important;filter:drop-shadow(0 8px 14px rgba(0,0,0,.26))}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty .arrow{position:relative;overflow:hidden;height:13px!important;border-radius:999px;background:linear-gradient(90deg,rgba(var(--flow-src-rgb),.24),rgba(var(--flow-feature-rgb),.24) 50%,rgba(var(--flow-dst-rgb),.28))!important;background-size:135% 100%!important;box-shadow:0 0 0 1px rgba(255,255,255,.04) inset!important}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty .arrow::before{content:"";position:absolute;left:1.8%;right:1.8%;top:50%;height:4px;border-radius:999px;transform:translateY(-50%);background:linear-gradient(90deg,rgba(255,255,255,.18),rgba(255,255,255,.34) 48%,rgba(255,255,255,.18));opacity:.56}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty .arrow.anim-one{animation:cxFlowRailMove 3.4s linear infinite}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty .arrow.anim-two{animation:cxFlowRailPingPong 3.8s ease-in-out infinite}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty .dot{position:absolute;top:50%;left:var(--flow-start,6%);transform:translate(-50%,-50%) scale(1);width:15px;height:15px;border-radius:999px;background:rgb(var(--flow-dot-rgb,var(--flow-feature-rgb)));box-shadow:0 0 12px rgba(var(--flow-dot-rgb,var(--flow-feature-rgb)),.68),0 0 18px rgba(var(--flow-dot-rgb,var(--flow-feature-rgb)),.18);will-change:left,transform,opacity}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty .dot.anim-one{animation:cxFlowDotTravelOne 3.1s linear infinite;animation-delay:var(--flow-delay,0s)}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty .dot.anim-two{animation:cxFlowDotTravelTwo 3.8s ease-in-out infinite;animation-delay:var(--flow-delay,0s)}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-feature-dots{display:flex;align-items:center;gap:10px}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-feature-dot{width:12px;height:12px;border-radius:999px;background:rgba(255,255,255,.14);box-shadow:inset 0 0 0 2px rgba(255,255,255,.10);opacity:.56;transition:transform .16s ease,opacity .16s ease,box-shadow .16s ease,background .16s ease}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-feature-dot.on{opacity:1;transform:scale(1.02);box-shadow:0 0 10px rgba(var(--dot-rgb),.78),0 0 20px rgba(var(--dot-rgb),.24);background:rgb(var(--dot-rgb))}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-feature-dot.wl{--dot-rgb:0,255,163}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-feature-dot.rt{--dot-rgb:255,196,0}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-feature-dot.hi{--dot-rgb:45,226,255}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-feature-dot.pr{--dot-rgb:167,139,250}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-feature-dot.pl{--dot-rgb:255,0,229}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-warn-area:empty{display:none}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty.off{filter:saturate(.75)}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty.off .arrow{background:linear-gradient(90deg,rgba(132,140,165,.14),rgba(156,164,186,.16))!important;box-shadow:0 0 0 1px rgba(255,255,255,.03) inset!important}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty.off .token{box-shadow:0 0 0 1px rgba(255,255,255,.08),0 10px 24px rgba(0,0,0,.24)!important}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty.off .arrow.anim-one,
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty.off .arrow.anim-two,
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty.off .dot.anim-one,
    .cx-modal-shell.pair-config-modal #cx-modal .flow-rail.pretty.off .dot.anim-two{animation:none}
    @keyframes cxFlowRailMove{
      0%{background-position:0% 50%}
      100%{background-position:100% 50%}
    }
    @keyframes cxFlowRailPingPong{
      0%,100%{background-position:10% 50%}
      50%{background-position:90% 50%}
    }
    @keyframes cxFlowDotTravelOne{
      0%{left:var(--flow-start,6%);transform:translate(-50%,-50%) scale(.84);opacity:0}
      10%{opacity:.98}
      85%{opacity:.98}
      100%{left:var(--flow-end,94%);transform:translate(-50%,-50%) scale(1.08);opacity:0}
    }
    @keyframes cxFlowDotTravelTwo{
      0%{left:var(--flow-start,6%);transform:translate(-50%,-50%) scale(.84);opacity:0}
      10%{opacity:.96}
      50%{left:var(--flow-end,94%);transform:translate(-50%,-50%) scale(1.06);opacity:1}
      60%{opacity:.96}
      100%{left:var(--flow-start,6%);transform:translate(-50%,-50%) scale(.84);opacity:0}
    }
    .cx-modal-shell.pair-config-modal #cx-modal .cx-tabsrow{gap:11px 16px;margin-top:3px;align-items:end}
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs{display:flex;align-items:flex-end;gap:8px!important;min-width:0;padding:0 8px 0 10px;overflow-x:auto;overflow-y:visible;scrollbar-width:none}
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs::-webkit-scrollbar{display:none}
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .ftab,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs button,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs a,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .tab{
      position:relative;display:inline-flex;align-items:center;justify-content:center;gap:8px;
      --feat:124,92,255;
      min-height:44px;padding:0 20px;margin-right:0;border:1px solid rgba(255,255,255,.075);
      border-bottom:none;border-radius:18px 18px 0 0;background:
      linear-gradient(180deg,rgba(29,35,53,.96),rgba(15,20,33,.985));
      box-shadow:inset 0 1px 0 rgba(255,255,255,.03),0 10px 22px rgba(0,0,0,.16);
      font-weight:800;color:rgba(214,222,238,.82);cursor:pointer;transform:translateY(5px);
      transition:transform .18s ease,background .18s ease,border-color .18s ease,color .18s ease,box-shadow .18s ease;
      white-space:nowrap
    }
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .ftab[data-key="watchlist"]{--feat:0,255,163}
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .ftab[data-key="ratings"]{--feat:255,196,0}
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .ftab[data-key="history"]{--feat:45,226,255}
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .ftab[data-key="progress"]{--feat:167,139,250}
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .ftab[data-key="playlists"]{--feat:255,0,229}
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .ftab::before,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs button::before,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs a::before,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .tab::before{
      content:"";position:absolute;inset:1px 1px 0;border-radius:17px 17px 0 0;
      background:linear-gradient(180deg,rgba(255,255,255,.07),rgba(255,255,255,0) 34%);
      opacity:.7;pointer-events:none
    }
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .ftab[data-key="watchlist"]::after,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .ftab[data-key="ratings"]::after,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .ftab[data-key="history"]::after,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .ftab[data-key="progress"]::after,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .ftab[data-key="playlists"]::after{
      content:"";position:absolute;left:14px;right:14px;bottom:8px;height:2px;border-radius:999px;
      background:rgba(var(--feat),.78);box-shadow:0 0 12px rgba(var(--feat),.32);opacity:.9;pointer-events:none
    }
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .ftab .material-symbols-rounded,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs button .material-symbols-rounded,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs a .material-symbols-rounded,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .tab .material-symbols-rounded{font-size:17px;opacity:.8}
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .ftab:hover,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs button:hover,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs a:hover,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .tab:hover{
      color:#eef3ff;border-color:rgba(var(--feat),.28);transform:translateY(3px);
      background:
      radial-gradient(120% 120% at 50% 0%,rgba(var(--feat),.12),rgba(var(--feat),0) 54%),
      linear-gradient(180deg,rgba(40,46,67,.98),rgba(18,23,38,.99));box-shadow:0 14px 28px rgba(0,0,0,.18),0 0 18px rgba(var(--feat),.08)
    }
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .ftab.active,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs button.active,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs a.active,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .tab.active,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs [aria-selected="true"],
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs [data-active="1"],
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .selected{
      z-index:3;color:#f7fbff;border-color:rgba(var(--feat),.42);transform:translateY(0);
      background:
      radial-gradient(120% 130% at 50% 0%,rgba(var(--feat),.28),rgba(var(--feat),0) 52%),
      linear-gradient(180deg,rgba(76,84,164,.96),rgba(36,47,99,.98) 44%,rgba(12,17,31,1) 100%);
      box-shadow:0 14px 30px rgba(var(--feat),.16),0 0 0 1px rgba(255,255,255,.03) inset
    }
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .ftab.active .material-symbols-rounded,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs [aria-selected="true"] .material-symbols-rounded{opacity:1}
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs .ftab:focus-visible,
    .cx-modal-shell.pair-config-modal #cx-modal .feature-tabs button:focus-visible{
      outline:none;z-index:4;box-shadow:0 0 0 3px rgba(109,139,255,.18),0 14px 30px rgba(42,61,145,.28)
    }
    .cx-modal-shell.pair-config-modal #cx-modal .flow-mode-inline .seg{display:inline-flex;align-items:center;gap:4px;padding:4px;border:1px solid rgba(255,255,255,.06);border-radius:16px;background:linear-gradient(180deg,rgba(24,29,43,.86),rgba(9,12,22,.94));box-shadow:inset 0 1px 0 rgba(255,255,255,.025)}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-mode-inline .seg label{padding:9px 13px;border-radius:12px;color:rgba(203,212,229,.68);min-width:96px;text-align:center}
    .cx-modal-shell.pair-config-modal #cx-modal .flow-mode-inline .seg label.disabled{opacity:.42;cursor:not-allowed}
    .cx-modal-shell.pair-config-modal #cx-modal #cx-mode-one:checked + label,
    .cx-modal-shell.pair-config-modal #cx-modal #cx-mode-two:checked + label{background:linear-gradient(135deg,rgba(91,99,231,.92),rgba(75,138,230,.9));box-shadow:0 12px 30px rgba(49,82,197,.28);color:#f4f7ff}
    .cx-modal-shell.pair-config-modal #cx-modal .panel{padding:13px;border-radius:22px;border:1px solid rgba(255,255,255,.055);background:
      radial-gradient(90% 130% at 0% 0%,rgba(80,65,172,.08) 0%,rgba(80,65,172,0) 38%),
      radial-gradient(90% 130% at 100% 100%,rgba(24,82,158,.07) 0%,rgba(24,82,158,0) 42%),
      linear-gradient(180deg,rgba(12,16,27,.9),rgba(4,7,14,.975));box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
    .cx-modal-shell.pair-config-modal #cx-modal .panel .panel-title{margin-bottom:11px;font-size:13px;letter-spacing:.01em;display:flex;align-items:center;gap:8px}
    .cx-modal-shell.pair-config-modal #cx-modal .panel .panel-title .material-symbols-rounded:first-child{flex:0 0 auto}
    .cx-modal-shell.pair-config-modal #cx-modal .panel .panel-title .cx-help{margin-left:0;transform:translateY(1px);flex:0 0 auto}
    .cx-modal-shell.pair-config-modal #cx-modal .panel .panel-title.small{margin-bottom:5px;color:rgba(203,212,228,.58)}
    .cx-modal-shell.pair-config-modal #cx-modal .opt-row{padding:8px 12px;border-radius:18px;border:1px solid rgba(255,255,255,.05);background:linear-gradient(180deg,rgba(16,20,34,.84),rgba(5,8,16,.96));box-shadow:inset 0 1px 0 rgba(255,255,255,.02);margin-bottom:6px;min-height:52px}
    .cx-modal-shell.pair-config-modal #cx-modal .cx-provider-pill{display:inline-flex;align-items:center;justify-content:center;min-width:72px;height:24px;margin-right:10px;padding:0 10px;border-radius:999px;border:1px solid rgba(255,255,255,.10);font-size:11px;font-weight:900;letter-spacing:.08em;text-transform:uppercase;vertical-align:middle;color:#f7fbff;background:linear-gradient(180deg,rgba(255,255,255,.07),rgba(255,255,255,.03));box-shadow:inset 0 1px 0 rgba(255,255,255,.04)}
    .cx-modal-shell.pair-config-modal #cx-modal .cx-provider-pill.provider-trakt{border-color:rgba(255,89,133,.28);background:linear-gradient(180deg,rgba(255,89,133,.18),rgba(255,89,133,.08));box-shadow:inset 0 1px 0 rgba(255,255,255,.04),0 0 18px rgba(255,89,133,.12)}
    .cx-modal-shell.pair-config-modal #cx-modal .cx-provider-pill.provider-mdblist{border-color:rgba(87,160,255,.28);background:linear-gradient(180deg,rgba(87,160,255,.18),rgba(87,160,255,.08));box-shadow:inset 0 1px 0 rgba(255,255,255,.04),0 0 18px rgba(87,160,255,.12)}
    .cx-modal-shell.pair-config-modal #cx-modal .cx-provider-pill.provider-simkl{border-color:rgba(196,92,255,.28);background:linear-gradient(180deg,rgba(196,92,255,.18),rgba(196,92,255,.08));box-shadow:inset 0 1px 0 rgba(255,255,255,.04),0 0 18px rgba(196,92,255,.12)}
    .cx-modal-shell.pair-config-modal #cx-modal .opt-row .t,.cx-modal-shell.pair-config-modal #cx-modal .opt-row strong{font-weight:700}
    .cx-modal-shell.pair-config-modal #cx-modal .opt-row .s{color:rgba(203,211,226,.52)}
    .cx-modal-shell.pair-config-modal #cx-modal .opt-row .switch .slider{border-color:rgba(255,255,255,.1);background:linear-gradient(180deg,rgba(8,11,21,.98),rgba(5,8,16,.98))}
    .cx-modal-shell.pair-config-modal #cx-modal .opt-row .switch input:checked + .slider{background:linear-gradient(135deg,#6d61ff,#4ca0ff);border-color:rgba(110,141,255,.34);box-shadow:0 0 0 1px rgba(255,255,255,.02) inset,0 0 24px rgba(73,122,255,.16)}
    .cx-modal-shell.pair-config-modal #cx-modal .providers-intro{display:flex;align-items:flex-start;justify-content:space-between;gap:14px;margin-bottom:14px;padding:14px 16px;border:1px solid rgba(255,255,255,.05);border-radius:18px;background:linear-gradient(180deg,rgba(18,22,37,.72),rgba(7,10,18,.92))}
    .cx-modal-shell.pair-config-modal #cx-modal .providers-intro-copy{display:grid;gap:5px}
    .cx-modal-shell.pair-config-modal #cx-modal .providers-intro-title{font-weight:800;color:#f5f8ff}
    .cx-modal-shell.pair-config-modal #cx-modal .providers-intro-sub{max-width:760px;color:rgba(203,212,228,.64);line-height:1.45}
    .cx-modal-shell.pair-config-modal #cx-modal .providers-intro-badge{flex:0 0 auto;padding:7px 11px;border-radius:999px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.04);color:rgba(235,241,255,.82);font-weight:700}
    .cx-modal-shell.pair-config-modal #cx-modal .provider-card-list{display:grid;gap:12px}
    .cx-modal-shell.pair-config-modal #cx-modal .provider-card{margin:0;border:1px solid rgba(255,255,255,.055);border-radius:20px;background:linear-gradient(180deg,rgba(17,21,35,.86),rgba(7,10,18,.96));box-shadow:inset 0 1px 0 rgba(255,255,255,.02)}
    .cx-modal-shell.pair-config-modal #cx-modal .provider-card-head{display:flex;align-items:center;justify-content:space-between;gap:18px;padding:16px 18px}
    .cx-modal-shell.pair-config-modal #cx-modal .provider-card-main{display:flex;align-items:center;gap:14px;min-width:0}
    .cx-modal-shell.pair-config-modal #cx-modal .provider-card-badge{display:inline-flex;align-items:center;justify-content:center;min-width:66px;height:36px;padding:0 12px;border-radius:14px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.07),rgba(255,255,255,.03));font-weight:800;color:#f7fbff}
    .cx-modal-shell.pair-config-modal #cx-modal .provider-card-copy{display:grid;gap:2px;min-width:0}
    .cx-modal-shell.pair-config-modal #cx-modal .provider-card-title{font-size:15px;font-weight:800;color:#f5f8ff}
    .cx-modal-shell.pair-config-modal #cx-modal .provider-card-sub{color:rgba(203,212,228,.62);font-weight:600}
    .cx-modal-shell.pair-config-modal #cx-modal .provider-card-meta{display:flex;align-items:center;gap:14px;min-width:0}
    .cx-modal-shell.pair-config-modal #cx-modal .provider-card-hint{max-width:420px;color:rgba(203,212,228,.56);font-size:12px;line-height:1.4;text-align:right}
    .cx-modal-shell.pair-config-modal #cx-modal .provider-card-body{padding:0 18px 18px}
    .cx-modal-shell.pair-config-modal #cx-modal .provider-card.provider-plex{background:
      radial-gradient(120% 140% at 0% 0%,rgba(230,160,32,.10),rgba(230,160,32,0) 38%),
      linear-gradient(180deg,rgba(17,21,35,.88),rgba(7,10,18,.96))}
    .cx-modal-shell.pair-config-modal #cx-modal .provider-card.provider-jellyfin{background:
      radial-gradient(120% 140% at 0% 0%,rgba(87,160,255,.12),rgba(87,160,255,0) 38%),
      linear-gradient(180deg,rgba(17,21,35,.88),rgba(7,10,18,.96))}
    .cx-modal-shell.pair-config-modal #cx-modal .provider-card.provider-emby{background:
      radial-gradient(120% 140% at 0% 0%,rgba(128,96,255,.12),rgba(128,96,255,0) 38%),
      linear-gradient(180deg,rgba(17,21,35,.88),rgba(7,10,18,.96))}
    .cx-modal-shell.pair-config-modal #cx-modal .providers-note{display:grid;gap:5px;margin-top:14px;padding:14px 16px;border:1px solid rgba(255,255,255,.05);border-radius:18px;background:linear-gradient(180deg,rgba(16,20,33,.76),rgba(8,11,19,.95))}
    .cx-modal-shell.pair-config-modal #cx-modal .providers-note-title{display:flex;align-items:center;gap:8px;font-weight:800;color:#f2f6ff}
    .cx-modal-shell.pair-config-modal #cx-modal .providers-note-title .material-symbols-rounded{font-size:18px;color:rgba(110,170,255,.9)}
    .cx-modal-shell.pair-config-modal #cx-modal .providers-note-body{color:rgba(203,212,228,.62);line-height:1.45}
    .cx-modal-shell.pair-config-modal #cx-modal .providers-empty{padding:18px;border:1px dashed rgba(255,255,255,.08);border-radius:18px;color:rgba(203,212,228,.58);text-align:center}
    .cx-modal-shell.pair-config-modal #cx-modal #gl-drop-adv{border-radius:18px;background:linear-gradient(180deg,rgba(14,18,31,.82),rgba(5,8,15,.95));border:1px solid rgba(255,255,255,.045)}
    .cx-modal-shell.pair-config-modal #cx-modal .rules .r{border-radius:16px;border-color:rgba(255,255,255,.055);background:linear-gradient(180deg,rgba(16,20,34,.82),rgba(5,8,16,.96))}
    .cx-modal-shell.pair-config-modal .cx-actions{padding:11px 19px 13px!important;border-top:1px solid rgba(255,255,255,.05)!important;background:linear-gradient(180deg,rgba(9,12,22,.34),rgba(8,11,20,.92) 32%,rgba(6,8,15,.985))!important;backdrop-filter:blur(14px)!important}
    .cx-modal-shell.pair-config-modal .cx-actions .cx-btn{border-radius:18px;padding:12px 18px;background:linear-gradient(180deg,rgba(28,33,46,.88),rgba(10,13,23,.96));border-color:rgba(255,255,255,.09);color:#f1f5ff;box-shadow:inset 0 1px 0 rgba(255,255,255,.025)}
    .cx-modal-shell.pair-config-modal .cx-actions .cx-btn.primary{background:linear-gradient(135deg,#685dff,#6d72ff 38%,#4ba4ff 100%);box-shadow:0 18px 40px rgba(55,91,255,.28),0 0 0 1px rgba(255,255,255,.04) inset}
    @media (max-width:1180px){
      .cx-modal-shell.pair-config-modal #cx-modal .cx-top,
      .cx-modal-shell.pair-config-modal #cx-modal .cx-main{grid-template-columns:1fr}
    }
    @media (max-width:980px){
      .cx-modal-shell.pair-config-modal #cx-modal .cx-st-row{grid-template-columns:repeat(2,minmax(0,1fr))}
      .cx-modal-shell.pair-config-modal #cx-modal .cx-tabsrow{grid-template-columns:1fr}
      .cx-modal-shell.pair-config-modal #cx-modal .flow-head{align-items:flex-start}
      .cx-modal-shell.pair-config-modal #cx-modal .flow-mode-inline{width:100%}
      .cx-modal-shell.pair-config-modal #cx-modal .flow-mode-inline .seg{width:100%}
      .cx-modal-shell.pair-config-modal #cx-modal .flow-mode-inline .seg label{flex:1 1 0;min-width:0}
      .cx-modal-shell.pair-config-modal #cx-modal .endpoint-top{align-items:flex-start;flex-direction:column}
      .cx-modal-shell.pair-config-modal #cx-modal .endpoint-role{align-self:flex-start}
      .cx-modal-shell.pair-config-modal #cx-modal .providers-intro,
      .cx-modal-shell.pair-config-modal #cx-modal .provider-card-head{grid-template-columns:1fr;display:grid}
      .cx-modal-shell.pair-config-modal #cx-modal .provider-card-meta{justify-content:space-between}
      .cx-modal-shell.pair-config-modal #cx-modal .provider-card-hint{text-align:left;max-width:none}
    }
    @media (max-width:720px){
      .cx-modal-shell.pair-config-modal{width:min(var(--cxModalMaxW,1280px),calc(100vw - 32px))!important;max-width:min(var(--cxModalMaxW,1280px),calc(100vw - 32px))!important;height:min(var(--cxModalMaxH,92vh),calc(100vh - 24px))!important}
      .cx-modal-shell.pair-config-modal #cx-modal .cx-head{padding:14px 16px}
      .cx-modal-shell.pair-config-modal #cx-modal .cx-body{padding:12px 16px 6px}
      .cx-modal-shell.pair-config-modal #cx-modal .cx-st-row{grid-template-columns:1fr}
      .cx-modal-shell.pair-config-modal .cx-actions{padding:10px 16px 14px!important}
    }
  `;
  document.head.appendChild(el);
}
