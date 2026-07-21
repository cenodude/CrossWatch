/* assets/helpers/playing-card.js */
/* Shared Playing Card renderer: one card, per-variant regions */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const STYLE_ID = "cw-playing-card-styles";
  const BASE_CSS = `  .cw-pc{position:fixed;left:50%;bottom:20px;transform:translate(-50%,calc(100% + 30px));width:min(720px,calc(100vw - 420px));background:radial-gradient(120% 140% at 0% 0%,rgba(91,73,197,.18) 0%,rgba(91,73,197,0) 38%),radial-gradient(100% 140% at 100% 100%,rgba(36,118,215,.12) 0%,rgba(36,118,215,0) 42%),linear-gradient(180deg,rgba(7,10,18,.985),rgba(3,5,10,.992));border:1px solid rgba(255,255,255,.06);border-radius:20px;box-shadow:0 24px 64px rgba(0,0,0,.56),inset 0 1px 0 rgba(255,255,255,.03);color:#fff;opacity:0;transition:transform .35s cubic-bezier(.22,.7,.25,1),opacity .25s ease-out,box-shadow .25s ease-out;z-index:10000;overflow:hidden;isolation:isolate}
  .cw-pc.show{transform:translate(-50%,0);opacity:1}
  .cw-pc.show:hover{transform:translate(-50%,-3px);box-shadow:0 28px 72px rgba(0,0,0,.62),inset 0 1px 0 rgba(255,255,255,.035)}
  html:not([data-tab="main"]) .cw-pc[data-tab-scope="main"]{display:none!important}
  html:not([data-tab="watchlist"]) .cw-pc[data-tab-scope="watchlist"]{display:none!important}
  .cw-pc::before{content:"";position:absolute;inset:0;border-radius:inherit;background-image:linear-gradient(90deg,rgba(25,29,38,.92) 0%,rgba(25,29,38,.76) 42%,rgba(25,29,38,.88) 100%),var(--pc-backdrop,none);background-size:100% 100%,cover;background-position:center center,center center;background-repeat:no-repeat,no-repeat;pointer-events:none;z-index:0}
  .cw-pc .pc-body{position:relative;display:flex;flex-direction:column;justify-content:flex-start;padding-bottom:0}
  .cw-pc .pc-inner{position:relative;z-index:1;display:grid;grid-template-columns:104px 1fr 170px;gap:14px;align-items:stretch;padding:14px}
  .cw-pc .pc-poster-link{display:block;width:104px;border-radius:14px;overflow:hidden;background:#05070d;text-decoration:none}
  .cw-pc .pc-poster-link[href]{cursor:pointer}
  .cw-pc .pc-poster{width:104px;border:1px solid rgba(255,255,255,.06);border-radius:14px;object-fit:cover;box-shadow:0 14px 30px rgba(0,0,0,.44),inset 0 1px 0 rgba(255,255,255,.04);background:#05070d}
  .cw-pc .pc-title-row{display:flex;align-items:flex-start;gap:8px}
  .cw-pc .pc-title{font-weight:700;font-size:17px;letter-spacing:.005em;line-height:1.2}
  .cw-pc .pc-title-actions{margin-left:auto;display:flex;align-items:center;justify-content:flex-end;gap:8px}
  .cw-pc .pc-nav{display:inline-flex;align-items:center;gap:6px;padding:3px 6px;border:1px solid rgba(255,255,255,.08);border-radius:999px;background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.018))}
  .cw-pc .pc-nav[hidden]{display:none!important}
  .cw-pc .pc-nav-btn{display:inline-flex;align-items:center;justify-content:center;width:26px;height:26px;border:0;border-radius:999px;background:rgba(255,255,255,.04);color:#eef3ff;cursor:pointer;transition:background .18s ease,transform .18s ease,opacity .18s ease}
  .cw-pc .pc-nav-btn .material-symbols-rounded{font-size:18px;line-height:1}
  .cw-pc .pc-nav-btn:hover{background:rgba(255,255,255,.09);transform:translateY(-1px)}
  .cw-pc .pc-nav-btn:disabled{opacity:.35;cursor:default;transform:none}
  .cw-pc .pc-nav-count{min-width:40px;text-align:center;font-size:11px;font-weight:800;letter-spacing:.08em;color:rgba(236,241,251,.88)}
  .cw-pc .pc-close{display:inline-flex;align-items:center;justify-content:center;flex:0 0 auto;width:32px;height:32px;min-width:32px;margin:0;padding:0;border:1px solid rgba(255,255,255,.08);border-radius:50%;background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.018));color:rgba(232,238,252,.78);cursor:pointer;line-height:1;transition:background .18s ease,border-color .18s ease,color .18s ease,transform .18s ease}
  .cw-pc .pc-close .material-symbols-rounded{font-size:18px;line-height:1;font-variation-settings:"FILL" 0,"wght" 400,"GRAD" 0,"opsz" 20}
  .cw-pc .pc-close:hover{color:#fff;border-color:rgba(255,255,255,.13);background:linear-gradient(180deg,rgba(255,255,255,.075),rgba(255,255,255,.03));transform:translateY(-1px)}
  .cw-pc .pc-meta{display:flex;flex-wrap:wrap;gap:5px;margin-top:6px}
  .cw-pc .pc-chip{display:inline-flex;align-items:center;justify-content:center;min-height:24px;padding:0 9px;border:1px solid rgba(255,255,255,.07);border-radius:999px;background:linear-gradient(180deg,rgba(255,255,255,.055),rgba(255,255,255,.022));box-shadow:inset 0 1px 0 rgba(255,255,255,.03);font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:rgba(236,241,251,.86);line-height:1}
  .pc-chip-streams{background:linear-gradient(180deg,rgba(255,255,255,.08),rgba(255,255,255,.032))}
  .cw-pc .pc-overview{margin-top:8px;font-size:12px;line-height:1.45;color:rgba(211,219,234,.7);max-height:3.2em;overflow:hidden;text-overflow:ellipsis;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}
  .cw-pc .pc-overview-wrap{position:static;min-width:0;min-height:0}
  .cw-pc .pc-overview-more{position:absolute;right:2px;bottom:2px;z-index:2;padding:0;border:0;background:transparent;color:rgba(236,241,251,.72);font:inherit;font-size:10px;font-weight:700;line-height:1;cursor:pointer}
  .cw-pc .pc-overview-more:hover{color:#fff}
  .cw-pc .pc-overview-more[hidden]{display:none}
  .cw-pc .pc-overview-wrap.is-expanded .pc-overview{display:block;overflow-x:hidden;overflow-y:auto;text-overflow:clip;-webkit-line-clamp:unset;scrollbar-width:thin;scrollbar-color:rgba(126,226,184,.55) rgba(255,255,255,.06)}
  .cw-pc .pc-progress-wrap{margin-top:auto;position:relative;width:100%;max-width:100%;box-sizing:border-box}
  .cw-pc .pc-progress-bg{position:relative;width:100%;height:22px;border:1px solid rgba(255,255,255,.06);border-radius:999px;background:linear-gradient(180deg,rgba(12,16,27,.96),rgba(8,11,19,.98));overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.025)}
  .cw-pc .pc-progress{width:0;height:100%;background:linear-gradient(90deg,#5fb6ff,#7ee2b8);transition:width .4s cubic-bezier(.22,.7,.25,1)}
  .cw-pc .pc-progress::after{content:"";position:absolute;inset:0;border-radius:999px;box-shadow:0 0 18px rgba(84,124,255,.22);pointer-events:none}
  .cw-pc .pc-progress-labels{position:absolute;inset:0;display:flex;align-items:center;justify-content:space-between;padding:0 10px;pointer-events:none;font-size:11px;font-weight:700;color:rgba(236,241,251,.92);text-shadow:0 1px 2px rgba(0,0,0,.8);box-sizing:border-box}
  .cw-pc .pc-info-block{display:grid;align-content:center;gap:4px;min-width:0;min-height:62px;padding:8px 10px;border:1px solid rgba(255,255,255,.14);border-radius:10px;background:rgba(20,26,36,.5);box-sizing:border-box}
  .cw-pc .pc-info-label{display:flex;align-items:center;gap:5px;min-width:0;font-size:8px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:rgba(200,209,226,.62);white-space:nowrap}
  .cw-pc .pc-info-value{display:flex;align-items:center;gap:6px;min-width:0;font-size:17px;font-weight:800;line-height:1}
  .cw-pc .pc-info-note{font-size:8px;line-height:1.1;color:rgba(200,209,226,.5);white-space:nowrap}
  .cw-pc .pc-info-icon{font-size:21px;line-height:1;font-variation-settings:"FILL" 1,"wght" 550,"GRAD" 0,"opsz" 22}
  .cw-pc .pc-information-block{align-content:start;gap:6px;min-height:80px}
  .cw-pc .pc-information-block.is-series{min-height:100px}
  .cw-pc .pc-information-rows{display:grid;gap:5px;min-width:0}
  .cw-pc .pc-information-row{display:grid;grid-template-columns:15px minmax(0,1fr);align-items:start;gap:6px;min-width:0;color:rgba(222,229,241,.78)}
  .cw-pc .pc-information-row-icon{font-size:15px;line-height:1.1;color:#22c55e;font-variation-settings:"FILL" 1,"wght" 500,"GRAD" 0,"opsz" 18}
  .cw-pc .pc-information-copy{min-width:0;font-size:9px;line-height:1.2}
  .cw-pc .pc-information-main{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .cw-pc .pc-information-sub{display:block;margin-top:2px;color:rgba(200,209,226,.48);font-size:8px;white-space:nowrap}
  .cw-pc .pc-rating-block{--pc-rating-color:#8b93a7}
  .cw-pc .pc-rating-block .pc-info-value{color:var(--pc-rating-color)}
  .cw-pc .pc-rating-block.rating-low{--pc-rating-color:#ef4444}
  .cw-pc .pc-rating-block.rating-mid{--pc-rating-color:#f59e0b}
  .cw-pc .pc-rating-block.rating-high{--pc-rating-color:#22c55e}
  .cw-pc .pc-status{position:static;font-size:10px;font-weight:700;line-height:1.45;letter-spacing:.08em;text-transform:uppercase;color:rgba(236,241,251,.88);opacity:.96;white-space:pre-line;text-align:left}
  .cw-pc,
  .cw-pc .pc-nav,
  .cw-pc .pc-nav-btn,
  .cw-pc .pc-close,
  .cw-pc .pc-chip,
  .cw-pc .pc-progress-bg,
  .cw-pc .pc-info-block,
  .cw-pc .pc-status{
    background:#20242d!important;
    border-color:rgba(255,255,255,.14)!important;
    box-shadow:none!important;
    text-shadow:none!important;
    filter:none!important;
  }
  .cw-pc .pc-progress::after{
    content:none!important;
    display:none!important;
    background:none!important;
    box-shadow:none!important;
  }
  .cw-pc::before{
    content:""!important;
    display:block!important;
    background-image:linear-gradient(90deg,rgba(32,36,45,.92) 0%,rgba(32,36,45,.76) 42%,rgba(32,36,45,.88) 100%),var(--pc-backdrop,none)!important;
    background-size:100% 100%,cover!important;
    background-position:center center,center center!important;
    background-repeat:no-repeat,no-repeat!important;
    opacity:1!important;
    filter:none!important;
    box-shadow:none!important;
  }
  .cw-pc.show:hover,
  .cw-pc .pc-nav-btn:hover,
  .cw-pc .pc-close:hover{
    background:#2b313d!important;
    border-color:rgba(255,255,255,.19)!important;
    box-shadow:none!important;
    filter:none!important;
    transform:translate(-50%,0)!important;
  }
  .cw-pc .pc-nav-btn:hover,
  .cw-pc .pc-close:hover{
    transform:none!important;
  }
  .cw-pc .pc-poster{
    box-shadow:none!important;
    filter:none!important;
    text-shadow:none!important;
  }
  .cw-pc .pc-progress{
    background:linear-gradient(90deg,#5fb6ff,#7ee2b8)!important;
    box-shadow:none!important;
  }
  .cw-pc .pc-info-block{
    background:linear-gradient(145deg,rgba(21,28,39,.62),rgba(14,20,30,.42))!important;
    border-color:rgba(255,255,255,.16)!important;
    box-shadow:inset 0 1px 0 rgba(255,255,255,.055),0 10px 28px rgba(0,0,0,.12)!important;
    -webkit-backdrop-filter:blur(12px) saturate(115%);
    backdrop-filter:blur(12px) saturate(115%);
  }
  html[data-cw-theme="flat-light"] .cw-pc,
  html[data-cw-theme="flat-light"] .cw-pc .pc-nav,
  html[data-cw-theme="flat-light"] .cw-pc .pc-nav-btn,
  html[data-cw-theme="flat-light"] .cw-pc .pc-close,
  html[data-cw-theme="flat-light"] .cw-pc .pc-chip,
  html[data-cw-theme="flat-light"] .cw-pc .pc-progress-bg,
  html[data-cw-theme="flat-light"] .cw-pc .pc-info-block,
  html[data-cw-theme="flat-light"] .cw-pc .pc-status{
    background:#ffffff!important;
    border-color:rgba(21,31,48,.14)!important;
    color:#172033!important;
  }
  html[data-cw-theme="flat-light"] .cw-pc.show:hover,
  html[data-cw-theme="flat-light"] .cw-pc .pc-nav-btn:hover,
  html[data-cw-theme="flat-light"] .cw-pc .pc-close:hover{
    background:#eef2f7!important;
    border-color:rgba(21,31,48,.20)!important;
  }
  html[data-cw-theme="flat-light"] .cw-pc .pc-progress{
    background:linear-gradient(90deg,#5fb6ff,#7ee2b8)!important;
  }
  html[data-cw-theme="flat-light"] .cw-pc .pc-info-block{
    background:linear-gradient(145deg,rgba(255,255,255,.72),rgba(244,247,251,.58))!important;
    border-color:rgba(21,31,48,.14)!important;
    box-shadow:inset 0 1px 0 rgba(255,255,255,.72),0 10px 26px rgba(31,41,55,.08)!important;
  }
  html[data-cw-theme="flat-light"] .cw-pc .pc-overview-more{color:rgba(23,32,51,.68)}
  html[data-cw-theme="flat-light"] .cw-pc .pc-overview-more:hover{color:#172033}
  html[data-cw-theme="flat-light"] .cw-pc::before{
    background-image:linear-gradient(90deg,rgba(255,255,255,.94) 0%,rgba(255,255,255,.80) 42%,rgba(255,255,255,.92) 100%),var(--pc-backdrop,none)!important;
  }
  /* Split Info Layout: preserve the current styling while reducing card height. */
  .cw-pc{width:min(780px,calc(100vw - 60px))}
  .cw-pc .pc-inner{grid-template-columns:126px minmax(235px,1fr) minmax(320px,350px);gap:14px;align-items:stretch;padding:0 14px 0 0;min-height:190px;box-sizing:border-box}
  .cw-pc .pc-poster-link{width:126px;height:190px;align-self:stretch;border-right:1px solid rgba(255,255,255,.06);border-radius:19px 0 0 19px;box-sizing:border-box}
  .cw-pc .pc-poster{display:block;width:100%;height:100%;border:0;border-radius:inherit;box-shadow:none;box-sizing:border-box}
  .cw-pc .pc-body{min-width:0;padding:14px 0;justify-content:flex-start}
  .cw-pc .pc-title{font-size:18px;line-height:1.15}
  .cw-pc .pc-overview{margin-top:10px;max-height:4.35em;line-height:1.45;-webkit-line-clamp:3}
  .cw-pc .pc-close{position:static;width:24px;height:24px;min-width:24px}
  .cw-pc .pc-stats{min-width:0;display:grid;grid-template-rows:auto auto;align-content:center;gap:9px;padding:14px 6px 14px 14px;border-left:1px solid rgba(255,255,255,.08)}
  .cw-pc .pc-progress-wrap{margin:0;align-self:start}
  .cw-pc .pc-progress-labels{position:static;display:flex;align-items:center;justify-content:space-between;min-height:24px;margin-bottom:5px;padding:0;pointer-events:auto;font-size:11px;text-shadow:none}
  .cw-pc .pc-progress-end{display:flex;align-items:center;justify-content:flex-end;gap:8px;min-width:0}
  .cw-pc .pc-progress-bg{height:8px}
  .cw-pc .pc-stats-bottom{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));align-items:start;gap:8px;min-width:0}
  .cw-pc .pc-info-block{min-height:62px}
  .cw-pc .pc-rating-stack{display:flex;flex-direction:column;gap:7px;min-width:0}
  .cw-pc .pc-status-head{display:flex;align-items:center;justify-content:center;gap:7px;min-width:0;padding:2px 2px 0}
  .cw-pc .pc-status-icon{flex:0 0 auto;margin-top:-1px;color:#22c55e;font-size:21px;line-height:1;font-variation-settings:"FILL" 1,"wght" 650,"GRAD" 0,"opsz" 22;filter:drop-shadow(0 0 6px rgba(34,197,94,.28))}
  .cw-pc .pc-status{min-width:0;border:0!important;background:transparent!important;text-align:center}
  @media (max-width:1024px){.cw-pc{width:min(780px,calc(100vw - 40px))}.cw-pc .pc-inner{grid-template-columns:112px minmax(220px,1fr) minmax(230px,250px)}.cw-pc .pc-poster-link{width:112px;height:190px}}
  @media (max-width:820px){.cw-pc .pc-inner{grid-template-columns:82px minmax(0,1fr) minmax(210px,240px);min-height:0;padding:12px}.cw-pc .pc-poster-link{width:82px;height:123px;grid-row:1 / span 2;align-self:center;border:1px solid rgba(255,255,255,.06);border-radius:11px}.cw-pc .pc-body{padding:4px 0}}
  @media (max-width:680px){.cw-pc{bottom:max(10px,env(safe-area-inset-bottom));width:calc(100vw - 20px);border-radius:18px}.cw-pc .pc-inner{grid-template-columns:64px minmax(0,1fr);gap:12px;padding:12px}.cw-pc .pc-poster-link{width:64px;height:96px;grid-row:auto;border-radius:10px}.cw-pc .pc-body{padding:2px 0}.cw-pc .pc-title{font-size:15px;line-height:1.15}.cw-pc .pc-title-actions{gap:6px}.cw-pc .pc-nav{padding:2px 4px}.cw-pc .pc-nav-count{min-width:32px;font-size:10px}.cw-pc .pc-nav-btn{width:24px;height:24px}.cw-pc .pc-meta{gap:4px;margin-top:4px}.cw-pc .pc-chip{min-height:21px;font-size:9px;padding:0 7px}.cw-pc .pc-overview{margin-top:6px;max-height:4.35em;font-size:11px;-webkit-line-clamp:3}.cw-pc .pc-overview-more{font-size:9px}.cw-pc .pc-stats{grid-column:1 / -1;grid-template-rows:auto auto;padding:10px 0 0;border-left:0;border-top:1px solid rgba(255,255,255,.08)}.cw-pc .pc-info-block{min-height:58px}}
  @media (max-width:460px){.cw-pc .pc-stats-bottom{grid-template-columns:1fr}.cw-pc .pc-overview{-webkit-line-clamp:1;max-height:1.5em}}
  @media (hover:none){.cw-pc.show:hover{transform:translate(-50%,0);box-shadow:0 20px 48px rgba(0,0,0,.6)}}
  /* the card must never grow taller than the 190px poster. */
  @media (min-width:821px){
    .cw-pc .pc-inner{min-height:190px;max-height:190px;height:190px;overflow:hidden}
    .cw-pc .pc-body{min-height:0;max-height:190px;overflow:hidden}
    .cw-pc .pc-stats{min-height:0;max-height:190px;overflow:hidden}
    .cw-pc .pc-title{display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;text-overflow:ellipsis;min-width:0}
    .cw-pc .pc-meta{overflow:hidden}
    .cw-pc .pc-overview-more{display:none!important}
  }`;
  const VARIANT_CSS = `
  .cw-pc{width:var(--pc-width,min(780px,calc(100vw - 60px)))}
  .cw-pc .pc-trailer,.cw-pc .pc-sources,.cw-pc .pc-actions{display:none}
  .cw-pc-watchlist .pc-status-head,.cw-pc-watchlist .pc-nav{display:none!important}
  .cw-pc-watchlist .pc-progress-bg,.cw-pc-watchlist .pc-progress-pct,.cw-pc-watchlist .pc-progress-time{display:none!important}
  .cw-pc-watchlist .pc-progress-labels{justify-content:flex-end;min-height:0;margin-bottom:0}
  .cw-pc-watchlist .pc-trailer{display:inline-flex}
  .cw-pc-watchlist .pc-sources{display:flex}
  .cw-pc-watchlist .pc-actions{display:flex}
  .cw-pc-watchlist .pc-stats{display:flex;flex-direction:column;justify-content:flex-start;gap:9px;padding:12px 12px 12px 14px}
  .cw-pc-watchlist .pc-stats-bottom{flex:0 0 auto}
  .cw-pc-watchlist .pc-rating-stack{gap:7px}
  .cw-pc .pc-actions{align-items:center;flex-wrap:wrap;gap:6px;min-width:0}
  .cw-pc .pc-links{display:contents}
  .cw-pc .pc-trailer{align-items:center;justify-content:center;min-height:26px;padding:0 10px;border:1px solid rgba(255,255,255,.10);border-radius:999px;background:rgba(255,255,255,.04);color:#eef3ff;font:inherit;font-size:10px;font-weight:800;letter-spacing:.04em;text-transform:uppercase;line-height:1;cursor:pointer;transition:background .16s ease,border-color .16s ease}
  .cw-pc .pc-trailer:hover{border-color:rgba(255,255,255,.16);background:rgba(255,255,255,.08)}
  .cw-pc .pc-sources{align-items:center;flex-wrap:wrap;gap:6px;min-width:0;padding-top:2px}
  .cw-pc .pc-source{display:inline-flex;align-items:center;justify-content:center;min-width:30px;min-height:26px;padding:0 7px;border:1px solid rgba(255,255,255,.09);border-radius:999px;background:rgba(255,255,255,.04)}
  .cw-pc .pc-source img{display:block;height:14px;max-width:20px;object-fit:contain}
  .cw-pc .pc-source-text{font-size:10px;font-weight:800;letter-spacing:.04em;color:rgba(245,248,255,.88)}
  .cw-pc .pc-link{display:inline-flex;align-items:center;justify-content:center;min-height:26px;padding:0 10px;border:1px solid rgba(255,255,255,.10);border-radius:999px;background:rgba(255,255,255,.04);color:#eef3ff;text-decoration:none;font-size:10px;font-weight:800;letter-spacing:.04em;text-transform:uppercase}
  .cw-pc .pc-link:hover{border-color:rgba(255,255,255,.16);background:rgba(255,255,255,.08)}
  html[data-cw-theme="flat-light"] .cw-pc .pc-trailer,
  html[data-cw-theme="flat-light"] .cw-pc .pc-source,
  html[data-cw-theme="flat-light"] .cw-pc .pc-link{background:#ffffff!important;border-color:rgba(21,31,48,.14)!important;color:#172033!important}
  html[data-cw-theme="flat-light"] .cw-pc .pc-source-text{color:#172033!important}
  html[data-cw-theme="flat-light"] .cw-pc .pc-trailer:hover,
  html[data-cw-theme="flat-light"] .cw-pc .pc-link:hover{background:#eef2f7!important}
  @media (min-width:821px){
    .cw-pc-watchlist .pc-inner{max-height:none;height:auto;min-height:190px}
    .cw-pc-watchlist .pc-body,.cw-pc-watchlist .pc-stats{max-height:none}
    .cw-pc-watchlist .pc-poster-link{height:auto;align-self:stretch}
  }
`;
  const TEMPLATE = `
    <div class="pc-inner">
      <a class="pc-poster-link" target="_blank" rel="noopener noreferrer">
        <img class="pc-poster" src="/assets/img/placeholder_poster.svg" alt="">
      </a>
      <div class="pc-body">
        <div class="pc-title-row">
          <div class="pc-title">Now Playing</div>
          <div class="pc-title-actions">
            <div class="pc-nav" hidden>
              <button class="pc-nav-btn pc-prev" type="button" aria-label="Previous stream"><span class="material-symbols-rounded">chevron_left</span></button>
              <span class="pc-nav-count">1 / 1</span>
              <button class="pc-nav-btn pc-next" type="button" aria-label="Next stream"><span class="material-symbols-rounded">chevron_right</span></button>
            </div>
          </div>
        </div>
        <div class="pc-meta"></div>
        <div class="pc-overview-wrap">
          <div class="pc-overview"></div>
          <button class="pc-overview-more" type="button" aria-expanded="false" hidden>More</button>
        </div>
      </div>
      <div class="pc-stats">
        <div class="pc-progress-wrap">
          <div class="pc-progress-labels">
            <span class="pc-progress-pct"></span>
            <span class="pc-progress-end">
              <span class="pc-progress-time"></span>
              <button class="pc-close" type="button" title="Hide" aria-label="Hide card"><span class="material-symbols-rounded" aria-hidden="true">close</span></button>
            </span>
          </div>
          <div class="pc-progress-bg"><div class="pc-progress"></div></div>
        </div>
        <div class="pc-stats-bottom">
          <div class="pc-info-block pc-information-block">
            <div class="pc-info-label">Information</div>
            <div class="pc-information-rows"></div>
          </div>
          <div class="pc-rating-stack">
            <div class="pc-info-block pc-rating-block">
              <div class="pc-info-label">TMDB Rating</div>
              <div class="pc-info-value"><span class="material-symbols-rounded pc-info-icon" aria-hidden="true">star</span><span class="pc-rating">--</span></div>
              <div class="pc-rating-votes pc-info-note">Rating unavailable</div>
            </div>
            <div class="pc-status-head"><span class="material-symbols-rounded pc-status-icon" aria-hidden="true">play_arrow</span><div class="pc-status">Now Playing</div></div>
            <div class="pc-sources"></div>
          </div>
        </div>
        <div class="pc-actions">
          <div class="pc-links"></div>
          <button class="pc-trailer" type="button">Watch trailer</button>
        </div>
      </div>
    </div>
`;

  function ensureStyles() {
    if (document.getElementById(STYLE_ID)) return;
    const style = document.createElement("style");
    style.id = STYLE_ID;
    style.textContent = BASE_CSS + VARIANT_CSS;
    document.head.appendChild(style);
  }

  const runtimeLabel = (mins) => {
    const m = Number(mins) || 0;
    if (!m) return "";
    const h = Math.floor(m / 60);
    const mm = m % 60;
    return h ? `${h}h ${mm ? `${mm}m` : ""}` : `${mm}m`;
  };

  const formatTime = (ms) => {
    const totalMs = Number(ms) || 0;
    if (!totalMs) return "";
    const totalSec = Math.floor(totalMs / 1000);
    const h = Math.floor(totalSec / 3600);
    const m = Math.floor((totalSec % 3600) / 60);
    const s = totalSec % 60;
    return h > 0 ? `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}` : `${m}:${String(s).padStart(2, "0")}`;
  };

  const formatDateLabel = (raw) => {
    const value = String(raw || "").trim().split("T")[0];
    const match = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!match) return value;
    const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
    return date.toLocaleDateString(undefined, { day: "numeric", month: "short", year: "numeric", timeZone: "UTC" });
  };

  const genreLabel = (meta, det) => {
    const raw = meta?.genres || det?.genres || [];
    return (Array.isArray(raw) ? raw : [])
      .map((genre) => (typeof genre === "string" ? genre : genre?.name))
      .map((genre) => String(genre || "").trim())
      .filter(Boolean)
      .join(", ") || "Genres unavailable";
  };

  const nextEpisodeLabels = (nextEpisode) => {
    if (!nextEpisode || typeof nextEpisode !== "object") return ["No upcoming episode", ""];
    const season = Number(nextEpisode.season_number);
    const episode = Number(nextEpisode.episode_number);
    const code = Number.isInteger(season) && Number.isInteger(episode)
      ? `S${String(season).padStart(2, "0")}E${String(episode).padStart(2, "0")}`
      : "Next episode";
    const airDate = String(nextEpisode.air_date || "").trim();
    let timing = "";
    const match = airDate.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (match) {
      const target = Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
      const now = new Date();
      const today = Date.UTC(now.getFullYear(), now.getMonth(), now.getDate());
      const days = Math.round((target - today) / 86400000);
      timing = days === 0 ? "Airs today" : days === 1 ? "Airs tomorrow" : days > 1 ? `Airs in ${days} days` : "Previously aired";
    }
    return [[code, timing].filter(Boolean).join(" · "), formatDateLabel(airDate)];
  };

  function informationFor(meta, isMovie) {
    const det = meta?.detail || {};
    const rows = [{ icon: "sell", main: genreLabel(meta, det) }];
    if (isMovie) {
      const releaseRaw = meta?.release?.date || det.release_date || "";
      const runtime = meta?.runtime_minutes ?? det.runtime_minutes ?? meta?.runtime ?? det.runtime;
      rows.push({ icon: "calendar_month", main: formatDateLabel(releaseRaw) || "Release date unavailable" });
      rows.push({ icon: "schedule", main: runtimeLabel(runtime) || "Runtime unavailable" });
      return rows;
    }
    rows.push({ icon: "tv", main: String(det.status || "Status unavailable") });
    const seasons = Number(det.number_of_seasons);
    const episodes = Number(det.number_of_episodes);
    const totals = [
      Number.isFinite(seasons) && seasons > 0 ? `${seasons} Season${seasons === 1 ? "" : "s"}` : "",
      Number.isFinite(episodes) && episodes > 0 ? `${episodes} Episode${episodes === 1 ? "" : "s"}` : "",
    ].filter(Boolean).join(" · ") || "Series totals unavailable";
    rows.push({ icon: "layers", main: totals });
    const [nextMain, nextSub] = nextEpisodeLabels(det.next_episode_to_air);
    rows.push({ icon: "arrow_forward", main: nextMain, sub: nextSub });
    return rows;
  }

  function informationRow(icon, main, sub = "") {
    const row = document.createElement("div");
    row.className = "pc-information-row";
    const iconEl = document.createElement("span");
    iconEl.className = "material-symbols-rounded pc-information-row-icon";
    iconEl.setAttribute("aria-hidden", "true");
    iconEl.textContent = icon;
    const copy = document.createElement("span");
    copy.className = "pc-information-copy";
    const mainEl = document.createElement("span");
    mainEl.className = "pc-information-main";
    mainEl.textContent = main;
    mainEl.title = main;
    copy.appendChild(mainEl);
    if (sub) {
      const subEl = document.createElement("span");
      subEl.className = "pc-information-sub";
      subEl.textContent = sub;
      copy.appendChild(subEl);
    }
    row.append(iconEl, copy);
    return row;
  }

  function mount(options = {}) {
    ensureStyles();
    const variant = options.variant === "watchlist" ? "watchlist" : "scrobble";
    const el = document.createElement("div");
    if (options.id) el.id = options.id;
    el.className = `cw-pc cw-pc-${variant}`;
    el.setAttribute("aria-live", "polite");
    if (options.label) el.setAttribute("aria-label", options.label);
    if (options.tabScope) el.dataset.tabScope = options.tabScope;
    if (options.width) el.style.setProperty("--pc-width", options.width);
    el.innerHTML = TEMPLATE;
    document.body.appendChild(el);

    const q = (sel) => el.querySelector(sel);
    const els = {
      poster: q(".pc-poster"),
      posterLink: q(".pc-poster-link"),
      title: q(".pc-title"),
      meta: q(".pc-meta"),
      overview: q(".pc-overview"),
      overviewWrap: q(".pc-overview-wrap"),
      overviewMore: q(".pc-overview-more"),
      progress: q(".pc-progress"),
      progressPct: q(".pc-progress-pct"),
      progressTime: q(".pc-progress-time"),
      informationBlock: q(".pc-information-block"),
      informationRows: q(".pc-information-rows"),
      ratingBlock: q(".pc-rating-block"),
      rating: q(".pc-rating"),
      ratingVotes: q(".pc-rating-votes"),
      status: q(".pc-status"),
      statusIcon: q(".pc-status-icon"),
      nav: q(".pc-nav"),
      navCount: q(".pc-nav-count"),
      prev: q(".pc-prev"),
      next: q(".pc-next"),
      trailer: q(".pc-trailer"),
      sources: q(".pc-sources"),
      links: q(".pc-links"),
    };

    els.poster.onerror = () => {
      els.poster.onerror = null;
      els.poster.src = "/assets/img/placeholder_poster.svg";
    };

    let overviewFrame = 0;
    const updateOverviewMore = () => {
      if (overviewFrame) cancelAnimationFrame(overviewFrame);
      overviewFrame = requestAnimationFrame(() => {
        overviewFrame = 0;
        if (els.overviewWrap.classList.contains("is-expanded")) return;
        const hasOverflow = !!els.overview.textContent.trim() && els.overview.scrollHeight > els.overview.clientHeight + 1;
        els.overviewWrap.classList.toggle("has-overflow", hasOverflow);
        els.overviewMore.hidden = !hasOverflow;
      });
    };

    els.overviewMore.addEventListener("click", () => {
      const expanded = els.overviewWrap.classList.toggle("is-expanded");
      els.overview.scrollTop = 0;
      els.overviewMore.textContent = expanded ? "Less" : "More";
      els.overviewMore.setAttribute("aria-expanded", expanded ? "true" : "false");
      if (!expanded) updateOverviewMore();
    });

    el.querySelectorAll(".pc-close").forEach((btn) => {
      btn.addEventListener("click", () => options.onClose?.(), true);
    });
    els.prev?.addEventListener("click", () => options.onPrev?.(), true);
    els.next?.addEventListener("click", () => options.onNext?.(), true);
    els.trailer?.addEventListener("click", () => options.onTrailer?.(), true);

    const setOverview = (value) => {
      els.overviewWrap.classList.remove("is-expanded");
      els.overview.scrollTop = 0;
      els.overview.textContent = String(value || "");
      els.overviewMore.textContent = "More";
      els.overviewMore.setAttribute("aria-expanded", "false");
      els.overviewMore.hidden = true;
      updateOverviewMore();
    };

    const setChips = (chips) => {
      els.meta.replaceChildren();
      for (const chip of chips || []) {
        const text = String(chip?.text ?? chip ?? "").trim();
        if (!text) continue;
        const span = document.createElement("span");
        span.className = "pc-chip";
        if (chip?.cls) span.classList.add(chip.cls);
        span.textContent = text;
        els.meta.appendChild(span);
      }
    };

    const setRating = (rawRating, rawVotes) => {
      const rating = Number(rawRating);
      const available = Number.isFinite(rating) && rating >= 1 && rating <= 10;
      els.ratingBlock.classList.toggle("rating-low", available && rating < 5);
      els.ratingBlock.classList.toggle("rating-mid", available && rating >= 5 && rating < 7);
      els.ratingBlock.classList.toggle("rating-high", available && rating >= 7);
      els.rating.textContent = available ? rating.toFixed(1) : "--";
      const votes = Number(rawVotes);
      els.ratingVotes.textContent = !available
        ? "Rating unavailable"
        : Number.isFinite(votes) && votes > 0
          ? `${votes.toLocaleString(undefined, { notation: "compact", maximumFractionDigits: 1 })} votes`
          : "0 votes";
    };

    const setPosterLink = (href, title = "") => {
      if (!href) {
        els.posterLink.removeAttribute("href");
        els.posterLink.removeAttribute("aria-label");
        els.posterLink.removeAttribute("title");
        els.posterLink.setAttribute("aria-disabled", "true");
        return;
      }
      els.posterLink.href = href;
      els.posterLink.setAttribute("aria-label", `Open ${title || "title"} on TMDb`);
      els.posterLink.title = `Open ${title || "title"} on TMDb`;
      els.posterLink.removeAttribute("aria-disabled");
    };

    const setInformation = (rows, isMovie) => {
      els.informationBlock.classList.toggle("is-series", isMovie === false);
      if (rows === "loading") {
        els.informationRows.replaceChildren(informationRow("hourglass_empty", "Loading information..."));
        return;
      }
      els.informationRows.replaceChildren(
        ...(Array.isArray(rows) ? rows : []).map((r) => informationRow(r.icon, r.main, r.sub || ""))
      );
    };

    const setSources = (sources) => {
      els.sources.replaceChildren();
      for (const source of sources || []) {
        const span = document.createElement("span");
        span.className = "pc-source";
        if (source?.label) span.title = source.label;
        if (source?.logo) {
          const img = document.createElement("img");
          img.src = source.logo;
          img.alt = `${source.label || ""} logo`;
          span.appendChild(img);
        } else {
          const text = document.createElement("span");
          text.className = "pc-source-text";
          text.textContent = String(source?.short || source?.label || "").slice(0, 4);
          span.appendChild(text);
        }
        els.sources.appendChild(span);
      }
    };

    const setLinks = (links) => {
      els.links.replaceChildren();
      for (const link of links || []) {
        if (!link?.href) continue;
        const a = document.createElement("a");
        a.className = "pc-link";
        a.href = link.href;
        a.target = "_blank";
        a.rel = "noopener noreferrer";
        a.textContent = link.text || "Open";
        els.links.appendChild(a);
      }
    };

    const setProgress = (progress) => {
      const pct = Math.max(0, Math.min(100, Number(progress?.pct) || 0));
      els.progress.style.width = `${pct}%`;
      els.progressPct.textContent = progress ? `${Math.round(pct)}% watched` : "";
      els.progressTime.textContent = progress?.remaining || "";
    };

    const setNav = (nav) => {
      const total = Number(nav?.total) || 0;
      els.nav.hidden = total <= 1;
      els.navCount.textContent = total > 0 ? `${(Number(nav?.index) || 0) + 1} / ${total}` : "0 / 0";
      els.prev.disabled = total <= 1;
      els.next.disabled = total <= 1;
    };

    function render(model) {
      if (!model) return;
      els.title.textContent = model.year ? `${model.title} ${model.year}` : (model.title || "");
      setChips(model.chips);
      setOverview(model.overview || "");
      els.poster.src = model.poster || "/assets/img/placeholder_poster.svg";
      els.poster.alt = model.title || "Poster";
      setPosterLink(model.posterHref || "", model.title);
      el.style.setProperty("--pc-backdrop", model.backdrop ? `url("${model.backdrop}")` : "none");
      setInformation(model.information, model.isMovie);
      setRating(model.rating?.value, model.rating?.votes);
      setProgress(model.progress);
      setNav(model.nav);
      setSources(model.sources);
      setLinks(model.links);
      if (model.status) {
        els.status.textContent = model.status.text || "";
        els.statusIcon.textContent = model.status.icon || "play_arrow";
        els.status.title = model.status.title || "";
      }
      if (els.trailer) els.trailer.textContent = model.trailerLabel || "Watch trailer";
    }

    return {
      el,
      render,
      renderProgress: setProgress,
      show: () => el.classList.add("show"),
      hide: () => el.classList.remove("show"),
      isVisible: () => el.classList.contains("show"),
      destroy: () => el.remove(),
    };
  }

  (window.CW ||= {}).PlayingCard = {
    mount,
    fmt: { runtimeLabel, formatTime, formatDateLabel, genreLabel, nextEpisodeLabels, informationFor },
  };
})();
