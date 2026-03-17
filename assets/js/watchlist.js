/* assets/js/watchlist.js */
/* refactored */
/* Watchlist page shell and components */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(function () {
  const authSetupPending = () => window.cwIsAuthSetupPending?.() === true;

/* Styles - refactored: includes own CSS*/
const css = `#page-watchlist{--wl-shell-bg:linear-gradient(180deg,rgba(8,10,15,.985),rgba(2,3,7,.975));--wl-panel-bg:linear-gradient(180deg,rgba(12,14,20,.95),rgba(4,5,10,.945));--wl-panel-bg-strong:linear-gradient(180deg,rgba(9,11,17,.985),rgba(2,3,7,.975));--wl-border:rgba(255,255,255,.09);--wl-border-soft:rgba(255,255,255,.055);--wl-shadow:0 20px 54px rgba(0,0,0,.42),inset 0 1px 0 rgba(255,255,255,.04);--wl-accent:rgba(94,90,184,.48);--wl-accent-soft:rgba(84,90,170,.10);--wl-fg:rgba(244,247,255,.96);--wl-fg-soft:rgba(201,210,228,.72);--wl-card-radius:18px}.wl-topline{display:flex;align-items:flex-start;justify-content:space-between;gap:14px;margin-bottom:12px;padding:14px 16px;border-radius:20px;border:1px solid var(--wl-border);background:radial-gradient(115% 120% at 0% 0%,rgba(78,68,170,.10),transparent 46%),radial-gradient(88% 100% at 100% 100%,rgba(34,46,108,.06),transparent 54%),var(--wl-shell-bg);box-shadow:var(--wl-shadow);backdrop-filter:blur(16px) saturate(130%);-webkit-backdrop-filter:blur(16px) saturate(130%)}.wl-title-stack{flex:1;min-width:0;display:grid;gap:6px;width:100%}.wl-title-row{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:12px;width:100%}.wl-title{font-weight:800;font-size:24px;letter-spacing:-.02em;line-height:1.05;color:var(--wl-fg)}.wl-sub{color:var(--wl-fg-soft);font-size:13px;line-height:1.4;max-width:72ch}.wl-head-pills{display:flex;align-items:center;justify-content:flex-end;justify-self:end;margin-left:auto;gap:8px;flex-wrap:wrap}.wl-wrap{display:grid;grid-template-columns:minmax(0,1fr) 336px;gap:14px;align-items:start}.wl-main-shell,.wl-side .ins-card,.wl-table-wrap,.wl-empty,.wl-detail,.wl-modal .box,.wl-snack{border:1px solid var(--wl-border);background:var(--wl-panel-bg);box-shadow:var(--wl-shadow);backdrop-filter:blur(14px) saturate(130%);-webkit-backdrop-filter:blur(14px) saturate(130%)}.wl-main-shell{border-radius:20px;padding:12px;min-width:0;overflow:hidden}.wl-side{display:flex;flex-direction:column;gap:10px;position:sticky;top:12px}.wl-toolbar{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-bottom:12px}.wl-toolbar-left,.wl-toolbar-right{display:flex;align-items:center;gap:8px;flex-wrap:wrap;min-width:0}.wl-toolbar-right{justify-content:flex-end}.wl-input,#page-watchlist select.wl-input,#page-watchlist input.wl-input{width:100%;min-height:38px;padding:8px 12px;font:inherit;color:var(--wl-fg);background:rgba(7,11,19,.78);border:1px solid rgba(255,255,255,.08);border-radius:12px;box-shadow:inset 0 1px 0 rgba(255,255,255,.02);outline:none;transition:border-color .16s ease,background .16s ease,box-shadow .16s ease,transform .16s ease}.wl-input:hover{border-color:rgba(255,255,255,.12);background:rgba(10,14,24,.86)}.wl-input:focus{border-color:rgba(94,90,184,.30);box-shadow:0 0 0 3px rgba(74,84,156,.10),inset 0 1px 0 rgba(255,255,255,.03);background:rgba(7,9,16,.94)}#page-watchlist select.wl-input option{background:#0d111a;color:#f7f9ff}#page-watchlist input[type="range"].wl-input{min-height:32px;padding:0;background:transparent;border:none;box-shadow:none}.wl-btn,.wl-chip,.wl-colchip{position:relative;display:inline-flex;align-items:center;justify-content:center;gap:7px;min-height:34px;padding:0 12px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.03));color:var(--wl-fg);line-height:1;white-space:nowrap;transition:transform .16s ease,background .16s ease,border-color .16s ease,box-shadow .16s ease,opacity .16s ease}.wl-btn{cursor:pointer;font-weight:700}.wl-btn:hover,.wl-chip:hover,.wl-colchip:hover{transform:translateY(-1px);background:linear-gradient(180deg,rgba(255,255,255,.09),rgba(255,255,255,.05));border-color:rgba(255,255,255,.14)}.wl-btn:active{transform:translateY(0)}.wl-btn[disabled],.wl-chip[disabled]{opacity:.48;cursor:not-allowed;transform:none}.wl-btn.danger{background:linear-gradient(180deg,rgba(118,28,46,.30),rgba(82,14,28,.24));border-color:rgba(255,138,160,.15);color:#ffe7ee}.wl-btn.danger:hover{background:linear-gradient(180deg,rgba(134,32,53,.34),rgba(94,18,34,.28))}.wl-refresh-btn{margin-left:auto;display:inline-flex;align-items:center;justify-content:center;width:34px;height:34px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:linear-gradient(180deg,rgba(255,255,255,.08),rgba(255,255,255,.04));color:#f7f9ff;cursor:pointer;transition:transform .16s ease,background .16s ease,border-color .16s ease,opacity .16s ease,box-shadow .16s ease;box-shadow:inset 0 1px 0 rgba(255,255,255,.04)}.wl-refresh-btn:hover{transform:translateY(-1px);background:linear-gradient(180deg,rgba(255,255,255,.11),rgba(255,255,255,.05));border-color:rgba(255,255,255,.14)}.wl-refresh-btn.loading{opacity:.68;pointer-events:none}.wl-refresh-btn .material-symbol{font-size:18px;line-height:1;color:#fff;-webkit-text-fill-color:#fff;font-variation-settings:'FILL' 1,'wght' 500,'GRAD' 0,'opsz' 24;display:inline-block;will-change:transform}.wl-refresh-btn.spin .material-symbol,.wl-refresh-btn.loading .material-symbol,.wl-refresh-btn[disabled] .material-symbol{animation:wlrot .6s linear infinite!important}@keyframes wlrot{to{transform:rotate(360deg)}}.wl-chip{font-size:12px;font-weight:700;color:var(--wl-fg-soft)}.wl-chip input,.wl-colchip input{accent-color:#8da3ff}.wl-chip strong{color:var(--wl-fg)}.wl-chip.is-accent{color:#f7f8ff;border-color:rgba(104,108,188,.24);background:linear-gradient(180deg,rgba(74,78,146,.16),rgba(36,40,78,.10));box-shadow:inset 0 1px 0 rgba(255,255,255,.05),0 8px 20px rgba(20,24,48,.18)}.wl-chip.is-muted{color:rgba(201,210,228,.64)}.wl-chip.is-filter{max-width:100%;overflow:hidden;text-overflow:ellipsis}.wl-muted{color:var(--wl-fg-soft)}.field-label{color:var(--wl-fg-soft);font-size:12px;font-weight:700;letter-spacing:.03em}.wl-grid{--wl-min:160px;--wl-overlay-pad:8px;--wl-overlay-gap:6px;--wl-provider-badge:26px;--wl-provider-icon-h:14px;--wl-type-pill-h:24px;--wl-type-pill-min:24px;display:grid;gap:12px;grid-template-columns:repeat(auto-fill,minmax(var(--wl-min),1fr))}.wl-card{position:relative;display:flex;flex-direction:column;justify-content:flex-end;min-height:0;aspect-ratio:2/3;isolation:isolate;overflow:hidden;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(10,12,18,.78),rgba(3,4,8,.95));box-shadow:0 14px 32px rgba(0,0,0,.30);transition:transform .18s ease,border-color .18s ease,box-shadow .18s ease}.wl-card::before{content:"";position:absolute;inset:0;background:linear-gradient(180deg,rgba(6,10,18,.02),rgba(6,10,18,.16) 42%,rgba(4,8,14,.84) 100%);z-index:1;pointer-events:none}.wl-card:hover{transform:translateY(-2px);border-color:rgba(255,255,255,.14);box-shadow:0 18px 34px rgba(0,0,0,.34),0 0 0 1px rgba(255,255,255,.03) inset}.wl-card img{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;display:block}.wl-card .wl-card-top{position:absolute;left:var(--wl-overlay-pad);right:var(--wl-overlay-pad);top:var(--wl-overlay-pad);display:flex;align-items:flex-start;justify-content:space-between;gap:var(--wl-overlay-gap);z-index:2;pointer-events:none}.wl-card .wl-provider-icons{display:flex;align-items:flex-start;gap:var(--wl-overlay-gap);flex-wrap:wrap;max-width:calc(100% - (var(--wl-type-pill-min) + var(--wl-overlay-gap)))}.wl-card .wl-provider-icon{display:inline-flex;align-items:center;justify-content:center;width:var(--wl-provider-badge);height:var(--wl-provider-badge);padding:0 6px;border-radius:999px;border:1px solid rgba(255,255,255,.09);background:rgba(7,11,18,.38);box-shadow:inset 0 1px 0 rgba(255,255,255,.04),0 8px 20px rgba(0,0,0,.18);backdrop-filter:blur(10px) saturate(120%);-webkit-backdrop-filter:blur(10px) saturate(120%)}.wl-card .wl-provider-icon img{position:static;inset:auto;width:auto;height:var(--wl-provider-icon-h);max-width:calc(var(--wl-provider-badge) - 10px);object-fit:contain;filter:brightness(1.02)}.wl-card .wl-provider-icon .wl-badge{font-size:10px;font-weight:800;line-height:1;color:rgba(245,248,255,.88)}.wl-card .wl-type-corner{display:inline-flex;align-items:center;justify-content:center;min-width:var(--wl-type-pill-min);height:var(--wl-type-pill-h);padding:0 8px;border-radius:999px;border:1px solid rgba(255,255,255,.09);background:rgba(7,11,18,.38);box-shadow:inset 0 1px 0 rgba(255,255,255,.04),0 8px 20px rgba(0,0,0,.18);backdrop-filter:blur(10px) saturate(120%);-webkit-backdrop-filter:blur(10px) saturate(120%);color:rgba(245,248,255,.90);font-size:10px;font-weight:800;letter-spacing:.08em;line-height:1;text-transform:uppercase;text-align:center}.wl-card .wl-card-meta{position:relative;z-index:2;display:grid;gap:6px;padding:10px 10px 11px}.wl-card .wl-card-title{color:#f7f8ff;font-weight:700;font-size:13px;line-height:1.25;text-shadow:0 2px 12px rgba(0,0,0,.55);display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}.wl-card .wl-card-sub{display:flex;align-items:center;justify-content:space-between;gap:8px;color:rgba(231,236,247,.76);font-size:11px;font-weight:600}.wl-card .wl-card-sub span:last-child{text-align:right}.wl-card.selected{border-color:rgba(98,92,182,.30);box-shadow:0 0 0 1px rgba(98,92,182,.20),0 18px 34px rgba(0,0,0,.34),0 0 0 6px rgba(64,70,128,.08)}.wl-table-wrap{border-radius:18px;overflow:auto;background:var(--wl-panel-bg-strong)}.wl-table{width:100%;border-collapse:separate;border-spacing:0;table-layout:fixed;color:var(--wl-fg)}.wl-table col.c-sel{width:46px}.wl-table col.c-poster{width:70px}.wl-table th,.wl-table td{padding:10px 8px;border-bottom:1px solid rgba(255,255,255,.06);text-align:left;overflow:hidden;vertical-align:middle}.wl-table th{position:sticky;top:0;z-index:2;font-size:12px;font-weight:700;letter-spacing:.04em;text-transform:uppercase;color:rgba(222,230,244,.70);background:linear-gradient(180deg,rgba(12,14,20,.985),rgba(5,7,12,.97));backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px)}.wl-table tbody tr{transition:background .14s ease}.wl-table tbody tr:hover{background:rgba(255,255,255,.03)}.wl-table tr:last-child td{border-bottom:none}.wl-table .wl-title{white-space:normal;text-transform:none;letter-spacing:normal;font-weight:inherit}.wl-table td.rel,.wl-table td.genre{white-space:normal;overflow:hidden;text-overflow:ellipsis;color:var(--wl-fg-soft);font-size:12px}.wl-table td.genre{white-space:nowrap}.wl-table th.sortable{cursor:pointer;user-select:none}.wl-table th.sortable::after{content:"";margin-left:6px;opacity:.6;font-size:10px}.wl-table th.sort-asc::after{content:"▲"}.wl-table th.sort-desc::after{content:"▼"}.wl-table td.title{white-space:normal;text-transform:none!important;letter-spacing:normal!important;font:inherit;color:inherit;-webkit-text-fill-color:currentColor}.wl-table td.title a{color:inherit;text-decoration:none;font:inherit;-webkit-text-fill-color:currentColor}.wl-table td.title a:visited{color:inherit}.wl-title-cell{display:grid;gap:5px;min-width:0}.wl-title-main{font-weight:700;color:var(--wl-fg);line-height:1.3;white-space:normal}.wl-title-sub{display:flex;align-items:center;gap:5px;flex-wrap:wrap;min-width:0}.wl-inline-pill{display:inline-flex;align-items:center;justify-content:center;min-height:20px;padding:0 8px;border-radius:999px;border:1px solid rgba(255,255,255,.09);background:rgba(255,255,255,.04);color:rgba(226,233,246,.76);font-size:11px;font-weight:700}.wl-mini{width:40px!important;height:60px!important;min-width:40px;min-height:60px;max-width:40px;max-height:60px;display:block;box-sizing:border-box;border-radius:8px;object-fit:cover;background:#0f131c;border:1px solid rgba(255,255,255,.08);box-shadow:0 10px 20px rgba(0,0,0,.20)}.wl-table td.wl-poster-cell{vertical-align:middle;background:transparent!important;border-radius:0!important}.wl-table td.sync{white-space:normal}.wl-col-hidden{display:none!important}.wl-cols{display:flex;flex-wrap:wrap;gap:8px}.wl-colchip{padding:0 10px;font-size:12px;font-weight:700;color:var(--wl-fg-soft)}.wl-matrix{display:flex;gap:6px;align-items:flex-start;flex-wrap:wrap;row-gap:6px}.wl-mat{display:inline-flex;align-items:center;justify-content:center;gap:5px;min-width:40px;min-height:28px;padding:0 8px;border:1px solid rgba(255,255,255,.08);border-radius:999px;background:rgba(255,255,255,.04);color:rgba(242,246,255,.78);box-shadow:inset 0 1px 0 rgba(255,255,255,.02)}.wl-mat img{height:13px;max-width:18px;filter:brightness(1.05)}.wl-mat .material-symbol{font-size:14px;line-height:1;color:currentColor;-webkit-text-fill-color:currentColor}.wl-mat.ok{border-color:rgba(111,214,173,.24);background:linear-gradient(180deg,rgba(53,119,92,.18),rgba(255,255,255,.04));color:#eafcf3}.wl-mat.miss{opacity:.34;filter:saturate(.12)}#page-watchlist .ins-card{position:relative;border-radius:18px;padding:10px 11px;overflow:hidden}#page-watchlist .ins-card::before{content:"";position:absolute;inset:0;pointer-events:none;background:radial-gradient(100% 120% at 100% 0%,rgba(76,68,170,.08),transparent 58%);opacity:.82}#page-watchlist .ins-row{position:relative;z-index:1;display:flex;align-items:center;gap:10px;padding:8px 4px;border-top:1px solid rgba(255,255,255,.05)}#page-watchlist .ins-row:first-child{border-top:none;padding-top:2px}#page-watchlist .ins-icon{width:34px;height:34px;border-radius:12px;display:flex;align-items:center;justify-content:center;background:linear-gradient(180deg,rgba(255,255,255,.07),rgba(255,255,255,.03));border:1px solid rgba(255,255,255,.08);box-shadow:0 8px 18px rgba(0,0,0,.18)}#page-watchlist .ins-title{font-weight:800;letter-spacing:-.01em}#page-watchlist .ins-kv{display:grid;grid-template-columns:96px minmax(0,1fr);gap:10px;align-items:center;width:100%}#page-watchlist .ins-kv label,#page-watchlist .ins-kv .field-label{color:var(--wl-fg-soft)}#page-watchlist .ins-metrics{display:grid;gap:10px;width:100%}#page-watchlist .wl-insight{display:grid;gap:10px;width:100%}#page-watchlist .wl-insight-hero{display:grid;grid-template-columns:minmax(112px,124px) minmax(0,1fr);align-items:stretch;gap:10px}#page-watchlist .wl-insight-score{position:relative;display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:108px;padding:14px 12px;border-radius:22px;overflow:hidden;border:1px solid rgba(255,255,255,.08);background:radial-gradient(120% 120% at 18% 16%,rgba(82,74,176,.12),transparent 44%),linear-gradient(180deg,rgba(9,11,17,.985),rgba(2,3,7,.955));box-shadow:inset 0 1px 0 rgba(255,255,255,.04);text-align:center}#page-watchlist .wl-insight-score::before{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(135deg,rgba(255,255,255,.06),transparent 58%)}#page-watchlist .wl-insight-score strong{position:relative;z-index:1;display:block;width:100%;margin:0;font-size:32px;font-weight:900;line-height:1;color:#f8fbff;letter-spacing:-.04em;text-align:center}#page-watchlist .wl-insight-score span{position:relative;z-index:1;display:block;width:100%;margin:8px 0 0;font-size:10px;font-weight:800;letter-spacing:.14em;line-height:1.2;text-transform:uppercase;color:rgba(223,231,245,.70);text-align:center}#page-watchlist .wl-insight-stats{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}#page-watchlist .wl-insight-stat{position:relative;min-height:50px;padding:10px 11px;border-radius:16px;border:1px solid rgba(255,255,255,.07);background:linear-gradient(180deg,rgba(9,11,17,.82),rgba(3,4,8,.88));box-shadow:inset 0 1px 0 rgba(255,255,255,.03);text-align:center}#page-watchlist .wl-insight-stat .k{display:block;font-size:10px;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:rgba(209,218,234,.62);text-align:center}#page-watchlist .wl-insight-stat .v{display:block;margin-top:6px;font-size:18px;font-weight:800;line-height:1.05;color:#f8fbff;text-align:center}#page-watchlist .wl-provider-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(132px,1fr));gap:8px}#page-watchlist .wl-provider-card{position:relative;padding:10px 10px 11px;border-radius:16px;overflow:hidden;border:1px solid rgba(255,255,255,.07);background:linear-gradient(180deg,rgba(10,12,18,.84),rgba(3,4,8,.88));box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}#page-watchlist .wl-provider-card::before{content:"";position:absolute;inset:0;pointer-events:none;background:radial-gradient(95% 120% at 100% 0%,rgba(82,74,176,.08),transparent 58%)}#page-watchlist .wl-provider-card.is-live{border-color:rgba(92,96,176,.14);box-shadow:inset 0 1px 0 rgba(255,255,255,.04),0 10px 24px rgba(12,14,28,.20)}#page-watchlist .wl-provider-card.is-idle{opacity:.72}#page-watchlist .wl-provider-top{position:relative;z-index:1;display:flex;align-items:center;justify-content:space-between;gap:8px}#page-watchlist .wl-provider-brand{display:inline-flex;align-items:center;justify-content:center;min-width:34px;min-height:26px;padding:0 8px;border-radius:999px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.04)}#page-watchlist .wl-provider-brand img{display:block;height:13px;max-width:40px}#page-watchlist .wl-provider-brand .wl-badge{line-height:1}#page-watchlist .wl-provider-top strong{position:relative;z-index:1;font-size:18px;font-weight:800;line-height:1;color:#f8fbff}#page-watchlist .wl-provider-name{position:relative;z-index:1;margin-top:10px;font-size:10px;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:rgba(209,218,234,.60)}#page-watchlist .wl-provider-sub{position:relative;z-index:1;margin-top:5px;font-size:11px;color:rgba(235,241,250,.72);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.wl-snack{position:fixed;left:50%;bottom:20px;transform:translateX(-50%);padding:10px 14px;display:flex;gap:10px;align-items:center;z-index:9999;border-radius:14px;color:#f7f9ff}.wl-hidden{display:none!important}.wl-modal{position:fixed;inset:0;display:none;align-items:center;justify-content:center;background:rgba(0,0,0,.76);backdrop-filter:blur(8px) saturate(120%);-webkit-backdrop-filter:blur(8px) saturate(120%);z-index:10050}.wl-modal.show{display:flex}.wl-modal .box{position:relative;width:min(88vw,920px);aspect-ratio:16/9;border-radius:18px;overflow:hidden;background:#04070d}.wl-modal .box iframe{width:100%;height:100%}.wl-modal .box .x{position:absolute;top:10px;right:10px;z-index:2}.wl-hide-overlays .wl-card-top,.wl-hide-overlays .wl-card .wl-card-meta{display:none!important}.wl-hide-overlays .wl-card::before{content:none!important}.wl-detail .actions .score.good{color:#5ee4ac}.wl-detail .actions .score.mid{color:#f0bf62}.wl-detail .actions .score.bad{color:#ff7d8c}.wl-detail{position:fixed;left:50%;bottom:12px;width:min(720px,calc(100vw - 396px));transform:translate(-50%,calc(100% + 16px));z-index:10000;overflow:hidden;border-radius:22px;transition:transform .26s ease;background:var(--wl-panel-bg-strong)}.wl-detail.show{transform:translate(-50%,0)}.wl-detail::before{content:"";position:absolute;inset:0;z-index:0;pointer-events:none;background:linear-gradient(90deg,rgba(2,3,7,.985) 0%,rgba(2,3,7,.96) 34%,rgba(2,3,7,.90) 64%,rgba(2,3,7,.76) 100%),var(--wl-backdrop,none);background-size:100% 100%,cover;background-position:center center,center center;background-repeat:no-repeat,no-repeat}.wl-detail .overview{margin-top:10px;padding:0;background:transparent;border:0;border-radius:0;line-height:1.46;color:rgba(242,246,255,.84);text-shadow:0 2px 10px rgba(0,0,0,.62)}.wl-detail .poster-col{display:flex;flex-direction:column;gap:8px;align-items:flex-start}.wl-detail .type-pill{display:inline-flex;align-items:center;justify-content:center;min-height:28px;padding:0 10px;border-radius:999px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.10);font-size:11px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:#eef3ff}.wl-detail .score{width:72px;height:72px}.wl-detail .score-label{font-size:11px;color:rgba(225,233,247,.68);font-weight:700}.wl-srcs{display:flex;gap:8px;justify-content:center;flex-wrap:wrap;margin-top:6px}.wl-src{display:inline-flex;align-items:center;justify-content:center;min-width:32px;min-height:28px;padding:0 8px;border-radius:999px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08)}.wl-src img{display:block;height:14px}.wl-badge{font-size:11px;font-weight:700;letter-spacing:.04em}.wl-detail .chip{display:inline-flex;align-items:center;justify-content:center;min-height:24px;padding:0 8px;border-radius:999px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.08);font-size:11px;font-weight:700;color:rgba(236,242,250,.82)}.wl-detail .dot{opacity:.42;display:inline-flex;align-items:center}.wl-resize{position:absolute;right:0;top:0;height:100%;width:8px;cursor:col-resize;opacity:.18}.wl-resize:hover{opacity:.45}.wl-pagination{display:flex;align-items:center;justify-content:center;gap:10px;margin-top:12px;font-size:12px;color:var(--wl-fg-soft)}.wl-pagination button{min-width:88px}@media (max-width:1140px){.wl-wrap{grid-template-columns:minmax(0,1fr)}.wl-side{position:static}.wl-detail{width:min(720px,calc(100vw - 24px))}}@media (max-width:760px){.wl-topline{padding:12px}.wl-main-shell{padding:10px}.wl-grid{--wl-min:132px}#page-watchlist .ins-kv{grid-template-columns:1fr;gap:8px}#page-watchlist .wl-insight-hero{grid-template-columns:1fr}.wl-toolbar{align-items:flex-start}.wl-toolbar-right{justify-content:flex-start}.wl-detail{width:min(720px,calc(100vw - 16px));bottom:8px}.wl-detail .inner{grid-template-columns:92px 1fr!important;padding:12px!important}.wl-detail .actions{grid-column:1 / -1;flex-direction:row!important;justify-content:space-between!important;width:100%;justify-self:stretch!important}.wl-table th,.wl-table td{padding:9px 8px}}`;

  // style inject
  const ensureStyle=(id,txt)=>{const s=document.getElementById(id)||Object.assign(document.createElement("style"),{id});s.textContent=txt;if(!s.parentNode)document.head.appendChild(s);};
  ensureStyle("watchlist-styles", css);

  /* Layout */
  const host=document.getElementById("page-watchlist"); if(!host) return;
  const readPrefs=()=>{try{return JSON.parse(localStorage.getItem("wl.prefs")||"{}")}catch{return{}}};
  const writePrefs=p=>{try{localStorage.setItem("wl.prefs",JSON.stringify(p))}catch{}};
  const prefs=Object.assign({posterMin:150,view:"posters",released:"both",overlays:"yes",genre:"",showHidden:false,sortKey:"title",sortDir:"asc",moreOpen:false,cols:{},colVis:{}},readPrefs());
  prefs.colVis = Object.assign({ poster:true, title:true, rel:true, genre:true, type:true, sync:true }, prefs.colVis || {});
  prefs.colVis.title = true;
  const providerMeta = () => window.CW?.ProviderMeta || {};
  const providerKey = (value) => providerMeta().keyOf?.(value) || String(value || "").trim().toUpperCase();
  const providerLabel = (value) => providerMeta().label?.(value) || providerKey(value) || String(value || "");
  const providerShortLabel = (value) => providerMeta().shortLabel?.(value) || providerLabel(value);
  let activeProviders = new Set();
  const watchlistProviderKeys = () => {
    const keys = providerMeta().watchlistProviders?.();
    return Array.isArray(keys) && keys.length
      ? keys
      : ["PLEX","SIMKL","ANILIST","TRAKT","TMDB","JELLYFIN","EMBY","MDBLIST","CROSSWATCH"];
  };
  const PROVIDERS = watchlistProviderKeys();
  const visibleProviders = () => PROVIDERS.filter((p) => p !== "CROSSWATCH" || activeProviders.has("CROSSWATCH"));
  const providerOptions=(empty="All")=>`<option value="">${empty}</option>${visibleProviders().map(p=>`<option value="${p}">${providerLabel(p)}</option>`).join("")}`;
  const deleteProviderOptions=pick=>`<option value="ALL">ALL (default)</option>${(pick ? PROVIDERS.filter(p=>pick.has(p)) : visibleProviders()).map(p=>`<option value="${p}">${providerLabel(p)}</option>`).join("")}`;
  host.innerHTML=`<div class="wl-topline"><div class="wl-title-stack"><div class="wl-title-row"><div class="wl-title">Watchlist</div><div class="wl-head-pills"><span id="wl-stat-total" class="wl-chip is-accent">0 items</span><span id="wl-stat-visible" class="wl-chip">0 visible</span><span id="wl-stat-sync" class="wl-chip is-muted">Awaiting sync</span></div></div><div class="wl-sub">Browse and manage your unified watchlist</div></div></div><div class="wl-wrap" id="watchlist-root"><div class="wl-main-shell"><div class="wl-toolbar"><div class="wl-toolbar-left"><label class="wl-chip wl-selectall"><input id="wl-select-all" type="checkbox"><span>Select all</span></label><span id="wl-count" class="wl-chip is-filter">0 selected</span></div><div class="wl-toolbar-right"><span id="wl-filter-state" class="wl-chip is-filter">All items</span></div></div><div id="wl-posters" class="wl-grid" style="display:none"></div><div id="wl-list" class="wl-table-wrap" style="display:none"><table class="wl-table"><colgroup><col class="c-sel"><col class="c-poster"><col class="c-title"><col class="c-rel"><col class="c-genre"><col class="c-type"><col class="c-sync"></colgroup><thead><tr><th style="text-align:center"><input id="wl-list-select-all" type="checkbox"></th><th class="sortable" data-sort="poster" data-col="poster" style="position:relative">Poster<span class="wl-resize"></span></th><th class="sortable" data-sort="title" data-col="title" style="position:relative">Title<span class="wl-resize"></span></th><th class="sortable" data-sort="release" data-col="rel" style="position:relative">Release<span class="wl-resize"></span></th><th class="sortable" data-sort="genre" data-col="genre" style="position:relative">Genre<span class="wl-resize"></span></th><th class="sortable" data-sort="type" data-col="type" style="position:relative">Type<span class="wl-resize"></span></th><th class="sortable" data-sort="sync" data-col="sync" style="position:relative">Sync<span class="wl-resize"></span></th></tr></thead><tbody id="wl-tbody"></tbody></table></div><div id="wl-pagination" class="wl-pagination" style="display:none"><button id="wl-page-prev" class="wl-btn">Previous</button><span id="wl-page-label" class="wl-muted">Page 1 of 1 • Rows 0–0 of 0</span><button id="wl-page-next" class="wl-btn">Next</button></div><div id="wl-empty" class="wl-empty wl-muted" style="display:none">No items match the current filters.</div></div><aside class="wl-side"><div class="ins-card"><div class="ins-row wl-ref-row" style="align-items:center"><div class="ins-icon"><span class="material-symbol">tune</span></div><div class="ins-title" style="margin-right:auto">Filters</div><button id="wl-refresh" class="wl-refresh-btn" title="Sync watchlist" aria-label="Sync watchlist"><span class="material-symbol ss-refresh-icon">sync</span></button></div><div class="ins-row"><div class="ins-kv"><label for="wl-view">View</label><select id="wl-view" name="wl-view" class="wl-input" style="width:auto;padding:6px 10px"><option value="posters">Posters</option><option value="list">List</option></select><label for="wl-q">Search</label><input id="wl-q" name="wl-q" class="wl-input" placeholder="Search title..."><label for="wl-type">Type</label><select id="wl-type" name="wl-type" class="wl-input"><option value="">All types</option><option value="movie">Movies</option><option value="tv">Shows</option><option value="anime">Anime</option></select><label for="wl-provider">Provider</label><select id="wl-provider" name="wl-provider" class="wl-input">${providerOptions()}</select><label id="wl-size-label" for="wl-size">Size</label><input id="wl-size" name="wl-size" type="range" min="120" max="320" step="10" class="wl-input" style="padding:0"></div></div><div class="ins-row" id="wl-more-panel" style="display:none"><div class="ins-kv"><label for="wl-released">Released</label><select id="wl-released" name="wl-released" class="wl-input"><option value="both">Both</option><option value="released">Released</option><option value="unreleased">Upcoming</option></select><label id="wl-overlays-label" for="wl-overlays">Overlays</label><select id="wl-overlays" name="wl-overlays" class="wl-input"><option value="yes">On</option><option value="no">Off</option></select><label for="wl-genre">Genre</label><select id="wl-genre" name="wl-genre" class="wl-input"><option value="">All</option></select><label for="wl-show-hidden">Hidden</label><label class="wl-chip" style="justify-content:flex-start"><input id="wl-show-hidden" type="checkbox"><span>Include local hidden</span></label><div id="wl-cols-label" class="field-label">Columns</div><div id="wl-cols" class="wl-cols"><label class="wl-colchip"><input type="checkbox" name="wl-col" data-col="poster">Poster</label><label class="wl-colchip"><input type="checkbox" name="wl-col" data-col="rel">Release</label><label class="wl-colchip"><input type="checkbox" name="wl-col" data-col="genre">Genre</label><label class="wl-colchip"><input type="checkbox" name="wl-col" data-col="type">Type</label><label class="wl-colchip"><input type="checkbox" name="wl-col" data-col="sync">Sync</label></div></div></div><div class="ins-row" style="justify-content:flex-end;gap:8px"><button id="wl-more" class="wl-btn" aria-expanded="false">More</button><button id="wl-clear" class="wl-btn">Reset</button></div></div><div class="ins-card"><div class="ins-row"><div class="ins-icon"><span class="material-symbol">flash_on</span></div><div class="ins-title">Actions</div></div><div class="ins-row"><div class="ins-kv"><div class="field-label">Delete</div><div class="wl-actions" style="display:flex;gap:10px;flex-wrap:wrap"><select id="wl-delete-provider" name="wl-delete-provider" class="wl-input" style="flex:1;min-width:180px">${deleteProviderOptions()}</select><button id="wl-delete" class="wl-btn danger" disabled>Delete</button></div><div class="field-label">Visibility</div><div class="wl-actions" style="display:flex;gap:10px;flex-wrap:wrap"><button id="wl-hide" class="wl-btn" disabled>Hide local</button><button id="wl-unhide" class="wl-btn">Unhide all</button></div></div></div></div><div class="ins-card"><div class="ins-row"><div class="ins-icon"><span class="material-symbol">monitoring</span></div><div class="ins-title">Coverage Pulse</div></div><div class="ins-row"><div id="wl-metrics" class="ins-metrics"></div></div></div></aside></div><div id="wl-snack" class="wl-snack wl-hidden" role="status" aria-live="polite"></div><div id="wl-detail" class="wl-detail" aria-live="polite"></div><div id="wl-trailer" class="wl-modal" aria-modal="true" role="dialog"><div class="box"><button class="wl-btn x" id="wl-trailer-close" title="Close"><span class="material-symbol">close</span></button></div></div>`;

  /* References to elements */
  const $ = id => document.getElementById(id);

  const postersEl   = $("wl-posters");
  const listWrapEl  = $("wl-list");
  const listBodyEl  = $("wl-tbody");
  const listSelectAll = $("wl-list-select-all");
  const empty       = $("wl-empty");
  const selAll      = $("wl-select-all");
  const selCount    = $("wl-count");
  const qEl         = $("wl-q");
  const tEl         = $("wl-type");
  const providerSel = $("wl-provider");
  const sizeInput   = $("wl-size");
  const sizeLabel   = $("wl-size-label");
  const delProv     = $("wl-delete-provider");
  const clearBtn    = $("wl-clear");
  const viewSel     = $("wl-view");
  const snack       = $("wl-snack");
  const metricsEl   = $("wl-metrics");
  const detailEl    = $("wl-detail");
  const sideEl      = document.querySelector(".wl-side");
  const moreBtn     = $("wl-more");
  const morePanel   = $("wl-more-panel");
  const releasedSel = $("wl-released");
  const overlaysSel = $("wl-overlays");
  const overlaysLabel = $("wl-overlays-label");
  const genreSel    = $("wl-genre");
  const colsLabel  = $("wl-cols-label");
  const colsBox    = $("wl-cols");
  const trailerModal= $("wl-trailer");
  const trailerClose= $("wl-trailer-close");
  const pagerEl     = $("wl-pagination");
  const pagerPrev   = $("wl-page-prev");
  const pagerNext   = $("wl-page-next");
  const pagerLabel  = $("wl-page-label");
  const topTotalEl  = $("wl-stat-total");
  const topVisibleEl= $("wl-stat-visible");
  const topSyncEl   = $("wl-stat-sync");
  const filterStateEl = $("wl-filter-state");
  const showHiddenChk = $("wl-show-hidden");
  const enhancedControlWrap = el => {
    const wrap = el?.nextElementSibling;
    return wrap?.classList?.contains("cw-icon-select") && wrap.__cwNativeSelect === el ? wrap : null;
  };
  const setControlVisible = (el, on) => {
    if (!el) return;
    el.style.display = on ? "" : "none";
    const wrap = enhancedControlWrap(el);
    if (wrap) wrap.style.display = on ? "" : "none";
  };

  /* Column sizing */
  const colSel = { title: ".c-title", rel: ".c-rel", genre: ".c-genre", type: ".c-type", sync: ".c-sync", poster: ".c-poster" };
  const minPx  = { title: 86, rel: 82, genre: 112, type: 72, sync: 118, poster: 56 };
  const defaultPx = { poster: 62, title: 240, rel: 110, genre: 150, type: 88, sync: 148 };
  try{const pw=parseInt((prefs.cols||{}).poster||"",10);if(pw&&pw>120){prefs.cols=prefs.cols||{};prefs.cols.poster=defaultPx.poster+"px";writePrefs(prefs);}}catch{}
  const isColVisible = k => k === "title" ? true : (prefs.colVis?.[k] !== false);

  function applyCols(init=false){
    const cg=document.querySelector(".wl-table colgroup"); if(!cg) return;
    prefs.cols=prefs.cols||{};
    let dirty=false;
    for(const [k,sel] of Object.entries(colSel)){
      const col=cg.querySelector(sel); if(!col) continue;
      const saved=parseInt(prefs.cols[k]||"",10);
      const next=Math.max(minPx[k], Number.isFinite(saved) ? saved : defaultPx[k]);
      const width=`${next}px`;
      if (prefs.cols[k] !== width) { prefs.cols[k] = width; dirty = true; }
      col.style.width = width;
    }
    if (dirty || init) writePrefs(prefs);
  }

  /* Column resizers */
  function attachResizers() {
    const cg = document.querySelector(".wl-table colgroup");
    if (!cg) return;

    const getCol = k => cg.querySelector(colSel[k]);
    const px = el => parseInt((el?.style.width || getComputedStyle(el).width), 10) || 0;
    const selColW = () => (document.querySelector(".wl-table col.c-sel") ? px(document.querySelector(".wl-table col.c-sel")) : 44);

    document.querySelectorAll(".wl-table thead th[data-col]").forEach(th => {
      const k = th.dataset.col, h = th.querySelector(".wl-resize"), c = getCol(k);
      if (!h || !c) return;

      const onDown = e => {
        e.preventDefault(); e.stopPropagation();
        const startX = e.clientX, base = px(c);

        const sumOther = () =>
          selColW() + Object.keys(colSel).reduce((s, kk) => {
            if (kk === k || !isColVisible(kk)) return s;
            return s + (px(getCol(kk)) || minPx[kk]);
          }, 0);

        const maxW = () => Math.max(minPx[k], (listWrapEl.clientWidth - 2) - sumOther());

        const onMove = ev => {
          const w = Math.min(maxW(), Math.max(minPx[k], base + (ev.clientX - startX)));
          c.style.width = w + "px";
        };
        const onUp = () => {
          prefs.cols[k] = c.style.width; writePrefs(prefs);
          window.removeEventListener("pointermove", onMove, true);
          window.removeEventListener("pointerup", onUp, true);
          window.removeEventListener("pointercancel", onUp, true);
        };

        window.addEventListener("pointermove", onMove, true);
        window.addEventListener("pointerup", onUp, true);
        window.addEventListener("pointercancel", onUp, true);
      };

      const onDbl = e => {
        e.stopPropagation();
        delete (prefs.cols || {})[k];
        writePrefs(prefs);
        applyCols(true);
      };

      h.addEventListener("pointerdown", onDown, true);
      h.addEventListener("dblclick", onDbl, true);
    });
  }

  applyCols(true);
  attachResizers();

  /* Date formatting */
  let [items, filtered] = [[], []];
  const selected = new Set();
  const hiddenSet = (() => { try { return new Set(JSON.parse(localStorage.getItem("wl.hidden") || "[]")); } catch { return new Set(); } })();
  const saveHidden = () => { try { localStorage.setItem("wl.hidden", JSON.stringify([...hiddenSet])); } catch {} };
  const hideBtn = document.getElementById("wl-hide"), unhideBtn = document.getElementById("wl-unhide");

  let viewMode = prefs.view === "list" ? "list" : "posters";
  let sortKey = prefs.sortKey || "title", sortDir = prefs.sortDir || "asc";

  const metaCache = new Map();
  const derivedCache = new Map();

  function rebuildProviderOptions() {
    if (providerSel) {
      const current = String(providerSel.value || "");
      providerSel.innerHTML = providerOptions();
      providerSel.value = Array.from(providerSel.options).some((o) => o.value === current) ? current : "";
    }
    if (delProv) {
      const current = String(delProv.value || "ALL");
      delProv.innerHTML = deleteProviderOptions();
      delProv.value = Array.from(delProv.options).some((o) => o.value === current) ? current : "ALL";
    }
  }

  let TMDB_OK = true;

  const PAGE_SIZE = 50;
  let currentPage = 1;
  let pageInfo = { start:0, end:0, total:0, pageCount:1 };
  let watchlistMeta = { lastSyncEpoch: 0 };

  /* utils */
  const esc = s => String(s).replace(/[&<>"]/g, m => ({ "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;" }[m]));
  const toLocale = () => navigator.language || "en-US";
  const cmp = (a, b) => a < b ? -1 : a > b ? 1 : 0;
  const cmpDir = v => (sortDir === "asc" ? v : -v);
  const normKey = it => it.key || it.guid || it.id || (it.ids?.tmdb && `tmdb:${it.ids.tmdb}`) || (it.ids?.imdb && `imdb:${it.ids.imdb}`) || (it.ids?.tvdb && `tvdb:${it.ids.tvdb}`) || "";
  const artUrl=(it,size,kind="poster")=>(!TMDB_OK||!(it?.tmdb||it?.ids?.tmdb))?"":`/art/tmdb/${(((it?.type||it?.media_type||"")+"").toLowerCase()==="movie"?"movie":"tv")}/${encodeURIComponent(String(it?.tmdb||it?.ids?.tmdb))}?kind=${encodeURIComponent(kind)}&size=${encodeURIComponent(size||"w342")}&locale=${encodeURIComponent(window.__CW_LOCALE||navigator.language||"en-US")}`;
  const parseReleaseDate = s => { if (typeof s !== "string" || !(s = s.trim())) return null; let y, m, d; if (/^\d{4}-\d{2}-\d{2}$/.test(s)) ([y, m, d] = s.split("-").map(Number)); else if (/^\d{2}-\d{2}-\d{4}$/.test(s)) { const a = s.split("-").map(Number); d = a[0]; m = a[1]; y = a[2]; } else return null; const t = Date.UTC(y, (m || 1) - 1, d || 1), dt = new Date(t); return Number.isFinite(dt.getTime()) ? dt : null; };
  const fmtDateSmart = (raw, loc) => { const dt = parseReleaseDate(raw); if (!dt) return ""; try { return new Intl.DateTimeFormat(loc || toLocale(), { day:"2-digit", month:"2-digit", year:"numeric", timeZone:"UTC" }).format(dt); } catch { return ""; } };
  const providersOf = it => Array.isArray(it.sources) ? it.sources.map(providerKey).filter(Boolean) : [];
  const metaKey = it => `${(String(it.type || "").toLowerCase() === "movie" ? "movie" : "tv")}:${it.tmdb || it.ids?.tmdb || ""}`;
  const getReleaseIso = it => {
    const tv = /^(tv|show|anime)$/i.test(String(it.type || ""));
    let iso = tv ? (it.first_air_date || it.firstAired || it.aired) : (it.release_date || it.released);
    iso = iso || it?.release?.date || "";
    if (!iso) {
      const m = metaCache.get(metaKey(it)) || {};
      iso = tv ? (m.detail?.first_air_date || m.release?.date || m.first_air_date || "")
              : (m.detail?.release_date || m.release?.date || "");
    }
    return typeof iso === "string" ? iso.trim() : "";
  };

  const typeLabelFor = it => {
    const raw = String(it?.type || "").toLowerCase();
    if (raw === "movie") return "Movie";
    if (raw === "anime") return "Anime";
    return "Show";
  };
  const posterTypeLabelFor = it => {
    const raw = String(it?.type || "").toLowerCase();
    if (raw === "movie") return "M";
    if (raw === "anime") return "A";
    return "S";
  };
  const yearFromIso = iso => (typeof iso === "string" && /^\d{4}/.test(iso) ? iso.slice(0,4) : "");
  const formatRelativeSync = epoch => {
    const n = Number(epoch) || 0;
    if (!n) return "Awaiting sync";
    const now = Math.floor(Date.now() / 1000);
    const diff = Math.max(0, now - n);
    if (diff < 45) return "Synced just now";
    if (diff < 3600) return `Synced ${Math.round(diff / 60)}m ago`;
    if (diff < 86400) return `Synced ${Math.round(diff / 3600)}h ago`;
    return `Synced ${Math.round(diff / 86400)}d ago`;
  };
  const describeFilters = () => {
    const bits = [];
    const q = (qEl?.value || "").trim();
    const ty = (tEl?.value || "").trim();
    const provider = (providerSel?.value || "").trim();
    const rel = normReleased(releasedSel?.value || prefs.released || "both");
    const genre = (genreSel?.value || prefs.genre || "").trim();
    if (q) bits.push(`Search: ${q}`);
    if (ty) bits.push(typeLabelFor({ type: ty }));
    if (provider) bits.push(providerLabel(provider));
    if (rel === "released") bits.push("Released only");
    if (rel === "unreleased") bits.push("Upcoming only");
    if (genre) bits.push(genre);
    if (showHiddenChk?.checked) bits.push("Hidden included");
    return bits.length ? bits.join(" • ") : "All items";
  };
  const updateHeaderSummary = () => {
    if (topTotalEl) topTotalEl.textContent = `${items.length} item${items.length === 1 ? "" : "s"}`;
    if (topVisibleEl) topVisibleEl.textContent = `${filtered.length} visible`;
    if (topSyncEl) {
      topSyncEl.textContent = formatRelativeSync(watchlistMeta.lastSyncEpoch);
      topSyncEl.title = watchlistMeta.lastSyncEpoch ? new Date(watchlistMeta.lastSyncEpoch * 1000).toLocaleString() : "";
    }
    if (filterStateEl) {
      filterStateEl.textContent = describeFilters();
      filterStateEl.title = filterStateEl.textContent;
    }
  };

  function computePageInfo() {
    const total = filtered.length;
    const pageCount = total ? Math.ceil(total / PAGE_SIZE) : 1;
    if (currentPage < 1) currentPage = 1;
    if (currentPage > pageCount) currentPage = pageCount;
    const start = total ? (currentPage - 1) * PAGE_SIZE : 0;
    const end = total ? Math.min(start + PAGE_SIZE, total) : 0;
    pageInfo = { start, end, total, pageCount };
  }

  function updatePaginationUI() {
    if (!pagerEl) return;
    const total = pageInfo.total;
    if (!total) {
      pagerEl.style.display = "none";
      return;
    }
    const start = pageInfo.start;
    const end = pageInfo.end;
    const pageCount = pageInfo.pageCount;
    pagerEl.style.display = "";
    pagerLabel.textContent = `Page ${currentPage} of ${pageCount} • Rows ${start + 1}\u2013${end} of ${total}`;
    pagerPrev.disabled = currentPage <= 1;
    pagerNext.disabled = currentPage >= pageCount;
  }

  /* Hide/Unhide buttons */
  hideBtn?.addEventListener("click", () => {
    if (!selected.size) return;
    selected.forEach(k => hiddenSet.add(k));
    saveHidden(); selected.clear(); applyFilters(); updateSelCount(); snackbar("Hidden locally");
  }, true);

  unhideBtn?.addEventListener("click", () => {
    hiddenSet.clear(); saveHidden(); applyFilters(); updateSelCount(); snackbar("All unhidden");
  }, true);

  /* Hydration listing */
  const _hydrating = new Set();
  const setText = (el, t) => {
    if (!el) return;
    const next = (t || "").trim();
    if (!next) return;
    el.textContent = next;
    el.title = next;
  };

  async function hydrateRow(it, tr){
    const k=normKey(it); if(_hydrating.has(k)) return; _hydrating.add(k);
    try{
      const canTMDB = (typeof TMDB_OK==="undefined") ? true : !!TMDB_OK;
      const movie = /^movie$/i.test(it.type||"");
      const m = canTMDB ? (await getMetaFor(it)) : null;

      const isoMeta = m ? (movie ? (m.detail?.release_date||m.release?.date||"") : (m.detail?.first_air_date||m.release?.date||"")) : "";
      const gs = m ? (Array.isArray(m.genres||m.detail?.genres) ? (m.genres||m.detail?.genres) : []) : [];
      const genresMeta = gs.map(g=>typeof g==="string"?g:(g?.name||g?.title||"")).filter(Boolean).slice(0,3).join(", ");

      const prev = derivedCache.get(k)||{};
      const isoBase = (prev.iso||getReleaseIso(it)||"").trim();
      const iso = (isoMeta||isoBase).trim();

      const genresBase = (prev.genresText||extractGenres(it).slice(0,3).join(", ")).trim();
      const genresText = (genresMeta||genresBase).trim();

      const relFmtBase = (prev.relFmt||fmtDateSmart(isoBase,toLocale())||"").trim();
      const relFmt = (fmtDateSmart(iso,toLocale())||relFmtBase).trim();

      derivedCache.set(k,{iso,relFmt,genresText});
      if(tr?.isConnected){ setText(tr.querySelector("td.rel"),relFmt); setText(tr.querySelector("td.genre"),genresText); }
    } finally { _hydrating.delete(k); }
  }

  /* API fetches */
  const fetchWatchlist = async () => {
    const r = await fetch("/api/watchlist/?limit=5000", { cache: "no-store" });
    if (!r.ok) throw new Error("watchlist fetch failed");
    const j = await r.json();
    TMDB_OK = !Boolean(j?.missing_tmdb_key);
    watchlistMeta.lastSyncEpoch = Number(j?.last_sync_epoch) || 0;
    return Array.isArray(j?.items) ? j.items : [];
  };

  const fetchConfig = async () => {
    if (authSetupPending()) return {};
    try { const r = await fetch("/api/config", { cache: "no-store" }); return r.ok ? await r.json() : {}; }
    catch { return {}; }
  };

  const getMetaFor = async it => {
    if (!TMDB_OK) return null;
    const k = metaKey(it), hit = metaCache.get(k);
    if (hit) return hit;
    const tmdb = String(it.tmdb || it.ids?.tmdb || "");
    if (!tmdb) return null;

    try {
      const r = await fetch("/api/metadata/bulk?overview=full", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          items: [{ type: k.startsWith("movie") ? "movie" : "show", tmdb }],
          need: { overview:1, tagline:1, runtime_minutes:1, poster:1, ids:1, videos:1, genres:1, certification:1, score:1, release:1, backdrop:1 },
          concurrency: 1
        })
      });
      const first = Object.values((await r.json())?.results || {})[0];
      const meta = first?.ok ? (first.meta || null) : null;
      if (meta) metaCache.set(k, meta);
      return meta;
    } catch { return null; }
  };

  /*  Genre extraction  */
  const extractGenres = it => {
    const meta = metaCache.get(metaKey(it));
    const srcs = [it.genres, it.genre, it.detail?.genres, it.meta?.genres, it.meta?.detail?.genres, meta?.genres, meta?.detail?.genres].filter(Boolean);
    return srcs.flatMap(s => Array.isArray(s)
      ? s.map(g => typeof g === "string" ? g : (g?.name || g?.title || g?.slug || ""))
      : String(s).split(/[|,\/]/)
    ).map(v => String(v || "").trim()).filter(Boolean);
  };

  function getDerived(it){
    const k = normKey(it);
    let d = derivedCache.get(k);
    if (d) return d;

    const iso = getReleaseIso(it);
    const relFmt = fmtDateSmart(iso, toLocale());
    const genresText = extractGenres(it).slice(0,3).join(", ");

    d = { iso, relFmt, genresText };
    derivedCache.set(k, d);
    return d;
  }

  const buildGenreIndex = list => {
    const m = new Map();
    for (const g of list.flatMap(extractGenres)) { const k = g.toLowerCase(); if (!m.has(k)) m.set(k, g); }
    return [...m.values()].sort((a, b) => a.localeCompare(b));
  };

  const populateGenreOptions = genres => {
    const mk = (v, l = v) => Object.assign(document.createElement("option"), { value: v, textContent: l });
    genreSel.replaceChildren(mk("", "All"), ...genres.map(g => mk(g)));
    genreSel.value = prefs.genre || "";
  };

  /* Provider chips */
  const providerLogoPath = name => window.CW?.ProviderMeta?.logoPath?.(name) || "";

  const providerChip = (name, state = "ok") => {
    const label = providerLabel(name);
    const shortLabel = providerShortLabel(name);
    const src = providerLogoPath(name), icon = state === "ok" ? "check_small" : "remove";
    return `<span class="wl-mat ${state}" title="${esc(label)} ${state === "ok" ? "present" : "missing"}">${src ? `<img src="${src}" alt="${esc(label)}">` : `<span class="wl-badge">${esc(shortLabel)}</span>`}<span class="material-symbol">${icon}</span></span>`;
  };
  const posterProviderIcon = name => {
    const label = providerLabel(name);
    const shortLabel = providerShortLabel(name);
    const src = providerLogoPath(name);
    return src
      ? `<span class="wl-provider-icon" title="${esc(label)}"><img src="${src}" alt="${esc(label)} logo"></span>`
      : `<span class="wl-provider-icon" title="${esc(label)}"><span class="wl-badge">${esc(shortLabel)}</span></span>`;
  };
  const providerMatrix = have => `<div class="wl-matrix">${PROVIDERS.map(p => activeProviders.has(p) ? providerChip(p, have.has(p) ? "ok" : "miss") : "").join("")}</div>`;
  const mapProvidersByKey = list => new Map(list.map(it => [normKey(it), new Set(providersOf(it))]).filter(([k]) => !!k));
  function updateMetrics() {
    const ORDER = PROVIDERS;

    const instsOf = (it, p) => {
      const sbp = it?.sources_by_provider || it?.sourcesByProvider || {};
      const arr = sbp?.[String(p || "").toLowerCase()];
      return Array.isArray(arr) ? arr.map(x => String(x || "").trim()).filter(Boolean) : [];
    };

    const visible = filtered.length;
    const hiddenLocal = items.reduce((n, it) => n + (hiddenSet.has(normKey(it)) ? 1 : 0), 0);
    const movies = filtered.filter(it => /^movie$/i.test(String(it?.type || ""))).length;
    const anime = filtered.filter(it => /^anime$/i.test(String(it?.type || ""))).length;
    const series = Math.max(0, visible - movies - anime);
    const active = ORDER.filter(p => activeProviders.has(p));
    const providerSlots = Math.max(active.length, 1);
    const syncDensity = visible
      ? Math.round(filtered.reduce((sum, it) => sum + providersOf(it).filter(p => activeProviders.has(p)).length, 0) / (visible * providerSlots) * 100)
      : 0;

    const stat = (label, value) => `<div class="wl-insight-stat"><span class="k">${label}</span><span class="v">${value}</span></div>`;

    const cards = active.map(p => {
      const count = filtered.reduce((n, it) => n + (providersOf(it).includes(p) ? 1 : 0), 0);
      const pct = visible ? Math.round((count / visible) * 100) : 0;
      const instSet = new Set();
      for (const it of filtered) {
        if (!providersOf(it).includes(p)) continue;
        for (const inst of instsOf(it, p)) instSet.add(inst);
      }
      const insts = [...instSet].filter(Boolean);
      insts.sort((a, b) => (a !== "default") - (b !== "default") || a.localeCompare(b));
      const hint = insts.length ? ` • ${esc(insts.slice(0, 2).join(", "))}${insts.length > 2 ? ` +${insts.length - 2}` : ""}` : "";
      const src = providerLogoPath(p);
      const label = providerLabel(p);
      const shortLabel = providerShortLabel(p);
      const brand = src
        ? `<span class="wl-provider-brand"><img src="${src}" alt="${esc(label)} logo"></span>`
        : `<span class="wl-provider-brand"><span class="wl-badge">${esc(shortLabel)}</span></span>`;
      return `<div class="wl-provider-card ${count ? "is-live" : "is-idle"}" title="${esc(`${label}: ${count}/${visible || 0}`)}">
        <div class="wl-provider-top">${brand}<strong>${count}</strong></div>
        <div class="wl-provider-name">${esc(label)}</div>
        <div class="wl-provider-sub">${pct}% coverage${hint}</div>
      </div>`;
    }).join("");

    metricsEl.innerHTML = `<div class="wl-insight">
      <div class="wl-insight-hero">
        <div class="wl-insight-score"><strong>${syncDensity}%</strong><span>Sync density</span></div>
        <div class="wl-insight-stats">
          ${stat("Visible", visible)}
          ${stat("Movies", movies)}
          ${stat("Series", series + anime)}
          ${stat("Hidden", hiddenLocal)}
        </div>
      </div>
      <div class="wl-provider-grid">${cards || '<div class="wl-provider-card is-idle"><div class="wl-provider-name">No active sources</div><div class="wl-provider-sub">Connect a source to see coverage.</div></div>'}</div>
    </div>`;
  }

  /* Sorting */
  const _t = it => String(it.title || "").toLowerCase();
  const _type = it => ((it.type || "").toLowerCase() === "show" ? "tv" : String(it.type || "").toLowerCase());

  function sortFilteredForList(arr) {
    const byTitle = (a, b) => cmp(_t(a), _t(b));
    const sorters = {
      title: (a, b) => cmpDir(byTitle(a, b)),
      type: (a, b) => cmpDir(cmp(_type(a), _type(b))),
      release: (a, b) => {
        const unk = sortDir === "asc" ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY;
        const ta = parseReleaseDate(getReleaseIso(a)), tb = parseReleaseDate(getReleaseIso(b));
        const va = ta ? ta.getTime() : unk, vb = tb ? tb.getTime() : unk;
        const diff = va - vb || byTitle(a, b);
        return cmpDir(diff);
      },
      genre: (a, b) => {
        const ga = (extractGenres(a)[0] || "").toLowerCase();
        const gb = (extractGenres(b)[0] || "").toLowerCase();
        const sentinel = "\uFFFF";
        const va = ga || (sortDir === "asc" ? sentinel : "");
        const vb = gb || (sortDir === "asc" ? sentinel : "");
        const diff = cmp(va, vb) || byTitle(a, b);
        return cmpDir(diff);
      },
      sync: (a, b) => {
        const ca = providersOf(a).length, cb = providersOf(b).length;
        return cmpDir((ca === cb ? cmp(String(a.title || ""), String(b.title || "")) : (ca - cb)));
      },
      poster: (a, b) => {
        const pa = !!artUrl(a, "w92"), pb = !!artUrl(b, "w92");
        return cmpDir((pa === pb ? cmp(String(a.title || ""), String(b.title || "")) : (pa ? 1 : -1)));
      }
    };
    const fn = sorters[sortKey] || (() => 0);
    return arr.slice().sort(fn);
  }

  function updateSortHeaderUI() {
    document.querySelectorAll(".wl-table th.sortable").forEach(th => {
      th.classList.toggle("sort-asc", th.dataset.sort === sortKey && sortDir === "asc");
      th.classList.toggle("sort-desc", th.dataset.sort === sortKey && sortDir === "desc");
    });
  }

  function setSort(k) {
    sortKey === k ? (sortDir = sortDir === "asc" ? "desc" : "asc") : (sortKey = k, sortDir = "asc");
    prefs.sortKey = sortKey; prefs.sortDir = sortDir; writePrefs(prefs);
    render(); updateSortHeaderUI();
  }

  function wireSortableHeaders() {
    document.querySelectorAll(".wl-table th.sortable").forEach(th =>
      th.addEventListener("click", e => { if (!e.target.closest(".wl-resize")) setSort(th.dataset.sort); }, true)
    );
    updateSortHeaderUI();
  }

  /* Filtering */
  const applyOverlayPrefUI = () => {
    const off = prefs.overlays === "no";
    postersEl.classList.toggle("wl-hide-overlays", off);
    if (off) forceHideDetail();
    const show = viewMode === "posters";
    if (overlaysLabel) overlaysLabel.style.display = show ? "" : "none";
    setControlVisible(overlaysSel, show);
  };


  function applyColVisibility(){
    const cg = document.querySelector(".wl-table colgroup");
    if (!cg) return;
    for (const k of Object.keys(colSel)) {
      const on = isColVisible(k);
      cg.querySelector(colSel[k])?.classList.toggle("wl-col-hidden", !on);
      document.querySelectorAll(`.wl-table [data-col="${k}"]`).forEach(el => el.classList.toggle("wl-col-hidden", !on));
    }
  }

  const applyColPrefUI = () => {
    const show = viewMode === "list";
    [colsLabel, colsBox].forEach(el => el && (el.style.display = show ? "" : "none"));
    if (!colsBox) return;
    colsBox.querySelectorAll('input[type="checkbox"][data-col]').forEach(cb => {
      cb.checked = isColVisible(cb.dataset.col);
    });
  };

  colsBox?.addEventListener("change", e => {
    const cb = e.target?.closest?.('input[type="checkbox"][data-col]');
    if (!cb) return;
    const k = cb.dataset.col;
    prefs.colVis = prefs.colVis || {};
    prefs.colVis[k] = !!cb.checked;
    prefs.colVis.title = true;
    writePrefs(prefs);
    applyCols();
    applyColVisibility();
  }, true);

const normReleased = v => (v === "yes" ? "released" : v === "no" ? "unreleased" : "both");

  function applyFilters() {
    currentPage = 1;
    const q = (qEl.value || "").toLowerCase().trim();
    const ty = (tEl.value || "").trim();
    const provider = (providerSel.value || "").toUpperCase();
    const releasedPref = normReleased(releasedSel?.value || prefs.released || "both");
    const genrePref = (genreSel?.value || prefs.genre || "").trim().toLowerCase();
    const todayUTC = Date.UTC(new Date().getUTCFullYear(), new Date().getUTCMonth(), new Date().getUTCDate());

    filtered = items.filter(it => {
      const key = normKey(it);
      if (hiddenSet.has(key) && !showHiddenChk?.checked) return false;

      const title = String(it.title || "").toLowerCase();
      const rawType = String(it.type || "").toLowerCase();
      const t = (rawType === "show" || rawType === "shows" || rawType === "series") ? "tv" : rawType;

      if (q && !title.includes(q)) return false;
      if (ty && t !== ty) return false;
      if (provider && !providersOf(it).includes(provider)) return false;

      if (releasedPref !== "both") {
        const dt = parseReleaseDate(getReleaseIso(it));
        const isRel = !!dt && dt.getTime() <= todayUTC;
        if ((releasedPref === "released" && !isRel) || (releasedPref === "unreleased" && isRel)) return false;
      }

      if (genrePref && !extractGenres(it).some(g => String(g).toLowerCase() === genrePref)) return false;
      return true;
    });

    render();
    updateMetrics();
  }

  /* Trailer modal */
  function pickTrailer(meta) {
    const flat = [meta?.videos, meta?.videos?.results, meta?.detail?.videos, meta?.detail?.videos?.results].flatMap(v => Array.isArray(v) ? v : []);
    const scored = flat.map(v => {
      const site0 = String(v.site || v.host || "").toLowerCase();
      const site = /youtube/.test(site0) ? "youtube" : /vimeo/.test(site0) ? "vimeo" : site0;
      const type = String(v.type || "").toLowerCase();
      const rank = (type.includes("trailer") ? 100 : type.includes("teaser") ? 60 : type.includes("clip") ? 40 : 10)
                + (v.official ? 30 : 0) + (site === "youtube" ? 5 : 0) + (v.published_at || v.created_at ? 1 : 0);
      return { site, key: v.key || v.id || "", name: v.name || "Trailer", rank };
    }).filter(v => v.site && v.key);
    const v = scored.sort((a,b)=>b.rank-a.rank)[0];
    if (!v) return null;
    if (v.site === "youtube") return { url: `https://www.youtube-nocookie.com/embed/${encodeURIComponent(v.key)}?autoplay=1&rel=0&modestbranding=1&playsinline=1`, title: v.name };
    if (v.site === "vimeo")   return { url: `https://player.vimeo.com/video/${encodeURIComponent(v.key)}?autoplay=1`, title: v.name };
    return null;
  }

  function openTrailerWithUrl(url, title="Trailer") {
    const box = trailerModal.querySelector(".box");
    box.querySelector("iframe")?.remove();
    const ifr = document.createElement("iframe");
    Object.assign(ifr, { title, src: url, loading: "lazy" });
    ifr.setAttribute("allow", "autoplay; fullscreen; encrypted-media; picture-in-picture");
    ifr.setAttribute("referrerpolicy", "strict-origin-when-cross-origin");
    box.appendChild(ifr);
    trailerModal.classList.add("show");
    trailerClose?.focus();
  }
  function closeTrailer() {
    trailerModal.classList.remove("show");
    const ifr = trailerModal.querySelector("iframe");
    if (ifr) { try { ifr.src = "about:blank"; } catch {} ifr.remove(); }
  }
  trailerClose?.addEventListener("click", e => (e.preventDefault(), closeTrailer()), true);
  document.addEventListener("keydown", e => { if (e.key === "Escape" && trailerModal?.classList.contains("show")) closeTrailer(); }, true);
  trailerModal?.addEventListener("click", e => { if (e.target === trailerModal) closeTrailer(); }, true);

  function createScoreSVG(score0to100) {
    const v = Math.max(0, Math.min(100, Number(score0to100) || 0));
    const r = 26, c = 2 * Math.PI * r, off = c * (1 - v / 100);
    return `<svg viewBox="0 0 60 60" class="score" aria-label="User score ${v}%">
      <circle cx="30" cy="30" r="${r}" fill="none" stroke="rgba(255,255,255,.12)" stroke-width="6"/>
      <circle cx="30" cy="30" r="${r}" fill="none" stroke="currentColor" stroke-width="6" stroke-linecap="round" stroke-dasharray="${c.toFixed(2)}" stroke-dashoffset="${off.toFixed(2)}"/>
      <text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle" font-size="14" font-weight="700" fill="#fff">${v}%</text>
    </svg>`;
  }

  function backdropFromMeta(it, meta){
  const tmdb = it?.tmdb || it?.ids?.tmdb || meta?.ids?.tmdb;
  if (!tmdb) return "";
  const type = (((it?.type||it?.media_type||meta?.type||"")+"").toLowerCase()==="movie"?"movie":"tv");
  return `/art/tmdb/${type}/${encodeURIComponent(String(tmdb))}?kind=backdrop&size=w1280&locale=${encodeURIComponent(window.__CW_LOCALE||navigator.language||"en-US")}`;
}

  function renderDetail(it, meta) {
    if (viewMode !== "posters" && viewMode !== "list") { forceHideDetail(); return; }
    const backdrop = backdropFromMeta(it, meta);
    detailEl.style.setProperty("--wl-backdrop", backdrop ? `url("${backdrop}")` : "none");
    const isMovie = String(it.type || "").toLowerCase() === "movie";
    const poster = artUrl(it, "w154") || "/assets/img/placeholder_poster.svg";
    const title = it.title || meta?.title || "Unknown";
    const year = String(it.year || meta?.year || yearFromIso(meta?.detail?.release_date || meta?.detail?.first_air_date || "") || "").trim();
    const runtime = (() => { const m = meta?.runtime_minutes|0; if (!m) return ""; const h = (m/60)|0, mm = m%60; return h ? `${h}h ${mm?mm+'m':''}` : `${mm}m`; })();
    const genresText = (Array.isArray(meta?.genres) ? meta.genres : Array.isArray(it?.genres) ? it.genres : []).slice(0,3).join(", ");
    const relIso = isMovie ? (meta?.detail?.release_date || meta?.release?.date || it?.release_date) : (meta?.detail?.first_air_date || it?.first_air_date);
    const metaLine = [runtime, fmtDateSmart(relIso, toLocale()), meta?.certification || meta?.release?.cert || meta?.detail?.certification, genresText]
      .filter(Boolean)
      .map((p,i)=> i? `<span class="dot">&bull;</span><span class="chip">${esc(p)}</span>` : `<span class="chip">${esc(p)}</span>`)
      .join("");
    const score100 = Number.isFinite(meta?.score) ? Math.round(meta.score) : (Number.isFinite(meta?.vote_average) ? Math.round(meta.vote_average*10) : null);
    const scoreCls = score100 == null ? "" : score100 >= 70 ? "good" : score100 >= 40 ? "mid" : "bad";
    const scoreHtml = score100 != null ? `<div style="text-align:center">${createScoreSVG(score100).replace('<svg', `<svg class="score ${scoreCls}"`)}<div class="score-label">User Score</div></div>` : "";

    const srcs = providersOf(it).map(s => {
      const src = providerLogoPath(s);
      return src ? `<span class="wl-src" title="${s}"><img src="${src}" alt="${s} logo"></span>` : `<span class="wl-src"><span class="wl-badge">${s}</span></span>`;
    }).join("");
    const hasTrailer = !!pickTrailer(meta);
    const overview = meta?.overview ? `<div class="overview" id="wl-overview">${esc(meta.overview)}</div>` : `<div class="overview wl-muted">No description available</div>`;

    detailEl.innerHTML = `
      <div class="inner" style="position:relative;z-index:1;max-width:unset;margin:0 auto;padding:12px 14px 14px;display:grid;grid-template-columns:112px minmax(0,1fr) 128px;gap:14px;align-items:start">
        <div class="poster-col">
          <img class="poster" src="${poster}" alt="" style="width:104px;border-radius:14px;box-shadow:0 12px 30px rgba(0,0,0,.42)" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'" />
          <div class="type-pill">${isMovie ? "Movie" : (String(it.type || "").toLowerCase() === "anime" ? "Anime" : "Show")}</div>
        </div>
        <div>
          <div style="display:flex;align-items:flex-start;gap:10px">
            <div style="flex:1;min-width:0">
              <div class="title" style="font-weight:800;font-size:20px;line-height:1.1;color:#f7f9ff">${esc(title)} ${year ? `<span class="year" style="color:rgba(226,233,247,.64)">${esc(year)}</span>` : ""}</div>
              <div class="meta" style="display:flex;flex-wrap:wrap;gap:8px;opacity:.95;margin-top:8px">${metaLine}</div>
            </div>
            <button class="wl-btn" id="wl-detail-close" title="Close"><span class="material-symbol">close</span></button>
          </div>
          ${overview}
        </div>
        <div class="actions" style="display:flex;flex-direction:column;align-items:center;gap:8px;align-self:start;justify-self:end">
          ${scoreHtml || ""}
          <button class="wl-btn" id="wl-play-trailer" ${hasTrailer ? "" : "data-fallback=1"}>Watch trailer</button>
          <div class="wl-srcs">${srcs}</div>
        </div>
      </div>`;
    detailEl.classList.add("show");

    document.getElementById("wl-detail-close")?.addEventListener("click", () => detailEl.classList.remove("show"), true);
    document.getElementById("wl-play-trailer")?.addEventListener("click", () => {
      const pick = pickTrailer(meta);
      if (pick) openTrailerWithUrl(pick.url, pick.title);
      else window.open(`https://www.youtube.com/results?search_query=${encodeURIComponent(`${it?.title || meta?.title || ""} ${(it?.year || meta?.year || "")} trailer`.trim())}`,"_blank","noopener,noreferrer");
    }, true);
  }

  /* preview on hover */
  let activePreviewKey = null;
  function forceHideDetail(){ if(!detailEl) return; detailEl.classList.remove("show"); activePreviewKey=null; }
  function showPreview(it, mode = viewMode){
    if (mode === "posters" && prefs.overlays === "no") return;
    if (viewMode !== mode) return;
    const k=normKey(it); activePreviewKey=k;
    getMetaFor(it).then(m=>{ if(activePreviewKey===k && viewMode === mode) renderDetail(it,m||{}); });
  }
  function hidePreview(it, mode = viewMode){
    if (viewMode !== mode) return;
    const k=normKey(it);
    if(!selected.has(k)&&activePreviewKey===k){ detailEl.classList.remove("show"); activePreviewKey=null; }
  }


  const _show = (el, on) => setControlVisible(el, on);

  function render() {
    const posters = viewMode === "posters";
    _show(postersEl, posters); _show(listWrapEl, !posters); _show(sizeInput, posters); _show(sizeLabel, posters);
    applyOverlayPrefUI();
    applyColPrefUI();

    computePageInfo();

    if (!filtered.length) {
      empty.style.display = ""; selAll.checked = false; listSelectAll.checked = false;
      postersEl.innerHTML = ""; listBodyEl.innerHTML = ""; selCount.textContent = "0 selected"; metricsEl.innerHTML = "";
      if (pagerEl) pagerEl.style.display = "none";
      updateHeaderSummary();
      return;
    }

    empty.style.display = "none";
    posters ? renderPosters() : renderList();
    selCount.textContent = `${selected.size} selected`;
    updatePaginationUI();
    updateHeaderSummary();
  }

  function renderPosters(){
    postersEl.replaceChildren();
    const frag=document.createDocumentFragment();
    const canTMDB=(typeof TMDB_OK==="undefined")?true:!!TMDB_OK;

    const start = pageInfo.start;
    const end = pageInfo.end;
    const pageItems = filtered.slice(start, end);

    pageItems.forEach((it,i)=>{
      const key=normKey(it);
      const imgUrl=canTMDB ? artUrl(it,"w342") : "";
      const src=imgUrl || "/assets/img/placeholder_poster.svg";
      const d = getDerived(it);
      const posterTypeLabel = posterTypeLabelFor(it);
      const relYear = String(it.year || yearFromIso(d.iso) || "").trim();
      const providerCount = providersOf(it).length;
      const card=document.createElement("div");
      card.className=`wl-card ${selected.has(key)?"selected":""}`;

      const provHtml = providersOf(it).map(p => posterProviderIcon(p)).join("");
      const eager=i<24?`loading="eager" fetchpriority="high"`:`loading="lazy"`;
      card.innerHTML=`
        <div class="wl-card-top">
          <div class="wl-provider-icons">${provHtml}</div>
          ${posterTypeLabel ? `<span class="wl-type-corner">${esc(posterTypeLabel)}</span>` : ""}
        </div>
        <img ${eager} decoding="async" src="${src}" alt="" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'"/>
        <div class="wl-card-meta">
          <div class="wl-card-title">${esc(it.title || "Unknown")}</div>
          <div class="wl-card-sub">
            <span>${esc(relYear || typeLabel || "Queued")}</span>
            <span>${providerCount} sync</span>
          </div>
        </div>`;

      card.addEventListener("click",()=>{
        selected.has(key)?selected.delete(key):selected.add(key);
        card.classList.toggle("selected"); updateSelCount();
        if(canTMDB) getMetaFor(it).then(m=>renderDetail(it,m||{})); else renderDetail(it,{});
      },true);

      card.addEventListener("mouseenter",()=>{ if(canTMDB) showPreview(it); },true);
      card.addEventListener("mouseleave",()=>hidePreview(it),true);

      frag.appendChild(card);
    });

    postersEl.appendChild(frag);
  }

  function renderList() {
    listBodyEl.replaceChildren();
    const frag = document.createDocumentFragment();
    const sorted = sortFilteredForList(filtered);
    const canTMDB=(typeof TMDB_OK==="undefined")?true:!!TMDB_OK;
    const start = pageInfo.start;
    const end = pageInfo.end;
    const rows = sorted.slice(start, end);

    rows.forEach(it => {
      const key = normKey(it), tr = document.createElement("tr");
      const typeLabel = typeLabelFor(it);
      const thumb = artUrl(it, "w92") || "/assets/img/placeholder_poster.svg";
      const have = new Set(providersOf(it));
      const matrix = providerMatrix(have);
      const d = getDerived(it);

      const yearHint = String(it.year || yearFromIso(d.iso) || "").trim();
      tr.innerHTML = `
        <td style="text-align:center"><input type="checkbox" name="wl-select" data-k="${key}" ${selected.has(key) ? "checked" : ""}></td>
        <td class="wl-poster-cell" data-col="poster" style="text-align:center"><img class="wl-mini" src="${thumb}" alt="" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'"/></td>
        <td class="title" data-col="title"><div class="wl-title-cell"><div class="wl-title-main">${esc(it.title || "")}</div><div class="wl-title-sub">${yearHint ? `<span class="wl-inline-pill">${esc(yearHint)}</span>` : ""}<span class="wl-inline-pill">${esc(typeLabel)}</span></div></div></td>
        <td class="rel" data-col="rel">${esc(d.relFmt)}</td>
        <td class="genre" data-col="genre" title="${esc(d.genresText)}">${esc(d.genresText)}</td>
        <td data-col="type"><span class="wl-inline-pill">${esc(typeLabel)}</span></td>
        <td class="sync" data-col="sync">${matrix}</td>
      `;

      if (!d.relFmt || !d.genresText) setTimeout(() => hydrateRow(it, tr), 0);
      const posterCell = tr.querySelector(".wl-poster-cell");
      const showFromCover = () => {
        if (canTMDB) showPreview(it, "list");
        else renderDetail(it, {});
      };
      const hideFromCover = e => {
        if (posterCell?.contains?.(e?.relatedTarget)) return;
        hidePreview(it, "list");
      };
      posterCell?.addEventListener("mouseenter", showFromCover, true);
      posterCell?.addEventListener("mouseleave", hideFromCover, true);
      posterCell?.addEventListener("focusin", showFromCover, true);
      posterCell?.addEventListener("focusout", hideFromCover, true);
      tr.querySelector('input[type=checkbox]')?.addEventListener("change", e => { e.target.checked ? selected.add(key) : selected.delete(key); updateSelCount(); }, true);
      frag.appendChild(tr);
    });

    listBodyEl.appendChild(frag);
    listSelectAll.checked = filtered.length > 0 && filtered.every(x => selected.has(normKey(x)));
    updateSortHeaderUI();
    applyColVisibility();
  }

  let snackTimer = null;
  function setSnackContent(parts){
    snack.replaceChildren(...parts.map(part =>
      typeof part === "string" ? document.createTextNode(part) : part
    ));
  }

  function snackbar(message){
    clearTimeout(snackTimer); snackTimer = null;
    setSnackContent([String(message ?? "")]);
    snack.classList.remove("wl-hidden");
    snackTimer = setTimeout(() => (snack.classList.add("wl-hidden"), snackTimer = null), 1800);
  }

  function rebuildDeleteProviderOptions(){
    const byKey = mapProvidersByKey(items), union = new Set(), prev = delProv.value;
    for (const k of selected) byKey.get(k)?.forEach?.(p => union.add(p));
    delProv.innerHTML = deleteProviderOptions(union);
    if ([...delProv.options].some(o => o.value === prev)) delProv.value = prev;
  }

  function updateSelCount(){
    selCount.textContent = `${selected.size} selected`;
    selCount.classList.toggle("is-accent", selected.size > 0);
    rebuildDeleteProviderOptions();
    document.getElementById("wl-delete").disabled = !(delProv.value && selected.size);
    document.getElementById("wl-hide").disabled = selected.size === 0;
    updateHeaderSummary();
  }

  async function postDelete(keys, provider){
    const send = async prov => {
      const r = await fetch("/api/watchlist/delete", {
        method:"POST", headers:{ "Content-Type":"application/json" },
        body: JSON.stringify({ keys, provider: prov })
      });
      const txt = await r.text(); let j=null; try{ j = txt ? JSON.parse(txt) : null }catch{}
      const okCount =
        typeof j?.deleted_ok === "number" ? j.deleted_ok :
        Array.isArray(j?.results) ? j.results.filter(x=>x && (x.ok===true || x.status==="ok")).length :
        (r.ok ? keys.length : 0);
      return { okCount, ok: r.ok || okCount>0 };
    };
    const p = (provider||"ALL");
    let res = await send(p.toUpperCase());
    if (!res.ok) res = await send(p.toLowerCase());
    return res;
  }

  const delBtn = document.getElementById("wl-delete");
  delBtn?.addEventListener("click", async () => {
    forceHideDetail();
    if (!selected.size) return snackbar("Nothing selected");
    const provider = (delProv?.value || "ALL");
    const PROV_UP = provider.toUpperCase();
    const keys = [...selected];
    const total = keys.length, CHUNK = 50;

    delBtn.disabled = delProv.disabled = true;
    const progress = d => {
      const count = document.createElement("b");
      count.textContent = `${d}/${total}`;
      setSnackContent([
        "Deleting ",
        count,
        ` ${PROV_UP==="ALL" ? "across providers" : "from " + PROV_UP}...`,
      ]);
      snack.classList.remove("wl-hidden");
    };
    progress(0);

    let done = 0, ok = 0;
    for (let i = 0; i < keys.length; i += CHUNK) {
      const res = await postDelete(keys.slice(i, i + CHUNK), provider);
      ok += res.okCount || 0; done = Math.min(total, i + CHUNK); progress(done);
    }
    snack.classList.add("wl-hidden");
    delBtn.disabled = delProv.disabled = false;

    const byProv = mapProvidersByKey(items);
    for (const k of keys){
      const s = byProv.get(k) || new Set();
      PROV_UP==="ALL" ? s.clear() : s.delete(PROV_UP);
      if (!s.size){ const idx = items.findIndex(it => normKey(it) === k); if (idx > -1) items.splice(idx,1); }
    }
    selected.clear(); applyFilters(); updateSelCount();
    forceHideDetail();
    hardReloadWatchlist().catch(()=>{});

    snackbar(ok>0 ? (PROV_UP==="ALL" ? `Deleted on available providers for ${ok}/${total}` : `Deleted ${ok}/${total} on ${PROV_UP}`) : "Delete completed with no visible changes");
  }, true);

  /* reference: event wiring */
  const on = (els, evts, fn, cap=true) => evts.forEach(e => els.forEach(el => el?.addEventListener(e, fn, cap)));
  const setPosterMin = px => {
    const min = Math.max(120, Math.min(320, Number(px) || 160));
    const badge = Math.max(20, Math.min(28, Math.round(min * 0.165)));
    const iconH = Math.max(11, Math.min(15, badge - 12));
    const typeH = Math.max(20, Math.min(24, Math.round(badge * 0.92)));
    const typeMin = Math.max(22, Math.min(28, Math.round(badge * 0.96)));
    postersEl.style.setProperty("--wl-min", `${min}px`);
    postersEl.style.setProperty("--wl-provider-badge", `${badge}px`);
    postersEl.style.setProperty("--wl-provider-icon-h", `${iconH}px`);
    postersEl.style.setProperty("--wl-type-pill-h", `${typeH}px`);
    postersEl.style.setProperty("--wl-type-pill-min", `${typeMin}px`);
  };

  ["pointerenter","pointerdown","focusin","mouseenter","touchstart"].forEach(ev =>
    sideEl?.addEventListener(ev, forceHideDetail, true)
  );

  qEl.addEventListener("input", applyFilters, true);
  on([tEl, providerSel], ["change","input"], applyFilters);

  moreBtn.addEventListener("click", () => {
    const open = morePanel.style.display !== "none";
    morePanel.style.display = open ? "none" : "";
    moreBtn.setAttribute("aria-expanded", String(!open));
    prefs.moreOpen = !open; writePrefs(prefs);
  }, true);

  on([releasedSel], ["change","input"], () => { prefs.released = normReleased(releasedSel.value); writePrefs(prefs); applyFilters(); });
  on([overlaysSel], ["change","input"], () => { prefs.overlays = overlaysSel.value || "yes"; writePrefs(prefs); applyOverlayPrefUI(); });
  on([genreSel], ["change","input"], () => { prefs.genre = genreSel.value || ""; writePrefs(prefs); applyFilters(); });
  showHiddenChk?.addEventListener("change", () => { prefs.showHidden = !!showHiddenChk.checked; writePrefs(prefs); applyFilters(); }, true);

  const selectAll = chk => { selected.clear(); if (chk.checked) filtered.forEach(it => { const k = normKey(it); if (k) selected.add(k); }); };
  selAll.addEventListener("change", () => { selectAll(selAll); (viewMode === "posters" ? renderPosters : renderList)(); updateSelCount(); }, true);
  listSelectAll.addEventListener("change", () => { selectAll(listSelectAll); renderList(); selAll.checked = listSelectAll.checked; updateSelCount(); }, true);

  clearBtn.addEventListener("click", () => {
    qEl.value = ""; tEl.value = ""; providerSel.value = "";
    releasedSel.value = "both"; overlaysSel.value = "yes"; genreSel.value = "";
    if (showHiddenChk) showHiddenChk.checked = false;
    Object.assign(prefs, { released:"both", overlays:"yes", genre:"", showHidden:false }); writePrefs(prefs);
    applyOverlayPrefUI(); applyFilters();
  }, true);

  delProv.addEventListener("change", updateSelCount, true);

  sizeInput.addEventListener("input", () => {
    const px = Math.max(120, Math.min(320, Number(sizeInput.value) || 150));
    setPosterMin(px); prefs.posterMin = px; writePrefs(prefs);
  }, true);

  document.addEventListener("keydown", e => {
    if (e.key === "Delete" && !document.getElementById("wl-delete").disabled) document.getElementById("wl-delete").click();
    if (e.key === "Escape") trailerModal.classList.contains("show") ? closeTrailer() : detailEl.classList.remove("show");
  }, true);

  viewSel.addEventListener("change", () => {
    viewMode = viewSel.value === "list" ? "list" : "posters";
    prefs.view = viewMode; writePrefs(prefs);
    forceHideDetail();
    render();
  });

  pagerPrev?.addEventListener("click", () => {
    if (currentPage > 1) {
      currentPage--;
      render();
    }
  }, true);

  pagerNext?.addEventListener("click", () => {
    const total = filtered.length;
    if (!total) return;
    const maxPage = Math.ceil(total / PAGE_SIZE);
    if (currentPage < maxPage) {
      currentPage++;
      render();
    }
  }, true);

  async function hardReloadWatchlist(){
    try{ items=await fetchWatchlist(); populateGenreOptions(buildGenreIndex(items)); applyFilters(); rebuildDeleteProviderOptions(); }
    catch(e){ console.warn("watchlist reload failed:", e); }
  }
  function _wlBusy(on){ const b=document.getElementById("wl-refresh"); if(!b)return; b.disabled=!!on; b.classList.toggle("loading",!!on); b.classList.toggle("spin",!!on); }
  document.getElementById("wl-refresh")?.addEventListener("click", async()=>{ if(hardReloadWatchlist._busy)return; hardReloadWatchlist._busy=true; _wlBusy(true); try{ await hardReloadWatchlist(); } finally{ _wlBusy(false); hardReloadWatchlist._busy=false; } }, {passive:true});
  window.Watchlist=Object.assign(window.Watchlist||{}, { refresh: hardReloadWatchlist });
  window.addEventListener("watchlist:refresh", hardReloadWatchlist);

  (async function init(){
    if (authSetupPending()) return;
    viewSel.value = viewMode;
    sizeInput.value = String(prefs.posterMin); setPosterMin(prefs.posterMin);
    releasedSel.value = prefs.released; overlaysSel.value = prefs.overlays; morePanel.style.display = prefs.moreOpen ? "" : "none";
    moreBtn.setAttribute("aria-expanded", String(!!prefs.moreOpen));
    if (showHiddenChk) showHiddenChk.checked = !!prefs.showHidden;

    const cfg = await fetchConfig();
    window.__CW_LOCALE = (cfg?.metadata?.locale || cfg?.ui?.locale || window.__CW_LOCALE || navigator.language || "en-US");
    const active = new Set();
    try {
      if (typeof window.getConfiguredProviders === "function") {
        for (const key of window.getConfiguredProviders(cfg || {})) active.add(providerKey(key));
      } else {
        if ((cfg?.crosswatch || cfg?.CrossWatch || {}).enabled !== false) active.add("CROSSWATCH");
        if (cfg?.plex?.account_token) active.add("PLEX");
        if (cfg?.simkl?.access_token) active.add("SIMKL");
        const anTok = cfg?.anilist?.access_token || cfg?.anilist?.token || cfg?.auth?.anilist?.access_token || cfg?.auth?.anilist?.token;
        if (anTok) active.add("ANILIST");
        if (cfg?.trakt?.access_token) active.add("TRAKT");
        if (cfg?.tmdb_sync?.api_key && cfg?.tmdb_sync?.session_id) active.add("TMDB");
        if (cfg?.jellyfin?.access_token) active.add("JELLYFIN");
        if (cfg?.emby?.access_token || cfg?.emby?.api_key || cfg?.emby?.token) active.add("EMBY");
        if (cfg?.mdblist?.api_key) active.add("MDBLIST");
      }
    } catch {}

    activeProviders = active;
    rebuildProviderOptions();
    items = await fetchWatchlist();
    populateGenreOptions(buildGenreIndex(items));
    applyOverlayPrefUI(); applyFilters(); rebuildDeleteProviderOptions(); wireSortableHeaders(); updateHeaderSummary();

    window.dispatchEvent(new CustomEvent("watchlist-ready"));
  })();
})();
