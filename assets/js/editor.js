/* assets/js/editor.js */
/* refactored */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const PAGE_SIZE = 50;
  const STORAGE_KEY = "cw-editor-ui";


  const ensureStyle = (id, txt) => {
    let s = document.getElementById(id);
    if (!s) {
      s = document.createElement("style");
      s.id = id;
    }
    s.textContent = txt;
    if (!s.parentNode) document.head.appendChild(s);
  };
  const css = `.cw-root{--cw-shell-bg:linear-gradient(180deg,rgba(7,10,16,.98),rgba(4,6,10,.97));--cw-panel-bg:linear-gradient(180deg,rgba(11,15,22,.96),rgba(6,8,14,.95));--cw-panel-strong:linear-gradient(180deg,rgba(9,12,19,.985),rgba(4,6,10,.975));--cw-border:rgba(255,255,255,.08);--cw-border-soft:rgba(255,255,255,.05);--cw-shadow:0 18px 46px rgba(0,0,0,.38),inset 0 1px 0 rgba(255,255,255,.03);--cw-fg:#f3f6ff;--cw-fg-soft:rgba(204,213,229,.70);--cw-accent:rgba(112,96,245,.34);--cw-accent-strong:rgba(112,96,245,.52);display:flex;flex-direction:column;gap:12px;color:var(--cw-fg)}.cw-topline{display:flex;align-items:flex-start;justify-content:space-between;gap:14px;flex-wrap:wrap;margin-bottom:2px;padding:16px 18px;border-radius:24px;border:1px solid var(--cw-border);background:radial-gradient(120% 130% at 12% 0%,rgba(86,75,196,.13),transparent 42%),radial-gradient(90% 120% at 100% 100%,rgba(70,54,170,.08),transparent 52%),var(--cw-shell-bg);box-shadow:var(--cw-shadow);backdrop-filter:blur(16px) saturate(125%);-webkit-backdrop-filter:blur(16px) saturate(125%)}.cw-head-copy{min-width:0;display:grid;gap:6px}.cw-title-row{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap}.cw-title{font-weight:900;font-size:28px;letter-spacing:-.03em;line-height:1.02;color:var(--cw-fg)}.cw-sub{max-width:74ch;color:var(--cw-fg-soft);font-size:14px;line-height:1.45}.cw-head-pills{margin-left:auto;display:flex;align-items:center;justify-content:flex-end;gap:8px;flex-wrap:wrap}.cw-chip{display:inline-flex;align-items:center;justify-content:center;gap:7px;min-height:40px;padding:0 14px;border-radius:999px;border:1px solid rgba(255,255,255,.09);background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.02));color:var(--cw-fg-soft);font-size:12px;font-weight:700;box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}.cw-chip strong{color:var(--cw-fg);font-weight:800}.cw-wrap{display:grid;grid-template-columns:minmax(0,1fr) 368px;gap:14px;align-items:start}.cw-main,.cw-side{display:flex;flex-direction:column;gap:12px;min-width:0}.cw-table-wrap,.cw-empty,#page-editor .ins-card,.cw-pop{border:1px solid var(--cw-border);background:var(--cw-panel-bg);box-shadow:var(--cw-shadow);backdrop-filter:blur(14px) saturate(124%);-webkit-backdrop-filter:blur(14px) saturate(124%)}.cw-controls{display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:12px;border-radius:20px;border:1px solid var(--cw-border);background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015));box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}.cw-controls .cw-input{flex:1 1 280px;max-width:none}.cw-controls-spacer{flex:1 1 auto}.cw-status-text{font-size:12px;color:var(--cw-fg-soft)}.cw-input,.cw-select,.cw-btn,.cw-pop-btn,.cw-extra-display{font:inherit;color:var(--cw-fg);outline:none}.cw-input,.cw-select{width:100%;min-height:40px;padding:9px 12px;border-radius:14px;border:1px solid rgba(255,255,255,.08);background:rgba(3,6,11,.86);box-shadow:inset 0 1px 0 rgba(255,255,255,.02);transition:border-color .16s ease,background .16s ease,box-shadow .16s ease,transform .16s ease}.cw-input:hover,.cw-select:hover{border-color:rgba(255,255,255,.12);background:rgba(5,8,14,.92)}.cw-input:focus,.cw-select:focus{border-color:rgba(117,104,240,.34);box-shadow:0 0 0 3px rgba(117,104,240,.11),inset 0 1px 0 rgba(255,255,255,.03);background:rgba(5,8,14,.96)}.cw-btn,.cw-pop-btn{min-height:38px;padding:0 14px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.025));cursor:pointer;display:inline-flex;align-items:center;justify-content:center;gap:7px;white-space:nowrap;font-weight:700;transition:transform .16s ease,background .16s ease,border-color .16s ease,opacity .16s ease,box-shadow .16s ease}.cw-btn:hover,.cw-pop-btn:hover,.cw-extra-display:hover{transform:translateY(-1px);border-color:rgba(255,255,255,.14);background:linear-gradient(180deg,rgba(255,255,255,.08),rgba(255,255,255,.04))}.cw-btn:active,.cw-pop-btn:active{transform:translateY(0)}.cw-btn[disabled],.cw-pop-btn[disabled]{opacity:.46;cursor:not-allowed;transform:none}.cw-btn.primary,.cw-pop-btn.primary{background:linear-gradient(180deg,rgba(96,104,242,.40),rgba(68,74,170,.26));border-color:rgba(133,140,255,.24);color:#f8fbff;box-shadow:0 8px 24px rgba(76,82,182,.16),inset 0 1px 0 rgba(255,255,255,.05)}.cw-btn.danger{background:linear-gradient(180deg,rgba(120,35,52,.30),rgba(72,18,29,.22));border-color:rgba(255,132,154,.14);color:#ffe7ee}.cw-btn-del{padding:0;width:30px;min-width:30px;height:30px;border-radius:10px}.cw-btn-del .material-symbol{font-size:15px;line-height:1}.cw-btn.sm{min-height:34px;padding:0 12px;font-size:12px}.cw-side .cw-select,.cw-side .cw-input{width:100%}.cw-backup-actions{display:flex;flex-wrap:wrap;gap:8px}.cw-table-wrap{border-radius:22px;overflow:auto;max-height:70vh;background:var(--cw-panel-strong)}.cw-table{width:100%;border-collapse:separate;border-spacing:0;table-layout:fixed;font-size:12px;color:var(--cw-fg)}.cw-table th,.cw-table td{padding:10px 10px;border-bottom:1px solid rgba(255,255,255,.05);text-align:left;vertical-align:middle;white-space:nowrap}.cw-table th{position:sticky;top:0;z-index:1;font-size:11px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:rgba(226,233,246,.68);background:linear-gradient(180deg,rgba(12,16,24,.98),rgba(7,9,15,.96));backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px)}.cw-table th.sortable{cursor:pointer;user-select:none}.cw-table th.sortable::after{content:"";margin-left:6px;opacity:.55;font-size:10px}.cw-table th.sort-asc::after{content:"▲"}.cw-table th.sort-desc::after{content:"▼"}.cw-table tbody tr{transition:background .15s ease,box-shadow .15s ease}.cw-table tbody tr:hover{background:rgba(255,255,255,.028)}.cw-table tr:last-child td{border-bottom:none}.cw-table input:not(.cw-checkbox){width:100%;min-height:34px;padding:7px 9px;background:rgba(3,6,11,.82);border:1px solid rgba(255,255,255,.08);border-radius:10px;font-size:12px;color:var(--cw-fg);transition:border-color .16s ease,box-shadow .16s ease,background .16s ease}.cw-table input:not(.cw-checkbox):focus{border-color:rgba(117,104,240,.36);box-shadow:0 0 0 3px rgba(117,104,240,.10);background:rgba(6,9,14,.95)}.cw-col-year input{min-width:74px}.cw-table .cw-key{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:11px}.cw-row-episode{background:rgba(96,104,242,.04)}.cw-row-deleted td{opacity:.38;text-decoration:line-through}.cw-title-cell{display:flex;flex-direction:column;align-items:stretch;gap:5px;min-width:0}.cw-title-row{display:flex;align-items:center;gap:8px;min-width:0;flex-wrap:nowrap}.cw-title-sub{font-size:11px;color:var(--cw-fg-soft);line-height:1.25;padding-left:2px;white-space:normal}.cw-title-row>input{flex:1 1 auto;min-width:0;width:auto}.cw-title-search-btn{flex:0 0 auto;width:34px;height:34px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.02));color:#eff4ff;display:inline-flex;align-items:center;justify-content:center;cursor:pointer;padding:0;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);transition:transform .16s ease,border-color .16s ease,background .16s ease,box-shadow .16s ease}.cw-title-search-btn:hover{transform:translateY(-1px);border-color:rgba(255,255,255,.15);background:linear-gradient(180deg,rgba(255,255,255,.09),rgba(255,255,255,.04));box-shadow:0 8px 20px rgba(0,0,0,.18)}.cw-title-search-btn .material-symbol{font-size:18px}.cw-pop{position:fixed;z-index:10060;padding:12px 12px 13px;color:var(--cw-fg);width:min(560px,calc(100vw - 28px));max-height:calc(100vh - 120px);overflow:hidden;display:flex;flex-direction:column;border-radius:22px;background:linear-gradient(180deg,rgba(8,11,18,.98),rgba(4,6,10,.97))}.cw-pop-title{font-size:11px;font-weight:800;margin-bottom:6px;letter-spacing:.12em;text-transform:uppercase;color:var(--cw-fg-soft)}.cw-pop-actions{display:flex;justify-content:flex-end;gap:8px;margin-top:10px;flex-wrap:wrap}.cw-pop-btn.ghost{background:rgba(255,255,255,.03)}.cw-search-bar{display:grid;gap:8px;padding:12px;border-radius:18px;border:1px solid rgba(255,255,255,.07);background:linear-gradient(180deg,rgba(255,255,255,.025),rgba(255,255,255,.012));box-shadow:inset 0 1px 0 rgba(255,255,255,.025)}.cw-search-bar input,.cw-search-bar select,.cw-pop input[type="time"]{width:100%;min-height:42px;padding:10px 14px;border-radius:14px;border:1px solid rgba(255,255,255,.08);background:rgba(2,4,9,.92);color:var(--cw-fg);outline:none}.cw-search-bar input:focus,.cw-search-bar select:focus,.cw-pop input[type="time"]:focus{border-color:rgba(117,104,240,.26);box-shadow:0 0 0 3px rgba(117,104,240,.08)}.cw-search-results{margin-top:10px;border:1px solid rgba(255,255,255,.06);border-radius:18px;overflow:auto;background:rgba(255,255,255,.02)}.cw-search-item{display:flex;gap:12px;width:100%;padding:14px;border:0;border-bottom:1px solid rgba(255,255,255,.05);cursor:pointer;transition:background .14s ease,border-color .14s ease,transform .14s ease;background:linear-gradient(180deg,rgba(255,255,255,.028),rgba(255,255,255,.015));color:var(--cw-fg);font:inherit;text-align:left}.cw-search-item:last-child{border-bottom:none}.cw-search-item:hover{background:rgba(255,255,255,.05)}.cw-search-poster{width:52px;height:76px;border-radius:10px;overflow:hidden;background:#050810;border:1px solid rgba(255,255,255,.06);flex:0 0 auto}.cw-search-poster img{width:100%;height:100%;object-fit:cover}.cw-search-poster-placeholder{width:100%;height:100%;display:flex;align-items:center;justify-content:center;color:var(--cw-fg-soft);font-size:11px}.cw-search-content{display:grid;gap:4px;min-width:0;align-content:start}.cw-search-title-line{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.cw-search-title{font-weight:800;color:var(--cw-fg)}.cw-search-tag,.cw-rating-pill,.cw-type-pill,.cw-type-chip,.cw-extra-display,.cw-tag{display:inline-flex;align-items:center;justify-content:center;border-radius:999px;border:1px solid rgba(255,255,255,.09);background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.025));box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}.cw-search-tag{min-height:22px;padding:0 8px;font-size:10px;font-weight:800;color:rgba(236,242,251,.78);letter-spacing:.04em;text-transform:uppercase}.cw-search-meta,.cw-search-overview,.cw-search-empty,.cw-search-status{font-size:12px;color:var(--cw-fg-soft);line-height:1.4}.cw-search-overview{display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}.cw-search-empty{padding:14px}.cw-datetime-grid,.cw-rating-grid,.cw-type-grid{display:grid;gap:8px}.cw-datetime-grid{grid-template-columns:repeat(auto-fit,minmax(150px,1fr))}.cw-rating-grid{grid-template-columns:repeat(auto-fit,minmax(64px,1fr));margin-top:10px}.cw-rating-pill,.cw-type-pill{min-height:34px;padding:0 10px;font-size:12px;font-weight:800;color:var(--cw-fg-soft);cursor:pointer;transition:transform .16s ease,border-color .16s ease,background .16s ease}.cw-rating-pill:hover,.cw-type-pill:hover,.cw-type-chip:hover{transform:translateY(-1px);border-color:rgba(255,255,255,.14);background:linear-gradient(180deg,rgba(255,255,255,.08),rgba(255,255,255,.04))}.cw-rating-pill.active,.cw-type-pill.active,.cw-type-chip.active{color:#f7f9ff;border-color:rgba(133,140,255,.22);background:linear-gradient(180deg,rgba(96,104,242,.24),rgba(70,74,150,.12))}.cw-type-grid{grid-template-columns:repeat(auto-fit,minmax(120px,1fr));margin-top:10px}.cw-type-filter{display:flex;gap:8px;flex-wrap:wrap}.cw-type-chip{min-height:34px;padding:0 12px;font-size:12px;font-weight:800;color:var(--cw-fg-soft);cursor:pointer;transition:transform .16s ease,border-color .16s ease,background .16s ease}.cw-empty{display:grid;place-items:center;min-height:160px;border-radius:22px;padding:18px;text-align:center;color:var(--cw-fg-soft)}.cw-pager{display:flex;align-items:center;justify-content:center;gap:10px;margin-top:2px;color:var(--cw-fg-soft);font-size:12px}.cw-pager .cw-page-info{min-width:200px;text-align:center}.cw-pager .cw-btn{min-width:110px}#page-editor .ins-card{position:relative;border-radius:22px;padding:12px 13px;overflow:hidden}#page-editor .ins-card::before{content:"";position:absolute;inset:0;pointer-events:none;background:radial-gradient(130% 120% at 100% 0%,rgba(94,81,210,.09),transparent 58%)}#page-editor .ins-row{position:relative;z-index:1;display:flex;align-items:center;gap:10px;padding:10px 4px;border-top:1px solid rgba(255,255,255,.05)}#page-editor .ins-row:first-child{border-top:none;padding-top:2px}#page-editor .ins-icon{width:36px;height:36px;border-radius:14px;display:flex;align-items:center;justify-content:center;background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.025));border:1px solid rgba(255,255,255,.08);box-shadow:0 10px 22px rgba(0,0,0,.22)}#page-editor .ins-title{font-weight:900;letter-spacing:-.01em;font-size:15px;color:var(--cw-fg)}#page-editor .ins-kv{display:grid;grid-template-columns:92px minmax(0,1fr);gap:10px;align-items:center;width:100%}#page-editor .ins-kv label{color:var(--cw-fg-soft);font-size:12px;font-weight:700;letter-spacing:.03em}#page-editor .ins-metrics{display:flex;flex-direction:column;gap:8px;width:100%}#page-editor .metric-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px}#page-editor .metric-divider{height:1px;background:rgba(255,255,255,.06);margin:2px 0}#page-editor .metric{position:relative;display:grid;grid-template-columns:32px minmax(0,1fr);align-items:center;gap:8px;min-height:60px;padding:10px;border-radius:16px;background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.015));border:1px solid rgba(255,255,255,.07);overflow:hidden}#page-editor .metric::before{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(135deg,rgba(255,255,255,.03),transparent 55%)}#page-editor .metric .material-symbol{font-size:18px;color:#edf3ff;opacity:.92;-webkit-text-fill-color:currentColor}#page-editor .metric .m-val{font-weight:900;font-size:18px;line-height:1;color:#f8fbff}#page-editor .metric .m-lbl{font-size:11px;opacity:.72;letter-spacing:.08em;text-transform:uppercase;margin-top:3px}.cw-tag{position:relative;gap:8px;min-height:34px;padding:0 12px;color:var(--cw-fg-soft);font-size:12px;font-weight:800}.cw-tag-dot{width:8px;height:8px;border-radius:999px;background:#94a3b8;box-shadow:0 0 0 6px rgba(148,163,184,.08)}.cw-tag.loaded{color:#ebfff4;border-color:rgba(108,216,167,.16);background:linear-gradient(180deg,rgba(31,85,58,.18),rgba(255,255,255,.025))}.cw-tag.loaded .cw-tag-dot{background:#42d392;box-shadow:0 0 0 6px rgba(66,211,146,.10)}.cw-tag.warn{color:#fff9ea;border-color:rgba(255,210,109,.18);background:linear-gradient(180deg,rgba(112,88,33,.18),rgba(255,255,255,.025))}.cw-tag.warn .cw-tag-dot{background:#f5c563;box-shadow:0 0 0 6px rgba(245,197,99,.10)}.cw-tag.error{color:#fff0f3;border-color:rgba(255,132,154,.16);background:linear-gradient(180deg,rgba(108,34,49,.18),rgba(255,255,255,.025))}.cw-tag.error .cw-tag-dot{background:#ff879d;box-shadow:0 0 0 6px rgba(255,135,157,.10)}.cw-extra-display{min-height:34px;width:100%;padding:0 12px;display:inline-flex;align-items:center;justify-content:space-between;gap:8px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.03);cursor:pointer;transition:transform .16s ease,border-color .16s ease,background .16s ease}.cw-extra-display-label,.cw-extra-display-placeholder{font-size:11px;font-weight:800;color:var(--cw-fg-soft);letter-spacing:.05em;text-transform:uppercase}.cw-extra-display-value{font-size:12px;font-weight:700;color:var(--cw-fg)}.cw-extra-display-icon{opacity:.7}.cw-state-hint{border:1px dashed rgba(255,255,255,.12);border-radius:16px;padding:12px 13px;background:rgba(255,255,255,.02);color:var(--cw-fg-soft);font-size:12px;line-height:1.5}.cw-state-hint strong{color:var(--cw-fg)}.cw-checkbox{appearance:none;-webkit-appearance:none;position:relative;display:inline-block;vertical-align:middle;flex:none;width:18px!important;height:18px!important;min-width:18px!important;min-height:18px!important;margin:0;padding:0!important;border-radius:6px;border:1px solid rgba(255,255,255,.14);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.015));box-shadow:inset 0 1px 0 rgba(255,255,255,.03),0 4px 12px rgba(0,0,0,.16);cursor:pointer;transition:border-color .16s ease,background .16s ease,box-shadow .16s ease,transform .16s ease}.cw-checkbox:hover{border-color:rgba(255,255,255,.22);background:linear-gradient(180deg,rgba(255,255,255,.065),rgba(255,255,255,.03))}.cw-checkbox:focus-visible{outline:none;box-shadow:0 0 0 3px rgba(104,112,236,.12),inset 0 1px 0 rgba(255,255,255,.04),0 4px 12px rgba(0,0,0,.18)}.cw-checkbox:checked{border-color:rgba(132,140,255,.34);background:linear-gradient(180deg,rgba(84,94,214,.52),rgba(56,63,144,.30));box-shadow:0 0 0 3px rgba(104,112,236,.11),inset 0 1px 0 rgba(255,255,255,.06),0 6px 16px rgba(0,0,0,.18)}.cw-checkbox:checked::after{content:"";position:absolute;left:5px;top:1px;width:5px;height:10px;border-right:2px solid #fff;border-bottom:2px solid #fff;transform:rotate(45deg)}.cw-checkbox:disabled{opacity:.45;cursor:not-allowed;box-shadow:none}.cw-bulk{display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:8px 10px;border-radius:16px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.02));box-shadow:inset 0 1px 0 rgba(255,255,255,.02)}.cw-bulk-count{font-size:12px;font-weight:800;color:var(--cw-fg)}.cw-progress{height:10px;border-radius:999px;background:rgba(255,255,255,.06);overflow:hidden;border:1px solid rgba(255,255,255,.07)}.cw-progress>span{display:block;height:100%;width:40%;background:linear-gradient(90deg,rgba(96,104,242,.10),rgba(96,104,242,.72),rgba(122,132,255,.88),rgba(96,104,242,.10));animation:cw-progress-move 1.15s linear infinite}@keyframes cw-progress-move{0%{transform:translateX(-100%)}100%{transform:translateX(250%)}}.cw-collapse summary{list-style:none;color:var(--cw-fg);font-weight:800}.cw-collapse summary::-webkit-details-marker{display:none}@media (max-width:1120px){.cw-wrap{grid-template-columns:minmax(0,1fr)}.cw-head-pills{margin-left:0;justify-content:flex-start}}@media (max-width:760px){.cw-topline{padding:14px}.cw-title{font-size:24px}.cw-sub{font-size:13px}.cw-controls{padding:10px}.cw-table th,.cw-table td{padding:9px 8px}#page-editor .ins-kv{grid-template-columns:1fr;gap:8px}.cw-pager{flex-wrap:wrap}}`;
  ensureStyle("editor-styles", css);
  ensureStyle("editor-scrollbars",".cw-table-wrap{scrollbar-width:thin;scrollbar-color:#8b5cf6 #10131a}.cw-table-wrap::-webkit-scrollbar{height:10px;width:10px}.cw-table-wrap::-webkit-scrollbar-track{background:rgba(255,255,255,.03);border-radius:12px}.cw-table-wrap::-webkit-scrollbar-thumb{background:linear-gradient(180deg,#8b5cf6 0%,#3b82f6 100%);border-radius:12px;border:2px solid #11141c;box-shadow:inset 0 0 0 1px rgba(139,92,246,.35),0 0 10px rgba(139,92,246,.4)}.cw-table-wrap::-webkit-scrollbar-thumb:hover{background:linear-gradient(180deg,#a78bfa 0%,#60a5fa 100%)}");
  ensureStyle("editor-icon-select-styles",".cw-editor-icon-select{min-width:200px;flex:1}.cw-editor-icon-select .cw-icon-select-btn{min-height:40px}.cw-editor-icon-select .cw-icon-select-icon{width:16px;height:16px;filter:drop-shadow(0 1px 2px rgba(0,0,0,.35))}.cw-editor-icon-select .cw-icon-select-label{font-size:13px}");

  let cwEditorBooted = false;
  let cwEditorBootRetryWired = false;

  function bootEditor() {
    if (cwEditorBooted) return;
    const host = document.getElementById("page-editor");
    if (!host) return;
    cwEditorBooted = true;

  const state = {
    source: "state",
    kind: "watchlist",
    snapshot: "",
    pair: "",
    instance: "default",
    pairs: [],
    baselineItems: {},
    manualAdds: {},
    manualBlocks: [],
    items: {},
    rows: [],
    selected: new Set(),
    pageRids: [],
    ridSeq: 1,
    filter: "",
    loading: false,
    saving: false,
    snapshots: [],
    instance: "default",
    importEnabled: false,
    importProviders: [],
    importProvider: "",
    importProviderInstance: "default",
    importMode: "replace",
    importFeatures: { watchlist: true, history: true, ratings: true, progress: true },
    hasChanges: false,
    page: 0,
    blockedOnly: false,
    typeFilter: { movie: true, show: true, anime: true, season: true, episode: true },
    sortKey: "title",
    sortDir: "asc",
  };

  function restoreUIState() {
    try {
      if (typeof localStorage === "undefined") return;
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return;
      const saved = JSON.parse(raw);

      const sources = ["tracker", "pair", "state"];
      if (saved.source && sources.includes(saved.source)) state.source = saved.source;

      if (typeof saved.blockedOnly === "boolean") state.blockedOnly = saved.blockedOnly;

      const kinds = ["watchlist", "history", "ratings", "progress"];
      if (saved.kind && kinds.includes(saved.kind)) state.kind = saved.kind;

      if (typeof saved.snapshot === "string") state.snapshot = saved.snapshot;
      if (typeof saved.instance === "string" && saved.instance.trim()) state.instance = saved.instance;

      if (typeof saved.pair === "string") state.pair = saved.pair;
      if (typeof saved.filter === "string") state.filter = saved.filter;

      if (saved.typeFilter && typeof saved.typeFilter === "object") {
        ["movie", "show", "anime", "season", "episode"].forEach(t => {
          if (typeof saved.typeFilter[t] === "boolean") state.typeFilter[t] = saved.typeFilter[t];
        });
      }

      const sortKeys = ["title", "type", "key", "extra"];
      if (saved.sortKey && sortKeys.includes(saved.sortKey)) state.sortKey = saved.sortKey;
      if (saved.sortDir === "asc" || saved.sortDir === "desc") state.sortDir = saved.sortDir;
    } catch (_) {}
  }

  restoreUIState();

  function wireStaticLabels(root) {
    if (!root) return;

    const bindPrevLabel = (fieldId) => {
      const field = root.querySelector(`#${fieldId}`);
      const label = field?.previousElementSibling;
      if (label?.tagName === "LABEL") label.htmlFor = fieldId;
    };

    bindPrevLabel("cw-source");
    bindPrevLabel("cw-kind");
    bindPrevLabel("cw-pair");
    bindPrevLabel("cw-snapshot");
    bindPrevLabel("cw-instance");

    const convertGroupLabel = (cardId) => {
      const field = root.querySelector(`#${cardId} .ins-kv`);
      const label = field?.firstElementChild;
      if (!field || label?.tagName !== "LABEL") return;
      const title = document.createElement("div");
      title.className = "field-label";
      title.textContent = label.textContent || "";
      label.replaceWith(title);
    };

    convertGroupLabel("cw-backup-card");
    convertGroupLabel("cw-state-backup-card");
  }

  host.innerHTML = `<div class="cw-root"><div class="cw-topline"><div class="cw-head-copy"><div class="cw-title-row"><div><div class="cw-title">Editor</div><div class="cw-sub">Edit your current state, tracker or cache</div></div></div></div><div class="cw-head-pills"><span class="cw-chip"><strong id="cw-pill-source">Current state</strong></span><span class="cw-chip"><strong id="cw-pill-kind">Watchlist</strong></span><span class="cw-chip"><strong id="cw-pill-count">0 rows</strong></span></div></div><div class="cw-wrap"><div class="cw-main"><div class="cw-controls"><input id="cw-filter" class="cw-input" placeholder="Filter by key / title / id..."><span class="cw-status-text" id="cw-status"></span><div class="cw-controls-spacer"></div><div class="cw-bulk" id="cw-bulk" style="display:none"><span class="cw-bulk-count" id="cw-bulk-count"></span><button id="cw-bulk-remove" class="cw-btn danger" type="button"></button><button id="cw-bulk-restore" class="cw-btn" type="button"></button><button id="cw-bulk-clear" class="cw-btn" type="button">Clear</button></div><button id="cw-reload" class="cw-btn" type="button">Reload</button><button id="cw-add" class="cw-btn" type="button">Add row</button><button id="cw-save" class="cw-btn primary" type="button">Save changes</button></div><div class="cw-table-wrap" id="cw-table-wrap"><table class="cw-table"><thead><tr><th style="width:34px"><input id="cw-select-page" class="cw-checkbox" type="checkbox" title="Select page"></th><th style="width:30px"></th><th style="width:12%" data-sort="key" class="sortable">Key</th><th style="width:13%" data-sort="type" class="sortable">Type</th><th style="width:33%" data-sort="title" class="sortable">Title</th><th style="width:84px">Year</th><th style="width:12%" id="cw-col-id-a">TMDB</th><th style="width:21%" data-sort="extra" class="sortable">Extra</th></tr></thead><tbody id="cw-tbody"></tbody></table></div><div class="cw-pager" id="cw-pager" style="display:none"><button id="cw-prev" class="cw-btn" type="button">Previous</button><span id="cw-page-info" class="cw-page-info"></span><button id="cw-next" class="cw-btn" type="button">Next</button></div><div class="cw-empty" id="cw-empty" style="display:none">No rows match this view.</div></div><aside class="cw-side"><div class="ins-card"><div class="ins-row"><div class="ins-icon"><span class="material-symbol">tune</span></div><div class="ins-title">Workspace</div></div><div class="ins-row"><div class="ins-kv" style="width:100%"><label>Source</label><select id="cw-source" class="cw-select"><option value="tracker">CW Tracker</option><option value="pair">Pair Cache</option><option value="state">Current State</option></select><label>Kind</label><select id="cw-kind" class="cw-select"><option value="watchlist">Watchlist</option><option value="history">History</option><option value="ratings">Ratings</option><option value="progress">Progress</option></select><label id="cw-pair-label" style="display:none">Pair</label><select id="cw-pair" class="cw-select" style="display:none"></select><label id="cw-snapshot-label">Snapshot</label><select id="cw-snapshot" class="cw-select"><option value="">Latest</option></select><label id="cw-instance-label" style="display:none">Profile</label><select id="cw-instance" class="cw-select" style="display:none"><option value="default">Default</option></select></div></div><div class="ins-row"><div class="ins-kv" style="width:100%"><div class="field-label">Types</div><div id="cw-type-filter" class="cw-type-filter"><button type="button" data-type="movie" class="cw-type-chip active">Movies</button><button type="button" data-type="show" class="cw-type-chip active">Shows</button><button type="button" data-type="anime" class="cw-type-chip active">Anime</button><button type="button" data-type="season" class="cw-type-chip active">Seasons</button><button type="button" data-type="episode" class="cw-type-chip active">Episodes</button><button type="button" id="cw-blocked-only" class="cw-type-chip">Blocked</button></div></div></div><div class="ins-row" id="cw-state-bulk" style="display:none"><details class="cw-collapse" id="cw-bulk-details" style="width:100%"><summary style="cursor:pointer;font-weight:700;user-select:none">Block rules</summary><div style="display:flex;flex-direction:column;gap:8px;width:100%;margin-top:10px"><select id="cw-bulk-type" class="cw-select" style="width:100%"></select><div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap"><button id="cw-bulk-block-type" class="cw-btn danger" type="button" style="flex:1 1 0;min-width:120px">Block all</button><button id="cw-bulk-unblock-type" class="cw-btn" type="button" style="flex:1 1 0;min-width:120px">Unblock all</button></div><div class="cw-status-text">Current State only • affects baseline items</div></div></details></div><div class="ins-row" id="cw-import-row" style="display:none"><details class="cw-collapse" id="cw-import-details" style="width:100%"><summary style="cursor:pointer;font-weight:700;user-select:none">Import provider state</summary><div style="display:flex;flex-direction:column;gap:10px;width:100%;margin-top:10px"><div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center"><select id="cw-import-provider" class="cw-select" style="flex:1;min-width:200px"></select><select id="cw-import-instance" class="cw-select" style="min-width:180px"></select><select id="cw-import-mode" class="cw-select" style="min-width:180px"><option value="replace">Replace baseline</option><option value="merge">Merge (keep old)</option></select></div><div style="display:flex;gap:12px;flex-wrap:wrap;align-items:center"><label id="cw-import-watchlist-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0"><input id="cw-import-watchlist" class="cw-checkbox" type="checkbox" checked>Watchlist </label><label id="cw-import-history-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0"><input id="cw-import-history" class="cw-checkbox" type="checkbox" checked>History </label><label id="cw-import-ratings-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0"><input id="cw-import-ratings" class="cw-checkbox" type="checkbox" checked>Ratings </label><label id="cw-import-progress-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0"><input id="cw-import-progress-cb" class="cw-checkbox" type="checkbox" checked>Progress </label><span style="flex:1 1 auto"></span><button id="cw-import-run" class="cw-btn sm" type="button">Import</button></div><div id="cw-import-progress" style="display:none"><div class="cw-progress"><span></span></div><div class="cw-status-text" id="cw-import-progress-text" style="margin-top:6px"></div></div></div></details></div></div><div class="ins-card"><div class="ins-row" style="align-items:center"><div class="ins-icon"><span class="material-symbol">insights</span></div><div class="ins-title" style="margin-right:auto">Pulse</div><span class="cw-tag" id="cw-tag-status"><span class="cw-tag-dot"></span><span id="cw-tag-label">Idle</span></span></div><div class="ins-row"><div class="ins-metrics"><div class="metric-row"><div class="metric"><span class="material-symbol">view_list</span><div><div class="m-val" id="cw-summary-total">0</div><div class="m-lbl">Total rows</div></div></div><div class="metric"><span class="material-symbol">visibility</span><div><div class="m-val" id="cw-summary-visible">0</div><div class="m-lbl">Rows visible</div></div></div></div><div class="metric-divider"></div><div class="metric-row"><div class="metric"><span class="material-symbol">movie</span><div><div class="m-val" id="cw-summary-movies">0</div><div class="m-lbl">Movies</div></div></div><div class="metric"><span class="material-symbol">monitoring</span><div><div class="m-val" id="cw-summary-shows">0</div><div class="m-lbl">Shows</div></div></div><div class="metric"><span class="material-symbol">layers</span><div><div class="m-val" id="cw-summary-seasons">0</div><div class="m-lbl">Seasons</div></div></div><div class="metric"><span class="material-symbol">live_tv</span><div><div class="m-val" id="cw-summary-episodes">0</div><div class="m-lbl">Episodes</div></div></div></div><div class="metric-divider"></div><div class="metric-row"><div class="metric"><span class="material-symbol">description</span><div><div class="m-val" id="cw-summary-state-files">0</div><div class="m-lbl">State files</div></div></div><div class="metric"><span class="material-symbol">folder_copy</span><div><div class="m-val" id="cw-summary-snapshots">0</div><div class="m-lbl">Snapshots</div></div></div></div><div id="cw-state-hint" class="cw-state-hint" style="display:none"><strong>No tracker data found.</strong> Run a CrossWatch sync with the tracker enabled once. After that, tracker state files and snapshots will appear here and you can edit them. </div></div></div></div><div class="ins-card" id="cw-backup-card"><div class="ins-row"><div class="ins-icon"><span class="material-symbol">backup</span></div><div class="ins-title">Archive</div></div><div class="ins-row"><div class="ins-kv" style="width:100%"><label>Export / Import</label><div class="cw-backup-actions"><button id="cw-download" class="cw-btn" type="button">Download ZIP</button><button id="cw-upload" class="cw-btn" type="button">Import file</button><input id="cw-upload-input" type="file" accept=".zip,.json" style="display:none"></div></div></div></div><div class="ins-card" id="cw-state-backup-card"><div class="ins-row"><div class="ins-icon"><span class="material-symbol">backup</span></div><div class="ins-title">Policy backup</div></div><div class="ins-row"><div class="ins-kv" style="width:100%"><label>Export / Import</label><div class="cw-backup-actions"><button id="cw-state-download" class="cw-btn" type="button">Download JSON</button><button id="cw-state-upload" class="cw-btn" type="button">Import file</button><input id="cw-state-upload-input" type="file" accept=".json" style="display:none"></div></div></div></div></aside></div></div>`;

  wireStaticLabels(host);

  host.querySelectorAll("input,select,textarea").forEach((field, idx) => {
    if (!field.name) field.name = field.id || `cw-field-${idx + 1}`;
  });

  const $ = id => document.getElementById(id);
  const pickEls = spec => Object.fromEntries(Object.entries(spec).map(([key, id]) => [key, $(id)]));
  const {
    sourceSel, kindSel, pairLabel, pairSel, snapLabel, snapSel, instanceLabel, instanceSel,
    filterInput, reloadBtn, addBtn, saveBtn, tbody, empty, statusEl, tag, tagLabel,
    summaryVisible, summaryTotal, summaryMovies, summaryShows, summarySeasons, summaryEpisodes,
    summaryStateFiles, summarySnapshots, stateHint, pager, prevBtn, nextBtn, pageInfo,
    typeFilterWrap, backupCard, blockedOnlyBtn, downloadBtn, uploadBtn, uploadInput,
    stateBackupCard, stateDownloadBtn, stateUploadBtn, stateUploadInput,
    pillSource, pillKind, pillCount,
    importRow, importProviderSel, importInstanceSel, importWatchlistCb, importHistoryCb,
    importRatingsCb, importProgressCb, importModeSel, importRunBtn, importWatchlistWrap,
    importHistoryWrap, importRatingsWrap, importProgressFeatWrap, importProgressWrap,
    importProgressText,
    selectPage, bulkWrap, bulkCount, bulkRemoveBtn, bulkRestoreBtn, bulkClearBtn,
    stateBulkRow, bulkTypeSel, bulkBlockTypeBtn, bulkUnblockTypeBtn,
  } = pickEls({
    sourceSel: "cw-source",
    kindSel: "cw-kind",
    pairLabel: "cw-pair-label",
    pairSel: "cw-pair",
    snapLabel: "cw-snapshot-label",
    snapSel: "cw-snapshot",
    instanceLabel: "cw-instance-label",
    instanceSel: "cw-instance",
    filterInput: "cw-filter",
    reloadBtn: "cw-reload",
    addBtn: "cw-add",
    saveBtn: "cw-save",
    tbody: "cw-tbody",
    empty: "cw-empty",
    statusEl: "cw-status",
    tag: "cw-tag-status",
    tagLabel: "cw-tag-label",
    summaryVisible: "cw-summary-visible",
    summaryTotal: "cw-summary-total",
    summaryMovies: "cw-summary-movies",
    summaryShows: "cw-summary-shows",
    summarySeasons: "cw-summary-seasons",
    summaryEpisodes: "cw-summary-episodes",
    summaryStateFiles: "cw-summary-state-files",
    summarySnapshots: "cw-summary-snapshots",
    stateHint: "cw-state-hint",
    pager: "cw-pager",
    prevBtn: "cw-prev",
    nextBtn: "cw-next",
    pageInfo: "cw-page-info",
    typeFilterWrap: "cw-type-filter",
    backupCard: "cw-backup-card",
    blockedOnlyBtn: "cw-blocked-only",
    downloadBtn: "cw-download",
    uploadBtn: "cw-upload",
    uploadInput: "cw-upload-input",
    stateBackupCard: "cw-state-backup-card",
    stateDownloadBtn: "cw-state-download",
    stateUploadBtn: "cw-state-upload",
    stateUploadInput: "cw-state-upload-input",
    pillSource: "cw-pill-source",
    pillKind: "cw-pill-kind",
    pillCount: "cw-pill-count",
    importRow: "cw-import-row",
    importProviderSel: "cw-import-provider",
    importInstanceSel: "cw-import-instance",
    importWatchlistCb: "cw-import-watchlist",
    importHistoryCb: "cw-import-history",
    importRatingsCb: "cw-import-ratings",
    importProgressCb: "cw-import-progress-cb",
    importModeSel: "cw-import-mode",
    importRunBtn: "cw-import-run",
    importWatchlistWrap: "cw-import-watchlist-wrap",
    importHistoryWrap: "cw-import-history-wrap",
    importRatingsWrap: "cw-import-ratings-wrap",
    importProgressFeatWrap: "cw-import-progress-wrap",
    importProgressWrap: "cw-import-progress",
    importProgressText: "cw-import-progress-text",
    selectPage: "cw-select-page",
    bulkWrap: "cw-bulk",
    bulkCount: "cw-bulk-count",
    bulkRemoveBtn: "cw-bulk-remove",
    bulkRestoreBtn: "cw-bulk-restore",
    bulkClearBtn: "cw-bulk-clear",
    stateBulkRow: "cw-state-bulk",
    bulkTypeSel: "cw-bulk-type",
    bulkBlockTypeBtn: "cw-bulk-block-type",
    bulkUnblockTypeBtn: "cw-bulk-unblock-type",
  });
  const sortHeaders = Array.from(host.querySelectorAll(".cw-table th[data-sort]"));
  const providerMeta = window.CW?.ProviderMeta || {};
  const providerKey = (name) => String(name || "").trim().toUpperCase();
  const providerLabel = (name, fallback = "") => {
    const key = providerKey(name);
    return providerMeta.label?.(key) || providerMeta.label?.(name) || fallback || String(name || "");
  };
  function syncProviderIconSelect(selectEl, show) {
    if (!selectEl) return;
    const helper = window.CW?.IconSelect?.enhance;
    const wrap = selectEl.nextElementSibling && selectEl.nextElementSibling.classList?.contains("cw-icon-select")
      ? selectEl.nextElementSibling
      : null;
    if (!show || typeof helper !== "function") {
      selectEl.classList.remove("cw-icon-select-native");
      if (wrap) wrap.style.display = "none";
      return;
    }
    helper(selectEl, {
      className: "cw-editor-icon-select",
      getOptionData: (value, option) => {
        const key = providerKey(value);
        const label = providerLabel(value, option?.textContent || value || "Select");
        const icon = providerMeta.logLogoPath?.(key) || providerMeta.logoPath?.(key) || providerMeta.logLogoPath?.(value) || providerMeta.logoPath?.(value) || "";
        return {
          label,
          icons: icon && value ? [{ src: icon, alt: label }] : [],
          disabled: !!option?.disabled,
        };
      },
    });
    const nextWrap = selectEl.nextElementSibling && selectEl.nextElementSibling.classList?.contains("cw-icon-select")
      ? selectEl.nextElementSibling
      : null;
    if (nextWrap) nextWrap.style.display = "";
  }

  let statusStickyUntil = 0;

  function syncHeaderPills(visible, total) {
    const srcMap = { tracker: "Tracker snapshots", pair: "Pair cache", state: "Current state" };
    const kindMap = { watchlist: "Watchlist", history: "History", ratings: "Ratings", progress: "Progress" };
    if (pillSource) pillSource.textContent = srcMap[state.source] || "Source";
    if (pillKind) pillKind.textContent = kindMap[state.kind] || "Kind";
    const all = typeof total === "number" ? total : ((state.rows && state.rows.length) || 0);
    const vis = typeof visible === "number" ? visible : all;
    if (pillCount) pillCount.textContent = all ? `${vis}/${all} rows` : "0 rows";
  }

  function setStatus(message) {
    if (!statusEl) return;
    statusEl.textContent = message || "";
  }

  function setStatusSticky(message, ms = 4000) {
    statusStickyUntil = Date.now() + ms;
    setStatus(message);
  }

  function setRowsStatus(message) {
    if (Date.now() < statusStickyUntil) return;
    setStatus(message);
  }

  if (filterInput && state.filter) filterInput.value = state.filter;

  function syncKindUI() {
    if (!kindSel) return;
    const allowed = ["watchlist", "history", "ratings", "progress"];
    if (!allowed.includes(state.kind)) state.kind = "watchlist";
    kindSel.value = state.kind;
  }

  function allowedTypesForKind(kind) {
    return kind === "watchlist"
      ? ["movie", "show", "anime"]
      : ["movie", "show", "anime", "season", "episode"];
  }
  function isAnilistMode() {
    return state.source === "state" && String(state.snapshot || "").trim().toUpperCase() === "ANILIST";
  }

  function syncIdColumnHeaders() {
    const a = $("cw-col-id-a");
    if (!a) return;
    a.textContent = isAnilistMode() ? "MAL" : "TMDB";
  }

  function enforceKindTypeRules() {
    const allowed = allowedTypesForKind(state.kind);
    for (const t of ["movie", "show", "anime", "season", "episode"]) {
      if (!allowed.includes(t)) state.typeFilter[t] = false;
      else if (typeof state.typeFilter[t] !== "boolean") state.typeFilter[t] = true;
    }
  }

  function syncTypeFilterUI() {
    if (!typeFilterWrap) return;
    enforceKindTypeRules();
    const allowed = allowedTypesForKind(state.kind);
    const buttons = typeFilterWrap.querySelectorAll("button[data-type]");
    buttons.forEach(btn => {
      const t = btn.dataset.type;
      const visible = allowed.includes(t);
      btn.style.display = visible ? "" : "none";
      const on = state.typeFilter[t] !== false;
      btn.classList.toggle("active", on);
    });
    if (blockedOnlyBtn) blockedOnlyBtn.classList.toggle("active", !!state.blockedOnly);
  }

  function syncStateBulkUI() {
    if (!stateBulkRow || !bulkTypeSel || !bulkBlockTypeBtn || !bulkUnblockTypeBtn) return;
    const show = state.source === "state" && state.kind !== "watchlist";
    stateBulkRow.style.display = show ? "" : "none";
    if (!show) return;

    const allowed = allowedTypesForKind(state.kind);
    const opts = allowed.map(t => ({ v: t, l: t.charAt(0).toUpperCase() + t.slice(1) }));
    const current = bulkTypeSel.value;
    bulkTypeSel.innerHTML = opts.map(o => `<option value="${o.v}">${o.l}</option>`).join("");
    if (opts.some(o => o.v === current)) bulkTypeSel.value = current;
    else bulkTypeSel.value = opts[0] ? opts[0].v : "movie";
  }

  function setImportBusy(on, message) {
    if (importProgressWrap) importProgressWrap.style.display = on ? "" : "none";
    if (importProgressText) importProgressText.textContent = message || "";
    const disabled = !!on;
    if (importRunBtn) importRunBtn.disabled = disabled;
    if (importProviderSel) importProviderSel.disabled = disabled;
    if (importModeSel) importModeSel.disabled = disabled;
    if (importWatchlistCb) importWatchlistCb.disabled = disabled || importWatchlistCb.disabled;
    if (importHistoryCb) importHistoryCb.disabled = disabled || importHistoryCb.disabled;
    if (importRatingsCb) importRatingsCb.disabled = disabled || importRatingsCb.disabled;
    if (importProgressCb) importProgressCb.disabled = disabled || importProgressCb.disabled;
  }

  
  function _escapeHtml(s) {
    return String(s || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  function renderInstanceOptions(selectEl, instances, current) {
    if (!selectEl) return "default";
    const list = Array.isArray(instances) ? instances : [];
    const norm = list
      .map(x => ({
        id: String((x && x.id) ? x.id : ""),
        label: String((x && x.label) ? x.label : (x && x.id) ? x.id : ""),
      }))
      .filter(x => x.id);

    if (!norm.some(x => x.id === "default")) norm.unshift({ id: "default", label: "Default" });

    const ids = norm.map(x => x.id);
    let next = String(current || "");
    if (!next || !ids.includes(next)) next = "default";
    const opts = norm.map(x => `<option value="${_escapeHtml(x.id)}">${_escapeHtml(x.label || x.id)}</option>`).join("");
    selectEl.innerHTML = opts;
    selectEl.value = next;
    selectEl.disabled = !ids.length;
    return next;
  }

  async function loadInstanceOptions(provider, selectEl, current) {
    if (!selectEl) return "default";
    if (!provider) {
      return renderInstanceOptions(selectEl, [{ id: "default", label: "Default" }], current);
    }
    try {
      const data = await fetchJSON(`/api/provider-instances/${encodeURIComponent(provider)}`);
      return renderInstanceOptions(selectEl, Array.isArray(data) ? data : [], current);
    } catch (_) {
      return renderInstanceOptions(selectEl, [{ id: "default", label: "Default" }], current);
    }
  }


  function syncImportUI() {
    if (!importRow) return;
    const show = state.source === "state" && state.importEnabled;
    importRow.style.display = show ? "" : "none";
    if (!show) return;

    if (importModeSel) importModeSel.value = state.importMode || "replace";

    const all = Array.isArray(state.importProviders) ? state.importProviders : [];
    const list = all.filter(p => p && p.configured && p.name);

    if (importProviderSel) {
      const current = importProviderSel.value || state.importProvider || "";
      const opts = list
        .map(p => {
          const name = p && p.name ? String(p.name) : "";
          const label = providerLabel(name, p && p.label ? String(p.label) : name);
          return `<option value="${name}">${label}</option>`;
        })
        .join("");

      importProviderSel.innerHTML = opts || `<option value="">No configured providers</option>`;

      const names = list.map(p => String(p.name));
      let next = current;

      if (!next || !names.includes(next)) {
        next = state.snapshot && names.includes(state.snapshot) ? state.snapshot : "";
      }
      if (!next) next = names[0] || "";

      state.importProvider = next;
      importProviderSel.value = next;
      importProviderSel.disabled = !names.length;
      syncProviderIconSelect(importProviderSel, true);
    }

    const sel = state.importProvider || (importProviderSel ? importProviderSel.value : "");
    const p = list.find(x => String((x || {}).name || "") === String(sel || ""));
    const feats = (p && p.features) ? p.features : {};

    if (importInstanceSel) {
      const ids = (p && Array.isArray(p.instances)) ? p.instances : ["default"];
      const instObjs = ids.map(x => ({ id: String(x), label: String(x) }));
      const nextInst = renderInstanceOptions(importInstanceSel, instObjs, state.importProviderInstance);
      if (nextInst !== state.importProviderInstance) {
        state.importProviderInstance = nextInst;
        persistUIState();
      }
      importInstanceSel.style.display = state.importProvider ? "" : "none";
    }

    const setCb = (wrap, cb, key) => {
      const supported = !!feats[key];
      if (wrap) wrap.style.display = supported ? "" : "none";
      if (!cb) return;
      cb.disabled = !supported;
      if (!supported) cb.checked = false;
      else if (state.importFeatures && typeof state.importFeatures[key] === "boolean") cb.checked = !!state.importFeatures[key];
    };

    setCb(importWatchlistWrap, importWatchlistCb, "watchlist");
    setCb(importHistoryWrap, importHistoryCb, "history");
    setCb(importRatingsWrap, importRatingsCb, "ratings");
    setCb(importProgressFeatWrap, importProgressCb, "progress");

    if (importRunBtn) importRunBtn.disabled = !state.importProvider;
  }

  async function loadImportProviders() {
    state.importEnabled = false;
    state.importProviders = [];
    if (!importRow) return;
    try {
      const data = await fetchJSON("/api/editor/state/import/providers");
      state.importEnabled = !!(data && data.enabled);
      state.importProviders = Array.isArray(data && data.providers) ? data.providers : [];
    } catch (e) {
      state.importEnabled = false;
      state.importProviders = [];
    }
    syncImportUI();
  }

  function _collectImportFeatures() {
    const feats = [];
    if (importWatchlistCb && importWatchlistCb.checked && !importWatchlistCb.disabled) feats.push("watchlist");
    if (importHistoryCb && importHistoryCb.checked && !importHistoryCb.disabled) feats.push("history");
    if (importRatingsCb && importRatingsCb.checked && !importRatingsCb.disabled) feats.push("ratings");
    if (importProgressCb && importProgressCb.checked && !importProgressCb.disabled) feats.push("progress");
    return feats;
  }

  async function runStateImport() {
    if (state.source !== "state") return;
    const provider = (importProviderSel ? importProviderSel.value : state.importProvider) || "";
    const features = _collectImportFeatures();
    const mode = (importModeSel ? importModeSel.value : state.importMode) || "replace";

    if (!provider) {
      setStatusSticky("Pick a provider first", 3000);
      return;
    }
    if (!features.length) {
      setStatusSticky("Pick at least one dataset", 3000);
      return;
    }

    state.importProvider = provider;
    state.importMode = mode;
    state.importFeatures = {
      watchlist: features.includes("watchlist"),
      history: features.includes("history"),
      ratings: features.includes("ratings"),
      progress: features.includes("progress"),
    };

    try {
      const msg = `Importing ${features.join(", ")} from ${provider}…`;
      setImportBusy(true, msg);
      setTag("warn", "Importing…");
      setStatus(msg);

      const res = await fetchJSON("/api/editor/state/import", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ provider, provider_instance: state.importProviderInstance || "default", features, mode }),
      });

      const featsOut = (res && res.features) ? res.features : {};
      const bits = [];
      let totalMs = 0;

      for (const k of Object.keys(featsOut)) {
        const r = featsOut[k] || {};
        if (r.skipped) continue;
        if (r.ok) bits.push(`${k}:${r.count}`);
        if (typeof r.elapsed_ms === "number") totalMs += r.elapsed_ms;
      }

      let done = "Imported " + (bits.length ? bits.join(" • ") : "done");
      if (totalMs) done += ` (${(totalMs / 1000).toFixed(1)}s)`;

      setTag("loaded", "Imported");
      setStatusSticky(done, 6000);
      if (window.cxToast) window.cxToast(done);

      state.snapshot = provider;
      state.instance = state.importProviderInstance || "default";
      persistUIState();
      await loadSnapshots();
      await loadState();
    } catch (e) {
      console.error(e);
      setTag("error", "Import failed");
      setStatus(String(e));
    } finally {
      setImportBusy(false, "");
      syncImportUI();
    }
  }


  syncKindUI();
  syncTypeFilterUI();
  syncStateBulkUI();

  function persistUIState() {
    try {
      if (typeof localStorage === "undefined") return;
      const data = {
        source: state.source,
        kind: state.kind,
        snapshot: state.snapshot,
        instance: state.instance,
        pair: state.pair,
        filter: state.filter,
        typeFilter: state.typeFilter,
        blockedOnly: state.blockedOnly,
        sortKey: state.sortKey,
        sortDir: state.sortDir,
      };
      localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
    } catch (_) {}
  }

  function syncBulkBar() {
    if (!bulkWrap || !bulkCount || !bulkRemoveBtn || !bulkRestoreBtn || !bulkClearBtn) return;
    const n = state.selected ? state.selected.size : 0;
    bulkWrap.style.display = n ? "flex" : "none";
    if (!n) return;
    bulkCount.textContent = `${n} selected`;
    if (state.source === "state") {
      bulkRemoveBtn.textContent = "Block selected";
      bulkRestoreBtn.textContent = "Unblock selected";
    } else {
      bulkRemoveBtn.textContent = "Delete selected";
      bulkRestoreBtn.textContent = "Restore selected";
    }
  }

  function clearSelection() {
    if (!state.selected) state.selected = new Set();
    state.selected.clear();
    syncBulkBar();
  }

  function syncSelectPageCheckbox() {
    if (!selectPage) return;
    const rids = Array.isArray(state.pageRids) ? state.pageRids : [];
    if (!rids.length) {
      selectPage.checked = false;
      selectPage.indeterminate = false;
      return;
    }
    const sel = state.selected || new Set();
    const all = rids.every(r => sel.has(r));
    const any = rids.some(r => sel.has(r));
    selectPage.checked = all;
    selectPage.indeterminate = any && !all;
  }

  function bulkSetDeletedForSelected(flag) {
    const sel = state.selected || new Set();
    if (!sel.size) return;
    let changed = 0;
    for (const row of state.rows || []) {
      if (!sel.has(row._rid)) continue;
      if (row.deleted !== flag) {
        row.deleted = flag;
        changed += 1;
      }
    }
    clearSelection();
    if (changed) {
      markChanged();
      renderRows();
      const verb = flag
        ? state.source === "state"
          ? "Blocked"
          : "Deleted"
        : state.source === "state"
          ? "Unblocked"
          : "Restored";
      setStatusSticky(`${verb} ${changed} item${changed === 1 ? "" : "s"}`, 3000);
    }
  }

  function bulkSetBlocksByType(type, flag) {
    if (state.source !== "state") return;
    const t = String(type || "").toLowerCase();
    if (!t) return;
    let changed = 0;
    for (const row of state.rows || []) {
      if (row._origin !== "baseline") continue;
      if (((row.type || "") + "").toLowerCase() !== t) continue;
      if (row.deleted !== flag) {
        row.deleted = flag;
        changed += 1;
      }
    }
    clearSelection();
    if (changed) {
      markChanged();
      renderRows();
      setStatusSticky(
        `${flag ? "Blocked" : "Unblocked"} ${changed} ${t} item${changed === 1 ? "" : "s"}`,
        3500
      );
    }
  }

  function syncSourceUI() {
    const isState = state.source === "state";
    const isPair = state.source === "pair";
    if (sourceSel) sourceSel.value = state.source;
    if (pairLabel) pairLabel.style.display = isPair ? "" : "none";
    if (pairSel) pairSel.style.display = isPair ? "" : "none";
    if (snapLabel) snapLabel.textContent = isState ? "Provider" : isPair ? "Dataset" : "Snapshot";
    if (instanceLabel) instanceLabel.style.display = isState ? "" : "none";
    if (instanceSel) instanceSel.style.display = isState ? "" : "none";
    if (backupCard) backupCard.style.display = isState ? "none" : "";
    if (stateBackupCard) stateBackupCard.style.display = isState ? "" : "none";
    if (blockedOnlyBtn) blockedOnlyBtn.style.display = isState ? "" : "none";

    if (!isState && state.instance && state.instance !== "default") {
      state.instance = "default";
      persistUIState();
    }

    if (!isState && state.blockedOnly) {
      state.blockedOnly = false;
      syncTypeFilterUI();
      persistUIState();
    }
    syncStateBulkUI();
    syncImportUI();
    syncHeaderPills();
  }

  function showStateHint(mode) {
    if (!stateHint) return;
    if (mode === "tracker") {
      stateHint.innerHTML =
        "<strong>No tracker data found.</strong> Run a CrossWatch sync with the tracker enabled once. After that, tracker state files and snapshots will appear here and you can edit them.";
      stateHint.style.display = "block";
      return;
    }
    if (mode === "pair") {
      stateHint.innerHTML =
        "<strong>No pair cache found.</strong> Run a CrossWatch sync once to generate .cw_state pair indexes. Then select a Pair and Dataset here.";
      stateHint.style.display = "block";
      return;
    }
    if (mode === "state") {
      stateHint.innerHTML =
        "<strong>No state.json found.</strong> Run a CrossWatch sync once to generate it. After that, your manual adds and blocks will show up here.";
      stateHint.style.display = "block";
      return;
    }
    stateHint.style.display = "none";
  }

  function setTag(mode, label) {
    if (!tag || !tagLabel) return;
    tag.classList.remove("warn", "error", "loaded");
    if (mode === "warn") tag.classList.add("warn");
    else if (mode === "error") tag.classList.add("error");
    else if (mode === "loaded") tag.classList.add("loaded");
    tagLabel.textContent = label;
  }

  function markChanged() {
    state.hasChanges = true;
    setTag("warn", "Unsaved changes");
  }

  let activePopup = null;

  function closePopup() {
    if (!activePopup) return;
    document.removeEventListener("mousedown", activePopup.onDoc);
    document.removeEventListener("keydown", activePopup.onKey);
    if (activePopup.node && activePopup.node.parentNode) {
      activePopup.node.parentNode.removeChild(activePopup.node);
    }
    activePopup = null;
  }

  function positionPopup(pop, anchor) {
    const rect = anchor.getBoundingClientRect();
    const margin = 8;
    const viewportWidth = document.documentElement.clientWidth;
    const viewportHeight = document.documentElement.clientHeight;
    let left = rect.left + window.scrollX;
    let top = rect.bottom + margin + window.scrollY;
    const width = pop.offsetWidth;
    const height = pop.offsetHeight;
    if (left + width + margin > window.scrollX + viewportWidth) {
      left = window.scrollX + viewportWidth - width - margin;
    }
    if (top + height + margin > window.scrollY + viewportHeight) {
      top = rect.top + window.scrollY - height - margin;
    }
    if (left < margin) left = margin;
    if (top < margin) top = margin;
    pop.style.left = left + "px";
    pop.style.top = top + "px";
  }

  function openPopup(anchor, builder) {
    closePopup();
    const pop = document.createElement("div");
    pop.className = "cw-pop";
    document.body.appendChild(pop);

    function doClose() {
      closePopup();
    }

    builder(pop, doClose);
    positionPopup(pop, anchor);

    const onDoc = ev => {
      if (pop.contains(ev.target) || anchor.contains(ev.target)) return;
      closePopup();
    };
    const onKey = ev => {
      if (ev.key === "Escape") closePopup();
    };
    activePopup = { node: pop, onDoc, onKey };
    document.addEventListener("mousedown", onDoc);
    document.addEventListener("keydown", onKey);
  }

  function formatHistoryLabel(iso) {
    if (!iso) return "";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    const pad = n => String(n).padStart(2, "0");
    return (
      d.getFullYear() +
      "-" +
      pad(d.getMonth() + 1) +
      "-" +
      pad(d.getDate()) +
      " " +
      pad(d.getHours()) +
      ":" +
      pad(d.getMinutes())
    );
  }
  function formatSxxEyy(season, episode) {
    const s = season == null ? NaN : parseInt(String(season), 10);
    if (!Number.isFinite(s)) return "";
    const pad = n => String(n).padStart(2, "0");
    const e = episode == null ? NaN : parseInt(String(episode), 10);
    if (Number.isFinite(e)) return `S${pad(s)}E${pad(e)}`;
    return `S${pad(s)}`;
  }



  function formatMs(ms) {
    const n = ms == null ? NaN : Number(ms);
    if (!Number.isFinite(n) || n <= 0) return "";
    const total = Math.floor(n / 1000);
    const pad = x => String(x).padStart(2, "0");
    const h = Math.floor(total / 3600);
    const m = Math.floor((total % 3600) / 60);
    const s = total % 60;
    if (h > 0) return `${h}:${pad(m)}:${pad(s)}`;
    return `${m}:${pad(s)}`;
  }

  function parseTimeToMs(v) {
    const s = (v == null ? "" : String(v)).trim();
    if (!s) return null;

    const lower = s.toLowerCase();
    if (lower.endsWith("ms")) {
      const num = parseFloat(lower.slice(0, -2));
      return Number.isFinite(num) ? Math.max(0, Math.floor(num)) : null;
    }

    if (s.includes(":")) {
      const parts = s.split(":").map(p => p.trim()).filter(Boolean);
      if (!parts.length) return null;
      const nums = parts.map(x => parseInt(x, 10));
      if (nums.some(n => !Number.isFinite(n))) return null;

      let sec = 0;
      if (nums.length === 3) sec = nums[0] * 3600 + nums[1] * 60 + nums[2];
      else if (nums.length === 2) sec = nums[0] * 60 + nums[1];
      else sec = nums[0];
      return Math.max(0, sec * 1000);
    }

    const num = parseFloat(s);
    if (!Number.isFinite(num)) return null;
    // Heuristic: large numbers are probably milliseconds.
    if (num >= 100000) return Math.max(0, Math.floor(num));
    return Math.max(0, Math.floor(num * 1000));
  }

  function appendPopupTitle(pop, text, marginTop = "") {
    const title = document.createElement("div");
    title.className = "cw-pop-title";
    title.textContent = text;
    if (marginTop) title.style.marginTop = marginTop;
    pop.appendChild(title);
  }

  function appendPopupActions(pop, defs) {
    const actions = document.createElement("div");
    actions.className = "cw-pop-actions";
    defs.forEach(def => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = `cw-pop-btn${def.kind ? ` ${def.kind}` : ""}`;
      btn.textContent = def.label;
      btn.onclick = def.onClick;
      actions.appendChild(btn);
    });
    pop.appendChild(actions);
  }

  function renderLockedPopup(pop, close) {
    const status = document.createElement("div");
    status.className = "cw-search-status";
    status.textContent = "Baseline rows are read-only. Block the row to exclude it.";
    pop.appendChild(status);
    appendPopupActions(pop, [{ label: "Close", kind: "primary", onClick: close }]);
  }

  function fillDateTimeInputs(iso, dateInput, timeInput) {
    if (!iso) return;
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return;
    const pad = n => String(n).padStart(2, "0");
    dateInput.value = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
    timeInput.value = `${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }

  function dateTimeInputsToIso(dateValue, timeValue) {
    if (!dateValue) return null;
    const parts = dateValue.split("-");
    const y = parseInt(parts[0], 10);
    const m = parseInt(parts[1], 10);
    const dDay = parseInt(parts[2], 10);
    const [hhRaw, mmRaw] = (timeValue || "").split(":");
    const hh = parseInt(hhRaw, 10) || 0;
    const mm = parseInt(mmRaw, 10) || 0;
    return new Date(y, m - 1, dDay, hh, mm, 0).toISOString().replace(/\.\d{3}Z$/, ".000Z");
  }

  function finishExtraChange(row, displayEl, close) {
    updateExtraDisplay(row, displayEl);
    markChanged();
    close();
  }

  function finishPopupChange(close, rerender = false) {
    markChanged();
    close();
    if (rerender) renderRows();
  }

  function updateExtraDisplay(row, el) {
    let label = "";
    let placeholder = "";
    let icon = "";
    if (state.kind === "ratings") {
      icon = "star";
      const r = row.raw && row.raw.rating;
      if (r == null || r === "") placeholder = "Set rating";
      else label = String(r) + "/10";
    } else if (state.kind === "history") {
      icon = "schedule";
      const w = row.raw && row.raw.watched_at;
      if (!w) placeholder = "Set time";
      else label = formatHistoryLabel(w);
    } else if (state.kind === "progress") {
      icon = "play_circle";
      const p = row.raw && row.raw.progress_ms;
      const d = row.raw && row.raw.duration_ms;
      const pm = p == null ? NaN : Number(p);
      const dm = d == null ? NaN : Number(d);
      if (!Number.isFinite(pm) || pm <= 0) placeholder = "Set progress";
      else {
        const left = formatMs(pm);
        const right = Number.isFinite(dm) && dm > 0 ? formatMs(dm) : "";
        label = right ? `${left} / ${right}` : left;
      }
    } else {
      placeholder = "";
    }

    el.innerHTML = "";
    const text = document.createElement("span");
    text.className = "cw-extra-display-label";
    if (label) {
      text.textContent = label;
      text.classList.add("cw-extra-display-value");
    } else {
      text.textContent = placeholder || "";
      text.classList.add("cw-extra-display-placeholder");
    }
    el.appendChild(text);

    if (icon) {
      const iconEl = document.createElement("span");
      iconEl.className = "material-symbol cw-extra-display-icon";
      iconEl.textContent = icon;
      el.appendChild(iconEl);
    }
  }

  function updateTypeDisplay(row, el) {
    let label = "";
    let icon = "category";
    const t = (row.type || "").toLowerCase();
    if (t === "movie") {
      label = "Movie";
      icon = "movie";
    } else if (t === "show") {
      label = "Show";
      icon = "monitoring";
    } else if (t === "anime") {
      label = "Anime";
      icon = "auto_awesome";
    } else if (t === "season") {
      label = "Season";
      icon = "layers";
    } else if (t === "episode") {
      label = "Episode";
      icon = "live_tv";
    }

    el.innerHTML = "";
    const text = document.createElement("span");
    text.className = "cw-extra-display-label";
    if (label) {
      text.textContent = label;
      text.classList.add("cw-extra-display-value");
    } else {
      text.textContent = "Set type";
      text.classList.add("cw-extra-display-placeholder");
    }
    el.appendChild(text);

    const iconEl = document.createElement("span");
    iconEl.className = "material-symbol cw-extra-display-icon";
    iconEl.textContent = icon;
    el.appendChild(iconEl);
  }

  function imdbFromKey(key) {
    const s = (key || "") + "";
    if (!s.startsWith("imdb:")) return "";
    return s.slice(5).split("#")[0];
  }

  function buildRows(items) {
    const rows = [];
    for (const [key, raw] of Object.entries(items || {})) {
      const ids = raw.ids || {};
      const showIds = raw.show_ids || {};
      const type = raw.type || "";
      const isEpisode = type === "episode";
      const baseTitle = raw.title || raw.series_title || "";
      rows.push({
        _rid: state.ridSeq++,
        key,
        type,
        title: baseTitle,
        year: raw.year != null ? String(raw.year) : "",
        imdb: ids.imdb || (type === "season" ? showIds.imdb || imdbFromKey(key) : ""),
        tmdb: ids.tmdb || showIds.tmdb || "",
        trakt: ids.trakt || showIds.trakt || "",
        mal: ids.mal || "",
        anilist: ids.anilist || "",
        raw: JSON.parse(JSON.stringify(raw)),
        deleted: false,
        episode: isEpisode,
      });
    }
    rows.sort((a, b) => (a.title || "").localeCompare(b.title || ""));
    return rows;
  }

  function applyFilter(rows) {
    const q = (state.filter || "").trim().toLowerCase();
    const filters = state.typeFilter || {};
    const hasTypeFilter = filters.movie || filters.show || filters.anime || filters.season || filters.episode;

    return rows.filter(r => {
      if (hasTypeFilter) {
        const t = (r.type || "").toLowerCase();
        const known = t === "movie" || t === "show" || t === "anime" || t === "season" || t === "episode";
        let allowed = true;
        if (known) {
          if (t === "movie") allowed = !!filters.movie;
          else if (t === "show") allowed = !!filters.show;
          else if (t === "anime") allowed = !!filters.anime;
          else if (t === "season") allowed = !!filters.season;
          else if (t === "episode") allowed = !!filters.episode;
        }
        if (!allowed) return false;
      }

      if (state.blockedOnly && state.source === "state") {
        if (!(r.deleted && r._origin === "baseline")) return false;
      }

      if (!q) return true;

      const parts = [
        r.key,
        r.title,
        r.type,
        r.year,
        r.imdb,
        r.tmdb,
        r.trakt,
        r.mal,
        r.anilist,
        r.raw && r.raw.series_title ? r.raw.series_title : "",
      ]
        .join(" ")
        .toLowerCase();

      return parts.includes(q);
    });
  }

  function openHistoryEditor(row, anchor, displayEl) {
    const locked = false;

    openPopup(anchor, (pop, close) => {
      appendPopupTitle(pop, "Watched at");
      if (locked) return renderLockedPopup(pop, close);

      const grid = document.createElement("div");
      grid.className = "cw-datetime-grid";

      const dateInput = document.createElement("input");
      dateInput.type = "date";

      const timeInput = document.createElement("input");
      timeInput.type = "time";
      timeInput.step = 60;

      fillDateTimeInputs(row.raw && row.raw.watched_at, dateInput, timeInput);

      grid.appendChild(dateInput);
      grid.appendChild(timeInput);
      pop.appendChild(grid);

      appendPopupActions(pop, [
        { label: "Clear", kind: "ghost", onClick: () => { row.raw.watched_at = null; finishExtraChange(row, displayEl, close); } },
        { label: "Close", kind: "ghost", onClick: close },
        { label: "Save", kind: "primary", onClick: () => { row.raw.watched_at = dateTimeInputsToIso(dateInput.value, timeInput.value); finishExtraChange(row, displayEl, close); } },
      ]);

      dateInput.focus();
    });
  }


  function openProgressEditor(row, anchor, displayEl) {
    const locked = false;

    openPopup(anchor, (pop, close) => {
      appendPopupTitle(pop, "Progress");
      if (locked) return renderLockedPopup(pop, close);

      const grid = document.createElement("div");
      grid.className = "cw-datetime-grid";
      grid.style.gridTemplateColumns = "minmax(0,1fr) minmax(0,1fr)";

      const posInput = document.createElement("input");
      posInput.type = "text";
      posInput.placeholder = "Position (mm:ss)";
      const curPos = row.raw && row.raw.progress_ms;
      const curDur = row.raw && row.raw.duration_ms;
      if (curPos != null) posInput.value = formatMs(curPos);

      const durInput = document.createElement("input");
      durInput.type = "text";
      durInput.placeholder = "Duration (mm:ss)";
      if (curDur != null) durInput.value = formatMs(curDur);

      grid.appendChild(posInput);
      grid.appendChild(durInput);
      pop.appendChild(grid);

      appendPopupTitle(pop, "Updated at", "10px");

      const whenGrid = document.createElement("div");
      whenGrid.className = "cw-datetime-grid";

      const dateInput = document.createElement("input");
      dateInput.type = "date";

      const timeInput = document.createElement("input");
      timeInput.type = "time";
      timeInput.step = 60;

      fillDateTimeInputs(row.raw && row.raw.progress_at, dateInput, timeInput);

      whenGrid.appendChild(dateInput);
      whenGrid.appendChild(timeInput);
      pop.appendChild(whenGrid);

      appendPopupActions(pop, [
        {
          label: "Clear",
          kind: "ghost",
          onClick: () => {
            row.raw.progress_ms = null;
            row.raw.duration_ms = null;
            row.raw.progress_at = null;
            finishExtraChange(row, displayEl, close);
          }
        },
        { label: "Close", kind: "ghost", onClick: close },
        {
          label: "Save",
          kind: "primary",
          onClick: () => {
        const posMs = parseTimeToMs(posInput.value);
        const durMs = parseTimeToMs(durInput.value);

        row.raw.progress_ms = posMs == null || posMs <= 0 ? null : posMs;
        row.raw.duration_ms = durMs == null || durMs <= 0 ? null : durMs;
        row.raw.progress_at = dateTimeInputsToIso(dateInput.value, timeInput.value);
        if (!row.raw.progress_at && row.raw.progress_ms != null) row.raw.progress_at = new Date().toISOString().replace(/\.\d{3}Z$/, ".000Z");
        finishExtraChange(row, displayEl, close);
          }
        },
      ]);

      posInput.focus();
    });
  }

  function openRatingEditor(row, anchor, displayEl) {
    const locked = false;

    openPopup(anchor, (pop, close) => {
      appendPopupTitle(pop, "Rating");
      if (locked) return renderLockedPopup(pop, close);

      const grid = document.createElement("div");
      grid.className = "cw-rating-grid";
      const current = row.raw && row.raw.rating != null ? Number(row.raw.rating) : null;

      for (let i = 1; i <= 10; i += 1) {
        const pill = document.createElement("button");
        pill.type = "button";
        pill.className = "cw-rating-pill" + (current === i ? " active" : "");
        pill.textContent = String(i);
        pill.onclick = () => {
          row.raw.rating = i;
          finishExtraChange(row, displayEl, close);
        };
        grid.appendChild(pill);
      }

      pop.appendChild(grid);
      appendPopupActions(pop, [
        { label: "Clear", kind: "ghost", onClick: () => { row.raw.rating = null; finishExtraChange(row, displayEl, close); } },
        { label: "Close", kind: "ghost", onClick: close },
      ]);
    });
  }

  function openTitleSearchEditor(row, anchor, refs) {
    openPopup(anchor, (pop, close) => {
      const title = document.createElement("div");
      title.className = "cw-pop-title";
      title.textContent = "Search metadata";
      pop.appendChild(title);

      const bar = document.createElement("div");
      bar.className = "cw-search-bar";

      const qInput = document.createElement("input");
      qInput.type = "text";
      qInput.id = "cw_meta_search_title";
      qInput.name = qInput.id;
      qInput.placeholder = "Title...";
      qInput.value = row.title || "";
      bar.appendChild(qInput);

      const yearInput = document.createElement("input");
      yearInput.type = "number";
      yearInput.id = "cw_meta_search_year";
      yearInput.name = yearInput.id;
      yearInput.placeholder = "Year";
      if (row.year) yearInput.value = row.year;
      bar.appendChild(yearInput);

      const typeSelect = document.createElement("select");
      typeSelect.id = "cw_meta_search_type";
      typeSelect.name = typeSelect.id;
      [["movie", "Movie"], ["show", "Show"], ["anime", "Anime"]].forEach(([val, label]) => {
        const opt = document.createElement("option");
        opt.value = val;
        opt.textContent = label;
        typeSelect.appendChild(opt);
      });
      typeSelect.value = row.type === "anime" ? "anime" : row.type === "show" || row.type === "episode" ? "show" : "movie";
      bar.appendChild(typeSelect);

      pop.appendChild(bar);

      const actions = document.createElement("div");
      actions.className = "cw-pop-actions";

      const searchBtn = document.createElement("button");
      searchBtn.type = "button";
      searchBtn.className = "cw-pop-btn primary";
      searchBtn.textContent = "Search";
      actions.appendChild(searchBtn);

      const closeBtn = document.createElement("button");
      closeBtn.type = "button";
      closeBtn.className = "cw-pop-btn ghost";
      closeBtn.textContent = "Close";
      closeBtn.onclick = close;
      actions.appendChild(closeBtn);

      pop.appendChild(actions);

      const status = document.createElement("div");
      status.className = "cw-search-status";
      pop.appendChild(status);

      const resultsBox = document.createElement("div");
      resultsBox.className = "cw-search-results";
      pop.appendChild(resultsBox);

      async function doSearch() {
        const q = (qInput.value || "").trim();
        const yearVal = parseInt(yearInput.value || "", 10);
        if (q.length < 2) {
          status.textContent = "Type at least 2 characters.";
          resultsBox.innerHTML = "";
          return;
        }
        const typ = String(typeSelect.value || "").toLowerCase();
        const makeUrl = t => {
          let u = `/api/metadata/search?q=${encodeURIComponent(q)}&typ=${encodeURIComponent(t)}`;
          if (!Number.isNaN(yearVal)) u += `&year=${yearVal}`;
          return u;
        };

        status.textContent = "Searching...";
        resultsBox.innerHTML = "";
        try {
          let items = [];
          if (typ === "anime") {
            const [showRes, movieRes] = await Promise.all([fetchJSON(makeUrl("show")), fetchJSON(makeUrl("movie"))]);

            const showOk = !!(showRes && showRes.ok !== false);
            const movieOk = !!(movieRes && movieRes.ok !== false);

            if (!showOk && !movieOk) {
              const msg = (showRes && showRes.error) || (movieRes && movieRes.error) || "Search failed.";
              status.textContent = msg;
              return;
            }

            const a = showOk && Array.isArray(showRes.results) ? showRes.results : [];
            const b = movieOk && Array.isArray(movieRes.results) ? movieRes.results : [];

            items = [...a.map(x => ({ ...x, _resolve_entity: "show" })), ...b.map(x => ({ ...x, _resolve_entity: "movie" }))];

            const seen = new Set();
            items = items.filter(it => {
              const k = `${String(it.tmdb || "")}:${String(it.type || "")}`;
              if (!k || seen.has(k)) return false;
              seen.add(k);
              return true;
            });
          } else {
            const data = await fetchJSON(makeUrl(typ));
            if (!data || data.ok === false) {
              status.textContent = data && data.error ? data.error : "Search failed.";
              return;
            }
            items = Array.isArray(data.results) ? data.results : [];
          }
          if (!items.length) {
            resultsBox.innerHTML = '<div class="cw-search-empty">No results.</div>';
            status.textContent = "";
            return;
          }

          resultsBox.innerHTML = "";
          items.forEach(item => {
            const btn = document.createElement("button");
            btn.type = "button";
            btn.className = "cw-search-item";

            const posterWrap = document.createElement("div");
            posterWrap.className = "cw-search-poster";

            if (item.poster_path) {
              const img = document.createElement("img");
              img.src = `/art/tmdb/${item.type === "show" ? "tv" : "movie"}/${encodeURIComponent(String(item.tmdb))}?size=w92`;
              img.alt = "";
              img.onerror = () => {
                img.remove();
                const ph = document.createElement("div");
                ph.className = "cw-search-poster-placeholder";
                ph.textContent = item.type === "show" ? "TV" : "MOV";
                posterWrap.appendChild(ph);
              };
              posterWrap.appendChild(img);
            } else {
              const ph = document.createElement("div");
              ph.className = "cw-search-poster-placeholder";
              ph.textContent = item.type === "show" ? "TV" : "MOV";
              posterWrap.appendChild(ph);
            }

            btn.appendChild(posterWrap);

            const content = document.createElement("div");
            content.className = "cw-search-content";

            const titleLine = document.createElement("div");
            titleLine.className = "cw-search-title-line";

            const t = document.createElement("div");
            t.className = "cw-search-title";
            const yearTxt = item.year ? ` (${item.year})` : "";
            t.textContent = (item.title || "") + yearTxt;
            titleLine.appendChild(t);

            const tag2 = document.createElement("span");
            tag2.className = "cw-search-tag";
            tag2.textContent = item.type === "show" ? "Show" : "Movie";
            titleLine.appendChild(tag2);

            content.appendChild(titleLine);

            const meta = document.createElement("div");
            meta.className = "cw-search-meta";
            const bits = [];
            if (item.year) bits.push(String(item.year));
            bits.push(item.type === "show" ? "TV" : "Movie");
            if (item.tmdb) bits.push(`TMDb ${item.tmdb}`);
            meta.textContent = bits.join(" - ");
            content.appendChild(meta);

            if (item.overview) {
              const ov = document.createElement("div");
              ov.className = "cw-search-overview";
              ov.textContent = item.overview;
              content.appendChild(ov);
            }

            btn.appendChild(content);

            btn.onclick = async () => {
              const picked = item;
              const newTitle = picked.title || row.title || "";
              row.title = newTitle;
              row.raw.title = newTitle || null;
              refs.titleIn.value = newTitle;

              if (picked.year) {
                row.year = String(picked.year);
                row.raw.year = picked.year;
                refs.yearIn.value = row.year;
              }

              const wantsAnime = String(typeSelect.value || "").toLowerCase() === "anime";
              const pickedType = String(picked.type || "movie").toLowerCase();

              const newType = wantsAnime ? "anime" : pickedType;
              const resolveEntity = wantsAnime ? picked._resolve_entity || pickedType || "movie" : newType;

              row.type = newType;
              row.raw.type = newType;
              row.episode = false;
              updateTypeDisplay(row, refs.typeBtn);

              const tmdbId = picked.tmdb;
              if (tmdbId != null) {
                const tmdbStr = String(tmdbId);
                row.tmdb = tmdbStr;
                row.raw.ids = row.raw.ids || {};
                row.raw.ids.tmdb = tmdbId;
                if (refs.tmdbIn) refs.tmdbIn.value = tmdbStr;
                const prevKey = (row.key || "").trim();
                if (!prevKey || /^(tmdb|imdb|trakt|tvdb|slug):/i.test(prevKey)) {
                  row.key = `tmdb:${tmdbStr}`;
                  if (refs.keyIn) refs.keyIn.value = row.key;
                }
              }

              if (tmdbId != null) {
                try {
                  const metaRes = await fetchJSON("/api/metadata/resolve", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ entity: resolveEntity, ids: { tmdb: tmdbId } }),
                  });

                  if (metaRes && metaRes.ok && metaRes.result && metaRes.result.ids) {
                    const ids = metaRes.result.ids || {};
                    row.raw.ids = row.raw.ids || {};

                    if (ids.imdb) {
                      row.imdb = ids.imdb;
                      row.raw.ids.imdb = ids.imdb;
                      refs.imdbIn.value = ids.imdb;
                    }
                    if (ids.tmdb) {
                      const tVal = String(ids.tmdb);
                      row.tmdb = tVal;
                      row.raw.ids.tmdb = ids.tmdb;
                      if (refs.tmdbIn) refs.tmdbIn.value = tVal;
                      const prevKey = (row.key || "").trim();
                      if (!prevKey || /^(tmdb|imdb|trakt|tvdb|slug):/i.test(prevKey)) {
                        row.key = `tmdb:${tVal}`;
                        if (refs.keyIn) refs.keyIn.value = row.key;
                      }
                    }
                    if (ids.trakt) {
                      const trVal = String(ids.trakt);
                      row.trakt = trVal;
                      row.raw.ids.trakt = ids.trakt;
                      if (refs.traktIn) refs.traktIn.value = trVal;
                    }
                  }
                } catch (err) {
                  console.error("metadata resolve failed", err);
                }
              }

              markChanged();
              setStatusSticky("Row updated from metadata", 2500);
              close();
              renderRows();
            };

            resultsBox.appendChild(btn);
          });

          status.textContent = `${items.length} result${items.length === 1 ? "" : "s"} found.`;
        } catch (err) {
          console.error("search failed", err);
          status.textContent = "Search failed.";
        }
      }

      searchBtn.onclick = () => doSearch();

      qInput.addEventListener("keydown", ev => {
        if (ev.key === "Enter") {
          ev.preventDefault();
          doSearch();
        }
      });

      if ((row.title || "").trim().length >= 3) doSearch();
      else status.textContent = "Enter a title and press Enter or Search.";
    });
  }

  function openTypeEditor(row, anchor) {
    const locked = false;

    openPopup(anchor, (pop, close) => {
      appendPopupTitle(pop, "Type");
      if (locked) return renderLockedPopup(pop, close);

      const grid = document.createElement("div");
      grid.className = "cw-type-grid";
      const current = (row.type || "").toLowerCase();
      const allowed = allowedTypesForKind(state.kind);
      const options = [
        { key: "movie", label: "Movie" },
        { key: "show", label: "Show" },
        { key: "anime", label: "Anime" },
        { key: "season", label: "Season" },
        { key: "episode", label: "Episode" },
      ].filter(o => allowed.includes(o.key));

      options.forEach(opt => {
        const pill = document.createElement("button");
        pill.type = "button";
        pill.className = "cw-type-pill" + (current === opt.key ? " active" : "");
        pill.textContent = opt.label;
        pill.onclick = () => {
          row.type = opt.key;
          row.raw.type = opt.key;
          row.episode = opt.key === "episode";
          finishPopupChange(close, true);
        };
        grid.appendChild(pill);
      });

      pop.appendChild(grid);
      appendPopupActions(pop, [
        {
          label: "Clear",
          kind: "ghost",
          onClick: () => {
            row.type = "";
            row.raw.type = null;
            row.episode = false;
            finishPopupChange(close, true);
          }
        },
        { label: "Close", kind: "ghost", onClick: close },
      ]);
    });
  }

  function compareValues(aVal, bVal) {
    if (typeof aVal === "number" && typeof bVal === "number") {
      if (aVal < bVal) return -1;
      if (aVal > bVal) return 1;
      return 0;
    }
    const aStr = aVal == null ? "" : String(aVal).toLowerCase();
    const bStr = bVal == null ? "" : String(bVal).toLowerCase();
    if (aStr < bStr) return -1;
    if (aStr > bStr) return 1;
    return 0;
  }

  function sortRows(rows) {
    const key = state.sortKey;
    const dir = state.sortDir === "desc" ? -1 : 1;
    if (!key) return rows;
    return rows.slice().sort((a, b) => {
      let av;
      let bv;
      if (key === "title") {
        av = a.title || "";
        bv = b.title || "";
      } else if (key === "type") {
        av = a.type || "";
        bv = b.type || "";
      } else if (key === "key") {
        av = a.key || "";
        bv = b.key || "";
      } else if (key === "extra") {
        if (state.kind === "ratings") {
          av = a.raw && a.raw.rating != null ? Number(a.raw.rating) : -Infinity;
          bv = b.raw && b.raw.rating != null ? Number(b.raw.rating) : -Infinity;
        } else if (state.kind === "history") {
          const aw = a.raw && a.raw.watched_at;
          const bw = b.raw && b.raw.watched_at;
          av = aw ? Date.parse(aw) || 0 : 0;
          bv = bw ? Date.parse(bw) || 0 : 0;
        } else {
          av = "";
          bv = "";
        }
      } else {
        av = "";
        bv = "";
      }
      return compareValues(av, bv) * dir;
    });
  }

  function updateSortUI() {
    sortHeaders.forEach(th => {
      const k = th.dataset.sort;
      th.classList.remove("sort-asc", "sort-desc");
      if (k === state.sortKey) th.classList.add(state.sortDir === "desc" ? "sort-desc" : "sort-asc");
    });
  }

  function renderRows() {
    closePopup();
    updateSortUI();
    syncIdColumnHeaders();

    let filtered = applyFilter(state.rows);
    const totalFiltered = filtered.length;
    const totalAll = state.rows.length;
    syncHeaderPills(totalFiltered, totalAll);

    filtered = sortRows(filtered);

    let movies = 0;
    let shows = 0;
    let seasons = 0;
    let episodes = 0;
    for (const row of state.rows) {
      const t = (row.type || "").toLowerCase();
      if (t === "movie") movies += 1;
      else if (t === "show") shows += 1;
      else if (t === "season") seasons += 1;
      else if (t === "episode") episodes += 1;
    }
    if (summaryMovies) summaryMovies.textContent = String(movies);
    if (summaryShows) summaryShows.textContent = String(shows);
    if (summarySeasons) summarySeasons.textContent = String(seasons);
    if (summaryEpisodes) summaryEpisodes.textContent = String(episodes);

    if (tbody) tbody.innerHTML = "";

    if (!totalFiltered) {
      if (empty) empty.style.display = "block";
      if (pager) pager.style.display = "none";
      if (summaryVisible) summaryVisible.textContent = "0";
      if (summaryTotal) summaryTotal.textContent = String(totalAll || 0);
      setStatus("0 rows visible");
      state.pageRids = [];
      syncSelectPageCheckbox();
      clearSelection();
      if (pageInfo) pageInfo.textContent = "";
      return;
    }

    if (empty) empty.style.display = "none";

    const pageCount = Math.max(1, Math.ceil(totalFiltered / PAGE_SIZE));
    if (state.page >= pageCount) state.page = pageCount - 1;
    if (state.page < 0) state.page = 0;

    const start = state.page * PAGE_SIZE;
    const end = start + PAGE_SIZE;
    const rows = filtered.slice(start, end);

    state.pageRids = rows.map(r => r._rid);
    syncSelectPageCheckbox();
    syncBulkBar();

    const frag = document.createDocumentFragment();
    const anilistMode = isAnilistMode();
    rows.forEach(row => {
      const tr = document.createElement("tr");
      const locked = false;
      const fieldName = suffix => `cw-row-${row._rid || "new"}-${suffix}`;
      if (row.episode) tr.classList.add("cw-row-episode");
      if (row.deleted) tr.classList.add("cw-row-deleted");

      const cell = inner => {
        const td = document.createElement("td");
        td.appendChild(inner);
        return td;
      };

      const selCb = document.createElement("input");
      selCb.type = "checkbox";
      selCb.name = fieldName("selected");
      selCb.className = "cw-checkbox";
      selCb.checked = (state.selected || new Set()).has(row._rid);
      selCb.onchange = () => {
        if (!state.selected) state.selected = new Set();
        if (selCb.checked) state.selected.add(row._rid);
        else state.selected.delete(row._rid);
        syncBulkBar();
        syncSelectPageCheckbox();
      };
      tr.appendChild(cell(selCb));

      const delBtn = document.createElement("button");
      delBtn.type = "button";
      delBtn.className = "cw-btn cw-btn-del danger";
      delBtn.innerHTML = '<span class="material-symbol">delete</span>';
      delBtn.title = locked ? (row.deleted ? "Unblock row" : "Block row") : "Delete row";
      delBtn.onclick = () => {
        row.deleted = !row.deleted;
        markChanged();
        renderRows();
      };
      tr.appendChild(cell(delBtn));

      const keyIn = document.createElement("input");
      keyIn.name = fieldName("key");
      keyIn.value = row.key || "";
      keyIn.className = "cw-key";
      keyIn.disabled = locked;
      keyIn.oninput = e => {
        row.key = e.target.value;
        markChanged();
      };
      tr.appendChild(cell(keyIn));

      const typeBtn = document.createElement("button");
      typeBtn.type = "button";
      typeBtn.className = "cw-extra-display";
      typeBtn.disabled = locked;
      if (locked) {
        typeBtn.style.opacity = "0.6";
        typeBtn.style.cursor = "not-allowed";
      }
      updateTypeDisplay(row, typeBtn);
      typeBtn.onclick = () => {
        if (typeBtn.disabled) return;
        openTypeEditor(row, typeBtn);
      };
      tr.appendChild(cell(typeBtn));

      const titleCell = document.createElement("div");
      titleCell.className = "cw-title-cell";

      const titleRow = document.createElement("div");
      titleRow.className = "cw-title-row";
      titleCell.appendChild(titleRow);

      const titleIn = document.createElement("input");
      titleIn.name = fieldName("title");
      titleIn.value = row.title || "";
      titleIn.disabled = locked;
      titleIn.oninput = e => {
        row.title = e.target.value;
        row.raw.title = e.target.value || null;
        markChanged();
      };
      titleRow.appendChild(titleIn);

      const yearIn = document.createElement("input");
      yearIn.name = fieldName("year");
      yearIn.value = row.year || "";
      yearIn.disabled = locked;
      yearIn.oninput = e => {
        row.year = e.target.value;
        const v = e.target.value.trim();
        const n = v ? parseInt(v, 10) : NaN;
        row.raw.year = Number.isFinite(n) ? n : null;
        markChanged();
      };

      const imdbIn = document.createElement("input");
      imdbIn.name = fieldName("imdb");
      imdbIn.value = row.imdb || "";
      imdbIn.disabled = locked;
      imdbIn.oninput = e => {
        row.imdb = e.target.value;
        row.raw.ids = row.raw.ids || {};
        if (e.target.value) row.raw.ids.imdb = e.target.value;
        else delete row.raw.ids.imdb;
        markChanged();
      };
      const idAIn = document.createElement("input");
      idAIn.name = fieldName(anilistMode ? "mal" : "tmdb");
      idAIn.value = anilistMode ? (row.mal || "") : (row.tmdb || "");
      idAIn.placeholder = anilistMode ? "MAL…" : "TMDB…";
      idAIn.disabled = locked;
      idAIn.oninput = e => {
        const v = e.target.value;
        row.raw.ids = row.raw.ids || {};
        if (anilistMode) {
          row.mal = v;
          if (v) row.raw.ids.mal = v;
          else delete row.raw.ids.mal;
        } else {
          row.tmdb = v;
          if (v) row.raw.ids.tmdb = v;
          else delete row.raw.ids.tmdb;
        }
        markChanged();
      };

      const idBIn = document.createElement("input");
      idBIn.name = fieldName(anilistMode ? "anilist" : "trakt");
      idBIn.value = anilistMode ? (row.anilist || "") : (row.trakt || "");
      idBIn.placeholder = anilistMode ? "AniList…" : "Trakt…";
      idBIn.disabled = locked;
      idBIn.oninput = e => {
        const v = e.target.value;
        row.raw.ids = row.raw.ids || {};
        if (anilistMode) {
          row.anilist = v;
          if (v) row.raw.ids.anilist = v;
          else delete row.raw.ids.anilist;
        } else {
          row.trakt = v;
          if (v) row.raw.ids.trakt = v;
          else delete row.raw.ids.trakt;
        }
        markChanged();
      };

      const searchBtn = document.createElement("button");
      searchBtn.type = "button";
      searchBtn.className = "cw-title-search-btn";
      searchBtn.innerHTML = '<span class="material-symbol">search</span>';
      searchBtn.title = "Search and fill IDs";
      searchBtn.disabled = locked;
      if (locked) {
        searchBtn.style.opacity = "0.6";
        searchBtn.style.cursor = "not-allowed";
      }
      searchBtn.onclick = () => {
        if (searchBtn.disabled) return;
        openTitleSearchEditor(row, searchBtn, {
          keyIn,
          titleIn,
          yearIn,
          imdbIn,
          tmdbIn: anilistMode ? null : idAIn,
          traktIn: null,
          typeBtn,
        });
      };
      titleRow.appendChild(searchBtn);

      const subType = (((row.raw && row.raw.type) || row.type || "") + "").toLowerCase();
      if ((subType === "episode" || subType === "season") && row.raw && row.raw.series_title) {
        const sub = document.createElement("div");
        sub.className = "cw-title-sub";
        let label = row.raw.series_title;
        const code = subType === "episode" ? formatSxxEyy(row.raw.season, row.raw.episode) : formatSxxEyy(row.raw.season, null);
        if (code) label += " - " + code;
        sub.textContent = label;
        titleCell.appendChild(sub);
      }
      tr.appendChild(cell(titleCell));

      const yearTd = cell(yearIn);
      yearTd.className = "cw-col-year";
      tr.appendChild(yearTd);
      tr.appendChild(cell(idAIn));

      const extraBtn = document.createElement("button");
      extraBtn.type = "button";
      extraBtn.className = "cw-extra-display";
      updateExtraDisplay(row, extraBtn);

      const extraEditable = !locked && (state.kind === "ratings" || state.kind === "history" || state.kind === "progress");
      if (!extraEditable) {
        extraBtn.disabled = true;
        extraBtn.style.opacity = "0.6";
        extraBtn.style.cursor = locked ? "not-allowed" : "default";
      } else if (state.kind === "ratings") {
        extraBtn.onclick = () => openRatingEditor(row, extraBtn, extraBtn);
      } else if (state.kind === "history") {
        extraBtn.onclick = () => openHistoryEditor(row, extraBtn, extraBtn);
      } else if (state.kind === "progress") {
        extraBtn.onclick = () => openProgressEditor(row, extraBtn, extraBtn);
      }

      tr.appendChild(cell(extraBtn));

      frag.appendChild(tr);
    });

    if (tbody) tbody.appendChild(frag);

    const vis = rows.length;
    const first = start + 1;
    const last = start + vis;

    if (summaryVisible) summaryVisible.textContent = String(vis);
    if (summaryTotal) summaryTotal.textContent = String(totalAll);

    if (pageInfo) pageInfo.textContent = `Page ${state.page + 1} of ${pageCount} • Rows ${first}-${last} of ${totalFiltered}`;
    if (pager) pager.style.display = pageCount > 1 ? "flex" : "none";
    if (prevBtn) prevBtn.disabled = state.page <= 0;
    if (nextBtn) nextBtn.disabled = state.page >= pageCount - 1;

    if (totalFiltered > vis) {
      setRowsStatus(`${vis} rows visible (rows ${first}-${last} of ${totalFiltered} filtered, ${totalAll} total)`);
    } else {
      setRowsStatus(`${vis} rows visible, ${totalAll} total`);
    }
  }

  function formatSnapshotLabel(s) {
    if (s && typeof s.ts === "number" && s.ts > 0) {
      const d = new Date(s.ts * 1000);
      const pad = n => String(n).padStart(2, "0");
      return (
        d.getFullYear() +
        "-" +
        pad(d.getMonth() + 1) +
        "-" +
        pad(d.getDate()) +
        " - " +
        pad(d.getHours()) +
        ":" +
        pad(d.getMinutes())
      );
    }
    if (s && s.name) return s.name;
    return "Snapshot";
  }

  function rebuildSnapshots() {
    if (!snapSel) return;
    const isState = state.source === "state";
    const isPair = state.source === "pair";
    if (snapLabel) snapLabel.textContent = isState ? "Provider" : isPair ? "Dataset" : "Snapshot";
    if (instanceLabel) instanceLabel.style.display = isState ? "" : "none";
    if (instanceSel) instanceSel.style.display = isState ? "" : "none";

    if (isState || isPair) {
      const list = Array.isArray(state.snapshots) ? state.snapshots : [];
      const options = list.map(p => `<option value="${p}">${isState ? providerLabel(p, p) : p}</option>`).join("");
      snapSel.innerHTML = options;
      const opts = Array.from(snapSel.options).map(o => o.value);
      const next = opts.includes(state.snapshot) ? state.snapshot : opts[0] || "";
      if (next !== state.snapshot) state.snapshot = next;
      snapSel.value = state.snapshot || "";
      syncProviderIconSelect(snapSel, isState);
      return;
    }

    const options = (state.snapshots || [])
      .map(s => {
        const label = formatSnapshotLabel(s);
        return `<option value="${s.name}">${label}</option>`;
      })
      .join("");

    snapSel.innerHTML = `<option value="">Latest</option>` + options;
    snapSel.value = state.snapshot || "";
    syncProviderIconSelect(snapSel, false);
  }


  function rebuildPairs() {
    if (!pairSel) return;
    const isPair = state.source === "pair";
    if (!isPair) return;
    const list = Array.isArray(state.pairs) ? state.pairs : [];
    const esc = s => String(s || "").replace(/[&<>\"\']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "\'": "&#39;" }[c] || c));
    const options = list
      .map(p => {
        const scope = p && p.scope ? String(p.scope) : "";
        const label = p && p.label ? String(p.label) : scope;
        return `<option value="${esc(scope)}">${esc(label)}</option>`;
      })
      .join("");
    pairSel.innerHTML = options || `<option value="">No pairs</option>`;
    const opts = Array.from(pairSel.options).map(o => o.value);
    const next = opts.includes(state.pair) ? state.pair : opts[0] || "";
    if (next !== state.pair) state.pair = next;
    pairSel.value = state.pair || "";
  }

  async function loadPairs() {
    try {
      const data = await fetchJSON("/api/editor/pairs");
      state.pairs = Array.isArray(data && data.pairs) ? data.pairs : [];
      if (!state.pair) state.pair = (data && data.default) ? String(data.default) : "";
      rebuildPairs();
    } catch (e) {
      console.error(e);
      state.pairs = [];
      rebuildPairs();
    }
  }

const on = (el, ev, fn) => el && el.addEventListener(ev, fn);

async function fetchJSON(url, opts) {
  if (window.cwIsAuthSetupPending?.() === true) throw new Error("auth setup pending");
  const res = await fetch(url, Object.assign({ cache: "no-store" }, opts || {}));
  if (!res.ok) throw new Error(`Request failed: ${res.status}`);
  return await res.json();
}

async function fetchBlob(url) {
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`Download failed: ${res.status}`);
  return await res.blob();
}

function saveBlob(blob, filename) {
  const href = URL.createObjectURL(blob);
  const a = Object.assign(document.createElement("a"), { href, download: filename });
  document.body.appendChild(a);
  a.click();
  setTimeout(() => {
    URL.revokeObjectURL(href);
    a.remove();
  }, 0);
}

async function downloadFile(url, filename, toast) {
  try {
    setTag("warn", "Preparing download…");
    saveBlob(await fetchBlob(url), filename);
    setTag("loaded", "Ready");
    if (toast && window.cxToast) window.cxToast(toast);
  } catch (e) {
    console.error(e);
    setTag("error", "Download failed");
    setStatus(String(e));
  }
}

async function uploadJSON(url, file) {
  const fd = new FormData();
  fd.append("file", file);
  const res = await fetch(url, { method: "POST", body: fd });
  if (!res.ok) {
    let msg = `Import failed: ${res.status}`;
    try {
      const err = await res.json();
      if (err && err.detail) msg += ` – ${err.detail}`;
    } catch (_) {}
    throw new Error(msg);
  }
  return await res.json();
}

const listParts = (data, defs) => defs.flatMap(([k, label]) => data && data[k] != null ? [`${data[k]} ${label}${data[k] === 1 ? "" : "s"}`] : []);

function bindFileImport(btn, input, url, done) {
  if (!btn || !input) return;
  on(btn, "click", () => input.click());
  on(input, "change", async () => {
    const file = input.files && input.files[0];
    if (!file) return;
    try {
      setTag("warn", "Importing…");
      setStatus("");
      await done(await uploadJSON(url, file));
    } catch (e) {
      console.error(e);
      setTag("error", "Import failed");
      setStatus(String(e));
    } finally {
      try { input.value = ""; } catch (_) {}
    }
  });
}

  async function loadSnapshots() {
    try {
      if (state.source === "pair") {
        if (!state.pair || !Array.isArray(state.pairs) || !state.pairs.length) await loadPairs();
        rebuildPairs();
        if (!state.pair) {
          state.snapshots = [];
          rebuildSnapshots();
          showStateHint("pair");
          return;
        }
        const data = await fetchJSON(`/api/editor/pairs/datasets?kind=${encodeURIComponent(state.kind)}&pair=${encodeURIComponent(state.pair)}`);
        const dsets = Array.isArray(data && data.datasets) ? data.datasets : [];
        state.snapshots = dsets.map(d => (d && d.name ? String(d.name) : "")).filter(Boolean);
        const defDs = data && data.default_dataset ? String(data.default_dataset) : "";
        rebuildSnapshots();
        const opts = state.snapshots;
        const next = opts.includes(state.snapshot) ? state.snapshot : (defDs && opts.includes(defDs) ? defDs : (opts[0] || ""));
        if (next !== state.snapshot) state.snapshot = next;
        if (snapSel) snapSel.value = state.snapshot || "";
        if (!state.snapshots.length) showStateHint("pair");
        else showStateHint(null);
        return;
      }
      if (state.source === "state") {
        const data = await fetchJSON(`/api/editor/state/providers`);
        state.snapshots = Array.isArray(data.providers) ? data.providers : [];
        rebuildSnapshots();

        const prov = state.snapshot || (snapSel ? (snapSel.value || "") : "");
        if (prov) {
          const nextInst = await loadInstanceOptions(prov, instanceSel, state.instance);
          if (prov !== state.snapshot || nextInst !== state.instance) {
            state.snapshot = prov;
            state.instance = nextInst;
            persistUIState();
          }
        } else {
          const nextInst = renderInstanceOptions(instanceSel, [{ id: "default", label: "Default" }], "default");
          if (state.instance !== nextInst) {
            state.instance = nextInst;
            persistUIState();
          }
        }

        if (!state.snapshots.length) showStateHint("state");
        else showStateHint(null);
        return;
      }
      const data = await fetchJSON(`/api/editor/snapshots?kind=${encodeURIComponent(state.kind)}`);
      state.snapshots = Array.isArray(data.snapshots) ? data.snapshots : [];
      rebuildSnapshots();
    } catch (e) {
      console.error(e);
    }
  }


  async function loadTrackerCounts() {
    try {
      const data = await fetchJSON("/api/maintenance/crosswatch-tracker");
      const counts = data && data.counts ? data.counts : {};

      let stateFiles = counts.state_files != null ? counts.state_files : 0;
      let snaps = counts.snapshots != null ? counts.snapshots : 0;

      if (stateFiles === 0 && snaps === 0) {
        for (let i = 0; i < 3; i += 1) {
          await new Promise(r => setTimeout(r, 400));
          const d2 = await fetchJSON("/api/maintenance/crosswatch-tracker");
          const c2 = d2 && d2.counts ? d2.counts : {};
          stateFiles = c2.state_files != null ? c2.state_files : stateFiles;
          snaps = c2.snapshots != null ? c2.snapshots : snaps;
          if (stateFiles || snaps) break;
        }
      }

      if (summaryStateFiles) summaryStateFiles.textContent = String(stateFiles);
      if (summarySnapshots) summarySnapshots.textContent = String(snaps);

      if (stateFiles === 0 && snaps === 0) showStateHint("tracker");
      else showStateHint(null);
    } catch (e) {
      console.error(e);
    }
  }

  async function loadState() {
    if (state.source === "pair") {
      const scope = String(state.pair || "").trim();
      if (!scope) {
        state.items = {};
        state.rows = [];
        state.selected = new Set();
        state.pageRids = [];
        state.ridSeq = 1;
        state.hasChanges = false;
        state.page = 0;
        renderRows();
        showStateHint("pair");
        setTag("loaded", "No cache yet");
        setStatus("");
        return;
      }
    }
    state.loading = true;
    setTag("warn", "Loading");
    try {
      const params = new URLSearchParams({ kind: state.kind, source: state.source });
      if (state.source === "tracker" && state.snapshot) params.set("snapshot", state.snapshot);
      if (state.source === "state" && state.snapshot) {
        params.set("provider", state.snapshot);
        params.set("provider_instance", state.instance || "default");
      }
      if (state.source === "pair") {
        if (state.pair) params.set("pair", state.pair);
        if (state.snapshot) params.set("dataset", state.snapshot);
      }

      const data = await fetchJSON(`/api/editor?${params.toString()}`);
      if (data && data.ok === false) throw new Error(data.error || data.detail || "Load failed");

      if (state.source === "state") {
        state.baselineItems = data.items || {};
        state.manualAdds = data.manual_adds || {};
        state.manualBlocks = Array.isArray(data.manual_blocks) ? data.manual_blocks : [];

        if (data && typeof data.provider_instance === "string") {
          state.instance = data.provider_instance;
          if (instanceSel) instanceSel.value = state.instance;
        }

        const merged = Object.assign({}, state.baselineItems || {});
        for (const [k, v] of Object.entries(state.manualAdds || {})) {
          if (!(k in merged)) merged[k] = v;
        }

        state.items = merged;
        state.selected = new Set();
        state.pageRids = [];
        state.ridSeq = 1;
        state.rows = buildRows(state.items);

        const baselineKeys = new Set(Object.keys(state.baselineItems || {}));
        const blocked = new Set(
          (state.manualBlocks || []).map(x => String(x || "").trim()).filter(Boolean)
        );

        for (const row of state.rows) {
          row._origin = baselineKeys.has(row.key) ? "baseline" : "manual";
          if (row._origin === "baseline") row.deleted = blocked.has(row.key);
        }
      } else {
        state.items = data.items || {};
        state.selected = new Set();
        state.pageRids = [];
        state.ridSeq = 1;
        state.rows = buildRows(state.items);
      }

      state.hasChanges = false;
      state.page = 0;
      renderRows();

      if (state.source === "state") {
        const hasBaseline = state.baselineItems && Object.keys(state.baselineItems).length > 0;
        const hasManual = state.manualAdds && Object.keys(state.manualAdds).length > 0;
        const hasBlocks = Array.isArray(state.manualBlocks) && state.manualBlocks.length > 0;
        showStateHint(hasBaseline || hasManual || hasBlocks ? null : "state");
      } else {
        showStateHint(null);
      }

      setTag("loaded", "Ready");
    } catch (e) {
      console.error(e);
      const msg = String(e || "");

      if (
        state.source === "state" &&
        (msg.includes("404") || /state\.json/i.test(msg) || /missing state/i.test(msg))
      ) {
        showStateHint("state");
        state.items = {};
        state.rows = [];
        renderRows();
        setTag("warn", "Missing state");
        setStatus("");
      } else {
        setTag("error", "Load failed");
        setStatus(msg);
      }
    } finally {
      state.loading = false;
    }
  }

  function findRowsMissingKey() {
    const missing = [];
    for (const row of state.rows) {
      if (row.deleted) continue;
      const key = (row.key || "").trim();
      if (key) continue;

      const hasOther =
        (row.title && row.title.trim()) ||
        (row.type && row.type.trim()) ||
        (row.year && String(row.year).trim()) ||
        (row.imdb && row.imdb.trim()) ||
        (row.tmdb && row.tmdb.trim()) ||
        (row.trakt && row.trakt.trim());

      if (hasOther) missing.push(row);
    }
    return missing;
  }

  async function saveState() {
    if (state.saving) return;

    const missing = findRowsMissingKey();
    if (missing.length) {
      setTag("error", "Missing key");
      setStatus(
        `Cannot save: ${missing.length} row${missing.length === 1 ? "" : "s"} have data but no Key. Fill the Key or delete the row.`
      );
      if (window.cxToast) window.cxToast("Fill Key for all rows with data before saving");
      return;
    }

    state.saving = true;
    setTag("warn", "Saving");
    if (saveBtn) saveBtn.disabled = true;

    try {
      const items = {};
      const blocks = [];
      const seenBlocks = new Set();

      for (const row of state.rows) {
        if (row.deleted) {
          if (state.source === "state" && row._origin === "baseline") {
            const k = (row.key || "").trim();
            if (k) {
              const kl = k.toLowerCase();
              if (!seenBlocks.has(kl)) {
                seenBlocks.add(kl);
                blocks.push(k);
              }
            }
          }
          continue;
        }

        if (state.source === "state" && row._origin === "baseline") continue;

        const key = (row.key || "").trim();
        if (!key) continue;

        const raw = row.raw || {};
        const ids = raw.ids || {};

        if (row.imdb) ids.imdb = row.imdb;
        else delete ids.imdb;

        if (row.tmdb) ids.tmdb = row.tmdb;
        else delete ids.tmdb;

        if (row.trakt) ids.trakt = row.trakt;
        else delete ids.trakt;

        raw.ids = ids;
        raw.type = row.type || raw.type || null;
        raw.title = row.title ? row.title : raw.title || null;

        const y = (row.year || "").trim();
        const n = y ? parseInt(y, 10) : NaN;
        raw.year = Number.isFinite(n) ? n : null;

        items[key] = raw;
      }

      const payload = { kind: state.kind, source: state.source, items };
      if (state.source === "pair") {
        payload.pair = state.pair;
        payload.dataset = state.snapshot;
      }
      if (state.source === "state") {
        payload.provider = state.snapshot;
        payload.provider_instance = state.instance || "default";
        payload.blocks = blocks;
      }

      const res = await fetchJSON("/api/editor", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      state.hasChanges = false;
      setTag("warn", "Saved");
      setStatus(`Saved ${res.count || Object.keys(items).length} items`);
      await loadSnapshots();
    } catch (e) {
      console.error(e);
      setTag("error", "Save failed");
      setStatus(String(e));
    } finally {
      state.saving = false;
      if (saveBtn) saveBtn.disabled = false;
    }
  }

  function addRow() {
    const raw = { ids: {}, type: "movie", title: "", year: null };
    state.rows.unshift({
      _rid: state.ridSeq++,
      key: "",
      type: raw.type,
      title: "",
      year: "",
      imdb: "",
      tmdb: "",
      trakt: "",
      raw,
      deleted: false,
      episode: false,
      _origin: state.source === "state" ? "manual" : "tracker",
    });
    state.page = 0;
    markChanged();
    renderRows();
  }

  on(prevBtn, "click", () => {
    if (state.page <= 0) return;
    state.page -= 1;
    renderRows();
  });

  on(nextBtn, "click", () => {
    const pageCount = Math.max(1, Math.ceil(applyFilter(state.rows).length / PAGE_SIZE));
    if (state.page >= pageCount - 1) return;
    state.page += 1;
    renderRows();
  });

  sortHeaders.forEach(th => {
    th.addEventListener("click", () => {
      const key = th.dataset.sort;
      if (!key) return;
      if (state.sortKey === key) state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
      else {
        state.sortKey = key;
        state.sortDir = "asc";
      }
      persistUIState();
      renderRows();
    });
  });

  if (typeFilterWrap) {
    typeFilterWrap.addEventListener("click", e => {
      const btn = e.target.closest("button[data-type]");
      if (!btn) return;
      const t = btn.dataset.type;
      const current = !!state.typeFilter[t];
      if (current) {
        const enabledCount = Object.values(state.typeFilter).filter(Boolean).length;
        if (enabledCount <= 1) return;
      }
      state.typeFilter[t] = !current;
      syncTypeFilterUI();
      state.page = 0;
      persistUIState();
      renderRows();
    });
  }

  if (blockedOnlyBtn) {
    blockedOnlyBtn.addEventListener("click", () => {
      state.blockedOnly = !state.blockedOnly;
      syncTypeFilterUI();
      state.page = 0;
      persistUIState();
      renderRows();
    });
  }

  on(downloadBtn, "click", () => downloadFile("/api/editor/export", "crosswatch-tracker.zip", "Tracker export downloaded"));

  bindFileImport(uploadBtn, uploadInput, "/api/editor/import", async data => {
    const parts = listParts(data, [["files", "file"], ["states", "state file"], ["snapshots", "snapshot"]]);
    let msg = "Imported " + (parts.length ? parts.join(", ") : "tracker data");
    if (data.overwritten) msg += ` (${data.overwritten} overwritten)`;
    setTag("loaded", "Ready");
    setStatusSticky(msg, 5000);
    if (window.cxToast) window.cxToast(msg);
    await loadTrackerCounts();
    await loadSnapshots();
    await loadState();
  });

  if (sourceSel) {
    sourceSel.addEventListener("change", async () => {
      state.source = (sourceSel.value || "tracker").trim();
      state.snapshot = "";
      state.page = 0;
      persistUIState();
      syncSourceUI();
      clearSelection();
      if (state.source === "state") await loadImportProviders();
      else if (importRow) syncImportUI();
      if (state.source === "pair") await loadPairs();
      if (state.source === "tracker") await loadTrackerCounts();
      await loadSnapshots();
      await loadState();
    });
  }

  if (kindSel) {
    kindSel.addEventListener("change", async () => {
      const prevKind = state.kind;
      state.kind = (kindSel.value || "watchlist").trim();
      if (prevKind === "watchlist" && state.kind !== "watchlist") {
        state.typeFilter.season = true;
        state.typeFilter.episode = true;
      }
      syncKindUI();
      syncTypeFilterUI();
      syncStateBulkUI();
      clearSelection();
      if (state.source !== "state") state.snapshot = "";
      state.page = 0;
      persistUIState();
      await loadSnapshots();
      renderRows();
      await loadState();
    });
  }

  if (snapSel) {
    snapSel.addEventListener("change", async () => {
      state.snapshot = snapSel.value || "";
      if (state.source === "state") syncProviderIconSelect(snapSel, true);
      if (state.source === "state") {
        state.instance = await loadInstanceOptions(state.snapshot, instanceSel, state.instance);
        persistUIState();
      }
      state.page = 0;
      if (state.source !== "state") persistUIState();
      await loadState();
    });
  }

  
  if (instanceSel) {
    instanceSel.addEventListener("change", async () => {
      state.instance = instanceSel.value || "default";
      state.page = 0;
      persistUIState();
      await loadState();
    });
  }

if (importProviderSel) {
    importProviderSel.addEventListener("change", () => {
      state.importProvider = importProviderSel.value || "";
      state.importProviderInstance = "default";
      persistUIState();
      syncImportUI();
    });
  }

  if (importInstanceSel) {
    importInstanceSel.addEventListener("change", () => {
      state.importProviderInstance = importInstanceSel.value || "default";
      persistUIState();
    });
  }

  if (importModeSel) {
    importModeSel.addEventListener("change", () => {
      state.importMode = importModeSel.value || "replace";
    });
  }

  [[importWatchlistCb, "watchlist"], [importHistoryCb, "history"], [importRatingsCb, "ratings"], [importProgressCb, "progress"]]
    .forEach(([el, key]) => on(el, "change", () => { state.importFeatures[key] = !!el.checked; }));

  on(importRunBtn, "click", runStateImport);


  if (filterInput) {
    filterInput.addEventListener("input", () => {
      state.filter = filterInput.value || "";
      state.page = 0;
      clearSelection();
      persistUIState();
      renderRows();
    });
  }

  if (pairSel) {
    pairSel.addEventListener("change", async () => {
      state.pair = pairSel.value || "";
      state.snapshot = "";
      state.page = 0;
      persistUIState();
      await loadSnapshots();
      await loadState();
    });
  }

  if (reloadBtn) {
    reloadBtn.addEventListener("click", async () => {
      state.snapshot = (snapSel && snapSel.value) ? snapSel.value : "";
      state.page = 0;
      if (state.source === "pair") await loadPairs();
      if (state.source !== "state") await loadTrackerCounts();
      await loadSnapshots();
      await loadState();
    });
  }

  if (selectPage) {
    selectPage.addEventListener("change", () => {
      if (!state.selected) state.selected = new Set();
      const on = !!selectPage.checked;
      for (const rid of state.pageRids || []) {
        if (on) state.selected.add(rid);
        else state.selected.delete(rid);
      }
      syncBulkBar();
      syncSelectPageCheckbox();
      renderRows();
    });
  }

  on(bulkRemoveBtn, "click", () => bulkSetDeletedForSelected(true));
  on(bulkRestoreBtn, "click", () => bulkSetDeletedForSelected(false));
  on(bulkClearBtn, "click", () => { clearSelection(); renderRows(); });
  on(bulkBlockTypeBtn, "click", () => bulkSetBlocksByType(bulkTypeSel && bulkTypeSel.value, true));
  on(bulkUnblockTypeBtn, "click", () => bulkSetBlocksByType(bulkTypeSel && bulkTypeSel.value, false));

  on(addBtn, "click", addRow);
  on(saveBtn, "click", saveState);

  window.addEventListener("beforeunload", e => {
    if (!state.hasChanges) return;
    e.preventDefault();
    e.returnValue = "";
  });

  on(stateDownloadBtn, "click", () => downloadFile("/api/editor/state/manual/export", "crosswatch-state-policy.json", "Policy export downloaded"));

  bindFileImport(stateUploadBtn, stateUploadInput, "/api/editor/state/manual/import?mode=merge", async data => {
    const msg = "Imported " + (listParts(data, [["providers", "provider"], ["blocks", "block"], ["adds", "add"]]).join(", ") || "policy");
    if (window.cxToast) window.cxToast(msg);
    setTag("warn", "Imported");
    await loadSnapshots();
    await loadState();
  });

  (async () => {
    syncSourceUI();
    await loadImportProviders();
    setTag("warn", state.source === "state" ? "Loading current state…" : state.source === "pair" ? "Loading pair cache…" : "Loading tracker state…");
    if (state.source === "pair") await loadPairs();
    if (state.source === "tracker") await loadTrackerCounts();
    await loadSnapshots();
    await loadState();
  })();
  }

  function bootWhenReady() {
    if (cwEditorBooted) return;
    if (window.cwIsAuthSetupPending?.() === true) {
      if (!cwEditorBootRetryWired) {
        cwEditorBootRetryWired = true;
        Promise.resolve(window.__cwAuthBootstrapPromise)
          .catch(() => null)
          .finally(() => {
            cwEditorBootRetryWired = false;
            if (window.cwIsAuthSetupPending?.() === true) return;
            bootWhenReady();
          });
      }
      return;
    }
    if (document.getElementById("page-editor")) {
      bootEditor();
      return;
    }
    const obs = new MutationObserver(() => {
      if (!document.getElementById("page-editor")) return;
      obs.disconnect();
      bootEditor();
    });
    obs.observe(document.documentElement, { childList: true, subtree: true });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootWhenReady, { once: true });
  } else {
    bootWhenReady();
  }

})();
