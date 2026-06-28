/* Playback Progress page */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const ROOT_ID = "playback-progress-root";
  const STYLE_ID = "playback-progress-style";
  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const api = async (url, opts = {}) => {
    const r = await fetch(url, { credentials: "same-origin", cache: "no-store", ...opts });
    const txt = await r.text();
    let data = {};
    try { data = txt ? JSON.parse(txt) : {}; } catch {}
    if (!r.ok) data.ok = false;
    return data;
  };
  const icon = (name) => `<span class="material-symbols-rounded" aria-hidden="true">${name}</span>`;
  const providerIcon = (provider) => {
    const p = String(provider || "").toUpperCase();
    return `<img src="/assets/img/${esc(p)}-log.svg" alt="" onerror="this.remove()">`;
  };
  const state = {
    mounted: false,
    page: 1,
    pageSize: 30,
    total: 0,
    items: [],
    providers: [],
    errors: [],
    selected: new Map(),
    filters: { provider: "", media_type: "", progress: "", age: "", rating: "", search: "", sort: "last_updated" },
    busy: false,
    loaded: false,
    settings: null
  };

  function ensureStyle() {
    if (document.getElementById(STYLE_ID)) return;
    const css = `
#page-playback_progress{max-width:none;width:100%;grid-column:1/-1;padding:0;background:transparent!important;border:0!important;box-shadow:none!important}
#${ROOT_ID}{--pp-shell-bg:linear-gradient(180deg,rgba(8,10,15,.985),rgba(2,3,7,.975));--pp-panel-bg:linear-gradient(180deg,rgba(12,14,20,.95),rgba(4,5,10,.945));--pp-panel-bg-strong:linear-gradient(180deg,rgba(9,11,17,.985),rgba(2,3,7,.975));--pp-input-bg:rgba(7,11,19,.78);--pp-border:rgba(255,255,255,.09);--pp-border-soft:rgba(255,255,255,.055);--pp-soft:rgba(201,210,228,.72);--pp-fg:rgba(244,247,255,.96);--pp-shadow:0 20px 54px rgba(0,0,0,.42),inset 0 1px 0 rgba(255,255,255,.04);--pp-danger-bg:linear-gradient(180deg,rgba(118,28,46,.30),rgba(82,14,28,.24));--pp-danger-border:rgba(255,138,160,.15);--pp-danger-fg:#ffe7ee;display:grid;gap:14px;color:var(--pp-fg)}
.pp-head,.pp-status,.pp-toolbar,.pp-card,.pp-errors,.pp-bulk,.pp-pager{border:1px solid var(--pp-border);background:var(--pp-panel-bg);box-shadow:var(--pp-shadow);border-radius:18px;color:var(--pp-fg)}
.pp-head{padding:18px 20px;display:flex;align-items:flex-start;justify-content:space-between;gap:16px;background:radial-gradient(115% 120% at 0% 0%,rgba(78,68,170,.10),transparent 46%),radial-gradient(88% 100% at 100% 100%,rgba(34,46,108,.06),transparent 54%),var(--pp-shell-bg);border-radius:20px}.pp-title{font-size:24px;font-weight:850;line-height:1.1}.pp-intro{margin-top:6px;color:var(--pp-soft);max-width:78ch;font-size:13px;line-height:1.45}.pp-head-actions{display:flex;gap:8px;align-items:center}.pp-btn{display:inline-flex;align-items:center;justify-content:center;gap:7px;min-height:36px;padding:0 12px;border-radius:12px;border:1px solid var(--pp-border);background:var(--pp-panel-bg);color:var(--pp-fg);font-weight:750;cursor:pointer;box-shadow:none}.pp-btn:hover{border-color:var(--pp-border);background:var(--pp-panel-bg-strong)}.pp-btn[disabled]{opacity:.45;cursor:not-allowed}.pp-btn.danger{color:var(--pp-danger-fg);border-color:var(--pp-danger-border);background:var(--pp-danger-bg)}.pp-icon-btn{width:38px;padding:0}.pp-status{padding:12px;display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px}.pp-provider{display:flex;align-items:center;gap:10px;min-width:0;padding:10px;border-radius:12px;background:var(--pp-panel-bg-strong);border:1px solid var(--pp-border-soft)}.pp-provider img{width:34px;max-height:18px;object-fit:contain}.pp-provider-main{min-width:0}.pp-provider-name{font-weight:800;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.pp-provider-sub{margin-top:2px;color:var(--pp-soft);font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.pp-dot{width:9px;height:9px;border-radius:50%;background:#8b93a7;margin-left:auto;flex:0 0 auto}.pp-dot.ok{background:#42d392}.pp-dot.warn{background:#f3ba5f}.pp-toolbar{padding:12px;display:grid;grid-template-columns:minmax(220px,1.4fr) repeat(6,minmax(120px,1fr));gap:10px}.pp-field{min-width:0;height:38px;border-radius:12px;border:1px solid var(--pp-border);background:var(--pp-input-bg);color:var(--pp-fg);padding:0 11px}.pp-field option{background:#0d111a;color:#f7f9ff}.pp-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.pp-card{position:relative;overflow:hidden;display:grid;grid-template-columns:112px minmax(0,1fr);min-height:190px;background:var(--pp-panel-bg-strong)}.pp-art{position:relative;background:#10131b}.pp-art img{width:100%;height:100%;object-fit:cover;display:block}.pp-check{position:absolute;left:8px;top:8px;z-index:2;width:24px;height:24px;accent-color:#79a7ff}.pp-body{padding:14px;display:grid;gap:10px;align-content:space-between;min-width:0}.pp-top{display:grid;gap:6px;min-width:0}.pp-badges{display:flex;gap:7px;align-items:center;flex-wrap:wrap}.pp-badge{display:inline-flex;align-items:center;gap:6px;min-height:24px;padding:0 8px;border-radius:999px;border:1px solid var(--pp-border-soft);background:var(--pp-panel-bg);font-size:11px;font-weight:800;color:var(--pp-soft)}.pp-badge img{max-width:46px;max-height:14px}.pp-card-title{font-size:17px;font-weight:850;line-height:1.15;min-width:0;color:var(--pp-fg)}.pp-card-sub{color:var(--pp-soft);font-size:13px;line-height:1.35;min-width:0}.pp-meta{display:flex;gap:8px;flex-wrap:wrap;color:var(--pp-soft);font-size:12px}.pp-progress{display:grid;gap:5px}.pp-progress-row{display:flex;justify-content:space-between;gap:10px;color:var(--pp-soft);font-size:12px}.pp-bar{height:8px;border-radius:999px;background:rgba(255,255,255,.08);overflow:hidden}.pp-bar span{display:block;height:100%;border-radius:inherit;background:linear-gradient(90deg,#5fb6ff,#7ee2b8);width:0}.pp-actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.pp-errors{padding:12px;color:var(--pp-danger-fg);font-size:13px;background:var(--pp-panel-bg)}.pp-pager{display:flex;align-items:center;justify-content:center;gap:10px;padding:10px;color:var(--pp-soft)}.pp-empty{grid-column:1/-1;padding:28px;text-align:center;color:var(--pp-soft);border:1px dashed var(--pp-border);border-radius:18px}.pp-bulk{position:sticky;bottom:12px;z-index:20;padding:10px 12px;display:flex;align-items:center;justify-content:space-between;gap:12px}.pp-bulk.hidden{display:none!important}.pp-bulk-left,.pp-bulk-actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.pp-toast{position:fixed;left:50%;bottom:20px;z-index:9999;transform:translateX(-50%);padding:10px 14px;border-radius:14px;border:1px solid var(--pp-border);background:var(--pp-panel-bg-strong);color:var(--pp-fg);box-shadow:var(--pp-shadow)}.pp-toast.hidden{display:none}
html[data-cw-theme=flat-dark] #${ROOT_ID}{--pp-shell-bg:#171a22;--pp-panel-bg:#171a22;--pp-panel-bg-strong:#20242d;--pp-input-bg:#12151c;--pp-border:rgba(255,255,255,.13);--pp-border-soft:rgba(255,255,255,.10);--pp-soft:#a9b0bd;--pp-fg:#eef1f6;--pp-shadow:none;--pp-danger-bg:#43272e;--pp-danger-border:rgba(216,102,114,.42);--pp-danger-fg:#ffe3e7}
html[data-cw-theme=flat-light] #${ROOT_ID}{--pp-shell-bg:#ffffff;--pp-panel-bg:#ffffff;--pp-panel-bg-strong:#f5f7fb;--pp-input-bg:#ffffff;--pp-border:rgba(16,24,40,.16);--pp-border-soft:rgba(16,24,40,.11);--pp-soft:#475467;--pp-fg:#111827;--pp-shadow:none;--pp-danger-bg:#f7dbe1;--pp-danger-border:rgba(169,63,77,.34);--pp-danger-fg:#7f1d2d}
html[data-cw-theme=flat-dark] .pp-head,html[data-cw-theme=flat-light] .pp-head{background:var(--pp-panel-bg)!important;background-image:none!important}
html[data-cw-theme=flat-light] .pp-field option{background:#ffffff;color:#111827}.cw-compact #page-playback_progress{display:block!important}.cw-compact #${ROOT_ID}{padding:10px}.cw-compact .pp-toolbar{grid-template-columns:1fr 1fr}.cw-compact .pp-head{display:grid}.cw-compact .pp-grid{grid-template-columns:1fr}.cw-compact .pp-card{grid-template-columns:92px minmax(0,1fr)}.cw-compact .pp-card-title{font-size:15px}
@media(max-width:980px){.pp-toolbar{grid-template-columns:1fr 1fr}.pp-grid{grid-template-columns:1fr}}@media(max-width:640px){.pp-toolbar{grid-template-columns:1fr}.pp-card{grid-template-columns:88px minmax(0,1fr)}.pp-head-actions{justify-content:flex-start}.pp-bulk{display:grid}.pp-bulk-left,.pp-bulk-actions{justify-content:center}}
.pp-toolbar{display:flex;align-items:center;gap:10px}.pp-toolbar #pp-search{flex:0 1 320px;min-width:180px}.pp-toolbar .pp-field:not(#pp-search),.pp-toolbar .cw-icon-select{flex:1 1 180px;min-width:170px}.pp-toolbar .cw-icon-select-btn{min-height:38px;border-radius:12px}.pp-toolbar .cw-icon-select-menu{min-width:185px}.pp-grid{align-items:start}.pp-card{grid-template-columns:96px minmax(0,1fr);height:144px;min-height:0;cursor:pointer;border-radius:16px;transition:border-color .16s ease,background .16s ease,transform .16s ease;isolation:isolate}.pp-card:hover{border-color:rgba(126,226,184,.28);transform:translateY(-1px)}.pp-card.selected{border-color:rgba(126,226,184,.68);box-shadow:0 0 0 1px rgba(126,226,184,.28),var(--pp-shadow)}.pp-card.selected:after{content:"";position:absolute;left:10px;top:10px;width:24px;height:24px;border-radius:7px;background:#7ee2b8;box-shadow:0 6px 16px rgba(0,0,0,.28)}.pp-card.selected:before{content:"check";position:absolute;left:10px;top:10px;z-index:2;width:24px;height:24px;display:grid;place-items:center;font-family:"Material Symbols Rounded";font-size:18px;color:#061015}.pp-check{display:none!important}.pp-art{z-index:1}.pp-art:after{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(90deg,rgba(3,5,10,.03),rgba(3,5,10,.16))}.pp-body{position:relative;overflow:hidden;padding:9px 14px;gap:6px}.pp-body:before{content:"";position:absolute;inset:0;z-index:0;pointer-events:none;background:linear-gradient(90deg,rgba(32,36,45,.96),rgba(32,36,45,.86) 54%,rgba(32,36,45,.92)),var(--pp-backdrop,none);background-size:cover;background-position:center;opacity:.48}.pp-body>*{position:relative;z-index:1}.pp-top{gap:4px}.pp-card-head{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:start;gap:12px;min-width:0}.pp-title-wrap{min-width:0}.pp-card-side{display:grid;gap:6px;justify-items:end;align-content:start}.pp-provider-stack{display:flex;align-items:center;justify-content:flex-end;gap:6px;flex-wrap:wrap;min-width:0}.pp-provider-pill{display:inline-flex;align-items:center;gap:6px;min-height:23px;padding:0 8px;border-radius:999px;border:1px solid var(--pp-border-soft);background:var(--pp-panel-bg);color:var(--pp-soft);font-size:11px;font-weight:850;white-space:nowrap;overflow:visible}.pp-provider-pill img{width:16px;height:16px;max-width:none;max-height:none;object-fit:contain;flex:0 0 16px}.pp-rating-chip{display:inline-flex;align-items:center;gap:4px;min-height:22px;padding:0 8px;border-radius:999px;border:1px solid rgba(255,205,86,.20);background:rgba(255,205,86,.10);color:#ffe29a;font-size:11px;font-weight:850;white-space:nowrap}.pp-rating-chip .material-symbols-rounded{font-size:15px;font-variation-settings:'FILL' 1}.pp-badges,.pp-kind{display:none}.pp-card-title{font-size:18px}.pp-card-sub{font-size:13px}.pp-progress{gap:5px}.pp-progress-row{align-items:center;font-size:13px;flex-wrap:wrap}.pp-progress-row strong{font-size:14px;color:var(--pp-fg)}.pp-timing{display:inline-flex;align-items:center;justify-content:flex-end;gap:12px;margin-left:auto;color:var(--pp-soft);white-space:nowrap}.pp-paused{color:var(--pp-soft);white-space:nowrap}.pp-live{color:#9de4d0;white-space:nowrap}.pp-meta{display:none}.pp-actions{justify-content:flex-end;gap:6px;align-self:end}.pp-actions .pp-btn{min-height:32px;border-radius:12px}.pp-action-btn{min-height:28px!important;padding:0 9px;border-radius:999px!important;background:rgba(255,255,255,.035)!important;border-color:rgba(255,255,255,.08)!important;color:rgba(230,236,248,.74)!important;font-size:12px;font-weight:780;gap:5px;box-shadow:none;opacity:.82}.pp-action-btn .material-symbols-rounded{font-size:19px}.pp-action-btn:hover{opacity:1;color:var(--pp-fg)!important;background:rgba(255,255,255,.07)!important;border-color:rgba(255,255,255,.15)!important}.pp-action-watch .material-symbols-rounded{color:#9de4d0}.pp-action-edit .material-symbols-rounded{color:#9cc7ff}.pp-action-remove{color:rgba(255,214,222,.70)!important;background:rgba(149,48,67,.08)!important;border-color:rgba(255,138,160,.12)!important}.pp-action-remove .material-symbols-rounded{color:#ff9aaa}.pp-action-remove:hover{color:#ffe8ee!important;background:rgba(149,48,67,.16)!important;border-color:rgba(255,138,160,.24)!important}.pp-modal{position:fixed;inset:0;z-index:9998;display:grid;place-items:center;background:rgba(0,0,0,.48);backdrop-filter:blur(4px)}.pp-modal.hidden{display:none!important}.pp-dialog{width:min(420px,calc(100vw - 28px));padding:16px;border-radius:16px;border:1px solid var(--pp-border);background:var(--pp-panel-bg-strong);box-shadow:var(--pp-shadow);display:grid;gap:14px}.pp-dialog-title{font-size:18px;font-weight:850}.pp-dialog-sub{color:var(--pp-soft);font-size:12px}.pp-progress-edit{display:grid;grid-template-columns:1fr 84px;gap:10px;align-items:center}.pp-progress-edit input[type=range]{width:100%;accent-color:#7ee2b8}.pp-progress-edit input[type=number]{height:38px;border-radius:10px;border:1px solid var(--pp-border);background:var(--pp-input-bg);color:var(--pp-fg);padding:0 10px;font-weight:800}.pp-dialog-error{min-height:18px;color:var(--pp-danger-fg);font-size:12px}.pp-dialog-actions{display:flex;justify-content:flex-end;gap:8px}.cw-compact .pp-card{grid-template-columns:90px minmax(0,1fr);height:136px;min-height:0}html[data-cw-theme=flat-dark] .pp-body:before{background:linear-gradient(90deg,rgba(32,36,45,.97),rgba(32,36,45,.88) 54%,rgba(32,36,45,.94)),var(--pp-backdrop,none);opacity:.46}html[data-cw-theme=flat-light] .pp-body:before{background:linear-gradient(90deg,rgba(245,247,251,.96),rgba(245,247,251,.84) 54%,rgba(245,247,251,.92)),var(--pp-backdrop,none);opacity:.42}html[data-cw-theme=flat-light] .pp-art:after{background:linear-gradient(90deg,rgba(255,255,255,.02),rgba(255,255,255,.14))}html[data-cw-theme=flat-light] .pp-rating-chip{background:rgba(184,121,0,.09);border-color:rgba(184,121,0,.18);color:#7a4f00}html[data-cw-theme=flat-light] .pp-action-btn{background:rgba(17,24,39,.035)!important;border-color:rgba(17,24,39,.12)!important;color:rgba(17,24,39,.68)!important}html[data-cw-theme=flat-light] .pp-action-btn:hover{background:rgba(17,24,39,.075)!important;border-color:rgba(17,24,39,.18)!important;color:#111827!important}html[data-cw-theme=flat-light] .pp-action-remove{background:rgba(169,63,77,.08)!important;border-color:rgba(169,63,77,.16)!important;color:#8f2738!important}html[data-cw-theme=flat-dark] .pp-art:after{background:linear-gradient(90deg,rgba(7,10,16,.02),rgba(7,10,16,.16))}.pp-status.hidden{display:none!important}.pp-status-message{grid-column:1/-1;padding:12px 14px;color:var(--pp-soft);font-weight:750;text-align:center}@media(max-width:980px){.pp-toolbar{flex-wrap:wrap}.pp-toolbar #pp-search{flex:1 1 100%;min-width:0}.pp-toolbar .pp-field:not(#pp-search),.pp-toolbar .cw-icon-select{flex:1 1 170px}.pp-card{grid-template-columns:90px minmax(0,1fr);height:136px}}@media(max-width:640px){.pp-toolbar{display:grid;grid-template-columns:1fr}.pp-toolbar #pp-search,.pp-toolbar .pp-field:not(#pp-search),.pp-toolbar .cw-icon-select{min-width:0;flex:auto}.pp-card{grid-template-columns:82px minmax(0,1fr);height:132px}.pp-card-head{grid-template-columns:1fr}.pp-card-side{justify-items:start}.pp-provider-stack{justify-content:flex-start}.pp-card-title{font-size:16px}.pp-timing{margin-left:0;flex-wrap:wrap;justify-content:flex-start;gap:8px}.pp-action-btn{padding:0 8px;font-size:0}.pp-action-btn .material-symbols-rounded{font-size:20px}}
.pp-toolbar,.pp-pager{position:relative;overflow:hidden;background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.015)),linear-gradient(90deg,rgba(255,255,255,.045) 1px,transparent 1px),linear-gradient(180deg,rgba(255,255,255,.045) 1px,transparent 1px),var(--pp-panel-bg)!important;background-size:auto,74px 74px,74px 74px,auto;background-position:0 0,0 0,0 0,0 0}.pp-toolbar:before,.pp-pager:before{content:"";position:absolute;inset:0;pointer-events:none;background:radial-gradient(80% 140% at 0% 0%,rgba(126,226,184,.08),transparent 52%),radial-gradient(90% 120% at 100% 100%,rgba(95,182,255,.07),transparent 56%);opacity:.9}.pp-toolbar>*,.pp-pager>*{position:relative;z-index:1}.pp-pager{min-height:58px}html[data-cw-theme=flat-light] .pp-toolbar,html[data-cw-theme=flat-light] .pp-pager{background:linear-gradient(180deg,rgba(255,255,255,.82),rgba(245,247,251,.92)),linear-gradient(90deg,rgba(16,24,40,.07) 1px,transparent 1px),linear-gradient(180deg,rgba(16,24,40,.07) 1px,transparent 1px),var(--pp-panel-bg)!important}html[data-cw-theme=flat-dark] .pp-toolbar,html[data-cw-theme=flat-dark] .pp-pager{background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.012)),linear-gradient(90deg,rgba(255,255,255,.055) 1px,transparent 1px),linear-gradient(180deg,rgba(255,255,255,.055) 1px,transparent 1px),var(--pp-panel-bg)!important}.pp-bulk{display:flex;width:max-content;max-width:min(940px,calc(100vw - 32px));margin:0 auto;padding:7px 9px;border-radius:999px;justify-content:center;flex-wrap:nowrap;gap:7px;background:linear-gradient(180deg,rgba(24,28,38,.94),rgba(12,14,20,.92));backdrop-filter:blur(14px);box-shadow:0 14px 36px rgba(0,0,0,.34),inset 0 1px 0 rgba(255,255,255,.07)}.pp-bulk .pp-btn{min-height:32px;border-radius:999px;box-shadow:none}.pp-bulk-left,.pp-bulk-actions{flex-wrap:nowrap;gap:6px}.pp-bulk-left{padding-right:0;border-right:0}.pp-selected-pill{display:inline-flex;align-items:center;gap:7px;min-height:32px;padding:0 11px 0 7px;border-radius:999px;border:1px solid var(--pp-border-soft);background:rgba(255,255,255,.045);font-weight:850;white-space:nowrap}.pp-selected-number{display:inline-grid;place-items:center;min-width:22px;height:22px;padding:0 6px;border-radius:999px;background:linear-gradient(135deg,#5fb6ff,#7ee2b8);color:#071016;font-size:12px;line-height:1}.pp-selected-label{color:var(--pp-soft);font-size:12px}.pp-bulk-divider{width:1px;height:28px;margin:0 7px;border-radius:999px;background:linear-gradient(180deg,transparent,rgba(255,255,255,.24),transparent);font-size:0;line-height:0;flex:0 0 1px;align-self:center}.pp-bulk-icon{width:36px;min-width:36px;padding:0!important}.pp-bulk-icon .material-symbols-rounded{font-size:21px}.pp-bulk-icon.danger{background:rgba(149,48,67,.16)!important;border-color:rgba(255,138,160,.20)!important;color:#ffdce4!important}html[data-cw-theme=flat-light] .pp-bulk{background:rgba(255,255,255,.94);box-shadow:0 14px 34px rgba(16,24,40,.14),inset 0 1px 0 rgba(255,255,255,.9)}html[data-cw-theme=flat-light] .pp-selected-pill{background:rgba(17,24,39,.04)}html[data-cw-theme=flat-light] .pp-bulk-divider{background:linear-gradient(180deg,transparent,rgba(17,24,39,.24),transparent)}.pp-settings-dialog{width:min(1040px,calc(100vw - 28px))}.pp-settings-list{display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));align-items:start;gap:12px;max-height:min(560px,64vh);overflow:auto;padding-right:2px}.pp-settings-list>.pp-dialog-sub{grid-column:1/-1}.pp-settings-table{display:grid;gap:8px;min-width:0}.pp-settings-row{display:grid;grid-template-columns:auto minmax(0,1fr) auto;align-items:center;gap:10px;min-height:74px;padding:10px;border-radius:12px;border:1px solid var(--pp-border-soft);background:rgba(255,255,255,.035)}.pp-settings-row input[type=checkbox]{width:18px;height:18px;accent-color:#7ee2b8}.pp-settings-main{min-width:0}.pp-settings-name{display:flex;align-items:center;gap:8px;font-weight:850;min-width:0}.pp-settings-name img{width:16px;height:16px;max-width:none;max-height:none;object-fit:contain;flex:0 0 16px}.pp-settings-sub{margin-top:2px;color:var(--pp-soft);font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.pp-settings-pill{font-size:11px;color:var(--pp-soft);font-weight:800}.pp-settings-timeout{display:flex;align-items:center;justify-content:flex-start;gap:12px;flex-wrap:wrap}.pp-settings-timeout-label{display:inline-flex;align-items:center;gap:6px;min-width:0}.pp-help-icon{display:inline-grid;place-items:center;width:18px;height:18px;border-radius:999px;border:1px solid var(--pp-border-soft);background:rgba(255,255,255,.045);color:var(--pp-soft);cursor:help}.pp-help-icon .material-symbols-rounded{font-size:15px;line-height:1}.pp-settings-timeout input{width:96px;height:38px;border-radius:10px;border:1px solid var(--pp-border);background:var(--pp-input-bg);color:var(--pp-fg);padding:0 10px;font-weight:800}@media(max-width:760px){.pp-settings-list{grid-template-columns:1fr;max-height:min(560px,58vh)}.pp-settings-row{grid-template-columns:auto 1fr}.pp-settings-pill{display:none}.pp-bulk{width:calc(100vw - 24px);border-radius:18px;flex-wrap:wrap}.pp-bulk-left,.pp-bulk-actions{flex-wrap:wrap;justify-content:center}.pp-bulk-divider{display:none}}
.pp-toolbar,.pp-pager{--pp-matrix-line:rgba(210,222,248,.055);--pp-matrix-line-strong:rgba(210,222,248,.035);position:relative;isolation:isolate;overflow:hidden;border-radius:20px;background:linear-gradient(180deg,rgba(34,40,55,.90),rgba(26,31,44,.86)),var(--pp-panel-bg)!important;box-shadow:var(--pp-shadow)}.pp-toolbar:before,.pp-pager:before{content:"";position:absolute;inset:-1px;z-index:0;pointer-events:none;background-image:linear-gradient(var(--pp-matrix-line) 1px,transparent 1px),linear-gradient(90deg,var(--pp-matrix-line) 1px,transparent 1px),linear-gradient(rgba(255,255,255,.025),rgba(255,255,255,0) 42%);background-size:58px 58px,58px 58px,100% 100%;background-position:center;opacity:.62}.pp-toolbar:after,.pp-pager:after{content:"";position:absolute;inset:0;z-index:0;pointer-events:none;background:radial-gradient(95% 120% at 100% 0%,rgba(76,68,170,.075),transparent 58%),linear-gradient(90deg,rgba(126,226,184,.035),transparent 18%,transparent 82%,rgba(95,182,255,.025));opacity:.88}.pp-toolbar>*,.pp-pager>*{position:relative;z-index:1}html[data-cw-theme=flat-light] .pp-toolbar,html[data-cw-theme=flat-light] .pp-pager{--pp-matrix-line:rgba(16,24,40,.075);background:linear-gradient(180deg,rgba(255,255,255,.90),rgba(244,247,252,.92)),var(--pp-panel-bg)!important}html[data-cw-theme=flat-light] .pp-toolbar:after,html[data-cw-theme=flat-light] .pp-pager:after{background:radial-gradient(95% 120% at 100% 0%,rgba(76,68,170,.055),transparent 58%)}html[data-cw-theme=flat-dark] .pp-toolbar,html[data-cw-theme=flat-dark] .pp-pager{--pp-matrix-line:rgba(210,222,248,.06);background:linear-gradient(180deg,rgba(34,40,55,.90),rgba(24,29,42,.88)),var(--pp-panel-bg)!important}.pp-pager{background:var(--pp-panel-bg)!important;min-height:0;isolation:auto}.pp-pager:before,.pp-pager:after{content:none!important}
#${ROOT_ID} .pp-toolbar,#${ROOT_ID} .pp-pager{--pp-matrix-line:rgba(255,255,255,.040);--pp-matrix-glow:rgba(78,68,170,.055);position:relative;isolation:isolate;overflow:hidden;background:var(--pp-panel-bg)!important}
#${ROOT_ID} .pp-toolbar:before,#${ROOT_ID} .pp-pager:before{content:""!important;position:absolute;inset:-1px;z-index:0;pointer-events:none;background-image:linear-gradient(var(--pp-matrix-line) 1px,transparent 1px),linear-gradient(90deg,var(--pp-matrix-line) 1px,transparent 1px);background-size:58px 58px;background-position:center;opacity:.48}
#${ROOT_ID} .pp-toolbar:after,#${ROOT_ID} .pp-pager:after{content:""!important;position:absolute;inset:0;z-index:0;pointer-events:none;background:radial-gradient(95% 120% at 100% 0%,var(--pp-matrix-glow),transparent 58%);opacity:.82}
#${ROOT_ID} .pp-toolbar>*,#${ROOT_ID} .pp-pager>*{position:relative;z-index:1}
html[data-cw-theme=flat-dark] #${ROOT_ID} .pp-toolbar,html[data-cw-theme=flat-dark] #${ROOT_ID} .pp-pager{--pp-matrix-line:rgba(255,255,255,.055);--pp-matrix-glow:rgba(255,255,255,.030)}
html[data-cw-theme=flat-light] #${ROOT_ID} .pp-toolbar,html[data-cw-theme=flat-light] #${ROOT_ID} .pp-pager{--pp-matrix-line:rgba(16,24,40,.065);--pp-matrix-glow:rgba(76,68,170,.050)}
.pp-loading-grid{min-height:0}.pp-loading-card{cursor:default!important;pointer-events:none}.pp-loading-card:hover{border-color:var(--pp-border)!important;transform:none!important}.pp-loading-card .pp-body:before{opacity:.18!important}.pp-loading-shape{position:relative;display:block;overflow:hidden;background:color-mix(in srgb,var(--pp-soft) 12%,transparent)}.pp-loading-shape:after{content:"";position:absolute;inset:0;background:linear-gradient(110deg,transparent 0%,rgba(255,255,255,.035) 40%,rgba(255,255,255,.16) 50%,rgba(255,255,255,.035) 60%,transparent 100%);transform:translateX(-120%);animation:ppSkeletonShimmer 1.35s ease-in-out infinite}.pp-loading-art{height:100%;background:color-mix(in srgb,var(--pp-soft) 10%,var(--pp-panel-bg-strong))}.pp-loading-copy{display:grid;gap:8px;align-content:start}.pp-loading-line{height:12px;border-radius:999px}.pp-loading-line.title{width:min(62%,240px);height:16px}.pp-loading-line.meta{width:min(40%,150px);opacity:.76}.pp-loading-chip{width:58px;height:22px;border-radius:999px}.pp-loading-progress{gap:5px}.pp-loading-progress .pp-loading-line{width:30%;height:12px}.pp-loading-bar{height:8px;border-radius:999px}.pp-loading-actions{gap:6px;align-self:end}.pp-loading-action{width:58px;height:28px;border-radius:999px}.pp-loading-status{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}.is-loading #pp-refresh .material-symbols-rounded{animation:ppLoadingSpin .8s linear infinite}.is-loading .pp-toolbar{pointer-events:none;transition:opacity .16s ease}.is-initial-loading .pp-toolbar{opacity:.72}.is-initial-loading .pp-status,.is-initial-loading .pp-pager{display:none!important}.cw-compact .pp-loading-card:nth-child(n+4){display:none}@keyframes ppSkeletonShimmer{to{transform:translateX(120%)}}@keyframes ppLoadingSpin{to{transform:rotate(360deg)}}@media(max-width:980px){.pp-loading-card:nth-child(n+4){display:none}}@media(prefers-reduced-motion:reduce){.pp-loading-shape:after,.is-loading #pp-refresh .material-symbols-rounded{animation:none!important}.pp-loading-shape:after{transform:none;opacity:.35}}
html[data-cw-theme=flat-dark] #${ROOT_ID} .pp-loading-shape{background:#2a2f39}html[data-cw-theme=flat-dark] #${ROOT_ID} .pp-loading-art{background:#242a34}html[data-cw-theme=flat-light] #${ROOT_ID} .pp-loading-shape{background:#e2e7ef}html[data-cw-theme=flat-light] #${ROOT_ID} .pp-loading-art{background:#e9edf3}html[data-cw-theme=flat-light] #${ROOT_ID} .pp-loading-shape:after{background:linear-gradient(110deg,transparent 0%,rgba(255,255,255,.16) 40%,rgba(255,255,255,.76) 50%,rgba(255,255,255,.16) 60%,transparent 100%)}
    `;
    document.head.appendChild(Object.assign(document.createElement("style"), { id: STYLE_ID, textContent: css }));
  }

  function root() {
    return document.getElementById(ROOT_ID);
  }

  function shell() {
    return `
      <div class="pp-head">
        <div><div class="pp-title">Playback Progress</div><div class="pp-intro">Manage unfinished playback records across supported providers.</div></div>
        <div class="pp-head-actions"><button class="pp-btn pp-icon-btn" id="pp-settings" title="Playback Progress settings" aria-label="Playback Progress settings">${icon("settings")}</button><button class="pp-btn pp-icon-btn" id="pp-refresh" title="Refresh provider data" aria-label="Refresh provider data">${icon("refresh")}</button></div>
      </div>
      <div class="pp-status" id="pp-status"></div>
      <div class="pp-toolbar">
        <input class="pp-field" id="pp-search" type="search" placeholder="Search">
        <select class="pp-field" id="pp-provider"><option value="">Loading providers...</option></select>
        <select class="pp-field" id="pp-type"><option value="">All Types</option><option value="movie">Movies</option><option value="episode">TV Episodes</option><option value="anime_episode">Anime Episodes</option></select>
        <select class="pp-field" id="pp-progress"><option value="">All Progress</option><option value="0:24.99">Under 25 percent</option><option value="25:50">25 to 50 percent</option><option value="50:75">50 to 75 percent</option><option value="75:100">Over 75 percent</option><option value="90:100">Nearly Finished</option></select>
        <select class="pp-field" id="pp-age"><option value="">All Time</option><option value="today">Today</option><option value="7d">Last 7 Days</option><option value="30d">Last 30 Days</option><option value="older_30d">Older Than 30 Days</option></select>
        <select class="pp-field hidden" id="pp-rating"><option value="">All Ratings</option><option value="6">6 and Higher</option><option value="7">7 and Higher</option><option value="8">8 and Higher</option><option value="9">9 and Higher</option></select>
        <select class="pp-field" id="pp-sort"><option value="last_updated">Last Updated</option><option value="progress_high">Progress High</option><option value="progress_low">Progress Low</option><option value="remaining_time">Remaining Time</option><option value="rating_high">Rating High</option><option value="title">Title</option><option value="provider">Provider</option></select>
      </div>
      <div class="pp-errors hidden" id="pp-errors"></div>
      <div class="pp-loading-status" id="pp-loading-status" role="status" aria-live="polite"></div>
      <div class="pp-grid" id="pp-grid"></div>
      <div class="pp-pager" id="pp-pager"><button class="pp-btn" id="pp-prev">${icon("chevron_left")}</button><span id="pp-page-text"></span><button class="pp-btn" id="pp-next">${icon("chevron_right")}</button></div>
      <div class="pp-bulk hidden" id="pp-bulk"><div class="pp-bulk-left"><strong class="pp-selected-pill"><span id="pp-selected-count" class="pp-selected-number">0</span><span class="pp-selected-label">selected</span></strong><button class="pp-btn" id="pp-select-visible">Select Visible</button><button class="pp-btn" id="pp-select-all">Select All Filtered Results</button><button class="pp-btn" id="pp-clear-selection">Clear Selection</button></div><span class="pp-bulk-divider" aria-hidden="true"></span><div class="pp-bulk-actions"><button class="pp-btn pp-bulk-icon" id="pp-bulk-edit" title="Edit progress" aria-label="Edit progress">${icon("edit")}</button><button class="pp-btn pp-bulk-icon" id="pp-bulk-watch" title="Mark as watched" aria-label="Mark as watched">${icon("check_circle")}</button><button class="pp-btn pp-bulk-icon danger" id="pp-bulk-remove" title="Remove progress" aria-label="Remove progress">${icon("delete")}</button></div></div>
      <div class="pp-modal hidden" id="pp-progress-dialog" role="dialog" aria-modal="true" aria-labelledby="pp-progress-dialog-title">
        <div class="pp-dialog">
          <div><div class="pp-dialog-title" id="pp-progress-dialog-title">Edit Progress</div><div class="pp-dialog-sub" id="pp-progress-dialog-sub"></div></div>
          <div class="pp-progress-edit"><input id="pp-progress-range" type="range" min="2" max="79" step="1"><input id="pp-progress-value" type="number" min="2" max="79" step="1"></div>
          <div class="pp-dialog-error" id="pp-progress-error"></div>
          <div class="pp-dialog-actions"><button class="pp-btn" id="pp-progress-cancel">Cancel</button><button class="pp-btn" id="pp-progress-apply">Apply</button></div>
        </div>
      </div>
      <div class="pp-modal hidden" id="pp-settings-dialog" role="dialog" aria-modal="true" aria-labelledby="pp-settings-title">
        <div class="pp-dialog pp-settings-dialog">
          <div><div class="pp-dialog-title" id="pp-settings-title">Playback Progress Settings</div><div class="pp-dialog-sub">Choose which provider profiles appear on this screen.</div></div>
          <div class="pp-settings-timeout"><span class="pp-dialog-sub pp-settings-timeout-label">Slow provider timeout <span class="pp-help-icon" tabindex="0" title="How many seconds Playback Progress waits for each provider profile before skipping it for this refresh. Slower providers are shown as timed out instead of blocking the whole page.">${icon("help")}</span></span><input id="pp-settings-timeout" type="number" min="3" max="60" step="1"></div>
          <div class="pp-settings-list" id="pp-settings-list"></div>
          <div class="pp-dialog-error" id="pp-settings-error"></div>
          <div class="pp-dialog-actions"><button class="pp-btn" id="pp-settings-cancel">Cancel</button><button class="pp-btn" id="pp-settings-save">Save</button></div>
        </div>
      </div>
      <div class="pp-toast hidden" id="pp-toast"></div>
    `;
  }

  const providerKey = (item) => `${item.provider}:${item.instance_id}`;
  const recordsOf = (item) => Array.isArray(item?.records) && item.records.length ? item.records : [item];
  const recordKey = (item) => item?.is_combined ? `combined:${item.remote_id || item.canonical_key || recordsOf(item).map((r) => `${providerKey(r)}:${r.remote_id}`).join("|")}` : `${providerKey(item)}:${item.remote_id}`;
  const fmtPct = (n) => Number.isFinite(Number(n)) ? `${Math.round(Number(n))}%` : "Unknown";
  const fmtRating = (n) => {
    const value = Number(n);
    if (!Number.isFinite(value) || value <= 0) return "";
    return Number.isInteger(value) ? String(value) : value.toFixed(1).replace(/\.0$/, "");
  };
  const fmtRemaining = (n) => {
    const s = Number(n);
    if (!Number.isFinite(s) || s <= 0) return "";
    const mins = Math.round(s / 60);
    return mins >= 60 ? `${Math.floor(mins / 60)}h ${mins % 60}m left` : `${mins}m left`;
  };
  const fmtDate = (v) => {
    const d = Date.parse(v || "");
    return Number.isFinite(d) ? new Date(d).toLocaleString() : "";
  };
  const fmtPaused = (v) => {
    const d = Date.parse(v || "");
    if (!Number.isFinite(d)) return "";
    const diff = Date.now() - d;
    if (diff >= 0) {
      const mins = Math.floor(diff / 60000);
      if (mins < 1) return "Paused just now";
      if (mins < 60) return `Paused ${mins} min ago`;
      const hours = Math.floor(mins / 60);
      if (hours < 24) return `Paused ${hours}h ago`;
      const days = Math.floor(hours / 24);
      if (days < 31) return `Paused ${days} day${days === 1 ? "" : "s"} ago`;
      const months = Math.floor(days / 30);
      if (months < 12) return `Paused ${months} month${months === 1 ? "" : "s"} ago`;
      const years = Math.floor(days / 365);
      return `Paused ${years} year${years === 1 ? "" : "s"} ago`;
    }
    return `Paused ${new Date(d).toLocaleDateString()}`;
  };
  const liveLabel = (it) => {
    const st = String(it?.live_state || "").toLowerCase();
    if (st === "playing") return "Playing now";
    if (st === "buffering") return "Buffering now";
    if (st === "paused") return fmtPaused((Number(it.live_updated) || 0) ? new Date(Number(it.live_updated) * 1000).toISOString() : "") || "Paused now";
    return "";
  };
  const titleOf = (it) => it.media_type === "movie" ? (it.title || "Untitled") : (it.series_title || it.title || "Untitled");
  const subOf = (it) => {
    if (it.media_type === "movie") return ["Movie", it.year].filter(Boolean).join(" - ");
    const ep = it.season != null && it.episode != null ? `S${String(it.season).padStart(2, "0")}E${String(it.episode).padStart(2, "0")}` : "";
    const type = it.media_type === "anime_episode" ? "Anime-Episode" : "TV-Episode";
    const label = [ep, it.episode_title].filter(Boolean).join(" - ");
    return label ? `${label} (${type})` : type;
  };
  const tmdbArtUrl = (it, size = "w342", kind = "poster") => {
    const media = String(it?.media_type || it?.type || "").toLowerCase();
    const meta = it?.provider_metadata && typeof it.provider_metadata === "object" ? it.provider_metadata : {};
    const showIds = meta.show_ids && typeof meta.show_ids === "object" ? meta.show_ids : {};
    const ids = it?.ids && typeof it.ids === "object" ? it.ids : {};
    const source = media === "movie" ? ids : (showIds.tmdb ? showIds : ids);
    const tmdb = source?.tmdb || it?.tmdb;
    if (!tmdb) return "";
    const typ = media === "movie" ? "movie" : "tv";
    return `/art/tmdb/${typ}/${encodeURIComponent(String(tmdb))}?kind=${encodeURIComponent(kind)}&size=${encodeURIComponent(size)}&locale=${encodeURIComponent(window.__CW_LOCALE || navigator.language || "en-US")}`;
  };
  const providerPills = (it) => {
    const providers = Array.isArray(it.providers) && it.providers.length
      ? it.providers
      : [{ provider: it.provider, provider_label: it.provider_label, instance_id: it.instance_id, instance_label: it.instance_label }];
    return providers.map((p) => `<span class="pp-provider-pill">${providerIcon(p.provider)}${esc(profileLabel(p))}</span>`).join("");
  };
  const profileLabel = (p) => {
    const provider = String(p.provider || "");
    const providerLabel = String(p.provider_label || provider);
    let label = String(p.instance_label || p.instance_id || "").trim();
    for (const prefix of [providerLabel, provider]) {
      if (prefix && label.toLowerCase().startsWith(prefix.toLowerCase())) {
        label = label.slice(prefix.length).trim();
      }
    }
    return label || (String(p.instance_id || "").trim() || "Default");
  };
  const actionRecords = (item, action) => recordsOf(item).filter((it) => {
    if (action === "mark_watched") return it.can_mark_watched;
    if (action === "update_progress") return it.can_update_progress;
    return it.can_remove_progress;
  });
  const actionPayloads = (items, action) => items.flatMap((it) => actionRecords(it, action).map((record) => ({
    provider: record.provider,
    instance_id: record.instance_id,
    remote_id: record.remote_id,
    canonical_key: record.canonical_key,
    record
  })));
  const avgProgress = (items) => {
    const values = items.flatMap((it) => recordsOf(it)).map((it) => Number(it.progress_percent)).filter(Number.isFinite);
    if (!values.length) return 25;
    return Math.max(2, Math.min(79, Math.round(values.reduce((a, b) => a + b, 0) / values.length)));
  };
  const actionTitle = (action) => action === "mark_watched" ? "Mark as Watched" : action === "update_progress" ? "Edit Progress" : "Remove Progress";

  function toast(msg) {
    const el = document.getElementById("pp-toast");
    if (!el) return;
    el.textContent = msg;
    el.classList.remove("hidden");
    clearTimeout(toast._t);
    toast._t = setTimeout(() => el.classList.add("hidden"), 2400);
  }

  function askProgress(defaultValue, count) {
    return new Promise((resolve) => {
      const dlg = document.getElementById("pp-progress-dialog");
      const range = document.getElementById("pp-progress-range");
      const value = document.getElementById("pp-progress-value");
      const sub = document.getElementById("pp-progress-dialog-sub");
      const err = document.getElementById("pp-progress-error");
      const apply = document.getElementById("pp-progress-apply");
      const cancel = document.getElementById("pp-progress-cancel");
      if (!dlg || !range || !value || !sub || !err || !apply || !cancel) return resolve(null);
      const initial = Math.max(2, Math.min(79, Math.round(Number(defaultValue) || 25)));
      range.value = String(initial);
      value.value = String(initial);
      sub.textContent = `${count || 1} provider record${count === 1 ? "" : "s"}`;
      err.textContent = "";
      dlg.classList.remove("hidden");
      value.focus();
      value.select?.();
      const syncFromRange = () => { value.value = range.value; err.textContent = ""; };
      const syncFromValue = () => { range.value = String(Math.max(2, Math.min(79, Math.round(Number(value.value) || initial)))); err.textContent = ""; };
      const done = (result) => {
        dlg.classList.add("hidden");
        range.removeEventListener("input", syncFromRange);
        value.removeEventListener("input", syncFromValue);
        apply.removeEventListener("click", onApply);
        cancel.removeEventListener("click", onCancel);
        dlg.removeEventListener("click", onBackdrop);
        dlg.removeEventListener("keydown", onKey);
        resolve(result);
      };
      const onApply = () => {
        const n = Number(value.value);
        if (!Number.isFinite(n) || n < 2 || n >= 80) {
          err.textContent = "Use 2-79%. Use Watched for completed items.";
          return;
        }
        done(Math.round(n * 100) / 100);
      };
      const onCancel = () => done(null);
      const onBackdrop = (e) => { if (e.target === dlg) done(null); };
      const onKey = (e) => {
        if (e.key === "Escape") done(null);
        if (e.key === "Enter") onApply();
      };
      range.addEventListener("input", syncFromRange);
      value.addEventListener("input", syncFromValue);
      apply.addEventListener("click", onApply);
      cancel.addEventListener("click", onCancel);
      dlg.addEventListener("click", onBackdrop);
      dlg.addEventListener("keydown", onKey);
    });
  }

  async function openSettings() {
    const dlg = document.getElementById("pp-settings-dialog");
    const list = document.getElementById("pp-settings-list");
    const timeout = document.getElementById("pp-settings-timeout");
    const err = document.getElementById("pp-settings-error");
    if (!dlg || !list || !timeout || !err) return;
    err.textContent = "";
    list.innerHTML = `<div class="pp-dialog-sub">Loading provider profiles...</div>`;
    dlg.classList.remove("hidden");
    const data = await api("/api/playback_progress/settings");
    state.settings = data;
    timeout.value = String(Math.round(Number(data.provider_timeout_seconds || 12)));
    const profiles = Array.isArray(data.profiles) ? data.profiles : [];
    if (!profiles.length) {
      list.innerHTML = `<div class="pp-dialog-sub">No compatible provider profiles were found.</div>`;
      return;
    }
    const rows = profiles.map((p) => {
      const key = `${p.provider}:${p.instance_id}`;
      const disabled = p.configured && p.read ? "" : " disabled";
      const status = p.configured && p.read ? "available" : (p.reason || "not configured");
      return `<label class="pp-settings-row">
        <input type="checkbox" data-key="${esc(key)}" data-provider="${esc(p.provider)}" data-instance="${esc(p.instance_id)}"${p.included ? " checked" : ""}${disabled}>
        <span class="pp-settings-main"><span class="pp-settings-name">${providerIcon(p.provider)}${esc(p.instance_label || p.provider_label || key)}</span><span class="pp-settings-sub">${esc(status)}</span></span>
        <span class="pp-settings-pill">${esc(p.provider_label || p.provider)}</span>
      </label>`;
    });
    const tables = [];
    for (let i = 0; i < rows.length; i += 5) {
      tables.push(`<div class="pp-settings-table">${rows.slice(i, i + 5).join("")}</div>`);
    }
    list.innerHTML = tables.join("");
  }

  function closeSettings() {
    document.getElementById("pp-settings-dialog")?.classList.add("hidden");
  }

  async function saveSettings() {
    const dlg = document.getElementById("pp-settings-dialog");
    const list = document.getElementById("pp-settings-list");
    const timeout = document.getElementById("pp-settings-timeout");
    const err = document.getElementById("pp-settings-error");
    if (!dlg || !list || !timeout || !err) return;
    const n = Number(timeout.value);
    if (!Number.isFinite(n) || n < 3 || n > 60) {
      err.textContent = "Use a timeout between 3 and 60 seconds.";
      return;
    }
    const profiles = [...list.querySelectorAll("input[type=checkbox][data-provider]")].map((el) => ({
      provider: el.dataset.provider,
      instance_id: el.dataset.instance,
      included: el.checked
    }));
    const res = await api("/api/playback_progress/settings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ profiles, provider_timeout_seconds: n })
    });
    if (!res.ok) {
      err.textContent = res.message || res.error || "Settings could not be saved.";
      return;
    }
    closeSettings();
    toast("Playback Progress settings saved");
    state.selected.clear();
    await load(true);
  }

  function providerOptions() {
    const readable = state.providers.filter((p) => p.read && p.configured && p.included !== false);
    const opts = ['<option value="">All Providers</option>'];
    readable.forEach((p) => opts.push(`<option value="${esc(p.provider)}:${esc(p.instance_id)}">${esc(p.instance_label || p.provider_label)}</option>`));
    document.getElementById("pp-provider").innerHTML = opts.join("");
    const cur = state.filters.provider;
    if ([...document.getElementById("pp-provider").options].some((o) => o.value === cur)) document.getElementById("pp-provider").value = cur;
  }

  function loadingCard() {
    return `<article class="pp-card pp-loading-card" aria-hidden="true">
      <div class="pp-art pp-loading-art pp-loading-shape"></div>
      <div class="pp-body">
        <div class="pp-top"><div class="pp-card-head">
          <div class="pp-title-wrap pp-loading-copy"><span class="pp-loading-line title pp-loading-shape"></span><span class="pp-loading-line meta pp-loading-shape"></span></div>
          <div class="pp-card-side"><span class="pp-loading-chip pp-loading-shape"></span></div>
        </div></div>
        <div class="pp-progress pp-loading-progress"><div class="pp-progress-row"><span class="pp-loading-line pp-loading-shape"></span></div><span class="pp-loading-bar pp-loading-shape"></span></div>
        <div class="pp-actions pp-loading-actions"><span class="pp-loading-action pp-loading-shape"></span><span class="pp-loading-action pp-loading-shape"></span></div>
      </div>
    </article>`;
  }

  function renderInitialLoading() {
    const grid = document.getElementById("pp-grid");
    const status = document.getElementById("pp-loading-status");
    document.getElementById("pp-errors")?.classList.add("hidden");
    document.getElementById("pp-bulk")?.classList.add("hidden");
    if (status) status.textContent = "Refreshing Playback Progress from configured providers.";
    if (grid) {
      grid.classList.add("pp-loading-grid");
      grid.innerHTML = Array.from({ length: 6 }, loadingCard).join("");
    }
  }

  function setLoadingState(loading, initial = false) {
    const el = root();
    if (!el) return;
    el.classList.toggle("is-loading", loading);
    el.classList.toggle("is-initial-loading", loading && initial);
    loading ? el.setAttribute("aria-busy", "true") : el.removeAttribute("aria-busy");
    el.querySelectorAll(".pp-toolbar .pp-field").forEach((field) => { field.disabled = loading; });
    const refresh = document.getElementById("pp-refresh");
    if (refresh) refresh.disabled = loading;
    if (!loading) {
      const status = document.getElementById("pp-loading-status");
      if (status) status.textContent = "";
    }
  }

  function renderStatus() {
    const wrap = document.getElementById("pp-status");
    const configured = state.providers.filter((p) => p.configured && p.read);
    const readable = configured.filter((p) => p.included !== false);
    if (readable.length) {
      wrap.classList.add("hidden");
      wrap.innerHTML = "";
      return;
    }
    wrap.innerHTML = `<div class="pp-status-message">${configured.length ? "Enable at least one provider profile in Playback Progress settings." : "Connect at least one compatible provider to view playback progress."}</div>`;
    wrap.classList.remove("hidden");
  }

  function renderErrors() {
    const el = document.getElementById("pp-errors");
    if (!state.errors.length) return el.classList.add("hidden");
    el.innerHTML = state.errors.map((e) => `${esc(e.provider || "Provider")} ${esc(e.instance_id || "")}: ${esc(e.message || e.error_code || "failed")}`).join("<br>");
    el.classList.remove("hidden");
  }

  function card(it) {
    const key = recordKey(it);
    const selected = state.selected.has(key) ? " selected" : "";
    const displayProgress = Number.isFinite(Number(it.live_progress_percent)) ? it.live_progress_percent : it.progress_percent;
    const pct = Math.max(0, Math.min(100, Number(displayProgress || 0)));
    const remaining = fmtRemaining(Number.isFinite(Number(it.live_remaining_seconds)) ? it.live_remaining_seconds : it.remaining_seconds);
    const directPoster = String(it.poster_url || "").trim();
    const metadataPoster = tmdbArtUrl(it, "w342");
    const posterImg = directPoster || metadataPoster || "/assets/img/placeholder_poster.svg";
    const posterFallback = directPoster && metadataPoster && directPoster !== metadataPoster ? metadataPoster : "";
    const posterFallbackAttr = posterFallback ? ` data-fallback-src="${esc(posterFallback)}"` : "";
    const posterKey = `${posterImg}|${posterFallback}`;
    const backdropImg = it.backdrop_url || tmdbArtUrl(it, "w780", "backdrop");
    const backdropStyle = backdropImg ? ` style="--pp-backdrop:url('${esc(backdropImg)}')"` : "";
    const live = liveLabel(it);
    const paused = live || fmtPaused(it.updated_at || it.progress_at);
    const ratingText = fmtRating(it.rating);
    const ratingChip = ratingText ? `<span class="pp-rating-chip" title="Rating ${esc(ratingText)}" aria-label="Rating ${esc(ratingText)}">${icon("star")}${esc(ratingText)}</span>` : "";
    const timing = [remaining ? `<span>${esc(remaining)}</span>` : "", paused ? `<span class="${live ? "pp-live" : "pp-paused"}">${esc(paused)}</span>` : ""].filter(Boolean).join("");
    const actionWatch = it.can_mark_watched ? `<button class="pp-btn pp-action-btn pp-action-watch" data-action="watch" data-key="${esc(key)}" title="Mark as watched" aria-label="Mark as watched">${icon("check_circle")}<span>Watched</span></button>` : "";
    const actionEdit = it.can_update_progress ? `<button class="pp-btn pp-action-btn pp-action-edit" data-action="edit" data-key="${esc(key)}" title="Edit progress" aria-label="Edit progress">${icon("edit")}<span>Edit</span></button>` : "";
    const actionRemove = it.can_remove_progress ? `<button class="pp-btn pp-action-btn pp-action-remove" data-action="remove" data-key="${esc(key)}" title="Remove progress" aria-label="Remove progress">${icon("delete")}<span>Remove</span></button>` : "";
    return `<article class="pp-card${selected}" data-key="${esc(key)}" role="checkbox" aria-checked="${state.selected.has(key) ? "true" : "false"}" tabindex="0"${backdropStyle}>
      <div class="pp-art"><img src="${esc(posterImg)}"${posterFallbackAttr} data-poster-key="${esc(posterKey)}" alt="" loading="lazy" decoding="async" onerror="const fallback=this.dataset.fallbackSrc;if(fallback){delete this.dataset.fallbackSrc;this.src=fallback}else{this.onerror=null;this.src='/assets/img/placeholder_poster.svg'}"></div>
      <div class="pp-body">
        <div class="pp-top">
          <div class="pp-card-head">
            <div class="pp-title-wrap"><div class="pp-card-title">${esc(titleOf(it))}</div><div class="pp-card-sub">${esc(subOf(it))}</div></div>
            <div class="pp-card-side"><div class="pp-provider-stack">${providerPills(it)}</div>${ratingChip}</div>
          </div>
        </div>
        <div class="pp-progress"><div class="pp-progress-row"><strong>${esc(fmtPct(displayProgress))} watched</strong><span class="pp-timing">${timing}</span></div><div class="pp-bar"><span style="width:${pct}%"></span></div></div>
        <div class="pp-actions">${actionEdit}${actionWatch}${actionRemove}</div>
      </div>
    </article>`;
  }

  function updateBulk() {
    const bulk = document.getElementById("pp-bulk");
    const count = state.selected.size;
    document.getElementById("pp-selected-count").textContent = String(count);
    bulk.classList.toggle("hidden", count === 0);
  }

  function renderItems(preserveArtwork = true) {
    const grid = document.getElementById("pp-grid");
    grid.classList.remove("pp-loading-grid");
    const ratingFilter = document.getElementById("pp-rating");
    ratingFilter.classList.toggle("hidden", !state.items.some((it) => Number(it.rating) > 0));
    const markup = state.items.length ? state.items.map(card).join("") : `<div class="pp-empty">No playback records found.</div>`;
    if (preserveArtwork && state.items.length && grid.children.length) {
      const existing = new Map(
        [...grid.querySelectorAll(".pp-card[data-key]")].map((cardEl) => [cardEl.dataset.key, cardEl])
      );
      const template = document.createElement("template");
      template.innerHTML = markup;
      template.content.querySelectorAll(".pp-card[data-key]").forEach((nextCard) => {
        const currentCard = existing.get(nextCard.dataset.key);
        const currentImg = currentCard?.querySelector(".pp-art img[data-poster-key]");
        const nextImg = nextCard.querySelector(".pp-art img[data-poster-key]");
        if (currentImg && nextImg && currentImg.dataset.posterKey === nextImg.dataset.posterKey) {
          nextImg.replaceWith(currentImg);
        }
      });
      grid.replaceChildren(template.content);
    } else {
      grid.innerHTML = markup;
    }
    const maxPage = Math.max(1, Math.ceil((state.total || 0) / state.pageSize));
    document.getElementById("pp-page-text").textContent = `${state.page} / ${maxPage} - ${state.total} total`;
    document.getElementById("pp-prev").disabled = state.page <= 1;
    document.getElementById("pp-next").disabled = state.page >= maxPage;
    updateBulk();
  }

  function query(force = false, all = false) {
    const params = new URLSearchParams();
    const [provider, instance] = String(state.filters.provider || "").split(":");
    if (provider) params.set("provider", provider);
    if (instance) params.set("instance_id", instance);
    if (state.filters.media_type) params.set("media_type", state.filters.media_type);
    if (state.filters.age) params.set("age", state.filters.age);
    if (state.filters.rating) params.set("rating_min", state.filters.rating);
    if (state.filters.search) params.set("search", state.filters.search);
    if (state.filters.sort) params.set("sort", state.filters.sort);
    if (state.filters.progress) {
      const [min, max] = state.filters.progress.split(":");
      if (min) params.set("progress_min", min);
      if (max) params.set("progress_max", max);
    }
    params.set("page", all ? "1" : String(state.page));
    params.set("page_size", all ? "250" : String(state.pageSize));
    if (force) params.set("force_refresh", "true");
    return params;
  }

  async function load(force = false) {
    if (state.busy) return;
    const initial = !state.loaded;
    state.busy = true;
    setLoadingState(true, initial);
    if (initial) renderInitialLoading();
    try {
      const data = await api(`/api/playback_progress/items?${query(force).toString()}`);
      state.items = Array.isArray(data.items) ? data.items : [];
      state.providers = Array.isArray(data.providers) ? data.providers : [];
      state.errors = Array.isArray(data.errors) ? data.errors : [];
      state.total = Number(data.total || 0);
      state.page = Number(data.page || 1);
      providerOptions();
      renderStatus();
      renderErrors();
      renderItems(!force);
      state.loaded = true;
    } catch (e) {
      state.items = [];
      state.errors = [{ provider: "Playback Progress", message: String(e?.message || e || "Request failed") }];
      state.total = 0;
      const status = document.getElementById("pp-status");
      if (status) {
        status.innerHTML = "";
        status.classList.add("hidden");
      }
      renderErrors();
      renderItems();
    } finally {
      state.busy = false;
      setLoadingState(false);
    }
  }

  async function act(action, item) {
    const bulkAction = action === "watch" ? "mark_watched" : action === "edit" ? "update_progress" : "remove_progress";
    const payloads = actionPayloads([item], bulkAction);
    let progressPercent = null;
    if (bulkAction === "update_progress") {
      if (!payloads.length) return toast("Edit Progress is unsupported for this record.");
      progressPercent = await askProgress(avgProgress([item]), payloads.length);
      if (progressPercent == null) return;
    }
    if (payloads.length > 1 || item.is_combined) {
      if (!payloads.length) return toast("Action is unsupported for this record.");
      const res = await api("/api/playback_progress/actions/bulk", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ action: bulkAction, progress_percent: progressPercent, items: payloads }) });
      toast(`Successful ${res.successful || 0}, failed ${res.failed || 0}, unsupported ${res.unsupported || 0}`);
      if ((res.successful || 0) > 0) {
        state.selected.delete(recordKey(item));
        await load(true);
      }
      return;
    }
    const url = action === "watch" ? "/api/playback_progress/actions/mark_watched" : action === "edit" ? "/api/playback_progress/actions/update_progress" : "/api/playback_progress/actions/remove";
    const record = recordsOf(item)[0] || item;
    const body = { provider: record.provider, instance_id: record.instance_id, remote_id: record.remote_id, canonical_key: record.canonical_key, progress_percent: progressPercent, record };
    const res = await api(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
    toast(res.message || (res.ok ? "Done" : "Action failed"));
    if (res.ok) {
      state.selected.delete(recordKey(item));
      await load(true);
    }
  }

  async function bulk(action) {
    const selected = [...state.selected.values()];
    if (!selected.length) return;
    const allRecords = selected.flatMap((it) => recordsOf(it));
    const payloads = actionPayloads(selected, action);
    const unsupported = allRecords.length - payloads.length;
    if (!payloads.length) return toast(`${actionTitle(action)} is unsupported for the selected records.`);
    let progressPercent = null;
    if (action === "update_progress") {
      progressPercent = await askProgress(avgProgress(selected), payloads.length);
      if (progressPercent == null) return;
    } else if (!confirm(`${actionTitle(action)} for ${payloads.length} eligible provider record(s)? ${unsupported ? `${unsupported} unsupported provider record(s) will be skipped.` : ""}`)) return;
    const res = await api("/api/playback_progress/actions/bulk", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ action, progress_percent: progressPercent, items: payloads }) });
    toast(`Successful ${res.successful || 0}, failed ${res.failed || 0}, unsupported ${res.unsupported || 0}`);
    state.selected.clear();
    await load(true);
  }

  function bind() {
    const r = root();
    const update = (key, val) => { state.filters[key] = val; state.page = 1; load(false); };
    r.addEventListener("change", (e) => {
      const t = e.target;
      if (!t) return;
      if (t.id === "pp-provider") update("provider", t.value);
      if (t.id === "pp-type") update("media_type", t.value);
      if (t.id === "pp-progress") update("progress", t.value);
      if (t.id === "pp-age") update("age", t.value);
      if (t.id === "pp-rating") update("rating", t.value);
      if (t.id === "pp-sort") update("sort", t.value);
    }, true);
    r.addEventListener("input", (e) => {
      if (e.target?.id !== "pp-search") return;
      clearTimeout(bind._search);
      bind._search = setTimeout(() => update("search", e.target.value), 180);
    }, true);
    r.addEventListener("click", async (e) => {
      const btn = e.target?.closest?.("button");
      if (btn) {
        if (btn.id === "pp-settings") return openSettings();
        if (btn.id === "pp-settings-cancel") return closeSettings();
        if (btn.id === "pp-settings-save") return saveSettings();
        if (btn.id === "pp-refresh") return load(true);
        if (btn.id === "pp-prev" && state.page > 1) { state.page--; return load(false); }
        if (btn.id === "pp-next") { state.page++; return load(false); }
        if (btn.id === "pp-select-visible") { state.items.forEach((it) => state.selected.set(recordKey(it), it)); renderItems(); return; }
        if (btn.id === "pp-clear-selection") { state.selected.clear(); renderItems(); return; }
        if (btn.id === "pp-select-all") {
          const data = await api(`/api/playback_progress/items?${query(false, true).toString()}`);
          (data.items || []).forEach((it) => state.selected.set(recordKey(it), it));
          renderItems();
          return;
        }
        if (btn.id === "pp-bulk-watch") return bulk("mark_watched");
        if (btn.id === "pp-bulk-edit") return bulk("update_progress");
        if (btn.id === "pp-bulk-remove") return bulk("remove_progress");
        if (btn.dataset?.action && btn.dataset?.key) {
          const item = state.items.find((it) => recordKey(it) === btn.dataset.key);
          if (item) return act(btn.dataset.action, item);
        }
        return;
      }
      const settingsDialog = e.target?.closest?.("#pp-settings-dialog");
      if (settingsDialog && e.target === settingsDialog) {
        closeSettings();
        return;
      }
      const card = e.target?.closest?.(".pp-card[data-key]");
      if (card) {
        const item = state.items.find((it) => recordKey(it) === card.dataset.key);
        if (!item) return;
        if (state.selected.has(card.dataset.key)) state.selected.delete(card.dataset.key);
        else state.selected.set(card.dataset.key, item);
        renderItems();
      }
    }, true);
    r.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && !document.getElementById("pp-settings-dialog")?.classList.contains("hidden")) {
        closeSettings();
        return;
      }
      if (e.key !== " " && e.key !== "Enter") return;
      const card = e.target?.closest?.(".pp-card[data-key]");
      if (!card) return;
      e.preventDefault();
      const item = state.items.find((it) => recordKey(it) === card.dataset.key);
      if (!item) return;
      if (state.selected.has(card.dataset.key)) state.selected.delete(card.dataset.key);
      else state.selected.set(card.dataset.key, item);
      renderItems();
    }, true);
  }

  function mount() {
    ensureStyle();
    const el = root();
    if (!el) return;
    if (!state.mounted) {
      el.innerHTML = shell();
      bind();
      state.mounted = true;
    }
    load(false);
  }

  window.PlaybackProgress = { mount, refresh: () => load(true) };
  document.addEventListener("tab-changed", (e) => {
    if ((e.detail?.id || e.detail?.tab) === "playback_progress") mount();
  });
  window.addEventListener("currently-watching-updated", () => {
    const page = document.getElementById("page-playback_progress");
    if (page && !page.classList.contains("hidden")) load(false);
  });
  const mountIfActive = () => {
    const page = document.getElementById("page-playback_progress");
    const tab = document.getElementById("tab-playback_progress");
    if (page && (!page.classList.contains("hidden") || tab?.classList.contains("active"))) mount();
  };
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", mountIfActive, { once: true });
  else mountIfActive();
})();
