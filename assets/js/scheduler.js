/* assets/js/scheduler.js */
/* CrossWatch - Advanced Scheduling UI */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(() => {
  "use strict";
  if (window.__SCHED_UI_INIT__) return; window.__SCHED_UI_INIT__ = true;
  const authSetupPending = () => window.cwIsAuthSetupPending?.() === true;

  // tiny helpers
  const $ = (s, r = document) => r.querySelector(s);
  const el = (t, c) => Object.assign(document.createElement(t), c ? { className: c } : {});

  const fieldKey = (value, fallback = "field") => String(value || fallback).replace(/[^a-z0-9_-]+/gi, "_");

  // stable id (UUID when possible)
  const genId = (() => {
    const withCrypto = () => {
      try {
        const b = new Uint8Array(16); crypto.getRandomValues(b);
        b[6] = (b[6] & 0x0f) | 0x40; b[8] = (b[8] & 0x3f) | 0x80;
        const h = [...b].map(x => x.toString(16).padStart(2, "0"));
        return `${h.slice(0,4).join("")}-${h.slice(4,6).join("")}-${h.slice(6,8).join("")}-${h.slice(8,10).join("")}-${h.slice(10).join("")}`;
      } catch { return null; }
    };
    return () => crypto?.randomUUID?.() || withCrypto() || `id_${Date.now().toString(36)}_${Math.random().toString(36).slice(2,10)}`;
  })();

  // styles (once)
  document.head.appendChild(Object.assign(el("style"), { id: "sch-css", textContent: `
#sec-scheduling{--sch-shell-bg:radial-gradient(120% 140% at 0% 0%,rgba(90,74,201,.11) 0%,rgba(90,74,201,0) 42%),radial-gradient(110% 130% at 100% 100%,rgba(41,110,214,.08) 0%,rgba(41,110,214,0) 44%),linear-gradient(180deg,rgba(8,11,18,.985),rgba(3,5,10,.99));--sch-card-bg:linear-gradient(180deg,rgba(10,13,21,.92),rgba(4,6,11,.96));--sch-card-bg-soft:linear-gradient(180deg,rgba(14,18,30,.84),rgba(5,7,13,.9));--sch-border:rgba(255,255,255,.08);--sch-border-soft:rgba(255,255,255,.055);--sch-shadow:0 22px 56px rgba(0,0,0,.4),inset 0 1px 0 rgba(255,255,255,.03);--sch-fg:#f3f7ff;--sch-fg-soft:rgba(208,217,233,.7)}
#sec-scheduling .cw-subpanel[data-sub]{padding-top:6px}
#sec-scheduling .cw-subpanel[data-sub="basic"] .auth-card,#schAdv{position:relative;border:1px solid var(--sch-border);border-radius:22px;background:var(--sch-shell-bg);box-shadow:var(--sch-shadow);overflow:hidden}
#sec-scheduling .cw-subpanel[data-sub="basic"] .auth-card::before,#schAdv::before{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(135deg,rgba(255,255,255,.045),transparent 38%),radial-gradient(90% 120% at 0% 0%,rgba(92,76,204,.1),transparent 52%)}
#sec-scheduling .cw-subpanel[data-sub="basic"] .auth-card-fields{position:relative;z-index:1;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;padding:16px}
#sec-scheduling .cw-subpanel[data-sub="basic"] .field{display:grid;gap:8px;padding:14px 16px;border:1px solid var(--sch-border-soft);border-radius:18px;background:var(--sch-card-bg);box-shadow:inset 0 1px 0 rgba(255,255,255,.025)}
#sec-scheduling .cw-subpanel[data-sub="basic"] .field:first-child{grid-column:1 / -1}
#sec-scheduling .cw-subpanel[data-sub="basic"] .field>.muted{margin:0!important;font-size:11px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:rgba(214,223,238,.58)}
#sec-scheduling .cw-subpanel[data-sub="basic"] .field>.muted .th-help{display:inline-flex;align-items:center;gap:8px}
#sec-scheduling .cw-subpanel[data-sub="basic"] .sch-help{position:relative;display:inline-flex;align-items:center;justify-content:center;width:28px;height:28px;border-radius:999px;border:1px solid rgba(255,255,255,.12);background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.025));color:#edf4ff;cursor:help;box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
#sec-scheduling .cw-subpanel[data-sub="basic"] .sch-help::before{content:"help";font-family:"Material Symbols Rounded","Material Symbols Outlined","Segoe UI Symbol",sans-serif;font-size:18px;line-height:1}
#sec-scheduling .cw-subpanel[data-sub="basic"] .sch-help:focus-visible{outline:none;box-shadow:0 0 0 3px rgba(101,107,255,.12),inset 0 1px 0 rgba(255,255,255,.03)}
#sec-scheduling .cw-subpanel[data-sub="basic"] .auth-card-notes{margin-top:0;color:var(--sch-fg-soft);font-size:12px;line-height:1.45}
#sec-scheduling .cw-subpanel[data-sub="basic"] select,#sec-scheduling .cw-subpanel[data-sub="basic"] input[type=time],#sec-scheduling .cw-subpanel[data-sub="basic"] input[type=number],.sch-adv select,.sch-adv input[type=time],.sch-adv input[type=number],.sch-adv input[type=text]{width:100%;min-height:46px;padding:0 14px;border:1px solid rgba(255,255,255,.08);border-radius:16px;background:linear-gradient(180deg,rgba(4,6,11,.94),rgba(2,4,8,.98));color:var(--sch-fg);box-shadow:inset 0 1px 0 rgba(255,255,255,.02);transition:border-color .18s ease,background .18s ease,box-shadow .18s ease,transform .18s ease}
#sec-scheduling .cw-subpanel[data-sub="basic"] select:hover,#sec-scheduling .cw-subpanel[data-sub="basic"] input[type=time]:hover,#sec-scheduling .cw-subpanel[data-sub="basic"] input[type=number]:hover,.sch-adv select:hover,.sch-adv input[type=time]:hover,.sch-adv input[type=number]:hover,.sch-adv input[type=text]:hover{border-color:rgba(255,255,255,.13);background:linear-gradient(180deg,rgba(7,10,18,.96),rgba(3,5,10,.985))}
#sec-scheduling .cw-subpanel[data-sub="basic"] select:focus,#sec-scheduling .cw-subpanel[data-sub="basic"] input[type=time]:focus,#sec-scheduling .cw-subpanel[data-sub="basic"] input[type=number]:focus,.sch-adv select:focus,.sch-adv input[type=time]:focus,.sch-adv input[type=number]:focus,.sch-adv input[type=text]:focus{outline:none;border-color:rgba(122,120,255,.42);box-shadow:0 0 0 3px rgba(101,107,255,.12),inset 0 1px 0 rgba(255,255,255,.025)}
.sch-adv{padding:16px}
.sch-adv .cw-panel-head{position:relative;z-index:1;display:flex;align-items:center;min-height:104px;margin:0 0 14px;padding:14px 16px;border:none;border-radius:0;background:transparent;box-shadow:none}
.sch-adv .cw-panel-head .cx-toggle{margin-top:20px}
.sch-adv .mini,.sch-adv .status{position:relative;z-index:1}
.sch-adv .mini{font-size:12px;color:var(--sch-fg-soft)}
.sch-adv .status{display:flex;align-items:center;min-height:22px;font-size:12px;font-weight:700;color:rgba(214,223,238,.82)}
.sch-adv .status:empty,.sch-adv .status.is-empty{display:none!important}
.sch-adv-section{position:relative;z-index:1;display:grid;gap:10px;margin-top:14px;padding:14px;border:1px solid var(--sch-border-soft);border-radius:18px;background:var(--sch-card-bg-soft);box-shadow:inset 0 1px 0 rgba(255,255,255,.02)}
.sch-adv-section:first-of-type{margin-top:0}
.sch-adv-section-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;flex-wrap:wrap}
.sch-adv-section-title{font-size:12px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:#f7f9ff}
.sch-adv-section-copy{max-width:70ch;font-size:12px;line-height:1.45;color:var(--sch-fg-soft)}
.sch-adv table{position:relative;z-index:1;width:100%;border-collapse:separate;border-spacing:0 10px;margin-top:0;table-layout:fixed}
.sch-adv thead th{padding:0 10px 6px;text-align:left;font-size:11px;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:rgba(214,223,238,.56);border-bottom:none}
.sch-adv .th-help{display:inline-flex;align-items:center;gap:8px}
.sch-adv .sch-help{position:relative;display:inline-flex;align-items:center;justify-content:center;width:28px;height:28px;border-radius:999px;border:1px solid rgba(255,255,255,.12);background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.025));color:#edf4ff;cursor:help;box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
.sch-adv .sch-help::before{content:"help";font-family:"Material Symbols Rounded","Material Symbols Outlined","Segoe UI Symbol",sans-serif;font-size:18px;line-height:1}
.sch-adv .sch-help:focus-visible{outline:none;box-shadow:0 0 0 3px rgba(101,107,255,.12),inset 0 1px 0 rgba(255,255,255,.03)}
.sch-adv tbody tr{background:var(--sch-card-bg);box-shadow:inset 0 1px 0 rgba(255,255,255,.02)}
.sch-adv tbody td{position:relative;overflow:visible;padding:12px 10px;vertical-align:middle;border-top:1px solid var(--sch-border-soft);border-bottom:1px solid var(--sch-border-soft)}
.sch-adv tbody td:first-child{border-left:1px solid var(--sch-border-soft);border-radius:18px 0 0 18px}
.sch-adv tbody td:last-child{border-right:1px solid var(--sch-border-soft);border-radius:0 18px 18px 0}
.sch-adv tbody tr.capture-detail-row{background:transparent;box-shadow:none}
.sch-adv tbody tr.capture-detail-row td{padding:0 8px 10px;border:none}
.sch-adv tbody tr.capture-detail-row td:first-child,.sch-adv tbody tr.capture-detail-row td:last-child{border:none;border-radius:0}
.sch-adv .capture-detail-card{display:grid;gap:12px;padding:14px 16px 16px;margin:0 0 2px;border:1px solid var(--sch-border-soft);border-top:none;border-radius:0 0 16px 16px;background:linear-gradient(180deg,rgba(12,15,25,.86),rgba(5,7,13,.94));box-shadow:inset 0 1px 0 rgba(255,255,255,.02)}
.sch-adv .capture-detail-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}
.sch-adv .capture-row-actions{display:flex;align-items:center;justify-content:flex-end;gap:8px}
.sch-adv .btn.ghost.capture-adv-toggle{display:inline-flex;align-items:center;justify-content:center;width:26px;min-width:26px;height:26px;min-height:26px;padding:0;border-radius:50%;aspect-ratio:1 / 1;border:1px solid rgba(255,255,255,.12);background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.025));color:#d7deef;box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
.sch-adv .btn.ghost.capture-adv-toggle:hover{border-color:rgba(255,255,255,.18);background:linear-gradient(180deg,rgba(110,112,255,.16),rgba(255,255,255,.04));color:#f2f6ff}
.sch-adv .capture-adv-toggle .material-symbols-rounded{font-size:14px;line-height:1;font-variation-settings:"FILL" 0,"wght" 400,"GRAD" 0,"opsz" 20}
.sch-adv .chipdays{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;align-items:center}
.sch-adv .chipdays label{display:inline-flex;align-items:center;justify-content:center;gap:7px;min-height:38px;padding:0 10px;border:1px solid var(--sch-border-soft);border-radius:999px;cursor:pointer;width:100%;background:var(--sch-card-bg-soft);color:rgba(236,241,251,.78);font-size:12px;font-weight:700;transition:border-color .18s ease,background .18s ease,transform .18s ease,color .18s ease}
.sch-adv .chipdays label:hover{transform:translateY(-1px);border-color:rgba(255,255,255,.13);background:linear-gradient(180deg,rgba(16,20,33,.92),rgba(7,9,15,.96))}
.sch-adv .chipdays input{accent-color:#7c76ff;transform:translateY(1px)}
.sch-adv .chipdays label:has(input:checked){color:#f7f9ff;border-color:rgba(122,120,255,.34);background:linear-gradient(180deg,rgba(89,86,196,.22),rgba(16,18,34,.96))}
.sch-adv .chipdays .chipspacer{width:100%;height:1px;visibility:hidden;pointer-events:none}
.sch-adv .stack{display:grid;gap:8px}
.sch-adv .stack.two{grid-template-columns:repeat(2,minmax(0,1fr))}
.sch-adv .stack.three{grid-template-columns:repeat(3,minmax(0,1fr))}
.sch-adv .subnote{font-size:10px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:rgba(214,223,238,.5)}
.sch-adv .field-mini{display:grid;gap:6px;min-width:0}
.sch-adv .capture-provider-stack{grid-template-columns:minmax(190px,1.4fr) minmax(150px,1fr)}
.sch-adv td[data-label="Feature"]{min-width:150px}
.sch-adv td[data-label="Days"]{min-width:330px}
.sch-adv td[data-label="Label template"]{min-width:170px}
.sch-adv td[data-label="Label template"] input{min-width:0;width:100%}
.sch-adv td[data-label="Source"]{min-width:260px}
.sch-adv td[data-label="Action"]{min-width:240px}
.sch-adv td[data-label="Source"] .stack,.sch-adv td[data-label="Action"] .stack{width:100%}
.sch-adv td[data-label="Source"] select,.sch-adv td[data-label="Action"] select{width:100%!important;min-width:0!important}
.sch-adv td[data-label="Action"] .cw-icon-select-text{display:flex;align-items:center;min-width:0}
.sch-adv td[data-label="Action"] .cw-icon-select-badges{flex-wrap:nowrap;white-space:nowrap}
.sch-adv td[data-label="Source"] select,.sch-adv td[data-label="Event"] select{min-width:146px}
.sch-adv .event-filter-stack{width:100%}
.sch-adv .event-filter-stack select,.sch-adv .event-filter-stack input{width:100%!important;min-width:0!important}
.sch-adv .checkline{display:flex;align-items:center;gap:8px;min-height:18px;font-size:12px;color:var(--sch-fg-soft)}
.sch-adv .checkline input{width:16px;height:16px;accent-color:#7c76ff}
.sch-adv .row-disabled{opacity:.5;filter:grayscale(.24)}
.sch-adv option[disabled]{color:#666}
.sch-adv-actions{position:relative;z-index:1;display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-top:12px}
.sch-adv-actions .status{margin-left:auto;justify-content:flex-end;text-align:right;max-width:100%}
.sch-adv-actions .status.inline{display:inline-flex;align-items:center;justify-content:flex-end;gap:8px;min-height:36px;padding:6px 12px;border:1px solid rgba(255,255,255,.08);border-radius:999px;background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.02));box-shadow:inset 0 1px 0 rgba(255,255,255,.03);flex:0 0 auto;max-width:min(100%,fit-content);white-space:nowrap;color:rgba(231,238,250,.86)}
.sch-adv-actions .status.inline::before{content:"info";font-family:"Material Symbols Rounded","Material Symbols Outlined","Segoe UI Symbol",sans-serif;font-size:16px;line-height:1;color:rgba(198,210,236,.74)}
.sch-adv .btn,.sch-adv .btn.ghost{min-height:40px;padding:0 14px;border-radius:999px;border:1px solid rgba(255,255,255,.09);background:linear-gradient(180deg,rgba(255,255,255,.065),rgba(255,255,255,.03));color:var(--sch-fg);box-shadow:inset 0 1px 0 rgba(255,255,255,.03);transition:transform .18s ease,background .18s ease,border-color .18s ease}
.sch-adv .btn:hover,.sch-adv .btn.ghost:hover{transform:translateY(-1px);border-color:rgba(255,255,255,.14);background:linear-gradient(180deg,rgba(110,112,255,.16),rgba(255,255,255,.04))}
.sch-adv tbody .btn.ghost{min-width:38px;padding:0 12px}
.sch-adv td[data-label="Remove"]{width:52px;min-width:52px;text-align:left;padding-left:2px;padding-right:12px}
.sch-adv td[data-label="Remove"] .sch-remove-btn{margin-left:-2px}
.sch-adv .btn.ghost.sch-remove-btn{display:inline-flex;align-items:center;justify-content:center;width:26px;min-width:26px;height:26px;min-height:26px;padding:0;border-radius:50%;aspect-ratio:1 / 1;border:1px solid rgba(255,92,92,.32);background:rgba(255,72,72,.14);color:#ffb3b3;box-shadow:inset 0 1px 0 rgba(255,255,255,.04)}
.sch-adv .btn.ghost.sch-remove-btn:hover{border-color:rgba(255,112,112,.48);background:rgba(255,72,72,.22);color:#ffd7d7}
.sch-adv .btn.ghost.sch-remove-btn .material-symbols-rounded{font-size:14px;line-height:1;font-variation-settings:"FILL" 0,"wght" 400,"GRAD" 0,"opsz" 20}
.sch-adv.adv-disabled{opacity:.55;filter:saturate(.75)}
.sch-std-toggle{margin-top:0}
@media (max-width:980px){#sec-scheduling .cw-subpanel[data-sub="basic"] .auth-card-fields{grid-template-columns:1fr}.sch-adv .chipdays{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media (max-width:760px){.sch-adv{padding:14px}.sch-adv .cw-panel-head{min-height:0;padding:14px}.sch-adv .cw-panel-head .cx-toggle{margin-top:18px}.sch-adv table,.sch-adv thead,.sch-adv tbody,.sch-adv tr,.sch-adv td,.sch-adv th{display:block}.sch-adv thead{display:none}.sch-adv tbody{display:grid;gap:10px}.sch-adv tbody tr{border:1px solid var(--sch-border-soft);border-radius:18px;overflow:hidden}.sch-adv tbody tr.capture-detail-row{border:none;border-radius:0;overflow:visible}.sch-adv tbody td{display:grid;gap:6px;border:none!important;border-radius:0!important;padding:10px 12px}.sch-adv tbody td[data-label]::before{content:attr(data-label);font-size:10px;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:rgba(214,223,238,.56)}.sch-adv .capture-detail-row td[data-label]::before{content:none}.sch-adv .capture-detail-card{margin-top:-10px;border-top:1px solid var(--sch-border-soft);border-radius:0 0 18px 18px}.sch-adv .capture-detail-grid{grid-template-columns:1fr}.sch-adv .chipdays{grid-template-columns:repeat(2,minmax(0,1fr))}.sch-adv .stack.two,.sch-adv .stack.three,.sch-adv .event-filter-grid{grid-template-columns:1fr}}
` }));
  document.head.appendChild(Object.assign(el("style"), { id: "sch-css-refine", textContent: `
#sec-scheduling .cw-subpanel[data-sub="basic"] .auth-card{display:grid;gap:0}
#sec-scheduling .sch-std-head{position:relative;z-index:1;display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:end;gap:16px;padding:18px 18px 12px!important}
#sec-scheduling .sch-std-head-copy{display:grid;gap:6px}
#sec-scheduling .sch-std-kicker{font-size:11px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:rgba(214,223,238,.58)}
#sec-scheduling .sch-std-title{font-size:26px;font-weight:900;letter-spacing:-.02em;color:#f3f7ff}
#sec-scheduling .sch-std-copy{max-width:64ch;font-size:13px;line-height:1.5;color:rgba(208,217,233,.72)}
#sec-scheduling .cw-subpanel[data-sub="basic"] .auth-card-fields{padding-top:12px}
#sec-scheduling .cw-subpanel[data-sub="basic"] .field{min-width:0}
#sec-scheduling .cw-subpanel[data-sub="basic"] .field select,
#sec-scheduling .cw-subpanel[data-sub="basic"] .field input[type=time],
#sec-scheduling .cw-subpanel[data-sub="basic"] .field input[type=number]{min-height:44px}
#sec-scheduling .cw-subpanel[data-sub="basic"] .field:first-child{display:grid;grid-template-columns:minmax(0,1fr);align-content:start}
#sec-scheduling .cw-subpanel[data-sub="basic"] .field.sch-std-enable-field{display:none}
#sec-scheduling .sch-std-head .cx-toggle{margin-top:16px;justify-self:end}
#sec-scheduling .sch-std-wizard{position:relative;z-index:1;display:grid;gap:18px;padding:0 18px 18px!important}
#sec-scheduling .sch-plan-summary,#sec-scheduling .sch-std-summary{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;padding:12px 14px;border:1px solid rgba(255,255,255,.08);border-radius:18px;background:linear-gradient(180deg,rgba(13,18,31,.82),rgba(6,8,14,.92));box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
#sec-scheduling .sch-std-summary{margin-top:6px!important}
#sec-scheduling .sch-plan-summary-copy,#sec-scheduling .sch-std-summary-copy{display:grid;gap:4px}
#sec-scheduling .sch-plan-summary-kicker,#sec-scheduling .sch-std-summary-kicker{font-size:10px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:rgba(214,223,238,.54)}
#sec-scheduling .sch-plan-summary-text,#sec-scheduling .sch-std-summary-text{font-size:14px;font-weight:800;color:#f4f7ff}
#sec-scheduling .sch-plan-summary-note,#sec-scheduling .sch-std-summary-note{font-size:12px;line-height:1.45;color:rgba(208,217,233,.7)}
#sec-scheduling .sch-plan-summary-state,#sec-scheduling .sch-std-summary-state{display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:999px;border:1px solid rgba(122,120,255,.2);background:rgba(94,89,205,.12);font-size:11px;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:#ebefff}
#sec-scheduling .sch-plan-summary-state.attention,#sec-scheduling .sch-std-summary-state.attention{border-color:rgba(255,176,32,.26);background:rgba(77,49,0,.26);color:#ffe6b2}
#sec-scheduling .sch-std-step{display:grid;gap:10px;padding:16px;border:1px solid var(--sch-border-soft);border-radius:18px;background:var(--sch-card-bg-soft);box-shadow:inset 0 1px 0 rgba(255,255,255,.02)}
#sec-scheduling .sch-std-step-head{display:grid;gap:4px}
#sec-scheduling .sch-std-step-kicker{font-size:10px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:rgba(214,223,238,.54)}
#sec-scheduling .sch-std-step-title{font-size:18px;font-weight:900;letter-spacing:-.01em;color:#f5f8ff}
#sec-scheduling .sch-std-step-copy{font-size:12px;line-height:1.5;color:rgba(208,217,233,.7)}
#sec-scheduling .cw-subpanel[data-sub="basic"] .field.sch-std-field-hidden{display:none!important}
#sec-scheduling .cw-subpanel[data-sub="basic"] .field.sch-std-primary{border-color:rgba(122,120,255,.24);background:linear-gradient(180deg,rgba(20,24,40,.9),rgba(7,10,18,.96))}
#sec-scheduling .cw-subpanel[data-sub="basic"] .field.sch-std-active{border-color:rgba(122,120,255,.3);box-shadow:0 0 0 1px rgba(122,120,255,.08),inset 0 1px 0 rgba(255,255,255,.02)}
#sec-scheduling .sch-std-inline{display:grid;grid-template-columns:minmax(0,.9fr) minmax(140px,.7fr);gap:10px}
#sec-scheduling .sch-std-inline > *{min-width:0}
#sec-scheduling .sch-std-warning{display:none;gap:8px;padding:14px 16px;border:1px solid rgba(255,176,32,.26);border-radius:18px;background:linear-gradient(180deg,rgba(77,49,0,.26),rgba(30,18,0,.3));box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
#sec-scheduling .sch-std-warning.is-visible{display:grid}
#sec-scheduling .sch-std-warning-title{font-size:12px;font-weight:900;letter-spacing:.08em;text-transform:uppercase;color:#ffd780}
#sec-scheduling .sch-std-warning-copy{font-size:12px;line-height:1.5;color:rgba(255,232,196,.84)}
.sch-adv{display:grid;gap:10px}
.sch-adv .cw-panel-head{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:end;gap:16px;min-height:0;padding:18px 18px 8px;border:none;border-radius:0;background:transparent;box-shadow:none}
.sch-adv-head-copy{display:grid;gap:6px}
.sch-adv-head-kicker{font-size:11px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:rgba(214,223,238,.58)}
.sch-adv-head-title{font-size:26px;font-weight:900;letter-spacing:-.02em;color:#f3f7ff}
.sch-adv-head-copy p{margin:0;max-width:64ch;font-size:13px;line-height:1.5;color:rgba(208,217,233,.72)}
.sch-adv .cw-panel-head .cx-toggle{margin-top:16px;justify-self:end}
.sch-adv-summary{margin:0 18px 4px}
.sch-adv-section{gap:12px;padding:16px}
.sch-adv-section-head{align-items:center}
.sch-adv-section-copy{font-size:12px;line-height:1.5}
.sch-adv table{border-spacing:0 8px}
.sch-adv thead th{padding:0 8px 6px}
.sch-adv tbody td{padding:10px 8px}
.sch-adv tbody td:first-child{border-radius:16px 0 0 16px}
.sch-adv tbody td:last-child{border-radius:0 16px 16px 0}
.sch-adv select,.sch-adv input[type=time],.sch-adv input[type=number],.sch-adv input[type=text]{min-height:42px;padding:0 12px;border-radius:14px}
.sch-adv .stack{gap:7px}
.sch-adv .stack.two{grid-template-columns:minmax(0,1fr) minmax(118px,.92fr)}
.sch-adv .stack.three{grid-template-columns:repeat(3,minmax(0,1fr))}
.sch-adv .field-mini{gap:5px}
.sch-adv .field-mini .subnote{font-size:10px;letter-spacing:.09em}
.sch-adv .field-mini.control-align .subnote{visibility:hidden;user-select:none}
.sch-adv .capture-detail-card .checkline{min-height:42px;padding:0 12px;border:1px solid rgba(255,255,255,.08);border-radius:14px;background:linear-gradient(180deg,rgba(4,6,11,.94),rgba(2,4,8,.98))}
.sch-adv .capture-provider-stack{grid-template-columns:minmax(220px,1.45fr) minmax(160px,1fr)}
.sch-adv .chipdays{gap:7px}
.sch-adv .chipdays label{min-height:36px;padding:0 9px}
.sch-adv-actions{gap:8px}
.sch-adv .btn,.sch-adv .btn.ghost{min-height:38px;padding:0 13px}
@media (max-width:1180px){.sch-adv .stack.two{grid-template-columns:1fr}}
@media (max-width:980px){#sec-scheduling .sch-std-head{grid-template-columns:1fr;padding-bottom:4px}#sec-scheduling .sch-std-head .cx-toggle{justify-self:start}#sec-scheduling .sch-std-inline{grid-template-columns:1fr}.sch-adv .cw-panel-head{grid-template-columns:1fr}.sch-adv .cw-panel-head .cx-toggle{justify-self:start}}
@media (max-width:760px){#sec-scheduling .sch-std-title{font-size:22px}#sec-scheduling .sch-std-wizard{padding:0 16px 16px}.sch-adv .cw-panel-head{padding:16px}.sch-adv-head-title{font-size:22px}.sch-adv .cw-panel-head .cx-toggle{margin-top:10px}}
` }));

  // state
  let _pairs = [], _jobs = [], _captureJobs = [], _eventRules = [], _advEnabled = false, _loading = false;
  let _captureProviders = [];
  let _eventRoutes = { watcher: [], webhook: [] }, _eventRouteError = "";
  const DAY = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"];
  const EVENT_SOURCE_OPTIONS = [["watcher", "Watcher"], ["webhook", "Webhook"]];
  const EVENT_NAME_OPTIONS = [["start", "Start"], ["pause", "Pause"], ["stop", "Stop"]];
  const EVENT_MEDIA_OPTIONS = [["", "Any"], ["movie", "Movie"], ["episode", "Episode"]];
  const HELP_TIPS = {
    std_enabled: "Use standard plan:\nTurn on the standard timer-based scheduler.\nWhen enabled, the advanced plan is turned off.",
    std_frequency: "Frequency:\nChoose how often the standard plan runs.\nUse Every hour, Every N hours, Daily at a fixed time, or a Custom interval.",
    std_every_n_hours: "Every N hours:\nSet how many hours to wait between runs when Frequency is set to Every N hours.",
    std_time: "Time:\nChoose the local time to run each day when Frequency is set to Daily at a fixed time.",
    std_custom_interval: "Custom interval:\nSet a custom repeat interval in minutes or hours.\nCustom schedules are clamped to a minimum of 15 minutes.",
    time_pair: "Pair:\nChoose which enabled sync pair this timed step should run.",
    time_time: "Time:\nChoose the local time when this step becomes due.",
    time_days: "Days:\nSelect which weekdays this step may run on.\nLeave all days unchecked to allow every day.",
    time_after: "After:\nOptional dependency.\nUse this to run the step only after another earlier step has completed.",
    capture_target: "Provider:\nChoose the provider profile to capture from.\nOnly configured snapshot-capable profiles are listed.",
    capture_feature: "Feature:\nPick one feature or All to store a full provider capture bundle.",
    capture_label: "Label template:\nOptional template for automated capture labels.\nUse placeholders like {provider}, {instance}, {feature}, {date}, {time}, {datetime}, or {stamp}.",
    capture_retention_days: "Keep captures for days:\nDelete scheduled captures older than this many days.\nLeave empty or 0 to keep by age forever.",
    capture_max_captures: "Max captures to keep:\nKeep only the newest scheduled captures up to this count.\nLeave empty or 0 for no count limit.",
    capture_cleanup: "Cleanup:\nWhen enabled, older scheduled captures are pruned after each successful scheduled capture using the limits above.",
    source: "Source:\nChoose where the trigger comes from.\nThen choose the exact watcher or webhook route under it.",
    event: "Event:\nChoose which playback activity should trigger the rule.\nStart: playback begins or resumes.\nPause: playback is paused.\nStop: playback ends or stops.",
    filters: "Filters:\nMedia: only movies or episodes.\nMin %: require minimum playback progress.",
    action: "Action:\nChoose what happens when the rule matches.\nSync pair:\nRun one specific enabled sync pair immediately.",
    guardrails: "Mute (min):\nIgnore new triggers for this rule after it runs.\nDedupe (sec):\nSuppress identical repeated events for a short window.\nMax / hour:\nHard safety cap for this rule in one hour."
  };
  const defaultJob = () => ({ id: genId(), pair_id: null, at: null, days: [], after: null, active: true });
  const defaultCaptureJob = () => ({
    id: genId(),
    provider: "",
    instance: "default",
    feature: "",
    at: null,
    days: [],
    label_template: "",
    retention_days: 0,
    max_captures: 0,
    auto_delete_old: false,
    active: true
  });
  const defaultEventRule = () => ({
    id: genId(),
    source: _eventRoutes.watcher?.length ? "watcher" : _eventRoutes.webhook?.length ? "webhook" : "watcher",
    event: "stop",
    filters: {
      route_id: "",
      provider: "",
      provider_instance: "",
      media_type: "",
      min_progress: null
    },
    action: {
      kind: "sync_pair",
      pair_id: null
    },
    guardrails: {
      cooldown_minutes: 15,
      dedupe_window_seconds: 30,
      max_runs_per_hour: 4
    },
    active: true
  });
  const setBooleanSelect = (sel, v) => {
    if (!sel) return;
    const want = v ? "true" : "false";
    const opts = [...(sel.options || [])];
    let hit = opts.find(o => String(o.value).trim().toLowerCase() === want);
    if (!hit) {
      const labels = v ? ["enabled","enable","on","yes","true","1"] : ["disabled","disable","off","no","false","0"];
      hit = opts.find(o => labels.includes(String(o.textContent).trim().toLowerCase()));
    }
    if (hit) sel.value = hit.value;
  };

  const asInt = (value, fallback, minimum = null) => {
    let out = parseInt(value, 10);
    if (!Number.isFinite(out)) out = fallback;
    if (minimum != null && out < minimum) out = minimum;
    return out;
  };
  const refreshSelectUi = (node) => {
    if (!node) return;
    try { window.CW?.IconSelect?.enhance?.(node, node.__cwIconSelectCfg || { className: "cw-plain-select" }); } catch {}
  };
  const setSelectValue = (node, value) => {
    if (!node) return;
    node.value = value;
    refreshSelectUi(node);
  };

  const normalizeStandardMode = (mode) => {
    const raw = String(mode || "").trim().toLowerCase();
    if (raw === "hourly" || raw === "every_hour") return "hourly";
    if (raw === "daily" || raw === "daily_at" || raw === "daily_time") return "daily_time";
    if (raw === "custom" || raw === "custom_interval" || raw === "custom_minutes" || raw === "interval") return "custom_interval";
    if (raw === "every_n_hours") return "every_n_hours";
    return "hourly";
  };

  const getStandardField = (id) => $("#" + id)?.closest(".field") || null;
  const describeMinutes = (minutes) => {
    const safe = Math.max(15, asInt(minutes, 60, 15));
    if (safe % 60 === 0) return `${safe / 60} hour${safe === 60 ? "" : "s"}`;
    return `${safe} minutes`;
  };
  const describeHours = (hours) => {
    const safe = Math.max(2, asInt(hours, 12, 1));
    return `${safe} hour${safe === 1 ? "" : "s"}`;
  };
  const getCustomIntervalMinutes = () => {
    const value = asInt($("#schCustomValue")?.value || "60", 60, 1);
    const unit = String($("#schCustomUnit")?.value || "minutes").trim().toLowerCase();
    return Math.max(15, unit === "hours" ? value * 60 : value);
  };
  const setCustomIntervalFromMinutes = (minutes) => {
    const safe = Math.max(15, asInt(minutes, 60, 15));
    const valueEl = $("#schCustomValue");
    const unitEl = $("#schCustomUnit");
    if (!valueEl || !unitEl) return;
    setSelectValue(unitEl, "minutes");
    valueEl.min = "15";
    valueEl.step = "15";
    valueEl.value = String(safe);
  };
  const ensureStandardWizard = () => {
    const basic = $("#sec-scheduling .cw-subpanel[data-sub='basic'] .auth-card");
    if (!basic) return null;
    let wizard = $("#schStdWizard", basic);
    if (!wizard) {
      wizard = el("div", "sch-std-wizard");
      wizard.id = "schStdWizard";
      wizard.innerHTML = `
        <section class="sch-std-summary" id="schStdSummary">
          <div class="sch-std-summary-copy">
            <div class="sch-std-summary-kicker">Current plan</div>
            <div class="sch-std-summary-text" id="schStdSummaryText"></div>
            <div class="sch-std-summary-note" id="schStdSummaryNote"></div>
          </div>
          <div class="sch-std-summary-state" id="schStdSummaryState">Idle</div>
        </section>
        <section class="sch-std-step" id="schStdStep1">
          <div class="sch-std-step-head">
            <div class="sch-std-step-kicker">Step 1</div>
            <div class="sch-std-step-title">Choose a frequency</div>
            <div class="sch-std-step-copy">Start with the type of cadence you want. We only show the next input that matters for that choice.</div>
          </div>
        </section>
        <section class="sch-std-step" id="schStdStep2">
          <div class="sch-std-step-head">
            <div class="sch-std-step-kicker">Step 2</div>
            <div class="sch-std-step-title" id="schStdDetailTitle">Review the schedule</div>
            <div class="sch-std-step-copy" id="schStdDetailCopy">Choose the additional timing details for the selected frequency.</div>
          </div>
        </section>
        <section class="sch-std-warning" id="schStdWarning" aria-live="polite">
          <div class="sch-std-warning-title">CAUTION</div>
          <div class="sch-std-warning-copy" id="schStdWarningCopy"></div>
        </section>
      `;
      const fieldsWrap = basic.querySelector(".auth-card-fields");
      if (fieldsWrap) {
        fieldsWrap.style.padding = "0";
        fieldsWrap.style.gap = "0";
        fieldsWrap.after(wizard);
      }
      else basic.appendChild(wizard);
    }

    const step1 = $("#schStdStep1", wizard);
    const step2 = $("#schStdStep2", wizard);
    const modeField = getStandardField("schMode");
    const everyField = getStandardField("schN");
    const timeField = getStandardField("schTime");
    const customField = getStandardField("schCustomValue");
    if (modeField && step1 && !step1.contains(modeField)) step1.appendChild(modeField);
    [everyField, timeField, customField].forEach((field) => {
      if (field && step2 && !step2.contains(field)) step2.appendChild(field);
    });
    const customRow = $("#schCustomValue")?.parentElement;
    if (customRow) customRow.classList.add("sch-std-inline");
    return wizard;
  };
  const updateStandardWizard = () => {
    const wizard = ensureStandardWizard();
    if (!wizard) return;
    const mode = normalizeStandardMode($("#schMode")?.value || "hourly");
    const everyField = getStandardField("schN");
    const timeField = getStandardField("schTime");
    const customField = getStandardField("schCustomValue");
    const summaryText = $("#schStdSummaryText", wizard);
    const summaryNote = $("#schStdSummaryNote", wizard);
    const summaryState = $("#schStdSummaryState", wizard);
    const detailTitle = $("#schStdDetailTitle", wizard);
    const detailCopy = $("#schStdDetailCopy", wizard);
    const warning = $("#schStdWarning", wizard);
    const warningCopy = $("#schStdWarningCopy", wizard);
    const stdOn = String($("#schEnabled")?.value || "").trim().toLowerCase() === "true";
    const advOn = !!$("#schAdvEnabled")?.checked;

    if ($("#schMode")) setSelectValue($("#schMode"), mode);
    if ($("#schCustomUnit")?.value === "hours") {
      const val = asInt($("#schCustomValue")?.value || "1", 1, 1);
      $("#schCustomValue").min = "1";
      $("#schCustomValue").step = "1";
      $("#schCustomValue").value = String(val);
    } else if ($("#schCustomValue")) {
      const val = asInt($("#schCustomValue")?.value || "60", 60, 15);
      $("#schCustomValue").min = "15";
      $("#schCustomValue").step = "15";
      $("#schCustomValue").value = String(val);
    }
    if ($("#schN")) {
      $("#schN").min = "2";
      $("#schN").step = "1";
      $("#schN").value = String(asInt($("#schN")?.value || "12", 12, 2));
    }
    if ($("#schCustomUnit")) refreshSelectUi($("#schCustomUnit"));

    [everyField, timeField, customField].forEach((field) => field?.classList.add("sch-std-field-hidden"));
    getStandardField("schMode")?.classList.add("sch-std-primary");
    [getStandardField("schMode"), everyField, timeField, customField].forEach((field) => field?.classList.remove("sch-std-active"));

    let summary = "Runs at the top of every hour";
    let note = "Good for frequent syncs without extra setup.";
    let detailHeading = "Hourly schedule";
    let detailBody = "No extra timing fields are needed. CrossWatch will queue the sync at the start of each hour.";

    if (mode === "every_n_hours") {
      const hours = Math.max(2, asInt($("#schN")?.value || "12", 12, 1));
      if ($("#schN")) $("#schN").value = String(hours);
      everyField?.classList.remove("sch-std-field-hidden");
      everyField?.classList.add("sch-std-active");
      summary = `Runs every ${describeHours(hours)}`;
      note = "Use this when hourly is too aggressive but you still want a repeating interval.";
      detailHeading = "Set the hour gap";
      detailBody = "Choose how many hours to wait between syncs. Use Every hour instead when you want a 60 minute cadence.";
    } else if (mode === "daily_time") {
      const time = $("#schTime")?.value || "03:30";
      timeField?.classList.remove("sch-std-field-hidden");
      timeField?.classList.add("sch-std-active");
      summary = `Runs daily at ${time}`;
      note = "Best when you want one predictable sync window each day.";
      detailHeading = "Pick the daily run time";
      detailBody = "Choose the local time when the daily sync should become due.";
    } else if (mode === "custom_interval") {
      const minutes = getCustomIntervalMinutes();
      customField?.classList.remove("sch-std-field-hidden");
      customField?.classList.add("sch-std-active");
      summary = `Runs every ${describeMinutes(minutes)}`;
      note = minutes < 60
        ? "Short custom intervals can look like aggressive polling to provider APIs."
        : "Use custom timing when you need something between the built-in hourly and daily options.";
      detailHeading = "Define the custom interval";
      detailBody = "Pick minutes or hours. CrossWatch enforces a 15 minute minimum for custom schedules.";
      if (warning && warningCopy) {
        warning.classList.toggle("is-visible", minutes < 60);
        warningCopy.textContent = "Custom schedules shorter than 1 hour can be seen as abusing trackers API's and may result in a ban. Use them carefully.";
      }
    }

    if (mode !== "custom_interval" && warning) warning.classList.remove("is-visible");
    if (summaryText) summaryText.textContent = summary;
    if (summaryNote) summaryNote.textContent = note;
    if (summaryState) summaryState.textContent = advOn ? "Advanced plan active" : stdOn ? "Standard plan active" : "Saved but off";
    if (detailTitle) detailTitle.textContent = detailHeading;
    if (detailCopy) detailCopy.textContent = detailBody;
  };
  const wireStandardWizard = () => {
    const ids = ["schMode", "schN", "schTime", "schCustomValue", "schCustomUnit"];
    ids.forEach((id) => {
      const node = $("#" + id);
      if (!node || node.dataset.stdWizardWired === "1") return;
      const handler = () => {
        updateStandardWizard();
        try { window.cwSchedSettingsHubUpdate?.(); } catch {}
      };
      node.addEventListener("change", handler);
      node.addEventListener("input", handler);
      node.dataset.stdWizardWired = "1";
    });
  };

const ensureStdEnabledToggle = () => {
  const sel = $("#schEnabled");
  if (!sel || sel.__toggleEnhanced) return;
  const box = sel.parentElement;
  if (!box) return;
  box.classList.add("sch-std-enable-field");

  const lab = box.querySelector("label");
  if (lab) lab.remove();
  sel.style.display = "none";

  const t = el("label", "cx-toggle sch-std-toggle");
  t.innerHTML = `<input type="checkbox" id="schEnabledToggle"><span class="cx-toggle-ui" aria-hidden="true"></span><span class="cx-toggle-text">Use standard plan</span><span class="cx-toggle-state" aria-hidden="true"></span>`;
  const head = $("#sec-scheduling .sch-std-head");
  (head || box).appendChild(t);

  const cb = $("#schEnabledToggle", head || box);
  const syncFromSel = () => { cb.checked = String(sel.value || "").trim().toLowerCase() === "true"; };
  sel.__toggleSync = syncFromSel;
  syncFromSel();

  cb.onchange = () => {
    // Standard on -> force advanced off
    if (cb.checked) {
      const advCb = $("#schAdvEnabled");
      if (advCb && advCb.checked) advCb.checked = false;
      _advEnabled = !!$("#schAdvEnabled")?.checked;
    }

    setBooleanSelect(sel, cb.checked);
    try { sel.dispatchEvent(new Event("change", { bubbles: true })); } catch {}
    try { sel.dispatchEvent(new Event("input", { bubbles: true })); } catch {}

    applyModeLocks();
  };

  sel.addEventListener("change", syncFromSel);
  sel.__toggleEnhanced = true;
};

  const setAdvDisabled = (disabled) => {
    const adv = $("#schAdv");
    if (!adv) return;
    adv.classList.toggle("adv-disabled", !!disabled);
    [...adv.querySelectorAll("select,input,button")].forEach((n) => {
      if (n && n.id === "schAdvEnabled") return;
      n.disabled = !!disabled;
    });
  };

  const decorateStandardPanel = () => {
    const basic = $("#sec-scheduling .cw-subpanel[data-sub='basic'] .auth-card");
    if (!basic || basic.dataset.schDecorated === "1") return;
    const head = el("div", "sch-std-head");
    head.innerHTML = `
      <div class="sch-std-head-copy">
        <div class="sch-std-kicker">Standard plan</div>
        <div class="sch-std-title">Scheduler setup wizard</div>
        <div class="sch-std-copy">Pick a cadence, fill in the matching timing field, and leave the rest out of the way. Switch to Advanced for chained steps.</div>
      </div>
    `;
    basic.prepend(head);
    ensureStandardWizard();
    basic.dataset.schDecorated = "1";
  };

  const decorateStandardFieldHelp = () => {
    const basic = $("#sec-scheduling .cw-subpanel[data-sub='basic'] .auth-card");
    if (!basic) return;
    const tipsByLabel = {
      Enable: "std_enabled",
      Frequency: "std_frequency",
      "Every N hours": "std_every_n_hours",
      Time: "std_time",
      "Custom interval": "std_custom_interval"
    };
    basic.querySelectorAll(".field > .muted").forEach((label) => {
      if (!label || label.dataset.helpDecorated === "1") return;
      const text = String(label.textContent || "").trim();
      const helpKey = tipsByLabel[text];
      if (!helpKey) return;
      const tip = HELP_TIPS[helpKey] || "";
      const wrap = el("span", "th-help");
      const textNode = document.createElement("span");
      textNode.textContent = text;
      wrap.appendChild(textNode);
      const btn = el("button", "sch-help");
      btn.type = "button";
      btn.setAttribute("aria-label", `${text} help`);
      if (tip) btn.title = tip;
      btn.onclick = (e) => {
        e.preventDefault();
        e.stopPropagation();
      };
      wrap.appendChild(btn);
      label.textContent = "";
      label.appendChild(wrap);
      label.dataset.helpDecorated = "1";
    });
  };

  const applyModeLocks = () => {
    const sel = $("#schEnabled");
    const stdToggle = $("#schEnabledToggle");
    const advCb = $("#schAdvEnabled");

    const stdEnabled = String(sel?.value || "").trim().toLowerCase() === "true";
    const advEnabled = !!advCb?.checked;

    if (advEnabled) {
      // Advanced on -> force standard off
      if (stdEnabled) {
        setBooleanSelect(sel, false);
        try { sel.dispatchEvent(new Event("change", { bubbles: true })); } catch {}
        try { sel.dispatchEvent(new Event("input", { bubbles: true })); } catch {}
      }
      if (stdToggle) stdToggle.checked = false;
    } else if (stdEnabled) {
      // Standard on -> force advanced off
      if (advCb && advCb.checked) advCb.checked = false;
      _advEnabled = !!advCb?.checked;
    }

    const advOn = !!advCb?.checked;
    const stdOn = String(sel?.value || "").trim().toLowerCase() === "true";

    // Lock standard fields when advanced is on
    const lockStdFields = advOn;
    ["schMode", "schN", "schTime", "schCustomValue", "schCustomUnit"].forEach((id) => {
      const n = $("#" + id);
      if (n) n.disabled = lockStdFields;
    });

    // Lock advanced fields when standard is on 
    setAdvDisabled(stdOn);

    updateStandardWizard();
    try { updateAdvancedStatus(); } catch {}

    try { window.refreshSchedulingBanner?.(); } catch {}
    try { window.cwSchedSettingsHubUpdate?.(); } catch {}
  };

  // data
  const fetchPairs = async () => {
    if (authSetupPending()) { _pairs = []; return; }
    try {
      const r = await fetch("/api/pairs", { cache: "no-store" });
      const arr = await r.json();
      _pairs = Array.isArray(arr) ? arr.map(p => ({
        id: String(p.id),
        label: `${String(p.source || "").toUpperCase()} -> ${String(p.target || "").toUpperCase()} ${String(p.mode || "")}`.trim(),
        enabled: !!p.enabled,
        source: String(p.source || "").trim().toLowerCase(),
        target: String(p.target || "").trim().toLowerCase(),
        source_instance: String(p.source_instance || "default").trim() || "default",
        target_instance: String(p.target_instance || "default").trim() || "default",
        mode: String(p.mode || "").trim(),
      })) : [];
    } catch (e) { console.warn("[scheduler] /api/pairs failed", e); _pairs = []; }
  };
  const fetchCaptureProviders = async () => {
    _captureProviders = [];
    if (authSetupPending()) return;
    try {
      const res = await fetch("/api/snapshots/manifest", { cache: "no-store" });
      const data = await res.json();
      const rows = Array.isArray(data?.providers) ? data.providers : [];
      _captureProviders = rows.map((row) => {
        const features = row && typeof row.features === "object" ? row.features : {};
        const supported = ["watchlist", "ratings", "history", "progress"].filter((feature) => !!features?.[feature]);
        return {
          id: String(row?.id || "").trim().toUpperCase(),
          label: String(row?.label || row?.id || "").trim() || String(row?.id || "").trim().toUpperCase(),
          configured: !!row?.configured,
          features: supported,
          instances: Array.isArray(row?.instances) ? row.instances.map((inst) => ({
            id: String(inst?.id || "default").trim() || "default",
            label: String(inst?.label || inst?.id || "Default").trim() || "Default",
            configured: inst?.configured !== false
          })) : []
        };
      }).filter((row) => row.id);
    } catch (e) {
      console.warn("[scheduler] /api/snapshots/manifest failed", e);
      _captureProviders = [];
    }
  };
  const eventRoutesFor = source => Array.isArray(_eventRoutes?.[source]) ? _eventRoutes[source] : [];
  const eventSourceReady = source => eventRoutesFor(source).length > 0;
  const eventSourceLabel = source => String(source || "").trim().toLowerCase() === "webhook" ? "Webhook" : "Watcher";
  const findEventRoute = (source, routeId) => eventRoutesFor(source).find(r => String(r?.id || "") === String(routeId || ""));
  const fetchEventRoutes = async () => {
    _eventRoutes = { watcher: [], webhook: [] };
    _eventRouteError = "";
    if (authSetupPending()) return;
    try {
      const res = await fetch("/api/scrobble/event_routes", { cache: "no-store" });
      const data = await res.json();
      _eventRoutes = {
        watcher: Array.isArray(data?.watcher_routes) ? data.watcher_routes.filter(r => r && typeof r === "object" && String(r.id || "").trim()) : [],
        webhook: Array.isArray(data?.webhook_routes) ? data.webhook_routes.filter(r => r && typeof r === "object" && String(r.id || "").trim()) : []
      };
      if (!eventSourceReady("watcher") && !eventSourceReady("webhook")) _eventRouteError = "No enabled watcher or webhook routes configured for event triggers.";
    } catch (e) {
      console.warn("[scheduler] /api/scrobble/event_routes failed", e);
      _eventRouteError = "Unable to load watcher or webhook routes for event triggers.";
    }
  };
  const sourceOptions = current => EVENT_SOURCE_OPTIONS.map(([value, label]) => [
    value,
    eventSourceReady(value) ? label : `${label} (unavailable)`,
    eventSourceReady(value) ? null : { disabled: true, selected: String(current || "") === value }
  ]);
  const routeOptions = (source, current) => {
    const routes = eventRoutesFor(source);
    const emptyLabel = source === "webhook" ? "Select webhook" : "Select route";
    const missingLabel = source === "webhook" ? "No webhooks configured" : "No routes configured";
    const options = [["", routes.length ? emptyLabel : missingLabel]];
    if (current && !routes.some(r => String(r?.id || "") === String(current))) options.push([current, `Missing route (${current})`, { disabled: true, selected: true }]);
    routes.forEach(route => options.push([route.id, route.label || route.id]));
    return options;
  };
  const eventRuleIssue = rule => {
    const pairId = String(rule?.action?.pair_id || "").trim();
    const routeId = String(rule?.filters?.route_id || "").trim();
    const hasContent = !!pairId || !!routeId;
    if (!hasContent) return "";
    if (!eventSourceReady(rule?.source || "")) return `Select an enabled source for event trigger ${rule?.id || ""}.`.trim();
    if (!routeId) return `Select a ${eventSourceLabel(rule?.source).toLowerCase()} route for each event trigger.`;
    if (!pairId) return "Select a sync pair for each event trigger.";
    return "";
  };
  const serializableEventRules = () => {
    const out = [];
    const issues = [];
    _eventRules.forEach(rule => {
      const issue = eventRuleIssue(rule);
      if (issue) {
        issues.push(issue);
        return;
      }
      const pairId = String(rule?.action?.pair_id || "").trim();
      const routeId = String(rule?.filters?.route_id || "").trim();
      if (!pairId && !routeId) return;
      out.push(rule);
    });
    return { rules: out, issues };
  };
  const syncRuleRoute = rule => {
    if (!rule?.filters) rule.filters = {};
    if (findEventRoute(rule.source, rule.filters.route_id || "")) return;
    rule.filters.route_id = "";
  };
  const isEnabled = pid => !!_pairs.find(p => String(p.id) === String(pid) && p.enabled);
  const normalizeEventRule = (rule = {}) => {
    const filters = rule && typeof rule.filters === "object" ? rule.filters : {};
    const action = rule && typeof rule.action === "object" ? rule.action : {};
    const guardrails = rule && typeof rule.guardrails === "object" ? rule.guardrails : {};
    const def = defaultEventRule();
    return {
      id: rule.id || genId(),
      source: ["watcher", "webhook"].includes(String(rule.source || "").trim().toLowerCase()) ? String(rule.source).trim().toLowerCase() : def.source,
      event: ["start", "pause", "stop"].includes(String(rule.event || "").trim().toLowerCase()) ? String(rule.event).trim().toLowerCase() : def.event,
      filters: {
        route_id: String(filters.route_id || filters.routeId || "").trim(),
        provider: String(filters.provider || "").trim().toLowerCase(),
        provider_instance: String(filters.provider_instance || filters.providerInstance || "").trim(),
        media_type: ["movie", "episode"].includes(String(filters.media_type || "").trim().toLowerCase()) ? String(filters.media_type).trim().toLowerCase() : "",
        min_progress: filters.min_progress === "" || filters.min_progress == null ? null : Math.max(0, Math.min(100, parseInt(filters.min_progress, 10) || 0))
      },
      action: {
        kind: "sync_pair",
        pair_id: String(action.pair_id || action.pairId || "").trim() || null
      },
      guardrails: {
        cooldown_minutes: Math.max(0, parseInt(guardrails.cooldown_minutes ?? def.guardrails.cooldown_minutes, 10) || 0),
        dedupe_window_seconds: Math.max(0, parseInt(guardrails.dedupe_window_seconds ?? def.guardrails.dedupe_window_seconds, 10) || 0),
        max_runs_per_hour: Math.max(0, parseInt(guardrails.max_runs_per_hour ?? def.guardrails.max_runs_per_hour, 10) || 0)
      },
      active: rule.active !== false
    };
  };
  const tdCell = (label, ...children) => {
    const td = el("td");
    td.dataset.label = label;
    td.append(...children);
    return td;
  };
  const stackWrap = (cls, ...children) => {
    const wrap = el("div", cls);
    wrap.append(...children);
    return wrap;
  };
  const buildSelect = ({ id, value, options, onChange }) => {
    const sel = el("select");
    sel.id = id;
    sel.name = id;
    options.forEach(([optValue, label, extra]) => {
      sel.appendChild(Object.assign(el("option"), {
        value: optValue,
        textContent: label,
        selected: String(value ?? "") === String(optValue),
        ...(extra || {})
      }));
    });
    if (onChange) sel.onchange = () => {
      onChange(sel.value);
      try { updateAdvancedStatus(); } catch {}
    };
    return sel;
  };
  const buildInput = ({ id, type = "text", value = "", min = null, max = null, placeholder = "", onChange }) => {
    const input = Object.assign(el("input"), { id, name: id, type, value, placeholder });
    if (min != null) input.min = String(min);
    if (max != null) input.max = String(max);
    if (onChange) input.oninput = () => {
      onChange(input.value, input);
      try { updateAdvancedStatus(); } catch {}
    };
    if (onChange) input.onchange = () => {
      onChange(input.value, input);
      try { updateAdvancedStatus(); } catch {}
    };
    return input;
  };
  const buildCheck = ({ id, checked, label, onChange }) => {
    const line = el("label", "checkline");
    const chk = Object.assign(el("input"), { id, name: id, type: "checkbox", checked: !!checked });
    if (onChange) chk.onchange = () => {
      onChange(chk.checked);
      try { updateAdvancedStatus(); } catch {}
    };
    line.append(chk, Object.assign(el("span"), { textContent: label }));
    return line;
  };
  const fieldMini = (label, control) => stackWrap("field-mini", Object.assign(el("div", "subnote"), { textContent: label }), control);
  const fieldMiniHelp = (label, control, helpKey) => {
    const head = el("div", "subnote");
    const wrap = el("span", "th-help");
    wrap.appendChild(Object.assign(el("span"), { textContent: label }));
    const btn = Object.assign(el("button"), {
      className: "sch-help",
      type: "button",
      ariaLabel: `${label} help`,
      title: HELP_TIPS[helpKey] || ""
    });
    btn.dataset.helpKey = helpKey;
    btn.onclick = (e) => {
      e.preventDefault();
      e.stopPropagation();
    };
    wrap.appendChild(btn);
    head.appendChild(wrap);
    return stackWrap("field-mini", head, control);
  };
  const alignedField = (control) => stackWrap("field-mini control-align", Object.assign(el("div", "subnote"), { textContent: "Label" }), control);
  const guardInput = ({ id, value, placeholder, title, onChange }) => {
    const input = buildInput({ id, type: "number", min: 0, value, placeholder, onChange });
    if (title) input.title = title;
    return input;
  };
  const providerMeta = () => window.CW?.ProviderMeta || {};
  const providerIcon = (value) => {
    const meta = providerMeta();
    return meta.logLogoPath?.(value) || meta.logoPath?.(value) || "";
  };
  const providerLabel = (value) => providerMeta().label?.(value) || String(value || "");
  const instanceLabel = (value) => {
    const raw = String(value || "").trim();
    return !raw || raw.toLowerCase() === "default" ? "Default" : raw;
  };
  const instanceBadge = (value) => {
    const raw = String(value || "").trim();
    if (!raw || raw.toLowerCase() === "default") return "D";
    const upper = raw.toUpperCase();
    const prof = upper.match(/(^|[^A-Z0-9])(P\d{1,3})(?=[^A-Z0-9]|$)/);
    if (prof?.[2]) return prof[2];
    const parts = upper.split(/[^A-Z0-9]+/).filter(Boolean);
    if (parts.length) {
      const last = parts[parts.length - 1];
      if (last.length <= 4) return last;
    }
    return upper.slice(0, 4);
  };
  const iconMeta = (value) => {
    const key = String(value || "").trim();
    const src = providerIcon(key);
    return src ? { src, alt: providerLabel(key) } : null;
  };
  const routeOptionData = (value, fallbackLabel = "") => {
    const route = [...eventRoutesFor("watcher"), ...eventRoutesFor("webhook")]
      .find((item) => String(item?.id || "") === String(value || ""));
    if (!route) return null;
    const icons = [iconMeta(route.provider), iconMeta(route.sink)].filter(Boolean);
    return {
      label: "",
      badges: route.sink
        ? [instanceBadge(route.provider_instance), instanceBadge(route.sink_instance)]
        : [instanceBadge(route.provider_instance)],
      note: "",
      showNote: false,
      icons,
      separator: icons.length > 1 ? "arrow" : "",
    };
  };
  const pairOptionData = (value, fallbackLabel = "") => {
    const pair = _pairs.find((item) => String(item?.id || "") === String(value || ""));
    if (!pair) return null;
    const icons = [iconMeta(pair.source), iconMeta(pair.target)].filter(Boolean);
    return {
      label: "",
      badges: [instanceBadge(pair.source_instance), instanceBadge(pair.target_instance)],
      note: "",
      showNote: false,
      icons,
      separator: icons.length > 1 ? "arrow" : "",
    };
  };
  const enhanceIconSelect = (select, getOptionData) => {
    const helper = window.CW?.IconSelect?.enhance;
    if (typeof helper !== "function") return select;
    return helper(select, {
      getOptionData: (value, option, nativeSelect) =>
        getOptionData?.(value, option, nativeSelect) || {
          label: String(option?.textContent || "").trim() || "-",
          disabled: !!option?.disabled,
        },
    });
  };
  const pairOptions = (selected, includeNoneText = "Select pair") => [
    ["", includeNoneText],
    ..._pairs.map(p => [p.id, p.label + (p.enabled ? "" : " (disabled)"), { disabled: !p.enabled }])
  ];
  const softenStatus = (value = "") => {
    const text = String(value || "").trim();
    if (!text) return "";
    return text
      .replace(/^Step (\d+): select a pair\.$/i, "Next: choose a pair for Step $1")
      .replace(/^Step (\d+): select a time\.$/i, "Next: set a time for Step $1")
      .replace(/^Capture schedule (\d+): select a provider\.$/i, "Next: choose a provider for Capture schedule $1")
      .replace(/^Capture schedule (\d+): choose a configured provider\.$/i, "Pick a configured provider for Capture schedule $1")
      .replace(/^Capture schedule (\d+): choose a configured provider profile\.$/i, "Pick a configured profile for Capture schedule $1")
      .replace(/^Capture schedule (\d+): select a feature\.$/i, "Next: choose a feature for Capture schedule $1")
      .replace(/^Capture schedule (\d+): feature (.+) is not supported for (.+)\.$/i, "Capture schedule $1: $2 is not available for $3")
      .replace(/^Capture schedule (\d+): select a time\.$/i, "Next: set a time for Capture schedule $1")
      .replace(/^Select a sync pair for each event trigger\.$/i, "Next: choose a sync pair for each event trigger")
      .replace(/^Select a (.+) route for each event trigger\.$/i, "Next: choose a $1 route for each event trigger")
      .replace(/^Select an enabled source for event trigger (.+)\.$/i, "Pick an enabled source for event trigger $1")
      .replace(/^Some timed steps reference disabled pairs\.$/i, "Update the disabled pair used in one of the timed steps")
      .replace(/^Some event triggers reference disabled pairs\.$/i, "Update the disabled pair used in one of the event triggers")
      .replace(/^Some event triggers need a valid configured watcher or webhook route\.$/i, "Choose a valid watcher or webhook route for the event trigger")
      .replace(/^No enabled watcher or webhook routes configured for event triggers\.$/i, "Add or enable a watcher/webhook route to use event triggers")
      .replace(/^Unable to load watcher or webhook routes for event triggers\.$/i, "Routes could not be loaded for event triggers");
  };
  const statusText = (node, value = "") => {
    if (!node) return;
    const text = softenStatus(value);
    node.textContent = text;
    node.classList.toggle("is-empty", !text);
  };
  const eventRuleHasContent = (rule) => {
    const pairId = String(rule?.action?.pair_id || "").trim();
    const routeId = String(rule?.filters?.route_id || "").trim();
    return !!pairId || !!routeId;
  };
  const issueLabel = (section, text) => {
    const body = softenStatus(text);
    return body ? `${section}: ${body}` : "";
  };
  const jobLabel = (job) => {
    const index = Math.max(0, _jobs.indexOf(job)) + 1;
    return `Step ${index}`;
  };
  const isBlankJob = (job) => {
    const pairId = String(job?.pair_id || "").trim();
    const time = String(job?.at || "").trim();
    const after = String(job?.after || "").trim();
    const days = Array.isArray(job?.days) ? job.days : [];
    return !pairId && !time && !after && days.length === 0;
  };
  const jobIssue = (job) => {
    if (isBlankJob(job)) return "";
    const pairId = String(job?.pair_id || "").trim();
    const time = String(job?.at || "").trim();
    const rowName = jobLabel(job);
    if (!pairId) return `${rowName}: select a pair.`;
    if (!time) return `${rowName}: select a time.`;
    return "";
  };
  const serializableJobs = () => {
    const jobs = [];
    const issues = [];
    _jobs.forEach((job) => {
      if (isBlankJob(job)) return;
      const issue = jobIssue(job);
      if (issue) {
        issues.push(issue);
        return;
      }
      jobs.push(job);
    });
    return { jobs, issues };
  };
  const captureProviderById = (provider) => _captureProviders.find((row) => String(row?.id || "") === String(provider || "").trim().toUpperCase()) || null;
  const captureInstanceOptions = (provider, current = "") => {
    const meta = captureProviderById(provider);
    const rows = Array.isArray(meta?.instances) ? meta.instances : [];
    const seen = new Set();
    const out = [];
    rows.forEach((row) => {
      const id = String(row?.id || "default").trim() || "default";
      if (seen.has(id)) return;
      seen.add(id);
      out.push([id, row?.label || instanceLabel(id), row?.configured === false ? { disabled: true } : null]);
    });
    const want = String(current || "").trim() || "default";
    if (want && !seen.has(want)) out.unshift([want, instanceLabel(want), { selected: true }]);
    if (!out.length) out.push(["default", "Default"]);
    return out;
  };
  const captureFeatureOptions = (provider, current = "") => {
    const meta = captureProviderById(provider);
    const feats = Array.isArray(meta?.features) ? meta.features : [];
    const out = [["", "Feature"]];
    if (feats.length) out.push(["all", "All features"]);
    feats.forEach((feature) => out.push([feature, feature === "all" ? "All features" : feature.charAt(0).toUpperCase() + feature.slice(1)]));
    const want = String(current || "").trim().toLowerCase();
    if (want && !out.some(([value]) => String(value) === want)) out.push([want, want === "all" ? "All features" : want]);
    return out;
  };
  const captureProviderOptions = (current = "") => {
    const rows = [["", "Select provider"]];
    _captureProviders.forEach((provider) => {
      rows.push([
        provider.id,
        provider.label + (provider.configured ? "" : " (not configured)"),
        provider.configured ? null : { disabled: true, selected: String(current || "").trim().toUpperCase() === provider.id }
      ]);
    });
    return rows;
  };
  const normalizeCaptureJob = (job = {}) => {
    const days = Array.isArray(job.days) ? [...new Set(job.days.map((n) => parseInt(n, 10)).filter((n) => n >= 1 && n <= 7))].sort((a, b) => a - b) : [];
    return {
      id: job.id || genId(),
      provider: String(job.provider || "").trim().toUpperCase(),
      instance: String(job.instance || job.instance_id || job.profile || "default").trim() || "default",
      feature: String(job.feature || "").trim().toLowerCase(),
      at: String(job.at || "").trim() || null,
      days,
      label_template: String(job.label_template || job.labelTemplate || "").trim() || "auto-{provider}-{feature}-{date}",
      retention_days: Math.max(0, parseInt(job.retention_days ?? job.retentionDays ?? job.keep_days ?? 0, 10) || 0),
      max_captures: Math.max(0, parseInt(job.max_captures ?? job.maxCaptures ?? job.keep_count ?? 0, 10) || 0),
      auto_delete_old: job.auto_delete_old === true || job.autoDeleteOld === true,
      active: job.active !== false
    };
  };
  const isBlankCaptureJob = (job) => {
    const provider = String(job?.provider || "").trim().toUpperCase();
    const feature = String(job?.feature || "").trim().toLowerCase();
    const time = String(job?.at || "").trim();
    const instance = String(job?.instance || "default").trim() || "default";
    const label = String(job?.label_template || "").trim();
    const defaultLabel = "auto-{provider}-{feature}-{date}";
    const retentionDays = Math.max(0, parseInt(job?.retention_days ?? 0, 10) || 0);
    const maxCaptures = Math.max(0, parseInt(job?.max_captures ?? 0, 10) || 0);
    const autoDeleteOld = job?.auto_delete_old === true;
    return !provider
      && !feature
      && !time
      && instance === "default"
      && retentionDays === 0
      && maxCaptures === 0
      && !autoDeleteOld
      && (!label || label === defaultLabel);
  };
  const captureJobLabel = (job) => {
    const index = Math.max(0, _captureJobs.indexOf(job)) + 1;
    return `Capture schedule ${index}`;
  };
  const captureLabelValue = (job) => {
    const label = String(job?.label_template || "").trim();
    return label === "auto-{provider}-{feature}-{date}" ? "" : label;
  };
  const captureJobIssue = (job) => {
    if (isBlankCaptureJob(job)) return "";
    const provider = String(job?.provider || "").trim().toUpperCase();
    const feature = String(job?.feature || "").trim().toLowerCase();
    const time = String(job?.at || "").trim();
    const instance = String(job?.instance || "default").trim() || "default";
    const label = String(job?.label_template || "").trim();
    const retentionDays = Math.max(0, parseInt(job?.retention_days ?? 0, 10) || 0);
    const maxCaptures = Math.max(0, parseInt(job?.max_captures ?? 0, 10) || 0);
    const autoDeleteOld = job?.auto_delete_old === true;
    const hasContent = !!provider || !!feature || !!time || !!label || retentionDays > 0 || maxCaptures > 0 || autoDeleteOld;
    const rowName = captureJobLabel(job);
    if (!hasContent) return "";
    const meta = captureProviderById(provider);
    if (!provider) return `${rowName}: select a provider.`;
    if (!meta || !meta.configured) return `${rowName}: choose a configured provider.`;
    const instMeta = Array.isArray(meta.instances) ? meta.instances.find((row) => String(row?.id || "") === instance) : null;
    if (instMeta && instMeta.configured === false) return `${rowName}: choose a configured provider profile.`;
    if (!feature) return `${rowName}: select a feature.`;
    if (feature !== "all" && !(meta.features || []).includes(feature)) return `${rowName}: feature ${feature} is not supported for ${provider}.`;
    if (!time) return `${rowName}: select a time.`;
    return "";
  };
  const serializableCaptureJobs = () => {
    const jobs = [];
    const issues = [];
    _captureJobs.forEach((job) => {
      if (isBlankCaptureJob(job)) return;
      const issue = captureJobIssue(job);
      if (issue) {
        issues.push(issue);
        return;
      }
      const provider = String(job?.provider || "").trim().toUpperCase();
      const feature = String(job?.feature || "").trim().toLowerCase();
      const at = String(job?.at || "").trim();
      const label = String(job?.label_template || "").trim();
      if (!provider && !feature && !at && !label) return;
      jobs.push(normalizeCaptureJob(job));
    });
    return { jobs, issues };
  };
  const sameCaptureJob = (a, b) => {
    const daysA = Array.isArray(a?.days) ? a.days.join(",") : "";
    const daysB = Array.isArray(b?.days) ? b.days.join(",") : "";
    return String(a?.provider || "") === String(b?.provider || "")
      && String(a?.instance || "default") === String(b?.instance || "default")
      && String(a?.feature || "") === String(b?.feature || "")
      && String(a?.label_template || "") === String(b?.label_template || "")
      && Math.max(0, parseInt(a?.retention_days ?? 0, 10) || 0) === Math.max(0, parseInt(b?.retention_days ?? 0, 10) || 0)
      && Math.max(0, parseInt(a?.max_captures ?? 0, 10) || 0) === Math.max(0, parseInt(b?.max_captures ?? 0, 10) || 0)
      && (a?.auto_delete_old === true) === (b?.auto_delete_old === true)
      && String(a?.at || "") === String(b?.at || "")
      && daysA === daysB;
  };

  // row builder
  const jobRow = j => {
    const tr = el("tr"); if (j.active !== false && j.pair_id && !isEnabled(j.pair_id)) tr.classList.add("row-disabled");
    const rowKey = fieldKey(j?.id, `job_${_jobs.indexOf(j) + 1}`);

    const sel = buildSelect({
      id: `sched_pair_${rowKey}`,
      value: j.pair_id || "",
      options: pairOptions(j.pair_id || null),
      onChange: value => { j.pair_id = value || null; }
    });

    const t = buildInput({
      id: `sched_time_${rowKey}`,
      type: "time",
      value: j.at || "",
      onChange: value => { j.at = (value || "").trim() || null; }
    });

    const wrap = el("div","chipdays"), cur = new Set(Array.isArray(j.days) ? j.days : []);
    DAY.forEach((d,i) => {
      const lab = el("label"), chk = Object.assign(el("input"), { type: "checkbox", checked: cur.has(i+1) }), txt = el("span");
      chk.id = `sched_days_${rowKey}_${i+1}`;
      chk.name = `sched_days_${rowKey}[]`;
      chk.onchange = () => {
        const S = new Set(Array.isArray(j.days) ? j.days : []);
        chk.checked ? S.add(i+1) : S.delete(i+1);
        j.days = [...S].sort((a,b)=>a-b);
        try { updateAdvancedStatus(); } catch {}
      };
      txt.textContent = d;
      lab.append(chk, txt); wrap.appendChild(lab);
    if(i===2){ wrap.appendChild(el("span","chipspacer")); }
    });

    const sa = buildSelect({
      id: `sched_after_${rowKey}`,
      value: j.after || "",
      options: [["", "None"], ..._jobs.filter(x => x !== j).map((x, i) => [String(x.id), `Step ${i + 1}`])],
      onChange: value => { j.after = value || null; renderJobs(); }
    });

    const c = Object.assign(el("input"), { type: "checkbox", checked: j.active !== false });
    c.id = `sched_active_${rowKey}`;
    c.name = c.id;
    c.onchange = () => { j.active = !!c.checked; renderJobs(); };

    const del = Object.assign(el("button"), { className: "btn ghost sch-remove-btn", type: "button", ariaLabel: "Remove step" });
    del.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">close</span>`;
    del.onclick = () => { _jobs = _jobs.filter(x => x !== j); renderJobs(); };

    const tdDays = tdCell("Days", wrap);
    tr.append(
      tdCell("Pair", enhanceIconSelect(sel, (value, option) => {
        const data = pairOptionData(value, String(option?.textContent || "").trim());
        return data ? { ...data, disabled: !!option?.disabled } : null;
      })),
      tdCell("Time", t),
      tdDays,
      tdCell("After", sa),
      tdCell("Active", c),
      tdCell("Remove", del)
    );
    return tr;
  };

  const captureJobRow = (job) => {
    const frag = document.createDocumentFragment();
    const tr = el("tr");
    const rowKey = fieldKey(job?.id, `capture_${_captureJobs.indexOf(job) + 1}`);
    const providerSel = buildSelect({
      id: `sched_cap_provider_${rowKey}`,
      value: job.provider || "",
      options: captureProviderOptions(job.provider || ""),
      onChange: (value) => {
        job.provider = String(value || "").trim().toUpperCase();
        const providerMeta = captureProviderById(job.provider);
        const instances = Array.isArray(providerMeta?.instances) ? providerMeta.instances : [];
        if (!instances.some((row) => String(row?.id || "") === String(job.instance || "default"))) {
          const next = instances.find((row) => row?.configured !== false)?.id || instances[0]?.id || "default";
          job.instance = String(next || "default");
        }
        const features = Array.isArray(providerMeta?.features) ? providerMeta.features : [];
        if (job.feature && job.feature !== "all" && !features.includes(job.feature)) job.feature = "";
        syncCaptureDraftJobs();
        renderCaptureJobs();
      }
    });
    const instanceSel = buildSelect({
      id: `sched_cap_instance_${rowKey}`,
      value: job.instance || "default",
      options: captureInstanceOptions(job.provider || "", job.instance || "default"),
      onChange: (value) => { job.instance = String(value || "default").trim() || "default"; syncCaptureDraftJobs(); }
    });
    const featureSel = buildSelect({
      id: `sched_cap_feature_${rowKey}`,
      value: job.feature || "",
      options: captureFeatureOptions(job.provider || "", job.feature || ""),
      onChange: (value) => { job.feature = String(value || "").trim().toLowerCase(); syncCaptureDraftJobs(); }
    });
    const timeInput = buildInput({
      id: `sched_cap_time_${rowKey}`,
      type: "time",
      value: job.at || "",
      onChange: (value) => { job.at = String(value || "").trim() || null; syncCaptureDraftJobs(); }
    });
    const daysWrap = el("div", "chipdays");
    const curDays = new Set(Array.isArray(job.days) ? job.days : []);
    DAY.forEach((d, i) => {
      const lab = el("label");
      const chk = Object.assign(el("input"), { type: "checkbox", checked: curDays.has(i + 1) });
      const txt = el("span");
      chk.id = `sched_cap_days_${rowKey}_${i + 1}`;
      chk.name = `sched_cap_days_${rowKey}[]`;
      chk.onchange = () => {
        const next = new Set(Array.isArray(job.days) ? job.days : []);
        chk.checked ? next.add(i + 1) : next.delete(i + 1);
        job.days = [...next].sort((a, b) => a - b);
        syncCaptureDraftJobs();
        try { updateAdvancedStatus(); } catch {}
      };
      txt.textContent = d;
      lab.append(chk, txt);
      daysWrap.appendChild(lab);
      if (i === 2) daysWrap.appendChild(el("span", "chipspacer"));
    });
    const labelInput = buildInput({
      id: `sched_cap_label_${rowKey}`,
      type: "text",
      value: captureLabelValue(job),
      placeholder: "",
      onChange: (value, input) => {
        job.label_template = String(value || "").trim();
        input.value = job.label_template;
        syncCaptureDraftJobs();
      }
    });
    labelInput.title = HELP_TIPS.capture_label;
    const retentionDaysInput = buildInput({
      id: `sched_cap_retention_days_${rowKey}`,
      type: "number",
      min: 0,
      value: job.retention_days || "",
      placeholder: "Forever",
      onChange: (value, input) => {
        job.retention_days = Math.max(0, parseInt(value || "0", 10) || 0);
        input.value = job.retention_days ? String(job.retention_days) : "";
        syncCaptureDraftJobs();
      }
    });
    const maxCapturesInput = buildInput({
      id: `sched_cap_max_captures_${rowKey}`,
      type: "number",
      min: 0,
      value: job.max_captures || "",
      placeholder: "Unlimited",
      onChange: (value, input) => {
        job.max_captures = Math.max(0, parseInt(value || "0", 10) || 0);
        input.value = job.max_captures ? String(job.max_captures) : "";
        syncCaptureDraftJobs();
      }
    });
    const autoDeleteCheck = buildCheck({
      id: `sched_cap_auto_delete_${rowKey}`,
      checked: job.auto_delete_old === true,
      label: "Auto-delete older captures after each scheduled run",
      onChange: (checked) => {
        job.auto_delete_old = !!checked;
        syncCaptureDraftJobs();
      }
    });
    const activeChk = Object.assign(el("input"), { id: `sched_cap_active_${rowKey}`, name: `sched_cap_active_${rowKey}`, type: "checkbox", checked: job.active !== false });
    activeChk.onchange = () => {
      job.active = !!activeChk.checked;
      syncCaptureDraftJobs();
      try { updateAdvancedStatus(); } catch {}
    };
    const toggleAdv = Object.assign(el("button"), { className: "btn ghost capture-adv-toggle", type: "button", ariaLabel: "Toggle advanced capture schedule options", title: "Show advanced options" });
    toggleAdv.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">keyboard_arrow_down</span>`;
    const del = Object.assign(el("button"), { className: "btn ghost sch-remove-btn", type: "button", ariaLabel: "Remove capture schedule" });
    del.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">close</span>`;
    del.onclick = () => { _captureJobs = _captureJobs.filter((row) => row !== job); syncCaptureDraftJobs(); renderCaptureJobs(); };
    const detailRow = el("tr");
    detailRow.className = "capture-detail-row";
    const detailCell = Object.assign(el("td"), { colSpan: 6 });
    const detailCard = el("div", "capture-detail-card");
    const detailGrid = el("div", "capture-detail-grid");
    detailGrid.append(
      fieldMiniHelp("Label template", labelInput, "capture_label"),
      fieldMiniHelp("Keep captures for days", retentionDaysInput, "capture_retention_days"),
      fieldMiniHelp("Max captures to keep", maxCapturesInput, "capture_max_captures"),
      fieldMiniHelp("Cleanup", autoDeleteCheck, "capture_cleanup")
    );
    detailCard.appendChild(detailGrid);
    detailCell.appendChild(detailCard);
    detailRow.appendChild(detailCell);
    const syncAdvancedState = () => {
      const open = job._advanced_open === true;
      detailRow.style.display = open ? "" : "none";
      toggleAdv.title = open ? "Hide advanced options" : "Show advanced options";
      toggleAdv.setAttribute("aria-expanded", open ? "true" : "false");
      const icon = toggleAdv.querySelector(".material-symbols-rounded");
      if (icon) icon.textContent = open ? "keyboard_arrow_up" : "keyboard_arrow_down";
    };
    toggleAdv.onclick = () => {
      job._advanced_open = !(job._advanced_open === true);
      syncAdvancedState();
    };

    tr.append(
      tdCell("Provider", stackWrap("stack two capture-provider-stack",
        fieldMini("Provider", enhanceIconSelect(providerSel, (value) => {
          const meta = captureProviderById(value);
          return meta ? { label: meta.label || meta.id, icons: [iconMeta(String(meta.id || "").toLowerCase())].filter(Boolean) } : null;
        })),
        fieldMini("Profile", instanceSel)
      )),
      tdCell("Feature", alignedField(featureSel)),
      tdCell("Time", alignedField(timeInput)),
      tdCell("Days", daysWrap),
      tdCell("Active", activeChk),
      tdCell("Actions", stackWrap("capture-row-actions", toggleAdv, del))
    );
    syncAdvancedState();
    frag.append(tr, detailRow);
    return frag;
  };

  const eventRuleRow = r => {
    const tr = el("tr");
    const pairId = r?.action?.pair_id || null;
    if (r.active !== false && pairId && !isEnabled(pairId)) tr.classList.add("row-disabled");
    const rowKey = fieldKey(r?.id, `event_${_eventRules.indexOf(r) + 1}`);
    const sourceSel = buildSelect({
      id: `sched_evt_source_${rowKey}`,
      value: r.source || "watcher",
      options: sourceOptions(r.source || "watcher"),
      onChange: value => {
        r.source = value || (_eventRoutes.watcher?.length ? "watcher" : _eventRoutes.webhook?.length ? "webhook" : "watcher");
        r.filters.route_id = "";
        renderEventRules();
      }
    });
    sourceSel.style.width = "100%";
    sourceSel.style.minWidth = "0";
    const eventSel = buildSelect({ id: `sched_evt_name_${rowKey}`, value: r.event || "stop", options: EVENT_NAME_OPTIONS, onChange: value => { r.event = value || "stop"; } });
    eventSel.style.width = "15ch";
    eventSel.style.minWidth = "15ch";
    const routeSel = buildSelect({
      id: `sched_evt_route_${rowKey}`,
      value: r.filters?.route_id || "",
      options: routeOptions(r.source || "watcher", r.filters?.route_id || ""),
      onChange: value => {
        r.filters.route_id = value || "";
        renderEventRules();
      }
    });
    routeSel.disabled = !eventRoutesFor(r.source || "watcher").length;
    routeSel.style.width = "100%";
    routeSel.style.minWidth = "0";
    const mediaSel = buildSelect({ id: `sched_evt_media_${rowKey}`, value: r.filters?.media_type || "", options: EVENT_MEDIA_OPTIONS, onChange: value => { r.filters.media_type = value || ""; } });
    mediaSel.style.width = "15ch";
    mediaSel.style.minWidth = "15ch";
    const minProgressInput = buildInput({
      id: `sched_evt_progress_${rowKey}`,
      type: "number",
      min: 0,
      max: 100,
      value: r.filters?.min_progress ?? "",
      placeholder: "Min %",
      onChange: (value, input) => {
        const next = (value || "").trim();
        r.filters.min_progress = next === "" ? null : Math.max(0, Math.min(100, parseInt(next, 10) || 0));
        input.value = r.filters.min_progress == null ? "" : String(r.filters.min_progress);
      }
    });
    minProgressInput.style.width = "9ch";
    minProgressInput.style.minWidth = "9ch";
    const pairSel = buildSelect({
      id: `sched_evt_pair_${rowKey}`,
      value: r.action?.pair_id || "",
      options: pairOptions(r.action?.pair_id || null),
      onChange: value => {
        r.action.kind = "sync_pair";
        r.action.pair_id = value || null;
        renderEventRules();
      }
    });
    pairSel.style.width = "100%";
    pairSel.style.minWidth = "0";
    const numGuard = (suffix, key, placeholder, title) => guardInput({
      id: `sched_evt_${suffix}_${rowKey}`,
      value: r.guardrails?.[key] ?? 0,
      placeholder,
      title,
      onChange: (value, input) => {
        r.guardrails[key] = Math.max(0, parseInt(value || "0", 10) || 0);
        input.value = String(r.guardrails[key]);
      }
    });
    const cooldownInput = numGuard("cooldown", "cooldown_minutes", "15", "Ignore repeat triggers for this many minutes after a rule runs.");
    cooldownInput.style.width = "8ch";
    cooldownInput.style.minWidth = "8ch";
    const dedupeInput = numGuard("dedupe", "dedupe_window_seconds", "30", "Suppress identical events that arrive again within this many seconds.");
    dedupeInput.style.width = "8ch";
    dedupeInput.style.minWidth = "8ch";
    const rateInput = numGuard("rate", "max_runs_per_hour", "4", "Hard cap for how many times this rule can run in one hour.");
    rateInput.style.width = "8ch";
    rateInput.style.minWidth = "8ch";
    const activeChk = Object.assign(el("input"), { id: `sched_evt_active_${rowKey}`, name: `sched_evt_active_${rowKey}`, type: "checkbox", checked: r.active !== false });
    activeChk.onchange = () => { r.active = !!activeChk.checked; renderEventRules(); };
    const del = Object.assign(el("button"), { className: "btn ghost sch-remove-btn", type: "button", ariaLabel: "Remove event trigger" });
    del.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">close</span>`;
    del.onclick = () => { _eventRules = _eventRules.filter(x => x !== r); renderEventRules(); };

    tr.append(
      tdCell("Source", stackWrap("stack", sourceSel, enhanceIconSelect(routeSel, (value, option) => {
        const data = routeOptionData(value, String(option?.textContent || "").trim());
        return data ? { ...data, disabled: !!option?.disabled } : null;
      }))),
      tdCell("Event", eventSel),
      tdCell("Filters", stackWrap("stack event-filter-stack", mediaSel, minProgressInput)),
      tdCell("Action", stackWrap(
        "stack",
        Object.assign(el("div", "subnote"), { textContent: "Sync pair" }),
        enhanceIconSelect(pairSel, (value, option) => {
          const data = pairOptionData(value, String(option?.textContent || "").trim());
          return data ? { ...data, disabled: !!option?.disabled } : null;
        })
      )),
      tdCell("Guardrails", stackWrap(
        "stack",
        stackWrap(
          "stack three",
          fieldMini("Mute (min)", cooldownInput),
          fieldMini("Dedupe (sec)", dedupeInput),
          fieldMini("Max / hour", rateInput)
        )
      )),
      tdCell("Active", activeChk),
      tdCell("Remove", del)
    );
    return tr;
  };

  // mount UI
  const ensureUI = () => {
    const host = $("#sched_advanced_mount") || $("#sec-scheduling .body");
    if (!host || $("#schAdv")) return;

    const adv = Object.assign(el("div", "sch-adv"), { id: "schAdv" });
    adv.innerHTML = `
<div class="cw-panel-head">
  <div class="sch-adv-head-copy">
    <div class="sch-adv-head-kicker">Advanced plan</div>
    <div class="sch-adv-head-title">Step and event scheduler</div>
    <p>Set up a Time Plan, Event Triggers, or both in one advanced schedule.</p>
  </div>
  <label class="cx-toggle">
    <input type="checkbox" id="schAdvEnabled">
    <span class="cx-toggle-ui" aria-hidden="true"></span>
    <span class="cx-toggle-text">Use advanced plan</span>
    <span class="cx-toggle-state" aria-hidden="true"></span>
  </label>
</div>

<section class="sch-plan-summary sch-adv-summary" id="schAdvSummary">
  <div class="sch-plan-summary-copy">
    <div class="sch-plan-summary-kicker">Current plan</div>
    <div class="sch-plan-summary-text" id="schAdvSummaryText"></div>
    <div class="sch-plan-summary-note" id="schAdvSummaryNote"></div>
  </div>
  <div class="sch-plan-summary-state" id="schAdvSummaryState">Advanced plan ready</div>
</section>

<section class="sch-adv-section">
  <div class="sch-adv-section-head">
    <div>
      <div class="sch-adv-section-title">Time Plan</div>
      <div class="sch-adv-section-copy">Use timed steps for the current advanced schedule. Only enabled pairs are selectable.</div>
    </div>
  </div>
  <table>
    <thead><tr>
      <th style="width:32%"><span class="th-help">Pair<button type="button" class="sch-help" aria-label="Pair help" title="Pair help" data-help-key="time_pair"></button></span></th>
      <th style="width:14%"><span class="th-help">Time<button type="button" class="sch-help" aria-label="Time help" title="Time help" data-help-key="time_time"></button></span></th>
      <th style="width:30%"><span class="th-help">Days<button type="button" class="sch-help" aria-label="Days help" title="Days help" data-help-key="time_days"></button></span></th>
      <th style="width:14%"><span class="th-help">After<button type="button" class="sch-help" aria-label="After help" title="After help" data-help-key="time_after"></button></span></th>
      <th style="width:6%">Active</th>
      <th style="width:6%"></th>
    </tr></thead>
    <tbody id="schJobsBody"></tbody>
  </table>
  <div class="sch-adv-actions">
    <button class="btn" id="btnAddStep">Add step</button>
    <button class="btn" id="btnAutoFromPairs">Auto-create from enabled pairs</button>
    <div class="status inline" id="schJobsStatus"></div>
  </div>
</section>

<section class="sch-adv-section">
  <div class="sch-adv-section-head">
    <div>
      <div class="sch-adv-section-title">Capture schedules</div>
      <div class="sch-adv-section-copy">Run automated provider captures on a schedule.</div>
    </div>
  </div>
  <table>
    <thead><tr>
      <th style="width:37%"><span class="th-help">Provider<button type="button" class="sch-help" aria-label="Provider help" title="Provider help" data-help-key="capture_target"></button></span></th>
      <th style="width:14%"><span class="th-help">Feature<button type="button" class="sch-help" aria-label="Feature help" title="Feature help" data-help-key="capture_feature"></button></span></th>
      <th style="width:11%"><span class="th-help">Time<button type="button" class="sch-help" aria-label="Time help" title="Time help" data-help-key="time_time"></button></span></th>
      <th style="width:26%"><span class="th-help">Days<button type="button" class="sch-help" aria-label="Days help" title="Days help" data-help-key="time_days"></button></span></th>
      <th style="width:4%">Active</th>
      <th style="width:8%"></th>
    </tr></thead>
    <tbody id="schCaptureJobsBody"></tbody>
  </table>
  <div class="sch-adv-actions">
    <button class="btn" id="btnAddCaptureJob">Add capture schedule</button>
    <div class="status inline" id="schCaptureStatus"></div>
  </div>
</section>

<section class="sch-adv-section">
  <div class="sch-adv-section-head">
    <div>
      <div class="sch-adv-section-title">Event Triggers</div>
      <div class="sch-adv-section-copy">Trigger a sync pair from watcher or webhook activity with filters and guardrails.</div>
    </div>
  </div>
  <table>
    <thead><tr>
      <th style="width:24%"><span class="th-help">Source<button type="button" class="sch-help" aria-label="Source help" title="Source help" data-help-key="source"></button></span></th>
      <th style="width:12%"><span class="th-help">Event<button type="button" class="sch-help" aria-label="Event help" title="Event help" data-help-key="event"></button></span></th>
      <th style="width:16%"><span class="th-help">Filters<button type="button" class="sch-help" aria-label="Filters help" title="Filters help" data-help-key="filters"></button></span></th>
      <th style="width:22%"><span class="th-help">Action<button type="button" class="sch-help" aria-label="Action help" title="Action help" data-help-key="action"></button></span></th>
      <th style="width:18%"><span class="th-help">Guardrails<button type="button" class="sch-help" aria-label="Guardrails help" title="Guardrails help" data-help-key="guardrails"></button></span></th>
      <th style="width:5%">Active</th>
      <th style="width:6%"></th>
    </tr></thead>
    <tbody id="schEventRulesBody"></tbody>
  </table>
  <div class="sch-adv-actions">
    <button class="btn" id="btnAddEventRule">Add event trigger</button>
    <div class="status inline" id="schEventStatus"></div>
  </div>
</section>

`;
    host.appendChild(adv);
    adv.querySelectorAll(".sch-help").forEach((btn) => {
      const tip = HELP_TIPS[btn.dataset.helpKey] || "";
      if (tip) {
        btn.title = tip;
      } else {
        btn.removeAttribute("title");
      }
      btn.onclick = (e) => {
        e.preventDefault();
        e.stopPropagation();
      };
    });

    $("#btnAddStep").onclick = () => { _jobs.push(defaultJob()); renderJobs(); };
    $("#btnAutoFromPairs").onclick = () => {
      const eps = _pairs.filter(p => p.enabled);
      _jobs = eps.map(p => ({ id: genId(), pair_id: p.id, at: null, days: [], after: null, active: true }));
      if (!_jobs.length) _jobs.push(defaultJob());
      renderJobs();
    };
    $("#btnAddCaptureJob").onclick = () => { _captureJobs.push(defaultCaptureJob()); renderCaptureJobs(); };
    $("#btnAddEventRule").onclick = () => { _eventRules.push(defaultEventRule()); renderEventRules(); };
    $("#schAdvEnabled").onchange = () => {
      _advEnabled = !!$("#schAdvEnabled").checked;

      // Advanced on -> force standard off
      if (_advEnabled) {
        const sel = $("#schEnabled");
        setBooleanSelect(sel, false);
        try { sel.dispatchEvent(new Event("change", { bubbles: true })); } catch {}
        try { sel.dispatchEvent(new Event("input", { bubbles: true })); } catch {}
        try { sel.__toggleSync?.(); } catch {}
      }

      applyModeLocks();
    };
  };

  // render
  const updateAdvancedStatus = () => {
    const jobsSt = $("#schJobsStatus");
    const captureSt = $("#schCaptureStatus");
    const eventSt = $("#schEventStatus");
    const summaryText = $("#schAdvSummaryText");
    const summaryNote = $("#schAdvSummaryNote");
    const summaryState = $("#schAdvSummaryState");
    statusText(jobsSt);
    statusText(captureSt);
    statusText(eventSt);

    const invalidJobs = serializableJobs().issues;
    const blockedJobs = _jobs.some(j => j._blocked);
    const invalidCaptureJobs = serializableCaptureJobs().issues;
    const blockedRules = _eventRules.some(r => r._blocked);
    const blockedRuleRoutes = _eventRules.some(r => r._route_blocked);
    const invalidRules = _eventRules.map(eventRuleIssue).filter(Boolean);
    const timedJobs = _jobs.filter(j => !isBlankJob(j));
    const activeTimedJobs = timedJobs.filter(j => j.active !== false);
    const captureJobs = _captureJobs.filter(j => !isBlankCaptureJob(j));
    const activeCaptureJobs = captureJobs.filter(j => j.active !== false);
    const eventRules = _eventRules.filter(eventRuleHasContent);
    const activeEventRules = eventRules.filter(r => r.active !== false);
    const totalConfigured = timedJobs.length + captureJobs.length + eventRules.length;
    const timedIssue = invalidJobs[0] || (blockedJobs ? "Some timed steps reference disabled pairs." : "");
    const captureIssue = invalidCaptureJobs[0] || "";
    const eventIssue = (
      (eventRules.length ? _eventRouteError : "")
      || invalidRules[0]
      || (blockedRuleRoutes ? "Some event triggers need a valid configured watcher or webhook route." : "")
      || (blockedRules ? "Some event triggers reference disabled pairs." : "")
      || ""
    );
    const summaryIssue = issueLabel("Timed steps", timedIssue)
      || issueLabel("Capture schedules", captureIssue)
      || issueLabel("Event triggers", eventIssue);

    statusText(
      jobsSt,
      timedIssue
    );
    statusText(captureSt, captureIssue);
    statusText(
      eventSt,
      eventIssue
    );

    if (summaryText) {
      if (!totalConfigured) summaryText.textContent = "No advanced rules configured yet";
      else {
        const parts = [];
        if (timedJobs.length) parts.push(`${activeTimedJobs.length}/${timedJobs.length} timed step${timedJobs.length === 1 ? "" : "s"}`);
        if (captureJobs.length) parts.push(`${activeCaptureJobs.length}/${captureJobs.length} capture schedule${captureJobs.length === 1 ? "" : "s"}`);
        if (eventRules.length) parts.push(`${activeEventRules.length}/${eventRules.length} event trigger${eventRules.length === 1 ? "" : "s"}`);
        summaryText.textContent = parts.join(" • ");
      }
    }
    if (summaryNote) {
      summaryNote.textContent = summaryIssue || (
        totalConfigured
          ? "Combine timed steps, automated captures, and event triggers in one advanced plan."
          : "Add timed steps, capture schedules, or event triggers to build an advanced automation flow."
      );
    }
    if (summaryState) {
      summaryState.textContent = summaryIssue
        ? (
          summaryIssue.startsWith("Timed steps:")
            ? "Timed steps need attention"
            : summaryIssue.startsWith("Capture schedules:")
              ? "Capture schedules need attention"
              : "Event triggers need attention"
        )
        : (_advEnabled ? "Advanced plan active" : "Advanced plan ready");
      summaryState.classList.toggle("attention", !!summaryIssue);
    }
  };

  const renderJobs = () => {
    const tbody = $("#schJobsBody"); if (!tbody) return;
    tbody.innerHTML = "";
    if (!_jobs.length) _jobs.push(defaultJob());
    _jobs.forEach(j => j._blocked = j.active !== false && j.pair_id && !isEnabled(j.pair_id));
    _jobs.forEach(j => tbody.appendChild(jobRow(j)));
    updateAdvancedStatus();
    try { window.cwSchedSettingsHubUpdate?.(); } catch {}
  };

  const renderCaptureJobs = () => {
    const tbody = $("#schCaptureJobsBody"); if (!tbody) return;
    tbody.innerHTML = "";
    if (!_captureJobs.length) _captureJobs.push(defaultCaptureJob());
    syncCaptureDraftJobs();
    _captureJobs.forEach((job) => tbody.appendChild(captureJobRow(job)));
    updateAdvancedStatus();
    try { window.cwSchedSettingsHubUpdate?.(); } catch {}
  };

  const renderEventRules = () => {
    const tbody = $("#schEventRulesBody"); if (!tbody) return;
    tbody.innerHTML = "";
    if (!_eventRules.length) _eventRules.push(defaultEventRule());
    _eventRules.forEach(syncRuleRoute);
    _eventRules.forEach(r => {
      r._blocked = r.active !== false && r.action?.pair_id && !isEnabled(r.action.pair_id);
      r._route_blocked = r.active !== false && !!r.action?.pair_id && !findEventRoute(r.source, r.filters?.route_id || "");
    });
    _eventRules.forEach(r => tbody.appendChild(eventRuleRow(r)));
    const addBtn = $("#btnAddEventRule");
    if (addBtn) {
      addBtn.disabled = !eventSourceReady("watcher") && !eventSourceReady("webhook");
      addBtn.title = addBtn.disabled ? (_eventRouteError || "No enabled watcher or webhook routes configured.") : "";
    }
    updateAdvancedStatus();
    try { window.cwSchedSettingsHubUpdate?.(); } catch {}
  };

  // load
  const loadScheduling = async () => {
    if (authSetupPending()) return;
    if (_loading) return; _loading = true;
    try {
      ensureUI();
      decorateStandardPanel();
      decorateStandardFieldHelp();
      wireStandardWizard();
      await fetchPairs();
      await fetchCaptureProviders();
      await fetchEventRoutes();

      let saved = {};
      try { saved = await fetch(`/api/scheduling?t=${Date.now()}`, { cache: "no-store" }).then(r => r.json()); } catch {}

      setBooleanSelect($("#schEnabled"), !!saved.enabled);
      ensureStdEnabledToggle();
      try { $("#schEnabled")?.__toggleSync?.(); } catch {}
      const mode = normalizeStandardMode(saved.mode || "hourly");
      $("#schMode") && ($("#schMode").value = mode);
      $("#schN")    && ($("#schN").value = String(Math.max(2, parseInt(saved.every_n_hours || 12, 10) || 12)));
      $("#schTime") && ($("#schTime").value = saved.daily_time || "03:30");
      setCustomIntervalFromMinutes(saved.custom_interval_minutes || saved.custom_minutes || 60);

      const adv = saved?.advanced || {};
      _advEnabled = !!adv.enabled;
      $("#schAdvEnabled") && ($("#schAdvEnabled").checked = _advEnabled);
      _jobs = Array.isArray(adv.jobs) ? adv.jobs.map(j => ({
        id: j.id || genId(),
        pair_id: j.pair_id || null,
        at: j.at || null,
        days: Array.isArray(j.days) ? j.days.filter(n => n >= 1 && n <= 7) : [],
        after: j.after || null,
        active: j.active !== false
      })) : [];
      _captureJobs = Array.isArray(adv.capture_jobs || adv.captureJobs) ? (adv.capture_jobs || adv.captureJobs).map(normalizeCaptureJob) : [];
      _captureJobs = mergeCaptureJobs(_captureJobs, getCaptureDraftJobs());
      _eventRules = Array.isArray(adv.event_rules || adv.eventRules) ? (adv.event_rules || adv.eventRules).map(normalizeEventRule) : [];
      _eventRules.forEach(syncRuleRoute);
      renderJobs();
      renderCaptureJobs();
      renderEventRules();
      const pendingCapturePrefills = getPendingCapturePrefills();
      if (pendingCapturePrefills.length) {
        setPendingCapturePrefills([]);
        queueCapturePrefills(pendingCapturePrefills);
      }

      applyModeLocks();
      updateStandardWizard();

      try { window.cwSchedSettingsHubInit?.(); } catch {}
      try { window.cwSchedSettingsHubUpdate?.(); } catch {}

      try { typeof window.refreshSchedulingBanner === "function" && window.refreshSchedulingBanner(); } catch {}
    } finally { _loading = false; }
  };
  window.loadScheduling = loadScheduling;

  // serialize advanced
  const serializeAdvanced = () => ({
    enabled: !!_advEnabled,
    jobs: _jobs.map(j => ({
      id: j.id,
      pair_id: j.pair_id || null,
      at: j.at || null,
      days: Array.isArray(j.days) ? j.days.slice() : [],
      after: j.after || null,
      active: j.active !== false
    })),
    capture_jobs: serializableCaptureJobs().jobs.map((job) => ({
      id: job.id,
      provider: job.provider || "",
      instance: job.instance || "default",
      feature: job.feature || "",
      at: job.at || null,
      days: Array.isArray(job.days) ? job.days.slice() : [],
      label_template: job.label_template || "",
      retention_days: Math.max(0, parseInt(job.retention_days || 0, 10) || 0),
      max_captures: Math.max(0, parseInt(job.max_captures || 0, 10) || 0),
      auto_delete_old: job.auto_delete_old === true,
      active: job.active !== false
    })),
    event_rules: serializableEventRules().rules.map(r => ({
      id: r.id,
      source: r.source || "watcher",
      event: r.event || "stop",
      filters: {
        route_id: r.filters?.route_id || "",
        media_type: r.filters?.media_type || "",
        min_progress: r.filters?.min_progress == null || r.filters?.min_progress === "" ? null : Math.max(0, Math.min(100, parseInt(r.filters.min_progress, 10) || 0))
      },
      action: {
        kind: "sync_pair",
        pair_id: r.action?.pair_id || null
      },
      guardrails: {
        cooldown_minutes: Math.max(0, parseInt(r.guardrails?.cooldown_minutes || 0, 10) || 0),
        dedupe_window_seconds: Math.max(0, parseInt(r.guardrails?.dedupe_window_seconds || 0, 10) || 0),
        max_runs_per_hour: Math.max(0, parseInt(r.guardrails?.max_runs_per_hour || 0, 10) || 0)
      },
      active: r.active !== false
    }))
  });
  const schedulingValidation = () => {
    const captureState = serializableCaptureJobs();
    const ruleState = serializableEventRules();
    return {
      captureIssues: captureState.issues.slice(),
      eventIssues: ruleState.issues.slice(),
      issues: [...captureState.issues, ...ruleState.issues]
    };
  };
  window.getSchedulingValidation = schedulingValidation;

  // public getter for current scheduling patch
  window.getSchedulingPatch = (opts = {}) => {
    const strict = opts?.strict !== false;
    const mode = normalizeStandardMode($("#schMode")?.value || "hourly");
    const every_n_hours = mode === "every_n_hours" ? Math.max(2, parseInt($("#schN")?.value || "12", 10) || 12) : 1;
    const daily_time = $("#schTime")?.value || "03:30";
    const custom_interval_minutes = getCustomIntervalMinutes();
    const validation = schedulingValidation();
    if (validation.captureIssues.length) {
      if (strict) throw new Error(validation.captureIssues[0]);
      return null;
    }
    if (validation.eventIssues.length) {
      if (strict) throw new Error(validation.eventIssues[0]);
      return null;
    }
    const advanced = serializeAdvanced();

    // Advanced plan disables standard scheduling
    const stdEnabled = ($("#schEnabled")?.value || "").trim() === "true";
    const enabled = advanced.enabled ? false : stdEnabled;

    return { enabled, mode, every_n_hours, daily_time, custom_interval_minutes, advanced };
  };

  // boot
  document.addEventListener("DOMContentLoaded", () => {
    if (authSetupPending()) return;
    loadScheduling().catch(e => console.warn("scheduler load failed", e));
    try { window.dispatchEvent(new Event("sched-banner-ready")); } catch {}
  });

  document.addEventListener("config-saved", (e) => {
    if (authSetupPending()) return;
    const section = e?.detail?.section;
    if (section && section !== "scheduling") return;
    setCaptureDraftJobs([]);
    loadScheduling().catch(err => console.warn("scheduler reload failed", err));
  });

  const getPendingCapturePrefills = () => {
    const raw = window.__cwCaptureSchedulerPrefillQueue;
    return Array.isArray(raw) ? raw.filter((row) => row && typeof row === "object") : [];
  };
  const setPendingCapturePrefills = (items) => {
    const queue = Array.isArray(items) ? items.filter((row) => row && typeof row === "object") : [];
    window.__cwCaptureSchedulerPrefillQueue = queue;
  };
  const appendPendingCapturePrefills = (items) => {
    const queue = getPendingCapturePrefills();
    const next = Array.isArray(items) ? items : [items];
    setPendingCapturePrefills(queue.concat(next.filter((row) => row && typeof row === "object")));
  };
  const canApplyCapturePrefills = () => !!$("#schAdv") && _captureProviders.length > 0;
  const getCaptureDraftJobs = () => {
    const raw = window.__cwCaptureSchedulerDraftJobs;
    return Array.isArray(raw) ? raw.filter((row) => row && typeof row === "object").map(normalizeCaptureJob) : [];
  };
  const setCaptureDraftJobs = (items) => {
    window.__cwCaptureSchedulerDraftJobs = (Array.isArray(items) ? items : [])
      .filter((row) => row && typeof row === "object")
      .map(normalizeCaptureJob)
      .filter((row) => !isBlankCaptureJob(row));
  };
  const syncCaptureDraftJobs = () => {
    setCaptureDraftJobs(_captureJobs);
  };
  const mergeCaptureJobs = (baseJobs = [], draftJobs = []) => {
    const merged = [];
    [...(Array.isArray(baseJobs) ? baseJobs : []), ...(Array.isArray(draftJobs) ? draftJobs : [])].forEach((job) => {
      const next = normalizeCaptureJob(job);
      if (isBlankCaptureJob(next)) return;
      const existingIx = merged.findIndex((row) => sameCaptureJob(row, next) || row.id === next.id);
      if (existingIx >= 0) merged[existingIx] = { ...merged[existingIx], ...next };
      else merged.push(next);
    });
    return merged;
  };

  const queueCapturePrefill = (payload = {}) => {
    const job = normalizeCaptureJob({
      provider: payload.provider,
      instance: payload.instance || payload.instance_id || payload.profile || "default",
      feature: payload.feature,
      label_template: payload.label_template || payload.labelTemplate || payload.label || "auto-{provider}-{feature}-{date}",
      retention_days: payload.retention_days ?? payload.retentionDays ?? payload.keep_days ?? 0,
      max_captures: payload.max_captures ?? payload.maxCaptures ?? payload.keep_count ?? 0,
      auto_delete_old: payload.auto_delete_old === true || payload.autoDeleteOld === true,
      at: payload.at || null,
      days: Array.isArray(payload.days) ? payload.days : [],
      active: payload.active !== false,
    });
    if (!job.provider || !job.feature) return false;
    if (_captureJobs.some((row) => sameCaptureJob(normalizeCaptureJob(row), job))) {
      syncCaptureDraftJobs();
      renderCaptureJobs();
      return true;
    }
    const blankIx = _captureJobs.findIndex((row) => {
      return isBlankCaptureJob(row);
    });
    if (blankIx >= 0) _captureJobs.splice(blankIx, 1);
    _captureJobs.push(job);
    syncCaptureDraftJobs();
    renderCaptureJobs();
    const host = $("#schCaptureJobsBody");
    try { host?.lastElementChild?.scrollIntoView({ behavior: "smooth", block: "nearest" }); } catch {}
    return true;
  };
  const queueCapturePrefills = (payloads = []) => {
    const rows = Array.isArray(payloads) ? payloads : [payloads];
    let added = 0;
    rows.forEach((payload) => {
      if (queueCapturePrefill(payload)) added += 1;
    });
    return added;
  };
  window.prefillCaptureSchedules = (payloads = []) => {
    appendPendingCapturePrefills(payloads);
    if (canApplyCapturePrefills()) {
      const pending = getPendingCapturePrefills();
      setPendingCapturePrefills([]);
      return queueCapturePrefills(pending) > 0;
    }
    return false;
  };
})();
