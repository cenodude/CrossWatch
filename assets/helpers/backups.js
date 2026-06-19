/* assets/helpers/backups.js */
/* CrossWatch Backup & Restore UI */
(function(){
  const DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
  const SPLIT_STORAGE_KEY = "cw.backups.controlsHeight.v4";
  const SPLIT_DEFAULTS = { manual: 215, scheduled: 315 };
  const SPLIT_MINS = { manual: 145, scheduled: 285 };
  const SCOPES = [
    ["config_only", "Config"],
    ["app_state", "Normal"],
    ["full", "Full"]
  ];
  const state = { backups: [], schedule: {}, selected: "", controlsHeight: {}, rowStatus: {}, message: "", mode: "manual", refreshing: false };

  function $(id){ return document.getElementById(id); }

  function api(url, init){
    return fetch(url, Object.assign({ cache: "no-store" }, init || {})).then(async (r) => {
      const j = await r.json().catch(() => ({}));
      if (!r.ok || j?.ok === false) throw new Error(j?.error || "Request failed");
      return j;
    });
  }

  function postJSON(url, body){
    return api(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {})
    });
  }

  function el(tag, attrs, children){
    const node = document.createElement(tag);
    const a = attrs || {};
    for (const [k, v] of Object.entries(a)) {
      if (v === false || v === null || v === undefined) continue;
      if (k === "class") node.className = String(v);
      else if (k === "text") node.textContent = String(v);
      else if (k === "title" || k === "aria-label" || k === "type" || k === "value" || k === "name" || k === "id" || k === "for" || k === "accept") node.setAttribute(k, String(v));
      else if (k === "checked") node.checked = !!v;
      else if (k === "disabled") node.disabled = !!v;
      else if (k === "on") {
        for (const [ev, fn] of Object.entries(v || {})) node.addEventListener(ev, fn);
      }
      else node.setAttribute(k, String(v));
    }
    for (const child of [].concat(children || [])) {
      if (child === null || child === undefined) continue;
      node.appendChild(typeof child === "string" ? document.createTextNode(child) : child);
    }
    return node;
  }

  function icon(name){ return el("span", { class: "material-symbols-rounded br-icon", text: name }); }

  function fmtBytes(n){
    const v = Number(n || 0);
    if (v < 1024) return `${v} B`;
    const units = ["KB", "MB", "GB", "TB"];
    let x = v / 1024;
    let i = 0;
    while (x >= 1024 && i < units.length - 1) { x /= 1024; i++; }
    return `${x.toFixed(x >= 10 ? 1 : 2)} ${units[i]}`;
  }

  function fmtDate(value, fallback){
    const raw = value || (fallback ? Number(fallback) * 1000 : 0);
    if (!raw) return "Never";
    const d = new Date(raw);
    if (Number.isNaN(d.getTime())) return "Unknown";
    return d.toLocaleString();
  }

  function scopeLabel(scope){
    const found = SCOPES.find((x) => x[0] === scope);
    return found ? found[1] : (scope || "Unknown");
  }

  function toast(text, delay){
    state.message = String(text || "");
    clearTimeout(toast._lt);
    const local = $("br-msg");
    if (local) {
      local.textContent = state.message;
      local.classList.remove("hidden");
    }
    toast._lt = setTimeout(() => {
      state.message = "";
      const cur = $("br-msg");
      if (cur) cur.classList.add("hidden");
    }, delay || 4200);
  }

  function ensureStyles(){
    if ($("cw-backups-style")) return;
    const s = document.createElement("style");
    s.id = "cw-backups-style";
    s.textContent = `
#cw-backups-modal.hidden{display:none!important}
#cw-backups-modal{--br-overlay:rgba(0,0,0,.68);--br-shell:linear-gradient(180deg,rgba(11,13,20,.99),rgba(4,5,10,.995));--br-head:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.01));--br-panel:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.018));--br-panel-hover:rgba(255,255,255,.095);--br-row:rgba(255,255,255,.025);--br-row-active:rgba(96,108,255,.08);--br-field:rgba(2,4,9,.78);--br-fg:#f5f7ff;--br-title:#f8fbff;--br-muted:rgba(210,218,235,.68);--br-soft:rgba(210,218,235,.58);--br-border:rgba(255,255,255,.085);--br-border-strong:rgba(255,255,255,.16);--br-focus:rgba(126,137,255,.35);--br-focus-ring:rgba(126,137,255,.10);--br-accent:#7d86ff;--br-primary:linear-gradient(180deg,rgba(89,97,216,.78),rgba(61,70,166,.72));--br-primary-border:rgba(149,158,255,.30);--br-primary-shadow:0 12px 24px rgba(42,48,132,.22);--br-danger:#ffdede;--br-danger-border:rgba(255,100,100,.32);--br-ok-bg:rgba(73,226,163,.09);--br-ok-border:rgba(73,226,163,.18);--br-ok-fg:#dfffee;--br-split:rgba(255,255,255,.16);--br-split-handle:rgba(255,255,255,.09);position:fixed;inset:0;z-index:1300;display:flex;align-items:center;justify-content:center;background:var(--br-overlay);padding:16px;color:var(--br-fg)}
html[data-cw-theme=flat-dark] #cw-backups-modal{--br-overlay:rgba(0,0,0,.58);--br-shell:#171a22;--br-head:#20242d;--br-panel:#20242d;--br-panel-hover:#2b3140;--br-row:#242936;--br-row-active:#2b3144;--br-field:#161a22;--br-fg:#eef1f6;--br-title:#f3f6ff;--br-muted:#a9b0bd;--br-soft:#8f98a8;--br-border:rgba(255,255,255,.13);--br-border-strong:rgba(255,255,255,.22);--br-focus:rgba(125,134,201,.58);--br-focus-ring:rgba(125,134,201,.18);--br-accent:#7d86c9;--br-primary:#253044;--br-primary-border:#253044;--br-primary-shadow:none;--br-danger:#ffe7eb;--br-danger-border:rgba(216,102,114,.42);--br-ok-bg:rgba(31,79,58,.72);--br-ok-border:rgba(87,181,138,.32);--br-ok-fg:#c9f7dd;--br-split:rgba(255,255,255,.18);--br-split-handle:#303642}
html[data-cw-theme=flat-light] #cw-backups-modal{--br-overlay:rgba(15,23,42,.42);--br-shell:#ffffff;--br-head:#ffffff;--br-panel:#ffffff;--br-panel-hover:#eef2f7;--br-row:#f5f7fb;--br-row-active:#e9ecf7;--br-field:#ffffff;--br-fg:#111827;--br-title:#111827;--br-muted:#475467;--br-soft:#667085;--br-border:rgba(16,24,40,.16);--br-border-strong:rgba(16,24,40,.26);--br-focus:rgba(70,86,166,.38);--br-focus-ring:rgba(70,86,166,.14);--br-accent:#4656a6;--br-primary:#253044;--br-primary-border:#253044;--br-primary-shadow:none;--br-danger:#7f1d2d;--br-danger-border:rgba(169,63,77,.35);--br-ok-bg:#dff8eb;--br-ok-border:rgba(18,185,129,.32);--br-ok-fg:#12623f;--br-split:rgba(16,24,40,.18);--br-split-handle:#d9e0ea}
#cw-backups-modal *{box-sizing:border-box}
#cw-backups-modal .br-dialog{width:min(1140px,calc(100vw - 32px));height:min(780px,calc(100vh - 32px));display:flex;flex-direction:column;border:1px solid var(--br-border);border-radius:16px;background:var(--br-shell);box-shadow:0 26px 72px rgba(0,0,0,.34);overflow:hidden}
#cw-backups-modal .br-head{display:flex;align-items:center;justify-content:space-between;gap:16px;flex:0 0 auto;padding:16px 18px 14px;border-bottom:1px solid var(--br-border);background:var(--br-head)}
#cw-backups-modal .br-title{font-weight:900;font-size:21px;line-height:1.12;letter-spacing:0;color:var(--br-title)}
#cw-backups-modal .br-sub{margin-top:5px;color:var(--br-muted);font-size:12px;line-height:1.45}
#cw-backups-modal .br-close,#cw-backups-modal .br-iconbtn{appearance:none;display:inline-flex;align-items:center;justify-content:center;flex:0 0 auto;width:36px;height:36px;padding:0;border:1px solid var(--br-border);border-radius:10px;background:var(--br-row);color:var(--br-fg);cursor:pointer}
#cw-backups-modal .br-close:hover,#cw-backups-modal .br-iconbtn:hover{background:var(--br-panel-hover);border-color:var(--br-border-strong)}
#cw-backups-modal .br-icon{font-size:20px;line-height:1}
#cw-backups-modal .br-iconbtn.spin .br-icon{animation:brspin .8s linear infinite}
@keyframes brspin{to{transform:rotate(360deg)}}
#cw-backups-modal .br-body{flex:1 1 auto;min-height:0;padding:14px;display:grid;grid-template-rows:minmax(145px,var(--br-controls-height,215px)) 10px minmax(0,1fr);grid-template-columns:minmax(0,1fr);gap:8px;overflow:hidden}
#cw-backups-modal .br-panel{min-width:0;min-height:0;border:1px solid var(--br-border);border-radius:12px;background:var(--br-panel);box-shadow:inset 0 1px 0 rgba(255,255,255,.025)}
#cw-backups-modal .br-controls{padding:12px;display:grid;grid-template-rows:auto minmax(0,1fr);gap:10px;overflow:hidden}
#cw-backups-modal .br-mode-top{display:flex;align-items:center;justify-content:space-between;gap:10px;min-width:0}
#cw-backups-modal .br-mode-top .br-msg{flex:1 1 auto;margin-top:0!important;min-width:180px;max-width:560px}
#cw-backups-modal .br-mode-tabs{display:inline-flex;align-items:center;gap:6px;padding:4px;border:1px solid var(--br-border);border-radius:12px;background:var(--br-row)}
#cw-backups-modal .br-mode-tab{appearance:none;min-height:34px;padding:0 12px;border:0;border-radius:9px;background:transparent;color:var(--br-muted);font:inherit;font-size:12px;font-weight:900;letter-spacing:.08em;text-transform:uppercase;cursor:pointer}
#cw-backups-modal .br-mode-tab.active{background:var(--br-primary);color:#fff;box-shadow:var(--br-primary-shadow)}
#cw-backups-modal .br-mode-window{min-width:0;min-height:0;overflow:hidden}
#cw-backups-modal .br-mode-track{height:100%;display:grid;grid-template-columns:100% 100%;transition:transform .2s ease}
#cw-backups-modal .br-mode-track.scheduled{transform:translateX(-100%)}
#cw-backups-modal .br-mode-pane{min-width:0;min-height:0;display:grid;gap:10px;align-content:start;padding:2px}
#cw-backups-modal .br-splitter{appearance:none;width:100%;height:10px;margin:0;border:0;border-radius:999px;background:transparent;cursor:row-resize;position:relative}
#cw-backups-modal .br-splitter:before{content:"";position:absolute;left:10px;right:10px;top:4px;height:2px;border-radius:999px;background:var(--br-split)}
#cw-backups-modal .br-splitter:after{content:"";position:absolute;left:50%;top:1px;width:56px;height:8px;transform:translateX(-50%);border-radius:999px;background:var(--br-split-handle);border:1px solid var(--br-border)}
#cw-backups-modal .br-splitter:hover:before,#cw-backups-modal .br-splitter:focus-visible:before{background:var(--br-focus)}
#cw-backups-modal .br-splitter:focus-visible{outline:2px solid var(--br-focus);outline-offset:2px}
#cw-backups-modal.br-resizing,#cw-backups-modal.br-resizing *{cursor:row-resize!important;user-select:none!important}
#cw-backups-modal .br-section{min-width:0;display:grid;gap:10px;align-content:start}
#cw-backups-modal .br-section-head{display:grid;grid-template-columns:auto minmax(230px,1fr) auto;align-items:center;gap:10px}
#cw-backups-modal .br-section-head h3{margin:0!important}
#cw-backups-modal .br-section-head .br-check{min-height:34px}
#cw-backups-modal .br-section-head .br-actions{margin:0;justify-content:flex-end}
#cw-backups-modal .br-section-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
#cw-backups-modal .br-schedule-grid{display:grid;grid-template-columns:minmax(150px,1.15fr) minmax(105px,.75fr) repeat(2,minmax(92px,.65fr));gap:10px}
#cw-backups-modal .br-manual-grid{display:grid;grid-template-columns:minmax(160px,.75fr) minmax(180px,1fr) auto;gap:10px;align-items:end}
#cw-backups-modal .br-section-wide{grid-column:1/-1}
#cw-backups-modal .br-list-panel{padding:14px;display:flex;flex-direction:column;overflow:hidden}
#cw-backups-modal .br-panel h3{margin:0 0 10px;font-size:11px;text-transform:uppercase;letter-spacing:.13em;color:var(--br-muted)}
#cw-backups-modal .br-field{display:grid;gap:6px;margin:0 0 10px}
#cw-backups-modal .br-section .br-field{margin:0}
#cw-backups-modal .br-field>label{display:block;margin:0;font-size:11px;color:var(--br-muted);font-weight:850;letter-spacing:.08em;text-transform:uppercase}
#cw-backups-modal .br-field input:not([type=checkbox]),#cw-backups-modal .br-field select{width:100%;height:38px;min-height:38px;border-radius:10px;border:1px solid var(--br-border);background:var(--br-field);color:var(--br-fg);padding:0 11px;font:inherit;font-size:13px;outline:none;color-scheme:dark}
html[data-cw-theme=flat-light] #cw-backups-modal .br-field input:not([type=checkbox]),html[data-cw-theme=flat-light] #cw-backups-modal .br-field select{color-scheme:light}
#cw-backups-modal .br-field input:focus,#cw-backups-modal .br-field select:focus{border-color:var(--br-focus);box-shadow:0 0 0 3px var(--br-focus-ring)}
#cw-backups-modal .br-check{display:grid!important;grid-template-columns:18px minmax(0,1fr);align-items:center;gap:9px;width:100%;min-height:30px;margin:0!important;padding:5px 7px;border-radius:9px;color:var(--br-fg)!important;font-size:12px;font-weight:750;letter-spacing:0;text-transform:none;background:var(--br-row);cursor:pointer}
#cw-backups-modal .br-check:hover{background:var(--br-panel-hover)}
#cw-backups-modal input[type=checkbox]{appearance:auto!important;-webkit-appearance:auto!important;width:15px!important;height:15px!important;min-width:15px!important;min-height:15px!important;max-width:15px!important;max-height:15px!important;margin:0!important;padding:0!important;accent-color:var(--br-accent);justify-self:center}
#cw-backups-modal .br-check span{min-width:0;line-height:1.3;text-align:left}
#cw-backups-modal .br-section-head .br-switch{justify-self:start}
#cw-backups-modal .br-switch{position:relative;display:inline-flex!important;align-items:center;gap:10px;width:auto;min-height:34px;margin:0!important;padding:0;color:var(--br-fg)!important;background:transparent;font-size:13px;font-weight:750;cursor:pointer;user-select:none}
#cw-backups-modal .br-switch input{position:absolute!important;opacity:0!important;pointer-events:none!important;width:1px!important;height:1px!important}
#cw-backups-modal .br-switch-ui{position:relative;display:inline-block;width:48px;height:28px;border-radius:999px;background:linear-gradient(180deg,rgba(255,255,255,.10),rgba(255,255,255,.05));border:1px solid var(--br-border);box-shadow:inset 0 1px 0 rgba(255,255,255,.03),inset 0 0 0 1px rgba(0,0,0,.16);transition:background .18s ease,border-color .18s ease,box-shadow .18s ease}
#cw-backups-modal .br-switch-ui:after{content:"";position:absolute;left:3px;top:3px;width:20px;height:20px;border-radius:50%;background:rgba(255,255,255,.94);box-shadow:0 10px 20px rgba(0,0,0,.34);transition:transform .18s ease,background .18s ease}
#cw-backups-modal .br-switch input:checked+.br-switch-ui{background:linear-gradient(180deg,rgba(42,202,126,.34),rgba(28,136,87,.28));border-color:rgba(34,197,94,.42)}
#cw-backups-modal .br-switch input:checked+.br-switch-ui:after{transform:translateX(20px);background:#fff}
#cw-backups-modal .br-switch input:focus-visible+.br-switch-ui{box-shadow:0 0 0 2px var(--br-focus-ring),0 0 0 6px rgba(100,110,255,.12),inset 0 0 0 1px rgba(0,0,0,.16)}
#cw-backups-modal .br-switch-text{min-width:0;white-space:nowrap;color:var(--br-muted);font-weight:800}
#cw-backups-modal .br-switch-state{display:inline-flex;align-items:center;min-height:24px;padding:0 9px;border-radius:999px;background:var(--br-row);border:1px solid var(--br-border);font-size:11px;font-weight:900;letter-spacing:.08em;text-transform:uppercase;color:var(--br-muted)}
#cw-backups-modal .br-switch-state:before{content:"Off"}
#cw-backups-modal .br-switch input:checked~.br-switch-state{color:var(--br-ok-fg);border-color:var(--br-ok-border);background:var(--br-ok-bg)}
#cw-backups-modal .br-switch input:checked~.br-switch-state:before{content:"On"}
html[data-cw-theme=flat-dark] #cw-backups-modal .br-switch-ui{background:#2b313d;border-color:rgba(255,255,255,.14);box-shadow:none}
html[data-cw-theme=flat-dark] #cw-backups-modal .br-switch-ui:after{background:#dce2ec;box-shadow:none}
html[data-cw-theme=flat-dark] #cw-backups-modal .br-switch input:checked+.br-switch-ui{background:#2d493d;border-color:rgba(87,181,138,.46)}
html[data-cw-theme=flat-light] #cw-backups-modal .br-switch-ui{background:#eef2f7;border-color:rgba(21,31,48,.14);box-shadow:none}
html[data-cw-theme=flat-light] #cw-backups-modal .br-switch-ui:after{background:#fff;box-shadow:none}
html[data-cw-theme=flat-light] #cw-backups-modal .br-switch input:checked+.br-switch-ui{background:#dff2e9;border-color:rgba(47,148,109,.36)}
#cw-backups-modal .br-actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:10px}
#cw-backups-modal .br-btn{appearance:none;display:inline-flex;align-items:center;justify-content:center;gap:7px;min-height:36px;padding:0 12px;border-radius:10px;border:1px solid var(--br-border);background:var(--br-row);color:var(--br-fg);cursor:pointer;font:inherit;font-size:13px;font-weight:850;white-space:nowrap}
#cw-backups-modal .br-btn:hover{background:var(--br-panel-hover);border-color:var(--br-border-strong)}
#cw-backups-modal .br-btn.primary{background:var(--br-primary);border-color:var(--br-primary-border);box-shadow:var(--br-primary-shadow);color:#fff}
#cw-backups-modal .br-btn.danger{border-color:var(--br-danger-border);color:var(--br-danger)}
#cw-backups-modal .br-btn:disabled{opacity:.45;cursor:not-allowed}
#cw-backups-modal .br-msg{margin-top:10px;padding:9px 10px;border-radius:10px;background:var(--br-ok-bg);border:1px solid var(--br-ok-border);font-size:12px;color:var(--br-ok-fg)}
#cw-backups-modal .br-msg.hidden{display:none}
body.br-backups-open #save-fab,body.br-backups-open #save-frost{display:none!important}
#cw-backups-modal .br-status{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-bottom:12px;flex:0 0 auto}
#cw-backups-modal .br-stat{min-width:0;border:1px solid var(--br-border);border-radius:10px;padding:10px 11px;background:var(--br-row)}
#cw-backups-modal .br-stat b{display:block;font-size:14px;line-height:1.2;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:var(--br-title)}
#cw-backups-modal .br-stat span{display:block;margin-top:5px;font-size:10px;text-transform:uppercase;letter-spacing:.08em;color:var(--br-soft)}
#cw-backups-modal .br-list-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin:0 0 10px;flex:0 0 auto}
#cw-backups-modal .br-list-head h3{margin:0}
#cw-backups-modal .br-list{display:flex;flex-direction:column;gap:8px;min-height:0;overflow:auto;padding-right:2px}
#cw-backups-modal .br-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:12px;align-items:center;padding:11px 12px;border:1px solid var(--br-border);border-radius:11px;background:var(--br-row)}
#cw-backups-modal .br-row.active{border-color:var(--br-focus);background:var(--br-row-active)}
#cw-backups-modal .br-main{min-width:0;display:grid;gap:7px}
#cw-backups-modal .br-name{font-weight:850;font-size:14px;line-height:1.2;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:var(--br-title)}
#cw-backups-modal .br-meta{display:flex;gap:6px;flex-wrap:wrap;color:var(--br-muted);font-size:12px}
#cw-backups-modal .br-pill{display:inline-flex;align-items:center;min-height:22px;padding:0 8px;border-radius:999px;border:1px solid var(--br-border);background:var(--br-panel)}
#cw-backups-modal .br-row-note{font-size:12px;line-height:1.35;color:var(--br-muted)}
#cw-backups-modal .br-row-note.ok{color:var(--br-ok-fg)}
#cw-backups-modal .br-row-note.warn{color:var(--br-danger)}
#cw-backups-modal .br-row-actions{display:flex;align-items:center;gap:6px;justify-content:flex-end}
#cw-backups-modal .br-days{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:6px}
#cw-backups-modal .br-days-field{grid-template-columns:auto minmax(0,1fr);align-items:center;gap:10px}
#cw-backups-modal .br-days-field>label{padding-top:0;min-width:42px}
#cw-backups-modal .br-day{display:grid!important;grid-template-columns:16px minmax(0,1fr);align-items:center;gap:7px;min-height:31px;margin:0!important;padding:0 8px;border:1px solid var(--br-border);border-radius:9px;background:var(--br-row);font-size:12px;font-weight:800;color:var(--br-fg)!important}
#cw-backups-modal .br-day span{text-align:left;white-space:nowrap}
#cw-backups-modal .br-upload{display:none}
@media (max-width:1100px){#cw-backups-modal .br-section-head{grid-template-columns:1fr auto}#cw-backups-modal .br-section-head .br-switch{grid-column:1/-1}#cw-backups-modal .br-schedule-grid{grid-template-columns:repeat(2,minmax(0,1fr))}#cw-backups-modal .br-days-field{grid-template-columns:1fr}#cw-backups-modal .br-days{grid-template-columns:repeat(4,minmax(0,1fr))}}
@media (max-width:920px){#cw-backups-modal .br-dialog{height:calc(100vh - 24px);width:calc(100vw - 24px)}#cw-backups-modal .br-body{grid-template-rows:auto minmax(0,1fr);overflow:auto}#cw-backups-modal .br-splitter{display:none}#cw-backups-modal .br-controls{overflow:visible}#cw-backups-modal .br-list-panel{overflow:visible}#cw-backups-modal .br-status{grid-template-columns:1fr 1fr}#cw-backups-modal .br-list{overflow:visible}}
@media (max-width:680px){#cw-backups-modal .br-mode-top{align-items:stretch;flex-direction:column}#cw-backups-modal .br-mode-tabs{width:100%;display:grid;grid-template-columns:1fr 1fr}#cw-backups-modal .br-manual-grid{grid-template-columns:1fr}}
@media (max-width:560px){#cw-backups-modal{padding:8px}#cw-backups-modal .br-dialog{width:calc(100vw - 16px);height:calc(100vh - 16px);border-radius:12px}#cw-backups-modal .br-head{padding:13px 14px}#cw-backups-modal .br-body{padding:10px;gap:10px}#cw-backups-modal .br-section-grid,#cw-backups-modal .br-schedule-grid,#cw-backups-modal .br-section-head{grid-template-columns:1fr}#cw-backups-modal .br-section-head .br-actions{justify-content:flex-start}#cw-backups-modal .br-status{grid-template-columns:1fr}#cw-backups-modal .br-row{grid-template-columns:1fr}#cw-backups-modal .br-row-actions{justify-content:flex-start}#cw-backups-modal .br-days{grid-template-columns:1fr 1fr}}
`;
    document.head.appendChild(s);
  }

  function scopeSelect(id, value){
    const sel = el("select", { id });
    for (const [v, label] of SCOPES) sel.appendChild(el("option", { value: v, text: label }));
    sel.value = value || "app_state";
    return sel;
  }

  function check(id, label, checked){
    const input = el("input", { id, type: "checkbox", checked: !!checked });
    return el("label", { class: "br-check", for: id }, [input, el("span", { text: label })]);
  }

  function switcher(id, label, checked){
    const input = el("input", { id, type: "checkbox", checked: !!checked });
    return el("label", { class: "br-switch", for: id }, [
      input,
      el("span", { class: "br-switch-ui", "aria-hidden": "true" }),
      el("span", { class: "br-switch-text", text: label }),
      el("span", { class: "br-switch-state", "aria-hidden": "true" })
    ]);
  }

  function buildShell(){
    ensureStyles();
    if ($("cw-backups-modal")) return;
    const modal = el("div", { id: "cw-backups-modal", class: "hidden", "aria-hidden": "true" });
    const dialog = el("div", { class: "br-dialog", role: "dialog", "aria-modal": "true", "aria-label": "Backup and Restore" });
    const head = el("div", { class: "br-head" }, [
      el("div", {}, [
        el("div", { class: "br-title", text: "Backup & Restore" }),
        el("div", { class: "br-sub", text: "Back up CrossWatch config, Normal state, or a Full archive." })
      ]),
      el("button", { class: "br-close", type: "button", title: "Close", "aria-label": "Close", on: { click: close } }, [icon("close")])
    ]);
    const body = el("div", { class: "br-body", id: "br-body" });
    dialog.appendChild(head);
    dialog.appendChild(body);
    modal.appendChild(dialog);
    modal.addEventListener("click", (e) => { if (e.target === modal) close(); });
    ["input", "change"].forEach((name) => {
      modal.addEventListener(name, (e) => e.stopPropagation());
    });
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && !modal.classList.contains("hidden")) close();
    });
    document.body.appendChild(modal);
    renderBody();
  }

  function renderBody(){
    const body = $("br-body");
    if (!body) return;
    body.replaceChildren();
    applySplitHeight(body);
    body.appendChild(renderControls());
    body.appendChild(renderSplitter(body));
    body.appendChild(renderListPanel());
  }

  function splitMode(){
    return state.mode === "scheduled" ? "scheduled" : "manual";
  }

  function splitStorageKey(mode){
    return `${SPLIT_STORAGE_KEY}.${mode || splitMode()}`;
  }

  function splitDefault(mode){
    return SPLIT_DEFAULTS[mode || splitMode()] || SPLIT_DEFAULTS.manual;
  }

  function splitMin(mode){
    return SPLIT_MINS[mode || splitMode()] || SPLIT_MINS.manual;
  }

  function clampSplitHeight(body, value){
    const mode = splitMode();
    const min = splitMin(mode);
    const max = Math.max(min, body.clientHeight - 220);
    return Math.min(Math.max(Number(value) || splitDefault(mode), min), max);
  }

  function storedSplitHeight(){
    const mode = splitMode();
    if (state.controlsHeight[mode]) return state.controlsHeight[mode];
    try {
      const raw = Number(window.localStorage?.getItem(splitStorageKey(mode)) || 0);
      return Number.isFinite(raw) && raw > 0 ? raw : splitDefault(mode);
    } catch {
      return splitDefault(mode);
    }
  }

  function applySplitHeight(body, value){
    const wanted = value || storedSplitHeight();
    if (body.clientHeight <= 0) {
      body.style.setProperty("--br-controls-height", `${Number(wanted) || splitDefault()}px`);
      return;
    }
    const height = clampSplitHeight(body, wanted);
    const mode = splitMode();
    state.controlsHeight[mode] = height;
    body.style.setProperty("--br-controls-height", `${height}px`);
    try { window.localStorage?.setItem(splitStorageKey(mode), String(height)); } catch {}
  }

  function renderSplitter(body){
    const splitter = el("button", {
      class: "br-splitter",
      type: "button",
      title: "Resize panels",
      "aria-label": "Resize backup panels",
      "aria-orientation": "horizontal"
    });
    splitter.addEventListener("pointerdown", (e) => {
      if (e.button !== 0) return;
      e.preventDefault();
      const modal = $("cw-backups-modal");
      const startY = e.clientY;
      const startHeight = state.controlsHeight[splitMode()] || body.getBoundingClientRect().height * 0.42;
      splitter.setPointerCapture(e.pointerId);
      modal?.classList.add("br-resizing");
      const move = (ev) => applySplitHeight(body, startHeight + ev.clientY - startY);
      const stop = () => {
        modal?.classList.remove("br-resizing");
        splitter.removeEventListener("pointermove", move);
        splitter.removeEventListener("pointerup", stop);
        splitter.removeEventListener("pointercancel", stop);
      };
      splitter.addEventListener("pointermove", move);
      splitter.addEventListener("pointerup", stop);
      splitter.addEventListener("pointercancel", stop);
    });
    splitter.addEventListener("keydown", (e) => {
      if (e.key !== "ArrowUp" && e.key !== "ArrowDown") return;
      e.preventDefault();
      applySplitHeight(body, (state.controlsHeight[splitMode()] || storedSplitHeight()) + (e.key === "ArrowDown" ? 20 : -20));
    });
    return splitter;
  }

  function renderControls(){
    const schedule = state.schedule || {};
    const panel = el("div", { class: "br-panel br-controls" });
    const activeMode = state.mode === "scheduled" ? "scheduled" : "manual";
    const setMode = (mode) => {
      state.mode = mode === "scheduled" ? "scheduled" : "manual";
      renderBody();
    };
    const tab = (mode, label) => el("button", {
      class: `br-mode-tab ${activeMode === mode ? "active" : ""}`,
      type: "button",
      "aria-pressed": activeMode === mode ? "true" : "false",
      on: { click: () => setMode(mode) }
    }, [label]);
    panel.appendChild(el("div", { class: "br-mode-top" }, [
      el("div", { class: "br-mode-tabs", role: "group", "aria-label": "Backup mode" }, [
        tab("manual", "Manual Backup"),
        tab("scheduled", "Scheduled Backups")
      ]),
      el("div", { id: "br-msg", class: `br-msg ${state.message ? "" : "hidden"}`, text: state.message })
    ]));

    const create = el("div", { class: "br-mode-pane br-section" });
    create.appendChild(el("div", { class: "br-manual-grid" }, [
      el("div", { class: "br-field" }, [el("label", { for: "br-scope", text: "Scope" }), scopeSelect("br-scope", "app_state")]),
      el("div", { class: "br-field" }, [el("label", { for: "br-label", text: "Label" }), el("input", { id: "br-label", type: "text", value: "manual" })]),
      el("div", { class: "br-actions" }, [
        el("button", { class: "br-btn primary", type: "button", on: { click: createNow } }, ["Create Backup"]),
        el("button", { class: "br-btn", type: "button", on: { click: () => $("br-upload")?.click() } }, ["Import"]),
        el("input", { id: "br-upload", class: "br-upload", type: "file", accept: ".zip", on: { change: uploadBackup } })
      ])
    ]));

    const scheduled = el("div", { class: "br-mode-pane br-section br-scheduled" });
    scheduled.appendChild(el("div", { class: "br-section-head" }, [
      el("h3", { text: "Scheduled Backups" }),
      switcher("br-sch-enabled", "Enable", !!schedule.active),
      el("div", { class: "br-actions" }, [
        el("button", { class: "br-btn primary", type: "button", on: { click: saveSchedule } }, ["Save Schedule"])
      ])
    ]));
    scheduled.appendChild(el("div", { class: "br-schedule-grid" }, [
      el("div", { class: "br-field" }, [el("label", { for: "br-sch-scope", text: "Scope" }), scopeSelect("br-sch-scope", schedule.scope || "app_state")]),
      el("div", { class: "br-field" }, [el("label", { for: "br-sch-at", text: "Time" }), el("input", { id: "br-sch-at", type: "time", value: schedule.at || "03:00" })]),
      el("div", { class: "br-field" }, [el("label", { for: "br-ret-days", text: "Retention days" }), el("input", { id: "br-ret-days", type: "number", value: String(schedule.retention_days ?? 30), min: "0" })]),
      el("div", { class: "br-field" }, [el("label", { for: "br-max-backups", text: "Max backups" }), el("input", { id: "br-max-backups", type: "number", value: String(schedule.max_backups ?? 10), min: "0" })])
    ]));
    const days = Array.isArray(schedule.days) ? schedule.days.map(Number) : [];
    const daysBox = el("div", { class: "br-days" });
    DAY_NAMES.forEach((name, i) => {
      const n = i + 1;
      const cb = el("input", { type: "checkbox", value: String(n), checked: days.includes(n) });
      daysBox.appendChild(el("label", { class: "br-day" }, [cb, el("span", { text: name })]));
    });
    scheduled.appendChild(el("div", { class: "br-field br-days-field" }, [el("label", { text: "Days" }), daysBox]));
    panel.appendChild(el("div", { class: "br-mode-window" }, [
      el("div", { class: `br-mode-track ${activeMode}` }, [create, scheduled])
    ]));
    return panel;
  }

  function renderStatus(panel){
    const latest = state.backups[0] || {};
    const total = state.backups.reduce((n, b) => n + Number(b.size || 0), 0);
    const ext = latest.external_key_required ? "External key" : (latest.master_key_included ? "Key included" : "No key needed");
    const next = (state.schedule || {}).active ? `${state.schedule.at || "03:00"}` : "Disabled";
    panel.appendChild(el("div", { class: "br-status" }, [
      el("div", { class: "br-stat" }, [el("b", { text: fmtDate(latest.created_at, latest.mtime) }), el("span", { text: "Last backup" })]),
      el("div", { class: "br-stat" }, [el("b", { text: next }), el("span", { text: "Schedule" })]),
      el("div", { class: "br-stat" }, [el("b", { text: fmtBytes(total) }), el("span", { text: "Stored" })]),
      el("div", { class: "br-stat" }, [el("b", { text: ext }), el("span", { text: "Key status" })])
    ]));
  }

  function renderListPanel(){
    const panel = el("div", { class: "br-panel br-list-panel" });
    panel.appendChild(el("div", { class: "br-list-head" }, [
      el("h3", { text: "Backup List" }),
      el("button", {
        class: `br-iconbtn ${state.refreshing ? "spin" : ""}`,
        type: "button",
        title: "Refresh",
        "aria-label": "Refresh",
        disabled: state.refreshing,
        on: { click: () => refresh({ busy: true }) }
      }, [icon("refresh")])
    ]));
    renderStatus(panel);
    const list = el("div", { class: "br-list", id: "br-list" });
    if (!state.backups.length) {
      list.appendChild(el("div", { class: "br-row" }, [el("div", { class: "br-main" }, [el("div", { class: "br-name", text: "No backups yet" }), el("div", { class: "br-meta" }, [el("span", { text: "Create one manually or enable the schedule." })])])]));
    } else {
      state.backups.forEach((b) => list.appendChild(renderBackupRow(b)));
    }
    panel.appendChild(list);
    return panel;
  }

  function renderBackupRow(b){
    const path = String(b.path || "");
    const row = el("div", { class: `br-row ${state.selected === path ? "active" : ""}` });
    const main = el("div", { class: "br-main" }, [
      el("div", { class: "br-name", text: b.label || PathName(path) }),
      el("div", { class: "br-meta" }, [
        el("span", { class: "br-pill", text: scopeLabel(b.scope) }),
        el("span", { class: "br-pill", text: fmtBytes(b.size) }),
        el("span", { class: "br-pill", text: fmtDate(b.created_at, b.mtime) }),
        el("span", { class: "br-pill", text: b.external_key_required ? "External key required" : (b.master_key_included ? "Key included" : "No key") })
      ])
    ]);
    const note = state.rowStatus[path];
    if (note?.text) {
      main.appendChild(el("div", { class: `br-row-note ${note.kind || ""}`, text: note.text }));
    }
    const actions = el("div", { class: "br-row-actions" }, [
      el("button", { class: "br-iconbtn", type: "button", title: "Download", "aria-label": "Download", on: { click: (e) => { e.stopPropagation(); downloadBackup(path); } } }, [icon("download")]),
      el("button", { class: "br-iconbtn", type: "button", title: "Validate", "aria-label": "Validate", on: { click: (e) => { e.stopPropagation(); validateBackup(path); } } }, [icon("verified")]),
      el("button", { class: "br-iconbtn", type: "button", title: "Restore", "aria-label": "Restore", on: { click: (e) => { e.stopPropagation(); restoreBackup(path); } } }, [icon("settings_backup_restore")]),
      el("button", { class: "br-iconbtn", type: "button", title: "Delete", "aria-label": "Delete", on: { click: (e) => { e.stopPropagation(); deleteBackup(path); } } }, [icon("delete")])
    ]);
    row.appendChild(main);
    row.appendChild(actions);
    row.addEventListener("click", () => { state.selected = path; renderBody(); });
    return row;
  }

  function PathName(path){
    const parts = String(path || "").split("/");
    return parts[parts.length - 1] || "Backup";
  }

  function currentCreateOptions(){
    return {
      scope: $("br-scope")?.value || "app_state",
      label: $("br-label")?.value || "manual",
      include_snapshots: false,
      include_reports: false,
      include_cache: false
    };
  }

  async function createNow(){
    try {
      toast("Creating backup...");
      await postJSON("/api/backups/create", currentCreateOptions());
      toast("Backup created");
      await refresh();
    } catch (e) {
      toast(`Backup failed: ${e.message || e}`, 3200);
    }
  }

  async function uploadBackup(e){
    const input = e.currentTarget;
    const file = input?.files?.[0];
    if (!file) return;
    try {
      const form = new FormData();
      form.append("file", file);
      toast("Importing backup...");
      await api("/api/backups/upload", { method: "POST", body: form });
      toast("Backup imported");
      input.value = "";
      await refresh();
    } catch (err) {
      toast(`Import failed: ${err.message || err}`, 3200);
    }
  }

  function downloadBackup(path){
    if (!path) return;
    window.location.href = `/api/backups/download?path=${encodeURIComponent(path)}`;
  }

  async function validateBackup(path){
    try {
      state.rowStatus[path] = { kind: "", text: "Validating backup..." };
      renderBody();
      const res = await postJSON("/api/backups/validate", { path });
      const errors = res?.validation?.errors || [];
      state.rowStatus[path] = errors.length
        ? { kind: "warn", text: `Validation found ${errors.length} issue(s).` }
        : { kind: "ok", text: "Successfully validated." };
      renderBody();
    } catch (e) {
      state.rowStatus[path] = { kind: "warn", text: `Validation failed: ${e.message || e}` };
      renderBody();
    }
  }

  async function restoreBackup(path){
    if (!path) return;
    const ok = window.confirm("Restore this CrossWatch backup?\n\nA pre-restore backup will be created first and CrossWatch will restart after restore.");
    if (!ok) return;
    try {
      toast("Restoring backup...");
      await postJSON("/api/backups/restore", { path, restart: true });
      toast("Restore applied. Restarting...");
      setTimeout(() => { try { window.location.reload(); } catch {} }, 2400);
    } catch (e) {
      toast(`Restore failed: ${e.message || e}`, 4200);
    }
  }

  async function deleteBackup(path){
    if (!path) return;
    if (!window.confirm("Delete this backup?")) return;
    try {
      await postJSON("/api/backups/delete", { path });
      toast("Backup deleted");
      await refresh();
    } catch (e) {
      toast(`Delete failed: ${e.message || e}`, 3200);
    }
  }

  function readScheduleForm(){
    const dayChecks = Array.from(document.querySelectorAll(".br-days input[type=checkbox]"));
    const days = dayChecks.filter((x) => x.checked).map((x) => Number(x.value)).filter((n) => n >= 1 && n <= 7);
    return {
      enabled: !!$("br-sch-enabled")?.checked,
      scope: $("br-sch-scope")?.value || "app_state",
      at: $("br-sch-at")?.value || "03:00",
      days,
      retention_days: Number($("br-ret-days")?.value || 30),
      max_backups: Number($("br-max-backups")?.value || 10),
      auto_delete_old: true,
      include_snapshots: false,
      include_reports: false,
      include_cache: false
    };
  }

  async function saveSchedule(){
    try {
      const res = await postJSON("/api/backups/schedule", readScheduleForm());
      state.schedule = res.schedule || {};
      toast("Backup schedule saved");
      renderBody();
    } catch (e) {
      toast(`Schedule failed: ${e.message || e}`, 3200);
    }
  }

  async function refresh(opts){
    const busy = !!(opts && opts.busy);
    if (busy) {
      state.refreshing = true;
      renderBody();
    }
    try {
      const [list, sched] = await Promise.all([
        api("/api/backups/list"),
        api("/api/backups/schedule")
      ]);
      state.backups = Array.isArray(list.backups) ? list.backups : [];
      state.schedule = sched.schedule || {};
    } finally {
      if (busy) state.refreshing = false;
      renderBody();
    }
  }

  function open(){
    buildShell();
    const modal = $("cw-backups-modal");
    if (!modal) return;
    document.body.classList.add("br-backups-open", "cx-modal-open");
    modal.classList.remove("hidden");
    modal.setAttribute("aria-hidden", "false");
    refresh().catch((e) => toast(`Refresh failed: ${e.message || e}`, 3200));
  }

  function close(){
    const modal = $("cw-backups-modal");
    if (!modal) return;
    modal.classList.add("hidden");
    modal.setAttribute("aria-hidden", "true");
    document.body.classList.remove("br-backups-open", "cx-modal-open");
  }

  window.openBackupRestore = open;
  (window.CW ||= {});
  window.CW.Backups = { open, close, refresh };
})();
