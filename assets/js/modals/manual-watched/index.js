/* assets/js/modals/manual-watched/index.js */
/* Manual watched item modal */

const STORAGE_KEY = "cw.manualWatched.providers";
const MEDIA_SERVER_PROVIDERS = new Set(["PLEX", "JELLYFIN", "EMBY"]);
const STEPS = [
  ["Search", "Find your movie or show"],
  ["Actions", "Choose what to do"],
  ["Providers", "Select where to send it"],
  ["Details", "Dates and rating"],
  ["Review", "Confirm and send"],
];
const ACTIONS = [
  ["history", "History", "history", "Mark the item as watched", "history_enabled"],
  ["watchlist", "Watchlist", "bookmark_add", "Add the item to watchlists", "watchlist_enabled"],
  ["rating", "Rating", "star", "Send a score to providers", "ratings_enabled"],
];

const fjson = async (url, opts = {}) => {
  const r = await fetch(url, { cache: "no-store", ...opts });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(data?.error || `${r.status}`);
  return data || {};
};

const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m]));
const providerKey = (item) => `${String(item?.provider || "").toUpperCase()}:${String(item?.instance || "default")}`;
const mediaLabel = (type) => String(type || "").toLowerCase() === "show" ? "Show" : "Movie";
const artType = (type) => String(type || "").toLowerCase() === "show" ? "tv" : "movie";
const todayLocal = () => new Date().toISOString().slice(0, 10);
const closeModal = () => window.cxCloseModal?.();

function injectCSS() {
  if (document.getElementById("cw-manual-watched-css")) return;
  const el = document.createElement("style");
  el.id = "cw-manual-watched-css";
  el.textContent = `
.cx-modal-shell.cw-manual-watched-modal{width:min(var(--cxModalMaxW,1180px),calc(100vw - 40px))!important;max-width:min(var(--cxModalMaxW,1180px),calc(100vw - 40px))!important;height:min(var(--cxModalMaxH,84vh),calc(100vh - 40px))!important;background:linear-gradient(180deg,rgba(8,10,18,.98),rgba(6,8,14,.98))!important;border:1px solid rgba(108,126,255,.18)!important;box-shadow:0 28px 90px rgba(0,0,0,.58),inset 0 0 0 1px rgba(255,255,255,.03)!important}.cw-mw{--mw-bg:#080b12;--mw-panel:#111722;--mw-card:#171d29;--mw-card-2:#1b2230;--mw-text:#eef3ff;--mw-muted:#aeb7c7;--mw-border:rgba(255,255,255,.1);--mw-border-soft:rgba(255,255,255,.07);--mw-accent:#737ee8;--mw-accent-soft:rgba(115,126,232,.18);--mw-ok:#55d889;position:relative;display:flex;flex-direction:column;height:100%;min-height:0;color:var(--mw-text);background:radial-gradient(900px 320px at 0 0,rgba(116,126,232,.11),transparent 45%),radial-gradient(740px 260px at 100% 0,rgba(64,139,255,.08),transparent 42%),linear-gradient(180deg,rgba(8,11,18,.985),rgba(5,7,12,.99));overflow:hidden}.cw-mw *{box-sizing:border-box}.cw-mw [hidden]{display:none!important}
.cw-mw .cx-head{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:start;gap:12px;min-height:86px;padding:18px 22px;border-bottom:1px solid var(--mw-border-soft);background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.01))}.cw-mw-title{font-size:24px;font-weight:850;line-height:1.1;letter-spacing:-.02em;color:#f6f8ff}.cw-mw-sub{margin-top:7px;color:var(--mw-muted);font-size:14px;line-height:1.35}.cw-mw-close{width:38px;height:38px;display:grid;place-items:center;border:1px solid transparent;border-radius:12px;background:transparent;color:var(--mw-muted);cursor:pointer}.cw-mw-close:hover{background:rgba(255,255,255,.05);border-color:var(--mw-border);color:var(--mw-text)}.cw-mw-close .material-symbols-rounded{font-size:22px}
.cw-mw-body{flex:1;min-height:0;display:grid;grid-template-columns:280px minmax(0,1fr);overflow:hidden}.cw-mw-steps{padding:16px 14px;border-right:1px solid var(--mw-border-soft);background:linear-gradient(180deg,rgba(255,255,255,.018),rgba(255,255,255,.004));overflow:auto}.cw-mw-step{position:relative;display:grid;grid-template-columns:38px minmax(0,1fr);gap:12px;width:100%;min-height:78px;padding:10px 12px;border:1px solid transparent;border-radius:16px;background:transparent;color:var(--mw-muted);text-align:left;cursor:pointer}.cw-mw-step:not(:last-child)::after{content:"";position:absolute;left:31px;top:50px;bottom:-18px;width:2px;background:rgba(255,255,255,.08)}.cw-mw-step-num{position:relative;z-index:1;display:grid;place-items:center;width:38px;height:38px;border-radius:50%;background:#1a2130;border:1px solid var(--mw-border);color:var(--mw-muted);font-size:13px;font-weight:800}.cw-mw-step-title{margin-top:4px;color:inherit;font-size:14px;font-weight:800;line-height:1.2}.cw-mw-step-sub{margin-top:5px;font-size:12px;line-height:1.3;color:var(--mw-muted)}.cw-mw-step.active{background:linear-gradient(135deg,rgba(94,105,222,.28),rgba(50,62,139,.16));border-color:rgba(122,132,255,.36);color:#f5f7ff}.cw-mw-step.active .cw-mw-step-num{background:linear-gradient(180deg,rgba(102,113,240,.92),rgba(75,86,198,.92));border-color:rgba(170,178,255,.35);color:#fff}.cw-mw-step.done .cw-mw-step-num{background:rgba(85,216,137,.14);border-color:rgba(85,216,137,.25);color:#b9ffd7}.cw-mw-step:disabled{cursor:default;opacity:.7}
.cw-mw-main{min-width:0;display:flex;flex-direction:column;min-height:0}.cw-mw-stage{flex:1;min-height:0;overflow:auto;padding:24px 28px}.cw-mw-form{max-width:900px;display:grid;gap:16px}.cw-mw-headline{display:flex;align-items:flex-start;gap:10px;min-width:0}.cw-mw-headline b{font-size:18px;line-height:1.2;color:#f6f8ff}.cw-mw-headline span{color:#8fb3ff;font-weight:800}.cw-mw-copy{color:var(--mw-muted);font-size:13px;line-height:1.45}.cw-mw-card,.cw-mw-result,.cw-mw-action,.cw-mw-provider,.cw-mw-review-row{border:1px solid var(--mw-border-soft);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.018));box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
.cw-mw-card{border-radius:16px;padding:14px}.cw-mw-label{display:block;margin-bottom:8px;color:rgba(205,215,235,.72);font-size:11px;font-weight:800;letter-spacing:.08em;text-transform:uppercase}.cw-mw-row{display:flex;align-items:center;gap:10px;flex-wrap:wrap}.cw-mw-search{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:10px}.cw-mw-input,.cw-mw-date{width:100%;height:40px;padding:0 12px;border:1px solid var(--mw-border);border-radius:12px;background:rgba(7,10,18,.92);color:var(--mw-text);font:inherit;font-size:13px;outline:none}.cw-mw-input:focus,.cw-mw-date:focus{border-color:rgba(122,132,255,.46);box-shadow:0 0 0 3px rgba(122,132,255,.14)}.cw-mw-btn,.cw-mw-chip{height:38px;display:inline-flex;align-items:center;justify-content:center;gap:7px;border:1px solid var(--mw-border);border-radius:14px;background:rgba(255,255,255,.05);color:var(--mw-text);font:inherit;font-size:12px;font-weight:800;letter-spacing:.04em;cursor:pointer}.cw-mw-btn{padding:0 14px}.cw-mw-chip{padding:0 12px}.cw-mw-btn:hover,.cw-mw-chip:hover{background:rgba(255,255,255,.08);border-color:rgba(255,255,255,.16)}.cw-mw-btn.primary,.cw-mw-chip.active{background:linear-gradient(135deg,rgba(111,91,255,.32),rgba(59,130,246,.24));border-color:rgba(122,107,255,.36);box-shadow:inset 0 1px 0 rgba(255,255,255,.06)}.cw-mw-btn:disabled,.cw-mw-chip:disabled{opacity:.45;cursor:not-allowed;box-shadow:none}.cw-mw-btn .material-symbols-rounded{font-size:18px}
.cw-mw-results{display:grid;gap:10px}.cw-mw-result{position:relative;width:100%;display:grid;grid-template-columns:66px minmax(0,1fr) 34px;gap:14px;align-items:center;padding:12px;border-radius:16px;color:inherit;text-align:left;cursor:pointer}.cw-mw-result:hover,.cw-mw-result.active{border-color:rgba(122,132,255,.55);background:linear-gradient(135deg,rgba(94,105,222,.19),rgba(45,55,124,.12))}.cw-mw-poster{width:66px;height:98px;border-radius:10px;overflow:hidden;border:1px solid var(--mw-border-soft);background:#151b26}.cw-mw-poster img{width:100%;height:100%;object-fit:cover;display:block}.cw-mw-ghost{height:100%;display:grid;place-items:center;color:var(--mw-muted);font-size:11px}.cw-mw-result-title{display:flex;align-items:center;gap:8px;min-width:0;color:#f5f7ff;font-size:16px;font-weight:800}.cw-mw-result-title span:first-child{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.cw-mw-badge{display:inline-flex;align-items:center;justify-content:center;min-height:22px;padding:0 8px;border-radius:999px;border:1px solid var(--mw-border);background:rgba(255,255,255,.05);color:rgba(226,234,248,.82);font-size:10px;font-weight:800;letter-spacing:.04em;text-transform:uppercase}.cw-mw-meta,.cw-mw-muted{color:var(--mw-muted);font-size:12px;line-height:1.4}.cw-mw-overview{margin-top:7px;color:rgba(220,228,243,.78);font-size:12px;line-height:1.45}.cw-mw-radio{display:grid;place-items:center;width:34px;height:34px;border-radius:50%;border:2px solid rgba(170,180,210,.26);color:transparent}.cw-mw-result.active .cw-mw-radio{border-color:rgba(122,132,255,.72);background:rgba(122,132,255,.82);color:#fff}.cw-mw-radio .material-symbols-rounded{font-size:21px}
.cw-mw-actions-grid,.cw-mw-providers{display:grid;gap:10px}.cw-mw-action,.cw-mw-provider{display:grid;grid-template-columns:38px minmax(0,1fr) 22px;gap:12px;align-items:center;min-height:72px;padding:13px;border-radius:14px;cursor:pointer;color:inherit;text-align:left}.cw-mw-action:hover,.cw-mw-action.active,.cw-mw-provider:hover,.cw-mw-provider.active{border-color:rgba(122,132,255,.36);background:linear-gradient(135deg,rgba(94,105,222,.18),rgba(45,55,124,.1))}.cw-mw-action-icon,.cw-mw-provider-icon{display:grid;place-items:center;width:38px;height:38px;border-radius:12px;border:1px solid var(--mw-border);background:rgba(0,0,0,.18);color:#dbe3ff}.cw-mw-action-icon .material-symbols-rounded{font-size:21px}.cw-mw-action-title,.cw-mw-provider-title{font-size:14px;font-weight:800;color:#f5f7ff;line-height:1.2}.cw-mw-action-copy,.cw-mw-provider-copy{display:grid;gap:5px;min-width:0}.cw-mw-tools{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap}.cw-mw-link{border:0;background:transparent;color:#c8d2ea;font:inherit;font-size:12px;font-weight:700;cursor:pointer;padding:0}.cw-mw-link:hover{color:#fff}.cw-mw-provider-logo{width:20px;height:20px;object-fit:contain}.cw-mw-provider-badges{display:flex;gap:6px;flex-wrap:wrap}.cw-mw-provider.disabled{opacity:.48;cursor:not-allowed}
.cw-mw-selected{display:grid;grid-template-columns:74px minmax(0,1fr);gap:14px;align-items:start}.cw-mw-selected .cw-mw-poster{width:74px;height:110px}.cw-mw-selected-title{display:flex;align-items:center;gap:8px;flex-wrap:wrap;color:#f7f9ff;font-size:18px;font-weight:850}.cw-mw-details-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.cw-mw-rating-value{min-width:54px;height:30px;display:inline-flex;align-items:center;justify-content:center;padding:0 10px;border-radius:999px;border:1px solid rgba(122,132,255,.28);background:rgba(122,132,255,.13);font-size:12px;font-weight:800}.cw-mw-slider{appearance:none;-webkit-appearance:none;width:100%;height:10px;border-radius:999px;border:1px solid var(--mw-border);background:linear-gradient(90deg,rgba(115,126,232,.9) 0%,rgba(115,126,232,.9) var(--rating-progress,80%),rgba(255,255,255,.07) var(--rating-progress,80%),rgba(255,255,255,.05) 100%);outline:none}.cw-mw-slider::-webkit-slider-thumb{appearance:none;-webkit-appearance:none;width:20px;height:20px;border-radius:50%;border:1px solid rgba(240,244,255,.72);background:#eef3ff;box-shadow:0 8px 16px rgba(0,0,0,.28)}.cw-mw-slider::-moz-range-thumb{width:20px;height:20px;border-radius:50%;border:1px solid rgba(240,244,255,.72);background:#eef3ff}.cw-mw-empty{padding:14px;border:1px dashed var(--mw-border);border-radius:14px;color:var(--mw-muted);font-size:13px;line-height:1.45}
.cw-mw-review{display:grid;gap:10px}.cw-mw-review-row{display:grid;grid-template-columns:150px minmax(0,1fr);gap:12px;align-items:start;padding:12px;border-radius:14px}.cw-mw-review-k{color:var(--mw-muted);font-size:11px;font-weight:800;letter-spacing:.08em;text-transform:uppercase}.cw-mw-review-v{display:flex;gap:7px;flex-wrap:wrap;color:#f5f7ff;font-size:13px;line-height:1.45}.cw-mw-status{min-height:20px;color:var(--mw-muted);font-size:12px}.cw-mw-status.error{color:#ffc7d1}.cw-mw-status.success{color:#c7ffe1}.cw-mw-foot{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:12px;align-items:center;padding:14px 18px;border-top:1px solid var(--mw-border-soft);background:rgba(5,8,14,.34)}.cw-mw-foot-actions{display:flex;align-items:center;gap:8px;justify-content:flex-end}
html[data-cw-theme=flat-light] .cw-mw{--mw-bg:#f8fafc;--mw-panel:#fff;--mw-card:#fff;--mw-card-2:#eef3fa;--mw-text:#172033;--mw-muted:#667085;--mw-border:rgba(16,24,40,.16);--mw-border-soft:rgba(16,24,40,.12);background:#f8fafc;color:#172033}.cw-mw .cw-mw-date{color-scheme:dark}html[data-cw-theme=flat-light] .cw-mw .cw-mw-date{color-scheme:light}html[data-cw-theme=flat-light] .cw-mw-input,html[data-cw-theme=flat-light] .cw-mw-date{background:#fff;color:#172033}html[data-cw-theme=flat-light] .cw-mw .cx-head,html[data-cw-theme=flat-light] .cw-mw-foot{background:#fff}html[data-cw-theme] .cw-mw :is(.cw-mw-result.active,.cw-mw-action.active,.cw-mw-provider.active,.cw-mw-chip.active){background:color-mix(in srgb,var(--cw-flat-accent) 18%,var(--cw-flat-panel-3))!important;border-color:color-mix(in srgb,var(--cw-flat-accent) 48%,var(--cw-flat-border))!important;box-shadow:none!important}.cw-mw :is(.cw-mw-stage,.cw-mw-steps){scrollbar-width:thin;scrollbar-color:var(--cw-scrollbar-thumb,#56607c) var(--cw-scrollbar-track,#0c1018)}.cw-mw :is(.cw-mw-stage,.cw-mw-steps)::-webkit-scrollbar{width:10px;height:10px}.cw-mw :is(.cw-mw-stage,.cw-mw-steps)::-webkit-scrollbar-corner{background:transparent}
.cw-mw :is(.cw-mw-stage,.cw-mw-steps)::-webkit-scrollbar-track{background:var(--cw-scrollbar-track-soft,rgba(8,11,18,.72));border-radius:12px;box-shadow:inset 0 0 0 1px var(--cw-scrollbar-track-ring,rgba(255,255,255,.06))}.cw-mw :is(.cw-mw-stage,.cw-mw-steps)::-webkit-scrollbar-thumb{border:2px solid var(--cw-scrollbar-thumb-border,rgba(8,11,18,.9));border-radius:12px;background:var(--cw-scrollbar-thumb,#56607c);box-shadow:var(--cw-scrollbar-thumb-shadow,none)}.cw-mw :is(.cw-mw-stage,.cw-mw-steps)::-webkit-scrollbar-thumb:hover{background:var(--cw-scrollbar-thumb-hover,#6b7695)}.cw-mw-step>span:last-child{display:grid;align-content:center;min-width:0}.cw-mw-step-title,.cw-mw-step-sub{display:block;min-width:0;overflow:hidden;text-overflow:ellipsis}.cw-mw-form{max-width:none}.cw-mw-selected{padding:14px;border:1px solid var(--mw-border-soft);border-radius:16px;background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.014));box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}.cw-mw-actions-grid{grid-template-columns:repeat(3,minmax(0,1fr))}.cw-mw-action{grid-template-columns:44px minmax(0,1fr) 24px;min-height:92px;padding:16px;border-radius:16px;background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.014))}
.cw-mw-action.active{box-shadow:inset 3px 0 0 var(--mw-accent),inset 0 1px 0 rgba(255,255,255,.04)}.cw-mw-action-icon{width:44px;height:44px;border-radius:14px;background:rgba(8,11,18,.62)}.cw-mw-action-title{font-size:15px}.cw-mw-providers{grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}.cw-mw-provider{grid-template-columns:46px minmax(0,1fr) 24px;min-height:98px;padding:15px;border-radius:16px;background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.014))}.cw-mw-provider.active{box-shadow:inset 3px 0 0 var(--mw-ok),inset 0 1px 0 rgba(255,255,255,.04)}.cw-mw-provider-icon{width:46px;height:46px;border-radius:14px;background:rgba(8,11,18,.64)}.cw-mw-provider-logo{width:27px;height:27px}.cw-mw-provider-title{font-size:15px}.cw-mw-provider-badges{margin-top:2px}.cw-mw-link{height:30px;padding:0 9px;border:1px solid transparent;border-radius:10px}.cw-mw-link:hover{border-color:var(--mw-border-soft);background:rgba(255,255,255,.04)}html[data-cw-theme=flat-dark] .cx-modal-shell.cw-manual-watched-modal{background:#10141c!important;border-color:rgba(255,255,255,.14)!important;box-shadow:none!important}
html[data-cw-theme=flat-dark] .cw-mw{--mw-bg:var(--bg,#0c0f15);--mw-panel:var(--panel,#151922);--mw-card:var(--panel2,#1b202b);--mw-card-2:#1e2430;--mw-text:var(--fg,#f2f4f8);--mw-muted:var(--muted,#a6afbf);--mw-border:var(--border,rgba(255,255,255,.12));--mw-border-soft:var(--border,rgba(255,255,255,.12));--mw-accent:var(--accent,#7d86c9);--mw-ok:var(--accent2,#52c794);background:var(--mw-bg)!important}html[data-cw-theme=flat-dark] .cw-mw :is(.cx-head,.cw-mw-steps,.cw-mw-foot){background:var(--mw-panel)!important;border-color:var(--mw-border)!important;box-shadow:none!important}html[data-cw-theme=flat-dark] .cw-mw :is(.cw-mw-card,.cw-mw-result,.cw-mw-action,.cw-mw-provider,.cw-mw-review-row,.cw-mw-selected,.cw-mw-empty,.cw-mw-input,.cw-mw-date,.cw-mw-chip,.cw-mw-btn){background:var(--mw-card)!important;border-color:var(--mw-border)!important;box-shadow:none!important}html[data-cw-theme=flat-dark] .cw-mw :is(.cw-mw-result:hover,.cw-mw-action:hover,.cw-mw-provider:hover,.cw-mw-link:hover){background:color-mix(in srgb,var(--mw-accent) 8%,var(--mw-card))!important;border-color:color-mix(in srgb,var(--mw-accent) 32%,var(--mw-border))!important}
html[data-cw-theme=flat-dark] .cw-mw :is(.cw-mw-step.active,.cw-mw-chip.active,.cw-mw-btn.primary,.cw-mw-result.active,.cw-mw-action.active,.cw-mw-provider.active){background:color-mix(in srgb,var(--mw-accent) 12%,var(--mw-card))!important;border-color:color-mix(in srgb,var(--mw-accent) 40%,var(--mw-border))!important;box-shadow:none!important}html[data-cw-theme=flat-dark] .cw-mw :is(.cw-mw-step.active,.cw-mw-action.active){box-shadow:inset 4px 0 0 var(--mw-accent)!important}html[data-cw-theme=flat-dark] .cw-mw .cw-mw-provider.active{box-shadow:inset 4px 0 0 var(--mw-ok)!important}html[data-cw-theme=flat-dark] .cw-mw :is(.cw-mw-step-title,.cw-mw-title,.cw-mw-headline b,.cw-mw-action-title,.cw-mw-provider-title,.cw-mw-selected-title,.cw-mw-result-title){color:var(--mw-text)!important;-webkit-text-fill-color:var(--mw-text)!important}html[data-cw-theme=flat-dark] .cw-mw :is(.cw-mw-sub,.cw-mw-copy,.cw-mw-step-sub,.cw-mw-muted,.cw-mw-meta,.cw-mw-overview){color:var(--mw-muted)!important;-webkit-text-fill-color:var(--mw-muted)!important}
html[data-cw-theme=flat-dark] .cw-mw .cw-mw-step.done .cw-mw-step-num{background:color-mix(in srgb,var(--mw-ok) 15%,var(--mw-card))!important;border-color:color-mix(in srgb,var(--mw-ok) 32%,var(--mw-border))!important;color:color-mix(in srgb,var(--mw-ok) 82%,white)!important}html[data-cw-theme=flat-light] .cw-mw :is(.cw-mw-card,.cw-mw-result,.cw-mw-action,.cw-mw-provider,.cw-mw-review-row,.cw-mw-selected,.cw-mw-empty,.cw-mw-chip,.cw-mw-btn){background:#fff!important;border-color:var(--mw-border)!important;box-shadow:none!important}html[data-cw-theme=flat-light] .cw-mw :is(.cw-mw-action-icon,.cw-mw-provider-icon,.cw-mw-step-num){background:#eef3fa!important;border-color:rgba(16,24,40,.14)!important;color:#344054!important}.cx-modal-shell.cw-manual-watched-modal{width:min(var(--cxModalMaxW,1360px),calc(100vw - 28px))!important;max-width:min(var(--cxModalMaxW,1360px),calc(100vw - 28px))!important;height:min(var(--cxModalMaxH,88vh),calc(100vh - 18px))!important;border-radius:18px!important}.cw-mw{background:linear-gradient(180deg,rgba(10,13,22,.98),rgba(7,10,17,.98))!important}
.cw-mw::before{content:"";position:absolute;right:-80px;top:82px;width:520px;height:calc(100% - 140px);background:linear-gradient(120deg,transparent 0 26%,rgba(124,92,255,.09) 26% 42%,transparent 42% 56%,rgba(58,142,255,.07) 56% 72%,transparent 72%);opacity:.9;pointer-events:none}.cw-mw .cx-head{position:relative;z-index:1;min-height:92px;padding:18px 22px!important;background:var(--mw-panel)!important;border-bottom:1px solid var(--mw-border)!important}.cw-mw-head{display:grid;grid-template-columns:56px minmax(0,1fr);align-items:center;gap:16px;min-width:0}.cw-mw-head-icon{display:grid;place-items:center;width:56px;height:56px;border-radius:14px;border:1px solid var(--mw-border);background:linear-gradient(180deg,rgba(255,255,255,.055),rgba(255,255,255,.025));color:var(--mw-accent)}.cw-mw-head-icon .material-symbols-rounded{font-size:30px}.cw-mw-title{font-size:25px!important;font-weight:900!important;letter-spacing:0!important}.cw-mw-sub{font-size:14px!important;font-weight:650;color:var(--mw-muted)!important}.cw-mw-close{width:44px!important;height:44px!important;border-radius:12px!important;border-color:var(--mw-border)!important;background:var(--mw-card)!important}.cw-mw-close .material-symbols-rounded{font-size:28px!important}
.cw-mw-body{position:relative;z-index:1;grid-template-columns:340px minmax(0,1fr)!important}.cw-mw-steps{padding:0!important;background:rgba(8,11,18,.08)!important;border-right:1px solid var(--mw-border)!important}.cw-mw-step{grid-template-columns:52px minmax(0,1fr) 20px!important;gap:14px!important;align-items:center;min-height:92px!important;margin:0!important;padding:18px 28px!important;border:0!important;border-bottom:1px solid var(--mw-border-soft)!important;border-radius:0!important;background:transparent!important}.cw-mw-step::before{content:"chevron_right";font-family:"Material Symbols Rounded";grid-column:3;grid-row:1;align-self:center;justify-self:end;font-size:22px;color:var(--mw-muted);opacity:.8}.cw-mw-step:not(:last-child)::after{display:none!important}.cw-mw-step-num{width:52px!important;height:52px!important;border-radius:14px!important;font-size:18px!important;background:var(--mw-card)!important}.cw-mw-step-title{margin:0!important;font-size:17px!important;font-weight:900!important;line-height:1.12!important}.cw-mw-step-sub{margin-top:7px!important;font-size:13px!important;line-height:1.38!important;white-space:normal!important}
.cw-mw-step.active{background:linear-gradient(90deg,color-mix(in srgb,var(--mw-accent) 26%,transparent),color-mix(in srgb,var(--mw-accent) 8%,transparent))!important;box-shadow:inset 4px 0 0 var(--mw-accent)!important}.cw-mw-step.active .cw-mw-step-num{background:color-mix(in srgb,var(--mw-accent) 20%,var(--mw-card))!important;border-color:color-mix(in srgb,var(--mw-accent) 46%,var(--mw-border))!important;color:#fff!important}.cw-mw-stage{padding:42px 58px 34px!important;scrollbar-gutter:stable}.cw-mw-stage-shell{display:grid;gap:22px;max-width:1120px}.cw-mw-form{gap:18px!important}.cw-mw-card{padding:20px!important;border-radius:16px!important}.cw-mw-label{font-size:12px!important;letter-spacing:.12em!important;margin-bottom:12px!important}.cw-mw-input,.cw-mw-date{height:52px!important;border-radius:14px!important;padding:0 18px!important;font-size:15px!important}.cw-mw-search{grid-template-columns:minmax(0,1fr) 170px!important;gap:14px!important}.cw-mw-btn{min-width:142px;height:48px!important;border-radius:12px!important;font-size:15px!important;font-weight:850!important}.cw-mw-chip{height:44px!important;min-width:112px;border-radius:14px!important;font-size:14px!important}.cw-mw-results{gap:14px!important}
.cw-mw-result{grid-template-columns:82px minmax(0,1fr) 42px!important;gap:18px!important;min-height:132px;padding:16px 18px!important;border-radius:16px!important}.cw-mw-poster{width:82px!important;height:118px!important;border-radius:12px!important}.cw-mw-result-title{font-size:19px!important}.cw-mw-meta,.cw-mw-muted{font-size:14px!important}.cw-mw-overview{font-size:14px!important;line-height:1.48!important}.cw-mw-radio{width:40px!important;height:40px!important}.cw-mw-selected{grid-template-columns:92px minmax(0,1fr)!important;padding:18px!important;gap:18px!important}.cw-mw-selected .cw-mw-poster{width:92px!important;height:132px!important}.cw-mw-selected-title{font-size:22px!important}.cw-mw-actions-grid{grid-template-columns:repeat(3,minmax(0,1fr))!important;gap:16px!important}.cw-mw-action{display:grid!important;grid-template-columns:56px minmax(0,1fr)!important;grid-template-rows:auto 28px!important;align-items:start!important;min-height:178px!important;padding:22px!important;border-radius:16px!important}.cw-mw-action-icon{width:56px!important;height:56px!important;border-radius:14px!important}.cw-mw-action-icon .material-symbols-rounded{font-size:29px!important}.cw-mw-action-copy{align-self:center!important}.cw-mw-action-title{font-size:20px!important}
.cw-mw-action .cw-mw-muted{font-size:14px!important;line-height:1.45!important}.cw-mw-providers{grid-template-columns:repeat(auto-fit,minmax(330px,1fr))!important;gap:16px!important}.cw-mw-provider{grid-template-columns:58px minmax(0,1fr) 28px!important;min-height:116px!important;padding:20px!important;border-radius:16px!important}.cw-mw-provider-icon{width:58px!important;height:58px!important;border-radius:14px!important}.cw-mw-provider-logo{width:36px!important;height:36px!important}.cw-mw-provider-title{font-size:20px!important}.cw-mw-badge{min-height:26px!important;padding:0 10px!important;font-size:11px!important}.cw-mw-tools{padding:0 2px}.cw-mw-link{height:34px!important;padding:0 12px!important;font-size:13px!important}.cw-mw-review-row{grid-template-columns:170px minmax(0,1fr)!important;padding:18px 20px!important;border-radius:16px!important}.cw-mw-review-k{font-size:12px!important}.cw-mw-review-v{font-size:15px!important}.cw-mw-foot{padding:20px 40px!important;background:var(--mw-panel)!important;border-top:1px solid var(--mw-border)!important}.cw-mw-foot-actions{gap:12px!important}.cw-mw-foot .cw-mw-btn{min-width:170px!important;height:52px!important}.cw-mw-media-toggle{display:flex;align-items:center;gap:14px;flex-wrap:wrap}
.cw-mw-media-toggle .cw-mw-chip{min-width:160px}.cw-mw-action{grid-template-columns:52px minmax(0,1fr)!important;grid-template-rows:auto!important;align-items:center!important;min-height:112px!important;padding:18px 20px!important}.cw-mw-action.active,.cw-mw-provider.active{box-shadow:none!important}.cw-mw-provider{position:relative;isolation:isolate;overflow:hidden;grid-template-columns:58px minmax(0,1fr)!important;min-height:106px!important}.cw-mw-provider::after{content:"";position:absolute;right:-18px;top:50%;width:42%;height:170%;transform:translateY(-50%) rotate(-8deg);background-image:var(--mw-provider-wm);background-repeat:no-repeat;background-position:center;background-size:contain;opacity:.075;filter:saturate(1.2) brightness(1.16);mix-blend-mode:screen;pointer-events:none;z-index:0}.cw-mw-provider>*{position:relative;z-index:1}.cw-mw-provider.active::after{opacity:.11}.cw-mw-provider.active{border-color:color-mix(in srgb,var(--mw-ok) 44%,var(--mw-border))!important;background:color-mix(in srgb,var(--mw-ok) 10%,var(--mw-card))!important}html[data-cw-theme=flat-dark] .cw-mw :is(.cw-mw-action.active,.cw-mw-provider.active){box-shadow:none!important}
html[data-cw-theme=flat-dark] .cw-mw .cw-mw-provider.active{background:color-mix(in srgb,var(--mw-ok) 10%,var(--mw-card))!important;border-color:color-mix(in srgb,var(--mw-ok) 34%,var(--mw-border))!important}.cw-mw-form-review{gap:14px!important}.cw-mw-form-review .cw-mw-selected{grid-template-columns:72px minmax(0,1fr)!important;min-height:0;padding:14px 16px!important;gap:14px!important}.cw-mw-form-review .cw-mw-selected .cw-mw-poster{width:72px!important;height:104px!important}.cw-mw-form-review .cw-mw-selected-title{font-size:18px!important}.cw-mw-form-review .cw-mw-overview{font-size:13px!important;line-height:1.38!important;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}.cw-mw-form-review .cw-mw-review{gap:8px!important}.cw-mw-form-review .cw-mw-review-row{grid-template-columns:132px minmax(0,1fr)!important;min-height:58px!important;padding:12px 14px!important;border-radius:14px!important}.cw-mw-form-review .cw-mw-review-k{font-size:11px!important}.cw-mw-form-review .cw-mw-review-v{font-size:14px!important}
@media(max-width:860px){.cx-modal-shell.cw-manual-watched-modal{width:min(var(--cxModalMaxW,1180px),calc(100vw - 20px))!important;height:min(var(--cxModalMaxH,84vh),calc(100vh - 20px))!important}.cw-mw .cx-head{padding:14px 16px}.cw-mw-title{font-size:20px}.cw-mw-body{grid-template-columns:1fr}.cw-mw-steps{display:flex;gap:8px;overflow:auto;border-right:0;border-bottom:1px solid var(--mw-border-soft);padding:10px 12px}.cw-mw-step{grid-template-columns:30px minmax(0,1fr);align-items:center;min-width:170px;min-height:52px;padding:8px}.cw-mw-step:not(:last-child)::after{display:none}.cw-mw-step-num{width:30px;height:30px}.cw-mw-step-title{margin:0}.cw-mw-step-sub{display:none}.cw-mw-stage{padding:16px}.cw-mw-actions-grid,.cw-mw-providers,.cw-mw-details-grid{grid-template-columns:1fr}.cw-mw-action,.cw-mw-provider{min-height:72px}.cw-mw-foot{grid-template-columns:1fr}.cw-mw-foot-actions{justify-content:stretch;display:grid;grid-template-columns:repeat(4,minmax(0,1fr))}.cw-mw-btn{padding:0 10px}.cw-mw-search{grid-template-columns:1fr}.cw-mw-result{grid-template-columns:54px minmax(0,1fr) 30px}.cw-mw-poster{width:54px;height:80px}.cw-mw-selected{grid-template-columns:58px minmax(0,1fr)}.cw-mw-selected .cw-mw-poster{width:58px;height:86px}.cw-mw-review-row{grid-template-columns:1fr}}
@media(max-width:980px){.cw-mw-body{grid-template-columns:1fr!important}.cw-mw-steps{display:flex!important;gap:0!important;overflow:auto!important;border-right:0!important;border-bottom:1px solid var(--mw-border)!important}.cw-mw-step{grid-template-columns:42px minmax(0,1fr)!important;min-width:220px!important;min-height:76px!important;padding:14px 18px!important}.cw-mw-step::before{display:none!important}.cw-mw-step-num{width:42px!important;height:42px!important;border-radius:12px!important;font-size:16px!important}.cw-mw-stage{padding:18px!important}.cw-mw-stage-shell{gap:14px!important}.cw-mw-actions-grid,.cw-mw-providers,.cw-mw-details-grid{grid-template-columns:1fr!important}.cw-mw-action{grid-template-columns:46px minmax(0,1fr)!important;grid-template-rows:auto!important;min-height:86px!important;padding:16px!important}.cw-mw-provider{grid-template-columns:48px minmax(0,1fr)!important;min-height:92px!important;padding:16px!important}.cw-mw-provider-icon{width:48px!important;height:48px!important}.cw-mw-search{grid-template-columns:1fr!important}.cw-mw-foot{grid-template-columns:1fr!important;padding:14px!important}.cw-mw-foot-actions{display:grid!important;grid-template-columns:repeat(2,minmax(0,1fr))!important}.cw-mw-foot .cw-mw-btn{width:100%!important;min-width:0!important}}
`;
  document.head.appendChild(el);
}

export default {
  async mount(root) {
    injectCSS();
    const shell = root.closest(".cx-modal-shell");
    shell?.classList.add("cw-manual-watched-modal");
    root.style.setProperty("--cxModalMaxW", "1360px");
    root.style.setProperty("--cxModalMaxH", "88vh");

    const state = {
      step: 0,
      type: "movie",
      providers: [],
      selectedProviders: new Set(),
      remembered: [],
      query: "",
      results: [],
      searching: false,
      selectedItem: null,
      dateMode: "today",
      watchedOn: todayLocal(),
      actions: { history: true, watchlist: false, rating: false },
      rating: 8,
      saving: false,
      status: "",
      statusTone: "",
    };

    const PM = window.CW?.ProviderMeta;
    const logoHtml = (p) => {
      const src = PM?.logLogoPath?.(p) || PM?.logoPath?.(p) || "";
      const label = PM?.label?.(p) || p;
      return src ? `<img class="cw-mw-provider-logo" src="${esc(src)}" alt="">` : `<span>${esc(String(label).slice(0, 2).toUpperCase())}</span>`;
    };
    const loadRemembered = () => {
      try {
        const raw = JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]");
        return Array.isArray(raw) ? raw.map((v) => String(v || "")) : [];
      } catch {
        return [];
      }
    };
    const saveSelection = () => {
      try { localStorage.setItem(STORAGE_KEY, JSON.stringify([...state.selectedProviders])); } catch {}
    };
    const renderPoster = (item) => !item?.tmdb
      ? `<div class="cw-mw-poster"><div class="cw-mw-ghost">No art</div></div>`
      : `<div class="cw-mw-poster"><img src="/art/tmdb/${artType(item.type)}/${encodeURIComponent(String(item.tmdb))}?size=w185" alt=""></div>`;
    const selectedTargets = () => state.providers.filter((item) => state.selectedProviders.has(providerKey(item)));
    const selectedActionKeys = () => ACTIONS.map(([key]) => key).filter((key) => !!state.actions[key]);
    const supportsAction = (item, key) => {
      const action = ACTIONS.find(([k]) => k === key);
      return !!(action && item?.[action[4]]);
    };
    const actionScope = () => selectedTargets().length ? selectedTargets() : state.providers;
    const availableActions = () => ACTIONS.filter(([key]) => actionScope().some((p) => supportsAction(p, key)));
    const providerCompatible = (item) => selectedActionKeys().every((key) => supportsAction(item, key));
    const compatibleProviders = () => state.providers.filter(providerCompatible);
    const selectedCompatibleTargets = () => selectedTargets().filter(providerCompatible);
    const actionLabels = () => selectedActionKeys().map((key) => ACTIONS.find(([k]) => k === key)?.[1] || key);
    const normalizeActions = () => {
      const allowed = new Set(availableActions().map(([key]) => key));
      ACTIONS.forEach(([key]) => { if (!allowed.has(key)) state.actions[key] = false; });
      if (!selectedActionKeys().length && allowed.size) state.actions[[...allowed][0]] = true;
      for (const key of [...state.selectedProviders]) {
        const item = state.providers.find((p) => providerKey(p) === key);
        if (!item || !providerCompatible(item)) state.selectedProviders.delete(key);
      }
      if (!state.actions.rating) state.rating = 8;
    };
    const dateText = () => state.dateMode === "custom" ? (state.watchedOn || "Choose date") : state.dateMode === "release" ? "Release date" : "Today";
    const validation = () => {
      if (!state.selectedItem) return "Select a movie or show first.";
      if (!selectedActionKeys().length) return "Select at least one action.";
      if (!selectedCompatibleTargets().length) return "Select at least one compatible provider.";
      if (state.actions.history && state.dateMode === "custom" && !state.watchedOn) return "Choose a watched date.";
      if (state.actions.rating && !(Number(state.rating) >= 1 && Number(state.rating) <= 10)) return "Choose a rating from 1 to 10.";
      return "";
    };
    const stepReady = (idx) => {
      if (idx <= 0) return true;
      if (!state.selectedItem) return false;
      if (idx <= 1) return true;
      if (!selectedActionKeys().length) return false;
      if (idx <= 2) return true;
      if (!selectedCompatibleTargets().length) return false;
      if (idx <= 3) return true;
      return !validation();
    };
    const maxStep = () => STEPS.findIndex((_, i) => !stepReady(i)) < 0 ? STEPS.length - 1 : Math.max(0, STEPS.findIndex((_, i) => !stepReady(i)) - 1);
    const setStatus = (text = "", tone = "") => {
      state.status = text;
      state.statusTone = tone;
      render();
    };
    const setStep = (idx) => {
      state.step = Math.max(0, Math.min(STEPS.length - 1, idx));
      render();
    };

    const selectedItemHtml = (compact = false) => state.selectedItem ? `
      <div class="cw-mw-selected ${compact ? "is-compact" : ""}">
        ${renderPoster(state.selectedItem)}
        <div>
          <div class="cw-mw-selected-title">
            <span>${esc(state.selectedItem.title || "Untitled")}</span>
            <span class="cw-mw-badge">${esc(mediaLabel(state.selectedItem.type))}</span>
            ${state.selectedItem.year ? `<span class="cw-mw-badge">${esc(state.selectedItem.year)}</span>` : ""}
          </div>
          <div class="cw-mw-overview">${esc(state.selectedItem.overview || "No overview available.")}</div>
        </div>
      </div>
    ` : `<div class="cw-mw-empty">No item selected yet.</div>`;

    const searchStep = () => `
      <div class="cw-mw-form">
        <div><div class="cw-mw-headline"><span>1.</span><b>Search, find and select a movie or show</b></div><div class="cw-mw-copy">Search TMDb and pick the exact item you want to add.</div></div>
        <div class="cw-mw-media-toggle">
          <button type="button" class="cw-mw-chip ${state.type === "movie" ? "active" : ""}" data-type="movie">Movies</button>
          <button type="button" class="cw-mw-chip ${state.type === "show" ? "active" : ""}" data-type="show">Shows</button>
        </div>
        <div class="cw-mw-search">
          <input class="cw-mw-input" data-role="query" placeholder="Search title..." autocomplete="off" value="${esc(state.query)}">
          <button type="button" class="cw-mw-btn primary" data-role="search"><span class="material-symbols-rounded">search</span><span>Search</span></button>
        </div>
        <div class="cw-mw-results">${resultsHtml()}</div>
      </div>
    `;

    const resultsHtml = () => {
      if (state.searching) return `<div class="cw-mw-empty">Searching TMDb...</div>`;
      if (!state.query.trim()) return `<div class="cw-mw-empty">Search TMDb for a movie or show.</div>`;
      if (!state.results.length) return `<div class="cw-mw-empty">No results found.</div>`;
      return state.results.map((item) => {
        const active = state.selectedItem && String(state.selectedItem.tmdb) === String(item.tmdb) && state.selectedItem.type === item.type;
        return `<button type="button" class="cw-mw-result ${active ? "active" : ""}" data-result-tmdb="${esc(item.tmdb)}">
          ${renderPoster(item)}
          <div>
            <div class="cw-mw-result-title"><span>${esc(item.title || "Untitled")}</span><span class="cw-mw-badge">${esc(mediaLabel(item.type))}</span></div>
            <div class="cw-mw-meta">${item.year ? esc(item.year) : "Unknown year"}</div>
            <div class="cw-mw-overview">${esc(item.overview || "No overview available.")}</div>
          </div>
          <span class="cw-mw-radio"><span class="material-symbols-rounded">check</span></span>
        </button>`;
      }).join("");
    };

    const actionsStep = () => {
      normalizeActions();
      const rows = availableActions().map(([key, label, icon, note]) => `
        <button type="button" class="cw-mw-action ${state.actions[key] ? "active" : ""}" data-action="${esc(key)}" aria-pressed="${state.actions[key] ? "true" : "false"}">
          <span class="cw-mw-action-icon"><span class="material-symbols-rounded">${esc(icon)}</span></span>
          <span class="cw-mw-action-copy"><span class="cw-mw-action-title">${esc(label)}</span><span class="cw-mw-muted">${esc(note)}</span></span>
        </button>
      `).join("");
      return `<div class="cw-mw-form">
        <div><div class="cw-mw-headline"><span>2.</span><b>Actions</b></div><div class="cw-mw-copy">Select History, Watchlist, Rating, or a valid combination.</div></div>
        ${selectedItemHtml()}
        <div class="cw-mw-actions-grid">${rows || `<div class="cw-mw-empty">No configured provider supports Quick Add actions.</div>`}</div>
      </div>`;
    };

    const providersStep = () => {
      normalizeActions();
      const providers = compatibleProviders();
      const direct = providers.filter((item) => !MEDIA_SERVER_PROVIDERS.has(String(item.provider || "").toUpperCase()));
      const servers = providers.filter((item) => MEDIA_SERVER_PROVIDERS.has(String(item.provider || "").toUpperCase()));
      const rows = [...direct, ...servers].map(providerRow).join("");
      return `<div class="cw-mw-form">
        <div><div class="cw-mw-headline"><span>3.</span><b>Providers</b></div><div class="cw-mw-copy">Select configured providers compatible with ${esc(actionLabels().join(", ") || "your actions")}.</div></div>
        <div class="cw-mw-tools">
          <div class="cw-mw-muted">${selectedCompatibleTargets().length} selected</div>
          <div class="cw-mw-row">
            <button type="button" class="cw-mw-link" data-role="use-last">Use last</button>
            <button type="button" class="cw-mw-link" data-role="select-all">Select all</button>
            <button type="button" class="cw-mw-link" data-role="clear-providers">Clear</button>
          </div>
        </div>
        <div class="cw-mw-providers">${rows || `<div class="cw-mw-empty">No configured provider supports this action combination.</div>`}</div>
      </div>`;
    };

    const providerRow = (item) => {
      const key = providerKey(item);
      const active = state.selectedProviders.has(key);
      const logo = PM?.logLogoPath?.(item.provider) || PM?.logoPath?.(item.provider) || "";
      const badges = ACTIONS.filter(([action]) => supportsAction(item, action)).map(([, label]) => `<span class="cw-mw-badge">${esc(label)}</span>`).join("");
      return `<button type="button" class="cw-mw-provider ${active ? "active" : ""}" data-provider-key="${esc(key)}" aria-pressed="${active ? "true" : "false"}" ${logo ? `style="--mw-provider-wm:url(&quot;${esc(logo)}&quot;)"` : ""}>
        <span class="cw-mw-provider-icon">${logoHtml(item.provider)}</span>
        <span class="cw-mw-provider-copy">
          <span class="cw-mw-provider-title">${esc(item.display || item.label || item.provider)}</span>
          <span class="cw-mw-provider-badges">${badges}</span>
        </span>
      </button>`;
    };

    const detailsStep = () => `
      <div class="cw-mw-form">
        <div><div class="cw-mw-headline"><span>4.</span><b>Details</b></div><div class="cw-mw-copy">Configure watched date and rating only when relevant.</div></div>
        ${selectedItemHtml()}
        <div class="cw-mw-details-grid">
          ${state.actions.history ? `<div class="cw-mw-card">
            <label class="cw-mw-label">Watched date</label>
            <div class="cw-mw-row">
              <button type="button" class="cw-mw-chip ${state.dateMode === "today" ? "active" : ""}" data-date-mode="today">Today</button>
              <button type="button" class="cw-mw-chip ${state.dateMode === "release" ? "active" : ""}" data-date-mode="release">Release date</button>
              <button type="button" class="cw-mw-chip ${state.dateMode === "custom" ? "active" : ""}" data-date-mode="custom">Choose date</button>
            </div>
            ${state.dateMode === "custom" ? `<div style="margin-top:12px"><input type="date" class="cw-mw-date" data-role="custom-date" value="${esc(state.watchedOn || "")}"></div>` : ""}
          </div>` : ""}
          ${state.actions.rating ? `<div class="cw-mw-card">
            <label class="cw-mw-label">Rating</label>
            <div class="cw-mw-row" style="justify-content:space-between;margin-bottom:12px"><span class="cw-mw-muted">Score from 1 to 10.</span><span class="cw-mw-rating-value">${esc(state.rating)}</span></div>
            <input type="range" min="1" max="10" step="1" class="cw-mw-slider" data-role="rating" value="${esc(state.rating)}" style="--rating-progress:${(Number(state.rating || 1) / 10) * 100}%">
          </div>` : ""}
        </div>
        ${!state.actions.history && !state.actions.rating ? `<div class="cw-mw-empty">No extra details are needed for Watchlist only.</div>` : ""}
      </div>
    `;

    const reviewStep = () => `
      <div class="cw-mw-form cw-mw-form-review">
        <div><div class="cw-mw-headline"><span>5.</span><b>Review</b></div><div class="cw-mw-copy">Check the final item, actions, providers, dates, and rating before sending.</div></div>
        ${selectedItemHtml(true)}
        <div class="cw-mw-review">
          <div class="cw-mw-review-row"><div class="cw-mw-review-k">Actions</div><div class="cw-mw-review-v">${actionLabels().map((x) => `<span class="cw-mw-badge">${esc(x)}</span>`).join("")}</div></div>
          <div class="cw-mw-review-row"><div class="cw-mw-review-k">Providers</div><div class="cw-mw-review-v">${selectedCompatibleTargets().map((p) => `<span class="cw-mw-badge">${esc(p.display || p.label || p.provider)}</span>`).join("")}</div></div>
          <div class="cw-mw-review-row"><div class="cw-mw-review-k">Dates</div><div class="cw-mw-review-v">${state.actions.history ? esc(dateText()) : "Not needed"}</div></div>
          <div class="cw-mw-review-row"><div class="cw-mw-review-k">Rating</div><div class="cw-mw-review-v">${state.actions.rating ? esc(`${state.rating}/10`) : "Not included"}</div></div>
        </div>
      </div>
    `;

    const stepContent = () => [searchStep, actionsStep, providersStep, detailsStep, reviewStep][state.step]();

    const render = (focusQuery = false) => {
      normalizeActions();
      const allowed = maxStep();
      if (state.step > allowed) state.step = allowed;
      const err = validation();
      root.innerHTML = `<div class="cw-mw">
        <div class="cx-head">
          <div class="cw-mw-head">
            <span class="cw-mw-head-icon"><span class="material-symbols-rounded">add_circle</span></span>
            <div><div class="cw-mw-title">Quick Add Item</div><div class="cw-mw-sub">Add a movie or show to your providers in a few simple steps.</div></div>
          </div>
          <button type="button" class="cw-mw-close" data-role="close" aria-label="Close"><span class="material-symbols-rounded">close</span></button>
        </div>
        <div class="cw-mw-body">
          <nav class="cw-mw-steps" aria-label="Quick Add steps">
            ${STEPS.map(([title, sub], i) => `<button type="button" class="cw-mw-step ${i === state.step ? "active" : ""} ${i < state.step && stepReady(i + 1) ? "done" : ""}" data-step="${i}" ${i > allowed ? "disabled" : ""}>
              <span class="cw-mw-step-num">${i + 1}</span><span><span class="cw-mw-step-title">${esc(title)}</span><span class="cw-mw-step-sub">${esc(sub)}</span></span>
            </button>`).join("")}
          </nav>
          <main class="cw-mw-main"><section class="cw-mw-stage"><div class="cw-mw-stage-shell">${stepContent()}</div></section></main>
        </div>
        <footer class="cw-mw-foot">
          <div class="cw-mw-status ${state.statusTone || (err ? "error" : "")}">${esc(state.status || (state.step === 4 ? err : ""))}</div>
          <div class="cw-mw-foot-actions">
            <button type="button" class="cw-mw-btn" data-role="cancel">Cancel</button>
            <button type="button" class="cw-mw-btn" data-role="prev" ${state.step === 0 || state.saving ? "disabled" : ""}>Previous</button>
            <button type="button" class="cw-mw-btn primary" data-role="next" ${state.step >= 4 || !stepReady(state.step + 1) || state.saving ? "disabled" : ""}>Next<span class="material-symbols-rounded">chevron_right</span></button>
            <button type="button" class="cw-mw-btn primary" data-role="send" ${state.step !== 4 || !!err || state.saving ? "disabled" : ""}>${state.saving ? "Sending..." : "Send"}</button>
          </div>
        </footer>
      </div>`;
      if (focusQuery) {
        const q = root.querySelector("[data-role=query]");
        q?.focus?.();
        try { q?.setSelectionRange?.(q.value.length, q.value.length); } catch {}
      }
    };

    const search = async () => {
      const q = state.query.trim();
      if (q.length < 2) {
        state.results = [];
        state.searching = false;
        render(true);
        return;
      }
      state.searching = true;
      render(true);
      try {
        const data = await fjson(`/api/metadata/search?q=${encodeURIComponent(q)}&typ=${encodeURIComponent(state.type)}&limit=12`);
        if (data?.ok === false) throw new Error(data.error || "Search failed");
        state.results = Array.isArray(data.results) ? data.results : [];
        state.status = "";
        state.statusTone = "";
      } catch (err) {
        state.results = [];
        state.status = String(err?.message || "Search failed");
        state.statusTone = "error";
      } finally {
        state.searching = false;
        render(true);
      }
    };

    let searchTimer = 0;
    const queueSearch = () => {
      window.clearTimeout(searchTimer);
      searchTimer = window.setTimeout(search, 260);
    };

    const loadProviders = async () => {
      try {
        const data = await fjson("/api/manual/providers");
        state.providers = Array.isArray(data.providers) ? data.providers : [];
        state.remembered = loadRemembered();
        const allowed = new Set(state.providers.map(providerKey));
        state.selectedProviders = new Set(state.remembered.filter((key) => allowed.has(key)));
        normalizeActions();
      } catch (err) {
        state.providers = [];
        state.selectedProviders = new Set();
        state.status = String(err?.message || "Failed to load providers");
        state.statusTone = "error";
      }
      render();
    };

    const submit = async () => {
      const err = validation();
      if (err || state.saving) return setStatus(err, "error");
      state.saving = true;
      render();
      const providers = selectedCompatibleTargets().map((item) => ({ provider: item.provider, instance: item.instance }));
      try {
        const data = await fjson("/api/manual/watched", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            item: state.selectedItem,
            date_mode: state.actions.history ? state.dateMode : "today",
            watched_on: state.actions.history && state.dateMode === "custom" ? state.watchedOn : null,
            actions: {
              history: !!state.actions.history,
              watchlist: !!state.actions.watchlist,
              rating: !!state.actions.rating,
            },
            rating: state.actions.rating ? state.rating : null,
            providers,
          }),
        });
        saveSelection();
        state.saving = false;
        setStatus(`Saved to ${providers.length} provider${providers.length === 1 ? "" : "s"}.`, "success");
        window.dispatchEvent(new CustomEvent("cw:manual-watched-saved", { detail: data }));
        window.setTimeout(closeModal, 520);
      } catch (err2) {
        state.saving = false;
        setStatus(String(err2?.message || "Save failed"), "error");
      }
    };

    root.addEventListener("input", (e) => {
      if (e.target.matches("[data-role=query]")) {
        state.query = e.target.value || "";
        queueSearch();
      } else if (e.target.matches("[data-role=custom-date]")) {
        state.watchedOn = e.target.value || "";
        render();
      } else if (e.target.matches("[data-role=rating]")) {
        state.rating = Math.max(1, Math.min(10, Number(e.target.value || 8)));
        render();
      }
    });

    root.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && e.target.matches("[data-role=query]")) {
        e.preventDefault();
        search();
      }
    });

    root.addEventListener("click", (e) => {
      const target = e.target;
      if (target.closest("[data-role=close],[data-role=cancel]")) return closeModal();
      if (target.closest("[data-role=prev]")) return setStep(state.step - 1);
      if (target.closest("[data-role=next]")) return stepReady(state.step + 1) && setStep(state.step + 1);
      if (target.closest("[data-role=send]")) return submit();
      if (target.closest("[data-role=search]")) return search();
      const stepBtn = target.closest("[data-step]");
      if (stepBtn) {
        const idx = Number(stepBtn.getAttribute("data-step") || 0);
        if (!stepBtn.disabled) setStep(idx);
        return;
      }
      const typeBtn = target.closest("[data-type]");
      if (typeBtn) {
        state.type = typeBtn.getAttribute("data-type") || "movie";
        state.selectedItem = null;
        state.results = [];
        if (state.query.trim().length >= 2) queueSearch();
        render(true);
        return;
      }
      const resultBtn = target.closest("[data-result-tmdb]");
      if (resultBtn) {
        const tmdb = String(resultBtn.getAttribute("data-result-tmdb") || "");
        state.selectedItem = state.results.find((item) => String(item.tmdb) === tmdb) || null;
        if (state.selectedItem) state.step = 1;
        render();
        return;
      }
      const actionBtn = target.closest("[data-action]");
      if (actionBtn) {
        const key = actionBtn.getAttribute("data-action") || "";
        state.actions[key] = !state.actions[key];
        if (key === "rating" && state.actions[key] && !state.rating) state.rating = 8;
        normalizeActions();
        render();
        return;
      }
      const modeBtn = target.closest("[data-date-mode]");
      if (modeBtn) {
        state.dateMode = modeBtn.getAttribute("data-date-mode") || "today";
        render();
        return;
      }
      const providerBtn = target.closest("[data-provider-key]");
      if (providerBtn) {
        const key = providerBtn.getAttribute("data-provider-key") || "";
        if (state.selectedProviders.has(key)) state.selectedProviders.delete(key);
        else state.selectedProviders.add(key);
        normalizeActions();
        render();
        return;
      }
      if (target.closest("[data-role=use-last]")) {
        const allowed = new Set(compatibleProviders().map(providerKey));
        state.selectedProviders = new Set(state.remembered.filter((key) => allowed.has(key)));
        render();
        return;
      }
      if (target.closest("[data-role=select-all]")) {
        state.selectedProviders = new Set(compatibleProviders().map(providerKey));
        render();
        return;
      }
      if (target.closest("[data-role=clear-providers]")) {
        state.selectedProviders = new Set();
        render();
      }
    });

    render();
    await loadProviders();
  },
  unmount() {},
};
