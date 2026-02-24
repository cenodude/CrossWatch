/* snapshots.js - Provider snapshots (watchlist/ratings/history) */
/* CrossWatch - Snapshots page UI logic */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {

  
  const css = `
  #page-snapshots .ss-top{display:flex;align-items:flex-end;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-bottom:10px}
  #page-snapshots .ss-title{font-weight:900;font-size:22px;letter-spacing:.01em}
  #page-snapshots .ss-sub{opacity:.72;font-size:13px;margin-top:4px;line-height:1.3}
  #page-snapshots .ss-wrap{display:grid;grid-template-columns:420px minmax(0,1fr) 380px;gap:16px;align-items:start}
  #page-snapshots .ss-col{display:flex;flex-direction:column;gap:14px}
  #page-snapshots .ss-card{
    background:linear-gradient(180deg,rgba(255,255,255,.02),transparent),var(--panel);
    border:1px solid rgba(255,255,255,.08);
    border-radius:22px;
    padding:16px;
    box-shadow:0 0 40px rgba(0,0,0,.25) inset;
  }
  #page-snapshots .ss-card h3{margin:0 0 12px 0;font-size:13px;letter-spacing:.10em;text-transform:uppercase;opacity:.85}
  #page-snapshots .ss-coll-head{display:flex;align-items:center;gap:10px;cursor:pointer}
  #page-snapshots .ss-coll-head h3{margin:0}
  #page-snapshots .ss-coll-head:focus{outline:2px solid rgba(255,255,255,.18);outline-offset:4px;border-radius:14px}
  #page-snapshots .ss-coll-ico{margin-left:auto;opacity:.7;transition:transform .12s ease}
  #page-snapshots .ss-card.is-collapsed .ss-coll-ico{transform:rotate(-90deg)}
  #page-snapshots .ss-coll-body{margin-top:12px}

  #page-snapshots .ss-card.ss-accent{
    border-color:rgba(111,108,255,.22);
    box-shadow:0 0 46px rgba(111,108,255,.10), 0 0 40px rgba(0,0,0,.25) inset;
  }
  #page-snapshots .ss-row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
  #page-snapshots .ss-row > *{flex:0 0 auto}
  #page-snapshots .ss-row .grow{flex:1 1 auto;min-width:180px}
  #page-snapshots .ss-note{font-size:12px;opacity:.72;line-height:1.35}
  #page-snapshots .ss-progress{display:flex;align-items:center;gap:10px;margin-top:12px}
  #page-snapshots .ss-progress.hidden{display:none}
  #page-snapshots .ss-pbar{position:relative;flex:1 1 auto;height:8px;border-radius:999px;background:rgba(255,255,255,.08);overflow:hidden}
  #page-snapshots .ss-pbar::before{content:"";position:absolute;inset:0;width:40%;transform:translateX(-60%);background:linear-gradient(90deg,transparent,var(--pcol,var(--accent)),transparent);animation:ssprog 1.05s ease-in-out infinite}
  @keyframes ssprog{0%{transform:translateX(-60%)}100%{transform:translateX(220%)}}
  #page-snapshots .ss-plabel{flex:0 0 auto;font-size:12px;opacity:.72;white-space:nowrap}

  #page-snapshots #ss-refresh.iconbtn{width:36px;height:36px;padding:0;display:inline-flex;align-items:center;justify-content:center}
  #page-snapshots #ss-refresh-icon{font-size:20px;line-height:1}

  #page-snapshots .ss-muted{opacity:.72}
  #page-snapshots .ss-small{font-size:12px}
  #page-snapshots .ss-hr{height:1px;background:rgba(255,255,255,.08);margin:12px 0}
  #page-snapshots .ss-grid2{display:grid;grid-template-columns:1fr 1fr;gap:10px}

  #page-snapshots .ss-pill{display:inline-flex;align-items:center;gap:6px;border-radius:999px;padding:6px 10px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.10);font-size:12px}
  #page-snapshots .ss-pill strong{font-weight:900}

  #page-snapshots .ss-list{display:flex;flex-direction:column;gap:10px;max-height:520px;overflow:auto;padding:3px 4px 3px 0}
  #page-snapshots .ss-item{
    display:flex;gap:10px;align-items:center;cursor:pointer;
    padding:12px 12px;border-radius:18px;
    border:1px solid rgba(255,255,255,.08);
    background:rgba(0,0,0,.10);
    transition:transform .08s ease,border-color .12s ease,background .12s ease;
  }
  #page-snapshots .ss-item:hover{transform:translateY(-1px);border-color:rgba(255,255,255,.16);background:rgba(255,255,255,.03)}
  #page-snapshots .ss-item.active{border-color:rgba(111,108,255,.55);box-shadow:inset 0 0 0 2px rgba(111,108,255,.24)}
  #page-snapshots .ss-item .meta{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
  #page-snapshots .ss-badge{font-size:11px;letter-spacing:.05em;text-transform:uppercase;padding:2px 10px;border-radius:999px;border:1px solid rgba(255,255,255,.12);opacity:.9}
  #page-snapshots .ss-badge.ok{border-color:rgba(48,255,138,.35)}
  #page-snapshots .ss-badge.warn{border-color:rgba(255,180,80,.35)}
  #page-snapshots .ss-item .d{opacity:.72;font-size:12px;margin-top:4px}
  #page-snapshots .ss-item .chev{opacity:.55;font-size:22px;line-height:1}

  #page-snapshots .ss-empty{padding:18px;border-radius:18px;border:1px dashed rgba(255,255,255,.14);text-align:center;opacity:.75}

  #page-snapshots .ss-actions{display:flex;gap:10px;flex-wrap:wrap}
  #page-snapshots .ss-actions .btn{display:inline-flex;align-items:center;gap:8px}


  #page-snapshots button:disabled{opacity:.42;cursor:not-allowed;filter:saturate(.5)}
  #page-snapshots .ss-status{display:flex;align-items:center;gap:10px;padding:10px 12px;border-radius:18px;border:1px solid rgba(255,255,255,.10);background:rgba(0,0,0,.12);margin:10px 0}
  #page-snapshots .ss-status.hidden{display:none}
  #page-snapshots .ss-status .msg{flex:1 1 auto;min-width:0;opacity:.9;font-size:12px}
  #page-snapshots .ss-status .chip{font-size:11px;letter-spacing:.04em;text-transform:uppercase;padding:2px 10px;border-radius:999px;border:1px solid rgba(255,255,255,.14);opacity:.9}
  #page-snapshots .ss-status .chip.ok{border-color:rgba(48,255,138,.35)}
  #page-snapshots .ss-status .chip.err{border-color:rgba(255,80,80,.35)}
  #page-snapshots .ss-statusbar{position:relative;flex:0 0 170px;height:8px;border-radius:999px;overflow:hidden;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.04)}
  #page-snapshots .ss-statusfill{position:absolute;inset:0;width:35%;border-radius:999px;background:rgba(111,108,255,.55);animation:ssmove 1.1s linear infinite}
  @keyframes ssmove{0%{transform:translateX(-120%)}100%{transform:translateX(320%)}}

    #page-snapshots .ss-refresh-icon.ss-spin{animation:ssrot .8s linear infinite}
  @keyframes ssrot{0%{transform:rotate(0deg)}100%{transform:rotate(360deg)}}
  #page-snapshots .ss-field{display:flex;align-items:center;gap:10px;padding:10px 12px;border-radius:14px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.03)}
  #page-snapshots .ss-field .material-symbol{opacity:.85}
  #page-snapshots .ss-field select,#page-snapshots .ss-field input{flex:1 1 auto;min-width:0;background:transparent;border:0;outline:0;color:inherit;font:inherit}
  #page-snapshots .ss-field select{appearance:none;color-scheme:dark}
#page-snapshots .ss-native{display:none !important}
#page-snapshots .ss-bsel{position:relative;flex:1 1 auto;min-width:0}
#page-snapshots .ss-bsel-btn{width:100%;display:flex;align-items:center;gap:10px;background:transparent;border:0;outline:0;color:inherit;font:inherit;cursor:pointer;padding:0;text-align:left}
#page-snapshots .ss-bsel-label{flex:1 1 auto;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;text-align:left}
#page-snapshots .ss-bsel-chev{opacity:.6;flex:0 0 auto}
#page-snapshots .ss-bsel-menu{position:absolute;left:-12px;right:-12px;top:calc(100% + 10px);z-index:50;border:1px solid rgba(255,255,255,.10);border-radius:16px;background:linear-gradient(180deg,rgba(255,255,255,.03),transparent),#0b0b16;box-shadow:0 14px 40px rgba(0,0,0,.55);padding:6px;max-height:320px;overflow:auto}
#page-snapshots .ss-bsel-menu.hidden{display:none}
#page-snapshots .ss-bsel-item{width:100%;display:flex;align-items:center;gap:10px;padding:10px 10px;border-radius:12px;border:1px solid transparent;background:transparent;color:inherit;cursor:pointer;text-align:left}
#page-snapshots .ss-bsel-item:hover{background:rgba(255,255,255,.04);border-color:rgba(255,255,255,.10)}
#page-snapshots .ss-bsel-item:disabled{opacity:.45;cursor:not-allowed}
#page-snapshots .ss-provico{width:18px;height:18px;flex:0 0 18px;border-radius:7px;border:1px solid rgba(255,255,255,.16);background:rgba(0,0,0,.18);background-image:var(--wm);background-repeat:no-repeat;background-position:center;background-size:contain;filter:grayscale(.05) brightness(1.12);opacity:.95}
#page-snapshots .ss-bsel-menu .ss-provico{width:20px;height:20px;flex-basis:20px}
#page-snapshots .ss-provico.empty{background-image:none;background:rgba(255,255,255,.05)}

#page-snapshots .ss-field select option{background:#141418;color:#f3f3f5}
#page-snapshots .ss-field select option:disabled{color:#7b7b86}
#page-snapshots select{color-scheme:dark}

  #page-snapshots .ss-field .chev{opacity:.6}

#page-snapshots .ss-difflist{display:flex;flex-direction:column;gap:10px;max-height:320px;overflow:auto;padding:3px 4px 3px 0}
#page-snapshots .ss-diffitem{padding:12px;border-radius:18px;border:1px solid rgba(255,255,255,0.08);background:rgba(0,0,0,0.10)}
#page-snapshots .ss-diffhead{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
#page-snapshots .ss-diffkey{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace;font-size:11px;opacity:.72;margin-top:6px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#page-snapshots .ss-code{white-space:pre-wrap;word-break:break-word;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace;font-size:11px;line-height:1.35;padding:10px;border-radius:14px;border:1px solid rgba(255,255,255,0.08);background:rgba(0,0,0,0.20);margin-top:8px}
#page-snapshots .ss-badge.add{border-color:rgba(48,255,138,0.35)}
#page-snapshots .ss-badge.del{border-color:rgba(255,80,80,0.35)}
#page-snapshots .ss-badge.upd{border-color:rgba(255,180,80,0.35)}

  @media (max-width: 1200px){
    #page-snapshots .ss-wrap{grid-template-columns:1fr;gap:14px}
    #page-snapshots .ss-list{max-height:420px}
  }
  
  /* Compare (captures) */
  #page-snapshots .ss-right{display:flex;align-items:center;gap:10px}
  #page-snapshots .ss-chk{width:18px;height:18px;accent-color:#6f6cff}
  #page-snapshots .ss-comparehint{display:flex;align-items:center;gap:8px;font-size:12px;opacity:.78;margin:10px 0 12px 0}
  #page-snapshots .ss-comparehint .material-symbol{font-size:18px;opacity:.9}
  #page-snapshots .ss-ab{display:inline-flex;align-items:center;justify-content:center;min-width:20px;height:20px;border-radius:999px;border:1px solid rgba(255,255,255,.14);font-size:11px;font-weight:900;letter-spacing:.03em;opacity:.92}
  #page-snapshots .ss-ab.a{border-color:rgba(111,108,255,.42)}
  #page-snapshots .ss-ab.b{border-color:rgba(255,180,80,.38)}
  #page-snapshots .ss-picked{display:grid;grid-template-columns:1fr 1fr;gap:10px}
  #page-snapshots .ss-pick-card{padding:12px;border-radius:18px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.08);min-width:0;cursor:grab;user-select:none}
  #page-snapshots .ss-pick-card:active{cursor:grabbing}
  #page-snapshots .ss-pick-date{font-weight:900;font-size:18px}
  #page-snapshots .ss-pick-meta{margin-top:6px;font-size:12px;opacity:.82}
  #page-snapshots .ss-pick-card.dragging{opacity:.65}
  #page-snapshots [data-coll-body="compare"]{overflow-x:hidden}
  #page-snapshots .ss-diffrow{margin-top:10px;padding:12px;border-radius:18px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.08)}
  #page-snapshots .ss-diffhead{display:flex;align-items:center;gap:10px}
  #page-snapshots .ss-difftitle{flex:1 1 auto;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:700}
  @media (max-width: 520px){#page-snapshots .ss-picked{grid-template-columns:1fr}}

  #page-snapshots .ss-diff-summary{display:flex;flex-wrap:nowrap;gap:8px;align-items:center}
  #page-snapshots .ss-diff-summary .ss-pill{justify-content:center;padding:5px 8px;font-size:11px;gap:5px;flex:0 1 auto;white-space:nowrap}
  #page-snapshots .ss-diff-summary .lbl{white-space:nowrap}
`;

  function injectCss() {
    if (document.getElementById("cw-snapshots-css")) return;
    const s = document.createElement("style");
    s.id = "cw-snapshots-css";
    s.textContent = css;
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

  const state = {
    providers: [],
    snapshots: [],
    selectedPath: "",
    selectedSnap: null,
    diffAPath: "",
    diffBPath: "",
    diffResult: null,
    diffKind: "all",
    diffQ: "",
    diffLimit: 200,
    diffExpanded: {},
    busy: false,
    lastRefresh: 0,
    statusHideTimer: null,
    listLimit: 5,
    showAll: false,
    expandedBundles: {},
    _spinUntil: 0,
  };

  function _provBrand(pid) {
    const v = String(pid || "").trim().toLowerCase().replace(/[^a-z0-9_-]/g, "");
    return v ? ("brand-" + v) : "";
  }

  function _closeAllBrandMenus(exceptMenu) {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    $$(".ss-bsel-menu", page).forEach((m) => {
      if (exceptMenu && m === exceptMenu) return;
      m.classList.add("hidden");
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
        const isHidden = menu.classList.contains("hidden");
        _closeAllBrandMenus(menu);
        if (isHidden) menu.classList.remove("hidden"); else menu.classList.add("hidden");
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
        menu.classList.add("hidden");
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

function _snapEpoch(s) {
  if (!s) return 0;
  if (s.stamp) return _stampEpoch(s.stamp);
  if (s.mtime) return Number(s.mtime) * 1000;
  return 0;
}

function _diffScope() {
  const picks = Array.isArray(state.diffPick) ? state.diffPick.filter(Boolean) : [];
  if (!picks.length) return null;
  const s0 = _findSnapByPath(String(picks[0] || ""));
  if (!s0) return null;
  return {
    provider: String(s0.provider || "").toLowerCase(),
    feature: String(s0.feature || "").toLowerCase(),
  };
}

function _snapMatchesScope(s, scope) {
  if (!s || !scope) return true;
  const p = String(s.provider || "").toLowerCase();
  const f = String(s.feature || "").toLowerCase();
  return p === scope.provider && f === scope.feature;
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
  state.diffManualOrder = false;
  state.diffResult = null;
  state.diffAPath = "";
  state.diffBPath = "";
  try { renderList(); } catch {}
  try { renderDiffPicked(); } catch {}
  try { renderDiff(); } catch {}
  try { updateDiffAvailability(); } catch {}
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
    state.diffManualOrder = false;
  } else {
    const ix = picks.indexOf(p);
    if (ix !== -1) picks.splice(ix, 1);
    if (picks.length < 2) state.diffManualOrder = false;
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

  
  function render() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    page.innerHTML = `
      <div class="ss-top">
        <div>
          <div class="ss-title">Captures</div>
          <div class="ss-sub">Capture and restore provider state (watchlist / ratings / history). <span class="ss-muted">Point-in-time API export - not a full backup.</span> Stored under <span class="ss-muted">/config/snapshots</span>.</div>
        </div>
        <div class="ss-actions">
          <button id="ss-refresh" class="iconbtn" title="Refresh" aria-label="Refresh"><span id="ss-refresh-icon" class="material-symbol ss-refresh-icon">sync</span></button>
        </div>
      </div>

      <div class="ss-wrap">
        <div class="ss-card ss-accent">
          <h3>Create capture</h3>

          <div class="ss-field">
            <select id="ss-prov"></select>
          </div>

          <div class="ss-field" style="margin-top:10px">
            <select id="ss-prov-inst" class="input grow"></select>
          </div>

          <div class="ss-field" style="margin-top:10px">
            <select id="ss-feature"></select>
            <span class="chev">v</span>
          </div>

          <div class="ss-field" style="margin-top:10px">
            <input id="ss-label" placeholder="Add label..." />
          </div>

          <div class="ss-row" style="margin-top:12px">
            <button id="ss-create" class="btn primary" style="width:100%">Create Capture</button>
          </div>
          <div id="ss-create-progress" class="ss-progress hidden">
            <div class="ss-pbar"></div>
            <div class="ss-plabel">Working…</div>
          </div>
        </div>

        <div class="ss-card">
          <h3>Captures</h3>
          <div class="ss-row">
            <input id="ss-filter" class="input grow" placeholder="Filter captures..."/>
          </div>
          <div class="ss-row" style="margin-top:10px">
            <select id="ss-filter-provider" class="input grow"></select>
            <select id="ss-filter-feature" class="input grow"></select>
          </div>
          <div class="ss-hr"></div>
          <div class="ss-comparehint"><span class="material-symbol">compare_arrows</span><div><b>Compare</b>: tick two boxes on the right (same provider and feature) or <b>click on a capture to restore</b>.</span></div></div>
          <div id="ss-list" class="ss-list"></div>
          <div id="ss-list-footer" class="ss-row" style="justify-content:space-between;margin-top:10px"></div>
        </div>

        <div class="ss-col">
          <div class="ss-card ss-coll" data-coll="restore">
  <div class="ss-coll-head" data-coll-head="restore" role="button" tabindex="0" aria-expanded="true">
    <h3>Restore capture</h3>
    <span class="material-symbol ss-coll-ico">expand_more</span>
  </div>
  <div class="ss-coll-body" data-coll-body="restore">
<div id="ss-selected" class="ss-muted ss-small">Pick a capture from the list.</div>
            <div class="ss-hr"></div>
            <div class="ss-note">
              <b>Merge</b> adds missing items only. <b>Clear and restore</b> wipes the provider feature first, then restores exactly the capture.
            </div>
            <div class="ss-row" style="margin-top:12px">
              <select id="ss-restore-inst" class="input grow"></select>
            </div>
            <div class="ss-row" style="margin-top:12px">
              <select id="ss-restore-mode" class="input grow">
                <option value="merge">Merge</option>
                <option value="clear_restore">Clear and restore</option>
              </select>
            </div>
            <div class="ss-row" style="margin-top:10px">
              <button id="ss-restore" class="btn danger" style="width:100%">Restore</button>
              <button id="ss-delete" class="btn" style="width:100%">Delete</button>
            </div>
            <div id="ss-restore-progress" class="ss-progress hidden">
              <div class="ss-pbar"></div>
              <div class="ss-plabel">Working…</div>
            </div>
            <div id="ss-restore-out" class="ss-small ss-muted" style="margin-top:10px"></div>
  </div>
</div>


<div class="ss-card ss-coll is-collapsed" data-coll="compare">
  <div class="ss-coll-head" data-coll-head="compare" role="button" tabindex="0" aria-expanded="false">
    <h3>Compare captures</h3>
    <span class="material-symbol ss-coll-ico">expand_more</span>
  </div>
  <div class="ss-coll-body hidden" data-coll-body="compare">
  <div class="ss-note">
    Select two captures and compare what changed: <b>Added</b>, <b>Deleted</b>, and <b>Updated</b> (with old/new values).
  </div>
  <div id="ss-diff-picked" class="ss-picked" style="margin-top:12px"></div>
<div class="ss-row" style="margin-top:10px">
    <select id="ss-diff-kind" class="input grow">
      <option value="all">All changes</option>
      <option value="added">Added</option>
      <option value="removed">Deleted</option>
      <option value="updated">Updated</option>
    </select>
    <select id="ss-diff-limit" class="input" style="min-width:110px">
      <option value="100">100</option>
      <option value="200" selected>200</option>
      <option value="500">500</option>
      <option value="1000">1000</option>
    </select>
  </div>
  <div class="ss-row" style="margin-top:10px">
    <input id="ss-diff-q" class="input grow" placeholder="Filter results..."/>
  </div>
  <div class="ss-row" style="margin-top:10px">
    <button id="ss-diff-run" class="btn grow">Compare</button>
    <button id="ss-diff-extend" class="btn grow">Advanced</button>
  </div>
  <div class="ss-small ss-muted" style="margin-top:8px">Advanced opens a full diff modal (includes unchanged records).</div>
  <div id="ss-diff-progress" class="ss-progress hidden">
    <div class="ss-pbar"></div>
    <div class="ss-plabel">Working…</div>
  </div>
  <div id="ss-diff-out" class="ss-muted ss-small" style="margin-top:10px"></div>
  <div id="ss-diff-list" class="ss-difflist" style="margin-top:10px"></div>

  </div>
</div>
          <div class="ss-card ss-coll is-collapsed" data-coll="tools">
            <div class="ss-coll-head" data-coll-head="tools" role="button" tabindex="0" aria-expanded="false">
              <h3>Tools</h3>
              <span class="material-symbol ss-coll-ico">expand_more</span>
            </div>
            <div class="ss-coll-body hidden" data-coll-body="tools">
            <div class="ss-row">
              <select id="ss-tools-prov" class="input grow"></select>
            </div>
            <div class="ss-row" style="margin-top:10px">
              <select id="ss-tools-inst" class="input grow"></select>
            </div>
            <div class="ss-grid2" style="margin-top:12px">
              <button class="btn danger" id="ss-clear-watchlist">Clear watchlist</button>
              <button class="btn danger" id="ss-clear-ratings">Clear ratings</button>
              <button class="btn danger" id="ss-clear-history">Clear history</button>
              <button class="btn danger" id="ss-clear-all">Clear all</button>
            </div>
            <div id="ss-tools-progress" class="ss-progress hidden">
              <div class="ss-pbar"></div>
              <div class="ss-plabel">Working…</div>
            </div>
            <div class="ss-note" style="margin-top:10px">
              These are destructive. Use with caution!
            </div>
            <div id="ss-tools-out" class="ss-small ss-muted" style="margin-top:10px"></div>
            </div>
          </div>
          </div>
        </div>
      </div>
    `;

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
    $("#ss-clear-all", page)?.addEventListener("click", () => onClearTool(["watchlist", "ratings", "history"]));
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


  function setStatus(kind, msg, busy) {
    const k = String(kind || "").toLowerCase();
    if (k === "err") console.warn("[snapshots]", msg);
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

  const { a, b, sa, sb } = _diffPickAB();
  const run = $("#ss-diff-run", page);
  const ext = $("#ss-diff-extend", page);

  const same = !!sa && !!sb
    && String(sa.provider || "").toLowerCase() === String(sb.provider || "").toLowerCase()
    && String(sa.feature || "").toLowerCase() === String(sb.feature || "").toLowerCase();

  const ok = !!a && !!b && a !== b && same;

  if (run) {
    run.disabled = state.busy || !ok;
    run.title = ok ? "" : "Pick two captures (same provider and feature)";
  }

  if (ext) {
    ext.disabled = state.busy || !ok;
    ext.title = ok ? "Open advanced diff" : "Pick two captures (same provider and feature)";
  }
}

async function onDiffExtend() {
  const page = document.getElementById("page-snapshots");
  if (!page) return;

  const { a, b, sa, sb } = _diffPickAB();

  const same = !!sa && !!sb
    && String(sa.provider || "").toLowerCase() === String(sb.provider || "").toLowerCase()
    && String(sa.feature || "").toLowerCase() === String(sb.feature || "").toLowerCase();

  if (!a || !b || a === b || !same) return toast("Pick two captures (same provider and feature)", false);
  if (!window.openCaptureCompare) return toast("Capture Compare modal not available", false);

  state.diffAPath = a;
  state.diffBPath = b;
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
      state.diffManualOrder = true;
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

  const { a, b, sa, sb } = _diffPickAB();
  const kind = String($("#ss-diff-kind", page)?.value || "all");
  const lim = parseInt(String($("#ss-diff-limit", page)?.value || "200"), 10) || 200;

  const same = !!sa && !!sb
    && String(sa.provider || "").toLowerCase() === String(sb.provider || "").toLowerCase()
    && String(sa.feature || "").toLowerCase() === String(sb.feature || "").toLowerCase();

  if (!a || !b || a === b || !same) return toast("Pick two captures (same provider and feature)", false);

  state.diffAPath = a;
  state.diffBPath = b;
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
      (addAll ? [{ id: "", label: "All providers", configured: true }] : []).concat(opts).forEach((p) => {
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

  function repopCreateInstances() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const pid = String($("#ss-prov", page)?.value || "").toUpperCase();
    const sel = $("#ss-prov-inst", page);
    _fillInstanceSelect(sel, pid, null);
  }

  function repopToolsInstances() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const pid = String($("#ss-tools-prov", page)?.value || "").toUpperCase();
    const sel = $("#ss-tools-inst", page);
    _fillInstanceSelect(sel, pid, null);
  }

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

    if (fSel) {
      const cur = String(fSel.value || "");
      fSel.innerHTML = "";
      ["all", "watchlist", "ratings", "history"].forEach((k) => {
        const o = document.createElement("option");
        o.value = k;
        o.textContent = (k === "all") ? "All features" : k;
        if (k === "all") o.disabled = !(feats.watchlist || feats.ratings || feats.history);
        else o.disabled = !feats[k];
        fSel.appendChild(o);
      });
      if (cur) fSel.value = cur;
    }

    const fFeat = $("#ss-filter-feature", page);
    if (fFeat && fFeat.options.length === 0) {
      ["", "watchlist", "ratings", "history"].forEach((k) => {
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
      const showPick = !isBundle && (inScope || ixPick !== -1);
      const pickHtml = showPick
        ? `${abTag ? `<span class="ss-ab ${abTag === "A" ? "a" : "b"}">${abTag}</span>` : ""}` +
          `<input class="ss-chk" type="checkbox" title="Select for compare" data-act="diffpick" ${ixPick !== -1 ? "checked" : ""} />`
        : "";

      item.innerHTML = `
        <div style="flex:1 1 auto;min-width:0">
          <div class="ss-meta">
            <span class="ss-badge ok">${(s.provider || "-").toUpperCase()}</span>
            ${showInst ? `<span class="ss-badge">${inst}</span>` : ``}
            <span class="ss-badge">${feat}</span>
            ${s.label ? `<span class="ss-badge warn">${escapeHtml(_uiCaptureLabel(s.label)).slice(0, 40)}</span>` : ``}
            ${extra}
          </div>
          <div class="d">${when} * ${humanBytes(s.size)} * <span class="ss-muted">${s.path || ""}</span></div>
        </div>
        <div class="ss-right">${pickHtml}<div class="chev">></div></div>
      `;

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
      host.classList.add("ss-muted");
      host.innerHTML = "Pick a capture from the list.";
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

    host.classList.remove("ss-muted");
    host.innerHTML = `
      <div class="ss-row" style="gap:8px;flex-wrap:wrap">
        <span class="ss-badge ok">${String(s.provider || "").toUpperCase()}</span>
        ${showInst ? `<span class="ss-badge">${inst}</span>` : ``}
        <span class="ss-badge">${String(s.feature || "").toLowerCase()}</span>
        ${s.label ? `<span class="ss-badge warn">${escapeHtml(_uiCaptureLabel(s.label)).slice(0, 40)}</span>` : ``}
      </div>
      <div class="ss-small ss-muted" style="margin-top:8px">
        ${s.created_at ? new Date(String(s.created_at)).toLocaleString() : "-"} * <b>${Number(stats.count || 0)}</b> items
      </div>
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
      setStatus("err", `Refresh failed: ${e.message || e}`, false);
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
      const r = await apiJson("/api/snapshots/create", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ provider, instance, feature, label }),
      });

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
        setStatus("err", err, false);
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
      setStatus("err", "Delete failed: " + (e.message || e), false);
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
      const r = await apiJson("/api/snapshots/restore", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ path: state.selectedPath, mode, instance }),
      });

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

    const what = (features || []).join(", ");
    const ok = confirm(`This will clear ${what} on ${provider} (${instance}). Continue?`);
    if (!ok) return;

    setProgress("#ss-tools-progress", true, `Clearing ${what}…`, "danger");
    setBusy(true);
    try {
      const r = await apiJson("/api/snapshots/tools/clear", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ provider, instance, features }),
      });

      const res = r && r.result ? r.result : {};
      const out = $("#ss-tools-out", page);
      if (out) {
        if (res.ok) {
          const parts = Object.keys(res.results || {}).map((k) => {
            const x = res.results[k] || {};
            if (x.skipped) return `${k}: skipped (${x.reason || "n/a"})`;
            return `${k}: removed ${x.removed || 0} (had ${x.count || 0})`;
          });
          out.textContent = parts.join(" * ");
        } else {
          out.textContent = `Clear finished with errors.`;
        }
      }
      if (!res.ok) setStatus("err", "Tool finished with errors.", false);
      toast(res.ok ? "Clear complete" : "Clear finished with errors", !!res.ok);
    } catch (e) {
      console.warn("[snapshots] clear failed", e);
      setStatus("err", `Tool failed: ${e.message || e}`, false);
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
    setBtn("#ss-clear-all", !!feats.watchlist || !!feats.ratings || !!feats.history, "Nothing to clear");
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
