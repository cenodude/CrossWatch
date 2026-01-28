// snapshots.js - Provider snapshots (watchlist/ratings/history)
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
  @media (max-width: 1200px){
    #page-snapshots .ss-wrap{grid-template-columns:1fr;gap:14px}
    #page-snapshots .ss-list{max-height:420px}
  }
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

    let wrap = parent.querySelector(`.ss-bsel[data-for="${sel.id}"]`);
    if (!wrap) {
      wrap = document.createElement("div");
      wrap.className = "ss-bsel";
      wrap.dataset.for = sel.id;

      // Keep only layout classes; avoid inheriting visual input styles.
      const keep = String(sel.className || "").split(/\s+/).filter((c) => c === "grow").join(" ");
      if (keep) wrap.className += " " + keep;

      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "ss-bsel-btn";

      const ico = document.createElement("span");
      ico.className = "ss-provico empty";

      const label = document.createElement("span");
      label.className = "ss-bsel-label";
      label.textContent = "-";

      const chev = document.createElement("span");
      chev.className = "ss-bsel-chev";
      chev.textContent = "v";

      btn.appendChild(ico);
      btn.appendChild(label);
      btn.appendChild(chev);

      const menu = document.createElement("div");
      menu.className = "ss-bsel-menu hidden";

      wrap.appendChild(btn);
      wrap.appendChild(menu);

      // Hide native select, keep it as source of truth.
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
    if (!btn || !ico || !lab) return;

    const opt = sel.options && sel.selectedIndex >= 0 ? sel.options[sel.selectedIndex] : null;
    const value = opt ? String(opt.value || "") : "";
    const text = opt ? String(opt.textContent || "") : "";

    const brand = _provBrand(value);
    ico.className = "ss-provico " + (brand ? ("prov-card " + brand) : "empty");
    lab.textContent = text || "-";
  }

  function _rebuildBrandSelectMenu(sel) {
    const wrap = _ensureBrandSelect(sel);
    if (!wrap) return;
    const menu = wrap.querySelector(".ss-bsel-menu");
    if (!menu) return;

    menu.innerHTML = "";
    Array.from(sel.options || []).forEach((opt) => {
      const b = document.createElement("button");
      b.type = "button";
      b.className = "ss-bsel-item";
      b.disabled = !!opt.disabled;

      const value = String(opt.value || "");
      const brand = _provBrand(value);

      const ico = document.createElement("span");
      ico.className = "ss-provico " + (brand ? ("prov-card " + brand) : "empty");

      const lab = document.createElement("span");
      lab.className = "ss-bsel-label";
      lab.textContent = String(opt.textContent || "-");

      b.appendChild(ico);
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

  function bundleKey(s) {
    const stamp = String((s && s.stamp) || "");
    const prov = String((s && s.provider) || "").toLowerCase();
    const label = String((s && s.label) || "").toLowerCase();
    return stamp + "|" + prov + "|" + label;
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
          <div class="ss-title">Snapshots</div>
          <div class="ss-sub">Capture and restore provider state (watchlist / ratings / history). Stored under <span class="ss-muted">/config/snapshots</span>.</div>
        </div>
        <div class="ss-actions">
          <button id="ss-refresh" class="iconbtn" title="Refresh" aria-label="Refresh"><span id="ss-refresh-icon" class="material-symbol ss-refresh-icon">sync</span></button>
        </div>
      </div>

      <div class="ss-wrap">
        <div class="ss-card ss-accent">
          <h3>Create snapshot</h3>

          <div class="ss-field">
            <select id="ss-prov"></select>
          </div>

          <div class="ss-field" style="margin-top:10px">
            <select id="ss-feature"></select>
            <span class="chev">v</span>
          </div>

          <div class="ss-field" style="margin-top:10px">
            <input id="ss-label" placeholder="Add label..." />
          </div>

          <div class="ss-row" style="margin-top:12px">
            <button id="ss-create" class="btn primary" style="width:100%">Create Snapshot</button>
          </div>
          <div id="ss-create-progress" class="ss-progress hidden">
            <div class="ss-pbar"></div>
            <div class="ss-plabel">Working…</div>
          </div>
        </div>

        <div class="ss-card">
          <h3>Snapshots</h3>
          <div class="ss-row">
            <input id="ss-filter" class="input grow" placeholder="Filter snapshots..."/>
          </div>
          <div class="ss-row" style="margin-top:10px">
            <select id="ss-filter-provider" class="input grow"></select>
            <select id="ss-filter-feature" class="input grow"></select>
          </div>
          <div class="ss-hr"></div>
          <div id="ss-list" class="ss-list"></div>
          <div id="ss-list-footer" class="ss-row" style="justify-content:space-between;margin-top:10px"></div>
        </div>

        <div class="ss-col">
          <div class="ss-card">
            <h3>Restore snapshot</h3>
            <div id="ss-selected" class="ss-muted ss-small">Pick a snapshot from the list.</div>
            <div class="ss-hr"></div>
            <div class="ss-note">
              <b>Merge</b> adds missing items only. <b>Clear + restore</b> wipes the provider feature first, then restores exactly the snapshot.
            </div>
            <div class="ss-row" style="margin-top:12px">
              <select id="ss-restore-mode" class="input grow">
                <option value="merge">Merge</option>
                <option value="clear_restore">Clear + restore</option>
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

          <div class="ss-card">
            <h3>Tools</h3>
            <div class="ss-row">
              <select id="ss-tools-prov" class="input grow"></select>
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
    `;

    $("#ss-refresh", page)?.addEventListener("click", () => {
      state._spinUntil = Date.now() + 550;
      setRefreshSpinning(true);
      refresh(true, true);
      setTimeout(() => { if (!state.busy) setRefreshSpinning(false); }, 600);
    });
    $("#ss-create", page)?.addEventListener("click", () => onCreate());
    $("#ss-prov", page)?.addEventListener("change", () => repopFeatures());
    $("#ss-filter", page)?.addEventListener("input", () => { state.showAll = false; renderList(); });
    $("#ss-filter-provider", page)?.addEventListener("change", () => { state.showAll = false; renderList(); });
    $("#ss-filter-feature", page)?.addEventListener("change", () => { state.showAll = false; renderList(); });

    $("#ss-restore", page)?.addEventListener("click", () => onRestore());
    $("#ss-delete", page)?.addEventListener("click", () => onDeleteSelected());
    updateRestoreAvailability();

    $("#ss-clear-watchlist", page)?.addEventListener("click", () => onClearTool(["watchlist"]));
    $("#ss-clear-ratings", page)?.addEventListener("click", () => onClearTool(["ratings"]));
    $("#ss-clear-history", page)?.addEventListener("click", () => onClearTool(["history"]));
    $("#ss-clear-all", page)?.addEventListener("click", () => onClearTool(["watchlist", "ratings", "history"]));
    $("#ss-tools-prov", page)?.addEventListener("change", () => updateToolsAvailability());
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

  function setStatus(kind, msg, busy) {
    const k = String(kind || "").toLowerCase();
    if (k === "err") console.warn("[snapshots]", msg);
  }

  function updateRestoreAvailability() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;
    const b = $("#ss-restore", page);
    const d = $("#ss-delete", page);
    if (!b) return;
    b.disabled = state.busy || !state.selectedPath;
    b.title = state.selectedPath ? "" : "Select a snapshot first";
    if (d) {
      d.disabled = state.busy || !state.selectedPath;
      d.title = state.selectedPath ? "" : "Select a snapshot first";
    }
  }

  function setBusy(on) {
    state.busy = !!on;
    if (!on) {
      setProgress("#ss-create-progress", false, "", "accent");
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
      // If current value no longer exists (e.g., unconfigured), fall back to empty.
      const has = Array.from(sel.options).some((o) => String(o.value) === cur);
      sel.value = has ? cur : "";
    };

    fill(provSel, false);
    fill(toolsSel, false);
    fill(fProv, true);

    // Provider dropdowns with brand icons (native select stays as source-of-truth)
    _rebuildBrandSelectMenu(provSel);
    _rebuildBrandSelectMenu(toolsSel);
    _rebuildBrandSelectMenu(fProv);

    repopFeatures();
    updateToolsAvailability();
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

    // Default UX: bundles show as one row. Children are hidden unless expanded.
    // If user filters by feature or types in search, allow children to appear as direct matches.
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
        footer.innerHTML = topOnly.length ? `<div class="ss-small ss-muted">${topOnly.length} snapshot(s)</div>` : "";
      }

      const more = $("#ss-more", footer);
      const less = $("#ss-less", footer);
      if (more) more.addEventListener("click", () => { state.showAll = true; renderList(); });
      if (less) less.addEventListener("click", () => { state.showAll = false; renderList(); });
    }

    if (rows.length === 0) {
      list.innerHTML = `<div class="ss-empty">No snapshots found.</div>`;
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
      const when = stamp || (s.mtime ? new Date((s.mtime || 0) * 1000).toLocaleString() : "");

      const feat = String(s.feature || "-").toLowerCase();
      const isBundle = feat === "all";
      const exp = !!(state.expandedBundles && state.expandedBundles[String(s.path || "")]);

      const extra = isBundle && childCount
        ? `<button class="ss-mini" data-act="toggle">${exp ? "Hide" : "Show"} ${childCount}</button>`
        : "";

      item.innerHTML = `
        <div style="flex:1 1 auto;min-width:0">
          <div class="ss-meta">
            <span class="ss-badge ok">${(s.provider || "-").toUpperCase()}</span>
            <span class="ss-badge">${feat}</span>
            ${s.label ? `<span class="ss-badge warn">${String(s.label).slice(0, 40)}</span>` : ``}
            ${extra}
          </div>
          <div class="d">${when} * ${humanBytes(s.size)} * <span class="ss-muted">${s.path || ""}</span></div>
        </div>
        <div class="chev">></div>
      `;

      const toggleBtn = item.querySelector('[data-act="toggle"]');
      if (toggleBtn) {
        toggleBtn.addEventListener("click", (ev) => {
          ev.preventDefault();
          ev.stopPropagation();
          const key = String(s.path || "");
          state.expandedBundles = state.expandedBundles || {};
          state.expandedBundles[key] = !state.expandedBundles[key];
          renderList();
        });
      }

      item.addEventListener("click", () => {
        const p = String(s.path || "");
        if (p && state.selectedPath === p) {
          state.selectedPath = "";
          state.selectedSnap = null;
          renderList();
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
      host.innerHTML = "Pick a snapshot from the list.";
      return;
    }

    const stats = s.stats || {};
    const by = stats.by_type || {};
    const featStats = stats.features || null;
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
        <span class="ss-badge">${String(s.feature || "").toLowerCase()}</span>
        ${s.label ? `<span class="ss-badge warn">${String(s.label).slice(0, 40)}</span>` : ``}
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

      // keep selection if possible
      if (state.selectedPath) {
        const still = state.snapshots.find((x) => x.path === state.selectedPath);
        if (!still) {
          state.selectedPath = "";
          state.selectedSnap = null;
          renderSelected();
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
      renderList();
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
        body: JSON.stringify({ provider, feature, label }),
      });

      const snap = r && r.snapshot ? r.snapshot : null;
      $("#ss-label", page).value = "";
      await refresh(true, false);

      if (snap && snap.path) {
        await selectSnapshot(snap.path);
      }
      toast("Snapshot created", true);
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
    const label = s.label ? " (" + String(s.label) + ")" : "";
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
        body: JSON.stringify({ path: state.selectedPath, mode }),
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
    if (!provider) return toast("Pick a provider first", false);

    const what = (features || []).join(", ");
    const ok = confirm(`This will clear ${what} on ${provider}. Continue?`);
    if (!ok) return;

    setProgress("#ss-tools-progress", true, `Clearing ${what}…`, "danger");
    setBusy(true);
    try {
      const r = await apiJson("/api/snapshots/tools/clear", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ provider, features }),
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
setBusy(false);
    }
  }


  function updateToolsAvailability() {
    const page = document.getElementById("page-snapshots");
    if (!page) return;

    const pid = String($("#ss-tools-prov", page)?.value || "").toUpperCase();
    const p = (state.providers || []).find((x) => String(x.id || "").toUpperCase() === pid);
    const feats = (p && p.features) ? p.features : {};

    const setBtn = (id, enabled, why) => {
      const b = $(id, page);
      if (!b) return;
      const ok = !!enabled && !!pid;
      b.disabled = !ok || !!state.busy;
      b.title = ok ? "" : (why || "Not supported by provider");
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

  // auto boot when the page exists
  if (document.getElementById("page-snapshots")) {
    init();
  } else {
    // when tabs are used, page might be injected later
    document.addEventListener("tab-changed", (e) => {
      if (e?.detail?.id === "snapshots") {
        try { init(); } catch {}
      }
    });
  }

})();
