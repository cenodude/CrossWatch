// /assets/watchlist.js
(() => {
  // ---------- styles ----------
  const css = `
  /* layout */
  .wl-wrap{display:grid;grid-template-columns:1fr 300px;gap:14px}
  .wl-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:12px}
  .wl-side{border:1px solid rgba(255,255,255,.1);border-radius:12px;padding:12px;background:rgba(255,255,255,.03)}
  .wl-controls{display:flex;align-items:center;gap:8px;margin-bottom:8px}
  .wl-controls .wl-chip{white-space:nowrap}
  .wl-actions{display:flex;gap:8px;margin:8px 0 2px}
  .wl-muted{opacity:.72}
  .wl-input{width:100%;padding:8px;border-radius:10px;border:1px solid rgba(255,255,255,.12);background:#121218;color:#fff}
  .wl-chip{display:inline-flex;gap:6px;align-items:center;border:1px solid rgba(255,255,255,.12);border-radius:999px;padding:2px 8px;font-size:12px}
  .wl-btn{padding:8px 10px;border-radius:10px;border:1px solid rgba(255,255,255,.14);background:#181820;color:#fff;cursor:pointer}
  .wl-btn.danger{border-color:rgba(255,80,80,.5);color:#ff7b7b}
  .wl-btn:disabled{opacity:.55;cursor:not-allowed}
  .wl-row{display:flex;gap:8px;align-items:center}

  /* card */
  .wl-card{position:relative;border-radius:12px;overflow:hidden;background:#0f0f13;border:1px solid rgba(255,255,255,.08)}
  .wl-card img{width:100%;height:100%;object-fit:cover;display:block}
  .wl-card .wl-top{position:absolute;inset:8px 8px auto auto;display:flex;gap:6px;align-items:center;opacity:0;transition:opacity .15s ease}
  .selmode .wl-card .wl-top,.wl-card:hover .wl-top{opacity:1}
  .wl-card .wl-top input[type="checkbox"]{width:18px;height:18px}
  .wl-card .wl-tags{position:absolute;left:8px;top:8px;display:flex;gap:6px;flex-wrap:wrap}
  .wl-tag{font-size:11px;padding:2px 6px;border-radius:6px;border:1px solid rgba(255,255,255,.12);background:rgba(0,0,0,.35);backdrop-filter:blur(4px)}
  .wl-card .wl-overlay{position:absolute;inset:auto 0 0 0;background:linear-gradient(0deg,rgba(0,0,0,.82),rgba(0,0,0,.0));padding:10px 10px 8px}
  .wl-title{font-weight:650;font-size:13px}
  .wl-sub{font-size:12px;opacity:.85}
  .wl-overview{font-size:12px;opacity:.9;margin-top:6px;max-height:6.6em;overflow:hidden}
  .wl-hidden{display:none!important}

  /* per-item delete */
  .wl-del{position:absolute;top:8px;left:8px;width:28px;height:28px;border-radius:8px;border:1px solid rgba(255,255,255,.14);background:rgba(0,0,0,.45);display:flex;align-items:center;justify-content:center;opacity:0;transition:opacity .15s ease,transform .15s ease}
  .wl-card:hover .wl-del{opacity:1}
  .wl-del svg{width:16px;height:16px}
  .wl-del:hover{transform:scale(1.06)}

  /* empty */
  .wl-empty{padding:24px;border:1px dashed rgba(255,255,255,.12);border-radius:12px;text-align:center}

  /* snackbar */
  .wl-snack{position:fixed;left:50%;transform:translateX(-50%);bottom:20px;background:#1a1a22;border:1px solid rgba(255,255,255,.15);border-radius:10px;padding:10px 12px;display:flex;gap:10px;align-items:center;z-index:9999}
  .wl-snack .wl-btn{padding:6px 10px}
  `;
  if (!document.getElementById("watchlist-styles")) {
    const st = document.createElement("style");
    st.id = "watchlist-styles";
    st.textContent = css;
    document.head.appendChild(st);
  }

  // host
  const host =
    document.getElementById("watchlist-root") ||
    document.getElementById("page-watchlist");
  if (!host) return;

  // replace content
  host.innerHTML = `
    <div class="title">Watchlist</div>
    <div class="wl-wrap" id="wl-root">
      <div>
        <div class="wl-controls">
          <label class="wl-chip"><input id="wl-select-all" type="checkbox"> Select all</label>
          <span id="wl-count" class="wl-muted">0 selected</span>
          <span class="wl-muted" style="margin-left:auto;font-size:12px">Tips: Shift+click = range • Del = delete</span>
        </div>
        <div id="wl-grid" class="wl-grid"></div>
        <div id="wl-empty" class="wl-empty wl-muted" style="display:none">No items</div>
      </div>
      <aside class="wl-side">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
          <div style="font-weight:700">Filters</div>
          <button id="wl-clear" class="wl-btn">Reset</button>
        </div>
        <div style="display:grid;gap:10px">
          <input id="wl-q" class="wl-input" placeholder="Search title…">
          <div class="wl-row">
            <input id="wl-year" class="wl-input" placeholder="Year" style="max-width:120px">
            <select id="wl-type" class="wl-input">
              <option value="">All types</option>
              <option value="movie">Movie</option>
              <option value="tv">TV</option>
            </select>
          </div>

          <div>
            <div class="wl-muted" style="font-size:12px;margin-bottom:6px">Provider</div>
            <div id="wl-providers" style="display:flex;gap:8px;flex-wrap:wrap"></div>
          </div>

            <div>
              <div class="wl-muted" style="font-size:12px;margin-bottom:6px">Special</div>
              <div class="wl-row">
                <label class="wl-chip"><input id="wl-missing-on" type="checkbox"> Only missing on</label>
                <select id="wl-missing-provider" class="wl-input" style="flex:1">
                  <option value="">Choose…</option>
                </select>
              </div>
              <label class="wl-chip"><input id="wl-show-hidden" type="checkbox"> Show locally hidden</label>
            </div>
        </div>

        <hr style="border:none;border-top:1px solid rgba(255,255,255,.1);margin:12px 0">

        <div style="font-weight:700;margin-bottom:6px">Actions</div>
        <div class="wl-actions">
          <select id="wl-delete-provider" class="wl-input" style="flex:1">
            <option value="">Delete from…</option>
          </select>
          <button id="wl-delete" class="wl-btn danger" disabled>Delete</button>
        </div>
        <div class="wl-actions">
          <button id="wl-hide" class="wl-btn" disabled>Hide (local)</button>
          <button id="wl-unhide" class="wl-btn">Unhide all</button>
        </div>
      </aside>
    </div>
    <div id="wl-snack" class="wl-snack wl-hidden" role="status" aria-live="polite"></div>
  `;

  // refs
  const root = document.getElementById("wl-root");
  const grid = document.getElementById("wl-grid");
  const empty = document.getElementById("wl-empty");
  const selAll = document.getElementById("wl-select-all");
  const selCount = document.getElementById("wl-count");
  const qEl = document.getElementById("wl-q");
  const yEl = document.getElementById("wl-year");
  const tEl = document.getElementById("wl-type");
  const provWrap = document.getElementById("wl-providers");
  const delProv = document.getElementById("wl-delete-provider");
  const delBtn = document.getElementById("wl-delete");
  const clearBtn = document.getElementById("wl-clear");
  const missingChk = document.getElementById("wl-missing-on");
  const missingProv = document.getElementById("wl-missing-provider");
  const showHidden = document.getElementById("wl-show-hidden");
  const hideBtn = document.getElementById("wl-hide");
  const unhideBtn = document.getElementById("wl-unhide");
  const snack = document.getElementById("wl-snack");

  // state
  let items = [];
  let filtered = [];
  const selected = new Set();
  let enabledProviders = [];
  let lastClickedKey = null;
  const hiddenSet = loadHidden();

  // utils
  function loadHidden(){
    try { return new Set(JSON.parse(localStorage.getItem("wl.hidden") || "[]")); }
    catch { return new Set(); }
  }
  function persistHidden(){
    try { localStorage.setItem("wl.hidden", JSON.stringify([...hiddenSet])); } catch {}
  }
  function setSelMode(on){ root.classList.toggle("selmode", !!on); }

  const artUrl = (it, size) => {
    const typ = (it.type === "tv" || it.type === "show") ? "tv" : "movie";
    const tmdb = it.tmdb || (it.ids && it.ids.tmdb);
    return tmdb ? `/art/tmdb/${typ}/${tmdb}?size=${encodeURIComponent(size||"w342")}` : "";
  };
  const normKey = (it) =>
    it.key || it.guid || it.id ||
    (it.ids?.imdb && `imdb:${it.ids.imdb}`) ||
    (it.ids?.tmdb && `tmdb:${it.ids.tmdb}`) ||
    (it.ids?.tvdb && `tvdb:${it.ids.tvdb}`) || "";

  async function getEnabledProviders(){
    try {
      const r = await fetch("/api/config", {cache:"no-store"});
      if (!r.ok) return [];
      const cfg = await r.json();
      const arr = [];
      if (cfg.plex?.account_token) arr.push("PLEX");
      if (cfg.simkl?.access_token || cfg.auth?.simkl?.access_token) arr.push("SIMKL");
      if (cfg.trakt?.access_token || cfg.auth?.trakt?.access_token) arr.push("TRAKT");
      return arr;
    } catch { return []; }
  }

  async function fetchWatchlist(){
    const r = await fetch("/api/watchlist?limit=5000", {cache:"no-store"});
    if (!r.ok) throw new Error("watchlist fetch failed");
    const j = await r.json();
    return Array.isArray(j?.items) ? j.items : [];
  }

  // accept array form or object form
  function providersOf(it){
    if (Array.isArray(it.sources) && it.sources.length)
      return it.sources.map(s => String(s).toUpperCase());
    const obj = it.providers || it.from || {};
    return Object.keys(obj).filter(k => obj[k]).map(k=>k.toUpperCase());
  }

  function applyFilters(){
    const q = (qEl.value||"").toLowerCase().trim();
    const yr = (yEl.value||"").trim();
    const ty = (tEl.value||"").trim();
    const prov = [...provWrap.querySelectorAll("input[type=checkbox]:checked")].map(x=>x.value);
    const missOn = missingChk.checked ? (missingProv.value||"").toUpperCase() : "";

    filtered = items.filter(it=>{
      const key = normKey(it);
      if (!showHidden.checked && hiddenSet.has(key)) return false;
      if (q && !String(it.title||"").toLowerCase().includes(q)) return false;
      if (yr && String(it.year||"") !== yr) return false;
      if (ty && (String(it.type||"").toLowerCase() !== ty)) return false;

      const have = providersOf(it);
      if (prov.length && !prov.some(p => have.includes(p))) return false;
      if (missOn && have.includes(missOn)) return false;

      return true;
    });
    renderGrid();
  }

  function renderProviders(){
    provWrap.innerHTML = "";
    delProv.innerHTML = `<option value="">Delete from…</option>`;
    missingProv.innerHTML = `<option value="">Choose…</option>`;
    enabledProviders.forEach(p=>{
      const id = `wl-prov-${p.toLowerCase()}`;
      provWrap.insertAdjacentHTML("beforeend",
        `<label class="wl-chip"><input type="checkbox" id="${id}" value="${p}">${p}</label>`);
      delProv.insertAdjacentHTML("beforeend", `<option value="${p}">${p}</option>`);
      missingProv.insertAdjacentHTML("beforeend", `<option value="${p}">${p}</option>`);
    });
  }

  function renderGrid(){
    grid.innerHTML = "";
    if (!filtered.length){
      empty.style.display = "";
      selAll.checked = false;
      selected.clear();
      updateSelCount();
      return;
    }
    empty.style.display = "none";

    const frag = document.createDocumentFragment();
    const cards = [];

    filtered.forEach(it=>{
      const img = artUrl(it, "w342");
      const provHtml = providersOf(it).map(p=>`<span class="wl-tag">${p}</span>`).join("");
      const key = normKey(it);
      const chk = selected.has(key) ? "checked" : "";
      const hiddenCls = hiddenSet.has(key) && !showHidden.checked ? "wl-hidden" : "";

      const el = document.createElement("div");
      el.className = `wl-card ${hiddenCls}`;
      el.dataset.key = key;
      el.innerHTML = `
        <button class="wl-del" title="Delete">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
            <polyline points="3 6 5 6 21 6"></polyline>
            <path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"></path>
            <path d="M10 11v6M14 11v6M9 6V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2"></path>
          </svg>
        </button>
        <div class="wl-tags">${provHtml}</div>
        <div class="wl-top"><input type="checkbox" ${chk} aria-label="select"></div>
        ${img ? `<img src="${img}" alt="">` : `<div style="height:220px;background:#101015"></div>`}
        <div class="wl-overlay">
          <div class="wl-title">${it.title||"?"}</div>
          <div class="wl-sub">${(it.year||"")}${it.type?` · ${String(it.type).toUpperCase()}`:""}</div>
          ${it.overview ? `<div class="wl-overview">${it.overview}</div>` : ""}
        </div>`;
      frag.appendChild(el);
      cards.push(el);
    });
    grid.appendChild(frag);

    // wire
    cards.forEach(card=>{
      const key = card.dataset.key;
      const cb = card.querySelector('input[type=checkbox]');
      cb.addEventListener("click", (e)=>{
        const list = cards;
        if (e.shiftKey && lastClickedKey) {
          const idx1 = list.findIndex(c=>c.dataset.key===lastClickedKey);
          const idx2 = list.findIndex(c=>c.dataset.key===key);
          if (idx1 >= 0 && idx2 >= 0) {
            const [a,b] = idx1 < idx2 ? [idx1,idx2] : [idx2,idx1];
            for (let i=a;i<=b;i++){
              const ck = list[i].dataset.key;
              selected.add(ck);
              list[i].querySelector('input[type=checkbox]').checked = true;
            }
          }
        } else {
          if (cb.checked) selected.add(key); else selected.delete(key);
        }
        lastClickedKey = key;
        updateSelCount();
      });

      const del = card.querySelector(".wl-del");
      del.addEventListener("click", async (e)=>{
        e.stopPropagation();
        const provider = delProv.value || "";
        if (!provider) { snackbar("Pick a provider in <b>Delete from…</b>"); return; }

        const backup = items.slice();
        items = items.filter(x => normKey(x) !== key);
        selected.delete(key);
        applyFilters();

        try {
          await fetch("/api/watchlist/delete", {
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body: JSON.stringify({ keys:[key], provider })
          });
          snackbar(`Deleted from <b>${provider}</b>`);
        } catch {
          items = backup; applyFilters();
          snackbar("Delete failed");
        }
      });
    });

    updateSelCount();
  }

  function updateSelCount(){
    selCount.textContent = `${selected.size} selected`;
    delBtn.disabled = !(selected.size && delProv.value);
    hideBtn.disabled = selected.size === 0;
    setSelMode(selected.size > 0 || selAll.checked);
  }

  function snackbar(html, actions=[]){
    snack.innerHTML = html + actions.map(a=>` <button class="wl-btn" data-k="${a.key}">${a.label}</button>`).join("");
    snack.classList.remove("wl-hidden");
    const handler = (e)=>{
      const k = e.target?.dataset?.k;
      if (!k) return;
      actions.find(a=>a.key===k)?.onClick?.();
      snack.classList.add("wl-hidden");
      snack.removeEventListener("click", handler, true);
    };
    snack.addEventListener("click", handler, true);
    setTimeout(()=> snack.classList.add("wl-hidden"), 5000);
  }

  // actions
  selAll.addEventListener("change", ()=>{
    selected.clear();
    if (selAll.checked) filtered.forEach(it => {
      const key = normKey(it); if (key) selected.add(key);
    });
    grid.querySelectorAll(".wl-card input[type=checkbox]").forEach(cb => cb.checked = selAll.checked);
    updateSelCount();
  });

  [qEl,yEl,tEl,missingProv].forEach(el => el.addEventListener("input", applyFilters));
  missingChk.addEventListener("change", applyFilters);
  showHidden.addEventListener("change", applyFilters);
  provWrap.addEventListener("change", applyFilters);

  clearBtn.addEventListener("click", ()=>{
    qEl.value = ""; yEl.value=""; tEl.value="";
    provWrap.querySelectorAll("input[type=checkbox]").forEach(cb => cb.checked = false);
    missingChk.checked = false; missingProv.value = "";
    applyFilters();
  });

  delProv.addEventListener("change", updateSelCount);

  delBtn.addEventListener("click", async ()=>{
    const provider = delProv.value || "";
    if (!provider || !selected.size) return;

    const keys = [...selected];
    const backup = items.slice();

    items = items.filter(it => !selected.has(normKey(it)));
    selected.clear();
    applyFilters();

    delBtn.disabled = true;
    try {
      const CHUNK = 20;
      for (let i=0;i<keys.length;i+=CHUNK){
        const part = keys.slice(i,i+CHUNK);
        await fetch("/api/watchlist/delete", {
          method:"POST",
          headers:{"Content-Type":"application/json"},
          body: JSON.stringify({ keys: part, provider })
        });
      }
      snackbar(`Deleted ${keys.length} from <b>${provider}</b>`, [
        { key:"undo", label:"Undo", onClick:()=>{ items = backup; applyFilters(); } }
      ]);
    } catch {
      items = backup; applyFilters();
      snackbar("Delete failed");
    } finally {
      delBtn.disabled = false;
    }
  });

  hideBtn.addEventListener("click", ()=>{
    const keys = [...selected];
    keys.forEach(k => hiddenSet.add(k));
    persistHidden();
    selected.clear();
    applyFilters();
    snackbar(`Hidden ${keys.length} locally`, [
      { key:"undo", label:"Undo", onClick:()=>{ keys.forEach(k=>hiddenSet.delete(k)); persistHidden(); applyFilters(); } }
    ]);
  });

  unhideBtn.addEventListener("click", ()=>{
    const prev = [...hiddenSet];
    hiddenSet.clear(); persistHidden(); applyFilters();
    snackbar(`Unhid ${prev.length}`, [
      { key:"undo", label:"Undo", onClick:()=>{ prev.forEach(k=>hiddenSet.add(k)); persistHidden(); applyFilters(); } }
    ]);
  });

  document.addEventListener("keydown", (e)=>{
    if (e.key === "Delete" && !delBtn.disabled) delBtn.click();
  }, true);

  // init
  async function boot(){
    enabledProviders = await getEnabledProviders();
    renderProviders();
    items = await fetchWatchlist();
    filtered = items.slice();
    renderGrid();
  }
  boot();

  // legacy hook
  window.loadWatchlist = async function(){ /* no-op */ };
})();

// ---- public adapter (global) ----
(function(){
  window.Watchlist = {
    async mount(hostEl){
      // already mounted by file load; nothing to do
    },
    async refresh(){
      // future: could refetch here if needed
    }
  };
  window.dispatchEvent(new CustomEvent("watchlist-ready"));
})();
