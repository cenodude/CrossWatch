(function(){
  // ---------- styles ----------
  const css = `
  .wl-wrap{display:grid;grid-template-columns:minmax(0,1fr) 300px;gap:16px}
  .wl-controls{display:flex;align-items:center;gap:12px;flex-wrap:wrap}
  .wl-grid{display:grid;gap:10px;grid-template-columns:repeat(auto-fill,minmax(150px,1fr))}
  .wl-side{display:flex;flex-direction:column;gap:10px}
  .wl-input{background:#15151c;border:1px solid rgba(255,255,255,.12);border-radius:8px;padding:8px 10px;color:#fff;width:100%}
  .wl-btn{background:#1d1d26;border:1px solid rgba(255,255,255,.15);border-radius:8px;color:#fff;padding:8px 10px;cursor:pointer}
  .wl-btn.danger{background:#2a1113;border-color:#57252a}
  .wl-chip{display:inline-flex;align-items:center;gap:6px;border-radius:16px;padding:6px 10px;background:#171720;border:1px solid rgba(255,255,255,.1);white-space:nowrap}
  .wl-chip input{accent-color:#6cf}
  .wl-muted{opacity:.7}
  .wl-row{display:flex;gap:10px}
  .wl-actions{display:flex;gap:10px}
  .wl-grid{min-height:240px}
  .wl-card{position:relative;border-radius:12px;overflow:hidden;background:#0f0f13;border:1px solid rgba(255,255,255,.08)}
  .wl-card img{width:100%;height:100%;object-fit:cover;display:block}
  .wl-card .wl-top{position:absolute;inset:8px 8px auto auto;display:flex;gap:6px;align-items:center}
  .wl-card .wl-top input[type="checkbox"]{width:18px;height:18px;opacity:0;pointer-events:none;transition:.18s ease}
  .wl-card:hover .wl-top input[type="checkbox"]{opacity:1;pointer-events:auto}
  .wl-card .wl-tags{position:absolute;left:8px;top:8px;display:flex;gap:6px;flex-wrap:wrap}
  .wl-tag{font-size:11px;padding:2px 6px;border-radius:6px;border:1px solid rgba(255,255,255,.12);background:rgba(0,0,0,.35);backdrop-filter:blur(4px)}
  .wl-hidden{display:none!important}
  .wl-empty{padding:24px;border:1px dashed rgba(255,255,255,.12);border-radius:12px;text-align:center}
  #wl-providers{display:flex;gap:8px;flex-wrap:nowrap;overflow-x:auto}
  .wl-snack{position:fixed;left:50%;transform:translateX(-50%);bottom:20px;background:#1a1a22;border:1px solid rgba(255,255,255,.15);border-radius:10px;padding:10px 12px;display:flex;gap:10px;align-items:center;z-index:9999}
  .wl-snack .wl-btn{padding:6px 10px}
  `;
  const st = document.createElement("style"); st.id = "watchlist-styles"; st.textContent = css;
  document.head.appendChild(st);

  // ---------- elements ----------
  const host = document.getElementById("page-watchlist");
  if (!host) return;

  host.innerHTML = `
    <div class="title">Watchlist</div>
    <div class="wl-wrap" id="watchlist-root">
      <div>
        <div class="wl-controls">
          <label class="wl-chip wl-selectall"><input id="wl-select-all" type="checkbox"> <span>Select all</span></label>
          <span id="wl-count" class="wl-muted">0 selected</span>
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
            <input id="wl-year" class="wl-input" placeholder="Year" style="max-width:140px">
            <select id="wl-type" class="wl-input">
              <option value="">All types</option>
              <option value="movie">Movie</option>
              <option value="tv">TV</option>
              <option value="show">TV</option>
            </select>
          </div>
          <div>
            <div class="wl-muted" style="font-size:12px;margin-bottom:6px">Provider</div>
            <div id="wl-providers"></div>
          </div>
        </div>

        <div style="height:10px"></div>
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

  const grid = document.getElementById("wl-grid");
  const empty = document.getElementById("wl-empty");
  const selAll = document.getElementById("wl-select-all");
  const selCount = document.getElementById("wl-count");
  const qEl = document.getElementById("wl-q");
  const yEl = document.getElementById("wl-year");
  const tEl = document.getElementById("wl-type");
  const provWrap = document.getElementById("wl-providers");
  const delProv = document.getElementById("wl-delete-provider");
  const clearBtn = document.getElementById("wl-clear");
  const hideBtn = document.getElementById("wl-hide");
  const unhideBtn = document.getElementById("wl-unhide");
  const snack = document.getElementById("wl-snack");

  // ---------- state ----------
  let items = [];
  let filtered = [];
  const selected = new Set();
  const allProviders = ["PLEX","SIMKL","TRAKT"]; // show all in filters
  const hiddenSet = loadHidden();
  let lastClickedKey = null;

  // ---------- utils ----------
  function loadHidden(){
    try { return new Set(JSON.parse(localStorage.getItem("wl.hidden")||"[]")); }
    catch { return new Set(); }
  }
  function persistHidden(){
    try { localStorage.setItem("wl.hidden", JSON.stringify([...hiddenSet])); } catch {}
  }

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

  const providersOf = (it) => (Array.isArray(it.sources) ? it.sources.map(s=>String(s).toUpperCase()) : []);

  const fetchWatchlist = async () => {
    const r = await fetch("/api/watchlist?limit=5000", {cache:"no-store"});
    if (!r.ok) throw new Error("watchlist fetch failed");
    const j = await r.json();
    return Array.isArray(j?.items) ? j.items : [];
  };

  function applyFilters(){
    const q = (qEl.value||"").toLowerCase().trim();
    const yr = (yEl.value||"").trim();
    const ty = (tEl.value||"").trim();
    const prov = [...provWrap.querySelectorAll("input[type=checkbox]:checked")].map(x=>x.value.toUpperCase());

    filtered = items.filter(it=>{
      const key = normKey(it);
      if (!document.getElementById("wl-show-hidden")?.checked && hiddenSet.has(key)) return false;

      if (q && !String(it.title||"").toLowerCase().includes(q)) return false;
      if (yr && String(it.year||"") !== yr) return false;

      const t = String(it.type||"").toLowerCase();
      const normType = (t === "show" ? "tv" : t);
      if (ty && normType !== ty) return false;

      const have = providersOf(it);
      if (prov.length && !prov.some(p => have.includes(p))) return false;

      return true;
    });
    renderGrid();
  }

  function renderProviders(){
    // provider chips: show all three
    provWrap.innerHTML = "";
    allProviders.forEach(p=>{
      const id = `wl-prov-${p.toLowerCase()}`;
      provWrap.insertAdjacentHTML("beforeend",
        `<label class="wl-chip"><input type="checkbox" id="${id}" value="${p}"><span>${p}</span></label>`);
    });

    // delete dropdown: only PLEX
    delProv.innerHTML = `<option value="">Delete from…</option><option value="PLEX">PLEX</option>`;
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
    filtered.forEach(it=>{
      const img = artUrl(it, "w342");
      const provHtml = providersOf(it).map(p=>`<span class="wl-tag">${p}</span>`).join("");
      const key = normKey(it);
      const chk = selected.has(key) ? "checked" : "";
      const hiddenCls = hiddenSet.has(key) && !document.getElementById("wl-show-hidden")?.checked ? "wl-hidden" : "";
      const card = document.createElement("div");
      card.className = `wl-card ${hiddenCls}`;
      card.innerHTML = `
        <div class="wl-top"><input type="checkbox" ${chk}></div>
        <div class="wl-tags">${provHtml}</div>
        ${img ? `<img loading="lazy" src="${img}" alt="">` : `<div style="height:225px"></div>`}
      `;
      card.addEventListener("click", (e)=>{
        const cb = card.querySelector("input[type=checkbox]");
        if (e.shiftKey && lastClickedKey){
          const keys = filtered.map(normKey);
          const i1 = keys.indexOf(lastClickedKey);
          const i2 = keys.indexOf(key);
          if (i1 >=0 && i2 >= 0){
            const [a,b] = i1 < i2 ? [i1,i2] : [i2,i1];
            for (let i=a;i<=b;i++) selected.add(keys[i]);
            document.querySelectorAll(".wl-card input[type=checkbox]").forEach((el,idx)=>{
              const k = keys[idx]; el.checked = selected.has(k);
            });
          }
        } else {
          if (cb) {
            cb.checked = !cb.checked;
            if (cb.checked) selected.add(key); else selected.delete(key);
          }
        }
        lastClickedKey = key;
        updateSelCount();
      });
      frag.appendChild(card);
    });
    grid.appendChild(frag);
    updateSelCount();
  }

  function updateSelCount(){
    selCount.textContent = `${selected.size} selected`;
    const provider = delProv.value || "";
    document.getElementById("wl-delete").disabled = !(provider && selected.size);
    document.getElementById("wl-hide").disabled = selected.size === 0;
  }

  function snackbar(html, actions=[]) {
    const snack = document.getElementById("wl-snack");
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
    setTimeout(()=> snack.classList.add("wl-hidden"), 4000);
  }

  // ---------- actions ----------
  [qEl,yEl,tEl].forEach(el => el.addEventListener("input", applyFilters));
  provWrap.addEventListener("change", applyFilters);

  document.getElementById("wl-select-all").addEventListener("change", ()=>{
    selected.clear();
    if (selAll.checked) filtered.forEach(it => { const key = normKey(it); if (key) selected.add(key); });
    grid.querySelectorAll(".wl-card input[type=checkbox]").forEach(cb => cb.checked = selAll.checked);
    updateSelCount();
  });

  clearBtn.addEventListener("click", ()=>{
    qEl.value = ""; yEl.value=""; tEl.value="";
    provWrap.querySelectorAll("input[type=checkbox]").forEach(cb => cb.checked = false);
    applyFilters();
  });

  delProv.addEventListener("change", updateSelCount);

  // Delete (PLEX only visible)
  document.getElementById("wl-delete").addEventListener("click", async ()=>{
    const provider = delProv.value || "";
    if (!provider || !selected.size) return;

    const keys = [...selected];
    const backup = items.slice();

    // optimistic
    items = items.filter(it => !selected.has(normKey(it)));
    selected.clear(); applyFilters();

    document.getElementById("wl-delete").disabled = true;
    try {
      const CHUNK = 50;
      for (let i=0;i<keys.length;i+=CHUNK){
        const part = keys.slice(i,i+CHUNK);
        const r = await fetch("/api/watchlist/delete", {
          method:"POST",
          headers:{"Content-Type":"application/json"},
          body: JSON.stringify({ keys: part, provider })
        });
        if (!r.ok) throw new Error("delete failed");
      }
      // hard refetch
      items = await fetchWatchlist();
      filtered = items.slice();
      renderGrid();
      snackbar(`Deleted ${keys.length} from <b>${provider}</b>`);
    } catch (e) {
      items = backup; applyFilters();
      snackbar(`Delete failed`);
    } finally {
      document.getElementById("wl-delete").disabled = false;
    }
  });

  // Hide / Unhide
  document.getElementById("wl-hide").addEventListener("click", ()=>{
    const keys = [...selected];
    keys.forEach(k => hiddenSet.add(k));
    persistHidden();
    selected.clear();
    applyFilters();
    snackbar(`Hidden ${keys.length} locally`, [
      { key:"undo", label:"Undo", onClick:()=>{ keys.forEach(k=>hiddenSet.delete(k)); persistHidden(); applyFilters(); } }
    ]);
  });
  document.getElementById("wl-unhide").addEventListener("click", ()=>{
    const prev = [...hiddenSet];
    hiddenSet.clear(); persistHidden(); applyFilters();
    snackbar(`Unhid ${prev.length}`, [
      { key:"undo", label:"Undo", onClick:()=>{ prev.forEach(k=>hiddenSet.add(k)); persistHidden(); applyFilters(); } }
    ]);
  });

  // Keyboard: Delete key
  document.addEventListener("keydown", (e)=>{
    if (e.key === "Delete" && !document.getElementById("wl-delete").disabled) document.getElementById("wl-delete").click();
  }, true);

  // ---------- init ----------
  (async function init(){
    renderProviders(); // show PLEX/SIMKL/TRAKT as filters; delete dropdown only PLEX
    items = await fetchWatchlist();
    filtered = items.slice();
    renderGrid();
  })();

  // Periodic auto-refresh (visible tab only)
  const AUTO_REFRESH_MS = 15000;
  setInterval(async ()=>{
    if (document.visibilityState !== "visible") return;
    try {
      const list = await fetchWatchlist();
      items = list;
      applyFilters();
    } catch {}
  }, AUTO_REFRESH_MS);

  // legacy adapter (no-op mount; manual refresh event hook)
  window.Watchlist = {
    async mount(_host) {},
    async refresh() {
      try {
        const r = await fetch("/api/watchlist?limit=5000", {cache:"no-store"});
        if (!r.ok) return;
        const j = await r.json();
        const list = Array.isArray(j?.items) ? j.items : [];
        items = list; applyFilters();
      } catch {}
    }
  };
  window.dispatchEvent(new CustomEvent("watchlist-ready"));
})();
