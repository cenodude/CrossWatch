(function(){
  // ---------- styles ----------
  const css = `
  .wl-wrap{display:grid;grid-template-columns:minmax(0,1fr) 360px;gap:16px}
  .wl-controls{display:flex;align-items:center;gap:12px;flex-wrap:wrap}
  .wl-input{background:#15151c;border:1px solid rgba(255,255,255,.12);border-radius:8px;padding:8px 10px;color:#fff;width:100%}
  .wl-btn{background:#1d1d26;border:1px solid rgba(255,255,255,.15);border-radius:8px;color:#fff;padding:8px 10px;cursor:pointer}
  .wl-btn.danger{background:#2a1113;border-color:#57252a}
  .wl-chip{display:inline-flex;align-items:center;gap:6px;border-radius:16px;padding:6px 10px;background:#171720;border:1px solid rgba(255,255,255,.1);white-space:nowrap}
  .wl-muted{opacity:.7}
  .wl-row{display:flex;gap:10px}
  .wl-actions{display:flex;gap:10px}
  .wl-empty{padding:24px;border:1px dashed rgba(255,255,255,.12);border-radius:12px;text-align:center}

  /* Posters view */
  .wl-grid{display:grid;gap:10px;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));min-height:240px}
  .wl-card{position:relative;border-radius:12px;overflow:hidden;background:#0f0f13;border:1px solid rgba(255,255,255,.08);transition:box-shadow .15s ease,border-color .15s ease}
  .wl-card img{width:100%;height:100%;object-fit:cover;display:block}
  .wl-card .wl-tags{position:absolute;left:8px;top:8px;display:flex;gap:6px;flex-wrap:wrap;z-index:2}
  .wl-tag{font-size:11px;padding:2px 6px;border-radius:6px;border:1px solid rgba(255,255,255,.12);background:rgba(0,0,0,.35);backdrop-filter:blur(4px)}
  .wl-card.selected{box-shadow:0 0 0 3px #6f6cff,0 0 0 5px rgba(111,108,255,.35)}

  /* List view */
  .wl-table-wrap{border:1px solid rgba(255,255,255,.12);border-radius:10px;overflow:auto}
  .wl-table{width:100%;border-collapse:separate;border-spacing:0;table-layout:fixed}
  .wl-table col.c-sel{width:44px}
  .wl-table col.c-title{width:auto}
  .wl-table col.c-type{width:120px}
  .wl-table col.c-sync{width:190px}
  .wl-table col.c-sources{width:220px}
  .wl-table col.c-poster{width:80px}
  .wl-table th,.wl-table td{padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.08);white-space:nowrap;text-align:left}
  .wl-table th{position:sticky;top:0;background:#101018;font-weight:600;z-index:1;text-transform:none;letter-spacing:0}
  .wl-table tr:last-child td{border-bottom:none}
  .wl-table .title{white-space:normal}
  .wl-table input[type=checkbox]{width:18px;height:18px}

  /* Sources */
  .wl-srcs{display:flex;gap:10px;align-items:center}
  .wl-src{display:inline-flex;align-items:center;justify-content:center;height:20px}
  .wl-src img{height:16px;display:block;opacity:.95}
  .wl-badge{padding:2px 6px;border-radius:6px;border:1px solid rgba(255,255,255,.12);font-size:11px}

  /* Sync matrix */
  .wl-matrix{display:flex;gap:10px;align-items:center}
  .wl-mat{display:flex;align-items:center;gap:6px;padding:4px 6px;border:1px solid rgba(255,255,255,.12);border-radius:8px;background:#14141c}
  .wl-mat img{height:14px}
  .wl-mat .material-symbol{font-size:16px}
  .wl-mat.ok{border-color:rgba(120,255,180,.35)}
  .wl-mat.miss{opacity:.6}

  .wl-mini{width:36px;height:54px;border-radius:4px;object-fit:cover;background:#0f0f13;border:1px solid rgba(255,255,255,.08)}

  /* Sidebar cards */
  .wl-side{display:flex;flex-direction:column;gap:12px}
  .ins-card{background:linear-gradient(180deg,rgba(20,20,28,.95),rgba(16,16,24,.95));border:1px solid rgba(255,255,255,.08);border-radius:16px;padding:14px}
  .ins-row{display:flex;align-items:center;gap:12px;padding:10px 6px;border-top:1px solid rgba(255,255,255,.06)}
  .ins-row:first-child{border-top:none}
  .ins-icon{width:32px;height:32px;border-radius:10px;display:flex;align-items:center;justify-content:center;background:#13131b;border:1px solid rgba(255,255,255,.06)}
  .ins-title{font-weight:700}
  .ins-kv{display:grid;grid-template-columns:110px 1fr;gap:10px;align-items:center}
  .ins-kv label{opacity:.85}

  .ins-metrics{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px}
  .metric{display:flex;align-items:center;gap:8px;background:#12121a;border:1px solid rgba(255,255,255,.08);border-radius:12px;padding:10px}
  .metric .material-symbol{font-size:18px;opacity:.9}
  .metric .m-val{font-weight:700}
  .metric .m-lbl{font-size:12px;opacity:.75}

  .wl-snack{position:fixed;left:50%;transform:translateX(-50%);bottom:20px;background:#1a1a22;border:1px solid rgba(255,255,255,.15);border-radius:10px;padding:10px 12px;display:flex;gap:10px;align-items:center;z-index:9999}
  .wl-snack .wl-btn{padding:6px 10px}
  `;
  const st = document.createElement("style"); st.id = "watchlist-styles"; st.textContent = css;
  document.head.appendChild(st);

  // ---------- elements ----------
  const host = document.getElementById("page-watchlist");
  if (!host) return;

  host.innerHTML = `
    <div class="title" style="display:flex;align-items:center;justify-content:space-between;gap:12px">
      <span>Watchlist</span>
    </div>

    <div class="wl-wrap" id="watchlist-root">
      <div>
        <div class="wl-controls">
          <label class="wl-chip wl-selectall"><input id="wl-select-all" type="checkbox"> <span>Select all</span></label>
          <span id="wl-count" class="wl-muted">0 selected</span>
        </div>

        <div id="wl-posters" class="wl-grid" style="display:none"></div>
        <div id="wl-list" class="wl-table-wrap" style="display:none">
          <table class="wl-table">
            <colgroup>
              <col class="c-sel">
              <col class="c-title">
              <col class="c-type">
              <col class="c-sync">
              <col class="c-sources">
              <col class="c-poster">
            </colgroup>
            <thead>
              <tr>
                <th style="text-align:center"><input id="wl-list-select-all" type="checkbox"></th>
                <th>Title</th>
                <th>Type</th>
                <th>Sync</th>
                <th>Sources</th>
                <th>Poster</th>
              </tr>
            </thead>
            <tbody id="wl-tbody"></tbody>
          </table>
        </div>

        <div id="wl-empty" class="wl-empty wl-muted" style="display:none">No items</div>
      </div>

      <aside class="wl-side">
        <!-- Filters -->
        <div class="ins-card">
          <div class="ins-row">
            <div class="ins-icon"><span class="material-symbol">tune</span></div>
            <div class="ins-title">Filters</div>
          </div>
          <div class="ins-row">
            <div class="ins-kv" style="width:100%">
              <label>View</label>
              <select id="wl-view" class="wl-input" style="width:auto;padding:6px 10px">
                <option value="posters" selected>Posters</option>
                <option value="list">List</option>
              </select>

              <label>Search</label>
              <input id="wl-q" class="wl-input" placeholder="Search title…">

              <label>Type</label>
              <select id="wl-type" class="wl-input">
                <option value="">All types</option>
                <option value="movie">Movie</option>
                <option value="tv">TV</option>
                <option value="show">TV</option>
              </select>

              <label>Provider</label>
              <select id="wl-provider" class="wl-input">
                <option value="">All</option>
                <option value="PLEX">PLEX</option>
                <option value="SIMKL">SIMKL</option>
                <option value="TRAKT">TRAKT</option>
              </select>
            </div>
          </div>
          <div class="ins-row" style="justify-content:flex-end">
            <button id="wl-clear" class="wl-btn">Reset</button>
          </div>
        </div>

        <!-- Actions -->
        <div class="ins-card">
          <div class="ins-row">
            <div class="ins-icon"><span class="material-symbol">flash_on</span></div>
            <div class="ins-title">Actions</div>
          </div>
          <div class="ins-row">
            <div class="ins-kv" style="width:100%">
              <label>Delete</label>
              <div class="wl-actions">
                <select id="wl-delete-provider" class="wl-input" style="flex:1">
                  <option value="">Delete from…</option>
                  <option value="PLEX">PLEX</option>
                </select>
                <button id="wl-delete" class="wl-btn danger" disabled>Delete</button>
              </div>

              <label>Visibility</label>
              <div class="wl-actions">
                <button id="wl-hide" class="wl-btn" disabled>Hide (local)</button>
                <button id="wl-unhide" class="wl-btn">Unhide all</button>
              </div>
            </div>
          </div>
        </div>

        <!-- List Insight -->
        <div class="ins-card">
          <div class="ins-row">
            <div class="ins-icon"><span class="material-symbol">insights</span></div>
            <div class="ins-title">List Insight</div>
          </div>
          <div class="ins-row">
            <div id="wl-metrics" class="ins-metrics" style="width:100%"></div>
          </div>
        </div>
      </aside>
    </div>

    <div id="wl-snack" class="wl-snack wl-hidden" role="status" aria-live="polite"></div>
  `;

  // ---------- refs ----------
  const postersEl = document.getElementById("wl-posters");
  const listWrapEl = document.getElementById("wl-list");
  const listBodyEl = document.getElementById("wl-tbody");
  const listSelectAll = document.getElementById("wl-list-select-all");
  const empty = document.getElementById("wl-empty");
  const selAll = document.getElementById("wl-select-all");
  const selCount = document.getElementById("wl-count");
  const qEl = document.getElementById("wl-q");
  const tEl = document.getElementById("wl-type");
  const providerSel = document.getElementById("wl-provider");
  const delProv = document.getElementById("wl-delete-provider");
  const clearBtn = document.getElementById("wl-clear");
  const hideBtn = document.getElementById("wl-hide");
  const unhideBtn = document.getElementById("wl-unhide");
  const viewSel = document.getElementById("wl-view");
  const snack = document.getElementById("wl-snack");
  const metricsEl = document.getElementById("wl-metrics");

  // ---------- state ----------
  let items = [];
  let filtered = [];
  const selected = new Set();
  const hiddenSet = loadHidden();
  let viewMode = "posters"; // "posters" | "list"

  // ---------- utils ----------
  function loadHidden(){ try { return new Set(JSON.parse(localStorage.getItem("wl.hidden")||"[]")); } catch { return new Set(); } }
  function persistHidden(){ try { localStorage.setItem("wl.hidden", JSON.stringify([...hiddenSet])); } catch {} }

  const artUrl = (it, size) => {
    const typ = (it.type === "tv" || it.type === "show") ? "tv" : "movie";
    const tmdb = it.tmdb || (it.ids && it.ids.tmdb);
    return tmdb ? `/art/tmdb/${typ}/${tmdb}?size=${encodeURIComponent(size||"w92")}` : "";
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

  const SRC_LOGOS = {
    PLEX: "/assets/PLEX-log.svg",
    SIMKL: "/assets/SIMKL-log.svg",
    TRAKT: "/assets/TRAKT-log.svg",
  };

  // ---------- filters & render ----------
  function applyFilters(){
    const q = (qEl.value||"").toLowerCase().trim();
    const ty = (tEl.value||"").trim();
    const provider = (providerSel.value||"").toUpperCase();

    filtered = items.filter(it=>{
      const key = normKey(it);
      if (!document.getElementById("wl-show-hidden")?.checked && hiddenSet.has(key)) return false;
      if (q && !String(it.title||"").toLowerCase().includes(q)) return false;

      const t = String(it.type||"").toLowerCase();
      const normType = (t === "show" ? "tv" : t);
      if (ty && normType !== ty) return false;

      if (provider){
        const have = providersOf(it);
        if (!have.includes(provider)) return false;
      }

      return true;
    });

    render();
    updateMetrics();
  }

  // ---- INSIGHT: counts per provider
  function updateMetrics(){
    const onPlex  = filtered.filter(it => providersOf(it).includes("PLEX")).length;
    const onSimkl = filtered.filter(it => providersOf(it).includes("SIMKL")).length;
    const onTrakt = filtered.filter(it => providersOf(it).includes("TRAKT")).length;

    metricsEl.innerHTML = `
      ${metric('movie_filter','PLEX', onPlex)}
      ${metric('playlist_add','SIMKL', onSimkl)}
      ${metric('featured_play_list','TRAKT', onTrakt)}
    `;
  }
  function metric(icon, label, val){
    return `<div class="metric">
      <span class="material-symbol">${icon}</span>
      <div>
        <div class="m-val">${val}</div>
        <div class="m-lbl">${label}</div>
      </div>
    </div>`;
  }

  function render(){
    postersEl.style.display = (viewMode === "posters") ? "" : "none";
    listWrapEl.style.display = (viewMode === "list") ? "" : "none";

    if (!filtered.length){
      empty.style.display = "";
      selAll.checked = false;
      listSelectAll.checked = false;
      postersEl.innerHTML = "";
      listBodyEl.innerHTML = "";
      updateSelCount();
      return;
    }
    empty.style.display = "none";

    if (viewMode === "posters") renderPosters();
    else renderList();

    updateSelCount();
  }

  function renderPosters(){
    postersEl.innerHTML = "";
    const frag = document.createDocumentFragment();

    filtered.forEach(it=>{
      const img = artUrl(it, "w342");
      const provHtml = providersOf(it).map(p=>`<span class="wl-tag">${p}</span>`).join("");
      const key = normKey(it);
      const card = document.createElement("div");
      card.className = `wl-card ${selected.has(key) ? "selected": ""}`;
      card.innerHTML = `
        <div class="wl-tags">${provHtml}</div>
        ${img ? `<img loading="lazy" src="${img}" alt="">` : `<div style="height:225px"></div>`}
      `;
      card.addEventListener("click", ()=>{
        if (selected.has(key)) selected.delete(key); else selected.add(key);
        card.classList.toggle("selected");
        updateSelCount();
      });
      frag.appendChild(card);
    });
    postersEl.appendChild(frag);
  }

  function providerChip(name, ok){
    const src = SRC_LOGOS[name];
    const icon = ok ? 'check_circle' : 'cancel';
    const cls = ok ? 'ok' : 'miss';
    return `<span class="wl-mat ${cls}" title="${name}${ok?' present':' missing'}">
      ${src ? `<img src="${src}" alt="${name}">` : `<span class="wl-badge">${name}</span>`}
      <span class="material-symbol">${icon}</span>
    </span>`;
  }

  function renderList(){
    listBodyEl.innerHTML = "";
    const frag = document.createDocumentFragment();

    filtered.forEach(it=>{
      const key = normKey(it);
      const tr = document.createElement("tr");

      const typeRaw = String(it.type||"").toLowerCase();
      const type = (typeRaw === "show" ? "tv" : typeRaw) || "";

      const logos = providersOf(it).map(s=>{
        const src = SRC_LOGOS[s];
        if (src) return `<span class="wl-src" title="${s}"><img src="${src}" alt="${s} logo"></span>`;
        return `<span class="wl-badge">${s}</span>`;
      }).join("");

      const thumb = artUrl(it, "w92");

      const have = {
        PLEX: providersOf(it).includes("PLEX"),
        SIMKL: providersOf(it).includes("SIMKL"),
        TRAKT: providersOf(it).includes("TRAKT")
      };

      const matrix = `
        <div class="wl-matrix">
          ${providerChip('PLEX', have.PLEX)}
          ${providerChip('SIMKL', have.SIMKL)}
          ${providerChip('TRAKT', have.TRAKT)}
        </div>`;

      tr.innerHTML = `
        <td style="text-align:center"><input type="checkbox" data-k="${key}" ${selected.has(key)?"checked":""}></td>
        <td class="title">${esc(it.title||"")}</td>
        <td>${esc(type)}</td>
        <td>${matrix}</td>
        <td><div class="wl-srcs">${logos}</div></td>
        <td>${thumb ? `<img class="wl-mini" src="${thumb}" alt="">` : ""}</td>
      `;
      tr.querySelector('input[type=checkbox]').addEventListener("change", (e)=>{
        if (e.target.checked) selected.add(key); else selected.delete(key);
        updateSelCount();
      });
      frag.appendChild(tr);
    });

    listBodyEl.appendChild(frag);

    const allKeys = filtered.map(normKey);
    listSelectAll.checked = allKeys.length>0 && allKeys.every(k => selected.has(k));
  }

  function esc(s){ return String(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }

  function updateSelCount(){
    selCount.textContent = `${selected.size} selected`;
    const provider = delProv.value || "";
    document.getElementById("wl-delete").disabled = !(provider && selected.size);
    document.getElementById("wl-hide").disabled = selected.size === 0;
  }

  function snackbar(html, actions=[]) {
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

  // ---------- actions & events ----------
  [qEl,tEl,providerSel].forEach(el => el.addEventListener("input", applyFilters));

  selAll.addEventListener("change", ()=>{
    selected.clear();
    if (selAll.checked) filtered.forEach(it => { const key = normKey(it); if (key) selected.add(key); });
    if (viewMode === "posters") renderPosters(); else renderList();
    updateSelCount();
  });

  listSelectAll.addEventListener("change", ()=>{
    selected.clear();
    if (listSelectAll.checked) filtered.forEach(it => { const key = normKey(it); if (key) selected.add(key); });
    renderList();
    selAll.checked = listSelectAll.checked;
    updateSelCount();
  });

  clearBtn.addEventListener("click", ()=>{
    qEl.value = ""; tEl.value=""; providerSel.value="";
    applyFilters();
  });

  delProv.addEventListener("change", updateSelCount);

  // Delete (PLEX only)
  document.getElementById("wl-delete").addEventListener("click", async ()=>{
    const provider = delProv.value || "";
    if (!provider || !selected.size) return;

    const keys = [...selected];
    const backup = items.slice();

    // optimistic remove
    const toRemove = new Set(keys);
    items = items.filter(it => !toRemove.has(normKey(it)));
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
      items = await fetchWatchlist();
      filtered = items.slice();
      render();
      updateMetrics();
      snackbar(`Deleted ${keys.length} from <b>${provider}</b>`);
    } catch (e) {
      items = backup; applyFilters();
      snackbar(`Delete failed`);
    } finally {
      document.getElementById("wl-delete").disabled = false;
    }
  });

  // Hide / Unhide
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

  // Keyboard: Delete key
  document.addEventListener("keydown", (e)=>{
    if (e.key === "Delete" && !document.getElementById("wl-delete").disabled) document.getElementById("wl-delete").click();
  }, true);

  // View mode toggle
  viewSel.addEventListener("change", ()=>{
    viewMode = viewSel.value === "list" ? "list" : "posters";
    render();
  });

  // ---------- init ----------
  (async function init(){
    viewMode = "posters";
    viewSel.value = "posters";
    postersEl.style.display = "";
    items = await fetchWatchlist();
    filtered = items.slice();
    render();
    updateMetrics();
  })();

  // Auto-refresh (visible tab only)
  const AUTO_REFRESH_MS = 60000; // 1 minute
  setInterval(async ()=>{
    if (document.visibilityState !== "visible") return;
    try {
      const list = await fetchWatchlist();
      items = list;
      applyFilters();
    } catch {}
  }, AUTO_REFRESH_MS);


  // Legacy adapter
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
