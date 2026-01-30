// assets/js/modals/about.js
const get = async (url) => { try { const r = await fetch(url,{cache:"no-store"}); return r.ok ? r.json() : null; } catch { return null; } };
const ABOUT_TTL_MS = 60_000;
let _aboutCacheAt = 0;
let _aboutCache = null;
let _aboutInflight = null;

async function loadAboutData(force=false){
  const now = Date.now();
  if(!force && _aboutCache && (now - _aboutCacheAt) < ABOUT_TTL_MS) return _aboutCache;
  if(_aboutInflight) return _aboutInflight;

  _aboutInflight = Promise.all([
    get("/api/version"),
    get("/api/modules/versions")
  ])
    .then(([ver, mods]) => ({ ver: ver || {}, mods: mods || {} }))
    .finally(() => { _aboutInflight = null; });

  const data = await _aboutInflight;
  _aboutCache = data;
  _aboutCacheAt = Date.now();
  return data;
}

function isNewer(a,b){ if(!a||!b) return false; const clean=s=>String(s).replace(/^v/i,"").split("-")[0];
  const A=clean(a).split(".").map(n=>parseInt(n,10)||0), B=clean(b).split(".").map(n=>parseInt(n,10)||0);
  for(let i=0;i<Math.max(A.length,B.length);i++){ const da=A[i]||0, db=B[i]||0; if(da!==db) return da>db; } return false; }

function rowsFromGroup(obj){
  const entries=Object.entries(obj||{});
  if(!entries.length) return `<div class="k">No providers</div><div class="key">—</div><div class="ver">—</div>`;
  const toName=(key)=>{ const last=String(key).split("_").pop()||key; return last?(last[0]+last.slice(1).toLowerCase()):last; };
  return entries.map(([key,ver])=>`
    <div class="k">${toName(key)}</div>
    <div class="key">${key}</div>
    <div class="ver">${ver||"—"}</div>
  `).join("");
}

function foldCard(id,title,rowsHTML){
  return `
    <div class="card fold" id="${id}">
      <button class="fold-head" type="button">
        <span class="dot" aria-hidden="true"></span>
        <span class="title">${title}</span>
        <span class="chev" aria-hidden="true">expand_more</span>
      </button>
      <div class="fold-body"><div class="rows">${rowsHTML||""}</div></div>
    </div>
  `;
}

function wireHostedFolds(root){
  root.querySelectorAll(".fold").forEach(f=>{
    const head=f.querySelector(".fold-head"), body=f.querySelector(".fold-body");
    const setH=(open)=>{ if(!body) return;
      if(open){ body.style.height="auto"; const h=body.getBoundingClientRect().height; body.style.height="0px"; requestAnimationFrame(()=>{ body.style.height=h+"px"; }); }
      else { body.style.height=body.getBoundingClientRect().height+"px"; requestAnimationFrame(()=>{ body.style.height="0px"; }); }
    };
    f.classList.remove("open"); body.style.height="0px";
    head?.addEventListener("click",()=>{ const willOpen=!f.classList.contains("open"); f.classList.toggle("open",willOpen); setH(willOpen); });
  });
}

async function renderHostedAbout(hostEl){
  // Parallel fetch + TTL cache
  const { ver, mods } = await loadAboutData(false);

  const info = { current: ver.current||ver.version, latest: ver.latest, html_url: ver.html_url || "https://github.com/cenodude/CrossWatch/releases" };
  const hasUpdate = isNewer(info.latest, info.current);

  hostEl.innerHTML = `
  <style>
    /* shell */
    #about-host{--w:760px;position:relative;display:flex;flex-direction:column;align-items:stretch;min-width:min(var(--w),92vw);color:#eaf1ff;background:rgba(13,17,23,.45);border:1px solid rgba(255,255,255,.08);border-radius:14px;box-shadow:0 20px 60px rgba(0,0,0,.55),inset 0 1px 0 rgba(255,255,255,.04);backdrop-filter:saturate(140%) blur(8px)}
    #about-host .cx-body{padding:12px 14px;max-height:65vh;overflow:auto}
    #about-host .foot{flex-shrink:0}
    /* head */
    #about-host .ab-head{display:flex;justify-content:space-between;gap:10px;padding:12px 14px;border-bottom:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.02))}
    #about-host .title-wrap{display:flex;gap:10px;align-items:center}
    #about-host .app-name{font-weight:800;letter-spacing:.3px}
    #about-host .app-sub{opacity:.75;font-size:12px}
    #about-host .actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
    #about-host .badge{display:inline-flex;gap:6px;align-items:center;padding:4px 9px;border-radius:999px;font-weight:700;background:linear-gradient(135deg,#7c4dff,#39c2ff);border:1px solid #7c4dff66;box-shadow:0 0 0 1px rgba(0,0,0,.2) inset}
    #about-host .badge .m{font-family:"Material Symbols Rounded"}
    #about-host .pill{display:inline-flex;gap:6px;align-items:center;padding:4px 9px;border-radius:999px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.14);font-weight:600}
    #about-host .ghost{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.14);padding:6px 10px;border-radius:10px;color:#fff;text-decoration:none}
    #about-host .ghost:hover{border-color:#ffffff40}
    /* update banner */
    #about-host .update-banner{display:flex;align-items:center;gap:10px;padding:8px 10px;margin:10px 14px 0;border-radius:10px;border:1px solid rgba(82,255,175,.35);background:linear-gradient(180deg,rgba(46,213,115,.16),rgba(46,213,115,.06));font-size:13px}
    #about-host .update-banner .m{font-family:"Material Symbols Rounded";flex:0 0 auto}
    #about-host .update-banner .t{font-weight:700}
    #about-host .update-banner .a{margin-left:auto}
    /* cards */
    #about-host .cards{display:grid;gap:10px}
    #about-host .card{background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.02));border:1px solid rgba(255,255,255,.10);border-radius:12px;overflow:hidden;box-shadow:0 6px 24px rgba(0,0,0,.25)}
    #about-host .intro{padding:12px 14px}
    #about-host .intro .lead{font-weight:800;margin-bottom:6px}
    /* folds */
    #about-host .fold-head{display:flex;align-items:center;gap:10px;width:100%;padding:10px 12px;font-weight:800;text-transform:uppercase;background:rgba(255,255,255,.03);border:0;border-bottom:1px solid rgba(255,255,255,.06);cursor:pointer}
    #about-host .fold-head .dot{width:8px;height:8px;border-radius:50%;background:radial-gradient(circle at 30% 30%,#7affc1,#39c2ff)}
    #about-host .fold-head .title{flex:1}
    #about-host .fold-head .chev{font-family:"Material Symbols Rounded";transition:transform .18s ease}
    #about-host .fold.open .fold-head .chev{transform:rotate(180deg)}
    #about-host .fold-body{height:0;overflow:hidden;opacity:0;transform:translateY(-2px);transition:height .18s ease,opacity .18s ease,transform .18s ease}
    #about-host .fold.open .fold-body{opacity:1;transform:none}
    #about-host .rows{display:grid;grid-template-columns:minmax(160px,1fr) minmax(140px,1fr) auto;gap:6px 10px;padding:8px 10px}
    #about-host .rows>*{padding:4px 0;border-bottom:1px dashed rgba(255,255,255,.06)}
    #about-host .rows>*:nth-last-child(-n+3){border-bottom:0}
    #about-host .k{opacity:.95}.key{opacity:.6}.ver{justify-self:end;font-variant-numeric:tabular-nums;opacity:.95}
    #about-host .note{padding:10px 12px;font-size:12.5px;opacity:.9}
    #about-host .foot{display:flex;justify-content:flex-end;padding:10px 14px;border-top:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.02),rgba(255,255,255,.01))}
    #about-host .btn{appearance:none;border:1px solid #7aa0ff66;border-radius:12px;padding:9px 14px;font-weight:800;background:linear-gradient(135deg,#4c7dff,#8ab0ff);color:#fff}
    /* Buy Me A Coffee */
    #about-host .bmc{position:absolute;left:12px;bottom:12px;z-index:5}
    #about-host .bmc a{display:inline-flex;align-items:center;gap:8px;padding:7px 10px;border-radius:12px;font-weight:700;font-size:12px;letter-spacing:.2px;background:linear-gradient(135deg,#1f2937,#0b1220);border:1px solid rgba(255,255,255,.10);box-shadow:0 4px 14px rgba(0,0,0,.35),inset 0 0 0 1px rgba(255,255,255,.05);color:#f8fafc;position:relative;overflow:hidden;text-decoration:none}
    #about-host .bmc a:hover{transform:translateY(-1px);box-shadow:0 8px 18px rgba(0,0,0,.45),inset 0 0 0 1px rgba(255,255,255,.08)}
    #about-host .bmc .sheen:after{content:"";position:absolute;inset:-40% -20% auto auto;width:110%;height:110%;transform:rotate(15deg) translateX(-60%);background:linear-gradient(110deg,transparent 40%,rgba(255,255,255,.10) 55%,transparent 70%);animation:abSheen 3.2s ease-in-out infinite}
    #about-host .bmc svg{width:16px;height:16px;display:block;filter:drop-shadow(0 2px 3px rgba(0,0,0,.35))}
    #about-host .bmc .txt{white-space:nowrap}
    #about-host .bmc .sub{opacity:.75;font-weight:600;margin-left:6px}
    #about-host .bmc .pulse{position:absolute;inset:-8px;border-radius:999px;pointer-events:none;box-shadow:0 0 0 0 rgba(255,193,7,.16);animation:abPulse 2.6s ease-out infinite}
    @keyframes abSheen{0%{transform:rotate(15deg) translateX(-80%)}100%{transform:rotate(15deg) translateX(40%)}}
    @keyframes abPulse{0%{box-shadow:0 0 0 0 rgba(255,193,7,.20)}100%{box-shadow:0 0 0 22px rgba(255,193,7,0)}}
    @media (max-width:980px){#about-host .bmc a{padding:6px 9px;font-size:11px;border-radius:11px} #about-host .bmc .sub{display:none}}
    @media (max-width:760px){#about-host .rows{grid-template-columns:1fr auto}}
    @media (max-width:560px){#about-host .bmc .txt{display:none}}
  </style>

  <div id="about-host">
    <div class="ab-head">
      <div class="title-wrap">
        <span class="material-symbols-rounded" aria-hidden="true">info</span>
        <div><div class="app-name">ABOUT</div><div class="app-sub">Version & modules</div></div>
      </div>
      <div class="actions">
        <span class="badge"><span class="m">bolt</span>v${info.current||"—"}</span>
        <span class="pill">Latest: ${info.latest||"—"}</span>
        <a id="about-releases" class="ghost" href="${info.html_url}" target="_blank" rel="noopener">Releases</a>
      </div>
    </div>

    ${hasUpdate?`
    <div class="update-banner">
      <span class="m" aria-hidden="true">new_releases</span>
      <div class="t">New version available: v${info.latest}</div>
      <a class="ghost a" href="${info.html_url}" target="_blank" rel="noopener">Get update</a>
    </div>`:""}

    <div class="cx-body">
      <div class="cards">
        <div class="card intro">
          <div class="lead">CrossWatch in a nutshell</div>
          <div class="p">A fast, local-first sync engine for Plex, Jellyfin, Emby, MDBList, AniList Tautulli, TMDB, SIMKL, and Trakt. Clean UI, solid planner, reliable runs.</div>
        </div>
        ${foldCard("fold-auth","Authentication Providers", rowsFromGroup(mods.groups?.AUTH))}
        ${foldCard("fold-sync","Synchronization Providers", rowsFromGroup(mods.groups?.SYNC))}
        <div class="card">
          <div class="note">
            <b>Disclaimer.</b> This is an independent, community-maintained project and is not affiliated with, endorsed by, or sponsored by Plex, Emby, Jellyfin, Trakt, SIMKL, TMDb, Tautulli, AniList or MDBList. Use at your own risk.
            <ul style="margin:.5em 0 0 1.25em;">
              <li>All product names, logos, and brands are property of their respective owners and used for identification only.</li>
              <li>Interacts with third-party services; you are responsible for complying with their Terms of Use and API rules.</li>
              <li>Provided “as is,” without warranties or guarantees.</li>
            </ul>
          </div>
        </div>
      </div>
    </div>

    <div class="foot"><button class="btn" data-x="close">OK</button></div>

    <div class="bmc">
      <a href="https://buymeacoffee.com/cenodude" target="_blank" rel="noopener" aria-label="Buy me a coffee">
        <span class="sheen"></span><span class="pulse"></span>
        <svg viewBox="0 0 24 24" aria-hidden="true"><path fill="#FCD34D" d="M5 10h11a3 3 0 0 0 0-6H5v6Zm13-4a1 1 0 0 1 0 2h-2V6h2Z"/><path fill="#D97706" d="M4 10h13c-.2 3.9-1.9 7-6.5 7S4.2 13.9 4 10Z"/><path fill="#0EA5E9" d="M3 19.5a.5.5 0 0 1 .5-.5h14a.5.5 0 0 1 0 1h-14a.5.5 0 0 1-.5-.5Z"/><path fill="#F59E0B" d="M9 3c0 .8-.6 1.2-.9 1.7S8 5.4 8 6h-1c0-.8.6-1.2.9-1.7S8 3.6 8 3h1Zm3 0c0 .8-.6 1.2-.9 1.7S11 5.4 11 6h-1c0-.8.6-1.2.9-1.7S11 3.6 11 3h1Zm3 0c0 .8-.6 1.2-.9 1.7S14 5.4 14 6h-1c0-.8.6-1.2.9-1.7S14 3.6 14 3h1Z"/></svg>
        <span class="txt">Buy me a coffee</span><span class="sub">/ cenodude</span>
      </a>
    </div>
  </div>
  `;

  const shell = hostEl.closest(".cx-modal-shell");
  if(shell){ shell.style.height="auto"; shell.style.minHeight="0"; shell.style.maxHeight="none"; shell.style.width="auto"; shell.style.maxWidth="none"; shell.style.display="inline-block"; }

  wireHostedFolds(hostEl);
  hostEl.querySelector(".ab-head")?.addEventListener("pointerdown",e=>e.stopPropagation(),true);
  hostEl.querySelector(".cx-body")?.addEventListener("pointerdown",e=>e.stopPropagation(),true);
  hostEl.querySelector("#about-releases")?.addEventListener("pointerdown",e=>e.stopPropagation(),true);
  hostEl.querySelector('[data-x="close"]')?.addEventListener("click",()=>window.cxCloseModal?.());
}

export default {
  async mount(hostEl){ if(!hostEl) return; await renderHostedAbout(hostEl); },
  unmount(){ /* noop */ }
};
