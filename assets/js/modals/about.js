/* assets/js/modals/about.js */
/* Refactored */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
const j = async (u) => {
  try {
    const r = await fetch(u, { cache: "no-store" });
    return r.ok ? r.json() : null;
  } catch {
    return null;
  }
};

const TTL = 60_000;
const cache = { at: 0, data: null, inflight: null };
const cleanVer = (v) => String(v || "").replace(/^v/i, "").split("-")[0].split(".").map((n) => parseInt(n, 10) || 0);
const newer = (a, b) => cleanVer(a).some((n, i) => n !== (cleanVer(b)[i] || 0) && n > (cleanVer(b)[i] || 0));
const nameOf = (k) => { const s = String(k || "").split("_").pop() || k || "—"; return s[0] ? s[0] + s.slice(1).toLowerCase() : s; };

async function loadAbout(force = false) {
  const now = Date.now();
  if (!force && cache.data && now - cache.at < TTL) return cache.data;
  if (cache.inflight) return cache.inflight;
  cache.inflight = Promise.all([j("/api/version"), j("/api/modules/versions")])
    .then(([ver, mods]) => ({ ver: ver || {}, mods: mods || {} }))
    .finally(() => { cache.inflight = null; });
  cache.data = await cache.inflight;
  cache.at = Date.now();
  return cache.data;
}

function providerRows(group) {
  const rows = Object.entries(group || {});
  return rows.length
    ? rows.map(([k, v]) => `<div class="r"><b>${nameOf(k)}</b><span>${k}</span><em>${v || "—"}</em></div>`).join("")
    : '<div class="r"><b>No providers</b><span>—</span><em>—</em></div>';
}

function fold(title, body, open = false) {
  return `<details class="card fold"${open ? " open" : ""}><summary><span>${title}</span><i class="material-symbols-rounded">expand_more</i></summary><div class="rows">${body}</div></details>`;
}

function view(info, mods) {
  const hasUpdate = newer(info.latest, info.current);
  return `
  <style>
    #about-host{--bg:rgba(9,13,19,.92);--bg2:rgba(17,23,31,.82);--line:rgba(205,216,255,.11);--line2:rgba(205,216,255,.16);--txt:#edf2ff;--muted:rgba(231,238,255,.68);--soft:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.022));--soft2:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.018));--accent:rgba(139,154,255,.18);display:flex;flex-direction:column;width:min(640px,92vw);color:var(--txt);background:radial-gradient(circle at top left,rgba(112,92,255,.10),transparent 36%),linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015)),var(--bg);border:1px solid var(--line);border-radius:18px;box-shadow:0 28px 70px rgba(0,0,0,.5),inset 0 1px 0 rgba(255,255,255,.04);backdrop-filter:blur(14px) saturate(130%);overflow:hidden}
    #about-host *{box-sizing:border-box} #about-host a,#about-host button{font:inherit} #about-host .head,#about-host .foot,.card,summary,.chip,.link,.btn{display:flex;align-items:center}
    #about-host .head,#about-host .foot{justify-content:space-between;gap:10px;padding:12px 14px;background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.015))}
    #about-host .head{border-bottom:1px solid rgba(255,255,255,.06)} #about-host .foot{border-top:1px solid rgba(255,255,255,.06)}
    #about-host .title{display:flex;align-items:center;gap:10px;min-width:0} #about-host .icon{display:grid;place-items:center;width:34px;height:34px;border-radius:12px;background:var(--soft);border:1px solid rgba(255,255,255,.09)}
    #about-host .title b{display:block;font-size:15px;letter-spacing:.2px} #about-host .title small{display:block;margin-top:2px;color:var(--muted);font-size:12px}
    #about-host .actions{display:flex;flex-wrap:wrap;justify-content:flex-end;gap:8px} #about-host .chip,#about-host .link,#about-host .btn{gap:7px;min-height:32px;padding:0 11px;border-radius:999px;text-decoration:none;white-space:nowrap}
    #about-host .chip{background:var(--soft);border:1px solid rgba(255,255,255,.1);font-weight:700} #about-host .chip.-a{background:linear-gradient(180deg,rgba(132,147,255,.22),rgba(132,147,255,.12));border-color:rgba(148,163,255,.22)}
    #about-host .link,#about-host .btn{justify-content:center;color:var(--txt);background:rgba(255,255,255,.045);border:1px solid rgba(255,255,255,.1);transition:background .16s ease,border-color .16s ease,transform .16s ease}
    #about-host .btn{padding-inline:14px;background:linear-gradient(180deg,rgba(255,255,255,.08),rgba(255,255,255,.03))} #about-host .link:hover,#about-host .btn:hover{background:rgba(255,255,255,.065);border-color:var(--line2)}
    #about-host .body{display:grid;gap:10px;padding:12px;max-height:min(68vh,760px);overflow:auto} #about-host .card{display:block;border:1px solid var(--line);border-radius:15px;background:var(--soft);box-shadow:0 10px 26px rgba(0,0,0,.22),inset 0 1px 0 rgba(255,255,255,.03)}
    #about-host .intro{padding:12px} #about-host .eyebrow{margin:0 0 6px;color:rgba(228,234,255,.54);font-size:11px;font-weight:800;letter-spacing:.14em;text-transform:uppercase} #about-host .lead{margin:0 0 6px;font-size:14px;font-weight:800;line-height:1.35} #about-host p,#about-host li{margin:0;color:var(--muted);font-size:12.5px;line-height:1.5}
    #about-host .stats{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:10px} #about-host .mini{padding:9px 10px;border-radius:12px;background:var(--soft2);border:1px solid rgba(255,255,255,.07)} #about-host .mini b{display:block;font-size:11px;margin-bottom:4px}
    #about-host .update{display:flex;align-items:center;gap:10px;padding:10px 12px;border:1px solid rgba(130,194,158,.2);border-radius:14px;background:linear-gradient(180deg,rgba(101,171,135,.12),rgba(101,171,135,.05))} #about-host .update b{font-size:13px} #about-host .update .link{margin-left:auto}
    #about-host .fold{overflow:hidden} #about-host .fold summary{justify-content:space-between;gap:10px;list-style:none;padding:11px 12px;cursor:pointer;background:rgba(255,255,255,.025)} #about-host .fold summary::-webkit-details-marker{display:none}
    #about-host .fold summary span{font-size:12.5px;font-weight:800;letter-spacing:.06em;text-transform:uppercase} #about-host .fold i{font-size:18px;opacity:.8;transition:transform .18s ease,opacity .18s ease} #about-host .fold[open] i{transform:rotate(180deg);opacity:1}
    #about-host .rows{padding:6px 12px 10px} #about-host .r{display:grid;grid-template-columns:minmax(110px,1fr) minmax(140px,1fr) auto;gap:8px;align-items:center;min-height:28px;border-top:1px dashed rgba(255,255,255,.06)} #about-host .r:first-child{border-top:0}
    #about-host .r b{font-size:12.5px} #about-host .r span{color:rgba(214,223,246,.56);font:11.5px ui-monospace,SFMono-Regular,Menlo,monospace} #about-host .r em{justify-self:end;font-style:normal;font-size:12.5px}
    #about-host .disclaimer{padding:12px} #about-host ul{display:grid;gap:7px;padding-left:18px;margin:10px 0 0}
    #about-host .support{color:rgba(240,245,255,.88)} #about-host .note{color:rgba(219,228,249,.56);font-size:11.5px}
    @media (max-width:720px){#about-host .head,#about-host .foot{flex-direction:column;align-items:stretch}#about-host .actions{justify-content:flex-start}#about-host .update{flex-wrap:wrap}#about-host .update .link{margin-left:0}#about-host .r{grid-template-columns:minmax(0,1fr) auto}#about-host .r span{display:none}}
    @media (max-width:520px){#about-host .stats{grid-template-columns:1fr}#about-host .actions{display:grid;grid-template-columns:1fr 1fr;gap:8px}#about-host .actions>*:last-child{grid-column:1/-1}#about-host .link,#about-host .btn,#about-host .chip{width:100%}#about-host .note{display:none}}
  </style>
  <div id="about-host">
    <header class="head">
      <div class="title">
        <div class="icon"><span class="material-symbols-rounded">info</span></div>
        <div><b>About CrossWatch</b><small>Version, modules, and some useful info</small></div>
      </div>
      <div class="actions">
        <span class="chip -a"><span class="material-symbols-rounded">bolt</span>v${info.current || "—"}</span>
        <span class="chip">Latest ${info.latest || "—"}</span>
        <a class="link" data-releases href="${info.html_url}" target="_blank" rel="noopener">Releases</a>
      </div>
    </header>
    <div class="body">
      ${hasUpdate ? `<section class="update"><span class="material-symbols-rounded">new_releases</span><div><b>Update available: v${info.latest}</b><p>You are on v${info.current || "—"}. Not broken. Just behind.</p></div><a class="link" href="${info.html_url}" target="_blank" rel="noopener">Open release</a></section>` : ""}
      <section class="card intro">
        <div class="eyebrow">Local-first sync</div>
        <div class="lead">Fast runs with good controls</div>
        <p>CrossWatch syncs Plex, Jellyfin, Emby, MDBList, AniList, Tautulli, TMDb, SIMKL, and Trakt. Keep it behind your network edge. Dont expose to the internet.</p>
        <div class="stats">
          <div class="mini"><b>Current</b><p>v${info.current || "—"}</p></div>
          <div class="mini"><b>Best practice</b><p>Finish auth providers and sync direction before enabling automation.</p></div>
        </div>
      </section>
      ${fold("Authentication providers", providerRows(mods.groups?.AUTH))}
      ${fold("Synchronization providers", providerRows(mods.groups?.SYNC))}
      <section class="card disclaimer">
        <div class="eyebrow">Disclaimer</div>
        <p>Independent community project. Not affiliated with, endorsed by, or sponsored by Plex, Emby, Jellyfin, Trakt, SIMKL, TMDb, Tautulli, AniList, or MDBList.</p>
        <ul>
          <li>Names, logos, and brands belong to their owners and are used for identification only.</li>
          <li>Third-party APIs have their own rules. Use them without getting yourself banned.</li>
          <li>Provided as-is, without any warranties. Backups still beat regret.</li>
        </ul>
      </section>
    </div>
    <footer class="foot">
      <a class="link support" href="https://buymeacoffee.com/cenodude" target="_blank" rel="noopener">Buy me a coffee / cenodude</a>
      <button class="btn" type="button" data-close>Close</button>
    </footer>
  </div>`;
}

async function render(host) {
  const { ver, mods } = await loadAbout();
  host.innerHTML = view({ current: ver.current || ver.version, latest: ver.latest, html_url: ver.html_url || "https://github.com/cenodude/CrossWatch/releases" }, mods);

  const shell = host.closest(".cx-modal-shell");
  if (shell) Object.assign(shell.style, { width: "auto", maxWidth: "none", height: "auto", minHeight: "0", maxHeight: "none", display: "inline-block" });

  host.addEventListener("pointerdown", (e) => e.stopPropagation(), true);
  host.querySelector("[data-close]")?.addEventListener("click", () => window.cxCloseModal?.());
}

export default {
  async mount(host) {
    if (host) await render(host);
  },
  unmount() {},
};
