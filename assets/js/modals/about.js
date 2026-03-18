/* assets/js/modals/about.js */
/* CrossWatch - about modal component */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const UPDATE_ENDPOINT = "/api/update";
const MODULES_ENDPOINT = "/api/modules/versions";
const RELEASES_URL = "https://github.com/cenodude/CrossWatch/releases";
const TTL = 60_000;

const _cwV = (() => {
  try { return new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__ || Date.now(); }
  catch { return window.__CW_VERSION__ || Date.now(); }
})();

const _cwVer = (u) => u + (u.includes("?") ? "&" : "?") + "v=" + encodeURIComponent(String(_cwV));

const { getJson } = await import(_cwVer("./core/net.js"));
const { escapeHtml, setModalShellInline } = await import(_cwVer("./core/app-auth-setup.js"));

const cache = { at: 0, data: null, inflight: null };

function _norm(v) {
  return String(v || "").replace(/^v/i, "").trim();
}

function _cmp(a, b) {
  const pa = _norm(a).split(".").map((n) => parseInt(n, 10) || 0);
  const pb = _norm(b).split(".").map((n) => parseInt(n, 10) || 0);
  for (let i = 0; i < Math.max(pa.length, pb.length); i += 1) {
    const da = pa[i] || 0;
    const db = pb[i] || 0;
    if (da !== db) return da > db ? 1 : -1;
  }
  return 0;
}

function _providerName(key) {
  const tail = String(key || "").split("_").pop() || key || "-";
  return tail ? tail.charAt(0).toUpperCase() + tail.slice(1).toLowerCase() : "-";
}

function _providerRows(group) {
  const rows = Object.entries(group || {});
  if (!rows.length) {
    return `
      <div class="r">
        <b>No providers</b>
        <span>-</span>
        <em>-</em>
      </div>
    `;
  }
  return rows.map(([key, value]) => `
    <div class="r">
      <b>${escapeHtml(_providerName(key))}</b>
      <span>${escapeHtml(key)}</span>
      <em>${escapeHtml(value || "-")}</em>
    </div>
  `).join("");
}

function _fold(title, body, open = false) {
  return `
    <details class="card fold"${open ? " open" : ""}>
      <summary>
        <span>${escapeHtml(title)}</span>
        <i class="material-symbols-rounded" aria-hidden="true">expand_more</i>
      </summary>
      <div class="rows">${body}</div>
    </details>
  `;
}

async function loadAbout(force = false) {
  const now = Date.now();
  if (!force && cache.data && now - cache.at < TTL) return cache.data;
  if (cache.inflight) return cache.inflight;

  cache.inflight = Promise.all([
    getJson(UPDATE_ENDPOINT, { cache: "no-store" }).catch(() => ({})),
    getJson(MODULES_ENDPOINT, { cache: "no-store" }).catch(() => ({})),
  ])
    .then(([update, mods]) => ({ update: update || {}, mods: mods || {} }))
    .finally(() => {
      cache.inflight = null;
    });

  cache.data = await cache.inflight;
  cache.at = Date.now();
  return cache.data;
}

function _versionInfo(update = {}) {
  const current = _norm(update.current_version || update.current || window.__CW_VERSION__ || "0.0.0");
  const latest = _norm(update.latest_version || update.latest || current);
  const hasUpdate = typeof update.update_available === "boolean"
    ? update.update_available
    : (_cmp(latest, current) > 0);
  const htmlUrl = String(update.html_url || update.url || RELEASES_URL).trim() || RELEASES_URL;
  const publishedAt = String(update.published_at || "").trim();
  return { current, latest, hasUpdate, htmlUrl, publishedAt };
}

function view(info, mods, logo) {
  const latestChip = info.latest ? `Latest v${escapeHtml(info.latest)}` : "Latest unavailable";
  const publishedChip = info.publishedAt
    ? `<span class="chip subtle">${escapeHtml(info.publishedAt.slice(0, 10))}</span>`
    : "";

  return `
    <style>
      #about-host{--w:760px;position:relative;overflow:hidden;min-width:min(var(--w),94vw);max-width:94vw;color:#eaf0ff;border-radius:18px;
        border:1px solid rgba(255,255,255,.08);
        background:
          radial-gradient(900px circle at 18% 18%, rgba(150,70,255,.22), transparent 55%),
          radial-gradient(900px circle at 92% 10%, rgba(60,140,255,.18), transparent 55%),
          radial-gradient(800px circle at 55% 110%, rgba(60,255,215,.08), transparent 60%),
          rgba(7,8,11,.92);
        box-shadow:0 30px 90px rgba(0,0,0,.70), inset 0 1px 0 rgba(255,255,255,.04);
        backdrop-filter:saturate(135%) blur(10px)
      }
      #about-host:before{content:"";position:absolute;inset:-120px;pointer-events:none;
        background:conic-gradient(from 180deg at 50% 50%, rgba(150,70,255,0), rgba(150,70,255,.28), rgba(60,140,255,.22), rgba(60,255,215,.08), rgba(150,70,255,0));
        filter:blur(90px);opacity:.34;transform:translate3d(0,0,0)
      }
      #about-host *{box-sizing:border-box}
      #about-host a,#about-host button{font:inherit}
      #about-host .head{position:relative;display:flex;align-items:center;gap:12px;padding:14px 16px;border-bottom:1px solid rgba(255,255,255,.08);
        background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.01))
      }
      #about-host .logoWrap{width:44px;height:44px;border-radius:14px;display:grid;place-items:center;flex:0 0 auto;
        background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);
        box-shadow:0 12px 30px rgba(0,0,0,.40), inset 0 1px 0 rgba(255,255,255,.04)
      }
      #about-host .logo{width:30px;height:30px;opacity:.95;filter:drop-shadow(0 10px 16px rgba(0,0,0,.45))}
      #about-host .title{font-weight:950;letter-spacing:.2px;font-size:15px;line-height:1.1;text-transform:uppercase;opacity:.90}
      #about-host .sub{opacity:.72;font-size:12px;margin-top:2px}
      #about-host .actions{margin-left:auto;display:flex;flex-wrap:wrap;justify-content:flex-end;gap:8px}
      #about-host .chip,#about-host .link,#about-host .btn{display:inline-flex;align-items:center;justify-content:center;gap:8px;min-height:34px;padding:0 12px;border-radius:999px;text-decoration:none;white-space:nowrap}
      #about-host .chip{font-weight:900;font-size:12px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.04)}
      #about-host .chip.accent{border-color:rgba(150,70,255,.35);background:linear-gradient(135deg,rgba(150,70,255,.28),rgba(60,140,255,.18))}
      #about-host .chip.subtle{opacity:.76}
      #about-host .link,#about-host .btn{color:#eaf0ff;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.04);transition:transform .16s ease,filter .16s ease,border-color .16s ease}
      #about-host .link:hover,#about-host .btn:hover{transform:translateY(-1px);filter:brightness(1.06);border-color:rgba(150,70,255,.28)}
      #about-host .body{position:relative;padding:16px 16px 8px 16px;max-height:72vh;overflow:auto;scrollbar-width:thin;scrollbar-color:rgba(150,70,255,.88) rgba(255,255,255,.08)}
      #about-host .body::-webkit-scrollbar{width:12px}
      #about-host .body::-webkit-scrollbar-track{background:rgba(255,255,255,.06);border-radius:999px}
      #about-host .body::-webkit-scrollbar-thumb{background:linear-gradient(180deg,rgba(170,92,255,.96),rgba(120,72,255,.88));border-radius:999px;border:2px solid rgba(10,12,20,.42)}
      #about-host .body::-webkit-scrollbar-thumb:hover{background:linear-gradient(180deg,rgba(184,112,255,.98),rgba(136,86,255,.92))}
      #about-host .card{display:block;padding:12px;border-radius:14px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.08);box-shadow:0 10px 30px rgba(0,0,0,.32);margin-bottom:10px}
      #about-host .badge{display:inline-flex;align-items:center;gap:8px;padding:6px 10px;border-radius:999px;background:rgba(0,0,0,.24);border:1px solid rgba(255,255,255,.08)}
      #about-host .badge .dot{width:8px;height:8px;border-radius:999px;background:rgba(150,70,255,.90);box-shadow:0 0 0 4px rgba(150,70,255,.18)}
      #about-host .headline{font-weight:950;font-size:20px;line-height:1.15;margin:10px 0 8px}
      #about-host .lede{opacity:.84;max-width:72ch;line-height:1.5}
      #about-host .eyebrow{margin:0 0 6px;color:rgba(228,234,255,.54);font-size:11px;font-weight:900;letter-spacing:.14em;text-transform:uppercase}
      #about-host .update{display:grid;grid-template-columns:auto 1fr auto;align-items:center;gap:12px;border-color:rgba(150,70,255,.22);
        background:linear-gradient(135deg,rgba(150,70,255,.14),rgba(60,140,255,.08))
      }
      #about-host .update .material-symbols-rounded{font-size:24px}
      #about-host .update .h{font-weight:950}
      #about-host .update .p{opacity:.82;margin-top:4px;line-height:1.45}
      #about-host .fold{padding:0;overflow:hidden}
      #about-host .fold summary{cursor:pointer;list-style:none;padding:14px 16px;font-weight:950;display:flex;align-items:center;justify-content:space-between;gap:8px;background:rgba(255,255,255,.02)}
      #about-host .fold summary::-webkit-details-marker{display:none}
      #about-host .fold summary span{font-size:12.5px;letter-spacing:.08em;text-transform:uppercase}
      #about-host .fold i{font-size:18px;opacity:.82;transition:transform .18s ease,opacity .18s ease}
      #about-host .fold[open] i{transform:rotate(180deg);opacity:1}
      #about-host .rows{padding:2px 16px 12px}
      #about-host .r{display:grid;grid-template-columns:minmax(110px,1fr) minmax(150px,1fr) auto;gap:8px;align-items:center;min-height:34px;border-top:1px solid rgba(255,255,255,.06)}
      #about-host .r:first-child{border-top:0}
      #about-host .r b{font-size:12.5px}
      #about-host .r span{color:rgba(214,223,246,.56);font:11.5px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
      #about-host .r em{justify-self:end;font-style:normal;font-size:12.5px;opacity:.88}
      #about-host .discBody{opacity:.84;line-height:1.5}
      #about-host ul{display:grid;gap:7px;padding-left:18px;margin:10px 0 0}
      #about-host .helpLink{display:flex;align-items:center;justify-content:space-between;gap:14px;margin-top:10px;padding:14px 16px;border-radius:14px;text-decoration:none;color:#eef3ff;
        background:linear-gradient(135deg,rgba(150,70,255,.18),rgba(60,140,255,.14));border:1px solid rgba(150,70,255,.22);
        box-shadow:0 14px 34px rgba(0,0,0,.24), inset 0 1px 0 rgba(255,255,255,.04);transition:transform .16s ease,filter .16s ease,border-color .16s ease
      }
      #about-host .helpLink:hover{transform:translateY(-1px);filter:brightness(1.06);border-color:rgba(150,70,255,.32)}
      #about-host .helpCopy{display:grid;gap:4px}
      #about-host .helpEyebrow{font-size:11px;font-weight:900;letter-spacing:.08em;text-transform:uppercase;opacity:.72}
      #about-host .helpTitle{font-size:16px;font-weight:950;line-height:1.15}
      #about-host .helpSub{font-size:12.5px;line-height:1.4;opacity:.8}
      #about-host .helpIcon{width:42px;height:42px;border-radius:12px;display:grid;place-items:center;flex:0 0 auto;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08)}
      #about-host .helpIcon .material-symbols-rounded{font-size:22px}
      #about-host .foot{position:relative;display:flex;justify-content:space-between;align-items:center;gap:12px;padding:12px 16px;border-top:1px solid rgba(255,255,255,.08);
        background:linear-gradient(180deg,rgba(255,255,255,.02),rgba(255,255,255,.01))
      }
      #about-host .support{color:#eef3ff}
      #about-host .btn.primary{border-color:rgba(150,70,255,.35);background:linear-gradient(135deg,rgba(150,70,255,.92),rgba(60,140,255,.82));box-shadow:0 16px 50px rgba(0,0,0,.38)}
      @media (max-width:760px){
        #about-host .head,#about-host .foot{flex-direction:column;align-items:stretch}
        #about-host .actions{margin-left:0;justify-content:flex-start}
        #about-host .update{grid-template-columns:1fr}
      }
      @media (max-width:560px){
        #about-host .actions{display:grid;grid-template-columns:1fr 1fr;gap:8px}
        #about-host .actions>*:last-child{grid-column:1 / -1}
        #about-host .chip,#about-host .link,#about-host .btn{width:100%}
        #about-host .r{grid-template-columns:minmax(0,1fr) auto}
        #about-host .r span{display:none}
      }
    </style>
    <div id="about-host" role="dialog" aria-label="About CrossWatch">
      <div class="head">
        <div class="logoWrap" aria-hidden="true"><img class="logo" src="${escapeHtml(logo)}" alt="" /></div>
        <div>
          <div class="title">About CrossWatch</div>
          <div class="sub">Version, modules, and useful project info</div>
        </div>
        <div class="actions">
          <span class="chip accent"><span class="material-symbols-rounded" aria-hidden="true">bolt</span>Engine v${escapeHtml(info.current || "-")}</span>
          <span class="chip">${latestChip}</span>
          ${publishedChip}
          <a class="link" href="${escapeHtml(info.htmlUrl)}" target="_blank" rel="noopener noreferrer">Releases</a>
        </div>
      </div>
      <div class="body">
        ${info.hasUpdate ? `
          <section class="card update">
            <span class="material-symbols-rounded" aria-hidden="true">new_releases</span>
            <div>
              <div class="h">Update available: v${escapeHtml(info.latest || info.current || "-")}</div>
              <div class="p">You are on v${escapeHtml(info.current || "-")}. Open the latest release notes when you are ready to update.</div>
            </div>
            <a class="link" href="${escapeHtml(info.htmlUrl)}" target="_blank" rel="noopener noreferrer">Open release</a>
          </section>
        ` : ""}
        <section class="card">
          <div class="badge"><span class="dot" aria-hidden="true"></span><span style="font-weight:900">Local-first sync</span></div>
          <div class="headline">Fast runs with good controls</div>
          <div class="lede">CrossWatch syncs Plex, Jellyfin, Emby, MDBList, AniList, Tautulli, TMDb, SIMKL, and Trakt. Keep it behind your network edge and avoid exposing it directly to the internet.</div>
        </section>
        ${_fold("Authentication providers", _providerRows(mods.groups?.AUTH))}
        ${_fold("Synchronization providers", _providerRows(mods.groups?.SYNC))}
        <section class="card">
          <div class="eyebrow">Disclaimer</div>
          <div class="discBody">
            <div>Independent community project. Not affiliated with, endorsed by, or sponsored by Plex, Emby, Jellyfin, Trakt, SIMKL, TMDb, Tautulli, AniList, or MDBList.</div>
            <ul>
              <li>Names, logos, and brands belong to their owners and are used for identification only.</li>
              <li>Third-party APIs have their own rules. Use them without getting yourself banned.</li>
              <li>Provided as-is, without any warranties. Backups still beat regret.</li>
            </ul>
          </div>
        </section>
        <section class="card">
          <div class="eyebrow">Need Help?</div>
          <a class="helpLink" href="https://wiki.crosswatch.app/" target="_blank" rel="noopener noreferrer">
            <span class="helpCopy">
              <span class="helpEyebrow">Documentation</span>
              <span class="helpTitle">Open the CrossWatch Wiki</span>
              <span class="helpSub">Setup guides, upgrade notes, and troubleshooting in one place.</span>
            </span>
            <span class="helpIcon" aria-hidden="true"><span class="material-symbols-rounded">menu_book</span></span>
          </a>
        </section>
      </div>
      <div class="foot">
        <a class="link support" href="https://buymeacoffee.com/cenodude" target="_blank" rel="noopener noreferrer">Buy me a coffee / cenodude</a>
        <button class="btn primary" type="button" data-close>Close</button>
      </div>
    </div>
  `;
}

async function render(host) {
  const { update, mods } = await loadAbout();
  const info = _versionInfo(update);
  const crossWatchLogo = window.CW?.ProviderMeta?.logoPath?.("crosswatch") || "/assets/img/CROSSWATCH.svg";

  host.innerHTML = view(info, mods, crossWatchLogo);

  const shell = host.closest(".cx-modal-shell");
  setModalShellInline(shell);

  host.addEventListener("pointerdown", (e) => e.stopPropagation(), true);
  host.querySelector("[data-close]")?.addEventListener("click", () => window.cxCloseModal?.());
}

export default {
  async mount(host) {
    if (host) await render(host);
  },
  unmount() {},
};
