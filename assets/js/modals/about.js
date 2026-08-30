/* assets/js/modals/about.js */
/* CrossWatch - standalone about modal */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const UPDATE_ENDPOINT = "/api/update";
const MODULES_ENDPOINT = "/api/modules/versions";
const RELEASES_URL = "https://github.com/cenodude/CrossWatch/releases";
const WIKI_URL = "https://wiki.crosswatch.app/";
const COMMUNITY_URL = "https://github.com/cenodude/CrossWatch/discussions";
const SUPPORT_URL = "https://buymeacoffee.com/cenodude";
const TTL = 60_000;

const _cwV = (() => {
  try { return new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__ || Date.now(); }
  catch { return window.__CW_VERSION__ || Date.now(); }
})();

const _cwVer = (u) => u + (u.includes("?") ? "&" : "?") + "v=" + encodeURIComponent(String(_cwV));

const { getJson } = await import(_cwVer("./core/net.js"));
const { escapeHtml } = await import(_cwVer("./core/app-auth-setup.js"));

const cache = { at: 0, data: null, inflight: null };
let activeModal = null;

const ABOUT_CSS = `
:host{all:initial}
*,*::before,*::after{box-sizing:border-box}
.about-backdrop{position:fixed;inset:0;z-index:30050;display:grid;place-items:center;padding:4px;background:rgba(2,5,10,.66);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#edf4ff}
.about-panel{position:relative;width:min(940px,calc(100vw - 8px));height:auto;max-height:min(910px,calc(100vh - 8px));display:grid;grid-template-rows:auto minmax(0,1fr);overflow:hidden;border:1px solid rgba(142,162,198,.24);border-radius:10px;background:radial-gradient(520px circle at 0% 0%,rgba(64,131,255,.15),transparent 58%),radial-gradient(460px circle at 100% 8%,rgba(139,92,246,.13),transparent 56%),linear-gradient(145deg,#07101b,#0b1421 58%,#09121d);box-shadow:0 28px 74px rgba(0,0,0,.58),inset 0 1px 0 rgba(255,255,255,.045);outline:none}
.about-x{position:absolute;top:14px;right:14px;z-index:4;display:grid;place-items:center;width:30px;height:30px;padding:0;border:0;background:transparent;color:#aab7cc;cursor:pointer}
.about-x:hover{color:#f5f8ff}
.material-symbols-rounded{font-family:"Material Symbols Rounded";font-weight:normal;font-style:normal;font-size:20px;line-height:1;letter-spacing:normal;text-transform:none;display:inline-block;white-space:nowrap;word-wrap:normal;direction:ltr;font-feature-settings:"liga";-webkit-font-feature-settings:"liga";-webkit-font-smoothing:antialiased;font-variation-settings:"FILL" 0,"wght" 560,"GRAD" 0,"opsz" 24}
.about-head{display:grid;grid-template-columns:auto minmax(140px,1fr) auto;align-items:center;gap:12px;padding:14px 58px 14px 20px;background:rgba(255,255,255,.015)}
.about-logo-wrap{display:grid;place-items:center;width:42px;height:42px;border:1px solid rgba(142,162,198,.26);border-radius:12px;background:linear-gradient(180deg,rgba(255,255,255,.07),rgba(255,255,255,.025));box-shadow:inset 0 1px 0 rgba(255,255,255,.06)}
.about-logo{width:28px;height:28px;object-fit:contain;filter:drop-shadow(0 8px 14px rgba(0,0,0,.38))}
.about-heading{min-width:0}
.about-title{font-size:18px;line-height:1.1;font-weight:800;color:#f4f7ff;white-space:nowrap}
.about-sub{margin-top:8px;font-size:13px;line-height:1.2;font-weight:600;color:#aab7cc;white-space:nowrap}
.about-actions{display:flex;align-items:center;justify-content:flex-end;gap:10px;min-width:0}
.chip,.about-link{display:inline-flex;align-items:center;justify-content:center;gap:7px;min-height:34px;padding:0 13px;border:1px solid rgba(142,162,198,.26);border-radius:999px;background:rgba(255,255,255,.035);color:#edf4ff;text-decoration:none;white-space:nowrap;font-size:12px;line-height:1;font-weight:760}
.chip .material-symbols-rounded,.about-link .material-symbols-rounded{font-size:17px}
.chip.accent{min-width:178px;border-color:rgba(139,92,246,.48);background:linear-gradient(135deg,rgba(98,52,190,.78),rgba(101,77,206,.55));box-shadow:inset 0 1px 0 rgba(255,255,255,.08),0 12px 28px rgba(98,52,190,.2)}
.chip.subtle{color:#d4deee}
.about-link:hover{border-color:rgba(142,162,198,.45);background:rgba(255,255,255,.06)}
.ext{font-size:15px}
.about-body{min-height:0;max-height:calc(100vh - 72px);overflow:auto;padding:0 28px 18px;scrollbar-width:thin;scrollbar-color:rgba(142,162,198,.5) transparent}
.about-body::-webkit-scrollbar{width:9px}
.about-body::-webkit-scrollbar-track{background:transparent}
.about-body::-webkit-scrollbar-thumb{border:2px solid rgba(8,16,27,.88);border-radius:999px;background:rgba(142,162,198,.52)}
.about-grid{display:block}
.about-main{display:grid;gap:14px;align-content:start}
.about-card,.about-fold,.help-section{border:1px solid rgba(142,162,198,.22);border-radius:8px;background:linear-gradient(145deg,rgba(21,30,44,.86),rgba(9,20,34,.72));box-shadow:inset 0 1px 0 rgba(255,255,255,.035)}
.about-card{position:relative;overflow:hidden;padding:18px}
.update{display:grid;grid-template-columns:auto 1fr auto;align-items:center;gap:12px;margin-bottom:14px;border-color:rgba(139,92,246,.32);background:linear-gradient(135deg,rgba(139,92,246,.18),rgba(64,131,255,.1))}
.update .material-symbols-rounded{font-size:22px;color:#a997ff}
.update .h{font-weight:800;color:#f4f7ff}
.update .p{margin-top:4px;color:#aab7cc;font-size:13px;line-height:1.4}
.lede-card{min-height:130px;display:grid;grid-template-columns:240px minmax(0,1fr) 72px;align-items:center;gap:18px;padding:20px 22px}
.lede-brand{display:grid;grid-template-columns:54px auto;grid-template-areas:"logo wordmark" "coffee coffee";align-items:center;gap:11px 14px;min-width:0}
.lede-logo{width:54px;height:54px;object-fit:contain;filter:drop-shadow(0 12px 22px rgba(100,80,255,.28))}
.lede-wordmark{font-size:26px;line-height:1;font-weight:800;color:#f5f8ff;white-space:nowrap}
.lede-wordmark span{color:#8b5cf6}
.lede-logo{grid-area:logo}
.lede-wordmark{grid-area:wordmark}
.coffee-link{grid-area:coffee;display:inline-flex;align-items:center;justify-content:center;gap:8px;width:max-content;min-height:34px;padding:0 13px;border:1px solid rgba(124,92,255,.38);border-radius:9px;background:linear-gradient(135deg,rgba(124,92,255,.2),rgba(71,199,120,.12));color:#f4f7ff;text-decoration:none;font-size:12px;font-weight:800;box-shadow:inset 0 1px 0 rgba(255,255,255,.05)}
.coffee-link:hover{border-color:rgba(124,92,255,.58);background:linear-gradient(135deg,rgba(124,92,255,.28),rgba(71,199,120,.16))}
.coffee-link .material-symbols-rounded{font-size:16px}
.lede-copy{display:grid;gap:12px;min-width:0}
.lede{color:#c6d2e4;font-size:14px;line-height:1.45}
.lede strong{color:#f4f7ff;font-weight:800}
.lede-mark{justify-self:end;font-size:66px;color:rgba(170,187,220,.18)}
.about-fold{overflow:hidden}
.about-fold summary{display:grid;grid-template-columns:28px minmax(0,1fr) auto;align-items:center;gap:12px;min-height:50px;padding:0 18px;list-style:none;cursor:pointer;color:#f0f4ff;font-size:14px;font-weight:800}
.about-fold summary::-webkit-details-marker{display:none}
.about-fold-icon{color:#aab7cc;font-size:22px}
.about-fold-chevron{color:#d4deee;font-size:19px;transition:transform .16s ease}
.about-fold[open] .about-fold-chevron{transform:rotate(180deg)}
.rows{display:grid;padding:0 18px 14px}
.r{display:grid;grid-template-columns:minmax(92px,.8fr) minmax(150px,1fr) auto;gap:10px;align-items:center;min-height:32px;border-top:1px solid rgba(142,162,198,.12)}
.r b{font-size:12.5px;color:#f2f6ff}
.r span{overflow:hidden;color:#7f8ba1;font:11px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;text-overflow:ellipsis;white-space:nowrap}
.r em{justify-self:end;color:#c9d4e6;font-style:normal;font-size:12px}
.disclaimer{padding:16px 18px}
.about-section-title{display:flex;align-items:center;gap:12px;margin-bottom:10px;color:#f3f7ff;font-size:15px;font-weight:800}
.about-section-title .warn{color:#ffbd4a;font-size:22px}
.discBody{display:grid;gap:7px;color:#c2cede;font-size:12.5px;line-height:1.35}
ul{display:grid;gap:8px;margin:2px 0 0;padding-left:18px}
li::marker{color:#8b5cf6}
.help-section{position:relative;margin-top:22px;padding:18px 14px 14px}
.help-label{position:absolute;left:18px;top:-12px;display:flex;align-items:center;gap:10px;color:#cbd6e8;font-size:13px;font-weight:800}
.help-label::after{content:"";display:block;width:130px;height:1px;background:linear-gradient(90deg,rgba(142,162,198,.28),transparent)}
.help-label span{padding:0 6px;background:#0b1421}
.help-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.helpLink{display:grid;grid-template-columns:58px minmax(0,1fr) auto;align-items:center;gap:16px;min-height:98px;padding:18px 16px;border:1px solid rgba(142,162,198,.18);border-radius:7px;background:linear-gradient(145deg,rgba(15,29,48,.9),rgba(12,21,34,.78));color:#edf4ff;text-decoration:none}
.helpIcon{display:grid;place-items:center;width:58px;height:58px;border-radius:9px;background:linear-gradient(135deg,#7b4ae2,#5431a5);color:#fff}
.support-card .helpIcon{background:linear-gradient(135deg,#69c341,#289647)}
.helpIcon .material-symbols-rounded{font-size:29px}
.helpCopy{display:grid;gap:4px;min-width:0}
.helpEyebrow{color:#aab7cc;font-size:12px;font-weight:700}
.helpTitle{color:#f5f8ff;font-size:15px;font-weight:800;line-height:1.15}
.helpSub{color:#aab7cc;font-size:12px;line-height:1.35}
.helpArrow{color:#aab7cc;font-size:24px}
@media(max-width:900px){
  .about-panel{max-height:min(910px,calc(100vh - 12px))}
  .about-head{grid-template-columns:auto minmax(0,1fr);padding-right:48px}
  .about-actions{grid-column:1 / -1;justify-content:flex-start;flex-wrap:wrap}
  .help-grid{grid-template-columns:1fr}
}
@media(max-width:560px){
  .about-backdrop{padding:0}
  .about-panel{width:100vw;height:100dvh;border:0;border-radius:0}
  .about-head{padding:12px 44px 12px 14px}
  .about-body{padding:0 16px 12px}
  .about-actions{display:grid;grid-template-columns:1fr 1fr;width:100%}
  .about-actions>*{min-width:0;width:100%;padding-inline:10px}
  .about-actions>*:last-child{grid-column:1 / -1}
  .update{grid-template-columns:1fr}
  .lede-card{grid-template-columns:1fr;padding:18px}
  .lede-brand{min-width:0}
  .lede-mark{display:none}
  .r{grid-template-columns:minmax(0,1fr) auto}
  .r span{display:none}
  .about-link{width:100%}
}
`;

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

function _fold(title, body, icon, open = false) {
  return `
    <details class="about-fold"${open ? " open" : ""}>
      <summary>
        <span class="material-symbols-rounded about-fold-icon" aria-hidden="true">${escapeHtml(icon || "info")}</span>
        <span>${escapeHtml(title)}</span>
        <i class="material-symbols-rounded about-fold-chevron" aria-hidden="true">expand_more</i>
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
  const current = _norm(update.current_version || update.current || window.CW_CURRENT_VERSION || window.APP_VERSION || window.__CW_VERSION__ || "0.0.0");
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
    ? `<span class="chip subtle"><span class="material-symbols-rounded" aria-hidden="true">calendar_month</span>${escapeHtml(info.publishedAt.slice(0, 10))}</span>`
    : "";
  const externalIcon = `<span class="material-symbols-rounded ext" aria-hidden="true">open_in_new</span>`;

  return `
    <div class="about-backdrop">
      <section class="about-panel" role="dialog" aria-modal="true" aria-label="About" tabindex="-1">
        <button class="about-x" type="button" data-close aria-label="Close">
          <span class="material-symbols-rounded" aria-hidden="true">close</span>
        </button>
        <header class="about-head">
          <div class="about-logo-wrap" aria-hidden="true"><img class="about-logo" src="${escapeHtml(logo)}" alt="" /></div>
          <div class="about-heading">
            <div class="about-title">About</div>
          </div>
          <div class="about-actions">
            <span class="chip accent"><span class="material-symbols-rounded" aria-hidden="true">bolt</span>Engine v${escapeHtml(info.current || "-")}</span>
            <span class="chip">${latestChip}</span>
            ${publishedChip}
            <a class="about-link" href="${escapeHtml(info.htmlUrl)}" target="_blank" rel="noopener noreferrer">Releases ${externalIcon}</a>
          </div>
        </header>
        <main class="about-body">
          ${info.hasUpdate ? `
            <section class="about-card update">
              <span class="material-symbols-rounded" aria-hidden="true">new_releases</span>
              <div>
                <div class="h">Update available: v${escapeHtml(info.latest || info.current || "-")}</div>
                <div class="p">You are on v${escapeHtml(info.current || "-")}. Open the latest release notes when you are ready to update.</div>
              </div>
              <a class="about-link" href="${escapeHtml(info.htmlUrl)}" target="_blank" rel="noopener noreferrer">Open release ${externalIcon}</a>
            </section>
          ` : ""}
          <div class="about-grid">
            <section class="about-main">
              <section class="about-card lede-card">
                <div class="lede-brand">
                  <img class="lede-logo" src="${escapeHtml(logo)}" alt="" />
                  <div class="lede-wordmark">Cross<span>Watch</span></div>
                  <a class="coffee-link" href="${SUPPORT_URL}" target="_blank" rel="noopener noreferrer"><span class="material-symbols-rounded" aria-hidden="true">local_cafe</span>Buy me a coffee ${externalIcon}</a>
                </div>
                <div class="lede-copy">
                  <div class="lede"><strong>CrossWatch (CW)</strong> is a synchronization engine that acts as a bridge and keeps your <strong>Plex, Jellyfin, Emby, SIMKL, Floppy, FlickList, Trakt, AniList, TMDb, MDBList, PublicMetaDB, PunchPlay, BingeBase, Scrob, Tautulli, Kodi, Nuvio, Stremio and CW local tracker</strong> in sync.</div>
                  <div class="lede"><strong>Please note:</strong> this software is still beta/experimental and may behave unpredictably. Make sure you have solid, tested backups before using it.</div>
                </div>
                <span class="material-symbols-rounded lede-mark" aria-hidden="true">language</span>
              </section>
              ${_fold("Authentication Providers", _providerRows(mods.groups?.AUTH), "lock")}
              ${_fold("Synchronization Providers", _providerRows(mods.groups?.SYNC), "sync")}
              <section class="about-card disclaimer">
                <div class="about-section-title"><span class="material-symbols-rounded warn" aria-hidden="true">warning</span><span>Disclaimer</span></div>
                <div class="discBody">
                  <div>CrossWatch is an independent community project. It is not affiliated with, endorsed by, or sponsored by Plex, Jellyfin, Emby, SIMKL, Floppy, FlickList, Trakt, AniList, TMDb, MDBList, PublicMetaDB, PunchPlay, BingeBase, Scrob, Tautulli, Kodi, Nuvio, Stremio, CW local tracker, or their owners.</div>
                  <div>CrossWatch uses the AniBridge mappings dataset and the animeApi dataset for anime identifier and episode translation.</div>
                  <ul>
                    <li>Names, logos, trademarks, and brands belong to their respective owners and are used for identification only.</li>
                    <li>Third-party APIs and services have their own terms, rate limits, and account policies. Use CrossWatch responsibly and within those rules.</li>
                    <li>CrossWatch is provided as-is, without warranties. Keep backups of any state, tracker, cache, or configuration data you edit.</li>
                  </ul>
                </div>
              </section>
            </section>
          </div>
          <section class="help-section">
            <div class="help-label"><span>Need help?</span></div>
            <div class="help-grid">
              <a class="helpLink" href="${WIKI_URL}" target="_blank" rel="noopener noreferrer">
                <span class="helpIcon" aria-hidden="true"><span class="material-symbols-rounded">menu_book</span></span>
                <span class="helpCopy">
                  <span class="helpEyebrow">Documentation</span>
                  <span class="helpTitle">Open the CrossWatch Wiki</span>
                  <span class="helpSub">Setup guides, upgrade notes, and troubleshooting in one place.</span>
                </span>
                <span class="material-symbols-rounded helpArrow" aria-hidden="true">chevron_right</span>
              </a>
              <a class="helpLink support-card" href="${COMMUNITY_URL}" target="_blank" rel="noopener noreferrer">
                <span class="helpIcon" aria-hidden="true"><span class="material-symbols-rounded">chat_bubble</span></span>
                <span class="helpCopy">
                  <span class="helpEyebrow">Community Support</span>
                  <span class="helpTitle">Ask questions & get help</span>
                  <span class="helpSub">Join the discussions and get help from the community.</span>
                </span>
                <span class="material-symbols-rounded helpArrow" aria-hidden="true">chevron_right</span>
              </a>
            </div>
          </section>
        </main>
      </section>
    </div>
  `;
}

function applyOpenState() {
  const body = document.body;
  if (!body) return () => {};
  const previousDataset = body.dataset.cxModalOpen;
  const hadOpenClass = body.classList.contains("cx-modal-open");
  body.dataset.cxModalOpen = "1";
  body.classList.add("cx-modal-open");
  body.classList.add("cw-about-open");
  return () => {
    if (previousDataset == null) delete body.dataset.cxModalOpen;
    else body.dataset.cxModalOpen = previousDataset;
    body.classList.toggle("cx-modal-open", hadOpenClass);
    body.classList.remove("cw-about-open");
  };
}

export function closeAboutModal() {
  if (!activeModal) return;
  const { overlay, restoreOpenState, onKeyDown } = activeModal;
  document.removeEventListener("keydown", onKeyDown, true);
  restoreOpenState?.();
  overlay.remove();
  activeModal = null;
}

export async function openAboutModal(props = {}) {
  closeAboutModal();

  const { update, mods } = await loadAbout(!!props.force);
  const info = _versionInfo(update);
  const crossWatchLogo = window.CW?.ProviderMeta?.logoPath?.("crosswatch") || "/assets/img/CROSSWATCH.svg";
  const overlay = document.createElement("div");
  overlay.id = "cw-about-standalone";
  const shadow = overlay.attachShadow({ mode: "open" });
  const restoreOpenState = applyOpenState();
  const onKeyDown = (e) => {
    if (e.key !== "Escape") return;
    e.preventDefault();
    e.stopPropagation();
    closeAboutModal();
  };

  shadow.innerHTML = `<style>${ABOUT_CSS}</style>${view(info, mods || {}, crossWatchLogo)}`;
  shadow.addEventListener("click", (e) => {
    if (e.target?.closest?.("[data-close]")) closeAboutModal();
  });
  document.addEventListener("keydown", onKeyDown, true);
  document.body.appendChild(overlay);

  activeModal = { overlay, restoreOpenState, onKeyDown };
  shadow.querySelector(".about-panel")?.focus({ preventScroll: true });
  return overlay;
}

async function render(host, props = {}) {
  const registryClose = window.cxCloseModal;
  if (host) host.innerHTML = "";
  if (host?.closest?.(".cx-modal-shell") && typeof registryClose === "function") registryClose();
  await openAboutModal(props);
}

export default {
  async mount(host, props) {
    await render(host, props);
  },
  unmount() {
    closeAboutModal();
  },
};
