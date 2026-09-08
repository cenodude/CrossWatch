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
.about-backdrop{--bg:#171a22;--panel:#20242d;--border:rgba(255,255,255,.14);--text:#eef1f6;--muted:#a9b0bd;--accent:#7c5cff;position:fixed;inset:0;z-index:30050;display:grid;place-items:center;padding:24px;background:rgba(6,8,13,.66);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:var(--text)}
.about-panel{position:relative;width:min(1080px,100%);max-height:calc(100vh - 48px);max-height:calc(100dvh - 48px);display:grid;grid-template-rows:auto minmax(0,1fr);overflow:hidden;border:1px solid var(--border);border-radius:20px;background:var(--bg);outline:none}
.material-symbols-rounded{font-family:"Material Symbols Rounded";font-weight:normal;font-style:normal;font-size:20px;line-height:1;letter-spacing:normal;text-transform:none;display:inline-block;white-space:nowrap;word-wrap:normal;direction:ltr;font-feature-settings:"liga";-webkit-font-feature-settings:"liga";-webkit-font-smoothing:antialiased;font-variation-settings:"FILL" 0,"wght" 560,"GRAD" 0,"opsz" 24}
.about-x{position:absolute;top:19px;right:20px;z-index:4;display:grid;place-items:center;width:36px;height:36px;padding:0;border:1px solid var(--border);border-radius:10px;background:var(--panel);color:var(--muted);cursor:pointer}
.about-x:hover{color:var(--text);border-color:var(--accent)}
.about-head{display:flex;align-items:center;gap:14px;padding:18px 70px 18px 24px}
.about-logo-wrap{display:grid;place-items:center;width:48px;height:48px;border:1px solid var(--border);border-radius:14px;background:var(--panel);flex-shrink:0}
.about-logo{width:34px;height:34px;object-fit:contain}
.about-heading{min-width:0}
.about-title{font-size:20px;line-height:1.2;font-weight:850;text-transform:uppercase;letter-spacing:.01em}
.about-sub{margin-top:4px;font-size:13px;color:var(--muted)}
.about-actions{display:flex;align-items:center;justify-content:flex-end;gap:8px;margin-left:auto;flex-wrap:wrap}
.chip,.about-link{display:inline-flex;align-items:center;justify-content:center;gap:7px;min-height:34px;padding:7px 12px;border:1px solid var(--border);border-radius:10px;background:var(--panel);color:var(--text);text-decoration:none;white-space:nowrap;font-size:12px;line-height:1.2;font-weight:700}
.chip{border-radius:999px}
.chip .material-symbols-rounded,.about-link .material-symbols-rounded{font-size:17px}
.chip.accent{border-color:rgba(124,92,255,.35);background:#2a2640;color:#d0c4ff}
.chip.subtle{color:var(--muted)}
.about-link:hover{border-color:var(--accent)}
.ext{font-size:16px}
.about-body{min-height:0;overflow:auto;padding:0 24px 22px;overscroll-behavior:contain;scrollbar-width:thin;scrollbar-color:var(--border) transparent}
.about-main{display:grid;gap:12px}
.about-card,.about-fold{border:1px solid var(--border);border-radius:16px;background:var(--panel)}
.about-card{padding:16px 20px}
.update{display:grid;grid-template-columns:auto 1fr auto;align-items:center;gap:14px;margin-bottom:12px;border-color:rgba(124,92,255,.4);background:#27263b}
.update>.material-symbols-rounded{display:grid;place-items:center;width:42px;height:42px;border-radius:12px;background:#312c4b;color:#b39aff;font-size:25px}
.update .h{font-size:16px;font-weight:800}
.update .p{margin-top:4px;color:var(--muted);font-size:13px;line-height:1.45}
.lede-card{display:grid;grid-template-columns:260px minmax(0,1fr);align-items:center;gap:28px;padding:24px}
.lede-brand{display:grid;grid-template-columns:48px auto;grid-template-areas:"logo wordmark" "motto motto" "coffee coffee";align-items:center;gap:12px;min-width:0}
.lede-logo{grid-area:logo;width:48px;height:48px;object-fit:contain}
.lede-wordmark{grid-area:wordmark;font-size:28px;line-height:1;font-weight:850;white-space:nowrap}
.lede-wordmark span{color:#997bff}
.lede-motto{grid-area:motto;margin:0 0 4px;color:#b4acd3;font:italic 17px/1.5 "Segoe Print","Bradley Hand","Comic Sans MS",cursive}
.coffee-link{grid-area:coffee;display:inline-flex;align-items:center;justify-content:center;gap:8px;width:max-content;min-height:38px;padding:8px 14px;border:1px solid #4db888;border-radius:11px;background:#1f6b50;color:#f0fff7;text-decoration:none;font-size:13px;font-weight:750}
.coffee-link:hover{background:#277e5e;border-color:#68d7a4}
.coffee-link .material-symbols-rounded{font-size:18px}
.lede-copy{display:grid;gap:14px;min-width:0;border-left:1px solid var(--border);padding-left:28px}
.lede{color:var(--muted);font-size:14px;line-height:1.55}
.lede strong{color:var(--text);font-weight:750}
.about-fold{overflow:hidden}
.about-fold summary{display:grid;grid-template-columns:28px minmax(0,1fr) auto;align-items:center;gap:12px;min-height:50px;padding:12px 18px;list-style:none;cursor:pointer;font-size:14px;font-weight:750}
.about-fold summary::-webkit-details-marker{display:none}
.about-fold summary:hover{background:rgba(124,92,255,.06)}
.about-fold-icon{color:#aa91ff;font-size:22px}
.about-fold-chevron{color:var(--muted);font-size:20px;transition:transform .16s ease}
.about-fold[open] .about-fold-chevron{transform:rotate(180deg)}
.rows{display:grid;padding:0 18px 14px}
.r{display:grid;grid-template-columns:minmax(92px,.8fr) minmax(150px,1fr) auto;gap:10px;align-items:center;min-height:34px;border-top:1px solid var(--border)}
.r b{font-size:13px}
.r span{overflow:hidden;color:var(--muted);font:11px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;text-overflow:ellipsis;white-space:nowrap}
.r em{justify-self:end;color:var(--muted);font-style:normal;font-size:12px}
.discBody{display:grid;gap:8px;padding:0 18px 18px;color:var(--muted);font-size:13px;line-height:1.5}
ul{display:grid;gap:6px;margin:2px 0 0;padding-left:18px}
li::marker{color:#997bff}
.help-section{margin-top:16px}
.help-label{margin-bottom:10px;font-size:14px;font-weight:750}
.help-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.helpLink{display:grid;grid-template-columns:44px minmax(0,1fr) auto;align-items:center;gap:14px;padding:16px;border:1px solid var(--border);border-radius:16px;background:var(--panel);color:var(--text);text-decoration:none}
.helpLink:hover{border-color:var(--accent)}
.helpIcon{display:grid;place-items:center;width:44px;height:44px;border:1px solid rgba(124,92,255,.35);border-radius:12px;background:#2e2943;color:#b59eff}
.support-card .helpIcon{background:#203b32;border-color:rgba(77,184,136,.35);color:#66d1a0}
.helpIcon .material-symbols-rounded{font-size:25px}
.helpCopy{display:grid;gap:4px;min-width:0}
.helpEyebrow{color:var(--muted);font-size:10px;font-weight:750;letter-spacing:.07em;text-transform:uppercase}
.helpTitle{font-size:15px;font-weight:800;line-height:1.3}
.helpSub{color:var(--muted);font-size:12px;line-height:1.45}
.helpArrow{color:var(--muted);font-size:21px}
:is(button,a,summary):focus-visible{outline:2px solid var(--accent);outline-offset:3px}
@media(max-width:1000px){.about-head{flex-wrap:wrap}.about-actions{width:100%;margin-left:0;justify-content:flex-start}.lede-card{grid-template-columns:230px minmax(0,1fr);gap:20px}.lede-copy{padding-left:20px}.lede-wordmark{font-size:24px}}
@media(max-width:650px){.about-backdrop{padding:12px}.about-panel{max-height:calc(100dvh - 24px);border-radius:16px}.about-head{padding:16px 60px 16px 16px}.about-title{font-size:17px}.about-actions{gap:6px}.chip,.about-link{font-size:11px;padding:7px 10px}.about-body{padding:0 16px 16px}.update{grid-template-columns:42px minmax(0,1fr);padding:14px}.update .about-link{grid-column:2;justify-self:start}.lede-card{grid-template-columns:1fr;gap:20px;padding:18px}.lede-brand{grid-template-columns:48px auto;justify-content:start}.lede-copy{padding:18px 0 0;border-left:0;border-top:1px solid var(--border)}.help-grid{grid-template-columns:1fr}.r{grid-template-columns:minmax(0,1fr) auto}.r span{display:none}}
@media(prefers-reduced-motion:reduce){.about-fold-chevron{transition:none}}
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
            <div class="about-title">About CrossWatch</div>
            <div class="about-sub">Your media, in sync.</div>
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
          <section class="about-main">
              <section class="about-card lede-card">
                <div class="lede-brand">
                  <img class="lede-logo" src="${escapeHtml(logo)}" alt="" />
                  <div class="lede-wordmark">Cross<span>Watch</span></div>
                  <p class="lede-motto">Your media. Your data.<br>On your terms.</p>
                  <a class="coffee-link" href="${SUPPORT_URL}" target="_blank" rel="noopener noreferrer"><span class="material-symbols-rounded" aria-hidden="true">local_cafe</span>Buy me a coffee ${externalIcon}</a>
                </div>
                <div class="lede-copy">
                  <div class="lede"><strong>CrossWatch (CW)</strong> is a synchronization engine that acts as a bridge and keeps your <strong>Plex, Jellyfin, Emby, SIMKL, Floppy, FlickList, Trakt, AniList, TMDb, MDBList, PublicMetaDB, PunchPlay, BingeBase, Scrob, Tautulli, Kodi, Nuvio, Stremio and CW local tracker</strong> in sync.</div>
                  <div class="lede"><strong>Please note:</strong> this software is still beta/experimental and may behave unpredictably. Make sure you have solid, tested backups before using it.</div>
                </div>
              </section>
              ${_fold("Synchronization Providers", _providerRows(mods.groups?.SYNC), "sync")}
              <details class="about-fold disclaimer">
                <summary><span class="material-symbols-rounded about-fold-icon" aria-hidden="true">info</span><span>Disclaimer &amp; credits</span><span class="material-symbols-rounded about-fold-chevron" aria-hidden="true">expand_more</span></summary>
                <div class="discBody">
                  <div>CrossWatch is an independent community project. It is not affiliated with, endorsed by, or sponsored by Plex, Jellyfin, Emby, SIMKL, Floppy, FlickList, Trakt, AniList, TMDb, MDBList, PublicMetaDB, PunchPlay, BingeBase, Scrob, Tautulli, Kodi, Nuvio, Stremio, CW local tracker, or their owners.</div>
                  <div>CrossWatch uses the AniBridge mappings dataset and the animeApi dataset for anime identifier and episode translation.</div>
                  <ul>
                    <li>Names, logos, trademarks, and brands belong to their respective owners and are used for identification only.</li>
                    <li>Third-party APIs and services have their own terms, rate limits, and account policies. Use CrossWatch responsibly and within those rules.</li>
                    <li>CrossWatch is provided as-is, without warranties. Keep backups of any state, tracker, cache, or configuration data you edit.</li>
                  </ul>
                </div>
              </details>
          </section>
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
