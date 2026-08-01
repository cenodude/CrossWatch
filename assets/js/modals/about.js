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

    <div id="about-host" role="dialog" aria-label="About CrossWatch">
      <div class="head">
        <div class="logoWrap" aria-hidden="true"><img class="logo" src="${escapeHtml(logo)}" alt="" /></div>
        <div>
          <div class="title">About CrossWatch</div>
          <div class="sub">Version and modules info</div>
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
          <div class="lede">CrossWatch syncs Plex, Jellyfin, Emby, Kodi, Nuvio, Stremio, MDBList, AniList, Floppy, TMDb, SIMKL, PublicMetaDB, Trakt and Tautulli. Keep it behind your network edge and avoid exposing it directly to the internet.</div>
        </section>
        ${_fold("Authentication providers", _providerRows(mods.groups?.AUTH))}
        ${_fold("Synchronization providers", _providerRows(mods.groups?.SYNC))}
        <section class="card">
          <div class="eyebrow">Disclaimer</div>
          <div class="discBody">
            <div>CrossWatch is an independent community project. It is not affiliated with, endorsed by, or sponsored by Plex, Jellyfin, Emby, Kodi, Nuvio, Stremio, MDBList, AniList, Floppy, TMDb, SIMKL, PublicMetaDB, Trakt, Tautulli, or their owners. CrossWatch uses the AniBridge mappings dataset for AniList identifier translation.</div>
            <ul>
              <li>Names, logos, trademarks, and brands belong to their respective owners and are used for identification only.</li>
              <li>Third-party APIs and services have their own terms, rate limits, and account policies. Use CrossWatch responsibly and within those rules.</li>
              <li>CrossWatch is provided as-is, without warranties. Keep backups of any state, tracker, cache, or configuration data you edit.</li>
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
