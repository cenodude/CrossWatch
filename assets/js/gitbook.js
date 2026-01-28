/* CrossWatch - GitBook integration */
(() => {
  const enabled = (() => {
    try {
      if (typeof window.__cwUiShowAI === "boolean") return window.__cwUiShowAI;
      const cfg = window._cfgCache;
      const ui = (cfg && typeof cfg === "object") ? (cfg.ui || cfg.user_interface || {}) : {};
      if (ui && typeof ui === "object" && typeof ui.show_AI === "boolean") return !!ui.show_AI;
    } catch (_) {}
    return true;
  })();
  if (!enabled) return;

  const CFG = window.__cwGitBookConfig || {};
  const SITE_URL = String(CFG.siteUrl || "https://wiki.crosswatch.app");
  const REPORT_URL = String(CFG.reportUrl || "https://github.com/cenodude/CrossWatch/issues/new");
  const DISCUSS_URL = String(CFG.discussUrl || "https://github.com/cenodude/CrossWatch/discussions");
  if (window.__cwGitBookBooted) return;
  window.__cwGitBookBooted = true;

  function openWikiHome() {
    try { if (typeof window.openHelp === "function") { window.openHelp(); return; } } catch (_) {}
    window.open(SITE_URL, "_blank", "noopener,noreferrer");
  }

  function openSettingsSection(sectionId) {
    try { if (typeof window.showTab === "function") window.showTab("settings"); } catch (_) {}
    setTimeout(() => {
      const el = document.getElementById(sectionId);
      if (!el) return;

      try {
        if (typeof window.toggleSection === "function" && !el.classList.contains("open")) window.toggleSection(sectionId);
      } catch (_) {}

      try { el.scrollIntoView({ behavior: "smooth", block: "start" }); } catch (_) {}
    }, 60);
  }

  function providerToSectionId(provider) {
    const p = String(provider || "").trim().toLowerCase();
    const map = {
      plex: "sec-plex",
      trakt: "sec-trakt",
      simkl: "sec-simkl",
      anilist: "sec-anilist",
      jellyfin: "sec-jellyfin",
      emby: "sec-emby",
      mdblist: "sec-mdblist",
      tautulli: "sec-tautulli"
    };
    return map[p] || "";
  }

  const sleep = (ms) => new Promise((r) => setTimeout(r, Math.max(0, ms | 0)));

  function ensureSectionOpen(id) {
    const el = document.getElementById(id);
    if (!el) return null;
    try {
      if (!el.classList.contains("open")) {
        if (typeof window.toggleSection === "function") window.toggleSection(id);
        else el.classList.add("open");
      }
    } catch (_) {}
    return el;
  }

  async function ensureAuthMounted() {
    const host = document.getElementById("auth-providers");
    if (!host) return;
    const hasHtml = (host.innerHTML || "").trim().length > 0;
    if (hasHtml) return;
    try { if (typeof window.mountAuthProviders === "function") await window.mountAuthProviders(); } catch (_) {}
  }

  async function openAuthProvider(provider, subtab) {
    const p = String(provider || "").trim().toLowerCase();
    try { window.showTab?.("settings"); } catch (_) {}
    await sleep(40);

    ensureSectionOpen("sec-auth");
    await sleep(20);

    await ensureAuthMounted();
    await sleep(20);

    const group =
      ["plex", "jellyfin", "emby"].includes(p) ? "sec-auth-media" :
      ["trakt", "simkl", "mdblist", "anilist"].includes(p) ? "sec-auth-trackers" :
      "sec-auth-others";

    ensureSectionOpen(group);
    await sleep(20);

    const sectionId = providerToSectionId(p);
    if (sectionId) ensureSectionOpen(sectionId);

    await sleep(40);

    const el = sectionId ? document.getElementById(sectionId) : null;
    if (el) {
      try { el.scrollIntoView({ behavior: "smooth", block: "start" }); } catch (_) {}
      if (subtab) {
        const st = String(subtab).trim().toLowerCase();
        const btn = el.querySelector(`.cw-subtile[data-sub="${st}"]`);
        try { btn?.click?.(); } catch (_) {}
      }
    }
    return { provider: p, sectionId, subtab: subtab ? String(subtab) : "" };
  }

  async function openScheduler(tab) {
    try { window.showTab?.("settings"); } catch (_) {}
    await sleep(40);

    const sec = ensureSectionOpen("sec-scheduling");
    await sleep(30);

    try { window.cwSchedProviderEnsure?.(); } catch (_) {}
    try { window.cwSchedProviderSelect?.(true, { persist: false }); } catch (_) {}

    const t = String(tab || "basic").trim().toLowerCase();
    const want = (t === "advanced") ? "advanced" : "basic";
    try { window.cwSchedSettingsSelect?.(want); } catch (_) {}

    if (sec) { try { sec.scrollIntoView({ behavior: "smooth", block: "start" }); } catch (_) {} }
    return { tab: want };
  }

  async function openUiSettings(tab) {
    try { window.showTab?.("settings"); } catch (_) {}
    await sleep(40);

    const sec = ensureSectionOpen("sec-ui");
    await sleep(30);

    try { window.cwUiSettingsHubInit?.(); } catch (_) {}
    const t = String(tab || "ui").trim().toLowerCase();
    const want = ["ui", "security", "tracker"].includes(t) ? t : "ui";
    try { window.cwUiSettingsSelect?.(want); } catch (_) {}

    if (sec) { try { sec.scrollIntoView({ behavior: "smooth", block: "start" }); } catch (_) {} }
    return { tab: want };
  }

  async function apiJson(url, opts) {
    const r = await fetch(url, {
      credentials: "same-origin",
      cache: "no-store",
      ...(opts || {})
    });
    const txt = await r.text();
    let data = null;
    try { data = txt ? JSON.parse(txt) : null; } catch (_) {}
    if (!r.ok) {
      const msg = data && (data.detail || data.message) ? (data.detail || data.message) : (r.status + " " + r.statusText);
      throw new Error(msg);
    }
    return data;
  }
function boot() {
    const GB = window.GitBook;
    if (typeof GB !== "function") return;
    try { GB("unload"); } catch (_) {}

    try {
      GB("init", { siteURL: SITE_URL });
    } catch (e) {
      console.warn("[docs] GitBook init failed:", e);
    }

    // Customize Assistant (UI)
    try {
      GB("configure", {
        tabs: ["assistant", "docs"],
        greeting: {
          title: "Welcome to CrossWatch",
          subtitle: "Alright, what’s broken this time?"
        },
        suggestions: [
          "How do I connect Plex?",
          "How do I run a sync?",
          "How does two-way mode work?"
        ],
        actions: [
          {
            icon: "messages",
            label: "Discussions",
            onClick: () => window.open(DISCUSS_URL, "_blank", "noopener,noreferrer")
          },
          {
            icon: "bug",
            label: "Report issue",
            onClick: () => window.open(REPORT_URL, "_blank", "noopener,noreferrer")
          }
        ]
      });
} catch (e) {
      console.warn("[docs] GitBook configure failed:", e);
    }

    try { GB("show"); } catch (e) { console.warn("[docs] GitBook show failed:", e); }
  }

  if (typeof window.GitBook === "function") { boot(); return; }

  let tries = 0;
  const t = setInterval(() => {
    tries += 1;
    if (typeof window.GitBook === "function") {
      clearInterval(t);
      boot();
    } else if (tries > 120) {
      clearInterval(t);
      console.warn("[docs] GitBook embed timed out");
    }
  }, 50);
})();
