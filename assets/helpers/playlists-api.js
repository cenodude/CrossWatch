/* helpers/playlists-api.js */
/* CrossWatch Playlists API helper */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const BASE = "/api/playlists";

  async function j(url, opt) {
    const r = await fetch(url, { cache: "no-store", headers: { "Content-Type": "application/json" }, ...(opt || {}) });
    let data = null;
    try { data = await r.json(); } catch { data = null; }
    if (!r.ok) {
      const msg = (data && (data.error || data.detail)) || `${r.status} ${r.statusText}`;
      const err = new Error(msg);
      err.data = data;
      throw err;
    }
    return data;
  }

  const API = {
    providers: () => j(`${BASE}/providers`),
    resources: (provider, instance) =>
      j(`${BASE}/resources?provider=${encodeURIComponent(provider)}&instance=${encodeURIComponent(instance || "default")}`),
    overview: () => j(`${BASE}/overview`),
    mappings: () => j(`${BASE}/mappings`),
    upsert: (payload) => j(`${BASE}/mappings`, { method: "POST", body: JSON.stringify(payload) }),
    remove: (pairId, mappingId) =>
      j(`${BASE}/mappings/${encodeURIComponent(pairId)}/${encodeURIComponent(mappingId)}`, { method: "DELETE" }),
    preview: (pairId, mappingId) =>
      j(`${BASE}/mappings/${encodeURIComponent(pairId)}/${encodeURIComponent(mappingId)}/preview`, { method: "POST" }),
    run: (pairId, mappingId, dryRun) =>
      j(`${BASE}/mappings/${encodeURIComponent(pairId)}/${encodeURIComponent(mappingId)}/run${dryRun ? "?dry_run=true" : ""}`, { method: "POST" }),
    result: (pairId, mappingId) =>
      j(`${BASE}/mappings/${encodeURIComponent(pairId)}/${encodeURIComponent(mappingId)}/result`),
    pairs: () => j(`${BASE}/pairs`),
  };

  NS.PlaylistsAPI = API;
})();
