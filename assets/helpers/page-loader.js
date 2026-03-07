/* assets/helpers/page-loader.js */
/* Refactored and expanded page loader with dynamic import and refresh capabilities */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function(){
  const state = window.__cwPageLoaderState || (window.__cwPageLoaderState = Object.create(null));

  function withVersion(src){
    const base = new URL(src, document.baseURI).href;
    return window.APP_VERSION ? `${base}?v=${encodeURIComponent(window.APP_VERSION)}` : base;
  }

  function loadClassic(src){
    return new Promise((resolve, reject) => {
      const s = document.createElement("script");
      s.src = src;
      s.async = true;
      s.onload = resolve;
      s.onerror = reject;
      document.head.appendChild(s);
    });
  }

  async function loadViaBlobModule(src){
    const res = await fetch(src, { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status} loading ${src}`);
    const text = await res.text();
    const head = text.slice(0, 200).toLowerCase();
    if (head.includes("<!doctype") || head.includes("<html")) throw new Error(`${src} was served as HTML`);
    const blobUrl = URL.createObjectURL(new Blob([text], { type: "application/javascript" }));
    try {
      await import(/* @vite-ignore */ blobUrl);
    } finally {
      URL.revokeObjectURL(blobUrl);
    }
  }

  async function importBestEffort(src){
    try {
      await import(/* @vite-ignore */ src);
    } catch (e1) {
      try {
        await loadViaBlobModule(src);
      } catch (e2) {
        await loadClassic(src);
      }
    }
  }

  async function ensure(opts){
    const key = String(opts?.key || "").trim();
    if (!key) throw new Error("page-loader: missing key");
    const slot = state[key] || (state[key] = { loaded: false, inflight: null });
    if (slot.inflight) return slot.inflight;

    slot.inflight = (async () => {
      if (!slot.loaded) {
        await importBestEffort(withVersion(opts.src));
        slot.loaded = true;
        return;
      }

      const ns = window[opts.namespace];
      if (typeof ns?.refresh === "function") {
        await ns.refresh(...(opts.refreshArgs || []));
        return;
      }

      if (opts.refreshEvent) {
        window.dispatchEvent(new CustomEvent(opts.refreshEvent));
      }
    })();

    try {
      await slot.inflight;
    } finally {
      slot.inflight = null;
    }
  }

  (window.CW ||= {});
  window.CW.PageLoader = { ensure };
})();
