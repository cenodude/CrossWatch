/* assets/helpers/view-cache.js */
/* CrossWatch - Background page refresh client */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
(function () {
  const jobs = new Map();
  const retries = new Set();
  const sessions = new Map();
  let statuses = new WeakMap();
  let generation = 0;
  const paths = new Set([
    "/api/profile/collection", "/api/profile/history", "/api/profile/ratings", "/api/profile/watchlist",
    "/api/insights", "/api/dashboard/widgets", "/api/state/wall",
    "/api/tracker/summary", "/api/tracker/items", "/api/tracker/history/titles", "/api/tracker/months",
    "/api/tracker/shows", "/api/tracker/overview", "/api/tracker/notes", "/api/tracker/stats",
  ]);
  const supported = url => {
    const parsed = new URL(url, location.origin);
    return parsed.origin === location.origin && paths.has(parsed.pathname) && parsed.searchParams.get("include_keys") !== "true" && parsed.searchParams.get("include_keys") !== "1";
  };
  const announce = (name, url, data) => window.dispatchEvent(new CustomEvent(name, {detail:{url, data}}));
  const abort = () => Object.assign(new Error("Cancelled"), {name:"AbortError"});
  const retryKey = url => {
    const parsed = new URL(url, location.origin);
    for (const name of ["background", "retry_view", "known_version", "t"]) parsed.searchParams.delete(name);
    parsed.searchParams.sort();
    return parsed.pathname + parsed.search;
  };

  async function read(url, fetcher, {signal, active = () => !document.hidden, watch = true, fresh = false, refresh = false, trackRevision = false} = {}) {
    const parsed = new URL(url, location.origin);
    parsed.searchParams.set("background", "1");
    parsed.searchParams.delete("known_version");
    parsed.searchParams.delete("t");
    const key = parsed.pathname + parsed.search;
    const group = parsed.pathname + (parsed.searchParams.has("summary_only") ? ":summary" : "") + (parsed.pathname === "/api/insights" ? `:${parsed.searchParams.get("include_events") || "1"}` : "") + (parsed.pathname === "/api/dashboard/widgets" ? `:${parsed.searchParams.get("view") || "overview"}` : "") + (watch ? "" : `:once:${parsed.search}`);
    const session = {};
    sessions.get(group)?.cancel?.();
    sessions.set(group, session);
    if (watch) clearTimeout(jobs.get(key));
    let retry = retries.delete(retryKey(parsed)) || refresh;
    const scope = generation;
    let previous = "";
    let delivered = false;
    const usable = () => scope === generation && sessions.get(group) === session && !signal?.aborted;
    const poll = async () => {
      if (!usable()) throw abort();
      if (document.hidden) await new Promise((resolve, reject) => {
        const finish = error => {
          document.removeEventListener("visibilitychange", visible);
          signal?.removeEventListener("abort", cancel);
          session.cancel = null;
          if (error) reject(error); else resolve();
        };
        const visible = () => { if (!document.hidden) finish(); };
        const cancel = () => finish(abort());
        session.cancel = cancel;
        document.addEventListener("visibilitychange", visible);
        signal?.addEventListener("abort", cancel, {once:true});
      });
      if (!usable() || !active()) throw abort();
      const data = await fetcher(key + (retry ? "&retry_view=1" : ""));
      retry = false;
      if (!usable()) throw abort();
      const meta = data?._view;
      announce("cw:view-status", url, meta || {});
      if (!meta) return data;
      if (meta.ready) {
        const version = trackRevision ? `${meta.version}:${meta.revision || ""}` : meta.version;
        if (delivered && previous !== version) announce("cw:view-updated", url, data);
        previous = version;
      }
      if (!usable()) return data;
      if (!meta.error) {
        if (meta.ready && (!fresh || !meta.refreshing)) {
          if (watch) {
            const timer = setTimeout(() => {
              jobs.delete(key);
              void poll().catch(error => {
                if (error.name !== "AbortError" && usable()) announce("cw:view-status", url, {ready:true, error:"refresh_failed"});
              });
            }, meta.refreshing ? Math.max(1, Number(meta.retry_after) || 2) * 1000 : 30000);
            clearTimeout(jobs.get(key));
            jobs.set(key, timer);
          }
        } else {
          await new Promise(resolve => setTimeout(resolve, Math.max(1, Number(meta.retry_after) || 2) * 1000));
          return poll();
        }
      }
      if (!meta.ready || (fresh && meta.error)) throw Object.assign(new Error("The view could not be prepared. Please retry."), {code:"view_refresh_failed"});
      delivered = true;
      return data;
    };
    try { return await poll(); }
    finally { if (!watch && sessions.get(group) === session) sessions.delete(group); }
  }

  function invalidate() {
    generation++;
    for (const timer of jobs.values()) clearTimeout(timer);
    jobs.clear();
    retries.clear();
    for (const session of sessions.values()) session.cancel?.();
    sessions.clear();
    statuses = new WeakMap();
    document.querySelectorAll("[data-view-status]").forEach(node => node.remove());
  }

  function status(host, meta, label = "items", retry, source = label) {
    if (!host) return;
    let current = statuses.get(host);
    if (!current) { current = new Map(); statuses.set(host, current); }
    current.set(source, {meta, retry});
    const selected = [...current.values()].find(value => value.meta.error) || [...current.values()].find(value => value.meta.refreshing);
    meta = selected?.meta || {};
    retry = selected?.retry;
    let node = host.querySelector(":scope > [data-view-status]");
    if (!node) {
      node = document.createElement("p");
      node.dataset.viewStatus = "";
      node.setAttribute("role", "status");
      host.prepend(node);
    }
    node.hidden = !meta.refreshing && !meta.error;
    node.textContent = meta.error ? "Refresh failed. Your last available results are still shown. " : meta.ready ? "Refreshing…" : `Preparing ${label}…`;
    if (meta.error && !meta.ready) node.textContent = `Could not prepare ${label}. `;
    if (meta.error && retry) {
      const button = document.createElement("button");
      button.type = "button";
      button.textContent = "Retry";
      button.addEventListener("click", retry, {once:true});
      node.append(button);
    }
  }

  window.CW = window.CW || {};
  window.CW.ViewCache = {read, supported, invalidate, status, retry:url => retries.add(retryKey(url))};
  window.addEventListener("cw:sync-state-cleared", invalidate);
  window.addEventListener("cw:overview-profile-changed", invalidate);
})();
