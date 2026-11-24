/* assets/helpers/view-cache.js */
/* CrossWatch - Background page refresh client */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  const jobs = new Map();
  const retries = new Set();
  const sessions = new Map();
  const snapshots = new Map();
  let snapshotBytes = 0;
  let snapshotGeneration = 0;
  const discard = () => {
    snapshotGeneration++;
    snapshots.clear();
    snapshotBytes = 0;
  };
  const dropSnapshot = key => {
    snapshotBytes -= snapshots.get(key)?.body.length || 0;
    snapshots.delete(key);
  };
  let statuses = new WeakMap();
  const refreshHosts = new Map();
  const updateRefresh = () => {
    for (const [host, matches] of refreshHosts) {
      if (!host.isConnected) { refreshHosts.delete(host); continue; }
      host.toggleAttribute("data-view-loading", [...sessions.values()].some(session => session.busy && !session.signal?.aborted && session.active() && matches(session.url)));
    }
  };
  const bindRefresh = (host, matches) => {
    if (!host) return;
    refreshHosts.set(host, matches);
    updateRefresh();
  };
  let generation = 0;
  const paths = new Set([
    "/api/profile/collection", "/api/profile/history", "/api/profile/ratings", "/api/profile/watchlist", "/api/profile/airing",
    "/api/insights", "/api/dashboard/widgets", "/api/state/wall",
    "/api/tracker/summary", "/api/tracker/items", "/api/tracker/history/titles", "/api/tracker/months",
    "/api/tracker/shows", "/api/tracker/overview", "/api/tracker/overview/shows", "/api/tracker/notes", "/api/tracker/stats",
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

  async function read(url, fetcher, {signal, active = () => !document.hidden, watch = true, fresh = false, refresh = false, trackRevision = false, interval = 30000, maxAge = 0, cacheScope = ""} = {}) {
    const parsed = new URL(url, location.origin);
    parsed.searchParams.set("background", "1");
    parsed.searchParams.delete("known_version");
    parsed.searchParams.delete("t");
    const key = parsed.pathname + parsed.search;
    const snapshotKey = JSON.stringify([cacheScope, retryKey(url)]);
    const group = parsed.pathname + (parsed.searchParams.has("summary_only") ? ":summary" : "") + (parsed.pathname === "/api/insights" ? `:${parsed.searchParams.get("include_events") || "1"}` : "") + (parsed.pathname === "/api/dashboard/widgets" ? `:${parsed.searchParams.get("view") || "overview"}` : "") + (watch ? "" : `:once:${parsed.search}`);
    const session = {url, signal, active, busy:false};
    sessions.get(group)?.dispose?.();
    sessions.get(group)?.cancel?.();
    sessions.set(group, session);
    const cancelRefresh = () => { session.busy = false; updateRefresh(); };
    signal?.addEventListener("abort", cancelRefresh, {once:true});
    session.dispose = () => signal?.removeEventListener("abort", cancelRefresh);
    updateRefresh();
    if (watch) clearTimeout(jobs.get(key));
    let retry = retries.delete(retryKey(parsed)) || refresh;
    const scope = generation;
    let previous = "";
    let delivered = false;
    let ready = false;
    let recovering = false;
    const usable = () => scope === generation && sessions.get(group) === session && !signal?.aborted;
    const schedule = delay => {
      if (!watch || !usable()) return;
      clearTimeout(jobs.get(key));
      jobs.set(key, setTimeout(() => {
        jobs.delete(key);
        void poll().catch(failed);
      }, delay));
    };
    const failed = error => {
      cancelRefresh();
      if (error.name === "AbortError" || !usable()) return;
      delivered = true;
      recovering = true;
      if (error.code === "view_refresh_failed") return;
      announce("cw:view-status", url, {ready, error:"refresh_failed"});
      schedule(30000);
    };
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
      session.busy = true;
      updateRefresh();
      dropSnapshot(snapshotKey);
      const snapshotVersion = snapshotGeneration;
      const data = await fetcher(key + (retry ? "&retry_view=1" : ""));
      retry = false;
      if (!usable()) throw abort();
      const meta = data?._view;
      dropSnapshot(snapshotKey);
      if (maxAge > 0 && snapshotVersion === snapshotGeneration && data?.ok !== false && !data?.not_modified && meta?.ready && !meta.refreshing && !meta.error) {
        const body = JSON.stringify(data);
        if (body.length <= 1024 * 1024) {
          snapshots.set(snapshotKey, {body, time:Date.now(), url:retryKey(url)});
          snapshotBytes += body.length;
          while (snapshots.size > 32 || snapshotBytes > 4 * 1024 * 1024) dropSnapshot(snapshots.keys().next().value);
        }
      }
      session.busy = !!meta?.refreshing && !meta?.error;
      updateRefresh();
      announce("cw:view-status", url, meta || {});
      if (!meta) return data;
      ready = !!meta.ready;
      if (meta.error) recovering = true;
      if (meta.ready) {
        const version = trackRevision ? `${meta.version}:${meta.revision || ""}` : meta.version;
        if (delivered && (previous !== version || recovering && !meta.error)) announce("cw:view-updated", url, data);
        previous = version;
        recovering = !!meta.error;
      }
      if (!usable()) return data;
      if (meta.error) {
        schedule(Math.max(1, Number(meta.retry_after) || 30) * 1000);
      } else {
        if (meta.ready && (!fresh || !meta.refreshing)) {
          schedule(meta.refreshing ? Math.max(1, Number(meta.retry_after) || 2) * 1000 : interval);
        } else {
          await new Promise(resolve => setTimeout(resolve, Math.max(1, Number(meta.retry_after) || 2) * 1000));
          return poll();
        }
      }
      if (!meta.ready || (fresh && meta.error)) throw Object.assign(new Error(T("common.viewCache.notPrepared")), {code:"view_refresh_failed"});
      delivered = true;
      return data;
    };
    try {
      const saved = snapshots.get(snapshotKey);
      const age = saved ? Date.now() - saved.time : Infinity;
      if (maxAge > 0 && !fresh && !retry && usable() && active() && age < maxAge) {
        const data = JSON.parse(saved.body);
        ready = !!data._view?.ready;
        previous = trackRevision ? `${data._view?.version}:${data._view?.revision || ""}` : data._view?.version;
        delivered = true;
        announce("cw:view-status", url, data._view || {});
        schedule(Math.max(1, Math.min(interval, maxAge) - age));
        return data;
      }
      return await poll();
    }
    catch (error) { failed(error); throw error; }
    finally {
      if (!watch && sessions.get(group) === session) {
        session.dispose();
        sessions.delete(group);
        updateRefresh();
      }
    }
  }

  function invalidate() {
    generation++;
    for (const timer of jobs.values()) clearTimeout(timer);
    jobs.clear();
    retries.clear();
    discard();
    for (const session of sessions.values()) { session.dispose?.(); session.cancel?.(); }
    sessions.clear();
    updateRefresh();
    statuses = new WeakMap();
    document.querySelectorAll("[data-view-status]").forEach(node => node.remove());
  }

  function status(host, meta, label = "items", retry, source = label) {
    if (!host) return;
    let current = statuses.get(host);
    if (!current) { current = new Map(); statuses.set(host, current); }
    current.set(source, {meta, retry});
    const selected = [...current.values()].find(value => value.meta.error);
    meta = selected?.meta || {};
    retry = selected?.retry;
    host.classList.add("cw-view-host");
    let node = host.querySelector(":scope > [data-view-status]");
    if (!node) {
      node = document.createElement("div");
      node.dataset.viewStatus = "";
      node.className = "cw-view-status";
      node.setAttribute("role", "status");
      host.prepend(node);
    }
    node.hidden = !selected;
    node.replaceChildren();
    if (!selected) return;
    node.dataset.state = "error";
    const message = meta.ready ? T("common.viewCache.refreshFailed") : T("common.viewCache.loadFailed", {label});
    const text = document.createElement("span");
    text.className = "cw-view-status-label";
    text.textContent = message;
    node.append(text);
    if (meta.error && retry) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "cw-btn";
      button.textContent = T("common.viewCache.retry");
      button.addEventListener("click", retry, {once:true});
      node.append(button);
    }
  }

  window.CW = window.CW || {};
  window.CW.ViewCache = {read, supported, invalidate, discard, status, bindRefresh, retry:url => {
    const key = retryKey(url);
    retries.add(key);
    for (const [savedKey, saved] of snapshots) if (saved.url === key) dropSnapshot(savedKey);
  }};
  document.addEventListener("visibilitychange", updateRefresh);
  window.addEventListener("cw:sync-state-cleared", invalidate);
  window.addEventListener("cw:overview-profile-changed", invalidate);
})();
