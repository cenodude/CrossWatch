/* assets/js/tracker-month-notifications.js */
/* CrossWatch - Completion notifications for monthly and yearly reviews left while preparing */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

export function monthNotifications(scope, {yearly = false} = {}) {
  const source = yearly ? "cw-wrapped" : "month-reviews", endpoint = yearly ? "wrapped" : "month", parameter = yearly ? "year" : "month";
  const waiting = new Map(), ready = new Map();
  let timer, controller;
  let owner = scope();
  const key = (instance, month) => JSON.stringify([instance, month]);
  const render = () => window.CW?.Notifications?.setSource(source, {items:[...ready.values()]});
  function schedule() {
    clearTimeout(timer);
    if (!document.hidden && [...waiting.values()].some(item => item.attempts < 30)) timer = setTimeout(refresh, 10000);
  }
  async function refresh() {
    clearTimeout(timer);
    if (document.hidden || controller) return;
    const request = controller = new AbortController();
    const timeout = setTimeout(() => request.abort(), 15000);
    try {
      for (const [id, item] of waiting) {
        if (request.signal.aborted) break;
        if (item.scope !== scope()) {waiting.delete(id); continue;}
        if (item.attempts >= 30) continue;
        item.attempts++;
        try {
          const response = await fetch(`/api/tracker/overview/${endpoint}?provider_instance=${encodeURIComponent(item.instance)}&${parameter}=${encodeURIComponent(item.month)}`, {credentials:"same-origin", cache:"no-store", signal:request.signal});
          if ([401,403,404].includes(response.status)) {waiting.delete(id); continue;}
          if (!response.ok) continue;
          const data = await response.json();
          if (data.disabled) {waiting.delete(id); ready.delete(id); render(); continue;}
          if (request.signal.aborted || waiting.get(id) !== item || item.scope !== scope()) continue;
          if (!data.report || data.pending || data.stale || data.error) continue;
          waiting.delete(id);
          const month = yearly ? data.year : data.month;
          if (!(yearly ? /^\d{4}$/ : /^\d{4}-(0[1-9]|1[0-2])$/).test(month)) continue;
          const name = yearly ? month : new Date(`${month}-02T12:00:00`).toLocaleDateString(undefined, {month:"long"});
          const readyId = key(item.instance, month);
          ready.set(readyId, {id:readyId, revision:String(data.generated || month), title:yearly ? `Your ${month} CrossWatch Wrapped is ready` : `Your ${name} review is ready`, detail:`${item.label} · ${month.slice(0,4)}`, icon:"auto_awesome", action:"View review", href:`#tracker?instance=${encodeURIComponent(item.instance)}&${parameter}=${encodeURIComponent(month)}`});
          while (ready.size > 8) ready.delete(ready.keys().next().value);
          render();
        } catch {}
      }
    } finally {
      clearTimeout(timeout);
      if (controller === request) controller = null;
      schedule();
    }
  }
  function reset() {
    clearTimeout(timer); controller?.abort(); waiting.clear(); ready.clear();
    window.CW?.Notifications?.removeSource(source);
  }
  function reconcile() {
    const current = scope();
    if (current !== owner) {reset(); owner = current;}
  }
  function resume() {
    reconcile();
    if (document.hidden) return;
    for (const item of waiting.values()) item.attempts = 0;
    render();
    void refresh();
  }
  for (const event of ["auth-changed","cw:auth-state-changed","cw:overview-profile-changed"]) window.addEventListener(event, () => {
    reconcile();
    queueMicrotask(() => {reconcile(); render(); schedule();});
  });
  for (const event of ["focus","cw:notifications-refresh"]) window.addEventListener(event, resume);
  document.addEventListener("visibilitychange", resume);
  document.addEventListener("config-saved", reset);
  return {
    leave(instance, month, label, owner) {
      reconcile();
      if (owner !== scope()) return;
      const id = key(instance, month);
      waiting.set(id, {instance, month, label, scope:owner, attempts:0});
      while (waiting.size > 8) waiting.delete(waiting.keys().next().value);
      schedule();
    },
    viewing(instance, month) {
      reconcile();
      for (const [id, item] of waiting) if (item.instance === instance && (!item.month || item.month === month)) waiting.delete(id);
      ready.delete(key(instance, month)); render(); schedule();
    },
  };
}
