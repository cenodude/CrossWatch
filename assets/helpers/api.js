/* helpers/api.js */
/* Refactored and expanded API helper with memoization and timeout handling */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function(){
  const JSON_HDR = { "Content-Type": "application/json" };
  const NS = (window.CW ||= {});

  function withTimeout(ms = 9000){
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort('timeout'), ms);
    return { signal: ac.signal, done: () => clearTimeout(timer) };
  }

  async function f(url, opt = {}, ms = 9000){
    const t = withTimeout(ms);
    try { return await fetch(url, { cache: 'no-store', ...opt, signal: t.signal }); }
    finally { t.done(); }
  }

  async function j(url, opt = {}, ms = 9000){
    const r = await f(url, opt, ms);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    const ct = r.headers.get('content-type') || '';
    return ct.includes('json') ? r.json() : r.text();
  }

  const _memo = new Map();

  function _slot(key){
    if (!_memo.has(key)) _memo.set(key, { ts: 0, value: null, pending: null });
    return _memo.get(key);
  }

  async function memo(key, ttl, loader, force = false){
    const s = _slot(key);
    const now = Date.now();
    if (!force && s.value !== null && (now - s.ts) < ttl) return s.value;
    if (s.pending) return s.pending;
    s.pending = Promise.resolve().then(loader).then((value) => {
      s.value = value;
      s.ts = Date.now();
      return value;
    }).finally(() => { s.pending = null; });
    return s.pending;
  }

  function setMemo(key, value){
    const s = _slot(key);
    s.value = value;
    s.ts = Date.now();
    s.pending = null;
  }

  function invalidate(keys){
    const list = !keys ? [..._memo.keys()] : (Array.isArray(keys) ? keys : [keys]);
    list.forEach((key) => {
      const s = _memo.get(key);
      if (!s) return;
      s.ts = 0;
      s.value = null;
      s.pending = null;
    });
  }

  const KEY = {
    config: 'config',
    pairs: 'pairs',
    providers: 'providers',
    metadataProviders: 'metadataProviders',
    schedulingStatus: 'schedulingStatus',
    watchStatus: 'watchStatus',
    currentlyWatching: 'currentlyWatching',
    status: 'status',
    insights: 'insights',
  };

  const TTL = {
    config: 60000,
    pairs: 15000,
    providers: 30000,
    metadataProviders: 30000,
    schedulingStatus: 3000,
    watchStatus: 3000,
    currentlyWatching: 3000,
    status: 3000,
    insights: 10000,
  };

  const Cache = {
    memo,
    invalidate,
    setCfg(v){ setMemo(KEY.config, v); try { window._cfgCache = v; } catch {} },
    getCfg(){ return _slot(KEY.config).value || window._cfgCache || null; },
  };

  const Config = {
    load(force = false){
      return memo(KEY.config, TTL.config, async () => {
        const cfg = await j('/api/config');
        try { window._cfgCache = cfg; } catch {}
        return cfg;
      }, force);
    },
    async save(cfg){
      const out = await j('/api/config', { method: 'POST', headers: JSON_HDR, body: JSON.stringify(cfg || {}) });
      Cache.setCfg(cfg || {});
      invalidate([KEY.pairs, KEY.metadataProviders, KEY.schedulingStatus, KEY.watchStatus, KEY.currentlyWatching, KEY.status, KEY.insights]);
      return out;
    }
  };

  const Pairs = {
    list(force = false){ return memo(KEY.pairs, TTL.pairs, () => j('/api/pairs'), force); },
    async save(p){
      const has = !!(p && p.id);
      const url = has ? `/api/pairs/${encodeURIComponent(p.id)}` : '/api/pairs';
      const out = await j(url, { method: has ? 'PUT' : 'POST', headers: JSON_HDR, body: JSON.stringify(p || {}) });
      invalidate([KEY.pairs, KEY.status, KEY.insights]);
      return out;
    },
    async delete(id){
      if (!id) return null;
      const out = await j(`/api/pairs/${encodeURIComponent(id)}`, { method: 'DELETE' });
      invalidate([KEY.pairs, KEY.status, KEY.insights]);
      return out;
    }
  };

  const Providers = {
    list(force = false){ return memo(KEY.providers, TTL.providers, () => j('/api/sync/providers'), force); },
    html(){ return j('/api/metadata/providers/html'); }
  };

  const Metadata = {
    providers(force = false){ return memo(KEY.metadataProviders, TTL.metadataProviders, () => j('/api/metadata/providers'), force); }
  };

  const Scheduling = {
    status(force = false){ return memo(KEY.schedulingStatus, TTL.schedulingStatus, () => j('/api/scheduling/status'), force); }
  };

  const Watch = {
    status(force = false){ return memo(KEY.watchStatus, TTL.watchStatus, () => j('/api/watch/status'), force); },
    currentlyWatching(force = false){ return memo(KEY.currentlyWatching, TTL.currentlyWatching, () => j('/api/watch/currently_watching'), force); }
  };

  const Status = { get(force = false){ return memo(KEY.status, TTL.status, () => j('/api/status'), force); } };
  const Insights = { get(force = false){ return memo(KEY.insights, TTL.insights, () => j('/api/insights'), force); } };

  document.addEventListener('config-saved', () => invalidate());
  window.addEventListener('auth-changed', () => invalidate());

  Object.assign(NS, { API: { j, f, Config, Pairs, Providers, Metadata, Scheduling, Watch, Status, Insights }, Cache });
})();
