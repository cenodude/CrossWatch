/* assets/js/analyzer/mapping.js */
/* CrossWatch - Analyzer entry point to the shared mapping workspace */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

async function loadStyles() {
  if (document.querySelector('link[href*="/assets/css/interactive-sync.css"]')) return;
  const link = document.createElement('link');
  link.rel = 'stylesheet';
  link.href = `/assets/css/interactive-sync.css?v=${encodeURIComponent(window.APP_VERSION || '1')}`;
  await new Promise(resolve => {
    link.onload = link.onerror = resolve;
    document.head.append(link);
  });
}

export async function openAnalyzerMapping({reference, signal, onSaved, onClose}) {
  let version = '';
  const request = async (action, extra = {}, options = {}) => {
    const response = await fetch('/api/analyzer/mapping', {
      method:'POST', credentials:'same-origin', cache:'no-store',
      headers:{'Content-Type':'application/json'},
      signal:options.signal ? AbortSignal.any([signal, options.signal]) : signal,
      body:JSON.stringify({...reference, version, action, ...extra}),
    });
    const data = await response.json();
    if (!response.ok || data.ok === false) throw new Error(data.detail || data.error || 'Could not load mapping.');
    return data;
  };
  const [data, module] = await Promise.all([
    request('context'),
    import(`/assets/js/interactive-sync-mapping.js?v=${encodeURIComponent(window.APP_VERSION || '1')}`),
    loadStyles(),
  ]);
  if (signal.aborted) return null;
  version = data.version;
  return module.openMappingWorkspace({
    rows:[data.row], total:1, standalone:true, onSaved, onClose,
    mappingApi:{
      catalogs: () => request('catalogs'),
      search: (_row, q, catalog, options) => request('search', {q, catalog}, options),
      episodes: (edits, options) => request('episodes', {item:edits[0].item}, options),
      save: edits => request('save', {item:edits[0].item}),
    },
  });
}
