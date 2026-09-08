/* assets/js/page-return.js */
/* CrossWatch - Page return navigation */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

export function pageBackLink(returnTo, currentHref, source = 'logs') {
  const fallback = {href:'#main', label:'Main'};
  if (!returnTo) return fallback;
  try {
    const current = new URL(currentHref), target = new URL(returnTo, current);
    if (!['http:', 'https:'].includes(target.protocol) || target.origin !== current.origin || target.username || target.password) return fallback;
    const path = target.hash.slice(1).split('?')[0];
    if (path === source) return fallback;
    const labels = new Map([
      ['', 'Main'], ['main', 'Main'], ['watchlist', 'Watchlist'], ['playback_progress', 'Playback'],
      ['snapshots', 'Captures'], ['playlists', 'Playlists'], ['editor', 'Editor'], ['analyzer', 'Sync Analyzer'],
      ['maintenance', 'Maintenance tools'], ['events', 'Events'], ['logs', 'Logs'], ['import_export', 'Import and Export'], ['interactive_sync', 'Interactive Sync'],
      ['settings', 'Settings'], ['settings/overview', 'Settings'], ['settings/providers', 'Connections'],
      ['settings/sync', 'Sync pairs'], ['settings/pairs', 'Sync pairs'], ['settings/scrobbler', 'Scrobbler'],
      ['settings/scheduling', 'Scheduling'], ['settings/app', 'UI and Security'], ['settings/maintenance', 'Maintenance'],
    ]);
    const label = target.pathname === '/profile' ? 'Profile' : target.pathname === '/' ? labels.get(path) : null;
    if (!label) return fallback;
    // Keep untrusted URL fields behind literal internal prefixes at every navigation sink.
    const fragment = target.hash.slice(1);
    const hash = fragment ? '#' + fragment : '';
    let href;
    if (target.pathname === current.pathname && target.search === current.search) {
      href = fragment ? '#' + fragment : '#main';
    } else if (target.pathname === '/profile') {
      href = '/profile' + (target.search ? '?' + target.search.slice(1) : '') + hash;
    } else {
      href = target.search ? '/?' + target.search.slice(1) + hash : fragment ? '/#' + fragment : '/';
    }
    return {href, label};
  } catch { return fallback; }
}

export function statisticsReturn(value) {
  try {
    const data = typeof value === 'string' ? JSON.parse(value) : value;
    if (data?.modal !== 'statistics') return null;
    return {
      modal:'statistics',
      day:Number.isInteger(data.day) && data.day >= 0 && data.day < 1000000 ? data.day : null,
      range:['3m','6m','12m'].includes(data.range) ? data.range : '12m',
      metric:['changes','runs','failed'].includes(data.metric) ? data.metric : 'changes',
      filter:typeof data.filter === 'string' ? data.filter.slice(0,200) : '',
      expanded:Array.isArray(data.expanded) ? data.expanded.filter(v=>typeof v==='string').slice(0,300) : [],
    };
  } catch { return null; }
}

export async function returnFromEvents(props) {
  const back = pageBackLink(props.returnTo, location.href, 'events');
  const modal = statisticsReturn(props.returnContext);
  const target = new URL(back.href, location.href);
  if (target.pathname !== location.pathname || target.search !== location.search || !window.showTab) {
    if (modal) {
      try { sessionStorage.setItem('cw.events.pendingReturn',JSON.stringify({href:target.href,modal})); } catch {}
    }
    location.href = target.href;
    return;
  }
  history.pushState(null,'',target.href);
  const route = target.hash.slice(1).split('?')[0];
  const [tab, pane] = route.split('/');
  if (tab === 'settings') window.__cwSettingsPane = pane || 'overview';
  await window.showTab(tab || 'main');
  if (modal) await window.openStatisticsModal?.(modal);
}

export function resumeEventsReturn() {
  try {
    const saved = JSON.parse(sessionStorage.getItem('cw.events.pendingReturn') || 'null');
    sessionStorage.removeItem('cw.events.pendingReturn');
    if (saved?.href !== location.href) return;
    const modal = statisticsReturn(saved.modal);
    if (modal) window.openStatisticsModal?.(modal);
  } catch {}
}
