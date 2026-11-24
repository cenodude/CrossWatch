/* assets/js/page-return.js */
/* CrossWatch - Page return navigation */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const backLabel = (key, english) => (globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n)?.t?.(key) ?? english;
const BACK_KEYS = {'Main':'navigation.tabs.main', 'Captures':'navigation.tabs.captures', 'Playlists':'navigation.tabs.playlists', 'Editor':'navigation.pages.editor', 'Sync Analyzer':'navigation.back.analyzer', 'Backup & Restore':'navigation.pages.backups', 'Maintenance tools':'navigation.pages.maintenance', 'Tracker':'navigation.tabs.tracker', 'Events':'navigation.pages.events', 'Logs':'navigation.pages.logs', 'Import and Export':'navigation.back.importExport', 'Interactive Sync':'navigation.pages.interactiveSync', 'Settings':'navigation.tabs.settings', 'Connections':'navigation.settingsMenu.connections', 'Sync pairs':'navigation.settingsMenu.pairs', 'Scrobbler':'navigation.settingsMenu.scrobbler', 'Scheduling':'navigation.settingsMenu.scheduling', 'UI and Security':'navigation.settingsMenu.app', 'Maintenance':'navigation.settingsMenu.maintenance', 'Hub':'navigation.tabs.hub', 'Profile':'navigation.account.profileFallback'};
const localBack = link => ({...link, label: BACK_KEYS[link.label] ? backLabel(BACK_KEYS[link.label], link.label) : link.label});

export function pageBackLink(returnTo, currentHref, source = 'logs') {
  return localBack(rawBackLink(returnTo, currentHref, source));
}

function rawBackLink(returnTo, currentHref, source) {
  const fallback = {href:'#main', label:'Main'};
  if (!returnTo) return fallback;
  try {
    const current = new URL(currentHref), target = new URL(returnTo, current);
    if (!['http:', 'https:'].includes(target.protocol) || target.origin !== current.origin || target.username || target.password) return fallback;
    const path = target.hash.slice(1).split('?')[0];
    if (path === source) return fallback;
    const labels = new Map([
      ['', 'Main'], ['main', 'Main'],
      ['snapshots', 'Captures'], ['playlists', 'Playlists'], ['editor', 'Editor'], ['analyzer', 'Sync Analyzer'],
      ['backups', 'Backup & Restore'], ['maintenance', 'Maintenance tools'], ['tracker', 'Tracker'], ['events', 'Events'], ['logs', 'Logs'], ['import_export', 'Import and Export'], ['interactive_sync', 'Interactive Sync'],
      ['settings', 'Settings'], ['settings/overview', 'Settings'], ['settings/providers', 'Connections'],
      ['settings/sync', 'Sync pairs'], ['settings/pairs', 'Sync pairs'], ['settings/scrobbler', 'Scrobbler'],
      ['settings/scheduling', 'Scheduling'], ['settings/app', 'UI and Security'], ['settings/maintenance', 'Maintenance'],
    ]);
    const label = target.pathname === '/hub' ? 'Hub' : target.pathname === '/profile' ? 'Profile' : target.pathname === '/' ? labels.get(path) : null;
    if (!label) return fallback;
    // Keep untrusted URL fields behind literal internal prefixes at every navigation sink.
    const fragment = target.hash.slice(1);
    const hash = fragment ? '#' + fragment : '';
    let href;
    if (target.pathname === current.pathname && target.search === current.search) {
      href = fragment ? '#' + fragment : '#main';
    } else if (target.pathname === '/hub') {
      href = '/hub' + (target.search ? '?' + target.search.slice(1) : '') + hash;
    } else if (target.pathname === '/profile') {
      href = '/profile' + (target.search ? '?' + target.search.slice(1) : '') + hash;
    } else {
      href = target.search ? '/?' + target.search.slice(1) + hash : fragment ? '/#' + fragment : '/';
    }
    return {href, label};
  } catch { return fallback; }
}

export async function returnFromEvents(props) {
  const back = pageBackLink(props.returnTo, location.href, 'events');
  const target = new URL(back.href, location.href);
  if (target.pathname !== location.pathname || target.search !== location.search || !window.showTab) {
    location.href = target.href;
    return;
  }
  history.pushState(null,'',target.href);
  const route = target.hash.slice(1).split('?')[0];
  const [tab, pane] = route.split('/');
  if (tab === 'settings') window.__cwSettingsPane = pane || 'overview';
  await window.showTab(tab || 'main');
}
