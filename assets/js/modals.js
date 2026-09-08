/* assets/js/modals.js */
/* CrossWatch - JavaScript Modal Management Module */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import {statisticsReturn, resumeEventsReturn} from './page-return.js';

const _cwGetV = () => {
  try {
    return (window.__CW_VERSION__ || new URL(import.meta.url).searchParams.get('v') || Date.now());
  } catch {
    return (window.__CW_VERSION__ || Date.now());
  }
};

const _cwVer = (u) => {
  const v = encodeURIComponent(String(_cwGetV()));
  return u + (u.includes('?') ? '&' : '?') + 'v=' + v;
};

const { ModalRegistry } = await import(_cwVer('./modals/core/registry.js'));

// Register modals
ModalRegistry.register('pair-config', () => import(_cwVer('./modals/pair-config/index.js')));
ModalRegistry.register('about',        () => import(_cwVer('./modals/about.js')));
ModalRegistry.register('manual-watched', () => import(_cwVer('./modals/manual-watched/index.js')));
ModalRegistry.register('insight-settings', () => import(_cwVer('./modals/insight-settings/index.js')));
ModalRegistry.register('tls-cert',     () => import(_cwVer('./modals/tls/index.js')));
ModalRegistry.register('setup-wizard', () => import(_cwVer('./modals/setup-wizard/index.js')));
ModalRegistry.register('upgrade-warning', () => import(_cwVer('./modals/upgrade-warning/index.js')));
ModalRegistry.register('capture-compare', () => import(_cwVer('./modals/capture-compare/index.js')));
ModalRegistry.register('provider-cleanup', () => import(_cwVer('./modals/provider-cleanup/index.js')));
ModalRegistry.register('scrobbler-webhook', () => import(_cwVer('./modals/scrobbler-webhook/index.js')));
ModalRegistry.register('scrobbler-route', () => import(_cwVer('./modals/scrobbler-route/index.js')));
ModalRegistry.register('editor-raw', () => import(_cwVer('./modals/editor-raw/index.js')));
ModalRegistry.register('anime-overrides', () => import(_cwVer('./modals/anime-overrides/index.js')));
ModalRegistry.register('support',      () => import(_cwVer('./modals/support/index.js')));
ModalRegistry.register('statistics',   () => import(_cwVer('./modals/statistics/index.js')));
ModalRegistry.register('sync-topology', () => import(_cwVer('./modals/sync-topology/index.js')));

export const openModal = ModalRegistry.open;
export const closeModal = ModalRegistry.close;

window.openPairModal = (pairOrId) => ModalRegistry.open('pair-config', { pairOrId });
window.cxEditPair = (id) => ModalRegistry.open('pair-config', { pairOrId: id });
window.closePairModal = () => ModalRegistry.close();
window.cxCloseModal = () => ModalRegistry.close();

window.openAbout = async (props = {}) => {
  const mod = await import(_cwVer('./modals/about.js'));
  return mod.openAboutModal?.(props);
};
window.closeAbout = async () => {
  const mod = await import(_cwVer('./modals/about.js'));
  return mod.closeAboutModal?.();
};

window.openAnalyzer = () => window.showTab ? window.showTab('analyzer') : (location.hash = 'analyzer');
window.openLogs = (props = {}) => {
  const query = new URLSearchParams({channel: props.channel || 'sync'});
  if (props.runId) query.set('runId', props.runId);
  if (props.pairId) query.set('pairId', props.pairId);
  if (props.latest) query.set('latest', '1');
  const returnTo = location.hash.startsWith('#logs')
    ? new URLSearchParams(location.hash.split('?')[1] || '').get('returnTo')
    : location.pathname + location.search + location.hash;
  if (returnTo) query.set('returnTo', returnTo);
  const hash = '#logs?' + query;
  ModalRegistry.close();
  if (!document.getElementById('page-logs') || !window.showTab) { location.href = '/?main=1' + hash; return; }
  if (location.hash !== hash) history.pushState(null, '', hash);
  return window.showTab('logs');
};
window.openEvents = (props = {}) => {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries({ groupId: props.groupId || props.eventGroupId, runId: props.runId || props.run_id, domain: props.domain, visibility: props.visibility, mode: props.mode })) {
    if (value) query.set(key, String(value));
  }
  const previous = location.hash.split('?')[0] === '#events' ? new URLSearchParams(location.hash.split('?')[1] || '') : null;
  const returnTo = previous ? previous.get('returnTo') : location.pathname + location.search + location.hash;
  const returnContext = statisticsReturn(props.returnContext || previous?.get('returnContext'));
  if (returnTo) query.set('returnTo', returnTo);
  if (returnContext) query.set('returnContext', JSON.stringify(returnContext));
  const hash = '#events' + (query.size ? `?${query}` : '');
  ModalRegistry.close();
  if (!document.getElementById('page-events') || !window.showTab) {
    location.href = '/?main=1' + hash;
    return;
  }
  if (location.hash !== hash) history.pushState(null, '', hash);
  return window.showTab('events');
};
window.openStatisticsModal = (props = {}) => ModalRegistry.open('statistics', props);
if (document.readyState === 'complete') resumeEventsReturn();
else window.addEventListener('load', resumeEventsReturn, {once:true});
window.openExporter = () => window.showTab ? window.showTab('import_export') : (location.hash = 'import_export');

window.openMaintenance = (props = {}) => {
  const previous = location.hash.split('?')[0] === '#maintenance'
    ? new URLSearchParams(location.hash.split('?')[1] || '') : null;
  const query = new URLSearchParams();
  const returnTo = previous ? previous.get('returnTo') : location.pathname + location.search + location.hash;
  if (returnTo) query.set('returnTo', returnTo);
  if (props.group || props.target) query.set('group', String(props.group || props.target));
  const hash = '#maintenance' + (query.size ? `?${query}` : '');
  ModalRegistry.close();
  if (!document.getElementById('page-maintenance') || !window.showTab) { location.href = '/?main=1' + hash; return; }
  if (location.hash !== hash) history.pushState(null, '', hash);
  return window.showTab('maintenance');
};
// Keep external callers working while Maintenance is now a page.
window.openMaintenanceModal = window.openMaintenance;
window.openManualWatchedModal = (props = {}) => ModalRegistry.open('manual-watched', props);
window.openTlsCertModal = (props = {}) => ModalRegistry.open('tls-cert', props);

function setupWizardBackdropClass(props = {}) {
  const classes = [props?.backdropClassName];
  if (props?.auth_reset_required !== true) classes.push('cw-welcome-setup-privacy-backdrop');
  return classes.filter(Boolean).join(' ');
}

window.openSetupWizard = (props = {}) => ModalRegistry.open('setup-wizard', {
  ...props,
  backdropClassName: setupWizardBackdropClass(props),
});
window.openUpgradeWarning = (props = {}) => ModalRegistry.open('upgrade-warning', props);

window.cxEnsureCfgModal = async (pairOrId = null) => {
  await ModalRegistry.open('pair-config', { pairOrId });
  return document.getElementById('cx-modal')?.closest('.cx-card') || document.querySelector('.cx-modal-shell');
};

window.cxOpenModalFor = async (pairOrId = null) => {
  await ModalRegistry.open('pair-config', { pairOrId });
  return true;
};

window.openInsightSettingsModal = (props = {}) => ModalRegistry.open('insight-settings', props);
window.openCaptureCompare = (props = {}) => ModalRegistry.open('capture-compare', props);
window.openProviderCleanupModal = (props = {}) => ModalRegistry.open('provider-cleanup', props);
window.openScrobblerWebhookModal = (props = {}) => ModalRegistry.open('scrobbler-webhook', { ...props, dismissible: false });
window.openScrobblerRouteModal = (props = {}) => ModalRegistry.open('scrobbler-route', { ...props, dismissible: false });
window.openEditorRawModal = (props = {}) => ModalRegistry.open('editor-raw', props);
window.openAnimeOverridesModal = (props = {}) => ModalRegistry.open('anime-overrides', props);
window.openSupportModal = (props = {}) => ModalRegistry.open('support', props);
