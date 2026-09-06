/* assets/js/modals.js */
/* CrossWatch - JavaScript Modal Management Module */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

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
ModalRegistry.register('events',       () => import(_cwVer('./modals/events/index.js')));
ModalRegistry.register('maintenance',  () => import(_cwVer('./modals/maintenance/index.js')));
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
window.openEvents = (props = {}) => ModalRegistry.open('events', props);
window.openStatisticsModal = (props = {}) => ModalRegistry.open('statistics', props);
window.openExporter = () => window.showTab ? window.showTab('import_export') : (location.hash = 'import_export');

window.openMaintenanceModal = (props = {}) => ModalRegistry.open('maintenance', props);
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
