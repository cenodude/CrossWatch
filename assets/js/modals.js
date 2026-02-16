/* assets/js/modals.js */
/* CrossWatch - JavaScript Modal Management Module */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const _cwGetV = () => {
  try {
    return (window.__CW_VERSION__ || window.__CW_BUILD__ || new URL(import.meta.url).searchParams.get('v') || Date.now());
  } catch {
    return (window.__CW_VERSION__ || window.__CW_BUILD__ || Date.now());
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
ModalRegistry.register('analyzer',     () => import(_cwVer('./modals/analyzer/index.js')));
ModalRegistry.register('exporter',     () => import(_cwVer('./modals/exporter/index.js')));
ModalRegistry.register('maintenance',  () => import(_cwVer('./modals/maintenance/index.js')));
ModalRegistry.register('insight-settings', () => import(_cwVer('./modals/insight-settings/index.js')));
ModalRegistry.register('tls-cert',     () => import(_cwVer('./modals/tls/index.js')));
ModalRegistry.register('setup-wizard', () => import(_cwVer('./modals/setup-wizard/index.js')));
ModalRegistry.register('upgrade-warning', () => import(_cwVer('./modals/upgrade-warning/index.js')));
ModalRegistry.register('capture-compare', () => import(_cwVer('./modals/capture-compare/index.js')));

export const openModal = ModalRegistry.open;
export const closeModal = ModalRegistry.close;

window.openPairModal = (pairOrId) => ModalRegistry.open('pair-config', { pairOrId });
window.cxEditPair = (id) => ModalRegistry.open('pair-config', { pairOrId: id });
window.closePairModal = () => ModalRegistry.close();
window.cxCloseModal = () => ModalRegistry.close();

window.openAbout = (props = {}) => ModalRegistry.open('about', props);
window.closeAbout = () => ModalRegistry.close();

window.openAnalyzer = (props = {}) => ModalRegistry.open('analyzer', props);
window.openExporter = (props = {}) => ModalRegistry.open('exporter', props);

window.openMaintenanceModal = (props = {}) => ModalRegistry.open('maintenance', props);
window.openTlsCertModal = (props = {}) => ModalRegistry.open('tls-cert', props);

window.openSetupWizard = (props = {}) => ModalRegistry.open('setup-wizard', props);
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
