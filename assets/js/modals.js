// assets/js/modals.js
import { ModalRegistry } from './modals/core/registry.js';

// Cache-buster for dynamic imports
const ver = (u) => u + (u.includes('?') ? '&' : '?') + 'v=' + (window.__CW_BUILD__ || Date.now());

// Register modals (versioned)
ModalRegistry.register('pair-config', () => import(ver('./modals/pair-config/index.js')));
ModalRegistry.register('about',        () => import(ver('./modals/about.js')));
ModalRegistry.register('analyzer',     () => import(ver('./modals/analyzer/index.js')));
ModalRegistry.register('exporter',     () => import(ver('./modals/exporter/index.js')));
ModalRegistry.register('maintenance',  () => import(ver('./modals/maintenance/index.js')));
ModalRegistry.register('setup-wizard', () => import(ver('./modals/setup-wizard/index.js')));
ModalRegistry.register('upgrade-warning', () => import(ver('./modals/upgrade-warning/index.js')));

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
