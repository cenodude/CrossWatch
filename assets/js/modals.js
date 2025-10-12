// Facade: legacy-safe entrypoint mapping old globals to the new modular host.
import { ModalRegistry } from './modals/core/registry.js';

ModalRegistry.register('pair-config', () => import('./modals/pair-config/index.js'));
ModalRegistry.register('about', () => import('./modals/about.js'));
ModalRegistry.register('analyzer', () => import('./modals/analyzer/index.js'));

// Public API + legacy bridges
export const openModal = ModalRegistry.open;
export const closeModal = ModalRegistry.close;

window.openPairModal = (pairOrId) => ModalRegistry.open('pair-config', { pairOrId });
window.cxEditPair = (id) => ModalRegistry.open('pair-config', { pairOrId: id });
window.closePairModal = () => ModalRegistry.close();
window.cxCloseModal = () => ModalRegistry.close();
window.openAbout = (props={}) => ModalRegistry.open('about', props);
window.closeAbout = () => ModalRegistry.close();
window.openAnalyzer = (props={}) => ModalRegistry.open('analyzer', props);

// Compatibility shims for older code
window.cxEnsureCfgModal = async (pairOrId=null)=>{
  await ModalRegistry.open('pair-config', { pairOrId });
  return document.getElementById('cx-modal')?.closest('.cx-card') || document.querySelector('.cx-modal-shell');
};
window.cxOpenModalFor = async (pairOrId=null /* legacy id param is ignored */)=>{
  await ModalRegistry.open('pair-config', { pairOrId });
  return true;
};
