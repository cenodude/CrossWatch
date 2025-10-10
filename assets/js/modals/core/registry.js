// Simple modal registry with a single host
import { ModalHost } from './host.js';
const reg = new Map(); let host=null;

export const ModalRegistry = {
  register(name, loader){ reg.set(name, loader); },
  async open(name, props={}){
    const loader = reg.get(name); if(!loader) throw new Error('Unknown modal: '+name);
    if(!host) host = new ModalHost();
    const mod = await loader(); const api = mod.default?.mount ? mod.default : mod;
    await host.mount(api, props);
  },
  close(){ host?.unmount(); }
};
