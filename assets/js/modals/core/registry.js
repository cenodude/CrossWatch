// modal registry wrapper

const _cwV = (() => {
  try { return new URL(import.meta.url).searchParams.get('v') || window.__CW_VERSION__ || window.__CW_BUILD__ || Date.now(); }
  catch { return window.__CW_VERSION__ || window.__CW_BUILD__ || Date.now(); }
})();

const _cwVer = (u) => u + (u.includes('?') ? '&' : '?') + 'v=' + encodeURIComponent(String(_cwV));

const { ModalHost } = await import(_cwVer('./host.js'));

const reg = new Map();
let host = null;

export const ModalRegistry = {
  register(name, loader) { reg.set(name, loader); },
  async open(name, props = {}) {
    const loader = reg.get(name);
    if (!loader) throw new Error('Unknown modal: ' + name);
    if (!host) host = new ModalHost();
    const mod = await loader();
    const api = mod.default?.mount ? mod.default : mod;
    await host.mount(api, props);
  },
  close() { host?.unmount(); }
};
