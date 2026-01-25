// Modal host

const _cwV = (() => {
  try { return new URL(import.meta.url).searchParams.get('v') || window.__CW_VERSION__ || window.__CW_BUILD__ || Date.now(); }
  catch { return window.__CW_VERSION__ || window.__CW_BUILD__ || Date.now(); }
})();

const _cwVer = (u) => u + (u.includes('?') ? '&' : '?') + 'v=' + encodeURIComponent(String(_cwV));

const { clampRectToViewport, trapFocus } = await import(_cwVer('./state.js'));

export class ModalHost {
  constructor() {
    this.backdrop = null;
    this.shell = null;
    this.api = null;
    this._drag = { active: false };
    this._foreign = null;
    this._pmove = null;
    this._pup = null;
    this._resize = null;
    this._esc = null;
  }

  _ensure() {
    if (this.backdrop) return;
    const b = document.createElement('div');
    b.className = 'cx-backdrop';
    b.addEventListener('click', (e) => { if (e.target === b) this.unmount(); });

    const s = document.createElement('div');
    s.className = 'cx-modal-shell';
    s.tabIndex = -1;

    b.appendChild(s);
    b.addEventListener('cw-modal-close', () => this.unmount());
    document.body.appendChild(b);

    this.backdrop = b;
    this.shell = s;
    document.body.dataset.cxModalOpen = '1';

    // drag via .cx-head
    s.addEventListener('pointerdown', (e) => this._onDown(e), true);
    this._pmove = (e) => this._onMove(e);
    this._pup = () => this._onUp();
    this._resize = () => this._clamp();
    window.addEventListener('pointermove', this._pmove, true);
    window.addEventListener('pointerup', this._pup, true);
    window.addEventListener('resize', this._resize, { passive: true });

    this._esc = (e) => { if (e.key === 'Escape') this.unmount(); };
    document.addEventListener('keydown', this._esc);

    // global close helper for modal content
    window.cxCloseModal = () => this.unmount();
  }

  _hideForeign() {
    if (this._foreign) return;
    const kills = [];
    const ids = ['save-fab', 'save-frost', 'savebar'];
    for (const id of ids) {
      const n = document.getElementById(id);
      if (n && n.parentNode) {
        const anchor = document.createComment('cx-anchor-' + id);
        n.parentNode.insertBefore(anchor, n);
        n.parentNode.removeChild(n);
        kills.push({ node: n, anchor });
      }
    }
    this._foreign = kills;
    document.body.classList.add('cx-modal-open');
  }

  _restoreForeign() {
    try {
      for (const k of (this._foreign || [])) {
        const { node, anchor } = k || {};
        if (anchor && anchor.parentNode) anchor.replaceWith(node);
      }
    } finally {
      this._foreign = null;
      document.body.classList.remove('cx-modal-open');
    }
  }

  async mount(api, props = {}) {
    this._ensure();
    this.api = api;
    this.shell.innerHTML = '';

    try {
      const shell = this.shell;
      await api.mount(shell, props);
      if (!this.shell || !this.shell.isConnected) return;

      // center then clamp
      this.shell.style.left = '50%';
      this.shell.style.top = '50%';
      this.shell.style.transform = 'translate(-50%,-50%)';

      if (typeof trapFocus === 'function') trapFocus(this.shell);
      this.shell.focus?.({ preventScroll: true });

      this._hideForeign();
      this._clamp();
    } catch (err) {
      console.error('ModalHost.mount failed:', err);
    }
  }

  unmount() {
    try { this.api?.unmount?.(); } catch {}
    this.api = null;

    // remove listeners
    if (this._pmove) window.removeEventListener('pointermove', this._pmove, true);
    if (this._pup) window.removeEventListener('pointerup', this._pup, true);
    if (this._resize) window.removeEventListener('resize', this._resize);
    if (this._esc) document.removeEventListener('keydown', this._esc);
    this._pmove = this._pup = this._resize = this._esc = null;

    // cleanup globals
    if (window.cxCloseModal) delete window.cxCloseModal;

    this._restoreForeign();
    this.backdrop?.remove();
    this.backdrop = null;
    this.shell = null;
    delete document.body.dataset.cxModalOpen;
  }

  _onDown(e) {
    const head = e.target.closest?.('.cx-head');
    if (!head) return;
    if (/INPUT|TEXTAREA|SELECT|BUTTON/.test(e.target.tagName)) return;
    const r = this.shell.getBoundingClientRect();
    this._drag = {
      active: true,
      x: e.clientX, y: e.clientY,
      left: r.left, top: r.top,
      id: e.pointerId || null
    };
    this.shell.style.transform = 'translate(0,0)';
    head.setPointerCapture?.(this._drag.id);
    e.preventDefault();
  }

  _onMove(e) {
    if (!this._drag.active) return;
    const dx = e.clientX - this._drag.x;
    const dy = e.clientY - this._drag.y;
    const r = this.shell.getBoundingClientRect();
    const nxt = { left: this._drag.left + dx, top: this._drag.top + dy, width: r.width, height: r.height };
    const c = clampRectToViewport(nxt);
    this.shell.style.left = c.left + 'px';
    this.shell.style.top = c.top + 'px';
  }

  _onUp() {
    this._drag.active = false;
  }

  _clamp() {
    if (!this.shell) return;
    requestAnimationFrame(() => {
      if (!this.shell) return;
      const r = this.shell.getBoundingClientRect();
      const c = clampRectToViewport({ left: r.left, top: r.top, width: r.width, height: r.height });
      const needsAdjust = Math.abs(c.left - r.left) > 1 || Math.abs(c.top - r.top) > 1;
      if (needsAdjust) {
        this.shell.style.transform = 'translate(0,0)';
        this.shell.style.left = c.left + 'px';
        this.shell.style.top = c.top + 'px';
      }
    });
  }
}
