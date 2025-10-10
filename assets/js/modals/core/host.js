// Modal host: backdrop, ESC, bounded drag, clamp on resize
// Also hides any external sticky save bars while a modal is open.
import { clampRectToViewport, trapFocus } from './state.js';

export class ModalHost {
  constructor(){
    this.backdrop = null;
    this.shell = null;
    this.api = null;
    this._drag = { active:false };
    this._foreign = null;
  }

  _ensure(){
    if (this.backdrop) return;
    const b = document.createElement('div');
    b.className = 'cx-backdrop';
    b.addEventListener('click', (e) => { if (e.target === b) this.unmount(); });

    const s = document.createElement('div');
    s.className = 'cx-modal-shell';
    s.tabIndex = -1;

    b.appendChild(s);
    document.body.appendChild(b);

    this.backdrop = b;
    this.shell = s;
    document.body.dataset.cxModalOpen = '1';

    // drag via .cx-head
    s.addEventListener('pointerdown', (e) => this._onDown(e), true);
    window.addEventListener('pointermove', (e) => this._onMove(e), true);
    window.addEventListener('pointerup', () => this._onUp(), true);
    window.addEventListener('resize', () => this._clamp(), { passive:true });
    window.addEventListener('keydown', (e) => { if (e.key === 'Escape') this.unmount(); });
  }

  _hideForeign(){
    if (this._foreign) return;
    const kills = [];
    const ids = ['save-fab','save-frost','savebar']; // common ids in the app
    for (const id of ids){
      const n = document.getElementById(id);
      if (n && n.parentNode){
        const anchor = document.createComment('cx-anchor-'+id);
        n.parentNode.insertBefore(anchor, n);
        n.parentNode.removeChild(n);
        kills.push({ node:n, anchor });
      }
    }
    this._foreign = kills;
    document.body.classList.add('cx-modal-open');
  }

  _restoreForeign(){
    try {
      for (const k of (this._foreign || [])){
        const { node, anchor } = k || {};
        if (anchor && anchor.parentNode) anchor.replaceWith(node);
      }
    } finally {
      this._foreign = null;
      document.body.classList.remove('cx-modal-open');
    }
  }

  async mount(api, props = {}){
    this._ensure();              // make sure shell exists
    this.api = api;
    this.shell.innerHTML = '';

    try {
      const shell = this.shell;  // snapshot in case 'api.mount' closes things
      await api.mount(shell, props);

      // If modal closed itself during mount, bail out gracefully
      if (!this.shell || !this.shell.isConnected) return;

      // center then clamp
      this.shell.style.left = '50%';
      this.shell.style.top = '50%';
      this.shell.style.transform = 'translate(-50%,-50%)';

      // focus & traps only when shell still alive
      if (typeof trapFocus === 'function') trapFocus(this.shell);
      this.shell.focus?.({ preventScroll: true });

      this._hideForeign();
      this._clamp();
    } catch (err) {
      console.error('ModalHost.mount failed:', err);
    }
  }

  unmount(){
    try { this.api?.unmount?.(); } catch {}
    this.api = null;
    this._restoreForeign();
    this.backdrop?.remove();
    this.backdrop = null;
    this.shell = null;
    delete document.body.dataset.cxModalOpen;
  }

  _onDown(e){
    const head = e.target.closest?.('.cx-head'); if (!head) return;
    if (/INPUT|TEXTAREA|SELECT|BUTTON/.test(e.target.tagName)) return;
    const r = this.shell.getBoundingClientRect();
    this._drag = {
      active:true,
      x:e.clientX, y:e.clientY,
      left:r.left, top:r.top,
      id:e.pointerId || null
    };
    this.shell.style.transform = 'translate(0,0)';
    head.setPointerCapture?.(this._drag.id);
    e.preventDefault();
  }

  _onMove(e){
    if (!this._drag.active) return;
    const dx = e.clientX - this._drag.x;
    const dy = e.clientY - this._drag.y;
    const r = this.shell.getBoundingClientRect();
    const nxt = { left:this._drag.left+dx, top:this._drag.top+dy, width:r.width, height:r.height };
    const c = clampRectToViewport(nxt);
    this.shell.style.left = c.left+'px';
    this.shell.style.top  = c.top +'px';
  }

  _onUp(){
    this._drag.active = false;
  }

  _clamp(){
    if (!this.shell) return;
    requestAnimationFrame(() => {
      if (!this.shell) return;
      const r = this.shell.getBoundingClientRect();
      const c = clampRectToViewport({ left:r.left, top:r.top, width:r.width, height:r.height });
      const needsAdjust = Math.abs(c.left - r.left) > 1 || Math.abs(c.top - r.top) > 1;
      if (needsAdjust){
        this.shell.style.transform = 'translate(0,0)';
        this.shell.style.left = c.left + 'px';
        this.shell.style.top  = c.top  + 'px';
      }
    });
  }
}
