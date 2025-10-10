// Small DOM helpers
export const $$ = (sel, root=document)=>Array.from(root.querySelectorAll(sel));
export const $ = (sel, root=document)=>root.querySelector(sel);

// Keep modal fully visible
export function clampRectToViewport({left, top, width, height}){
  const pad = 24;
  const vw = document.documentElement.clientWidth;
  const vh = document.documentElement.clientHeight;
  const maxL = Math.max(pad, vw - width - pad);
  const maxT = Math.max(pad, vh - height - pad);
  return { left: Math.min(Math.max(left, pad), maxL), top: Math.min(Math.max(top, pad), maxT), width, height };
}

// Simple focus trap
export function trapFocus(container){
  const Q = 'a[href],button,textarea,input,select,[tabindex]:not([tabindex="-1"])';
  function list(){ return Array.from(container.querySelectorAll(Q)).filter(n=>!n.disabled && n.offsetParent!==null); }
  container.addEventListener('keydown', (e)=>{
    if(e.key!=='Tab') return;
    const items = list(); if(!items.length) return;
    const first = items[0], last = items[items.length-1];
    if(e.shiftKey && document.activeElement===first){ last.focus(); e.preventDefault(); }
    else if(!e.shiftKey && document.activeElement===last){ first.focus(); e.preventDefault(); }
  });
}
