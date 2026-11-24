/* assets/helpers/mobile-experience.js */
/* CrossWatch - Mobile navigation, sheets and viewport behavior */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  const CW = window.CW ||= {};
  if (CW.MobileUI) return;
  const mobile = matchMedia('(max-width:760px)');
  const scrollRestoration = history.scrollRestoration;
  const syncScrollRestoration = () => { history.scrollRestoration = mobile.matches ? 'manual' : scrollRestoration; };
  mobile.addEventListener('change', syncScrollRestoration);
  syncScrollRestoration();
  const layers = [];
  let pending = null;
  let serial = 0;
  let cancelScroll = () => {};
  if (history.state?.cwMobileLayer) {
    const state = {...history.state};
    delete state.cwMobileLayer;
    history.replaceState(state, '', location.href);
  }

  function layer(close, onBack = close) {
    const entry = {id:`mobile-${Date.now()}-${++serial}`, close, onBack, active:false, closed:false};
    const activate = () => {
      if (entry.closed || !mobile.matches) return;
      history.pushState({...history.state, cwMobileLayer:entry.id}, '', location.href);
      layers.push(entry);
      entry.active = true;
    };
    if (pending) pending.promise.then(activate);
    else activate();
    const handle = {
      dismiss(after) {
        if (pending && !entry.closed && entry.active) return pending.promise.then(() => handle.dismiss(after));
        if (!entry.closed) {
          const index = layers.indexOf(entry);
          const removed = index < 0 ? [entry] : layers.splice(index);
          for (const item of removed.reverse()) { item.closed = true; item.close(); }
          if (entry.active) {
            let resolve;
            const promise = new Promise(done => { resolve = done; });
            pending = {promise, resolve};
            history.go(-removed.length);
          }
        }
        return (pending?.promise || Promise.resolve()).then(() => after?.());
      },
    };
    entry.handle = handle;
    return handle;
  }

  async function beforeNavigate() {
    if (!mobile.matches) return;
    if (pending) await pending.promise;
    if (layers.length) await layers[0].handle.dismiss();
  }

  window.addEventListener('popstate', () => {
    const index = layers.findIndex(entry => entry.id === history.state?.cwMobileLayer);
    for (const entry of layers.splice(index + 1).reverse()) { entry.closed = true; entry.onBack(); }
    const finished = pending;
    pending = null;
    finished?.resolve();
  });

  const remember = (key, value) => {
    try { sessionStorage.setItem(`cw.mobile.${key}`, JSON.stringify(value)); } catch {}
  };
  const recall = key => {
    try { return JSON.parse(sessionStorage.getItem(`cw.mobile.${key}`) || 'null'); } catch { return null; }
  };

  function restoreScroll(top, current = () => true, done = () => {}) {
    cancelScroll();
    if (!mobile.matches || !Number.isFinite(top)) return;
    let observer, timer, frame, stopped = false;
    const stop = () => {
      if (stopped) return;
      stopped = true;
      observer?.disconnect();
      clearTimeout(timer);
      cancelAnimationFrame(frame);
      for (const name of ['touchstart', 'wheel', 'keydown', 'pointerdown']) window.removeEventListener(name, stop);
      done();
    };
    const restore = () => {
      if (!current()) return stop();
      window.scrollTo({top:Math.max(0, top), behavior:'instant'});
      if (Math.abs(window.scrollY - top) < 2) stop();
    };
    cancelScroll = stop;
    observer = new ResizeObserver(() => { cancelAnimationFrame(frame); frame = requestAnimationFrame(restore); });
    observer.observe(document.body);
    for (const name of ['touchstart', 'wheel', 'keydown', 'pointerdown']) window.addEventListener(name, stop, {passive:true});
    timer = setTimeout(stop, 3000);
    frame = requestAnimationFrame(restore);
  }

  const dialogs = new Map();
  function overlay(root, requestClose, forceClose = requestClose) {
    let released = false;
    let handle;
    const arm = () => {
      handle = layer(() => { if (!released) forceClose(); }, () => {
        if (released) return;
        queueMicrotask(() => {
          if (released || !root.isConnected) return;
          arm();
          requestClose();
        });
      });
    };
    arm();
    return {dismiss() { released = true; return handle.dismiss(); }};
  }

  function openDialog(dialog) {
    if (dialog.open) return true;
    for (const [previous, handle] of dialogs) if (!previous.open) { dialogs.delete(previous); void handle.dismiss(); }
    if (mobile.matches) dialog.classList.add('cw-page-mobile-sheet');
    dialog.showModal();
    if (mobile.matches) {
      const handle = overlay(dialog, () => {
        if (dialog.open && dialog.dispatchEvent(new Event('cancel', {cancelable:true}))) dialog.close();
      }, () => { if (dialog.open) dialog.close(); });
      dialogs.set(dialog, handle);
      dialog.addEventListener('close', () => {
        if (dialogs.get(dialog) === handle) dialogs.delete(dialog);
        void handle.dismiss();
      }, {once:true});
    }
    return true;
  }

  function ask(message) {
    if (!mobile.matches) return window.confirm(message);
    const destructive = /\b(delete|remove|discard|clear|reset|replace|unlink|prune)\b/i.test(message);
    return confirmAction({title:T("common.mobile.confirmTitle"), message, confirmLabel:T("common.mobile.continue"), danger:destructive});
  }

  function sheet(panel, {title = T("common.mobile.filters"), trigger, onClose} = {}) {
    const existing = panel.closest('dialog.cw-page-panel-sheet');
    if (existing) return existing;
    const anchor = document.createElement('span');
    anchor.hidden = true;
    panel.before(anchor);
    const dialog = document.createElement('dialog');
    dialog.className = 'cw-page-panel-sheet';
    dialog.setAttribute('aria-label', title);
    const heading = document.createElement('h2');
    heading.textContent = title;
    const done = document.createElement('button');
    done.type = 'button';
    done.className = 'cw-btn';
    done.textContent = T("common.mobile.done");
    done.addEventListener('click', () => dialog.close());
    const style = panel.getAttribute('style');
    const hidden = panel.hidden;
    dialog.append(heading, panel, done);
    anchor.after(dialog);
    panel.hidden = false;
    panel.style.display = 'grid';
    trigger?.setAttribute('aria-expanded', 'true');
    dialog.addEventListener('close', () => {
      anchor.replaceWith(panel);
      if (style === null) panel.removeAttribute('style'); else panel.setAttribute('style', style);
      panel.hidden = hidden;
      dialog.remove();
      trigger?.setAttribute('aria-expanded', 'false');
      trigger?.focus({preventScroll:true});
      onClose?.();
    }, {once:true});
    openDialog(dialog);
    return dialog;
  }

  async function confirmAction({title, message, confirmLabel = T("common.mobile.remove"), danger = true, input = false, initialValue = '', notice = false}) {
    const dialog = document.createElement('dialog');
    dialog.className = 'cw-page-confirm';
    const id = `cw-mobile-confirm-${++serial}`;
    dialog.setAttribute('aria-labelledby', `${id}-title`);
    dialog.setAttribute('aria-describedby', `${id}-message`);
    const heading = document.createElement('h2');
    heading.id = `${id}-title`;
    heading.textContent = title;
    const body = document.createElement('p');
    body.id = `${id}-message`;
    body.textContent = message;
    const footer = document.createElement('footer');
    const cancel = document.createElement('button');
    cancel.type = 'button';
    cancel.className = 'cw-btn';
    cancel.textContent = T("common.mobile.cancel");
    cancel.hidden = notice;
    const accept = document.createElement('button');
    accept.type = 'button';
    accept.className = `cw-btn${danger ? ' is-danger' : ''}`;
    accept.textContent = confirmLabel;
    footer.append(cancel, accept);
    dialog.append(heading, body, footer);
    const field = document.createElement('input');
    if (input) {
      field.type = 'text';
      field.value = initialValue;
      field.setAttribute('aria-label', title);
      footer.before(field);
      field.addEventListener('keydown', event => { if (event.key === 'Enter' && !event.isComposing) { event.preventDefault(); accept.click(); } });
    }
    document.body.append(dialog);
    let accepted = false;
    const result = new Promise(resolve => {
      dialog.addEventListener('close', () => {
        const handle = dialogs.get(dialog);
        void (handle?.dismiss() || pending?.promise || Promise.resolve()).then(() => {
          dialog.remove();
          resolve(input ? accepted ? field.value : null : accepted);
        });
      }, {once:true});
    });
    cancel.addEventListener('click', () => dialog.close());
    accept.addEventListener('click', () => { accepted = true; dialog.close(); feedback(); });
    openDialog(dialog);
    (input ? field : notice ? accept : cancel).focus({preventScroll:true});
    return result;
  }

  function promptAction(message, initialValue = '') {
    if (!mobile.matches) return window.prompt(message, initialValue);
    return confirmAction({title:message, message:'', confirmLabel:T("common.mobile.continue"), danger:false, input:true, initialValue});
  }

  function notice(message) {
    if (!mobile.matches) return window.alert(message);
    return confirmAction({title:'CrossWatch', message, confirmLabel:T("common.mobile.ok"), danger:false, notice:true});
  }

  function sectionPicker(nav) {
    if (!nav) return;
    const buttons = [...nav.querySelectorAll(':scope > button')];
    if (buttons.length < 2) return;
    let picker = nav.querySelector(':scope > .cw-mobile-section-picker');
    if (!picker) {
      picker = document.createElement('label');
      picker.className = 'cw-mobile-section-picker';
      const label = document.createElement('span');
      label.textContent = T("common.mobile.section");
      const select = document.createElement('select');
      select.className = 'cw-input';
      select.addEventListener('change', () => nav.querySelectorAll(':scope > button')[Number(select.value)]?.click());
      picker.append(label, select);
      nav.prepend(picker);
      nav.classList.add('cw-mobile-section-nav');
    }
    const select = picker.querySelector('select');
    select.replaceChildren(...buttons.map((button, index) => {
      const option = document.createElement('option');
      option.value = String(index);
      option.textContent = button.querySelector('strong')?.textContent || button.textContent;
      option.disabled = button.disabled || button.hidden;
      option.selected = button.classList.contains('active');
      return option;
    }));
  }

  function feedback() {
    try { if (mobile.matches && localStorage.getItem('cw.mobile.haptics') !== '0') navigator.vibrate?.(20); } catch {}
  }

  const viewport = window.visualViewport;
  const editable = node => node instanceof HTMLElement && node.matches('input:not([type=checkbox]):not([type=radio]):not([type=button]):not([type=submit]),textarea,[contenteditable="true"]');
  let viewportFrame = 0;
  let baselineHeight = innerHeight;
  let baselineWidth = innerWidth;
  function updateViewport() {
    cancelAnimationFrame(viewportFrame);
    viewportFrame = requestAnimationFrame(() => {
      const height = viewport?.height || innerHeight;
      const inset = Math.max(0, innerHeight - height - (viewport?.offsetTop || 0));
      if (Math.abs(innerWidth - baselineWidth) > 80 || !editable(document.activeElement)) { baselineHeight = innerHeight; baselineWidth = innerWidth; }
      const keyboard = mobile.matches && (viewport?.scale || 1) < 1.05 && editable(document.activeElement) && Math.max(inset, baselineHeight - height) > 120;
      document.documentElement.classList.toggle('cw-mobile-keyboard', keyboard);
      document.documentElement.style.setProperty('--cw-mobile-visible-height', `${height}px`);
      document.documentElement.style.setProperty('--cw-mobile-keyboard-inset', `${keyboard ? inset : 0}px`);
      if (keyboard) {
        const rect = document.activeElement.getBoundingClientRect();
        const top = viewport?.offsetTop || 0;
        if (rect.top < top + 12 || rect.bottom > top + height - 12) document.activeElement.scrollIntoView({block:'nearest', behavior:'instant'});
      }
    });
  }
  viewport?.addEventListener('resize', updateViewport);
  viewport?.addEventListener('scroll', updateViewport);
  window.addEventListener('resize', updateViewport);
  document.addEventListener('focusin', updateViewport);
  document.addEventListener('focusout', updateViewport);
  updateViewport();
  if (location.pathname === '/') {
    let route = location.hash.slice(1) || 'main';
    let top = window.scrollY;
    const key = name => `page-position.${document.documentElement.dataset.cwProfileId || 'default'}.${name}`;
    const save = () => { if (mobile.matches && !route.startsWith('tracker')) remember(key(route), top); };
    window.addEventListener('scroll', () => { top = window.scrollY; }, {passive:true});
    window.addEventListener('pagehide', save);
    window.addEventListener('hashchange', () => {
      save();
      route = location.hash.slice(1) || 'main';
      top = 0;
      if (!route.startsWith('tracker')) {
        const wanted = route;
        restoreScroll(recall(key(route)) || 0, () => route === wanted);
      }
    });
    window.addEventListener('load', () => { if (!route.startsWith('tracker')) restoreScroll(recall(key(route)) || 0); }, {once:true});
  }
  CW.MobileUI = {layer, overlay, beforeNavigate, openDialog, sheet, sectionPicker, ask, prompt:promptAction, notice, confirm:confirmAction, remember, recall, restoreScroll, feedback, mobile:() => mobile.matches};
})();
