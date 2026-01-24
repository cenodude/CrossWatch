/* CrossWatch: lightweight screenshot viewer (no deps) */
(() => {
  const SELECTOR = 'a[data-cw-gallery]';
  const openClass = 'cw-lightbox--open';
  const bodyOpenClass = 'cw-lightbox-open';

  /** @type {Map<string, Array<{href: string, alt: string, thumb: string}>>} */
  const galleries = new Map();

  const norm = (s) => (s || '').trim();

  const collect = () => {
    const links = Array.from(document.querySelectorAll(SELECTOR));
    if (!links.length) return;

    for (const a of links) {
      const name = norm(a.getAttribute('data-cw-gallery')) || 'default';
      const img = a.querySelector('img');
      const href = norm(a.getAttribute('href'));
      if (!href || !img) continue;
      const alt = norm(img.getAttribute('alt')) || 'Screenshot';
      const thumb = norm(img.getAttribute('src')) || href;

      if (!galleries.has(name)) galleries.set(name, []);
      const arr = galleries.get(name);
      arr.push({ href, alt, thumb });

      a.dataset.cwIndex = String(arr.length - 1);
      a.classList.add('cw-shot-link');
    }
  };

  const el = (tag, cls, attrs = {}) => {
    const n = document.createElement(tag);
    if (cls) n.className = cls;
    for (const [k, v] of Object.entries(attrs)) {
      if (v === undefined || v === null) continue;
      n.setAttribute(k, String(v));
    }
    return n;
  };

  const build = () => {
    const root = el('div', 'cw-lightbox', {
      role: 'dialog',
      'aria-modal': 'true',
      'aria-label': 'Screenshot viewer',
    });

    const backdrop = el('div', 'cw-lightbox__backdrop');

    const panel = el('div', 'cw-lightbox__panel');

    const closeBtn = el('button', 'cw-lightbox__close', { type: 'button', 'aria-label': 'Close' });
    closeBtn.innerHTML = '×';

    const prevBtn = el('button', 'cw-lightbox__nav cw-lightbox__prev', { type: 'button', 'aria-label': 'Previous' });
    prevBtn.innerHTML = '‹';

    const nextBtn = el('button', 'cw-lightbox__nav cw-lightbox__next', { type: 'button', 'aria-label': 'Next' });
    nextBtn.innerHTML = '›';

    const figure = el('figure', 'cw-lightbox__figure');
    const img = el('img', 'cw-lightbox__img', { alt: '' });
    const caption = el('figcaption', 'cw-lightbox__caption');
    figure.append(img, caption);

    const thumbs = el('div', 'cw-lightbox__thumbs', { 'aria-label': 'Thumbnails' });
    const counter = el('div', 'cw-lightbox__counter');

    panel.append(closeBtn, prevBtn, figure, nextBtn, thumbs, counter);
    root.append(backdrop, panel);
    document.body.append(root);

    return { root, backdrop, panel, closeBtn, prevBtn, nextBtn, img, caption, thumbs, counter };
  };

  /** @type {{root: HTMLDivElement, backdrop: HTMLDivElement, panel: HTMLDivElement, closeBtn: HTMLButtonElement, prevBtn: HTMLButtonElement, nextBtn: HTMLButtonElement, img: HTMLImageElement, caption: HTMLElement, thumbs: HTMLDivElement, counter: HTMLDivElement} | null} */
  let ui = null;
  let activeGallery = 'default';
  let activeIndex = 0;
  /** @type {HTMLElement | null} */
  let lastFocus = null;

  const preload = (href) => {
    const i = new Image();
    i.src = href;
  };

  const clampIndex = (arr, idx) => {
    if (!arr.length) return 0;
    if (idx < 0) return arr.length - 1;
    if (idx >= arr.length) return 0;
    return idx;
  };

  const renderThumbs = () => {
    if (!ui) return;
    const arr = galleries.get(activeGallery) || [];
    ui.thumbs.innerHTML = '';

    for (let i = 0; i < arr.length; i++) {
      const item = arr[i];
      const b = el('button', 'cw-lightbox__thumb', { type: 'button', 'aria-label': `Open ${item.alt}` });
      const t = el('img', 'cw-lightbox__thumbImg', { src: item.thumb, alt: item.alt, loading: 'lazy' });
      b.append(t);
      if (i === activeIndex) b.classList.add('is-active');
      b.addEventListener('click', () => show(i));
      ui.thumbs.append(b);
    }
  };

  const show = (idx) => {
    if (!ui) return;
    const arr = galleries.get(activeGallery) || [];
    if (!arr.length) return;

    activeIndex = clampIndex(arr, idx);
    const item = arr[activeIndex];

    ui.img.src = item.href;
    ui.img.alt = item.alt;
    ui.caption.textContent = item.alt;
    ui.counter.textContent = `${activeIndex + 1} / ${arr.length}`;

    // Update active thumb
    for (const n of ui.thumbs.querySelectorAll('.cw-lightbox__thumb')) n.classList.remove('is-active');
    const activeThumb = ui.thumbs.children[activeIndex];
    if (activeThumb) activeThumb.classList.add('is-active');

    // Preload neighbors
    preload(arr[clampIndex(arr, activeIndex + 1)].href);
    preload(arr[clampIndex(arr, activeIndex - 1)].href);
  };

  const open = (gallery, idx) => {
    if (!ui) ui = build();
    activeGallery = gallery;
    activeIndex = idx;

    lastFocus = /** @type {HTMLElement} */ (document.activeElement);

    document.body.classList.add(bodyOpenClass);
    ui.root.classList.add(openClass);

    renderThumbs();
    show(activeIndex);

    // Focus close for keyboard users
    ui.closeBtn.focus();
  };

  const close = () => {
    if (!ui) return;
    ui.root.classList.remove(openClass);
    document.body.classList.remove(bodyOpenClass);
    if (lastFocus) lastFocus.focus();
  };

  const nav = (dir) => {
    const arr = galleries.get(activeGallery) || [];
    if (!arr.length) return;
    show(activeIndex + dir);
  };

  const bind = () => {
    if (!ui) return;

    ui.backdrop.addEventListener('click', close);
    ui.closeBtn.addEventListener('click', close);
    ui.prevBtn.addEventListener('click', () => nav(-1));
    ui.nextBtn.addEventListener('click', () => nav(1));

    document.addEventListener('keydown', (e) => {
      if (!ui || !ui.root.classList.contains(openClass)) return;
      if (e.key === 'Escape') close();
      if (e.key === 'ArrowLeft') nav(-1);
      if (e.key === 'ArrowRight') nav(1);
    });

    // Swipe (touch/pointer)
    let startX = 0;
    let startY = 0;
    let active = false;

    const start = (x, y) => {
      startX = x;
      startY = y;
      active = true;
    };

    const end = (x, y) => {
      if (!active) return;
      active = false;

      const dx = x - startX;
      const dy = y - startY;
      if (Math.abs(dx) < 40 || Math.abs(dx) < Math.abs(dy)) return;
      nav(dx < 0 ? 1 : -1);
    };

    ui.panel.addEventListener('touchstart', (e) => {
      if (!e.touches || e.touches.length !== 1) return;
      const t = e.touches[0];
      start(t.clientX, t.clientY);
    }, { passive: true });

    ui.panel.addEventListener('touchend', (e) => {
      const t = e.changedTouches && e.changedTouches[0];
      if (!t) return;
      end(t.clientX, t.clientY);
    }, { passive: true });
  };

  const wireLinks = () => {
    const links = Array.from(document.querySelectorAll(SELECTOR));
    for (const a of links) {
      const gallery = norm(a.getAttribute('data-cw-gallery')) || 'default';
      const idx = Number(a.dataset.cwIndex || '0');
      a.addEventListener('click', (e) => {
        // Keep non-local links behaving normally
        const href = norm(a.getAttribute('href'));
        if (!href || /^https?:\/\//i.test(href)) return;
        e.preventDefault();
        open(gallery, idx);
      });
    }
  };

  const init = () => {
    collect();
    if (!galleries.size) return;
    if (!ui) ui = build();
    bind();
    wireLinks();

    // Add a tiny hint via cursor/hover styling
    for (const img of document.querySelectorAll('img.cw-screenshot')) {
      img.setAttribute('loading', 'lazy');
    }
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
