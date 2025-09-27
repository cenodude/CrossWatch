// Lightbox viewer for gallery
(() => {
  const gallery = document.querySelector('[data-gallery]');
  if (!gallery) return;

  const items = [...gallery.querySelectorAll('a')];
  const viewer = document.getElementById('viewer');
  const img = document.getElementById('viewer-img');
  const cap = document.getElementById('viewer-cap');
  const count = document.getElementById('viewer-count');
  const btnPrev = viewer.querySelector('.lb-prev');
  const btnNext = viewer.querySelector('.lb-next');
  const btnClose = viewer.querySelector('.lb-close');

  let i = 0, touchX = null;

  const clamp = (n, a, b) => Math.max(a, Math.min(b, n));
  const preload = (idx) => { if (idx < 0 || idx >= items.length) return; new Image().src = items[idx].href; };

  function show(idx) {
    const a = items[idx];
    img.src = a.href;
    img.alt = a.querySelector('img')?.alt || '';
    cap.textContent = a.dataset.title || img.alt || 'Screenshot';
    count.textContent = ` ${idx + 1} / ${items.length}`;
    i = idx;
  }

  function open(idx) {
    i = clamp(idx, 0, items.length - 1);
    show(i);
    viewer.hidden = false;
    document.body.style.overflow = 'hidden';
    preload(i - 1); preload(i + 1);
  }

  function close() {
    viewer.hidden = true;
    document.body.style.overflow = '';
  }

  const next = () => show(clamp(i + 1, 0, items.length - 1));
  const prev = () => show(clamp(i - 1, 0, items.length - 1));

  // No passive here; we must preventDefault() to avoid navigating to the image.
  items.forEach((a, idx) => a.addEventListener('click', (e) => { e.preventDefault(); open(idx); }));

  btnClose.addEventListener('click', close);
  btnNext.addEventListener('click', next);
  btnPrev.addEventListener('click', prev);

  // Click backdrop to close
  viewer.addEventListener('click', (e) => { if (e.target === viewer) close(); });

  // Keyboard
  window.addEventListener('keydown', (e) => {
    if (viewer.hidden) return;
    if (e.key === 'Escape') close();
    else if (e.key === 'ArrowRight') next();
    else if (e.key === 'ArrowLeft') prev();
  });

  // Touch swipe
  viewer.addEventListener('touchstart', (e) => { touchX = e.touches[0].clientX; }, { passive: true });
  viewer.addEventListener('touchend', (e) => {
    if (touchX == null) return;
    const dx = e.changedTouches[0].clientX - touchX;
    touchX = null;
    if (Math.abs(dx) < 40) return;
    dx < 0 ? next() : prev();
  }, { passive: true });
})();
