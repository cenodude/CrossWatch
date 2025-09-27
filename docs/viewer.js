// ligthbox viewer for screenshots
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

  // Open viewer
  function open(idx) {
    i = clamp(idx, 0, items.length - 1);
    show(i);
    viewer.hidden = false;
    document.body.style.overflow = 'hidden';
    // Preload neighbors
    preload(i - 1); preload(i + 1);
  }

  // Close viewer
  function close() {
    viewer.hidden = true;
    document.body.style.overflow = '';
  }

  // Show index
  function show(idx) {
    const a = items[idx];
    img.src = a.getAttribute('href');
    img.alt = a.querySelector('img')?.alt || '';
    cap.textContent = a.dataset.title || img.alt || 'Screenshot';
    count.textContent = ` ${idx + 1} / ${items.length}`;
    i = idx;
  }

  const next = () => show(clamp(i + 1, 0, items.length - 1));
  const prev = () => show(clamp(i - 1, 0, items.length - 1));

  // Helpers
  const clamp = (n, a, b) => Math.max(a, Math.min(b, n));
  function preload(idx) {
    if (idx < 0 || idx >= items.length) return;
    const src = items[idx].getAttribute('href');
    const im = new Image(); im.src = src;
  }

  // Click bindings
  items.forEach((a, idx) => {
    a.addEventListener('click', e => {
      e.preventDefault();
      open(idx);
    }, { passive: true });
  });
  btnClose.addEventListener('click', close);
  btnNext.addEventListener('click', next);
  btnPrev.addEventListener('click', prev);
  viewer.addEventListener('click', e => {
    if (e.target === viewer) close(); // click backdrop to close
  });

  // Keyboard
  window.addEventListener('keydown', e => {
    if (viewer.hidden) return;
    if (e.key === 'Escape') close();
    else if (e.key === 'ArrowRight') next();
    else if (e.key === 'ArrowLeft') prev();
  });

  // Touch swipe
  viewer.addEventListener('touchstart', e => { touchX = e.touches[0].clientX; }, { passive: true });
  viewer.addEventListener('touchend', e => {
    if (touchX == null) return;
    const dx = e.changedTouches[0].clientX - touchX;
    touchX = null;
    if (Math.abs(dx) < 40) return; // threshold
    dx < 0 ? next() : prev();
  }, { passive: true });
})();
