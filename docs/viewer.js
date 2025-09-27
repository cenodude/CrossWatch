// Lightbox 
(() => {
  const gallery = document.querySelector('[data-gallery]');
  if (!gallery) return;

  const items = [...gallery.querySelectorAll('a')];
  const srcs = items.map(a => a.dataset.full || a.href);
  const titles = items.map(a => a.dataset.title || a.querySelector('img')?.alt || 'Screenshot');

  const viewer = document.getElementById('viewer');
  const img = document.getElementById('viewer-img');
  const cap = document.getElementById('viewer-cap');
  const count = document.getElementById('viewer-count');
  const btnPrev = viewer.querySelector('.lb-prev');
  const btnNext = viewer.querySelector('.lb-next');
  const btnClose = viewer.querySelector('.lb-close');

  let i = 0, touchX = null;

  const clamp = (n,a,b) => Math.max(a, Math.min(b, n));
  const preload = idx => { if (idx<0 || idx>=srcs.length) return; const im = new Image(); im.src = srcs[idx]; };

  function show(idx){
    img.src = srcs[idx];
    img.alt = titles[idx];
    cap.textContent = titles[idx];
    count.textContent = ` ${idx+1} / ${srcs.length}`;
    i = idx;
  }
  function open(idx){
    i = clamp(idx, 0, srcs.length-1);
    show(i);
    viewer.hidden = false;
    document.body.style.overflow = 'hidden';
    preload(i-1); preload(i+1);
  }
  function close(){
    viewer.hidden = true;
    document.body.style.overflow = '';
  }
  const next = () => show(clamp(i+1, 0, srcs.length-1));
  const prev = () => show(clamp(i-1, 0, srcs.length-1));

  // Prevent navigation completely
  items.forEach((a, idx) => {
    a.addEventListener('click', e => { e.preventDefault(); e.stopImmediatePropagation(); open(idx); });
  });

  btnClose.addEventListener('click', close);
  btnNext.addEventListener('click', next);
  btnPrev.addEventListener('click', prev);
  viewer.addEventListener('click', e => { if (e.target === viewer) close(); });

  window.addEventListener('keydown', e => {
    if (viewer.hidden) return;
    if (e.key === 'Escape') close();
    else if (e.key === 'ArrowRight') next();
    else if (e.key === 'ArrowLeft') prev();
  });

  viewer.addEventListener('touchstart', e => { touchX = e.touches[0].clientX; }, { passive:true });
  viewer.addEventListener('touchend', e => {
    if (touchX == null) return;
    const dx = e.changedTouches[0].clientX - touchX; touchX = null;
    if (Math.abs(dx) < 40) return;
    dx < 0 ? next() : prev();
  }, { passive:true });
})();
