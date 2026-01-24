/* CrossWatch Pages: lightweight screenshot lightbox (no deps). */
(() => {
  "use strict";

  const qsa = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  const groups = new Map(); // name -> {items: Array<{href, alt, thumb}>, anchors: HTMLAnchorElement[]}

  function buildGroups() {
    const anchors = qsa('a[data-cw-gallery]');
    for (const a of anchors) {
      const name = a.getAttribute("data-cw-gallery") || "default";
      const img = a.querySelector("img");
      const href = a.getAttribute("href") || "";
      const alt = (img && img.getAttribute("alt")) || "Screenshot";
      const thumb = (img && img.getAttribute("src")) || href;

      if (!groups.has(name)) groups.set(name, { items: [], anchors: [] });
      const g = groups.get(name);
      g.items.push({ href, alt, thumb });
      g.anchors.push(a);
    }
  }

  function createUI() {
    const root = document.createElement("div");
    root.className = "cw-lb";
    root.id = "cwLightbox";
    root.innerHTML = `
      <div class="cw-lb__backdrop" data-cw-lb-close="1"></div>
      <div class="cw-lb__dialog" role="dialog" aria-modal="true" aria-label="Screenshot viewer">
        <div class="cw-lb__top">
          <div class="cw-lb__title"><span id="cwLbCounter"></span></div>
          <button class="cw-lb__close" type="button" aria-label="Close">Close</button>
        </div>
        <div class="cw-lb__stage">
          <button class="cw-lb__nav prev" type="button" aria-label="Previous">‹</button>
          <img class="cw-lb__img" alt="" />
          <button class="cw-lb__nav next" type="button" aria-label="Next">›</button>
        </div>
        <div class="cw-lb__thumbs" aria-label="Thumbnails"></div>
      </div>
    `;
    document.body.appendChild(root);
    return root;
  }

  function clampIndex(i, len) {
    if (len <= 0) return 0;
    const mod = i % len;
    return mod < 0 ? mod + len : mod;
  }

  function preload(src) {
    const img = new Image();
    img.decoding = "async";
    img.loading = "eager";
    img.src = src;
  }

  function init() {
    buildGroups();
    if (groups.size === 0) return;

    const ui = createUI();
    const backdrop = ui.querySelector(".cw-lb__backdrop");
    const btnClose = ui.querySelector(".cw-lb__close");
    const imgEl = ui.querySelector(".cw-lb__img");
    const btnPrev = ui.querySelector(".cw-lb__nav.prev");
    const btnNext = ui.querySelector(".cw-lb__nav.next");
    const thumbs = ui.querySelector(".cw-lb__thumbs");
    const counter = ui.querySelector("#cwLbCounter");

    let activeGroup = null;
    let activeIndex = 0;
    let lastFocus = null;
    let restoreOverflow = "";

    function setOpen(open) {
      if (open) {
        lastFocus = document.activeElement;
        restoreOverflow = document.body.style.overflow || "";
        document.body.style.overflow = "hidden";
        ui.classList.add("is-open");
        btnClose.focus();
      } else {
        ui.classList.remove("is-open");
        document.body.style.overflow = restoreOverflow;
        if (lastFocus && typeof lastFocus.focus === "function") lastFocus.focus();
      }
    }

    function renderThumbs(items) {
      thumbs.innerHTML = "";
      items.forEach((it, idx) => {
        const t = document.createElement("img");
        t.src = it.thumb;
        t.alt = it.alt;
        t.loading = "lazy";
        t.decoding = "async";
        t.addEventListener("click", () => go(idx));
        thumbs.appendChild(t);
      });
    }

    function setActiveThumb(i) {
      const ts = qsa("img", thumbs);
      ts.forEach((el, idx) => el.classList.toggle("is-active", idx === i));
      const active = ts[i];
      if (active) active.scrollIntoView({ block: "nearest", inline: "nearest" });
    }

    function go(i) {
      if (!activeGroup) return;
      const items = activeGroup.items;
      activeIndex = clampIndex(i, items.length);
      const it = items[activeIndex];

      imgEl.src = it.href;
      imgEl.alt = it.alt;
      counter.textContent = `${activeIndex + 1} / ${items.length} — ${it.alt}`;

      btnPrev.disabled = items.length <= 1;
      btnNext.disabled = items.length <= 1;

      setActiveThumb(activeIndex);

      // Preload neighbors for snappy navigation
      preload(items[clampIndex(activeIndex - 1, items.length)].href);
      preload(items[clampIndex(activeIndex + 1, items.length)].href);
    }

    function open(name, index) {
      const g = groups.get(name);
      if (!g) return;
      activeGroup = g;
      renderThumbs(g.items);
      setOpen(true);
      go(index);
    }

    function close() {
      setOpen(false);
      activeGroup = null;
      thumbs.innerHTML = "";
      imgEl.src = "";
    }

    btnClose.addEventListener("click", close);
    backdrop.addEventListener("click", close);

    btnPrev.addEventListener("click", () => go(activeIndex - 1));
    btnNext.addEventListener("click", () => go(activeIndex + 1));

    // Click on image to go next (unless it was a drag/swipe)
    imgEl.addEventListener("click", () => {
      if (!activeGroup) return;
      if (activeGroup.items.length <= 1) return;
      go(activeIndex + 1);
    });

    // Keyboard
    document.addEventListener("keydown", (e) => {
      if (!ui.classList.contains("is-open")) return;
      if (e.key === "Escape") return close();
      if (e.key === "ArrowLeft") return go(activeIndex - 1);
      if (e.key === "ArrowRight") return go(activeIndex + 1);
    });

    // Touch swipe
    let startX = 0;
    let startY = 0;
    let tracking = false;

    ui.addEventListener("pointerdown", (e) => {
      if (!ui.classList.contains("is-open")) return;
      tracking = true;
      startX = e.clientX;
      startY = e.clientY;
    });

    ui.addEventListener("pointerup", (e) => {
      if (!ui.classList.contains("is-open")) return;
      if (!tracking) return;
      tracking = false;

      const dx = e.clientX - startX;
      const dy = e.clientY - startY;
      if (Math.abs(dx) < 40 || Math.abs(dx) < Math.abs(dy)) return;

      if (dx > 0) go(activeIndex - 1);
      else go(activeIndex + 1);
    });

    // Wire anchors
    for (const [name, g] of groups.entries()) {
      g.anchors.forEach((a, idx) => {
        a.addEventListener("click", (e) => {
          e.preventDefault();
          open(name, idx);
        });
      });
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
