/* assets/js/topology/viewport.js */
/* CrossWatch - Sync topology zoom and pan controls */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const clamp = (value, min, max) => Math.min(max, Math.max(min, value));

export function graphViewBox(width, height, zoom, center = { x: .5, y: .5 }) {
  const scale = clamp(zoom, .5, 2.5);
  const viewWidth = width / scale, viewHeight = height / scale;
  const margin = Math.min(.5, .5 / scale);
  const x = clamp(center.x, margin, 1 - margin), y = clamp(center.y, margin, 1 - margin);
  return { x: x * width - viewWidth / 2, y: y * height - viewHeight / 2, width: viewWidth, height: viewHeight, center: { x, y }, scale };
}

export function bindGraphViewport(graph, controls) {
  let zoom = 1, center = { x: .5, y: .5 }, drag = null;
  const slider = controls.querySelector('input[type="range"]');
  const output = controls.querySelector("output");
  const hint = controls.querySelector(".topology-pan-hint");
  const controller = new AbortController();
  const listen = (element, type, handler) => element.addEventListener(type, handler, { signal: controller.signal });
  const update = () => {
    const svg = graph.querySelector("svg");
    controls.querySelectorAll("button,input").forEach(element => { element.disabled = !svg; });
    graph.classList.toggle("is-zoomed", !!svg && zoom > 1);
    hint.hidden = !svg || zoom <= 1;
    slider.value = String(Math.round(zoom * 100));
    slider.setAttribute("aria-valuetext", `${slider.value}%`);
    output.value = `${slider.value}%`;
    if (!svg) return;
    const view = graphViewBox(Number(svg.getAttribute("width")), Number(svg.getAttribute("height")), zoom, center);
    center = view.center;
    svg.setAttribute("viewBox", `${view.x} ${view.y} ${view.width} ${view.height}`);
    controls.querySelector('[data-zoom="out"]').disabled = zoom <= .5;
    controls.querySelector('[data-zoom="in"]').disabled = zoom >= 2.5;
  };
  const setZoom = value => { zoom = clamp(value, .5, 2.5); update(); };
  const reset = () => { zoom = 1; center = { x: .5, y: .5 }; update(); };
  listen(slider, "input", () => setZoom(Number(slider.value) / 100));
  listen(controls, "click", event => {
    const action = event.target.closest("button[data-zoom]")?.dataset.zoom;
    if (action === "fit") reset();
    else if (action) setZoom(Math.round(zoom * 100 + (action === "in" ? 10 : -10)) / 100);
  });
  listen(graph, "pointerdown", event => {
    if (zoom <= 1 || event.button !== 0 || drag) return;
    const svg = graph.querySelector("svg");
    if (!svg) return;
    drag = { id: event.pointerId, x: event.clientX, y: event.clientY, center: { ...center }, bounds: svg.getBoundingClientRect() };
    graph.setPointerCapture(event.pointerId);
    graph.classList.add("is-dragging");
    graph.focus({ preventScroll: true });
    event.preventDefault();
  });
  listen(graph, "pointermove", event => {
    if (!drag || event.pointerId !== drag.id) return;
    center = { x: drag.center.x - (event.clientX - drag.x) / drag.bounds.width / zoom,
      y: drag.center.y - (event.clientY - drag.y) / drag.bounds.height / zoom };
    update();
  });
  const stopDrag = () => {
    const id = drag?.id;
    drag = null;
    graph.classList.remove("is-dragging");
    if (id !== undefined && graph.hasPointerCapture(id)) graph.releasePointerCapture(id);
  };
  listen(graph, "pointerup", stopDrag);
  listen(graph, "pointercancel", stopDrag);
  listen(graph, "lostpointercapture", stopDrag);
  listen(graph, "keydown", event => {
    if (event.ctrlKey || event.metaKey || event.altKey) return;
    const moves = { ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] };
    if (["+", "="].includes(event.key)) setZoom(zoom + .1);
    else if (event.key === "-") setZoom(zoom - .1);
    else if (["0", "Home"].includes(event.key)) reset();
    else if (moves[event.key] && zoom > 1) {
      const [x, y] = moves[event.key];
      center = { x: center.x + x * .1 / zoom, y: center.y + y * .1 / zoom };
      update();
    } else return;
    event.preventDefault();
  });
  return { update, dispose() { stopDrag(); controller.abort(); } };
}
