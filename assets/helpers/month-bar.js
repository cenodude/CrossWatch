/* assets/helpers/month-bar.js */
/* CrossWatch - Month timeline bar shared by the profile page and the tracker */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const ZOOM_STEPS = [1, 1.6, 2.5, 4];
  const esc = (value) => String(value ?? "").replace(/[&<>"]/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;" }[m]));
  const pad = (value) => String(value).padStart(2, "0");
  const numberFmt = new Intl.NumberFormat();
  const defaultMonthLabel = (() => {
    const fmt = new Intl.DateTimeFormat(undefined, { month: "long", year: "numeric", timeZone: "UTC" });
    return (year, month) => fmt.format(new Date(Date.UTC(year, month - 1, 1)));
  })();

  function create({ host, noun, zoom = 0, onZoom = () => {}, onJump = () => {}, monthLabel = defaultMonthLabel }) {
    let level = Number.isInteger(zoom) && zoom >= 0 && zoom < ZOOM_STEPS.length ? zoom : 0;
    let signature = "";

    function syncZoomUi(node = host()) {
      if (!node) return;
      const label = node.querySelector(".cw-hist-zoom-level");
      if (label) label.textContent = `${Math.round(ZOOM_STEPS[level] * 100)}%`;
      node.querySelectorAll("[data-timeline-zoom]").forEach((btn) => {
        btn.disabled = Number(btn.dataset.timelineZoom) < 0 ? level <= 0 : level >= ZOOM_STEPS.length - 1;
      });
    }

    function render(months = [], { visible = new Set(), key = "" } = {}) {
      const node = host();
      if (!node) return;
      const counts = new Map((Array.isArray(months) ? months : [])
        .filter((entry) => /^\d{4}-\d{2}$/.test(String(entry?.month || "")))
        .map((entry) => [String(entry.month), Number(entry.count) || 0]));
      const keys = [...counts.keys()].sort();
      if (!keys.length) {
        node.hidden = true;
        node.innerHTML = "";
        signature = "";
        return;
      }
      const series = [];
      let [year, month] = keys[0].split("-").map(Number);
      const [endYear, endMonth] = keys[keys.length - 1].split("-").map(Number);
      while ((year < endYear || (year === endYear && month <= endMonth)) && series.length < 600) {
        const monthKey = `${year}-${pad(month)}`;
        series.push({ key: monthKey, year, month, count: counts.get(monthKey) || 0 });
        month += 1;
        if (month > 12) {
          month = 1;
          year += 1;
        }
      }
      const max = Math.max(1, ...series.map((entry) => entry.count));
      const total = series.reduce((sum, entry) => sum + entry.count, 0);
      const next = `${key}|${keys[0]}|${keys[keys.length - 1]}|${keys.length}|${total}`;
      const entering = next !== signature;
      signature = next;
      const last = series.length - 1;
      const bars = series.map((entry, index) => {
        const yearLabel = (index === 0 && entry.month <= 10) || entry.month === 1 ? `<span class="cw-hist-bar-year">${esc(entry.year)}</span>` : "";
        const label = `${monthLabel(entry.year, entry.month)} · ${noun(entry.count)}`;
        const delay = `--d:${Math.min(last - index, 48) * 14}ms`;
        if (!entry.count) return `<span class="cw-hist-bar is-empty" style="${delay}" data-timeline-tip="${esc(label)}"><span class="cw-hist-bar-fill"></span>${yearLabel}</span>`;
        const height = Math.max(10, Math.round(Math.sqrt(entry.count / max) * 100));
        return `<button type="button" class="cw-hist-bar${visible.has(entry.key) ? " is-visible" : ""}" style="${delay};--h:${height}%" data-timeline-month-jump="${esc(entry.key)}" data-timeline-tip="${esc(label)}" aria-label="${esc(label)}"><span class="cw-hist-bar-fill"></span>${yearLabel}</button>`;
      }).join("");
      const previous = node.querySelector(".cw-hist-bars");
      const previousLeft = previous ? previous.scrollLeft : 0;
      node.hidden = false;
      node.innerHTML = `<div class="cw-hist-timeline-head">
          <span class="material-symbols-rounded" aria-hidden="true">timeline</span>
          <strong>Timeline</strong>
          <small>${esc(`${numberFmt.format(keys.length)} active ${keys.length === 1 ? "month" : "months"} · click to jump · Ctrl + scroll to zoom`)}</small>
          <div class="cw-hist-zoom" role="group" aria-label="Timeline zoom">
            <button type="button" data-timeline-zoom="-1" title="Zoom out" aria-label="Zoom out"><span class="material-symbols-rounded" aria-hidden="true">zoom_out</span></button>
            <span class="cw-hist-zoom-level"></span>
            <button type="button" data-timeline-zoom="1" title="Zoom in" aria-label="Zoom in"><span class="material-symbols-rounded" aria-hidden="true">zoom_in</span></button>
          </div>
        </div>
        <div class="cw-hist-bars${entering ? " is-entering" : ""}" style="--zoom:${ZOOM_STEPS[level]}">${bars}</div>
        <div class="cw-hist-tip" hidden></div>`;
      syncZoomUi(node);
      const track = node.querySelector(".cw-hist-bars");
      if (!track) return;
      const active = track.querySelector(".cw-hist-bar.is-visible");
      const target = active ? Math.max(0, active.offsetLeft - track.clientWidth / 2) : track.scrollWidth;
      if (entering) {
        track.scrollLeft = target;
        clearTimeout(node.__enterTimer);
        node.__enterTimer = setTimeout(() => track.classList.remove("is-entering"), Math.min(last, 48) * 14 + 750);
      } else {
        track.scrollLeft = previousLeft;
        track.scrollTo({ left: target, behavior: "smooth" });
      }
    }

    function setZoom(step, anchorX = null) {
      const node = host();
      const track = node?.querySelector(".cw-hist-bars");
      const next = Math.max(0, Math.min(ZOOM_STEPS.length - 1, level + step));
      if (!track || next === level) return;
      const anchor = anchorX == null ? track.clientWidth / 2 : anchorX;
      const ratio = (track.scrollLeft + anchor) / Math.max(1, track.scrollWidth);
      level = next;
      onZoom(level);
      track.classList.remove("is-entering", "is-zooming");
      track.style.setProperty("--zoom", String(ZOOM_STEPS[next]));
      track.scrollLeft = ratio * track.scrollWidth - anchor;
      void track.offsetWidth;
      track.classList.add("is-zooming");
      clearTimeout(track.__zoomTimer);
      track.__zoomTimer = setTimeout(() => track.classList.remove("is-zooming"), 340);
      node.querySelector(".cw-hist-tip")?.setAttribute("hidden", "");
      syncZoomUi(node);
    }

    function wire() {
      const timeline = host();
      if (!timeline || timeline.dataset.monthBarWired === "1") return;
      timeline.dataset.monthBarWired = "1";
      let drag = null;
      let wheelAt = 0;
      const tip = () => timeline.querySelector(".cw-hist-tip");
      const hideTip = () => tip()?.setAttribute("hidden", "");
      const showTip = (bar) => {
        const node = tip();
        if (!node || !bar) return;
        node.textContent = bar.dataset.timelineTip || "";
        node.classList.toggle("is-empty", bar.classList.contains("is-empty"));
        node.hidden = false;
        const hostRect = timeline.getBoundingClientRect();
        const barRect = bar.getBoundingClientRect();
        const fillRect = bar.querySelector(".cw-hist-bar-fill")?.getBoundingClientRect() || barRect;
        const half = node.offsetWidth / 2;
        const center = barRect.left + barRect.width / 2 - hostRect.left;
        node.style.left = `${Math.max(half + 8, Math.min(hostRect.width - half - 8, center))}px`;
        node.style.top = `${fillRect.top - hostRect.top}px`;
      };
      timeline.addEventListener("click", (event) => {
        const zoomBtn = event.target?.closest?.("[data-timeline-zoom]");
        if (zoomBtn) {
          if (!zoomBtn.disabled) setZoom(Number(zoomBtn.dataset.timelineZoom) < 0 ? -1 : 1);
          return;
        }
        const monthBtn = event.target?.closest?.("[data-timeline-month-jump]");
        if (monthBtn) onJump(monthBtn.dataset.timelineMonthJump || "");
      });
      timeline.addEventListener("pointerover", (event) => {
        const bar = event.target?.closest?.("[data-timeline-tip]");
        if (bar && !drag) showTip(bar);
      });
      timeline.addEventListener("pointerleave", hideTip);
      timeline.addEventListener("focusin", (event) => {
        const bar = event.target?.closest?.("[data-timeline-tip]");
        if (bar) showTip(bar);
      });
      timeline.addEventListener("focusout", hideTip);
      timeline.addEventListener("scroll", hideTip, true);
      timeline.addEventListener("wheel", (event) => {
        const track = event.target?.closest?.(".cw-hist-bars");
        if (!track || !(event.ctrlKey || event.metaKey)) return;
        event.preventDefault();
        const now = Date.now();
        if (now - wheelAt < 140) return;
        wheelAt = now;
        setZoom(event.deltaY < 0 ? 1 : -1, event.clientX - track.getBoundingClientRect().left);
      }, { passive: false });
      timeline.addEventListener("pointerdown", (event) => {
        const track = event.target?.closest?.(".cw-hist-bars");
        if (!track || event.pointerType !== "mouse" || event.button !== 0 || track.scrollWidth <= track.clientWidth) return;
        drag = { track, x: event.clientX, left: track.scrollLeft, moved: false };
      });
      window.addEventListener("pointermove", (event) => {
        if (!drag) return;
        const dx = event.clientX - drag.x;
        if (!drag.moved && Math.abs(dx) < 5) return;
        drag.moved = true;
        drag.track.classList.add("is-dragging");
        drag.track.scrollLeft = drag.left - dx;
        hideTip();
      });
      window.addEventListener("pointerup", () => {
        if (!drag) return;
        const moved = drag.moved;
        drag.track.classList.remove("is-dragging");
        drag = null;
        if (!moved) return;
        const swallow = (event) => {
          event.stopPropagation();
          event.preventDefault();
        };
        window.addEventListener("click", swallow, { capture: true, once: true });
        setTimeout(() => window.removeEventListener("click", swallow, true), 0);
      });
    }

    return { render, wire };
  }

  (window.CW ||= {}).MonthBar = { ZOOM_STEPS, create };
})();
