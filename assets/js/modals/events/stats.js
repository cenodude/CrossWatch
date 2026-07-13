/* assets/js/modals/events/stats.js */
/* CrossWatch - Events statistics dashboard */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

const SERIES = [
  { key: "sync", label: "Sync runs", color: "var(--ev-ok)" },
  { key: "watcher", label: "Watcher", color: "var(--ev-info)" },
  { key: "webhook", label: "Webhooks", color: "var(--ev-warn)" },
  { key: "failed", label: "Failed", color: "var(--ev-err)" },
];
const OUTCOME_COLOR = { completed: "var(--ev-ok)", warning: "var(--ev-warn)", failed: "var(--ev-err)", active: "var(--ev-info)" };
const TYPE_COLOR = { sync: "var(--ev-ok)", watcher: "var(--ev-info)", webhook: "var(--ev-warn)" };

const nf = new Intl.NumberFormat();
const fmtNum = (n) => nf.format(Math.round(Number(n || 0)));
const fmtDur = (s) => {
  s = Number(s || 0);
  if (s <= 0) return "0s";
  if (s < 60) return `${s < 10 ? s.toFixed(1) : Math.round(s)}s`;
  if (s < 3600) return `${(s / 60).toFixed(1)}m`;
  return `${(s / 3600).toFixed(1)}h`;
};
const fmtPct = (n) => (n == null ? "—" : `${Number(n).toFixed(1)}%`);
const fmtDelta = (d) => {
  if (d == null) return `<span class="stat-delta flat">—</span>`;
  const up = d >= 0;
  const cls = up ? "up" : "down";
  return `<span class="stat-delta ${cls}"><span class="material-symbols-rounded" aria-hidden="true">${up ? "trending_up" : "trending_down"}</span>${Math.abs(d).toFixed(1)}%</span>`;
};
const tLabel = (epoch, bucket) => {
  const d = new Date(Number(epoch) * 1000);
  if (bucket < 86400) return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  return d.toLocaleDateString([], { month: "short", day: "numeric" });
};

let TIP;
const tip = () => (TIP ||= (() => { const t = document.createElement("div"); t.className = "stat-tip"; t.hidden = true; document.body.appendChild(t); return t; })());
const showTip = (html, x, y) => { const t = tip(); t.innerHTML = html; t.hidden = false; const r = t.getBoundingClientRect(); t.style.left = `${Math.min(x + 14, window.innerWidth - r.width - 8)}px`; t.style.top = `${Math.max(8, y - r.height - 12)}px`; };
const hideTip = () => { if (TIP) TIP.hidden = true; };

const niceScale = (v, ticks = 4) => {
  v = Math.max(1, v);
  const rough = v / ticks;
  const pow = Math.pow(10, Math.floor(Math.log10(rough)));
  const n = rough / pow;
  const step = (n <= 1 ? 1 : n <= 2 ? 2 : n <= 2.5 ? 2.5 : n <= 5 ? 5 : 10) * pow;
  return { max: Math.max(step, Math.ceil(v / step) * step), step };
};

function stackedTrend(rows, bucket) {
  const W = 1000, H = 170, padL = 40, padR = 44, padT = 8, padB = 20;
  if (!rows.length) return `<div class="stat-empty">No activity in this period.</div>`;
  const plotW = W - padL - padR, plotH = H - padT - padB;
  const totals = rows.map((r) => r.sync + r.watcher + r.webhook + r.failed);
  const sc = niceScale(Math.max(1, ...totals));
  const maxV = sc.max;
  const n = rows.length;
  const bw = Math.max(2, Math.min(26, (plotW / n) * 0.7));
  const step = plotW / n;
  const x = (i) => padL + step * i + (step - bw) / 2;
  const yv = (v) => padT + plotH * (1 - v / maxV);
  const gap = 2;

  const ticks = [];
  for (let t = 0; t <= maxV + 0.5; t += sc.step) ticks.push(t);
  const grid = ticks.map((t) => {
    const gy = yv(t);
    return `<line x1="${padL}" y1="${gy.toFixed(1)}" x2="${W - padR}" y2="${gy.toFixed(1)}" class="stat-grid"/><text x="${padL - 6}" y="${(gy + 3).toFixed(1)}" class="stat-ax stat-ax-y">${fmtNum(t)}</text>`;
  }).join("");

  const bars = rows.map((r, i) => {
    let acc = 0;
    const segs = SERIES.map((s) => {
      const v = r[s.key] || 0;
      if (v <= 0) return "";
      const y0 = yv(acc), y1 = yv(acc + v);
      acc += v;
      const h = Math.max(0, y0 - y1 - gap);
      return `<rect x="${x(i).toFixed(1)}" y="${y1.toFixed(1)}" width="${bw.toFixed(1)}" height="${h.toFixed(1)}" rx="2" fill="${s.color}"/>`;
    }).join("");
    const hit = `<rect class="stat-hit" x="${(padL + step * i).toFixed(1)}" y="${padT}" width="${step.toFixed(1)}" height="${plotH}" data-i="${i}"/>`;
    return segs + hit;
  }).join("");

  const rated = rows.filter((r) => r.rate != null);
  let rateLine = "";
  if (rated.length > 1) {
    const rmin = Math.min(80, ...rows.map((r) => (r.rate == null ? 100 : r.rate)));
    const lo = Math.floor(rmin / 5) * 5, hi = 100;
    const ry = (v) => padT + plotH * (1 - (v - lo) / Math.max(1, hi - lo));
    const pts = rows.map((r, i) => (r.rate == null ? null : `${(x(i) + bw / 2).toFixed(1)},${ry(r.rate).toFixed(1)}`)).filter(Boolean).join(" ");
    const dots = rows.map((r, i) => (r.rate == null ? "" : `<circle cx="${(x(i) + bw / 2).toFixed(1)}" cy="${ry(r.rate).toFixed(1)}" r="2.6" class="stat-rate-dot"/>`)).join("");
    rateLine = `<polyline points="${pts}" class="stat-rate-line"/>${dots}` +
      [lo, (lo + hi) / 2, hi].map((v) => `<text x="${W - padR + 6}" y="${(ry(v) + 3).toFixed(1)}" class="stat-ax stat-ax-r">${v}%</text>`).join("");
  }

  const every = Math.ceil(n / 12);
  const xlabels = rows.map((r, i) => (i % every === 0 ? `<text x="${(x(i) + bw / 2).toFixed(1)}" y="${H - 8}" class="stat-ax stat-ax-x">${esc(tLabel(r.t, bucket))}</text>` : "")).join("");

  return `<svg viewBox="0 0 ${W} ${H}" class="stat-svg stat-svg-trend" preserveAspectRatio="xMidYMid meet" role="img">${grid}${bars}${rateLine}${xlabels}</svg>`;
}

function donut(segments, opts = {}) {
  const total = segments.reduce((a, s) => a + s.value, 0);
  const S = 220, r = 84, rin = 56, cx = S / 2, cy = S / 2;
  if (total <= 0) return `<div class="stat-empty">No data.</div>`;
  let a0 = -Math.PI / 2;
  const gap = 0.03;
  const arcs = segments.map((s) => {
    const frac = s.value / total;
    const a1 = a0 + frac * Math.PI * 2;
    const g = frac > gap * 1.5 ? gap : 0;
    const s0 = a0 + g / 2, s1 = a1 - g / 2;
    const large = s1 - s0 > Math.PI ? 1 : 0;
    const p = (ang, rad) => `${(cx + rad * Math.cos(ang)).toFixed(2)} ${(cy + rad * Math.sin(ang)).toFixed(2)}`;
    a0 = a1;
    if (s1 <= s0) return "";
    return `<path d="M ${p(s0, r)} A ${r} ${r} 0 ${large} 1 ${p(s1, r)} L ${p(s1, rin)} A ${rin} ${rin} 0 ${large} 0 ${p(s0, rin)} Z" fill="${s.color}"><title>${esc(s.label)}: ${fmtNum(s.value)} (${((s.value / total) * 100).toFixed(1)}%)</title></path>`;
  }).join("");
  const center = `<text x="${cx}" y="${cy - 4}" class="stat-donut-num">${fmtNum(total)}</text><text x="${cx}" y="${cy + 16}" class="stat-donut-cap">${esc(opts.caption || "Total")}</text>`;
  const legend = segments.map((s) => `<div class="stat-lg-item"><span class="stat-lg-dot" style="background:${s.color}"></span><span class="stat-lg-lb">${esc(s.label)}</span><span class="stat-lg-vl">${fmtNum(s.value)} · ${((s.value / total) * 100).toFixed(1)}%</span></div>`).join("");
  return `<div class="stat-donut"><svg viewBox="0 0 ${S} ${S}" class="stat-svg-donut" preserveAspectRatio="xMidYMid meet">${arcs}${center}</svg><div class="stat-legend">${legend}</div></div>`;
}

function area(points, color, bucket, unit) {
  const W = 520, H = 150, padL = 34, padR = 8, padT = 8, padB = 18;
  if (!points.length) return `<div class="stat-empty">No data.</div>`;
  const plotW = W - padL - padR, plotH = H - padT - padB;
  const maxV = niceScale(Math.max(1, ...points.map((p) => p.v))).max;
  const n = points.length;
  const x = (i) => padL + (n === 1 ? plotW / 2 : (plotW * i) / (n - 1));
  const y = (v) => padT + plotH * (1 - v / maxV);
  const line = points.map((p, i) => `${x(i).toFixed(1)},${y(p.v).toFixed(1)}`).join(" ");
  const areaPath = `${padL},${(padT + plotH).toFixed(1)} ${line} ${x(n - 1).toFixed(1)},${(padT + plotH).toFixed(1)}`;
  const grid = [0, 0.5, 1].map((f) => { const gy = padT + plotH * (1 - f); return `<line x1="${padL}" y1="${gy.toFixed(1)}" x2="${W - padR}" y2="${gy.toFixed(1)}" class="stat-grid"/><text x="${padL - 5}" y="${(gy + 3).toFixed(1)}" class="stat-ax stat-ax-y">${fmtNum(maxV * f)}</text>`; }).join("");
  const every = Math.ceil(n / 8);
  const xlabels = points.map((p, i) => (i % every === 0 ? `<text x="${x(i).toFixed(1)}" y="${H - 6}" class="stat-ax stat-ax-x">${esc(tLabel(p.t, bucket))}</text>` : "")).join("");
  const dots = points.map((p, i) => `<circle class="stat-hit-dot" cx="${x(i).toFixed(1)}" cy="${y(p.v).toFixed(1)}" r="9" data-t="${p.t}" data-v="${p.v}"/>`).join("");
  return `<svg viewBox="0 0 ${W} ${H}" class="stat-svg" preserveAspectRatio="xMidYMid meet" data-unit="${esc(unit || "")}" data-bucket="${bucket}">${grid}<polygon points="${areaPath}" fill="${color}" fill-opacity="0.14"/><polyline points="${line}" fill="none" stroke="${color}" stroke-width="2"/>${xlabels}${dots}</svg>`;
}

function lineChart(points, color, bucket) {
  return area(points.map((p) => ({ t: p.t, v: p.v })), color, bucket, "s");
}

export default function createStatsView(host, { fetchJson }) {
  let alive = true;
  let last = null;

  const kpiTile = (icon, label, valueHTML, delta, cls = "") => `
    <div class="stat-tile ${cls}">
      <div class="stat-tile-top"><span class="stat-tile-ic"><span class="material-symbols-rounded" aria-hidden="true">${icon}</span></span><span class="stat-tile-lb">${esc(label)}</span></div>
      <div class="stat-tile-val">${valueHTML}</div>
      <div class="stat-tile-foot">${fmtDelta(delta)}<span class="stat-tile-cmp">vs prev</span></div>
    </div>`;

  const render = (d) => {
    last = d;
    const k = d.kpis || {};
    const bucket = (d.range || {}).bucket || 86400;
    const kpis = [
      kpiTile("sync", "Sync runs", fmtNum(k.sync_runs?.value), k.sync_runs?.delta),
      kpiTile("music_note", "Scrobbles", fmtNum(k.scrobbles?.value), k.scrobbles?.delta),
      kpiTile("timer", "Avg sync duration", fmtDur(k.avg_duration?.value), k.avg_duration?.delta == null ? null : -k.avg_duration.delta),
      kpiTile("error", "Failures", fmtNum(k.failures?.value), k.failures?.delta == null ? null : -k.failures.delta, "warn"),
      kpiTile("block", "Blocked", fmtNum(k.blocked?.value), k.blocked?.delta == null ? null : -k.blocked.delta, "warn"),
      kpiTile("verified", "Success rate", fmtPct(k.success_rate?.value), k.success_rate?.delta, "ok"),
    ].join("");

    const legend = SERIES.map((s) => `<div class="stat-lg-item"><span class="stat-lg-dot" style="background:${s.color}"></span><span class="stat-lg-lb">${esc(s.label)}</span></div>`).join("") +
      `<div class="stat-lg-item"><span class="stat-lg-line"></span><span class="stat-lg-lb">Success rate</span></div>`;

    const routes = (d.routes || []).map((r) => {
      const kind = r.runs && r.scrobbles ? `${fmtNum(r.runs)} runs · ${fmtNum(r.scrobbles)} scrobbles` : r.runs ? `${fmtNum(r.runs)} sync run${r.runs === 1 ? "" : "s"}` : `${fmtNum(r.scrobbles)} scrobbles`;
      const pct = r.success_rate == null ? 0 : r.success_rate;
      return `<tr><td class="stat-rt-route" title="${esc(kind)}">${esc(r.source || "?")} → ${esc(r.destination || "?")}</td>
      <td class="stat-rt-num">${fmtNum(r.volume)}</td>
      <td class="stat-rt-bar"><span class="stat-bar"><span class="stat-bar-fill" style="width:${Math.max(2, pct).toFixed(0)}%"></span></span><span class="stat-rt-pct">${fmtPct(r.success_rate)}</span></td></tr>`;
    }).join("") || `<tr><td colspan="3" class="stat-empty-cell">No route activity.</td></tr>`;

    const fails = (d.failure_reasons || []).map((f) => `
      <tr><td class="stat-fr-reason">${esc(f.reason)}</td>
      <td class="stat-rt-num">${fmtNum(f.count)}</td>
      <td class="stat-rt-num">${f.share.toFixed(1)}%</td>
      <td class="stat-fr-trend">${fmtDelta(f.delta)}</td></tr>`).join("") || `<tr><td colspan="4" class="stat-empty-cell">No failures in this period.</td></tr>`;

    const pc = d.duration_percentiles || {};
    const outcomes = (d.outcomes || []).map((o) => ({ ...o, color: OUTCOME_COLOR[o.key] || "var(--ev-muted)" }));
    const types = (d.types || []).map((t) => ({ ...t, color: TYPE_COLOR[t.key] || "var(--ev-muted)" }));
    const whPts = (d.trend || []).map((r) => ({ t: r.t, v: r.webhook }));
    const waPts = (d.trend || []).map((r) => ({ t: r.t, v: r.watcher }));
    const durPts = (d.duration_series || []).map((r) => ({ t: r.t, v: r.avg }));

    host.innerHTML = `
      <div class="stat-wrap">
        <div class="stat-kpis">${kpis}</div>

        <section class="stat-card stat-card-wide">
          <div class="stat-card-head"><div><h4>Activity trends over time</h4><p>Outcomes by source, with the success-rate overlay.</p></div><div class="stat-legend stat-legend-row">${legend}</div></div>
          ${stackedTrend(d.trend || [], bucket)}
        </section>

        <div class="stat-grid3">
          <section class="stat-card"><div class="stat-card-head"><h4>Outcome distribution</h4></div>${donut(outcomes, { caption: "Threads" })}</section>
          <section class="stat-card"><div class="stat-card-head"><h4>Activity by type</h4></div>${donut(types, { caption: "Total" })}</section>
          <section class="stat-card"><div class="stat-card-head"><h4>Top routes</h4></div>
            <table class="stat-table"><thead><tr><th>Route</th><th>Volume</th><th>Success</th></tr></thead><tbody>${routes}</tbody></table>
          </section>
        </div>

        <div class="stat-grid2">
          <section class="stat-card"><div class="stat-card-head"><h4>Webhook activity</h4></div>${area(whPts, "var(--ev-warn)", bucket, "")}</section>
          <section class="stat-card"><div class="stat-card-head"><h4>Watcher activity</h4></div>${area(waPts, "var(--ev-info)", bucket, "")}</section>
        </div>

        <div class="stat-grid2">
          <section class="stat-card"><div class="stat-card-head"><div><h4>Sync duration over time</h4></div>
            <div class="stat-pcts"><span><b>P50</b> ${fmtDur(pc.p50)}</span><span><b>P90</b> ${fmtDur(pc.p90)}</span><span><b>P99</b> ${fmtDur(pc.p99)}</span></div></div>
            ${lineChart(durPts, "var(--ev-accent)", bucket)}
          </section>
          <section class="stat-card"><div class="stat-card-head"><h4>Top failure reasons</h4></div>
            <table class="stat-table"><thead><tr><th>Reason</th><th>Count</th><th>Share</th><th>Trend</th></tr></thead><tbody>${fails}</tbody></table>
          </section>
        </div>
      </div>`;

    bindHovers(d, bucket);
  };

  const bindHovers = (d, bucket) => {
    const trend = d.trend || [];
    host.querySelectorAll(".stat-svg-trend .stat-hit").forEach((el) => {
      el.addEventListener("mousemove", (e) => {
        const r = trend[Number(el.dataset.i)]; if (!r) return;
        const rows = SERIES.filter((s) => r[s.key] > 0).map((s) => `<div class="stat-tip-row"><span class="stat-lg-dot" style="background:${s.color}"></span>${esc(s.label)}<b>${fmtNum(r[s.key])}</b></div>`).join("");
        showTip(`<div class="stat-tip-h">${esc(tLabel(r.t, bucket))}</div>${rows}${r.rate != null ? `<div class="stat-tip-row stat-tip-rate">Success rate<b>${fmtPct(r.rate)}</b></div>` : ""}`, e.clientX, e.clientY);
      });
      el.addEventListener("mouseleave", hideTip);
    });
    host.querySelectorAll(".stat-hit-dot").forEach((el) => {
      el.addEventListener("mousemove", (e) => {
        const unit = el.closest("svg").dataset.unit;
        const bk = Number(el.closest("svg").dataset.bucket) || bucket;
        const v = Number(el.dataset.v);
        showTip(`<div class="stat-tip-h">${esc(tLabel(Number(el.dataset.t), bk))}</div><div class="stat-tip-row"><b>${unit === "s" ? fmtDur(v) : fmtNum(v)}</b></div>`, e.clientX, e.clientY);
      });
      el.addEventListener("mouseleave", hideTip);
    });
  };

  const skeleton = () => {
    const card = (cls = "") => `<section class="stat-card ${cls}"><div class="stat-sk stat-sk-line w40"></div><div class="stat-sk stat-sk-block"></div></section>`;
    host.innerHTML = `<div class="stat-wrap stat-skel">
      <div class="stat-kpis">${'<div class="stat-tile stat-sk"></div>'.repeat(6)}</div>
      <section class="stat-card stat-card-wide"><div class="stat-sk stat-sk-line w30"></div><div class="stat-sk stat-sk-block tall"></div></section>
      <div class="stat-grid3">${card()}${card()}${card()}</div>
      <div class="stat-grid2">${card()}${card()}</div>
      <div class="stat-grid2">${card()}${card()}</div>
    </div>`;
  };

  const load = async ({ range = "30d", force = false } = {}) => {
    if (!host.querySelector(".stat-wrap")) skeleton();
    try {
      const d = await fetchJson(`/api/events/statistics?range=${encodeURIComponent(range)}${force ? `&_=${Date.now()}` : ""}`);
      if (!alive) return null;
      if (!d || d.ok === false) throw new Error("unavailable");
      render(d);
      return d;
    } catch (err) {
      if (alive) host.innerHTML = `<div class="stat-loading"><span class="material-symbols-rounded" aria-hidden="true">error</span>Failed to load statistics.</div>`;
      return null;
    }
  };

  return {
    load,
    destroy() { alive = false; hideTip(); if (TIP) { TIP.remove(); TIP = null; } },
  };
}
