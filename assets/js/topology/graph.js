/* assets/js/topology/graph.js */
/* CrossWatch - Sync topology graph layout and rendering */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import { edgeId } from "./analysis.js";

export const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
export const providerName = node => window.CW?.ProviderMeta?.label?.(node.provider) || node.provider;
export const instanceName = node => window.CW?.ProviderMeta?.instanceLabel?.(node.provider, node.instance) || (node.instance === "default" ? "Default instance" : node.instance);
export const nodeName = node => node ? `${providerName(node)}${instanceName(node) === "Default instance" ? "" : ` (${instanceName(node)})`}` : "Provider";
export const featureName = feature => window.CW?.FeatureMeta?.label?.(feature) || feature;
export const connectionLabel = (connection, nodes) => `${nodeName(nodes.get(connection.source))} ${connection.twoWay ? "↔" : "→"} ${nodeName(nodes.get(connection.target))}`;
export const pairBadge = connection => connection.number ? `<span class="topology-pair-badge" aria-label="Pair ${connection.number}" title="Pair ${connection.number}">${connection.number}</span>` : "";
const connectionOrder = (a, b) => (a.number || Infinity) - (b.number || Infinity) || a.id.localeCompare(b.id);

export function findingConnections(finding, graph) {
  const selected = new Set(finding.edges);
  return graph.connections.filter(connection => (!connection.features || connection.features.includes(finding.feature)) &&
    (selected.has(edgeId(connection.source, connection.target)) ||
    (connection.twoWay && selected.has(edgeId(connection.target, connection.source)))));
}

export function mergeGraphs(graphs) {
  const nodes = new Map(), edges = new Map(), connections = new Map();
  for (const graph of graphs) {
    graph.nodes.forEach(node => nodes.set(node.id, node));
    for (const edge of graph.edges) {
      if (!edges.has(edge.id)) edges.set(edge.id, { ...edge, features: [] });
      edges.get(edge.id).features.push(graph.feature);
      edges.get(edge.id).pairs = [...new Set([...edges.get(edge.id).pairs, ...edge.pairs])].sort();
    }
    for (const connection of graph.connections) {
      if (!connections.has(connection.id)) connections.set(connection.id, { ...connection, features: [] });
      connections.get(connection.id).features.push(graph.feature);
    }
  }
  return { nodes: [...nodes.values()].sort((a, b) => a.id.localeCompare(b.id)),
    edges: [...edges.values()].sort((a, b) => a.id.localeCompare(b.id)),
    connections: [...connections.values()].sort(connectionOrder) };
}

export function layoutGraph(graph, compact = false) {
  const positions = new Map();
  if (compact) {
    const desktop = layoutGraph(graph);
    const ordered = [...graph.nodes].sort((a, b) => desktop.positions.get(a.id).x - desktop.positions.get(b.id).x ||
      desktop.positions.get(a.id).y - desktop.positions.get(b.id).y || a.id.localeCompare(b.id));
    ordered.forEach((node, index) => positions.set(node.id, { x: 132, y: 70 + index * 144 }));
    return { positions, width: 290, height: Math.max(180, graph.nodes.length * 144 + 30) };
  }
  const neighbors = new Map(graph.nodes.map(node => [node.id, new Set()]));
  graph.edges.forEach(edge => {
    if (edge.source === edge.target) return;
    neighbors.get(edge.source).add(edge.target);
    neighbors.get(edge.target).add(edge.source);
  });
  let offset = 64, width = 620;
  const visited = new Set();
  for (const node of graph.nodes) {
    if (visited.has(node.id)) continue;
    const component = [node.id];
    visited.add(node.id);
    for (let i = 0; i < component.length; i++) for (const next of neighbors.get(component[i])) {
      if (!visited.has(next)) { visited.add(next); component.push(next); }
    }
    component.sort();
    const members = new Set(component);
    const edges = graph.connections.filter(edge => members.has(edge.source) && members.has(edge.target));
    const incoming = new Map(component.map(id => [id, 0]));
    edges.forEach(edge => incoming.set(edge.target, incoming.get(edge.target) + 1));
    const levels = new Map(component.map(id => [id, 0]));
    const queue = component.filter(id => incoming.get(id) === 0);
    for (let i = 0; i < queue.length; i++) for (const edge of edges.filter(edge => edge.source === queue[i])) {
      levels.set(edge.target, Math.max(levels.get(edge.target), levels.get(edge.source) + 1));
      incoming.set(edge.target, incoming.get(edge.target) - 1);
      if (!incoming.get(edge.target)) queue.push(edge.target);
    }
    let componentWidth, height;
    if (component.length <= 2) {
      height = 210;
      componentWidth = 620;
      const ordered = queue.length === component.length ? queue : component;
      ordered.forEach((id, i) => positions.set(id, { x: component.length === 1 ? 310 : 155 + i * 310, y: offset + height / 2 }));
    } else if (queue.length === component.length) {
      const columns = new Map();
      component.forEach(id => {
        const level = levels.get(id);
        if (!columns.has(level)) columns.set(level, []);
        columns.get(level).push(id);
      });
      const orderColumns = backwards => {
        const ordered = [...columns.keys()].sort((a, b) => backwards ? b - a : a - b);
        for (const level of ordered) {
          const rank = new Map([...columns.values()].flatMap(column => column.map((id, index) => [id, index / Math.max(1, column.length - 1)])));
          const score = id => {
            const linked = edges.filter(edge => backwards ? edge.source === id : edge.target === id)
              .map(edge => rank.get(backwards ? edge.target : edge.source));
            return linked.length ? linked.reduce((sum, value) => sum + value, 0) / linked.length : rank.get(id);
          };
          columns.get(level).sort((a, b) => score(a) - score(b) || a.localeCompare(b));
        }
      };
      orderColumns(true);
      orderColumns(false);
      height = Math.max(210, Math.max(...[...columns.values()].map(column => column.length)) * 144 + 10);
      componentWidth = Math.max(300, columns.size * 320 - 100);
      for (const [level, column] of columns) column.forEach((id, i) => {
        positions.set(id, { x: 110 + level * 320, y: offset + height / 2 + (i - (column.length - 1) / 2) * 144 });
      });
    } else {
      const radius = Math.max(175, component.length * 260 / (2 * Math.PI));
      componentWidth = radius * 2 + 220;
      height = radius * 2 + 120;
      component.forEach((id, i) => {
        const angle = 2 * Math.PI * i / component.length - Math.PI / 2;
        positions.set(id, { x: componentWidth / 2 + Math.cos(angle) * radius, y: offset + height / 2 + Math.sin(angle) * radius });
      });
    }
    if (componentWidth < 620) component.forEach(id => { positions.get(id).x += (620 - componentWidth) / 2; });
    width = Math.max(width, componentWidth);
    offset += height + 24;
  }
  return { positions, width, height: Math.max(220, offset) };
}

function connectionPath(from, to, positions) {
  if (from === to) return { d: `M ${from.x - 30} ${from.y - 36} C ${from.x - 85} ${from.y - 110},${from.x + 85} ${from.y - 110},${from.x + 30} ${from.y - 36}`, badge: { x: from.x, y: from.y - 92 } };
  const dx = to.x - from.x, dy = to.y - from.y;
  const scale = Math.min(96 / Math.max(Math.abs(dx), 0.001), 39 / Math.max(Math.abs(dy), 0.001));
  const sx = from.x + dx * scale, sy = from.y + dy * scale;
  const tx = to.x - dx * scale, ty = to.y - dy * scale;
  if (Math.abs(dx) < 2 && Math.abs(dy) > 180) {
    const lane = from.x + 132, sign = Math.sign(dy);
    return { d: `M ${from.x + 96} ${from.y} L ${lane - 12} ${from.y} Q ${lane} ${from.y} ${lane} ${from.y + 12 * sign} L ${lane} ${to.y - 12 * sign} Q ${lane} ${to.y} ${lane - 12} ${to.y} L ${to.x + 96} ${to.y}`, badge: { x: lane, y: (from.y + to.y) / 2 } };
  }
  if (Math.abs(dx) > 400) {
    const between = [...positions.values()].filter(pos => pos.x >= Math.min(from.x, to.x) && pos.x <= Math.max(from.x, to.x));
    const lane = Math.min(...between.map(pos => pos.y)) - 84, sign = Math.sign(dx);
    const startLane = from.x + sign * 120, endLane = to.x - sign * 120;
    return { d: `M ${from.x + sign * 96} ${from.y} L ${startLane - sign * 12} ${from.y} Q ${startLane} ${from.y} ${startLane} ${from.y - 12} L ${startLane} ${lane + 12} Q ${startLane} ${lane} ${startLane + sign * 12} ${lane} L ${endLane - sign * 12} ${lane} Q ${endLane} ${lane} ${endLane} ${lane + 12} L ${endLane} ${to.y - 12} Q ${endLane} ${to.y} ${endLane + sign * 12} ${to.y} L ${to.x - sign * 96} ${to.y}`, badge: { x: (from.x + to.x) / 2, y: lane } };
  }
  return { d: `M ${sx} ${sy} L ${tx} ${ty}`, badge: { x: (sx + tx) / 2, y: (sy + ty) / 2 } };
}

export function renderGraph(graph, finding = null, compact = false) {
  if (!graph.nodes.length) return '<div class="topology-empty"><span class="material-symbol" aria-hidden="true">route</span><strong>No active routes</strong><p>Enable a supported feature in a sync pair to see its topology.</p></div>';
  const { positions, width, height } = layoutGraph(graph, compact);
  const highlights = new Set(finding?.edges || []), selectedNodes = new Set(finding?.nodes || []);
  const nodes = new Map(graph.nodes.map(node => [node.id, node]));
  const byId = new Map(graph.edges.map(edge => [edge.id, edge]));
  const drawn = new Set();
  const badges = [];
  const occupied = [...positions.values()].map(pos => ({ x: pos.x - 94, y: pos.y - 38, width: 188, height: 76 }));
  const paths = graph.edges.map(edge => {
    if (drawn.has(edge.id)) return "";
    const reverse = byId.get(edgeId(edge.target, edge.source));
    const twoWay = reverse && reverse.id !== edge.id;
    drawn.add(edge.id);
    if (twoWay) drawn.add(reverse.id);
    const selected = highlights.has(edge.id) || (twoWay && highlights.has(reverse.id));
    const cls = selected ? "is-selected" : finding ? "is-muted" : "";
    const title = `${nodeName(nodes.get(edge.source))} → ${nodeName(nodes.get(edge.target))}: ${edge.features.map(featureName).join(", ")}` +
      (twoWay ? `\n${nodeName(nodes.get(edge.target))} → ${nodeName(nodes.get(edge.source))}: ${reverse.features.map(featureName).join(", ")}` : "");
    const path = connectionPath(positions.get(edge.source), positions.get(edge.target), positions);
    const pairIds = new Set([...edge.pairs, ...(twoWay ? reverse.pairs : [])]);
    const connections = graph.connections.filter(connection => pairIds.has(connection.id) && connection.number);
    connections.forEach((connection, index) => {
      const badgeWidth = Math.max(22, String(connection.number).length * 7 + 12);
      const candidates = [0, -36, 36, -72, 72, -108, 108].flatMap(dy => [0, -40, 40].map(dx => ({
        x: path.badge.x + dx, y: path.badge.y + (index - (connections.length - 1) / 2) * 36 + dy
      })));
      const fits = pos => pos.x >= badgeWidth / 2 && pos.x <= width - badgeWidth / 2 && pos.y >= 12 && pos.y <= height - 12 &&
        occupied.every(box => pos.x + badgeWidth / 2 + 10 <= box.x || pos.x - badgeWidth / 2 - 10 >= box.x + box.width || pos.y + 21 <= box.y || pos.y - 21 >= box.y + box.height);
      const { x, y } = candidates.find(fits) || candidates[0];
      occupied.push({ x: x - badgeWidth / 2, y: y - 11, width: badgeWidth, height: 22 });
      const leader = x !== path.badge.x || y !== path.badge.y ? `<path class="topology-badge-leader" d="M ${path.badge.x - x} ${path.badge.y - y} L 0 0"/>` : "";
      badges.push(`<g class="topology-edge-badge ${cls}" transform="translate(${x},${y})"><title>${esc(`Pair ${connection.number}: ${connectionLabel(connection, nodes)}`)}</title>${leader}<rect x="${-badgeWidth / 2}" y="-11" width="${badgeWidth}" height="22" rx="8"/><text text-anchor="middle" dy=".35em">${connection.number}</text></g>`);
    });
    return `<path class="topology-edge ${cls}" d="${path.d}" marker-end="url(#topology-arrow${selected ? "-selected" : ""})" ${twoWay ? `marker-start="url(#topology-arrow${selected ? "-selected" : ""})"` : ""}><title>${esc(title)}</title></path>`;
  }).join("");
  const cards = graph.nodes.map(node => {
    const pos = positions.get(node.id), meta = window.CW?.ProviderMeta;
    const logo = meta?.logoPath?.(node.provider);
    const tone = meta?.tone?.(node.provider)?.solid || "#7c5cff";
    const label = providerName(node), detail = instanceName(node);
    const cls = selectedNodes.has(node.id) ? "is-selected" : finding ? "is-muted" : "";
    return `<g class="topology-node ${cls}" transform="translate(${pos.x - 90},${pos.y - 34})">
      <title>${esc(nodeName(node))}</title><rect class="topology-node-surface" width="180" height="68" rx="13"/>
      <rect class="topology-node-brand" x="1" y="14" width="3" height="40" rx="1.5" fill="${esc(tone)}"/>
      <rect class="topology-node-icon-bg" x="13" y="17" width="34" height="34" rx="9"/>
      ${logo ? `<image href="${esc(logo)}" x="19" y="23" width="22" height="22"/>` : `<text x="30" y="39" text-anchor="middle">${esc(label.slice(0, 2))}</text>`}
      <text class="topology-node-label" x="58" y="29">${esc(label.length > 16 ? label.slice(0, 15) + "…" : label)}</text>
      <text class="topology-node-detail" x="58" y="47">${esc(detail.length > 19 ? detail.slice(0, 18) + "…" : detail)}</text>
    </g>`;
  }).join("");
  return `<svg class="topology-svg" width="${Math.ceil(width)}" height="${Math.ceil(height)}" viewBox="0 0 ${width} ${height}" role="img" aria-label="${esc(finding ? `Highlighted ${featureName(finding.feature)} routes` : "Configured synchronization routes")}">
    <defs><marker id="topology-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M 1 1 L 9 5 L 1 9 z"/></marker>
    <marker id="topology-arrow-selected" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M 1 1 L 9 5 L 1 9 z"/></marker></defs>${paths}${cards}${badges.join("")}</svg>`;
}
