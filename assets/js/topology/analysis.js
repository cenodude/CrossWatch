/* assets/js/topology/analysis.js */
/* CrossWatch - Sync topology analysis and profile filtering */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import { featureAllowedForPair } from "../modals/pair-config/custom-rules.js";

const key = value => String(value || "").trim().toUpperCase();
const instance = value => String(value || "default").trim() || "default";
const enabled = value => {
  if (value && typeof value === "object") value = value.enable;
  return [true, 1, "1", "true", "on", "yes"].includes(typeof value === "string" ? value.trim().toLowerCase() : value);
};
export const endpointId = (provider, id) => JSON.stringify([key(provider), instance(id)]);
export const edgeId = (source, target) => JSON.stringify([source, target]);
const sorted = values => [...values].sort();

export function pairsForProfile(pairs, profile) {
  if (!profile?.id) return pairs;
  return pairs.filter(pair => pair.profile_id
    ? pair.profile_id === profile.id
    : ["source", "target"].every(side => {
      const values = profile.instances?.[key(pair[side])] || [];
      return (Array.isArray(values) ? values : [values]).includes(instance(pair[`${side}_instance`]));
    }));
}

function shortestPath(adjacency, source, target, excluded = "") {
  const queue = [source], previous = new Map([[source, null]]);
  for (let index = 0; index < queue.length; index++) {
    const current = queue[index];
    if (current === target) {
      const path = [];
      for (let node = target; node !== null; node = previous.get(node)) path.push(node);
      return path.reverse();
    }
    for (const next of adjacency.get(current) || []) {
      if (edgeId(current, next) === excluded || previous.has(next)) continue;
      previous.set(next, current);
      queue.push(next);
    }
  }
  return null;
}

function alternatePath(adjacency, path) {
  for (let index = 1; index < path.length; index++) {
    const alternate = shortestPath(adjacency, path[0], path.at(-1), edgeId(path[index - 1], path[index]));
    if (alternate) return alternate;
  }
  return null;
}

function analyzeGraph(graph) {
  const adjacency = new Map(graph.nodes.map(node => [node.id, []]));
  for (const edge of graph.edges) adjacency.get(edge.source).push(edge.target);
  for (const next of adjacency.values()) next.sort();
  const findings = [];
  const add = (type, identity, paths, edges, severity = "observation", details = {}) => {
    const ids = sorted(new Set(edges.map(edge => edge.id)));
    findings.push({ id: JSON.stringify([graph.feature, type, identity]), feature: graph.feature,
      type, severity, paths, edges: ids, nodes: sorted(new Set(edges.flatMap(edge => [edge.source, edge.target]))), ...details });
  };
  const byId = new Map(graph.edges.map(edge => [edge.id, edge]));
  const pathEdges = paths => paths.flatMap(path => path.slice(1).map((node, index) => byId.get(edgeId(path[index], node))));
  const reachable = new Map();
  for (const node of graph.nodes) {
    const seen = new Set([node.id]), queue = [node.id];
    for (let i = 0; i < queue.length; i++) for (const next of adjacency.get(queue[i])) {
      if (!seen.has(next)) { seen.add(next); queue.push(next); }
    }
    reachable.set(node.id, seen);
  }

  const grouped = new Set();
  for (const node of graph.nodes) {
    if (grouped.has(node.id)) continue;
    const members = graph.nodes.map(item => item.id).filter(id => reachable.get(node.id).has(id) && reachable.get(id).has(node.id));
    members.forEach(id => grouped.add(id));
    const memberSet = new Set(members);
    const edges = graph.edges.filter(edge => memberSet.has(edge.source) && memberSet.has(edge.target));
    for (const edge of edges) {
      const reverse = byId.get(edgeId(edge.target, edge.source));
      const expectedReturn = edge.source !== edge.target && edge.twoWay && reverse?.twoWay && edge.pairs.some(id => reverse.pairs.includes(id));
      const back = shortestPath(adjacency, edge.target, edge.source, expectedReturn ? reverse.id : "");
      if (!back) continue;
      const path = [edge.source, ...back];
      add("loop", members, [path], pathEdges([path]), "warning");
      break;
    }
  }

  for (const target of graph.nodes) {
    const incoming = graph.edges.filter(edge => edge.target === target.id && edge.source !== target.id);
    if (new Set(incoming.map(edge => edge.source)).size > 1) {
      const expectedSharing = incoming.every(edge => {
        const reverse = byId.get(edgeId(edge.target, edge.source));
        return edge.twoWay && reverse?.twoWay && edge.pairs.some(id => reverse.pairs.includes(id));
      });
      if (expectedSharing) {
        const connectedEdges = incoming.flatMap(edge => [edge, byId.get(edgeId(edge.target, edge.source))]);
        add("observation", [target.id, "two-way-sharing"], incoming.map(edge => [edge.source, edge.target]), connectedEdges,
          "observation", { reason: "two-way-sharing", informational: true, destination: target.id });
      } else {
        add("conflict", target.id, incoming.map(edge => [edge.source, edge.target]), incoming,
          ["ratings", "progress"].includes(graph.feature) ? "conflict" : "warning", { destination: target.id });
      }
    }
  }
  for (const source of graph.nodes) for (const target of graph.nodes) {
    if (source.id === target.id) continue;
    const direct = shortestPath(adjacency, source.id, target.id);
    if (!direct) continue;
    const alternate = alternatePath(adjacency, direct);
    if (alternate) add("redundancy", [source.id, target.id], [direct, alternate], pathEdges([direct, alternate]));
  }
  const duplicateGroups = new Set();
  for (const edge of graph.edges) {
    if (edge.pairs.length < 2) continue;
    const group = JSON.stringify([sorted([edge.source, edge.target]), edge.pairs]);
    if (duplicateGroups.has(group)) continue;
    duplicateGroups.add(group);
    add("redundancy", [edge.id, "duplicate"], [[edge.source, edge.target]], [edge]);
  }
  if (!findings.length && graph.nodes.length >= 5) {
    add("observation", "complexity", [], graph.edges);
  }
  return findings;
}

export function analyzeTopology(pairs, providers, featureOrder) {
  const metadata = new Map(providers.map(provider => [key(provider.key || provider.name), provider]));
  const active = pairs.filter(pair => pair && pair.enabled !== false);
  const graphs = [];
  const unsupported = new Set();
  for (const feature of featureOrder) {
    const nodes = new Map(), edges = new Map(), connections = [];
    active.forEach((pair, index) => {
      if (!enabled(pair.features?.[feature])) return;
      const source = key(pair.source), target = key(pair.target);
      const twoWay = String(pair.mode || "one-way").toLowerCase().startsWith("two");
      if (!source || !target) return;
      if (!enabled(metadata.get(source)?.features?.[feature]) || !enabled(metadata.get(target)?.features?.[feature]) ||
          !featureAllowedForPair({ src: source, dst: target, twoWay }, feature) ||
          (feature === "collection" && twoWay)) {
        unsupported.add(String(pair.id ?? index));
        return;
      }
      for (const side of ["source", "target"]) {
        const id = endpointId(pair[side], pair[`${side}_instance`]);
        nodes.set(id, { id, provider: key(pair[side]), instance: instance(pair[`${side}_instance`]) });
      }
      const src = endpointId(source, pair.source_instance), dst = endpointId(target, pair.target_instance);
      const directions = [[src, dst]];
      if (twoWay && featureAllowedForPair({ src: target, dst: source, twoWay }, feature)) directions.push([dst, src]);
      connections.push({ id: String(pair.id ?? index), source: src, target: dst, twoWay: directions.length > 1 });
      for (const [from, to] of directions) {
        const id = edgeId(from, to);
        if (!edges.has(id)) edges.set(id, { id, source: from, target: to, pairs: [], twoWay });
        const edge = edges.get(id);
        edge.pairs.push(String(pair.id ?? index));
        edge.twoWay &&= twoWay;
      }
    });
    if (!edges.size) continue;
    const graph = { feature, nodes: [...nodes.values()].sort((a, b) => a.id.localeCompare(b.id)),
      edges: [...edges.values()].sort((a, b) => a.id.localeCompare(b.id)),
      connections: connections.sort((a, b) => a.id.localeCompare(b.id)) };
    graph.edges.forEach(edge => { edge.pairs = sorted(new Set(edge.pairs)); });
    graphs.push(graph);
  }
  const findings = graphs.flatMap(analyzeGraph).sort((a, b) => {
    const rank = { conflict: 0, warning: 1, observation: 2 };
    return Number(!!a.informational) - Number(!!b.informational) || rank[a.severity] - rank[b.severity] || a.id.localeCompare(b.id);
  });
  const conflicts = findings.filter(f => f.type === "conflict").length;
  const loops = findings.filter(f => f.type === "loop").length;
  const suggestions = findings.filter(f => !f.informational && ["redundancy", "observation"].includes(f.type)).length;
  return { graphs, findings, activePairs: active.length, unsupportedPairs: unsupported.size,
    conflicts, loops, suggestions,
    status: findings.some(f => f.severity === "conflict") ? "attention" :
      findings.some(f => f.severity === "warning") ? "review" : "healthy" };
}
