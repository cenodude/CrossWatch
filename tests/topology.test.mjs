/* tests/topology.test.mjs */
/* CrossWatch - Sync topology analysis and layout tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { runInNewContext } from "node:vm";
import { analyzeTopology, endpointId, pairsForProfile } from "../assets/js/topology/analysis.js";
import { connectionLabel, findingConnections, layoutGraph, mergeGraphs, nodeName, renderGraph } from "../assets/js/topology/graph.js";
import { currentTopology } from "../assets/js/topology/state.js";
import { graphViewBox } from "../assets/js/topology/viewport.js";

globalThis.window = {};
await import("../assets/helpers/feature-meta.js");
const order = window.CW.FeatureMeta.order;
const providers = ["A", "B", "C", "D", "E", "PLEX", "TRAKT", "MDBLIST", "SIMKL", "STREMIO"].map(name => ({ name,
  features: Object.fromEntries(order.map(feature => [feature, true])), capabilities: { bidirectional: true } }));
const pair = (source, target, options = {}) => ({ id: `${source}-${target}`, source, target, enabled: true,
  mode: "one-way", features: { history: { enable: true } }, ...options });
const analyze = (pairs, metadata = providers) => analyzeTopology(pairs, metadata, order);
const findingsOf = (result, type) => result.findings.filter(finding => finding.type === type);

test("simple A → B is healthy", () => {
  const result = analyze([pair("A", "B")]);
  assert.equal(result.status, "healthy");
  assert.equal(result.activePairs, 1);
  assert.equal(result.graphs[0].edges.length, 1);
  assert.deepEqual(result.findings, []);
});
test("A ↔ B contains both directions without warning about a normal return route", () => {
  const result = analyze([pair("A", "B", { mode: "two-way" })]);
  assert.equal(result.graphs[0].edges.length, 2);
  assert.equal(result.loops, 0);
  assert.deepEqual(result.findings, []);
});
test("A → B → C has no redundant paths", () => {
  assert.deepEqual(analyze([pair("A", "B"), pair("B", "C")]).findings, []);
});
test("a shortcut and indirect route produce one redundancy with two witness paths", () => {
  const result = analyze([pair("A", "C"), pair("A", "B"), pair("B", "C")]);
  const redundancy = findingsOf(result, "redundancy");
  assert.equal(redundancy.length, 1);
  assert.deepEqual(redundancy[0].paths, [[endpointId("A"), endpointId("C")], [endpointId("A"), endpointId("B"), endpointId("C")]]);
  assert.equal(result.conflicts, 1);
  assert.equal(result.status, "review");
});
test("a diamond detects alternate indirect routes even without a direct edge", () => {
  const result = analyze([pair("A", "B"), pair("A", "C"), pair("B", "D"), pair("C", "D")]);
  assert.equal(findingsOf(result, "redundancy").length, 1);
  assert.ok(findingsOf(result, "redundancy")[0].paths.every(path => path.length === 3));
});
test("A ↔ B ↔ C does not report expected two-way returns as a loop", () => {
  const result = analyze([pair("A", "B", { mode: "two-way" }), pair("B", "C", { mode: "two-way" })]);
  assert.equal(result.loops, 0);
  assert.equal(result.conflicts, 0);
  assert.equal(result.status, "healthy");
  assert.equal(result.findings[0].reason, "two-way-sharing");
});
test("a directed triangle reports a single loop, not rotations", () => {
  const result = analyze([pair("A", "B"), pair("B", "C"), pair("C", "A")]);
  assert.equal(result.loops, 1);
  const path = findingsOf(result, "loop")[0].paths[0];
  assert.equal(path[0], path.at(-1));
  assert.equal(path.length, 4);
});
test("separately configured reciprocal one-way pairs deserve review", () => {
  assert.equal(analyze([pair("A", "B"), pair("B", "A")]).loops, 1);
});
test("different features never form a synthetic cycle or redundant route", () => {
  const result = analyze([pair("A", "B"), pair("B", "C"), pair("C", "A", { features: { watchlist: true } })]);
  assert.equal(result.graphs.length, 2);
  assert.equal(result.loops, 0);
  assert.equal(result.findings.length, 0);
});
test("disabled pairs do not enter graphs or active counts", () => {
  const result = analyze([pair("A", "B"), pair("B", "A", { enabled: false })]);
  assert.equal(result.activePairs, 1);
  assert.equal(result.graphs[0].edges.length, 1);
});
test("disabled and absent features are omitted", () => {
  const result = analyze([pair("A", "B", { features: { history: { enable: false }, ratings: false } })]);
  assert.equal(result.activePairs, 1);
  assert.deepEqual(result.graphs, []);
});
test("boolean and structured feature flags follow pair configuration", () => {
  const result = analyze([pair("A", "B", { features: { history: true, ratings: { enable: "true" }, watchlist: { enable: "false" } } })]);
  assert.deepEqual(result.graphs.map(graph => graph.feature), ["ratings", "history"]);
});
test("providers without feature capability cannot create paths", () => {
  const metadata = providers.map(provider => provider.name === "B" ? { ...provider, features: { history: false } } : provider);
  const result = analyze([pair("A", "B"), pair("B", "C"), pair("A", "C")], metadata);
  assert.equal(result.graphs[0].edges.length, 1);
  assert.equal(result.unsupportedPairs, 2);
  assert.equal(result.findings.length, 0);
});
test("missing provider metadata is surfaced rather than assumed capable", () => {
  assert.equal(analyze([pair("A", "UNKNOWN")]).unsupportedPairs, 1);
});
test("Stremio ratings respect the shared incoming-only restriction", () => {
  const incoming = pair("TRAKT", "STREMIO", { features: { ratings: true } });
  assert.equal(analyze([incoming]).graphs.length, 1);
  assert.equal(analyze([{ ...incoming, mode: "two-way" }]).graphs.length, 0);
  assert.equal(analyze([pair("STREMIO", "TRAKT", { features: { ratings: true } })]).graphs.length, 0);
});
test("Collections follow the API one-way-only constraint", () => {
  const result = analyze([pair("PLEX", "TRAKT", { mode: "two-way", features: { collection: true } })]);
  assert.equal(result.graphs.length, 0);
  assert.equal(result.unsupportedPairs, 1);
  assert.equal(analyze([pair("PLEX", "TRAKT", { features: { collection: true } })]).graphs[0].edges.length, 1);
});
test("Playlists use canonical capability metadata", () => {
  assert.equal(analyze([pair("A", "B", { features: { playlists: { enable: true, managed_by: "playlists", mappings: ["m1"] } } })]).graphs[0].feature, "playlists");
});
test("multiple ratings writers indicate potential contradictory values", () => {
  const result = analyze([pair("A", "C", { features: { ratings: true } }), pair("B", "C", { features: { ratings: true } })]);
  assert.equal(result.conflicts, 1);
  assert.equal(result.status, "attention");
});
test("provider instances are distinct nodes, including instance IDs with separators", () => {
  const result = analyze([pair("A", "B", { target_instance: "family:one" }), pair("B", "A", { source_instance: "family:two" })]);
  assert.equal(result.graphs[0].nodes.length, 3);
  assert.equal(result.loops, 0);
});
test("duplicate configured routes produce one redundancy and no duplicate writers", () => {
  const result = analyze([pair("A", "B", { id: "one" }), pair("A", "B", { id: "two" })]);
  assert.equal(result.suggestions, 1);
  assert.equal(result.conflicts, 0);
  assert.equal(result.graphs[0].edges.length, 1);
});
test("duplicate bidirectional pairs produce only one duplicate route finding", () => {
  const result = analyze([pair("A", "B", { id: "one", mode: "two-way" }), pair("A", "B", { id: "two", mode: "two-way" })]);
  assert.equal(findingsOf(result, "redundancy").length, 1);
});
test("self routes are detected", () => {
  assert.equal(analyze([pair("A", "A")]).loops, 1);
});
test("analysis does not mutate inputs, duplicate findings or depend on pair order", () => {
  const pairs = [pair("A", "B", { mode: "two-way" }), pair("B", "C", { mode: "two-way" }), pair("A", "C")];
  const before = JSON.stringify(pairs);
  const result = analyze(pairs);
  assert.equal(JSON.stringify(pairs), before);
  assert.deepEqual(result, analyze([...pairs].reverse(), [...providers].reverse()));
  assert.equal(new Set(result.findings.map(finding => finding.id)).size, result.findings.length);
  for (const finding of result.findings) {
    assert.equal(new Set(finding.edges).size, finding.edges.length);
    assert.equal(new Set(finding.nodes).size, finding.nodes.length);
  }
});
test("profile selection includes assigned and matching legacy pairs only", () => {
  const pairs = [pair("A", "B", { profile_id: "one" }), pair("A", "C", { profile_id: "two" }), pair("A", "B", { id: "legacy" })];
  assert.equal(pairsForProfile(pairs, { id: "one", instances: { A: ["default"], B: ["default"] } }).length, 2);
  assert.equal(pairsForProfile(pairs, null).length, 3);
});
test("a larger valid topology can offer a complexity observation", () => {
  const result = analyze([pair("A", "B"), pair("B", "C"), pair("C", "D"), pair("D", "E")]);
  assert.equal(findingsOf(result, "observation").length, 1);
  assert.equal(result.status, "healthy");
});
test("layout has no overlapping cards for chains, stars, circles and disconnected components", () => {
  for (const pairs of [
    [pair("A", "B"), pair("B", "C")],
    [pair("A", "B"), pair("A", "C"), pair("A", "D"), pair("A", "E")],
    [pair("A", "B"), pair("B", "C"), pair("C", "A")],
    [pair("A", "B"), pair("C", "D")],
  ]) {
    const layout = layoutGraph(mergeGraphs(analyze(pairs).graphs));
    const values = [...layout.positions.values()];
    for (let i = 0; i < values.length; i++) for (let j = i + 1; j < values.length; j++) {
      assert.ok(Math.abs(values[i].x - values[j].x) >= 190 || Math.abs(values[i].y - values[j].y) >= 78);
    }
  }
});

test("compact layout keeps provider cards readable within a narrow graph", () => {
  const graph = mergeGraphs(analyze([pair("A", "B"), pair("A", "C"), pair("A", "D"), pair("A", "E")]).graphs);
  const layout = layoutGraph(graph, true);
  assert.ok(layout.width <= 300);
  const positions = [...layout.positions.values()];
  positions.forEach((position, index) => {
    assert.ok(position.x - 90 >= 0 && position.x + 90 <= layout.width);
    assert.ok(position.y - 34 >= 0 && position.y + 34 <= layout.height);
    if (index) assert.ok(position.y - positions[index - 1].y >= 100);
  });
});

const plexHub = (mode = "two-way", feature = "watchlist") => ["MDBLIST", "SIMKL", "TRAKT"].map(target => pair("PLEX", target, { mode, features: { [feature]: true } }));

test("a Plex two-way hub is healthy and describes expected sharing as information", () => {
  const result = analyze(plexHub());
  assert.equal(result.activePairs, 3);
  assert.equal(result.loops, 0);
  assert.equal(result.conflicts, 0);
  assert.equal(result.suggestions, 0);
  assert.equal(result.status, "healthy");
  assert.equal(result.findings.length, 1);
  assert.equal(result.findings[0].informational, true);
  assert.equal(result.findings[0].destination, endpointId("PLEX"));
});

test("hub layout places the configured source left and all targets right for either sync mode", () => {
  for (const mode of ["one-way", "two-way"]) {
    const graph = mergeGraphs(analyze(plexHub(mode)).graphs);
    const { positions } = layoutGraph(graph);
    const source = positions.get(endpointId("PLEX"));
    const targets = ["MDBLIST", "SIMKL", "TRAKT"].map(provider => positions.get(endpointId(provider)));
    assert.ok(targets.every(position => position.x > source.x));
    assert.equal(new Set(targets.map(position => position.x)).size, 1);
    assert.equal(source.y, targets[1].y);
    const compact = layoutGraph(graph, true);
    assert.ok(targets.length && [...compact.positions.entries()].filter(([id]) => id !== endpointId("PLEX")).every(([, position]) => position.y > compact.positions.get(endpointId("PLEX")).y));
  }
});

test("finding routes preserve the configured source and two-way arrows", () => {
  const result = analyze(plexHub());
  const graph = mergeGraphs(result.graphs);
  const nodes = new Map(graph.nodes.map(node => [node.id, node]));
  const connections = findingConnections(result.findings[0], graph);
  assert.deepEqual(connections.map(connection => connectionLabel(connection, nodes)), ["PLEX ↔ MDBLIST", "PLEX ↔ SIMKL", "PLEX ↔ TRAKT"]);
  assert.equal(graph.connections.length, 3);
  assert.equal(graph.edges.length, 6);
});

test("two-way ratings sharing is not evidence of a conflict by itself", () => {
  const result = analyze(plexHub("two-way", "ratings"));
  assert.equal(result.status, "healthy");
  assert.equal(result.conflicts, 0);
  assert.equal(result.findings[0].informational, true);
});

test("mixed one-way and two-way writers retain each pair's actual direction", () => {
  const result = analyze([pair("PLEX", "TRAKT", { mode: "two-way" }), pair("MDBLIST", "PLEX")]);
  assert.equal(result.conflicts, 1);
  const graph = mergeGraphs(result.graphs);
  const nodes = new Map(graph.nodes.map(node => [node.id, node]));
  const routes = findingConnections(findingsOf(result, "conflict")[0], graph).map(connection => connectionLabel(connection, nodes));
  assert.deepEqual(routes, ["MDBLIST → PLEX", "PLEX ↔ TRAKT"]);
});

test("an additional connection between hub targets creates a genuine return route", () => {
  const result = analyze([...plexHub(), pair("MDBLIST", "TRAKT", { features: { watchlist: true } })]);
  assert.equal(result.loops, 1);
  const finding = findingsOf(result, "loop")[0];
  assert.equal(finding.paths[0].length, 4);
  assert.equal(new Set(finding.paths[0]).size, 3);
  assert.ok(!finding.nodes.includes(endpointId("SIMKL")));
  assert.equal(finding.edges.length, 3);
});

test("a two-way triangle uses a real three-provider return path", () => {
  const result = analyze([pair("A", "B", { mode: "two-way" }), pair("B", "C", { mode: "two-way" }), pair("C", "A", { mode: "two-way" })]);
  assert.equal(result.loops, 1);
  const path = findingsOf(result, "loop")[0].paths[0];
  assert.equal(path.length, 4);
  assert.equal(new Set(path).size, 3);
});

test("configured source ordering also applies to a single two-way pair", () => {
  const graph = mergeGraphs(analyze([pair("TRAKT", "PLEX", { mode: "two-way" })]).graphs);
  const { positions } = layoutGraph(graph);
  assert.ok(positions.get(endpointId("TRAKT")).x < positions.get(endpointId("PLEX")).x);
});

test("friendly instance names are shared by graph labels and configured pair routes", () => {
  const config = { plex: { label: "Home", instances: { "plex-p01": { label: "Family & friends" } } }, tmdb_sync: { label: "Movies" } };
  const context = { window: { _cfgCache: config } };
  runInNewContext(readFileSync(new URL("../assets/helpers/provider-meta.js", import.meta.url), "utf8"), context);
  const meta = context.window.CW.ProviderMeta;
  assert.equal(meta.instanceLabel("Plex", "plex-p01"), "Family & friends");
  assert.equal(meta.instanceLabel("PLEX", "default"), "Home");
  assert.equal(meta.instanceLabel("TMDB", "default"), "Movies");
  assert.equal(meta.instanceLabel("PLEX", "missing"), "missing");
  assert.equal(meta.instanceLabel("SIMKL", "default"), "Default instance");
  const previous = window.CW.ProviderMeta;
  window.CW.ProviderMeta = meta;
  try {
    const graph = mergeGraphs(analyze([pair("PLEX", "SIMKL", { source_instance: "plex-p01" })]).graphs);
    const nodes = new Map(graph.nodes.map(node => [node.id, node]));
    assert.equal(nodeName(nodes.get(endpointId("PLEX", "plex-p01"))), "Plex (Family & friends)");
    assert.equal(connectionLabel(graph.connections[0], nodes), "Plex (Family & friends) → SIMKL");
    assert.match(renderGraph(graph), /Family &amp; friends/);
    config.plex.instances["plex-p01"].label = "Updated name";
    assert.match(renderGraph(graph), /Updated name/);
  } finally { window.CW.ProviderMeta = previous; }
});

test("pair numbers retain board order through disabled pairs, profile and feature filters", () => {
  const previous = window.cx, previousProfile = window.CW.OverviewProfile;
  window.cx = { providers, pairs: [
    pair("A", "B", { enabled: false }),
    pair("PLEX", "TRAKT", { profile_id: "home" }),
    pair("PLEX", "SIMKL", { profile_id: "away" }),
    pair("PLEX", "MDBLIST", { profile_id: "home", features: { watchlist: true } })
  ] };
  window.CW.OverviewProfile = { profile: { id: "home" } };
  try {
    const result = currentTopology();
    assert.deepEqual(mergeGraphs(result.graphs).connections.map(connection => connection.number), [2, 4]);
    assert.equal(mergeGraphs(result.graphs.filter(graph => graph.feature === "watchlist")).connections[0].number, 4);
    window.cx.pairs.reverse();
    const reordered = mergeGraphs(currentTopology().graphs).connections;
    assert.deepEqual(reordered.map(connection => [connection.id, connection.number]), [["PLEX-MDBLIST", 1], ["PLEX-TRAKT", 3]]);
  } finally { window.cx = previous; window.CW.OverviewProfile = previousProfile; }
});

test("a merged two-way route carries each pair number once across features", () => {
  const previous = window.cx;
  window.cx = { providers, pairs: [
    pair("PLEX", "TRAKT", { id: "first", mode: "two-way", features: { history: true, watchlist: true } }),
    pair("PLEX", "TRAKT", { id: "second", mode: "two-way", features: { history: true, watchlist: true } })
  ] };
  try {
    const result = currentTopology(), graph = mergeGraphs(result.graphs);
    assert.deepEqual(graph.connections.map(connection => connection.number), [1, 2]);
    const svg = renderGraph(graph);
    assert.equal((svg.match(/class="topology-edge-badge /g) || []).length, 2);
    assert.match(svg, /Pair 1:/);
    assert.match(svg, /Pair 2:/);
    assert.deepEqual(findingConnections(result.findings[0], graph).map(connection => connection.number), [1, 2]);
  } finally { window.cx = previous; }
});

test("separate pairs on the same edge retain their numbers when features are merged", () => {
  const previous = window.cx;
  window.cx = { providers, pairs: [pair("PLEX", "TRAKT", { id: "history" }), pair("PLEX", "TRAKT", { id: "watchlist", features: { watchlist: true } })] };
  try {
    const result = currentTopology();
    const merged = renderGraph(mergeGraphs(result.graphs));
    assert.match(merged, /Pair 1:/);
    assert.match(merged, /Pair 2:/);
    const watchlist = renderGraph(mergeGraphs(result.graphs.filter(graph => graph.feature === "watchlist")));
    assert.doesNotMatch(watchlist, /Pair 1:/);
    assert.match(watchlist, /Pair 2:/);
  } finally { window.cx = previous; }
});

test("zoom keeps its center, supports zooming out and clamps panning to the graph", () => {
  const fit = graphViewBox(800, 400, 1);
  assert.deepEqual([fit.x, fit.y, fit.width, fit.height], [0, 0, 800, 400]);
  const enlarged = graphViewBox(800, 400, 2);
  assert.deepEqual([enlarged.x, enlarged.y, enlarged.width, enlarged.height], [200, 100, 400, 200]);
  const edge = graphViewBox(800, 400, 2, { x: 100, y: -100 });
  assert.equal(edge.x + edge.width, 800);
  assert.equal(edge.y, 0);
  const zoomedOut = graphViewBox(800, 400, .5, edge.center);
  assert.deepEqual([zoomedOut.x, zoomedOut.y, zoomedOut.width, zoomedOut.height], [-400, -200, 1600, 800]);
  assert.equal(graphViewBox(800, 400, 10).scale, 2.5);
  assert.equal(graphViewBox(800, 400, .1).scale, .5);
});

test("two source instances align with their targets to avoid crossing diagonals", () => {
  const graph = mergeGraphs(analyze([
    pair("PLEX", "SIMKL", { mode: "two-way" }),
    pair("PLEX", "TRAKT", { mode: "two-way" }),
    pair("PLEX", "MDBLIST", { mode: "two-way" }),
    pair("SIMKL", "MDBLIST"),
    pair("PLEX", "SIMKL", { id: "family", source_instance: "family" })
  ]).graphs);
  const { positions } = layoutGraph(graph);
  const home = positions.get(endpointId("PLEX")), family = positions.get(endpointId("PLEX", "family"));
  const simkl = positions.get(endpointId("SIMKL")), trakt = positions.get(endpointId("TRAKT"));
  assert.equal(Math.sign(family.y - home.y), Math.sign(simkl.y - trakt.y));
  assert.ok(Math.abs(home.y - family.y) >= 140);
  assert.ok(simkl.x - family.x >= 310);
});
